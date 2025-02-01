package mr

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	filename  string
	content   string
	status    string
	timestamp time.Time
}

type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	phase       string        // current phase ("map" or "reduce")
	nMap        int           // number of map tasks
	nReduce     int           // number of reduce tasks
	mapTasks    map[int]*Task // list of map task input files
	reduceTasks map[int]*Task // list of reduce task numbers
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	// reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) InitWorker(args *InitWorkerArgs, reply *InitWorkerReply) error {
	reply.NMap = c.nMap
	reply.NReduce = c.nReduce
	return nil
}

// GetTask handles task assignment requests from workers
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	// Add mutex to Coordinator struct if not already present:
	// mu sync.Mutex
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.mapTasks) == 0 {
		c.phase = "reduce"
	}

	// Check if there are map tasks to be done
	if len(c.mapTasks) > 0 {
		// Assign a map task
		for i, mapTask := range c.mapTasks {
			if mapTask.status == "idle" {
				reply.Scheduled = true
				reply.Phase = "map"
				reply.TaskId = i
				reply.Filename = mapTask.filename
				reply.Content = mapTask.content
				mapTask.status = "in-progress"
				mapTask.timestamp = time.Now()
				break
			}
		}
	} else {
		// If all map tasks are done, move to reduce tasks
		// Assign a reduce task
		for i, reduceTask := range c.reduceTasks {
			if reduceTask.status == "idle" {
				reply.Scheduled = true
				reply.Phase = "reduce"
				reply.TaskId = i
				reply.Filename = fmt.Sprintf("mr-*-%d", i)
				reply.Content = ""
				reduceTask.status = "in-progress"
				reduceTask.timestamp = time.Now()
				break
			}
		}
	}

	return nil
}

func (c *Coordinator) CommitTask(
	args *CommitTaskArgs,
	reply *CommitTaskReply,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	phase := args.Phase
	taskId := args.TaskId
	if phase == "map" && c.mapTasks[taskId].status == "in-progress" {
		delete(c.mapTasks, taskId)
	} else if phase == "reduce" && c.reduceTasks[taskId].status == "in-progress" {
		delete(c.reduceTasks, taskId)
	}

	reply.Done = (c.phase == "reduce" && len(c.reduceTasks) == 0)
	return nil
}

func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == "reduce" && len(c.reduceTasks) == 0
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) heartbeat() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, task := range c.mapTasks {
		if task.status == "idle" {
			continue
		}
		diff := time.Since(task.timestamp)
		if diff.Seconds() > 10 {
			task.status = "idle"
		}
	}

	for _, task := range c.reduceTasks {
		if task.status == "idle" {
			continue
		}
		diff := time.Since(task.timestamp)
		if diff.Seconds() > 10 {
			task.status = "idle"
		}
	}
}

func (c *Coordinator) UpdateTaskTimestamp(
	args *UpdateTimestampTaskArgs,
	reply *UpdateTimestampReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	phase := args.Phase
	log.Printf("Update task timestamp is called with %d", args.TaskId)

	if phase == "map" {
		if task, exists := c.mapTasks[args.TaskId]; exists {
			task.timestamp = time.Now()
			reply.Done = true
		} else {
			reply.Done = false
		}
	} else if phase == "reduce" {
		if task, exists := c.reduceTasks[args.TaskId]; exists {
			task.timestamp = time.Now()
			reply.Done = true
		} else {
			reply.Done = false
		}
	} else {
		reply.Done = false
	}

	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		phase:       "map",
		nMap:        len(files),
		nReduce:     nReduce,
		mapTasks:    make(map[int]*Task),
		reduceTasks: make(map[int]*Task),
	}

	for index, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()

		c.mapTasks[index] = &Task{
			filename: filename,
			content:  string(content),
			status:   "idle",
		}
	}

	for index := 0; index < nReduce; index++ {
		c.reduceTasks[index] = &Task{
			status: "idle",
		}
	}

	go func() {
		for {
			c.heartbeat()
			time.Sleep(time.Second)
		}
	}()

	c.server()
	return &c
}
