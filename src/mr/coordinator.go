package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	mapTasks    []string          // list of map task input files
	reduceTasks []int             // list of reduce task numbers
	nReduce     int               // number of reduce tasks
	nMap        int               // number of map tasks
	phase       string            // current phase ("map" or "reduce")
	taskStatus  map[string]string // tracks status of each task ("idle", "in-progress", "completed")

}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	// reply.Y = args.X + 1
	return nil
}

// GetTask handles task assignment requests from workers
func (c *Coordinator) GetTask(args *ExampleArgs, reply *ExampleReply) error {
	// Add mutex to Coordinator struct if not already present:
	// mu sync.Mutex
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if there are map tasks to be done
	if len(c.mapTasks) > 0 {
		// Assign a map task
		for i, filename := range c.mapTasks {
			if c.taskStatus[filename] == "idle" {
				reply.TaskType = "map"
				reply.Filename = filename
				reply.TaskNum = i
				reply.NReduce = c.nReduce
				c.taskStatus[filename] = "in-progress"

				// Remove task from mapTasks slice
				c.mapTasks = append(c.mapTasks[:i], c.mapTasks[i+1:]...)
				return nil
			}
		}
	}

	// If all map tasks are done, move to reduce tasks
	if len(c.mapTasks) == 0 && len(c.reduceTasks) > 0 {
		// Assign a reduce task
		for i, taskNum := range c.reduceTasks {
			taskKey := fmt.Sprintf("reduce-%d", taskNum)
			if c.taskStatus[taskKey] != "in-progress" {
				reply.TaskType = "reduce"
				reply.TaskNum = taskNum
				reply.NMap = c.nMap
				c.taskStatus[taskKey] = "in-progress"

				// Remove task from reduceTasks slice
				c.reduceTasks = append(c.reduceTasks[:i], c.reduceTasks[i+1:]...)
				return nil
			}
		}
	}

	// No tasks available
	reply.TaskType = "wait"
	return nil
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	// check if all tasks are completed
	if len(c.mapTasks) > 0 || len(c.reduceTasks) > 0 {
		return false
	}
	ret = true

	return ret
}

/*func (c *Coordinator) tasksDone(args *DoneArgs, reply *DoneReply) error {
	reply.Done = c.Done()
	return nil
}*/

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// initialize coordinator fields
	c.mapTasks = make([]string, len(files))
	c.reduceTasks = make([]int, nReduce)
	c.nReduce = nReduce
	c.nMap = len(files)
	c.taskStatus = make(map[string]string)

	// populate map tasks with input files
	for i, file := range files {
		c.mapTasks[i] = file
		c.taskStatus[file] = "idle"
	}

	// initialize reduce tasks
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = i
		c.taskStatus[fmt.Sprintf("reduce-%d", i)] = "idle"
	}

	c.server()
	return &c
}
