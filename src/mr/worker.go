package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// infinite loop to keep requesting and executing tasks
	for {
		// declare task request and reply structures
		args := ExampleArgs{}
		reply := ExampleReply{}

		// request a task from coordinator
		ok := call("Coordinator.GetTask", &args, &reply)
		if !ok {
			//log.Printf("connection failed to build")
			return
		}

		// handle different task types
		switch reply.TaskType {
		case "map":
			// read input file
			file, err := os.Open(reply.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", reply.Filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Filename)
			}
			file.Close()

			// call Map function
			kva := mapf(reply.Filename, string(content))

			// partition map output into nReduce intermediate files
			intermediateFiles := make([]*os.File, reply.NReduce)
			for i := 0; i < reply.NReduce; i++ {
				oname := fmt.Sprintf("mr-%d-%d", reply.TaskNum, i)
				intermediateFiles[i], _ = os.Create(oname)
			}

			// write key-value pairs to intermediate files
			for _, kv := range kva {
				r := ihash(kv.Key) % reply.NReduce
				fmt.Fprintf(intermediateFiles[r], "%v %v\n", kv.Key, kv.Value)
			}

			// close all intermediate files
			for _, f := range intermediateFiles {
				f.Close()
			}

		case "reduce":
			// read all intermediate files for this reduce task
			var intermediate []KeyValue
			for i := 0; i < reply.NMap; i++ {
				filename := fmt.Sprintf("mr-%d-%d", i, reply.TaskNum)
				file, err := os.Open(filename)
				if err != nil {
					continue
				}
				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					var kv KeyValue
					fmt.Sscanf(scanner.Text(), "%v %v", &kv.Key, &kv.Value)
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}

			// sort intermediate data
			sort.Sort(ByKey(intermediate))

			// create output file
			oname := fmt.Sprintf("mr-out-%d", reply.TaskNum)
			ofile, _ := os.Create(oname)

			// call Reduce on each distinct key
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}
			ofile.Close()

		case "wait":
			time.Sleep(time.Second)
			continue

		case "exit":
			return
		}

		// notify coordinator task is complete
		// doneArgs := DoneArgs{}
		// doneReply := DoneReply{}
		// call("Coordinator.tasksDone", &doneArgs, &doneReply)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
/*func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}*/

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		//log.Fatal("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
