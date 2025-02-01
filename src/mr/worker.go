package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
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
	// declare task request and reply structures
	initArgs := InitWorkerArgs{}
	initReply := InitWorkerReply{}

	call("Coordinator.InitWorker", &initArgs, &initReply)
	nMap := initReply.NMap
	nReduce := initReply.NReduce

	for {
		getTaskArgs := GetTaskArgs{}
		getTaskReply := GetTaskReply{}

		call("Coordinator.GetTask", &getTaskArgs, &getTaskReply)

		if !getTaskReply.Scheduled {
			time.Sleep(time.Second)
			continue
		}

		taskId := getTaskReply.TaskId
		phase := getTaskReply.Phase
		filename := getTaskReply.Filename
		content := getTaskReply.Content

		if phase == "map" {
			mapResult := mapf(filename, content)
			outputFileList := make([]*os.File, nReduce)
			for i := 0; i < nReduce; i++ {
				outputFile, err := os.CreateTemp("", fmt.Sprintf("mr-%d-%d", taskId, i))
				if err != nil {
					log.Fatalf("cannot create mr-%d-%d", taskId, i)
				}
				outputFileList[i] = outputFile
			}

			for _, kv := range mapResult {
				reduceId := ihash(kv.Key) % nReduce
				outputFile := outputFileList[reduceId]

				enc := json.NewEncoder(outputFile)
				encodeErr := enc.Encode(&kv)
				if encodeErr != nil {
					return
				}
			}

			for reduceId := 0; reduceId < nReduce; reduceId++ {
				resultFilename := fmt.Sprintf("mr-%d-%d", taskId, reduceId)
				outputFile := outputFileList[reduceId]
				if err := os.Rename(outputFile.Name(), resultFilename); err != nil {
					inputFile, err := os.Open(outputFile.Name())
					if err != nil {
						log.Fatalf("cannot open temporary output file %s, %s", outputFile.Name(), err)
					}
					defer inputFile.Close()

					outputFileCopy, err := os.Create(resultFilename)
					if err != nil {
						log.Fatalf("cannot create output file %s, %s", resultFilename, err)
					}
					defer outputFileCopy.Close()

					// Copy the contents
					if _, err := io.Copy(outputFileCopy, inputFile); err != nil {
						log.Fatalf("failed to copy file from %s to %s, %s", outputFile.Name(), resultFilename, err)
					}

					// Remove the original temporary file
					os.Remove(outputFile.Name())
				}
				outputFile.Close()
			}
		}

		//log.Println("I am here 1")

		if phase == "reduce" {
			reduceInput := []KeyValue{}
			for mapId := 0; mapId < nMap; mapId++ {
				filename := fmt.Sprintf("mr-%d-%d", mapId, taskId)
				file, err := os.Open(filename)
				if err != nil {
					break
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					reduceInput = append(reduceInput, kv)
				}
			}

			sort.Sort(ByKey(reduceInput))

			outputFileName := fmt.Sprintf(
				"mr-out-%d",
				taskId,
			)
			outputFile, _ := os.CreateTemp("", outputFileName)
			log.Printf("output temporary file %s", outputFileName)
			if _, err := os.Stat(outputFile.Name()); os.IsNotExist(err) {
				log.Printf("Temporary output file %s does not exist, creating it", outputFile.Name())
			}

			i := 0
			for i < len(reduceInput) {
				j := i + 1
				for j < len(reduceInput) && reduceInput[j].Key == reduceInput[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, reduceInput[k].Value)
				}
				output := reducef(reduceInput[i].Key, values)

				fmt.Fprintf(outputFile, "%v %v\n", reduceInput[i].Key, output)

				i = j
			}

			if err := os.Rename(outputFile.Name(), outputFileName); err != nil {
				log.Printf("cannot rename the output file %s, %s", outputFileName, err)
				inputFile, err := os.Open(outputFile.Name())
				if err != nil {
					log.Fatalf("cannot open temporary output file %s, %s", outputFile.Name(), err)
				}
				defer inputFile.Close()

				outputFileCopy, err := os.Create(outputFileName)
				if err != nil {
					log.Fatalf("cannot create output file %s, %s", outputFileName, err)
				}
				defer outputFileCopy.Close()

				// Copy the contents
				if _, err := io.Copy(outputFileCopy, inputFile); err != nil {
					log.Fatalf("failed to copy file from %s to %s, %s", outputFile.Name(), outputFileName, err)
				}

				os.Remove(outputFile.Name())
			}

			outputFile.Close()
		}
		//log.Printf("I am here 2")

		CommitTaskArgs := CommitTaskArgs{
			Phase:  phase,
			TaskId: taskId,
		}
		CommitTaskReply := CommitTaskReply{}
		call("Coordinator.CommitTask", &CommitTaskArgs, &CommitTaskReply)
		if CommitTaskReply.Done {
			return
		}

	}

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
