package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		reply := getTask(GetTaskArgs{})
		if reply.Task == nil {
			return
		}

		switch reply.Task.Type {
		case TaskTypeWait:
			time.Sleep(500 * time.Millisecond)
		case TaskTypeMap:
			file, err := os.Open(reply.Task.Filenames[0])
			if err != nil {
				log.Fatal(err)
			}
			defer file.Close()

			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatal(err)
			}

			kva := mapf(reply.Task.Filenames[0], string(content))
			buckets := buckets(kva, reply.NReduce)

			onames := make([]string, 0)
			for i, kva := range buckets {
				oname := fmt.Sprintf("mr-%d-%d", reply.Task.Idx, i)
				ofile, err := os.Create(oname)
				if err != nil {
					log.Fatal(err)
				}
				defer ofile.Close()

				enc := json.NewEncoder(ofile)
				for _, kv := range kva {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatal(err)
					}
				}

				onames = append(onames, oname)
			}

			taskFinish(TaskFinishArgs{
				Idx:             reply.Task.Idx,
				Type:            reply.Task.Type,
				ReduceFilenames: onames,
			})
		case TaskTypeReduce:
			intermediate := kva(reply.Task.Filenames)
			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%d", reply.Task.Idx)
			ofile, err := os.Create(oname)
			if err != nil {
				log.Fatal(err)
			}
			defer ofile.Close()

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := make([]string, 0)
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			taskFinish(TaskFinishArgs{
				Idx:  reply.Task.Idx,
				Type: reply.Task.Type,
			})
		}
	}
}

// returns nReduce buckets of intermediate key-values
func buckets(kva []KeyValue, nReduce int) [][]KeyValue {
	buckets := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		idx := ihash(kv.Key) % nReduce
		buckets[idx] = append(buckets[idx], kv)
	}
	return buckets
}

// returns intermediate key-values from files
func kva(files []string) []KeyValue {
	kva := make([]KeyValue, 0)
	for _, name := range files {
		file, err := os.Open(name)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	return kva
}

func getTask(args GetTaskArgs) GetTaskReply {
	var reply GetTaskReply
	if ok := call("Coordinator.GetTask", &args, &reply); !ok {
		log.Println("call: something went wrong")
	}
	return reply
}

func taskFinish(args TaskFinishArgs) TaskFinishReply {
	var reply TaskFinishReply
	if ok := call("Coordinator.TaskFinish", &args, &reply); !ok {
		log.Println("call: something went wrong")
	}
	return reply
}
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
