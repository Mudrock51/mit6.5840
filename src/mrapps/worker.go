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

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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

type ByKey []KeyValue

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		args := CallTaskArgs{}
		reply := CallTaskResponse{}
		//fmt.Printf("call task\n")
		ok := call("Coordinator.CallTask", &args, &reply)
		//fmt.Printf("get call task\n")
		//now:=time.Now()
		if ok {
			switch reply.TaskType {
			case mapType:
				executeMap(reply.FileName, reply.NReduce, reply.TaskID, mapf)
			case reduceType:
				//fmt.Printf("get a reduce\n")
				executeReduce(reply.NFiles, reply.TaskID, reducef)
			case waiting:
				time.Sleep(time.Second * 2)
				continue
			case done:
				//fmt.Printf("Worker exited")
				os.Exit(0)
			}
		} else {
			//fmt.Printf("don't get call reply")
			time.Sleep(time.Second * 2)
			continue
		}
		//fmt.Printf("finish task: %v %v use %v\n",reply.TaskID,reply.taskType,time.Since(now).Seconds())
		a := CallTaskDoneArgs{reply.TaskID, reply.TaskType}
		r := CallTaskDoneResponse{}
		//fmt.Print("callDone\n")
		call("Coordinator.CallTaskDone", &a, &r)
		//fmt.Print("get callDone\n")
		time.Sleep(time.Second * 2)
	}

}

func executeMap(FileName string, nReduce int, taskID int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(FileName)
	if err != nil {
		log.Fatalf("cannot open %v", FileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", FileName)
	}
	file.Close()
	kva := mapf(FileName, string(content))
	files := []*os.File{}
	tmpFileNames := []string{}
	encoders := []*json.Encoder{}
	for i := 0; i < nReduce; i++ {
		tempFile, err := os.CreateTemp("./", "")
		if err != nil {
			log.Fatalf("cannot open temp file")
		}
		files = append(files, tempFile)
		tmpFileNames = append(tmpFileNames, tempFile.Name())
		encoders = append(encoders, json.NewEncoder(tempFile))
	}
	for _, kv := range kva {
		n := ihash(kv.Key) % nReduce
		encoders[n].Encode(kv)
	}
	for i := 0; i < nReduce; i++ {
		files[i].Close()
		os.Rename(tmpFileNames[i], "./"+intermediate(taskID, i))
	}
}

func intermediate(x, y int) string {
	return fmt.Sprintf("mr-%v-%v", x, y)
}

func executeReduce(nFiles int, taskID int, reducef func(string, []string) string) {
	kvs := []KeyValue{}
	for i := 0; i < nFiles; i++ {
		filename := intermediate(i, taskID)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		file.Close()
	}
	oname := fmt.Sprintf("mr-out-%v", taskID)
	//fmt.Printf("create mr-out!")
	tempFile, _ := os.CreateTemp("./", "")
	tempFileName := tempFile.Name()
	sort.Sort(ByKey(kvs))
	for i := 0; i < len(kvs); {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		output := reducef(kvs[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", kvs[i].Key, output)
		i = j
	}
	tempFile.Close()
	os.Rename(tempFileName, "./"+oname)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
