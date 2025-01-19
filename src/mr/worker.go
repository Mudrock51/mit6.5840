package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
)

// use ihash(key) % NReduce to choose the reduce
// Task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// *****************************************************

type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type MRWorker struct {
	WorkerId   int
	MapFunc    func(string, string) []KeyValue
	ReduceFunc func(string, []string) string
	Task       *Task
	NReduce    int
	IsDone     bool
}

// *****************************************************

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.
	worker := MRWorker{
		WorkerId:   -1,
		MapFunc:    mapf,
		ReduceFunc: reducef,
		IsDone:     false,
	}

	for !worker.IsDone {
		worker.Work()
	}
}

// worker 的具体工作流程 loop
func (worker *MRWorker) Work() {
	task := worker.AskForTask()
	if task != nil {
		worker.Task = task
		switch task.TaskType {
		case Map:
			worker.WorkerId = task.MapWorkerId
			worker.ExecuteMap(worker.MapFunc)
		case Reduce:
			worker.WorkerId = task.ReduceWorkerId
			worker.ExecuteReduce(worker.ReduceFunc)
		}
	} else {
		if !worker.IsDone {
			worker.Work()
		}
	}
}

// 请求任务
func (worker *MRWorker) AskForTask() *Task {
	args := TaskArgs{}
	reply := TaskReply{}
	err := Call("Coordinator.AssignTasks", &args, &reply)
	if err != nil {
		log.Println("AskForTask(), Call(), err = ", err)
		return nil
	}

	if &reply != nil {
		if reply.AllDone {
			worker.IsDone = true
			return nil
		}

		worker.NReduce = reply.NReduce
		return reply.Task
	} else {
		return nil
	}
}

//*****************************************************
// MapFunc 任务环节

// 执行 Mapf 任务
func (worker *MRWorker) ExecuteMap(mapf func(string, string) []KeyValue) {
	task := worker.Task

	// 生成 Map 任务结果, intermediate 是 KeyValue 切片
	intermediate := worker.GenerateIntermediate(task.InputFile, mapf)

	// 将结果写入临时文件
	files := worker.WriteIntermediateToTmpFiles(intermediate)

	// 通过 rpc 告诉 Coordinator 目前已经完成了 Map 任务
	worker.MapTaskDone(files)
}

// 调用 Mapf 函数获得中间结果 Intermediate
func (worker *MRWorker) GenerateIntermediate(filename string, mapf func(string, string) []KeyValue) []KeyValue {
	intermediate := make([]KeyValue, 0)
	f, err := os.Open(filename)
	if err != nil {
		log.Fatalln("GenerateIntermediate(), os.Open(), err = ", err)

	}

	content, err := io.ReadAll(f)
	if err != nil {
		log.Fatalln("GenerateIntermediate(), io.ReadAll(), err = ", err)

	}
	f.Close()

	// MapFunc 崩溃处理
	defer worker.CrashHandle()

	kva := worker.MapFunc(filename, string(content))
	intermediate = append(intermediate, kva...)
	return intermediate
}

// write intermediates to temp Files
func (worker *MRWorker) WriteIntermediateToTmpFiles(intermediate []KeyValue) []string {
	hashedIntermediate := make([][]KeyValue, worker.NReduce)

	for _, kv := range intermediate {
		// 通过 Hash 均分任务
		hashVal := ihash(kv.Key) % worker.NReduce
		hashedIntermediate[hashVal] = append(hashedIntermediate[hashVal], kv)
	}

	var tmpFiles []string
	for i := 0; i < worker.NReduce; i++ {
		tmpFile, err := os.CreateTemp(TempFilePath, "mr-")
		if err != nil {
			log.Println("WriteIntermediateToTmpFiles(), os.CreateTemp(), err = ", err)
		}

		// JSON 格式存储在临时文件
		enc := json.NewEncoder(tmpFile)
		for _, kv := range hashedIntermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Println("WriteIntermediateToTmpFiles(), enc.Encode(), err = ", err)
			}
		}

		tempName := fmt.Sprintf("mr-%v-%v", worker.WorkerId, i)
		err = os.Rename(tmpFile.Name(), TempFilePath+tempName)
		if err != nil {
			log.Println("WriteIntermediateToTmpFiles(), os.Rename(), err = ", err)
		}
		tmpFiles = append(tmpFiles, TempFilePath+tempName)
	}

	return tmpFiles
}

// 结束 MapFunc
func (worker *MRWorker) MapTaskDone(files []string) {
	args := MapDoneArgs{
		MapWorkerId: worker.WorkerId,
		Files:       files,
	}
	reply := MapDoneReply{}
	Call("Coordinator.MapTaskDone", &args, &reply)
}

//*****************************************************

//*****************************************************
// ReduceFunc 任务环节

// 执行 Reducef 任务
func (worker *MRWorker) ExecuteReduce(reducef func(string, []string) string) {
	task := worker.Task

	// 自定义产生 Reduce 任务, intermediate 是 KeyValue 切片
	intermediate := worker.GenerateReduceResult(task.InputFile, reducef)

	// 将 Reduce 结果写入临时文件
	files := worker.WriteReduceResultToTmpFile(task, intermediate)

	// 通过 rpc 告知 Coordinator 任务完成
	worker.ReduceTaskDone(files)
}

// generate reduce result
func (worker *MRWorker) GenerateReduceResult(filePattern string, reducef func(string, []string) string) []KeyValue {
	intermediate := make([]KeyValue, 0)
	files, _ := filepath.Glob(filePattern)

	for _, filePath := range files {
		file, _ := os.Open(filePath)

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	result := make([]KeyValue, 0)
	i := 0
	for i < len(intermediate) {
		// intermediate[i..j] are same elems
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		defer worker.CrashHandle()

		output := reducef(intermediate[i].Key, values)

		kv := KeyValue{
			Key:   intermediate[i].Key,
			Value: output,
		}

		result = append(result, kv)

		i = j
	}
	return result
}

// write result to file
func (worker *MRWorker) WriteReduceResultToTmpFile(task *Task, intermediate []KeyValue) []string {
	tempFile, err := os.CreateTemp(TempFilePath, "mr-")
	if err != nil {
		log.Println("WriteReduceResultToTmpFile(), os.CreateTemp(), err = ", err)
	}

	for _, kv := range intermediate {
		fmt.Fprintf(tempFile, "%v %v\n", kv.Key, kv.Value)
	}

	index := task.InputFile[len(task.InputFile)-1:]
	tempName := "mr-out-" + index
	err = os.Rename(tempFile.Name(), TempFilePath+tempName)
	if err != nil {
		log.Println("WriteReduceResultToTmpFile(), os.Rename(), err = ", err)
	}

	return []string{TempFilePath + tempName}
}

// 结束 ReduceFunc
func (worker *MRWorker) ReduceTaskDone(files []string) {
	args := ReduceDoneArgs{
		ReduceWorkerId: worker.WorkerId,
		Files:          files,
	}
	reply := ReduceDoneReply{}
	Call("Coordinator.ReduceTaskDone", &args, &reply)

}

// 处理崩溃 Crash
func (worker *MRWorker) CrashHandle() {
	if r := recover(); r != nil {
		worker.Work()
	}
}

//*****************************************************

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func Call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		log.Println("Call(), err = ", err)
		return err
	}

	return nil
}
