package mr

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Coordinator struct {
	MapChannel      chan *Task      // map tasks 的 Channel 通道, 缓冲池
	ReduceChannel   chan *Task      // reduce tasks 的 Channel 通道, 缓冲池
	Files           []string        // 输入文件
	MapNum          int             // mapper 数量
	ReduceNum       int             // reducer 数量
	DistributePhase DistributePhase // 当前工作阶段
	WorkerId        int
	Mutex           sync.Mutex
	IsDone          bool
}

type Task struct {
	TaskType       TaskType   // 任务类型
	MapWorkerId    int        // 保存 Map 任务的 MapWorkerId
	ReduceWorkerId int        // 保存 Reduce 任务的 ReduceWorkerId
	InputFile      string     // 任务要处理的文件路径
	TaskBeginTime  time.Time  // Task begin time, given by master
	TaskStatus     TaskStatus // ready, running or finished, for worker it should always be running
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Your code here.
	c.Mutex.Lock()
	done := c.IsDone
	c.Mutex.Unlock()
	return done
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) Server() {
	rpc.Register(c)
	rpc.HandleHTTP()

	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Printf("Coordinator RPC Server listening on port:%s", sockname)
	go http.Serve(l, nil)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.Println("run MakeCoordinator()")

	// Init Coordinator
	c := Coordinator{
		MapChannel:      make(chan *Task, len(files)),
		ReduceChannel:   make(chan *Task, nReduce),
		Files:           files,
		MapNum:          len(files),
		ReduceNum:       nReduce,
		DistributePhase: MapPhase,
		WorkerId:        1,
	}

	// 自定义生成 Map 任务
	c.GenerateMapTasks(files)

	// 处理过期 (>10 seconds) 的 Map、Reduce 任务
	go c.PeriodicallyRemoveExpiredTasks()

	// 监听 rpc 调用
	c.Server()

	return &c
}

// Your code here -- RPC handlers for the worker to Call.

// 生产者/Coordinator 生成对应的任务
func (c *Coordinator) GenerateMapTasks(files []string) {
	// 清除上次运行记录
	oldFiles, _ := filepath.Glob(TempFilePath + "mr-*")
	for _, oldFile := range oldFiles {
		err := os.Remove(oldFile)
		if err != nil {
			log.Fatalln("GenerateMapTasks, os.Remove(), err = ", err)
		}
	}

	// 创建任务
	c.Mutex.Lock()
	for _, file := range files {
		task := Task{
			TaskType:   Map,
			InputFile:  file,
			TaskStatus: Ready,
		}
		c.MapChannel <- &task
	}
	c.Mutex.Unlock()
	// log.Println("Finish generating all map tasks")
}
func (c *Coordinator) GenerateReduceTasks() {
	c.Mutex.Lock()
	for i := 0; i < c.ReduceNum; i++ {
		task := Task{
			TaskType:   Reduce,
			InputFile:  fmt.Sprintf("%vmr-*-%v", TempFilePath, i),
			TaskStatus: Ready,
		}
		c.ReduceChannel <- &task
	}
	c.Mutex.Unlock()
	// log.Println("Finish generating all reduce tasks")
}

// 处理超时任务, 重置状态
func (c *Coordinator) PeriodicallyRemoveExpiredTasks() {
	for !c.Done() {
		time.Sleep(time.Second)
		c.Mutex.Lock()
		if c.DistributePhase == MapPhase { // 处理 Mapf 超时
			for i := 0; i < c.MapNum; i++ {
				task := <-c.MapChannel
				c.MapChannel <- task
				c.ReMoveTimeoutTask(task)
			}
		} else { // 处理 Reducef 超时
			for i := 0; i < c.ReduceNum; i++ {
				task := <-c.ReduceChannel
				c.ReduceChannel <- task
				c.ReMoveTimeoutTask(task)
			}
		}
		c.Mutex.Unlock()
	}
}
func (c *Coordinator) ReMoveTimeoutTask(task *Task) {
	if task.TaskStatus == Running && (time.Now().Sub(task.TaskBeginTime) >= (TaskExpiredTime * time.Second)) {
		task.TaskStatus = Ready
	}
}

// 分配闲置任务
func (c *Coordinator) AssignTasks(args *TaskArgs, reply *TaskReply) error {
	if c.DistributePhase == MapPhase {
		c.Mutex.Lock()
		for i := 0; i < c.MapNum; i++ {
			task := <-c.MapChannel
			c.MapChannel <- task

			if task.TaskStatus == Ready {
				task.MapWorkerId = c.WorkerId
				c.WorkerId++
				task.TaskStatus = Running
				task.TaskBeginTime = time.Now()

				reply.Task = task
				reply.NReduce = c.ReduceNum

				c.Mutex.Unlock()
				return nil
			}
		}

		c.Mutex.Unlock()

		if c.IsAllMapDone() {
			// Mapf 任务全部完成, 开启 Reducef 任务
			c.GenerateReduceTasks()
			c.Mutex.Lock()
			c.DistributePhase = ReducePhase
			c.Mutex.Unlock()
		} else {
			time.Sleep(time.Second)
		}

		return nil

	} else {
		c.Mutex.Lock()
		for i := 0; i < c.ReduceNum; i++ {
			task := <-c.ReduceChannel
			c.ReduceChannel <- task
			if task.TaskStatus == Ready {
				task.ReduceWorkerId = c.WorkerId
				c.WorkerId++
				task.TaskStatus = Running
				task.TaskBeginTime = time.Now()

				// 初始化 reply
				reply.Task = task
				c.Mutex.Unlock()
				return nil
			}
		}

		c.Mutex.Unlock()

		// 所有 Mapf 和 Reducef 都完成了就直接返回
		if c.IsAllReduceDone() {
			reply.AllDone = true
			go c.FinishTasks()
		} else {
			time.Sleep(time.Second)
		}

		return nil
	}
}

// Map Task 完成后的回调
func (c *Coordinator) MapTaskDone(args *MapDoneArgs, reply *MapDoneReply) error {
	c.Mutex.Lock()

	for i := 0; i < c.MapNum; i++ {
		task := <-c.MapChannel
		c.MapChannel <- task
		// 处理超时任务
		c.ReMoveTimeoutTask(task)

		// 任务和 rpc.Call() 调用的 WorkerId 一致 且 任务状态满足完成条件
		if args.MapWorkerId == task.MapWorkerId && task.TaskStatus == Running {
			task.TaskStatus = Finished
			// 任务在规定时间内完成
			c.GenerateMapFile(args.Files)
			break
		}
	}
	c.Mutex.Unlock()

	return nil
}

// Reduce Task 完成后的回调
func (c *Coordinator) ReduceTaskDone(args *ReduceDoneArgs, reply *ReduceDoneReply) error {
	c.Mutex.Lock()

	for i := 0; i < c.ReduceNum; i++ {
		task := <-c.ReduceChannel
		c.ReduceChannel <- task

		c.ReMoveTimeoutTask(task)

		// 完成任务, 输出结果
		if args.ReduceWorkerId == task.ReduceWorkerId && task.TaskStatus == Running {
			task.TaskStatus = Finished
			c.GenerateReduceFile(args.Files)

			break
		}
	}
	c.Mutex.Unlock()

	return nil
}

// 遍历所有 Map 缓冲池中的任务, 判断所有任务是否完成
func (c *Coordinator) IsAllMapDone() bool {
	c.Mutex.Lock()
	for i := 0; i < c.MapNum; i++ {
		task := <-c.MapChannel
		c.MapChannel <- task
		if task.TaskStatus != Finished {
			c.Mutex.Unlock()
			return false
		}
	}

	c.Mutex.Unlock()
	return true
}

// 遍历所有 Reduce 缓冲池中的任务, 判断所有任务是否完成
func (c *Coordinator) IsAllReduceDone() bool {
	c.Mutex.Lock()
	for i := 0; i < c.ReduceNum; i++ {
		task := <-c.ReduceChannel
		c.ReduceChannel <- task
		if task.TaskStatus != Finished {
			c.Mutex.Unlock()
			return false
		}
	}
	c.Mutex.Unlock()
	return true
}

func (c *Coordinator) FinishTasks() {
	time.Sleep(time.Second * 3)
	c.Mutex.Lock()
	c.IsDone = true
	c.Mutex.Unlock()
}

// 生成中间文件
func (c *Coordinator) GenerateMapFile(files []string) {
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			log.Fatalln("GenerateMapFile(), os.Open(), err = ", err)
		}
		defer f.Close()

		curDirname, _ := os.Getwd()
		fileName := filepath.Base(file)
		filePath := curDirname + "/" + fileName

		distDirname, err := os.Create(filePath)
		if err != nil {
			log.Fatalln("GenerateMapFile(), os.Create(), err = ", err)
		}
		defer distDirname.Close()

		_, err = io.Copy(distDirname, f)
		if err != nil {
			log.Fatalln("GenerateMapFile(), io.Copy(), err = ", err)
		}
	}
}
func (c *Coordinator) GenerateReduceFile(files []string) {
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			log.Fatalln("GenerateReduceFile(), os.Open(), err = ", err)
		}
		defer f.Close()

		curDirname, _ := os.Getwd()
		fileName := filepath.Base(file)
		filePath := curDirname + "/" + fileName

		distDirname, err := os.Create(filePath)
		if err != nil {
			log.Fatalln("GenerateReduceFile(), os.Create(), err = ", err)
		}
		defer distDirname.Close()

		_, err = io.Copy(distDirname, f)
		if err != nil {
			log.Fatalln("GenerateReduceFile(), io.Copy(), err = ", err)
		}
	}
}
