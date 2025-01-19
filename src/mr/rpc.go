package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type TaskType int        // 任务类型
type TaskStatus int      // 任务进行阶段
type DistributePhase int // 任务分配阶段

const (
	// Define Task type
	Map    TaskType = 0
	Reduce TaskType = 1

	// Define DistributePhase
	MapPhase    DistributePhase = 0
	ReducePhase DistributePhase = 1

	TempFilePath = "/var/tmp/"

	TaskExpiredTime = 10

	Ready    TaskStatus = 0
	Running  TaskStatus = 1
	Finished TaskStatus = 2
)

// **********************************************

// Add your RPC definitions here.

///
/// Task
///

type TaskArgs struct {
}

type TaskReply struct {
	Task    *Task // 分配的任务
	NReduce int   // Reduce 任务数量
	AllDone bool  // Coordinator 中的任务是否全部完成
}

///
/// Mapper and Reducer
///

type MapDoneArgs struct {
	MapWorkerId int
	Files       []string
}

type MapDoneReply struct {
}

type ReduceDoneArgs struct {
	ReduceWorkerId int
	Files          []string
}

type ReduceDoneReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
