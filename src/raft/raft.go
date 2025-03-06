package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"bytes"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

type NodeState int

const (
	Follower  NodeState = 0
	Candidate NodeState = 1
	Leader    NodeState = 2
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 所有节点(Leader, Candidate, Follower)上的非易失状态
	currentTerm int        // 最新的可见任期
	votedFor    int        // 当前任期发生的选举中支持的候选人 id
	logs        []LogEntry // 日志条目，包括 State Machine 要执行和命令和接受条目 Leader 的任期

	// 所有节点(Leader, Candidate, Follower)上的易失状态
	commitIndex int // 已知已提交的最高日志条目的索引（初始化为0，单调递增）
	lastApplied int // 已知已应用到状态机的最高日志索引（初始化为0，单调递增）

	// Leader 节点上的易失状态，在选举结束后重新初始化
	nextIndex  []int // Leader 认为 Follower 下次应该接收的日志索引。
	matchIndex []int // Follower 已成功复制的最新日志索引。

	// 其余辅助属性
	state          NodeState     // 当前节点的身份
	electionTimer  *time.Timer   // 选举计时器
	heartbeatTimer *time.Timer   // 心跳计时器
	applyCh        chan ApplyMsg // channel to send apply msg to service
	applyCond      *sync.Cond    // condition variable for apply goroutine
	replicatorCond []*sync.Cond  // condition variable for replicator goroutine
}

func (rf *Raft) ChangeState(state NodeState) {
	if rf.state == state {
		return
	}
	DPrintf("{Node %v} 从 %v 状态变成 %v 状态", rf.me, rf.state, state)
	rf.state = state
	switch state {
	case Follower:
		rf.electionTimer.Reset(RandomElectionTimeout())
		rf.heartbeatTimer.Stop()
	case Candidate:
	case Leader:
		rf.electionTimer.Stop()
		rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) GetId() int {
	return rf.me
}

// Example:
// w := new(bytes.Buffer)
// e := labgob.NewEncoder(w)
// e.Encode(rf.xxx)
// e.Encode(rf.yyy)
// raftstate := w.Bytes()
// rf.persister.Save(raftstate, nil)
func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	return w.Bytes()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	rf.persister.Save(rf.encodeState(), nil)
}

// restore previously persisted state.
// Example:
// r := bytes.NewBuffer(data)
// d := labgob.NewDecoder(r)
// var xxx
// var yyy
//
//	if d.Decode(&xxx) != nil || d.Decode(&yyy) != nil {
//		  error...
//	} else {
//
//		  rf.xxx = xxx
//		  rf.yyy = yyy
//	}
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	// Your code here (3C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		DPrintf("{Node %v} 无法 Decode 持久化状态", rf.me)
	}
	rf.currentTerm, rf.votedFor, rf.logs = currentTerm, votedFor, logs
	rf.lastApplied, rf.commitIndex = rf.getFirstLog().Index, rf.getFirstLog().Index
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	snapshotIndex := rf.getFirstLog().Index
	if index <= snapshotIndex || index > rf.getLastLog().Index {
		DPrintf("{Node %v} 在 Term %v 拒绝用快照 index %v 替换当前的更新的快照 snapshotIndex %v", rf.me, rf.currentTerm, index, snapshotIndex)
		return
	}
	rf.logs = shrinkEntries(rf.logs[index-snapshotIndex:])
	rf.logs[0].Command = nil
	rf.persister.Save(rf.encodeState(), snapshot)
	DPrintf("{Node %v} 接受了序号为 %v 的快照后，状态是{state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v}", rf.me, index, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog())
}

// 选举
func (rf *Raft) StartElection() {
	rf.votedFor = rf.me
	rf.persist()
	args := rf.genRequestVoteArgs()
	grantedVotes := 1
	DPrintf("{Node %v} 启动选举，选票参数 RequestVoteArgs %v", rf.me, args)
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := new(RequestVoteReply)
			if rf.sendRequestVote(peer, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("(Candidate){Node %v} 从 {Node %v} 接收到选票响应 RequestVoteReply %v", rf.me, peer, reply)
				if args.Term == rf.currentTerm && rf.state == Candidate {
					if reply.VoteGranted {
						grantedVotes += 1
						// check over half of the votes
						if grantedVotes > len(rf.peers)/2 {
							DPrintf("(Leader){Node %v} 接收到来自多数节点的投票", rf.me)
							rf.ChangeState(Leader)
							rf.BoardcastHeartbeat(true)
						}
					} else if reply.Term > rf.currentTerm {
						rf.ChangeState(Follower)
						rf.currentTerm, rf.votedFor = reply.Term, -1
						rf.persist()
					}
				}
			}
		}(peer)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}

	// 首先为自己新增日志条目
	newLogIndex := rf.getLastLog().Index + 1
	rf.logs = append(rf.logs, LogEntry{
		Term:    rf.currentTerm,
		Command: command,
		Index:   newLogIndex,
	})
	rf.persist()
	rf.matchIndex[rf.me] = newLogIndex
	rf.nextIndex[rf.me] = newLogIndex + 1
	DPrintf("(Leader){Node %v} 在 Term %v 开始通过 LogEntry 封装命令 {Command %v}", rf.me, rf.currentTerm, command)
	// 接下来向对等节点广播日志条目
	rf.BoardcastHeartbeat(false)
	return newLogIndex, rf.currentTerm, true
}

// Leader 广播心跳/日志附加
func (rf *Raft) BoardcastHeartbeat(isHeartbeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartbeat {
			// 立即发送给所有的 peer(对等) 节点
			go rf.replicateOnceRound(peer)
		} else {
			// 通知 replicator 将日志发送给 peer(对等) 节点
			rf.replicatorCond[peer].Signal()
		}
	}
}

// §5.4.1 选举限制
func (rf *Raft) isLogUpToDate(index, term int) bool {
	lastLog := rf.getLastLog()
	// 比较两份日志最后的条目的索引值和任期号定义更新的日志 §5.4.1
	return term > lastLog.Term || (term == lastLog.Term && index >= lastLog.Index)
}

// 检查指定索引的日志条目任期是否与 Raft 节点日志匹配。
func (rf *Raft) isLogMatched(index, term int) bool {
	// index <= rf.getLastLog() 确保索引不超过日志末尾，否则不存在该条目。
	// index-rf.getFirstLog().Index 将全局索引映射到数组的相对位置。
	return index <= rf.getLastLog().Index && term == rf.logs[index-rf.getFirstLog().Index].Term
}

func (rf *Raft) advanceCommitIndexForLeader() {
	n := len(rf.matchIndex)
	sortMatchIndex := make([]int, n)
	copy(sortMatchIndex, rf.matchIndex)
	sort.Ints(sortMatchIndex)
	// 计算大多数节点已知的最大日志索引
	newCommitIndex := sortMatchIndex[n-(n/2+1)]
	if newCommitIndex > rf.commitIndex {
		if rf.isLogMatched(newCommitIndex, rf.currentTerm) {
			DPrintf("(Leader){Node %v} 在 Term %v 更新提交索引 %v -> %v", rf.me, rf.currentTerm, rf.commitIndex, newCommitIndex)
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()
		}
	}
}

// Leader 同步日志的尝试
// replicateOnceRound 负责将日志条目或快照发送给指定的 Follower 节点。
// 如果当前节点不是 Leader，则直接返回。
// 如果 Follower 节点的日志落后于 Leader 的日志，Leader 会发送快照；
// 否则，Leader 会发送日志条目。
// 该函数会根据 Follower 的响应更新 Leader 的状态和索引信息。
func (rf *Raft) replicateOnceRound(peer int) {
	rf.mu.RLock()
	if rf.state != Leader {
		rf.mu.RUnlock()
		return
	}
	// Leader 认为 Follower 下一次应该接收的日志索引的前一条日志索引
	prevLogIndex := rf.nextIndex[peer] - 1

	// 如果 prevLogIndex 小于当前日志的起点，说明 Follower 太过落后
	// Leader 的日志已经被压缩为快照(Snapshot)
	if prevLogIndex < rf.getFirstLog().Index {
		// 分支1: 发送快照
		args := rf.genInstallSnapshotArgs()
		rf.mu.RUnlock()
		reply := new(InstallSnapshotReply)
		if rf.sendInstallSnapshot(peer, args, reply) {
			rf.mu.Lock()
			if rf.state == Leader && rf.currentTerm == args.Term {
				if reply.Term > rf.currentTerm {
					rf.ChangeState(Follower)
					rf.currentTerm, rf.votedFor = reply.Term, -1
					rf.persist()
				} else {
					rf.nextIndex[peer] = args.LastIncludedIndex + 1
					rf.matchIndex[peer] = args.LastIncludedIndex
				}
			}
			rf.mu.Unlock()
			DPrintf("{Node %v} 向 {Node %v} 发送快照 RPC", rf.me, peer)
		}
	} else {
		// 分支2: 发送日志条目
		args := rf.genAppendEntriesArgs(prevLogIndex)
		rf.mu.RUnlock()
		reply := new(AppendEntriesReply)
		if rf.sendAppendEntries(peer, args, reply) {
			rf.mu.Lock()
			if args.Term == rf.currentTerm && rf.state == Leader {
				if !reply.Success {
					// Follower 任期更高，Leader 退化为 Follower
					if reply.Term > rf.currentTerm {
						rf.ChangeState(Follower)
						rf.currentTerm, rf.votedFor = reply.Term, -1
						rf.persist()
					} else if reply.Term == rf.currentTerm {
						// 递减 nextIndex 并且重试
						rf.nextIndex[peer] = reply.ConflictIndex
						// TODO: optimize the nextIndex finding, maybe use binary search
						if reply.ConflictTerm != -1 {
							firstLogIndex := rf.getFirstLog().Index
							for index := args.PrevLogIndex - 1; index >= firstLogIndex; index-- {
								if rf.logs[index-firstLogIndex].Term == reply.ConflictTerm {
									rf.nextIndex[peer] = index
									break
								}
							}
						}
					}
				} else {
					rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[peer] = rf.matchIndex[peer] + 1
					// 尽可能的提高 CommitIndex
					rf.advanceCommitIndexForLeader()
				}
			}
			rf.mu.Unlock()
			DPrintf("{Node %v} 发送 AppendEntries Args %v 给 {Node %v} 并且得到回复 AppendEntries Reply %v", rf.me, args, peer, reply)
		}
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == Leader && rf.matchIndex[peer] < rf.getLastLog().Index
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for rf.killed() == false {
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		// 给对等节点发送日志
		rf.replicateOnceRound(peer)
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		select {
		// 选举超时，当前节点变成 Candidate，执行 StartElection()
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			rf.ChangeState(Candidate)
			rf.currentTerm += 1
			rf.persist()
			// Start election
			rf.StartElection()
			rf.electionTimer.Reset(RandomElectionTimeout())
			rf.mu.Unlock()
		// 心跳超时，如果是 Leader，广播心跳重置跟随者心跳 BoardcastHeartbeat(true)
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.BoardcastHeartbeat(true)
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		// 检查 commitIndex 是否最新
		for rf.commitIndex <= rf.lastApplied {
			// Wait 释放锁并等待更新 commitIndex 后，获取锁继续执行
			rf.applyCond.Wait()
		}

		// 将日志项的 Command 应用到状态机上
		firstLogIndex, commitIndex, lastApplied := rf.getFirstLog().Index, rf.commitIndex, rf.lastApplied
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.logs[lastApplied-firstLogIndex+1:commitIndex-firstLogIndex+1])
		rf.mu.Unlock()

		// 将 应用消息(Apply Msg) 通过 applyCh 发送给状态机复制
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
				CommandTerm:  entry.Term,
			}
		}
		rf.mu.Lock()
		DPrintf("{Node %v} 在 Term %v 对状态机应用了索引范围为 [%v : %v] 的日志条目", rf.me, rf.currentTerm, lastApplied+1, commitIndex)
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

func (rf *Raft) HasLogInCurrentTerm() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.getLastLog().Term == rf.currentTerm
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(
	peers []*labrpc.ClientEnd,
	me int,
	persister *Persister,
	applyCh chan ApplyMsg,
) *Raft {
	rf := &Raft{
		mu:             sync.RWMutex{},
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		currentTerm:    0,
		votedFor:       -1,
		logs:           make([]LogEntry, 1),
		commitIndex:    0,
		lastApplied:    0,
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		state:          Follower,
		electionTimer:  time.NewTimer(RandomElectionTimeout()),
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),
		applyCh:        applyCh,
		replicatorCond: make([]*sync.Cond, len(peers)),
	}

	// (如果崩溃了)从上一次崩溃前持久化的状态初始化
	rf.readPersist(persister.ReadRaftState())
	// 使用 mutex 来保护 applyCond，避免其他 goroutine 改变临界区
	rf.applyCond = sync.NewCond(&rf.mu)
	// 初始化 nextIndex and matchIndex, 同时启动 replicator goroutine
	for peer := range peers {
		rf.matchIndex[peer], rf.nextIndex[peer] = 0, rf.getLastLog().Index+1
		if peer != rf.me {
			rf.replicatorCond[peer] = sync.NewCond(&sync.Mutex{})
			// 启动 replicator goroutine 向 peer 节点发送日志
			go rf.replicator(peer)
		}
	}
	// 启动 ticker goroutine 来发起选举
	go rf.ticker()
	// 启动 applier goroutine 来将日志应用到 状态机
	go rf.applier()
	return rf
}
