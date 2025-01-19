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
	"log"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Command interface{}
	Index   int
	Term    int
}

type Identity int

const (
	Follower  Identity = 0
	Candidate Identity = 1
	Leader    Identity = 2
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//
	// TODO Lab3A
	//

	identity Identity
	applyCh  chan ApplyMsg

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []Entry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// Election relative state
	electionVotesNum  int
	isElectionRunning bool
	nextExpireTime    time.Time
}

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("%v 节点 收到来自 Candidate %v 的选票请求，当前支持的是 %v", rf.me, args.CandidateId, rf.votedFor)

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.identity = Follower
		rf.votedFor = -1
	}

	size := len(rf.log)
	lastLogIndex := rf.log[size-1].Index
	lastLogTerm := rf.log[size-1].Term

	upToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if upToDate {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.nextExpireTime = rf.getNextExpireTime()
			return
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.nextExpireTime = rf.getNextExpireTime()

	if rf.isElectionRunning {
		rf.mu.Unlock()
		return
	}
	rf.isElectionRunning = true
	rf.identity = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.electionVotesNum = 1

	size := len(rf.log)
	lastLogIndex := rf.log[size-1].Index
	lastLogTerm := rf.log[size-1].Term

	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		log.Printf("Candidate %v 节点向 %v 节点发送选票请求，当前 Term：%v", rf.me, i, rf.currentTerm)

		// Parallelism send RequestVote RPCs
		go func(server int) {

			rf.mu.Lock()
			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := &RequestVoteReply{}
			rf.mu.Unlock()

			if ok := rf.sendRequestVote(server, args, reply); ok {

				log.Printf("Candidate %v 节点收到 %v 的回复，投票结果是 %v", rf.me, server, reply.VoteGranted)

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					rf.identity = Follower
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.isElectionRunning = false
					rf.nextExpireTime = rf.getNextExpireTime()

					return
				}

				if reply.VoteGranted {
					rf.electionVotesNum += 1
				}

			}
		}(i)
	}

	for rf.killed() == false {

		rf.mu.Lock()
		votes := rf.electionVotesNum
		identity := rf.identity

		// election result 1: get the majority votes and become Leader
		if votes > len(rf.peers)/2 {

			// rf.me win the election, now its identity convert to Leader
			log.Printf("%v 赢得了选举, 作为 Leader 的任期 Term 是 %v", rf.me, rf.currentTerm)

			rf.isElectionRunning = false
			rf.mu.Unlock()

			rf.finishElection()
			return
		}

		// election result 2: someone become Leader
		if identity == Follower {
			rf.isElectionRunning = false
			rf.mu.Unlock()
			return
		}

		// election result 3: election phase timeout, restart the election
		if time.Now().After(rf.nextExpireTime) {
			rf.isElectionRunning = false
			rf.mu.Unlock()

			rf.startElection()
			return
		}
		rf.mu.Unlock()

		time.Sleep(1 * time.Millisecond)
	}

}

// win the election and convert to Leader
func (rf *Raft) finishElection() {

	rf.mu.Lock()

	rf.identity = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = len(rf.log)
	}

	rf.mu.Unlock()

	go rf.sendHeartBeat()
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictLogEntryTerm      int
	FirstMatchedLogEntryIndex int
	FollowerLogLength         int
}

// AppendEntries RPC Handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("%v 节点 收到来自 Leader %v 的附加日志请求", rf.me, args.LeaderId)

	if rf.identity == Candidate {
		log.Printf("%v 节点从 Candiadate 变成了 Follower", rf.me)
	}
	rf.identity = Follower
	rf.nextExpireTime = rf.getNextExpireTime()

	log.Printf("%v 节点因为 心跳/附加日志 重置了自己的心跳", rf.me)

	reply.Term = rf.currentTerm
	reply.Success = false

	// implementation 1: if term < currentTerm, let Reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}

	// implementation 2: if rf.log exclude prev-*(Term and Index),
	// 					 let Reply.success = false
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictLogEntryTerm = rf.log[args.PrevLogIndex].Term
		for i := 0; i <= args.PrevLogIndex; i++ {
			if rf.log[i].Term == reply.ConflictLogEntryTerm {
				reply.FirstMatchedLogEntryIndex = rf.log[i].Index
				break
			}
		}
		reply.FollowerLogLength = len(rf.log)
		return
	}

	// implementation 3: if an existing entry conflicts with a new one
	// 					 (same index but different terms)
	// delete the existing entry
	if args.Entries != nil {
		for i, newEntry := range args.Entries {

			// if there find any entry which is not the same as newEntry,
			// 		delete the whole entry behind the newEntry.Index
			if len(rf.log) > newEntry.Index && rf.log[newEntry.Index].Term != newEntry.Term {
				rf.log = rf.log[:newEntry.Index]
			}

			// implementation 4: any new entries worth appending to rf.log
			// if newEntry.Index is longer than current log.Size, append it
			if len(rf.log) <= newEntry.Index {
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
		}
	}

	// implementation 5: if leaderCommit > commitIndex, update
	if args.LeaderCommit > rf.commitIndex {
		lastIndex := len(rf.log) - 1
		rf.commitIndex = min(args.LeaderCommit, rf.log[lastIndex].Index)
	}

	reply.Success = true
	return
}

// Leader
func (rf *Raft) sendHeartBeat() {
	count := 1
	identity := Leader

	for ; identity == Leader && !rf.killed(); count++ {

		rf.mu.Lock()

		log.Printf("Leader %v 第 %v 次 发送心跳", rf.me, count)

		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			args := rf.initAppendEntriesArgs(i)
			go rf.doneAppendEntriesReply(i, args)
		}

		identity = rf.identity

		rf.mu.Unlock()

		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) initAppendEntriesArgs(serverId int) *AppendEntriesArgs {

	var sendEntries []Entry

	prevLogIndex := rf.nextIndex[serverId] - 1
	var prevLogTerm int

	if prevLogIndex <= 0 {
		prevLogIndex, prevLogTerm = 0, -1
	} else {
		prevLogTerm = rf.log[prevLogIndex].Term
	}

	// if lastLogIndex >= rf.nextIndex[i]
	// leader need to send the log entry[nextIndex:]
	if rf.nextIndex[serverId] <= len(rf.log) {
		sendEntries = rf.log[prevLogIndex:]
	} else {
		sendEntries = nil
	}

	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      sendEntries,
		LeaderCommit: rf.commitIndex,
	}

	return args
}

func (rf *Raft) doneAppendEntriesReply(serverId int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}

	if ok := rf.sendAppendEntries(serverId, args, reply); ok {

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.currentTerm < reply.Term {
			log.Printf("%v 节点从 Leader 身份变成了 Follower", rf.me)
			rf.identity = Follower
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.nextExpireTime = rf.getNextExpireTime()
			return
		}

		// reply.Success = True
		if reply.Success {
			rf.matchIndex[serverId] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[serverId] = rf.matchIndex[serverId] + 1

			for i := rf.log[len(rf.log)-1].Index; i >= rf.matchIndex[serverId] && i > rf.commitIndex; i-- {
				count := 0
				for j := range rf.peers {
					if rf.matchIndex[j] > i {
						count++
					}
					if count > (len(rf.peers)/2) && rf.log[i].Term == rf.currentTerm {
						rf.commitIndex = i
						break
					}
				}
			}
			return

		} else { // reply.Success = False

			if (reply.FollowerLogLength - 1) < args.PrevLogTerm {
				if reply.FollowerLogLength != -1 {
					rf.nextIndex[serverId] = reply.FollowerLogLength
				}
				return

			}

			lastMatchTerm := -1
			for i := len(rf.log) - 1; i >= 0; i-- {
				if rf.log[i].Term == reply.ConflictLogEntryTerm {
					lastMatchTerm = rf.log[i].Index
					break
				}
			}

			if lastMatchTerm == -1 {
				rf.nextIndex[serverId] = reply.FirstMatchedLogEntryIndex
			} else {
				rf.nextIndex[serverId] = lastMatchTerm
			}
		}
	}

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool = false
	// Your code here (3A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if rf.identity == Leader {
		isLeader = true
	}

	return term, isLeader
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
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm

	size := len(rf.log)
	lastEntry := rf.log[size-1]
	if term != lastEntry.Term {
		index = 1
	} else {
		index = lastEntry.Index + 1
	}

	entry := Entry{
		Command: command,
		Index:   index,
		Term:    term,
	}

	rf.log = append(rf.log, entry)

	if rf.identity != Leader {
		isLeader = false
	}

	return index, term, isLeader
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
		rf.mu.Lock()

		if time.Now().After(rf.nextExpireTime) && rf.identity != Leader && !rf.isElectionRunning {

			log.Printf("%v 节点超时，开始新一轮的选举，任期 Term = %v", rf.me, rf.currentTerm)

			rf.mu.Unlock()
			go rf.startElection()

			ms := 50 + (rand.Int63() % 300)
			time.Sleep(time.Duration(ms) * time.Millisecond)

			continue
		}

		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) getNextExpireTime() time.Time {
	millisecond := 150 + rand.Intn(150)
	return time.Now().Add(time.Duration(millisecond) * time.Millisecond)
}

func (rf *Raft) applier() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.log[rf.lastApplied].Index,
			}
		}
		rf.mu.Unlock()
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.identity = Follower
	rf.isElectionRunning = false

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.electionVotesNum = 0
	rf.log = []Entry{{nil, -1, 0}}

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.nextExpireTime = rf.getNextExpireTime()

	// start applier goroutine to commit entry to applyCh
	go rf.applier()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
