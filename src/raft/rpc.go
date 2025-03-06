package raft

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

func (rf *Raft) genRequestVoteArgs() *RequestVoteArgs {
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLog().Index,
		LastLogTerm:  rf.getLastLog().Term,
	}
	return args
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v} 处理完当前投票请求后的状态: {state %v, term %v}",
		rf.me, rf.state, rf.currentTerm)

	// Reply false if term < currentTerm(§5.1)
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.ChangeState(Follower)
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}

	// (§5.4.1) 新 Leader 的日志要确保包含所有已提交的日志条目
	if !rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	rf.votedFor = args.CandidateId
	rf.persist()
	rf.electionTimer.Reset(RandomElectionTimeout())
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int // Leader 认为 Follower 下一次应该接收的日志索引的前一条日志索引
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) genAppendEntriesArgs(prevLogIndex int) *AppendEntriesArgs {
	firstLogIndex := rf.getFirstLog().Index
	entries := make([]LogEntry, len(rf.logs[prevLogIndex-firstLogIndex+1:]))
	copy(entries, rf.logs[prevLogIndex-firstLogIndex+1:])
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.logs[prevLogIndex-firstLogIndex].Term,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}
	return args
}

// AppendEntries RPC Handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("在处理完日志附加RPC后 {Node %v} 的状态是 {state %v, term %v}}", rf.me, rf.state, rf.currentTerm)

	// Reply false if term < currentTerm(§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 表示当前节点是 Leader
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	rf.ChangeState(Follower)
	rf.electionTimer.Reset(RandomElectionTimeout())

	// 如果日志在 prevLogIndex 处不包含任期与 prevLogTerm 匹配的条目，
	// 返回 false (§5.3)
	if args.PrevLogIndex < rf.getFirstLog().Index {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// Lab3C
	// 检查日志是否匹配，若不匹配，返回冲突的索引和任期
	// 如果现有条目与新条目冲突（相同索引但不同任期），
	// 删除该条目及其后续所有条目 (§5.3)
	if !rf.isLogMatched(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Term = rf.currentTerm
		reply.Success = false
		lastLogIndex := rf.getLastLog().Index
		// find the first index of the conflicting term
		if lastLogIndex < args.PrevLogIndex {
			// 最后一项日志的 index 小于 prevLogIndex，
			// 那么 ConfilictIndex 就是 peer 的最后一项日志的 index
			reply.ConflictIndex = lastLogIndex + 1
			reply.ConflictTerm = -1 // TODO
		} else {
			firstLogIndex := rf.getFirstLog().Index

			// 找到第一个 conflicting Term 的 Index
			index := args.PrevLogIndex
			for index >= firstLogIndex && rf.logs[index-firstLogIndex].Term == args.PrevLogTerm {
				index--
			}
			reply.ConflictIndex = index
			reply.ConflictTerm = args.PrevLogTerm
		}
		DPrintf("{Node %v} 的日志索引 %v 与 Leader 的日志索引 %v 发生冲突", rf.me, reply.ConflictIndex, args.PrevLogIndex)
		return
	}

	// 添加新日志
	firstLogIndex := rf.getFirstLog().Index
	DPrintf("{Node %v} 追加日志前的日志内容为 %v", rf.me, rf.logs)
	for index, entry := range args.Entries {
		// 追加新日志并截断不一致的部分
		if entry.Index-firstLogIndex >= len(rf.logs) || rf.logs[entry.Index-firstLogIndex].Term != entry.Term {
			rf.logs = shrinkEntries(append(rf.logs[:entry.Index-firstLogIndex], args.Entries[index:]...))
			rf.persist()
			break
		}
	}
	DPrintf("{Node %v} 追加日志后的日志内容为 %v", rf.me, rf.logs)

	// 如果 leaderCommit > commitIndex，更新 commitIndex
	// commitIndex = min(leaderCommit, index of last new entry)
	newCommitIndex := Min(args.LeaderCommit, rf.getLastLog().Index)
	if newCommitIndex > rf.commitIndex {
		DPrintf("{Node %v} 在 Term %v 将 CommitIndex 更新 %v -> %v, 同时 LeaderIndex 是 %v", rf.me, rf.currentTerm, rf.commitIndex, newCommitIndex, args.LeaderCommit)
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()
	}
	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	// unused fields
	// Offset int	// byte offset where chunk is positioned in the snapshot file
	// Done   bool	// true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) genInstallSnapshotArgs() *InstallSnapshotArgs {
	firstLog := rf.getFirstLog()
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: firstLog.Index,
		LastIncludedTerm:  firstLog.Term,
		Data:              rf.persister.ReadSnapshot(),
	}
	return args
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	// reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}
	rf.ChangeState(Follower)
	rf.electionTimer.Reset(RandomElectionTimeout())

	// check the snapshot is more up-to-date than the current log
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
