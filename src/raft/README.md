# 20250228

BUG: 无法通过 Lab3C 的 Figure 8 测试，即返回冲突索引并不能使得 Leader 将冲突后的所有日志发送给 Follower 并且覆盖其后的所有日志。

具体表现：
```
Test (3C): Figure 8 ... FAIL
```

# 20250301

将 reply.ConflictIndex = index + 1 改为 index 修正了冲突索引的计算，使 Leader 能准确同步日志，从而通过测试。

在修改前，循环 index 指向最后一个匹配 `args.PrevLogTerm` 的日志条目，然后返回下一个位置（冲突点）。但是当 index + 1 超过了匹配范围，就会截断过多/遗漏日志，导致日志未正确对齐，在 `Test3CFigura8` 测试中一直尝试同步日志。

修改后，index 直接指向最后一个匹配 prevLogTerm 的位置，ConflictIndex 指示 Leader 从此处下一条开始同步。index + 1 多跳一步，导致冲突点偏移，修改后准确指向分叉处，修复一致性问题。

```go
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
        reply.ConflictTerm = -1
    } else {
        firstLogIndex := rf.getFirstLog().Index

        // 找到第一个 conflicting Term 的 Index
        index := args.PrevLogIndex
        for index >= firstLogIndex && rf.logs[index-firstLogIndex].Term == args.PrevLogTerm {
            index--
        }
        // reply.ConflictIndex = index + 1
        reply.ConflictIndex = index // Modify
        reply.ConflictTerm = args.PrevLogTerm
    }
    DPrintf("{Node %v} 的日志索引 %v 与 Leader 的日志索引 %v 发生冲突", rf.me, reply.ConflictIndex, args.PrevLogIndex)
    return
}
```

在修改后成功通过 `go test -run 3C`.

```bash
$ go test -run 3C
Test (3C): basic persistence ...
  ... Passed --   5.4  3  121   31167    7
Test (3C): more persistence ...
  ... Passed --  14.9  5 1014  193435   16
Test (3C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   1.4  3   38    9320    4
Test (3C): Figure 8 ...
  ... Passed --  30.2  5 1660  430557   73
Test (3C): unreliable agreement ...
  ... Passed --   1.6  5  353  121762  246
Test (3C): Figure 8 (unreliable) ...
  ... Passed --  32.8  5 3138 8331493  326
Test (3C): churn ...
  ... Passed --  16.1  5 5924 4375683 2457
Test (3C): unreliable churn ...
  ... Passed --  16.2  5 2911 1489912  959
PASS
ok      6.5840/raft     118.945s
```