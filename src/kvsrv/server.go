package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Result struct {
	LastRequestSeq uint64
	Value          string
}

type KVServer struct {
	mu sync.Mutex

	// TODO[]: Your definitions here.
	kvDB    map[string]string
	kvCache map[int64]*Result
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Get op just need to read current Value
	reply.Value = kv.kvDB[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Put op need to get current Value and refresh it to args.Value
	value, ok := kv.kvCache[args.ClientId]
	if ok {
		// avoid duplicate operation
		if value.LastRequestSeq >= args.RequestSeq {
			return
		}
	}

	kv.kvDB[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Append op need to update cache Value
	value, ok := kv.kvCache[args.ClientId]
	if ok {
		if value.LastRequestSeq >= args.RequestSeq {
			// do not need to append one more time, just return the old Value
			reply.Value = kv.kvCache[args.ClientId].Value
			return
		}
	}

	reply.Value = kv.kvDB[args.Key]
	kv.kvCache[args.ClientId] = &Result{
		LastRequestSeq: args.RequestSeq,
		Value:          kv.kvDB[args.Key],
	}

	kv.kvDB[args.Key] += args.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvDB = make(map[string]string)
	kv.kvCache = make(map[int64]*Result)
	return kv
}
