package raftkv

import (
	"labgob"
	"labrpc"
	"raft"
	"sync"
	"time"
)

type Operation int

const ApplyTimeout = time.Duration(2 * time.Second)
const (
	GET Operation = iota
	PUT
	APPEND
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	OpName   Operation // "GET", "PUT" or "APPEND"
	ClientId int64     // Client id
	ReqSeqId int       // Unique sequential numbers of request
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	data           map[string]string // Key/value pairs to store in the KV service
	duplicateMap   map[int64]int     // (ClientId:ReqId). Prevent applying duplicate state to service
	requestHandler map[int]chan bool // Channel to notice client that request has been processed
}

// RPC handler for Get
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		OpName:   GET,
		Key:      args.Key,
		ClientId: args.ClientId,
		ReqSeqId: args.ReqSeqId,
	}

	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(op)
	kv.mu.Unlock()

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	// Blocking call
	ok := kv.waitApplying(index, term)

	if !ok {
		reply.WrongLeader = true
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if value, ok := kv.data[args.Key]; ok {
		reply.Value = value
		reply.Res = OK
	} else {
		reply.Res = ErrNoKey
	}
}

// RPC handler for Put/Append
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		ReqSeqId: args.ReqSeqId,
	}

	if args.Op == "Put" {
		op.OpName = PUT
	} else {
		op.OpName = APPEND
	}

	kv.mu.Lock()
	index, term, isLeader := kv.rf.Start(op)
	kv.mu.Unlock()

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	// Blocking call
	ok := kv.waitApplying(index, term)

	if !ok {
		reply.WrongLeader = true
		return
	}

	reply.Res = OK
}

// Wait until Raft committed the entry and kv server applied the state locally
func (kv *KVServer) waitApplying(index int, term int) bool {
	resCh := make(chan bool, 1)
	kv.mu.Lock()
	kv.requestHandler[index] = resCh
	kv.mu.Unlock()

	// Block until the request processed, or timeout
	select {
	case <-time.After(ApplyTimeout):
		kv.mu.Lock()
		delete(kv.requestHandler, index)
		kv.mu.Unlock()
		return false
	case <-resCh:
	}

	curTerm, isLeader := kv.rf.GetState()

	// Not a leader or wrong term
	if !isLeader || term != curTerm {
		return false
	}
	return true
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.data = make(map[string]string)
	kv.duplicateMap = make(map[int64]int)
	kv.requestHandler = make(map[int]chan bool)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applyStateDaemon()

	return kv
}

func (kv *KVServer) applyStateDaemon() {
	for {
		msg := <-kv.applyCh

		if msg.CommandValid {
			op := msg.Command.(Op) // Syntax of type conversion

			kv.mu.Lock()

			// Apply the command if it's NOT a duplicate
			if reqSeqId, ok := kv.duplicateMap[op.ClientId]; !ok || reqSeqId < op.ReqSeqId {
				// Update sequential number with the latest
				kv.duplicateMap[op.ClientId] = op.ReqSeqId

				// Update kv state
				switch op.OpName {
				case PUT:
					kv.data[op.Key] = op.Value
				case APPEND:
					kv.data[op.Key] += op.Value
				default:
				}
			}

			// Notify through the channel to reply to the client
			if notifyCh, ok := kv.requestHandler[msg.CommandIndex]; ok && notifyCh != nil {
				close(notifyCh)
				delete(kv.requestHandler, msg.CommandIndex)
			}

			kv.mu.Unlock()

		}
	}
}
