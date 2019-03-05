package kvraft

import (
	"time"

	"github.com/oopDaniel/COEN317-Raft-KVService-go_server/kv/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd

	id         int64
	lastLeader int // last known leader
	ReqSeqId   int // unique serial numbers to every command for linearizability
}

const RequestTimeout = time.Duration(150 * time.Millisecond)

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers

	ck.id = NumRand()
	ck.lastLeader = 0
	ck.ReqSeqId = 0

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.ReqSeqId++
	serverCount := len(ck.servers)
	args := GetArgs{
		Key:      key,
		ClientId: ck.id,
		ReqSeqId: ck.ReqSeqId,
	}

	for {
		var reply GetReply
		if ck.servers[ck.lastLeader].Call("KVServer.Get", &args, &reply) && !reply.WrongLeader {
			if reply.Res == OK {
				return reply.Value
			}
			return ""
		}

		ck.lastLeader = (ck.lastLeader + 1) % serverCount
		time.Sleep(RequestTimeout)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.ReqSeqId++
	serverCount := len(ck.servers)
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.id,
		ReqSeqId: ck.ReqSeqId,
	}

	for {
		var reply PutAppendReply
		if ck.servers[ck.lastLeader].Call("KVServer.PutAppend", &args, &reply) && !reply.WrongLeader && reply.Res == OK {
			return
		}
		ck.lastLeader = (ck.lastLeader + 1) % serverCount
		time.Sleep(RequestTimeout)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
