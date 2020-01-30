package raftkv

import (
	"labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

var CurrentId = 0
var IdGuard sync.Mutex

func takeClerkId() int {
	var id = 0
	IdGuard.Lock()
	CurrentId++
	id = CurrentId
	IdGuard.Unlock()
	return id
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader *labrpc.ClientEnd
	seq int
	seqGuard sync.Mutex
	id int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.seq = 0
	ck.id = takeClerkId()
	return ck
}

func (ck *Clerk) takeSeq() int {
	ck.seqGuard.Lock()
	defer ck.seqGuard.Unlock()
	ck.seq++
	return ck.seq
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

	// You will have to modify this function.
	args := &GetArgs{
		Key:key,
		Seq:ck.takeSeq(),
		Ident:ck.id,
	}

	if ck.leader != nil {
		var reply GetReply
		if ck.leader.Call("KVServer.Get", args, &reply) && !reply.WrongLeader {
			return reply.Value
		}
		ck.leader  = nil
	}

	for {
		for _, peer := range ck.servers {
			var reply GetReply
			if peer.Call("KVServer.Get", args, &reply) && !reply.WrongLeader {
				ck.leader = peer
				return reply.Value
			}
		}
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
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key:key,
		Value:value,
		Op:op,
		Seq:ck.takeSeq(),
		Ident:ck.id,
	}

	if ck.leader != nil {
		var reply PutAppendReply
		if ck.leader.Call("KVServer.PutAppend", args, &reply) && !reply.WrongLeader {
			return
		}
		ck.leader  = nil
	}

	for {
		for _, peer := range ck.servers {
			var reply PutAppendReply
			if peer.Call("KVServer.PutAppend", args, &reply) && !reply.WrongLeader {
				ck.leader = peer
				return
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
