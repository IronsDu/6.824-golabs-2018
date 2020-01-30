package raftkv

import (
	"encoding/gob"
	"errors"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func DFatal(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Fatalf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kv	map[string]string
	kvGuard sync.RWMutex
	waitChMap map[int]chan interface{}
	waitChGuard sync.RWMutex

	clientSeqMap map[int]int
	clientSeqMapGuard sync.Mutex
}

type KVCommandType int
const (
	KvGet	KVCommandType = iota
	KvPutAppend
)

type KVLogStructure struct {
	Command	KVCommandType
	Args interface{}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	log := KVLogStructure{
		Command:KvGet,
		Args:*args,
	}

	var logIndex int
	if index, err := kv.waitStartLog(log); err != nil {
		reply.WrongLeader = true
		reply.Err = Err(err.Error())
		return
	} else {
		logIndex = index
	}

	kv.kvGuard.Lock()
	defer kv.kvGuard.Unlock()

	if v, ok := kv.kv[args.Key]; ok {
		reply.Value = v
		_, _ = DPrintf("KVServer Raft[%d] log index:%d, Get key:%s, value:%s\n",
			kv.rf.GetMe(), logIndex, args.Key, v)
	}
}

func (kv *KVServer) waitStartLog(log KVLogStructure) (int, error) {
	index, term, ok := kv.rf.Start(log)
	if !ok {
		return index, errors.New("not be leader")
	}

	ch := make(chan interface{})
	defer func() {
		kv.waitChGuard.Lock()
		defer kv.waitChGuard.Unlock()
		delete(kv.waitChMap, index)
	}()
	kv.waitChGuard.Lock()
	kv.waitChMap[index] = ch
	kv.waitChGuard.Unlock()

	<- ch

	if logEntry, err := kv.rf.GetLogEntry(index); err != nil {
		return index, err
	} else if term != logEntry.Term {
		return index, errors.New("任期过期")
	}

	return index, nil
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	log := KVLogStructure{
		Command:KvPutAppend,
		Args:*args,
	}

	if logIndex, err := kv.waitStartLog(log); err != nil {
		reply.WrongLeader = true
		reply.Err = Err(err.Error())
		return
	} else {
		_, _ = DPrintf("KVServer Raft[%d] log index:%d, PutAppend args:%v \n", kv.rf.GetMe(), logIndex, args)
	}
}

func (kv *KVServer) applyPutAppendCommand(args PutAppendArgs) string {
	switch args.Op {
	case "Put":
		kv.kv[args.Key] = args.Value
		return args.Value
	case "Append":
		kv.clientSeqMapGuard.Lock()
		defer kv.clientSeqMapGuard.Unlock()
		if seq, ok := kv.clientSeqMap[args.Ident]; !ok {
			kv.clientSeqMap[args.Ident] = args.Seq
		} else if args.Seq <= seq {
			_, _ = DPrintf("don't process this request \n")
			return ""
		} else {
			kv.clientSeqMap[args.Ident] = args.Seq
		}
		if v, ok := kv.kv[args.Key]; ok {
			v += args.Value
			kv.kv[args.Key] = v
			return v
		} else {
			kv.kv[args.Key] = args.Value
			return args.Value
		}
	default:
		_, _ = DFatal("invalid op:%s \n", args.Op)
	}

	return ""
}

func (kv *KVServer) applyUserLog(commandIndex int, userLog KVLogStructure) {
	switch userLog.Command {
	case KvGet:
		break
	case KvPutAppend:
		args := userLog.Args.(PutAppendArgs)
		lastValue := kv.applyPutAppendCommand(args)
		_, _ = DPrintf("raft:%d apply, Put[%s] by [%s], current value:%s , log index[%d]\n",
			kv.rf.GetMe(), args.Key, args.Value, lastValue, commandIndex)
		break
	default:
		_, _ = DFatal("invalid command of:%d \n", userLog.Command)
	}
}

func (kv *KVServer) watchLogApply() {
	for msg := range kv.applyCh {
		if _, ok := msg.Command.(int); ok {
			continue
		}
		userLog, ok := msg.Command.(KVLogStructure)
		if !ok {
			_,_ = DFatal("not be kv log structure")
			continue
		}

		kv.applyUserLog(msg.CommandIndex, userLog)

		// notice
		func() {
			kv.waitChGuard.Lock()
			defer kv.waitChGuard.Unlock()

			ch := kv.waitChMap[msg.CommandIndex]
			if ch != nil {
				ch <- true
			}
		}()
	}
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

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kv = make(map[string]string)
	kv.waitChMap = make(map[int]chan interface{})
	kv.clientSeqMap = make(map[int]int)

	// You may need initialization code here.

	go kv.watchLogApply()

	return kv
}

func init() {
	gob.Register(KVLogStructure{})
	gob.Register(PutAppendArgs{})
	gob.Register(GetArgs{})
}