package shardmaster


import (
	"encoding/gob"
	"errors"
	"raft"
)
import "labrpc"
import "sync"
import "labgob"


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dataGuard	sync.RWMutex
	waitChMap map[int]chan interface{}
	waitChGuard sync.RWMutex
	configs []Config // indexed by config num

	clientSeqMap map[int]int
	clientSeqMapGuard sync.Mutex
}


type Op struct {
	// Your data here.
	Args interface{}
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	log := Op{
		Args:*args,
	}

	if _, err := sm.waitStartLog(log); err != nil {
		reply.WrongLeader = true
		reply.Err = Err(err.Error())
		return
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	log := Op{
		Args:*args,
	}

	if _, err := sm.waitStartLog(log); err != nil {
		reply.WrongLeader = true
		reply.Err = Err(err.Error())
		return
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	log := Op{
		Args:*args,
	}

	if _, err := sm.waitStartLog(log); err != nil {
		reply.WrongLeader = true
		reply.Err = Err(err.Error())
		return
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	log := Op{
		Args:*args,
	}

	if _, err := sm.waitStartLog(log); err != nil {
		reply.WrongLeader = true
		reply.Err = Err(err.Error())
		return
	}

	sm.dataGuard.Lock()
	defer sm.dataGuard.Unlock()

	if args.Num < 0 || args.Num >= len(sm.configs) {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else {
		reply.Config = sm.configs[args.Num]
	}

	raft.DPrintf("in query rpc, args.Num:%d, len:%d", args.Num, len(sm.configs))
}

func (sm *ShardMaster) waitStartLog(log Op) (int, error) {
	index, term, ok := sm.rf.Start(log)
	if !ok {
		return index, errors.New("not be leader")
	}

	ch := make(chan interface{})
	defer func() {
		sm.waitChGuard.Lock()
		defer sm.waitChGuard.Unlock()
		delete(sm.waitChMap, index)
	}()
	sm.waitChGuard.Lock()
	sm.waitChMap[index] = ch
	sm.waitChGuard.Unlock()

	<- ch

	if logEntry, err := sm.rf.GetLogEntry(index); err != nil {
		return index, err
	} else if term != logEntry.Term {
		return index, errors.New("任期过期")
	}

	return index, nil
}

func (sm *ShardMaster) makeNewCopyConfig() Config {
	lastConfig := sm.configs[len(sm.configs)-1]
	newConfig := Config{
		Num:len(sm.configs),
		Shards:lastConfig.Shards,
		Groups:make(map[int][]string),
	}
	if lastConfig.Groups != nil {
		for i,v := range lastConfig.Groups {
			newConfig.Groups[i] = v
		}
	}

	return newConfig
}

func (sm *ShardMaster) checkDuplicatedRequest(ident int, seq int) {
	sm.clientSeqMapGuard.Lock()
	defer sm.clientSeqMapGuard.Unlock()
	if lastSeq, ok := sm.clientSeqMap[ident]; !ok {
		sm.clientSeqMap[ident] = seq
	} else if seq <= lastSeq {
		panic("")
	} else {
		sm.clientSeqMap[ident] = seq
	}
}

func (sm *ShardMaster) applyUserLog(commandIndex int, userLog Op) {

	defer func() {
		// for checkDuplicatedRequest
		if err := recover(); err != nil {
			raft.DPrintf("panic err:%v", err)
		}
	}()

	sm.dataGuard.Lock()
	defer sm.dataGuard.Unlock()

	switch arg := userLog.Args.(type) {
	case JoinArgs:
		raft.DPrintf("apply JoinArgs op")
		sm.checkDuplicatedRequest(arg.Ident, arg.Seq)
		newConfig := sm.makeNewCopyConfig()
		for gid, servers := range arg.Servers {
			newConfig.Groups[gid] = servers
		}
		sm.configs = append(sm.configs, newConfig)
		raft.DPrintf("apply JoinArgs op success, len:%d", len(sm.configs))
		break
	case LeaveArgs:
		raft.DPrintf("apply LeaveArgs op")
		sm.checkDuplicatedRequest(arg.Ident, arg.Seq)
		newConfig := sm.makeNewCopyConfig()
		for _, gid := range arg.GIDs {
			delete(newConfig.Groups, gid)
		}
		sm.configs = append(sm.configs, newConfig)
		break
	case MoveArgs:
		raft.DPrintf("apply MoveArgs op")
		sm.checkDuplicatedRequest(arg.Ident, arg.Seq)
		newConfig := sm.makeNewCopyConfig()
		newConfig.Shards[arg.Shard] = arg.GID
		sm.configs = append(sm.configs, newConfig)
		break
	case QueryArgs:
		raft.DPrintf("apply QueryArgs op")
		sm.checkDuplicatedRequest(arg.Ident, arg.Seq)
		break
	default:
		raft.DPrintf("invalid op on apply")
	}
}

func (sm *ShardMaster) watchLogApply() {
	for msg := range sm.applyCh {
		if _, ok := msg.Command.(int); ok {
			continue
		}
		userLog, ok := msg.Command.(Op)
		if !ok {
			raft.DPrintf("invalid op, log:%v", userLog)
			continue
		}

		sm.applyUserLog(msg.CommandIndex, userLog)

		// notice
		func() {
			sm.waitChGuard.Lock()
			defer sm.waitChGuard.Unlock()

			ch := sm.waitChMap[msg.CommandIndex]
			if ch != nil {
				ch <- true
			}
		}()
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.waitChMap = make(map[int]chan interface{})
	sm.clientSeqMap = make(map[int]int)
	go sm.watchLogApply()

	return sm
}

func init() {
	gob.Register(JoinArgs{})
	gob.Register(LeaveArgs{})
	gob.Register(MoveArgs{})
	gob.Register(QueryArgs{})
}