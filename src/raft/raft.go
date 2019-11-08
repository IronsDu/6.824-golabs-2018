package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"labgob"
	"math/rand"
	"sort"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

type PeerRaftState int

const (
	PeerRaftStateFollower PeerRaftState = iota + 1
	PeerRaftStateLeader
	PeerRaftStateCandidate
)

const (
	LeaderAppendLogTimeoutMilliSeconds = 80
	DefaultElectionTimeoutMilliSeconds = 300
	InvalidPeerNodeIndex               = -1
)

type FollowerState struct {
	nextIndex  int
	matchIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm  int
	votedFor     int
	lastApplied  int
	leaderCommit int

	logs []LogEntry

	leaderIndex int
	raftState   PeerRaftState

	electionCh        chan interface{}
	electionSeq       int
	appendLogCh       chan interface{}
	shutdownCh        chan interface{}
	voted             []int
	lastAppendLogTime int64

	followerStates map[int]*FollowerState
	applyCh        chan ApplyMsg

	lastReplicateLogTime		int64
	electionCoroutineNum		int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.raftState == PeerRaftStateLeader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		_, _ = DPrintf("[%d] encode current term:%d failed:%s", rf.me, rf.currentTerm, err.Error())
		return
	}
	if err := e.Encode(rf.votedFor); err != nil {
		_, _ = DPrintf("[%d] encode votedFor:%d failed:%s", rf.me, rf.votedFor, err.Error())
		return
	}
	if err := e.Encode(rf.logs); err != nil {
		_, _ = DPrintf("[%d] encode log failed:%s", rf.me, err.Error())
		return
	}
	rf.persister.SaveRaftState(w.Bytes())

	lastLogTerm, lastLogIndex := rf.getLastLogTermAndIndex()
	_, _ = DPrintf("[%d] persist, current term:%d, votedFor:%d, " +
		"log len:%d, last log term:%d, last log index:%d",
		rf.me, rf.currentTerm, rf.votedFor, len(rf.logs), lastLogTerm, lastLogIndex)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var logs []LogEntry

	if err := d.Decode(&currentTerm); err != nil {
		_, _ = DPrintf("[%d] decode currentTerm failed:%s", rf.me, err.Error())
		return
	}
	if err := d.Decode(&votedFor); err != nil {
		_, _ = DPrintf("[%d] decode votedFor failed:%s", rf.me, err.Error())
		return
	}
	if err := d.Decode(&logs); err != nil {
		_, _ = DPrintf("[%d] decode logs failed:%s", rf.me, err.Error())
		return
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs = logs

	lastLogTerm, lastLogIndex := rf.getLastLogTermAndIndex()
	_, _ = DPrintf("[%d] read persist, current term:%d, votedFor:%d," +
		" log len:%d, last log term:%d, last log index:%d",
		rf.me, rf.currentTerm, rf.votedFor, len(rf.logs), lastLogTerm, lastLogIndex)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = args.Term

	// 检测任期
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		_, _ = DPrintf("[%d] failed handle RequestVote from:%d, current term:%d, request term:%d is smaller",
			rf.me, args.CandidateId, rf.currentTerm, args.Term)
		return
	} else if args.Term == rf.currentTerm {
		// 任期相同时则判断是否投票给了其他人
		if rf.votedFor != InvalidPeerNodeIndex &&
			rf.votedFor != args.CandidateId {
			reply.VoteGranted = false
			_, _ = DPrintf("[%d] failed handle RequestVote from:%d, but votedFor is:%d",
				rf.me, args.CandidateId, rf.votedFor)
			return
		}
	} else {
		rf.stepDown(args.Term)
		_, _ = DPrintf("[%d] handle RequestVote from:%d, term:%d is same, step down",
			rf.me, args.CandidateId, rf.currentTerm)
	}

	// 检测此候选者的日志跟自己比较是否足够新
	lastLogTerm, lastLogIndex := rf.getLastLogTermAndIndex()
	if args.LastLogTerm < lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		reply.VoteGranted = false
		_, _ = DPrintf("[%d] failed handle RequestVote from:%d, his term:%d, " +
			"but the candidate's log not update to date,"+
			" his last log term:%d, last log index:%d, self last log term:%d, last log index:%d",
			rf.me, args.CandidateId, args.Term, args.LastLogTerm, args.LastLogIndex, lastLogTerm, lastLogIndex)
		return
	}

	reply.VoteGranted = true
	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateId
	rf.persist()
	rf.switchFollower()

	_, _ = DPrintf("[%d] recv vote from:%d, his term:%d, self term:%d, vote granted",
		rf.me, args.CandidateId, args.Term, rf.currentTerm)
}

type AppendLogRequest struct {
	Term         int
	CandidateId  int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}

type AppendLogReply struct {
	Term    int
	Success bool
}

func (rf *Raft) getFollowerState(id int) *FollowerState {
	state, ok := rf.followerStates[id]
	if !ok {
		state = &FollowerState{
			nextIndex:  1,
			matchIndex: -1,
		}
		rf.followerStates[id] = state
	}

	return state
}

func (rf *Raft) initFollowerState() {
	_, lastLogIndex := rf.getLastLogTermAndIndex()
	rf.followerStates = make(map[int]*FollowerState)
	for peerIndex := 0; peerIndex < len(rf.peers); peerIndex++ {
		if peerIndex == rf.me {
			continue
		}

		rf.followerStates[peerIndex] = &FollowerState{
			nextIndex:  lastLogIndex + 1,
			matchIndex: -1,
		}
	}
}

func (rf *Raft) RequestAppendLog(args *AppendLogRequest, reply *AppendLogReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = args.Term
	reply.Success = false

	if args.Term < rf.currentTerm {
		_, _ = DPrintf("[%d] failed handle RequestAppendLog from [%d] by small term, "+
			"self term:%d, request term:%d, seq:%d",
			rf.me, args.CandidateId, rf.currentTerm, args.Term, rf.electionSeq)
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		_, _ = DPrintf("[%d] handle RequestAppendLog from [%d], update term from:%d to:%d",
			rf.me, args.CandidateId, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = InvalidPeerNodeIndex
		rf.persist()
	}
	rf.switchFollower()

	if args.PrevLogIndex > 0 && args.PrevLogTerm > 0 {
		_, lastLogIndex := rf.getLastLogTermAndIndex()
		// 检查本地日志是否缺失(此次收到的日志)之前的日志
		// 且验证之前日志的任期
		if args.PrevLogIndex > lastLogIndex ||
			args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {
			_, _ = DPrintf("[%d] failed handle RequestAppendLog recv invalid log," +
				" PrevLogIndex:%d, lastLogIndex:%d,"+
				"PrevLogTerm:%d",
				rf.me, args.PrevLogIndex, lastLogIndex, args.PrevLogTerm)
			return
		}
	}

	for _, entry := range args.Entries {
		if entry.Index < len(rf.logs) {
			rf.logs = rf.logs[:entry.Index]
		}
		rf.logs = append(rf.logs, entry)
	}

	reply.Success = true

	now := time.Now().UnixNano()
	_, _ = DPrintf("[%d] handle RequestAppendLog from [%d], "+
		"self term:%d, request term:%d, self:%p, seq:%d, wait:%d ms, now:%d",
		rf.me, args.CandidateId, rf.currentTerm, args.Term,
		rf, rf.electionSeq, (now-rf.lastAppendLogTime)/1000/1000, now)

	rf.leaderIndex = args.CandidateId
	if args.LeaderCommit >= rf.leaderCommit {
		rf.leaderCommit = args.LeaderCommit
	} else {
		_, _ = DPrintf("[%d] leader commit index error", rf.me)
	}
	rf.applyLog()
	rf.lastAppendLogTime = now
	if len(args.Entries) > 0 {
		rf.persist()
	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendLog(server int, args *AppendLogRequest, reply *AppendLogReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendLog", args, reply)
	return ok
}

//
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
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.raftState != PeerRaftStateLeader {
		return index, term, false
	}

	term, index = rf.appendCommand(command)
	rf.persist()
	_, _ = DPrintf("[%d] start %v, term:%d, index:%d", rf.me, command, rf.currentTerm, index)
	return index, term, isLeader
}

func (rf *Raft) appendCommand(command interface{}) (int, int) {
	_, lastLogIndex := rf.getLastLogTermAndIndex()
	lastLogIndex++

	rf.logs = append(rf.logs, LogEntry{
		Index:   lastLogIndex,
		Term:    rf.currentTerm,
		Command: command,
	})

	return rf.currentTerm, lastLogIndex
}
//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	close(rf.shutdownCh)
	_, _ = DPrintf("[%d] kill raft", rf.me)
}

func (rf *Raft) switchToLeader() {
	rf.leaderIndex = rf.me
	rf.raftState = PeerRaftStateLeader
	lastLogTerm, lastLogIndex := rf.getLastLogTermAndIndex()
	_, _ = DPrintf("[%d] switch to leader, current term:%d, lastLogTerm:%d, lastLogIndex:%d",
		rf.me, rf.currentTerm, lastLogTerm, lastLogIndex)
	rf.initFollowerState()
	rf.stopElectionTimer()
	rf.resetAppendLogRoutine()
	// 立即进行一次日志复制
	go func() {
		rf.replicatedLog(0)
	}()
}

func (rf *Raft) stepDown(newTerm int) {
	rf.currentTerm = newTerm
	rf.votedFor = InvalidPeerNodeIndex
	rf.persist()
	rf.raftState = PeerRaftStateFollower
	_, _ = DPrintf("[%d] step down to follower, seq:%d, current term:%d",
		rf.me, rf.electionSeq, rf.currentTerm)
	if rf.electionCh == nil {
		rf.resetElectionTimer()
	}
	rf.stopAppendLogRoutine()
}

func (rf *Raft) switchFollower() {
	rf.raftState = PeerRaftStateFollower
	_, _ = DPrintf("[%d] switch to follower, seq:%d, current term:%d",
		rf.me, rf.electionSeq, rf.currentTerm)
	rf.resetElectionTimer()
	rf.stopAppendLogRoutine()
}

func (rf *Raft) switchToCandidate() {
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voted = rf.voted[:0]
	rf.voted = append(rf.voted, rf.me)
	rf.raftState = PeerRaftStateCandidate
	rf.leaderIndex = InvalidPeerNodeIndex
	rf.persist()
	_, _ = DPrintf("[%d] switch to candidate, current term:%d", rf.me, rf.currentTerm)
}

func (rf *Raft) stopElectionTimer() {
	if rf.electionCh != nil {
		close(rf.electionCh)
		rf.electionCh = nil

		_, _ = DPrintf("[%d] stop election timer, it's seq:%d, current term:%d",
			rf.me, rf.electionSeq, rf.currentTerm)
	}
}

func (rf *Raft) resetElectionTimer() {
	rf.stopElectionTimer()
	rf.electionCh = make(chan interface{})
	rf.electionSeq++
	_, _ = DPrintf("[%d] resetElectionTimer, current seq:%d, term:%d", rf.me, rf.electionSeq, rf.currentTerm)
	electionCh, electionSeq, currentTerm := rf.electionCh, rf.electionSeq, rf.currentTerm
	go rf.startElectionTimer(electionCh, electionSeq, currentTerm, time.Now().UnixNano())
}

func (rf *Raft) startElectionTimer(electionCh <-chan interface{}, electionSeq int, term int, n int64) {
	timeoutSecond := DefaultElectionTimeoutMilliSeconds + rand.Intn(DefaultElectionTimeoutMilliSeconds/2)
	ticker := time.NewTimer(time.Millisecond * time.Duration(timeoutSecond))
	_, _ = DPrintf("[%d] startElectionTimer, self:%p, seq:%d, term:%d, n:%d", rf.me, rf, electionSeq, term, n)
	select {
	case <-electionCh:
		_, _ = DPrintf("[%d] recv notify stop election, seq:%d", rf.me, electionSeq)
		break
	case <-rf.shutdownCh:
		_, _ = DPrintf("[%d] recv kill, seq:%d", rf.me, electionSeq)
		break
	case <-ticker.C:
		_, _ = DPrintf("[%d] recv ticker, seq:%d", rf.me, electionSeq)
		rf.election(electionSeq, term, n)
		break
	}

	_, _ = DPrintf("[%d] startElectionTimer function ending, seq:%d, now:%d",
		rf.me, electionSeq, time.Now().UnixNano())
}

func (rf *Raft) stopAppendLogRoutine() {
	if rf.appendLogCh != nil {
		close(rf.appendLogCh)
		rf.appendLogCh = nil
	}
}

func (rf *Raft) resetAppendLogRoutine() {
	_, _ = DPrintf("[%d] resetAppendLogRoutine, term:%d", rf.me, rf.currentTerm)
	rf.stopAppendLogRoutine()
	rf.appendLogCh = make(chan interface{})

	appendLogCh := rf.appendLogCh
	go rf.startAppendLogRoutine(appendLogCh)
}

func (rf *Raft) replicatedLog(lastTime int64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	seq := rf.electionSeq
	if rf.raftState != PeerRaftStateLeader {
		return
	}

	now := time.Now().UnixNano()
	_, _ = DPrintf("[%d] replicatedLog, diff last time:%d ms, now:%d, cost:%d",
		rf.me, (now-rf.lastReplicateLogTime)/1000/1000, now, now-lastTime)
	rf.lastReplicateLogTime = now

	rf.resetAppendLogRoutine()

	peerLen := len(rf.peers)

	for peerIndex := 0; peerIndex < peerLen; peerIndex++ {
		if peerIndex == rf.me {
			continue
		}

		appendLogRequest := &AppendLogRequest{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			PrevLogIndex: -1,
			PrevLogTerm:  -1,
			LeaderCommit: rf.leaderCommit,
		}

		state := rf.getFollowerState(peerIndex)
		maxLogIndex := -1
		maxLogTerm := -1
		for i := state.nextIndex; i < len(rf.logs); i++ {
			entry := rf.logs[i]
			appendLogRequest.Entries = append(appendLogRequest.Entries, LogEntry{
				Index:   entry.Index,
				Term:    entry.Term,
				Command: entry.Command,
			})
			maxLogIndex = entry.Index
			maxLogTerm = entry.Term
		}

		prevLogIndex := state.nextIndex - 1
		if prevLogIndex < len(rf.logs) && prevLogIndex >= 0 {
			prevLog := rf.logs[prevLogIndex]
			appendLogRequest.PrevLogTerm = prevLog.Term
			appendLogRequest.PrevLogIndex = prevLog.Index
		}

		go func(peerIndex int) {
			var reply AppendLogReply
			now := time.Now().UnixNano()
			_, _ = DPrintf("[%d] start append log to [%d], term:%d, seq:%d, log size:%d, now:%d",
				rf.me, peerIndex, appendLogRequest.Term, seq, len(appendLogRequest.Entries), now)
			if rf.sendAppendLog(peerIndex, appendLogRequest, &reply) {
				rf.handleAppendLogReply(peerIndex, maxLogIndex, maxLogTerm, appendLogRequest.Term, reply)
				_, _ = DPrintf("[%d] recv append log reply from [%d], msg term:%d, seq:%d",
					rf.me, peerIndex, appendLogRequest.Term, seq)
			} else {
				_, _ = DPrintf("[%d] send append log to [%d] failed", rf.me, peerIndex)
			}
		}(peerIndex)
	}
}

func (rf *Raft) startAppendLogRoutine(appendLogCh <-chan interface{}) {
	ticker := time.NewTimer(time.Millisecond * time.Duration(LeaderAppendLogTimeoutMilliSeconds))
	now := time.Now().UnixNano()
	_, _ = DPrintf("[%d] startAppendLogRoutine, now:%d", rf.me, now)
	select {
	case <-appendLogCh:
		_, _ = DPrintf("[%d] recv notify stop append log", rf.me)
		return
	case <-rf.shutdownCh:
		_, _ = DPrintf("[%d] recv kill", rf.me)
		break
	case <-ticker.C:
		rf.replicatedLog(now)
		break
	}
}

func (rf *Raft) getLastLogTermAndIndex() (int, int) {
	logLen := len(rf.logs)
	if logLen == 0 {
		return -1, -1
	}

	lastLog := rf.logs[logLen-1]
	return lastLog.Term, lastLog.Index
}

func (rf *Raft) isCandidate() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.raftState == PeerRaftStateCandidate
}

func (rf *Raft) applyLog() {
	_, _ = DPrintf("[%d] apply log to state machine, leader commit:%d", rf.me, rf.leaderCommit)

	for rf.lastApplied < rf.leaderCommit && rf.lastApplied < len(rf.logs) {
		rf.lastApplied++
		// 应用日志到状态机
		log := rf.logs[rf.lastApplied]
		msg := ApplyMsg{
			CommandValid: true,
			Command:      log.Command,
			CommandIndex: log.Index,
		}
		rf.applyCh <- msg
	}
}

func (rf *Raft) handleAppendLogReply(peerIndex int,
	maxLogIndex int,
	maxLogTerm int,
	requestTerm int,
	reply AppendLogReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.raftState != PeerRaftStateLeader {
		_, _ = DPrintf("[%d] already not be leader", rf.me)
		return
	}

	if !reply.Success {
		_, _ = DPrintf("[%d] append log to [%d] failed, current term:%d, reply term:%d",
			rf.me, peerIndex, rf.currentTerm, reply.Term)
		if reply.Term > rf.currentTerm {
			rf.stepDown(reply.Term)
		} else {
			// 收到了过期的日志回复
			if requestTerm != rf.currentTerm {
				return
			}
			state := rf.getFollowerState(peerIndex)
			state.nextIndex = 1
			if state.nextIndex < 0 {
				state.nextIndex = 0
			}
		}
		return
	}
	// 收到了过期的日志回复
	if requestTerm != rf.currentTerm {
		return
	}
	// 表示复制的日志为空,则不必做后续处理
	if maxLogIndex == -1 && maxLogTerm == -1 {
		return
	}
	state := rf.getFollowerState(peerIndex)
	state.matchIndex = maxLogIndex
	state.nextIndex = maxLogIndex + 1
	_, _ = DPrintf("[%d] change peer[%d]'s match index to:%d, next index to:%d",
		rf.me, peerIndex, state.matchIndex, state.nextIndex)

	// 更新leaderCommit
	var matchIndexArr []int
	for _, state := range rf.followerStates {
		matchIndexArr = append(matchIndexArr, state.matchIndex)
	}
	sort.Ints(matchIndexArr)

	majorityCommitIndex := matchIndexArr[(len(rf.peers)-1)/2]
	_, _ = DPrintf("[%d] majorityCommitIndex is:%d, matchIndexArr is:%v, rf.leaderCommit :%d",
		rf.me, majorityCommitIndex, matchIndexArr, rf.leaderCommit)
	if majorityCommitIndex > rf.leaderCommit {
		termOfCommitIndex := rf.logs[majorityCommitIndex].Term
		if termOfCommitIndex == rf.currentTerm {
			rf.leaderCommit = majorityCommitIndex
			rf.applyLog()
		}
	}
}

func (rf *Raft) handleVoteReply(serverIndex int, requestTerm int, reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.VoteGranted {
		// 收到了过期的投票回复
		if requestTerm != rf.currentTerm {
			return
		}

		for _, i := range rf.voted {
			if i == serverIndex {
				return
			}
		}
		_, _ = DPrintf("[%d] recv vote granted from [%d]", rf.me, serverIndex)
		rf.voted = append(rf.voted, serverIndex)
		if len(rf.voted) >= (len(rf.peers)/2 + 1) {
			rf.switchToLeader()
		}
	} else if reply.Term > rf.currentTerm {
		_, _ = DPrintf("[%d] recv vote reject from [%d], reply term:%d, current term:%d, step down",
			rf.me, serverIndex, reply.Term, rf.currentTerm)
		rf.stepDown(reply.Term)
	} else {
		_, _ = DPrintf("[%d] recv other vote reply from [%d], reply term:%d, self term:%d, granted:%t",
			rf.me, serverIndex, reply.Term, rf.currentTerm, reply.VoteGranted)
	}
}

func (rf *Raft) election(electionSeq int, term int, n int64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	now := time.Now().UnixNano()
	_, _ = DPrintf("[%d] start election, term:%d, self:%p, seq:%d, cost:%d ms, now:%d, logs:%v",
		rf.me, rf.currentTerm, rf, electionSeq, (now-n)/1000/1000, now, rf.logs)

	if rf.raftState == PeerRaftStateLeader {
		_, _ = DPrintf("[%d] state is:%d when election check", rf.me, int(rf.raftState))
		return
	}
	if term != rf.currentTerm {
		_, _ = DPrintf("[%d] when election, current term %d not equal %d, seq:%d",
			rf.me, rf.currentTerm, term, electionSeq)
	}
	rf.switchToCandidate()
	rf.resetElectionTimer()

	voteRequest := RequestVoteArgs{
		CandidateId: rf.me}
	voteRequest.Term = rf.currentTerm
	voteRequest.LastLogTerm, voteRequest.LastLogIndex = rf.getLastLogTermAndIndex()

	for peerIndex := 0; peerIndex < len(rf.peers); peerIndex++ {
		if peerIndex == rf.me {
			continue
		}

		go func(peerIndex int, voteRequest RequestVoteArgs) {
			var reply RequestVoteReply
			_, _ = DPrintf("[%d] start send vote request to [%d], term:%d, seq:%d",
				rf.me, peerIndex, voteRequest.Term, electionSeq)
			if rf.sendRequestVote(peerIndex, &voteRequest, &reply) {
				_, _ = DPrintf("[%d] recv vote reply from [%d], term:%d, seq:%d",
					rf.me, peerIndex, voteRequest.Term, electionSeq)
				rf.handleVoteReply(peerIndex, voteRequest.Term, reply)
			}
		}(peerIndex, voteRequest)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.leaderIndex = InvalidPeerNodeIndex
	rf.currentTerm = 0
	rf.votedFor = InvalidPeerNodeIndex
	rf.shutdownCh = make(chan interface{})
	rf.electionSeq = 0
	rf.lastAppendLogTime = 0
	rf.lastReplicateLogTime = 0
	rf.applyCh = applyCh
	rf.electionCoroutineNum = 0
	rf.leaderCommit = 0
	rf.lastApplied = 0

	// 初始化时在队列首部添加一个哑日志
	rf.appendCommand(0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.switchFollower()

	return rf
}
