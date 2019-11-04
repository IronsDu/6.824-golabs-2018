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

	totalRecvAppendLogRequest []AppendLogRequest
	lastReplicateLogTime      int64
	electionCoroutineNum	int
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

	if isleader {
		DPrintf("[%d] is leader, term:%d, raft state:%d, self:%p",
			rf.me, rf.currentTerm, int(rf.raftState), rf)
	}
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
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	rf.persister.SaveRaftState(w.Bytes())
	DPrintf("[%d] persist, [%d, %d, len:%d]", rf.me, rf.currentTerm, rf.votedFor, len(rf.logs))
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
		DPrintf("[%d] readPersist currentTerm failed of:%s", rf.me, err.Error())
		return
	}
	if err := d.Decode(&votedFor); err != nil {
		DPrintf("[%d] readPersist votedFor failed of:%s", rf.me, err.Error())
		return
	}
	if err := d.Decode(&logs); err != nil {
		DPrintf("[%d] readPersist logs failed of:%s", rf.me, err.Error())
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs = logs

	DPrintf("[%d] readPersist, [%d, %d, len:%d]", rf.me, rf.currentTerm, rf.votedFor, len(rf.logs))
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

	defer func(oldTerm int) {
		DPrintf("[%d] handle vote request from [%d], term:%d, self old term:%d, "+
			"self end term:%d, self votedFor:%d, result:%t",
			rf.me, args.CandidateId, args.Term, oldTerm,
			rf.currentTerm, rf.votedFor, reply.VoteGranted)
	}(rf.currentTerm)

	lastLogTerm, lastLogIndex := rf.getLastLogTermAndIndex()
	DPrintf("[%d] recv vote request from [%d], args:%v, rf:[%d, %d]",
		rf.me, args.CandidateId, args, lastLogIndex, lastLogTerm)

	// 检测任期
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("[%d] recv vote request, request term is smaller , %d < %d",
			rf.me, args.Term, rf.currentTerm)
		return
	} else if args.Term > rf.currentTerm {
		rf.votedFor = InvalidPeerNodeIndex
		rf.currentTerm = args.Term
		rf.persist()
		rf.switchFollower(true)
	} else if args.Term == rf.currentTerm {
		// 任期相同时则判断是否投票给了其他人
		if rf.votedFor != InvalidPeerNodeIndex &&
			rf.votedFor != args.CandidateId {
			reply.VoteGranted = false
			DPrintf("[%d] recv vote request, votedFor:[%d] not none", rf.me, rf.votedFor)
			return
		}
	}

	// 检测此候选者的日志跟自己比较是否足够新
	if args.LastLogTerm < lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		reply.VoteGranted = false
		DPrintf("[%d] recv vote request, but log not update to date,"+
			" args:%v, self:[%d, %d], rf:%v", rf.me, args, lastLogIndex, lastLogTerm, rf)
		return
	}

	reply.VoteGranted = true
	DPrintf("[%d] recv vote, granted true, set term %d to %d, self:%p",
		rf.me, rf.currentTerm, args.Term, rf)
	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateId
	rf.persist()
	rf.switchFollower(false)
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
	s, ok := rf.followerStates[id]
	if !ok {
		s = &FollowerState{
			nextIndex:  1,
			matchIndex: -1,
		}
		rf.followerStates[id] = s
	}

	return s
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

	rf.totalRecvAppendLogRequest = append(rf.totalRecvAppendLogRequest, *args)
	defer func() {
		if !reply.Success {
			DPrintf("[%d] append log from [%d] reply failed", rf.me, args.CandidateId)
		}
	}()
	reply.Success = false

	if args.Term < rf.currentTerm {
		DPrintf("[%d] error handle RequestAppendLog from [%d] by small term, "+
			"self term:%d, request term:%d, self:%p, seq:%d",
			rf.me, args.CandidateId, rf.currentTerm, args.Term, rf, rf.electionSeq)
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.votedFor = InvalidPeerNodeIndex
		rf.currentTerm = args.Term
		rf.persist()
	}
	rf.switchFollower(false)

	if args.PrevLogIndex > 0 && args.PrevLogTerm > 0 {
		_, lastLogIndex := rf.getLastLogTermAndIndex()
		// 检查本地日志是否缺失(此次收到的日志)之前的日志
		// 且验证之前日志的任期
		if args.PrevLogIndex > lastLogIndex ||
			args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {
			DPrintf("[%d] handle RequestAppendLog recv invalid log, PrevLogIndex:%d, lastLogIndex:%d,"+
				"PrevLogTerm:%d, self log:%v",
				rf.me, args.PrevLogIndex, lastLogIndex, args.PrevLogTerm, rf.logs)
			return
		}
	}

	for _, entry := range args.Entries {
		if entry.Index < len(rf.logs) {
			DPrintf("[%d] log conflict, %v between %v, ready delete log after this entry",
				rf.me, rf.logs[entry.Index], entry)
			rf.logs = rf.logs[:entry.Index]
		}
		DPrintf("[%d] append log:%v", rf.me, entry)
		rf.logs = append(rf.logs, entry)
	}

	reply.Success = true

	now := time.Now().UnixNano()
	DPrintf("[%d] handle RequestAppendLog from [%d], "+
		"self term:%d, request term:%d, self:%p, seq:%d, wait:%d ms, now:%d, current logs:%v",
		rf.me, args.CandidateId, rf.currentTerm, args.Term,
		rf, rf.electionSeq, (now-rf.lastAppendLogTime)/1000/1000, now, rf.logs)

	rf.leaderIndex = args.CandidateId
	if args.LeaderCommit >= rf.leaderCommit {
		rf.leaderCommit = args.LeaderCommit
	} else {
		DPrintf("[%d] leader commit index error", rf.me)
	}
	rf.applyLog()
	rf.lastAppendLogTime = now
	DPrintf("[%d] set term from %d to %d", rf.me, rf.currentTerm, args.Term)
	if rf.currentTerm < args.Term || len(args.Entries) > 0 {
		if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term
			rf.votedFor = InvalidPeerNodeIndex
		}
		rf.persist()
	}
	rf.switchFollower(false)
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

	_, index = rf.getLastLogTermAndIndex()
	index++
	term = rf.currentTerm

	rf.logs = append(rf.logs, LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	})
	rf.persist()
	DPrintf("[%d] start %v, current logs:%v", rf.me, command, rf.logs)
	return index, term, isLeader
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
	DPrintf("[%d] kill raft", rf.me)
}

func (rf *Raft) switchToLeader() {
	rf.leaderIndex = rf.me
	rf.raftState = PeerRaftStateLeader
	DPrintf("[%d] switch to leader, term:%d, self:%p, logs:%v", rf.me, rf.currentTerm, rf, rf.logs)
	rf.initFollowerState()
	rf.stopElectionTimer()
	rf.resetAppendLogRoutine()
	// 立即进行一次日志复制
	go func() {
		rf.replicatedLog(0)
	}()
}

func (rf *Raft) switchFollower(stepDown bool) {
	rf.raftState = PeerRaftStateFollower
	DPrintf("[%d] switch to follower, self:%p, seq:%d, term:%d, stepDown:%v", rf.me, rf, rf.electionSeq, rf.currentTerm, stepDown)
	if !stepDown || rf.electionCh == nil {
		rf.resetElectionTimer()
	}
	rf.stopAppendLogRoutine()
}

func (rf *Raft) switchToCandidate() {
	DPrintf("[%d] set term %d to %d, self:%p, in switchToCandidate",
		rf.me, rf.currentTerm, rf.currentTerm+1, rf)
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voted = rf.voted[:0]
	rf.voted = append(rf.voted, rf.me)
	rf.raftState = PeerRaftStateCandidate
	rf.leaderIndex = InvalidPeerNodeIndex
	rf.persist()
	DPrintf("[%d] switch to candidate, term:%d, self:%p", rf.me, rf.currentTerm, rf)
}

func (rf *Raft) stopElectionTimer() {
	if rf.electionCh != nil {
		DPrintf("[%d] stop election timer, seq:%d", rf.me, rf.electionSeq)
		close(rf.electionCh)
		rf.electionCh = nil
	}
}

func (rf *Raft) resetElectionTimer() {
	rf.stopElectionTimer()
	rf.electionCh = make(chan interface{})
	rf.electionSeq++
	DPrintf("[%d] resetElectionTimer, self:%p, seq:%d", rf.me, rf, rf.electionSeq)
	n := time.Now().UnixNano()
	go rf.startElectionTimer(rf.electionCh, rf.electionSeq, rf.currentTerm, n)
}

func (rf *Raft) startElectionTimer(electionCh <-chan interface{}, electionSeq int, term int, n int64) {
	timeoutSecond := DefaultElectionTimeoutMilliSeconds + rand.Intn(DefaultElectionTimeoutMilliSeconds/2)
	ticker := time.NewTimer(time.Millisecond * time.Duration(timeoutSecond))
	DPrintf("[%d] startElectionTimer, self:%p, seq:%d, term:%d, n:%d", rf.me, rf, electionSeq, term, n)
	select {
	case <-electionCh:
		DPrintf("[%d] recv notify stop election, seq:%d", rf.me, electionSeq)
		break
	case <-rf.shutdownCh:
		DPrintf("[%d] recv kill, seq:%d", rf.me, electionSeq)
		break
	case <-ticker.C:
		DPrintf("[%d] recv ticker, seq:%d", rf.me, electionSeq)
		rf.election(electionSeq, term, n)
		break
	}

	DPrintf("[%d] startElectionTimer function ending, seq:%d, now:%d", rf.me, electionSeq, time.Now().UnixNano())
}

func (rf *Raft) stopAppendLogRoutine() {
	if rf.appendLogCh != nil {
		close(rf.appendLogCh)
		rf.appendLogCh = nil
	}
}

func (rf *Raft) resetAppendLogRoutine() {
	DPrintf("[%d] resetAppendLogRoutine, term:%d", rf.me, rf.currentTerm)
	rf.stopAppendLogRoutine()
	rf.appendLogCh = make(chan interface{})

	go rf.startAppendLogRoutine(rf.appendLogCh)
}

func (rf *Raft) replicatedLog(lastTime int64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	seq := rf.electionSeq
	if rf.raftState != PeerRaftStateLeader {
		return
	}

	now := time.Now().UnixNano()
	DPrintf("[%d] replicatedLog, self logs:%v, diff last time:%d ms, now:%d, cost:%d",
		rf.me, rf.logs, (now-rf.lastReplicateLogTime)/1000/1000, now, now-lastTime)
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
			DPrintf("[%d] start append log to [%d], term:%d, seq:%d, log size:%d, now:%d",
				rf.me, peerIndex, appendLogRequest.Term, seq, len(appendLogRequest.Entries), now)
			if rf.sendAppendLog(peerIndex, appendLogRequest, &reply) {
				rf.handleAppendLogReply(peerIndex, maxLogIndex, maxLogTerm, appendLogRequest, reply)
				DPrintf("[%d] recv append log reply from [%d], msg term:%d, seq:%d",
					rf.me, peerIndex, appendLogRequest.Term, seq)
			} else {
				DPrintf("[%d] send append log to [%d] failed", rf.me, peerIndex)
			}
		}(peerIndex)
	}
}

func (rf *Raft) startAppendLogRoutine(appendLogCh <-chan interface{}) {
	ticker := time.NewTimer(time.Millisecond * time.Duration(LeaderAppendLogTimeoutMilliSeconds))
	now := time.Now().UnixNano()
	DPrintf("[%d] startAppendLogRoutine, now:%d", rf.me, now)
	select {
	case <-appendLogCh:
		DPrintf("[%d] recv notify stop append log", rf.me)
		return
	case <-rf.shutdownCh:
		DPrintf("[%d] recv kill", rf.me)
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
	DPrintf("[%d] apply log to state machine, leader commit:%d", rf.me, rf.leaderCommit)

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
	request *AppendLogRequest,
	reply AppendLogReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.raftState != PeerRaftStateLeader {
		DPrintf("[%d] already not be leader", rf.me)
		return
	}
	// 收到了过期的回复
	if rf.currentTerm != request.Term {
		return
	}

	if !reply.Success {
		DPrintf("[%d] append log to [%d] failed", rf.me, peerIndex)
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = InvalidPeerNodeIndex
			rf.persist()
			rf.switchFollower(true)
		} else {
			state := rf.getFollowerState(peerIndex)
			state.nextIndex = 1
			if state.nextIndex < 0 {
				state.nextIndex = 0
			}
		}
		return
	}

	// 表示复制的日志为空,则不必做后续处理
	if maxLogIndex == -1 && maxLogTerm == -1 {
		return
	}
	state := rf.getFollowerState(peerIndex)
	state.matchIndex = maxLogIndex
	state.nextIndex = maxLogIndex + 1
	DPrintf("[%d] change peer[%d]'s match index to:%d, next index to:%d",
		rf.me, peerIndex, state.matchIndex, state.nextIndex)

	// 更新leaderCommit
	var matchIndexArr []int
	for _, state := range rf.followerStates {
		matchIndexArr = append(matchIndexArr, state.matchIndex)
	}
	sort.Ints(matchIndexArr)

	majorityCommitIndex := matchIndexArr[(len(rf.peers)-1)/2]
	DPrintf("[%d] majorityCommitIndex is:%d, matchIndexArr is:%v, rf.leaderCommit :%d",
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

	// 收到了过期的投票
	if requestTerm != rf.currentTerm {
		return
	}
	if rf.raftState != PeerRaftStateCandidate {
		DPrintf("[%d] recv vote reply from [%d], but self state not candidate", rf.me, serverIndex)
		return
	}

	if reply.VoteGranted {
		for _, i := range rf.voted {
			if i == serverIndex {
				return
			}
		}
		DPrintf("[%d] recv vote granted from [%d]", rf.me, serverIndex)
		rf.voted = append(rf.voted, serverIndex)
		if len(rf.voted) >= (len(rf.peers)/2 + 1) {
			rf.switchToLeader()
		}
	} else if reply.Term > rf.currentTerm {
		DPrintf("[%d] recv vote reject from [%d]", rf.me, serverIndex)
		DPrintf("[%d] set term %d to %d, self:%p", rf.me, rf.currentTerm, reply.Term, rf)
		rf.currentTerm = reply.Term
		rf.votedFor = InvalidPeerNodeIndex
		rf.persist()
		rf.switchFollower(true)
	} else {
		DPrintf("[%d] recv other vote reply from [%d], reply term:%d, self term:%d, granted:%t",
			rf.me, serverIndex, reply.Term, rf.currentTerm, reply.VoteGranted)
	}
}

func (rf *Raft) election(electionSeq int, term int, n int64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	now := time.Now().UnixNano()
	DPrintf("[%d] start election, term:%d, self:%p, seq:%d, cost:%d ms, now:%d, logs:%v",
		rf.me, rf.currentTerm, rf, electionSeq, (now-n)/1000/1000, now, rf.logs)

	if rf.raftState == PeerRaftStateLeader {
		DPrintf("[%d] state is:%d when election check", rf.me, int(rf.raftState))
		return
	}
	if term != rf.currentTerm {
		DPrintf("[%d] exit election, current term %d not equal %d, seq:%d",
			rf.me, rf.currentTerm, term, electionSeq)
		//return
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
			DPrintf("[%d] start send vote request to [%d], term:%d, seq:%d",
				rf.me, peerIndex, voteRequest.Term, electionSeq)
			if rf.sendRequestVote(peerIndex, &voteRequest, &reply) {
				DPrintf("[%d] recv vote reply from [%d], term:%d, seq:%d",
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

	// 初始化时在队列首部添加一个哑日志
	{
		rf.leaderCommit = 0
		rf.lastApplied = 0
		_, lastLogIndex := rf.getLastLogTermAndIndex()
		lastLogIndex++
		rf.logs = append(rf.logs, LogEntry{
			Index:   lastLogIndex,
			Term:    0,
			Command: 0,
		})
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	DPrintf("[%d] Make rf:%p", rf.me, rf)
	rf.switchFollower(false)

	return rf
}
