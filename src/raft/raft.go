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
	"math/rand"
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
	Index int
	Term int
	Command interface{}
}

type PeerRaftState int

const (
	PeerRaftStateFollower PeerRaftState = iota+1
	PeerRaftStateLeader
	PeerRaftStateCandidate
)

const (
	LeaderAppendLogTimeoutMilliSeconds = 200
	DefaultElectionTimeoutMilliSeconds = 500
	InvalidPeerNodeIndex = -1
)

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
	currentTerm int
	votedFor int
	lastApplied int

	logs	[]LogEntry

	lastLogTerm int
	lastLogIndex int

	leaderIndex int
	raftState PeerRaftState

	electionCh chan interface{}
	appendLogCh chan interface{}

	voted	[]int
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
		DPrintf("[%d] is leader", rf.me)
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
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	oldTerm := rf.currentTerm

	defer func() {
		DPrintf("[%d] recv vote request from %d, term:%d, self old term:%d, " +
			"self end term:%d, self votedFor:%d, result:%t",
			rf.me, args.CandidateId, args.Term, oldTerm,
			rf.currentTerm, rf.votedFor, reply.VoteGranted)
	}()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.switchFollower()
		reply.VoteGranted = true
	} else if rf.votedFor != InvalidPeerNodeIndex {
		reply.VoteGranted = false
	} else if args.LastLogTerm < rf.lastLogTerm ||
		(args.LastLogTerm == rf.lastLogTerm && args.LastLogIndex < rf.lastLogIndex) {
		reply.VoteGranted = false
	} else {
		rf.currentTerm = args.Term
		rf.switchFollower()
		reply.VoteGranted = true
	}
}

type AppendLogRequest struct {
	Term int
}

type AppendLogReply struct {

}

func (rf *Raft) RequestAppendLog(args *AppendLogRequest, reply *AppendLogReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		return
	} else if args.Term == rf.currentTerm && rf.raftState == PeerRaftStateLeader {
		return
	} else if args.Term > rf.currentTerm {

	}

	rf.resetElectionTimer()
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
}

func (rf *Raft) switchToLeader() {
	rf.leaderIndex = rf.me
	rf.raftState = PeerRaftStateLeader
	rf.stopElectionTimer()
	rf.resetAppendLogRoutine()

	DPrintf("[%d] switch to leader", rf.me)
}

func (rf *Raft) switchFollower() {
	rf.raftState = PeerRaftStateFollower
	rf.resetElectionTimer()
}

func (rf* Raft) switchToCandidate() {
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voted = rf.voted[:0]
	rf.voted = append(rf.voted, rf.me)
	rf.raftState = PeerRaftStateCandidate
	rf.leaderIndex = InvalidPeerNodeIndex

	DPrintf("[%d] switch to candidate, term:%d", rf.me, rf.currentTerm)
}

func (rf *Raft) stopElectionTimer() {
	if rf.electionCh != nil {
		close(rf.electionCh)
		rf.electionCh = nil
	}
}

func (rf *Raft) resetElectionTimer() {
	rf.stopElectionTimer()
	rf.electionCh = make(chan interface{})

	go rf.startElectionTimer(rf.electionCh)
}

func (rf *Raft) startElectionTimer(electionCh <- chan interface{}) {
	timeoutSecond := DefaultElectionTimeoutMilliSeconds+rand.Intn(DefaultElectionTimeoutMilliSeconds)
	ticker := time.NewTimer(time.Millisecond * time.Duration(timeoutSecond))

	select {
	case <- electionCh:
		DPrintf("[%d] recv notify stop election", rf.me)
		break
	case <- ticker.C:
		rf.election()
		break
	}
}

func (rf *Raft) stopAppendLogRoutine() {
	if rf.appendLogCh != nil {
		close(rf.appendLogCh)
		rf.appendLogCh = nil
	}
}

func (rf *Raft) resetAppendLogRoutine() {
	rf.stopAppendLogRoutine()
	rf.appendLogCh = make(chan interface{})

	go rf.startAppendLogRoutine(rf.appendLogCh)
}

func (rf *Raft) startAppendLogRoutine(appendLogCh <- chan interface{}) {
	for {
		ticker := time.NewTimer(time.Millisecond * time.Duration(LeaderAppendLogTimeoutMilliSeconds))

		select {
		case <- appendLogCh:
			DPrintf("[%d] recv notify stop election", rf.me)
			return
		case <- ticker.C:
			break
		}

		go func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.raftState != PeerRaftStateLeader {
				return
			}
			peerLen := len(rf.peers)
			appendLogRequest := &AppendLogRequest{
				Term:rf.currentTerm,
			}

			for peerIndex := 0; peerIndex < peerLen; peerIndex++ {
				if peerIndex == rf.me {
					continue
				}

				go func(peerIndex int) {
					var reply AppendLogReply
					rf.sendAppendLog(peerIndex, appendLogRequest, &reply)
				}(peerIndex)
			}
		}()
	}
}

func (rf *Raft) getLastLogTermAndIndex() (int, int) {
	logLen := len(rf.logs)
	if logLen == 0 {
		return -1, -1
	} else {
		return rf.logs[logLen-1].Term, rf.logs[logLen-1].Index
	}
}

func (rf *Raft) isCandidate() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.raftState == PeerRaftStateCandidate
}

func (rf *Raft) handleVoteReply(serverIndex int, requestTerm int, reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if requestTerm != rf.currentTerm {
		return
	}

	if rf.raftState != PeerRaftStateCandidate {
		DPrintf("[%d] recv vote reply from %d, but self state not candidate", rf.me, serverIndex)
		return
	}

	if reply.VoteGranted {
		for _, i := range rf.voted {
			if i == serverIndex {
				return
			}
		}
		DPrintf("[%d] recv vote granted from %d", rf.me, serverIndex)
		rf.voted = append(rf.voted, serverIndex)
		if len(rf.voted) >= (len(rf.peers)/2+1) {
			rf.switchToLeader()
		}
	} else if reply.Term > rf.currentTerm {
		DPrintf("[%d] recv vote reject from %d", rf.me, serverIndex)
		rf.currentTerm = reply.Term
		rf.switchFollower()
	} else {
		DPrintf("[%d] recv other vote reply from %d, reply term:%d, self term:%d, granted:%t",
			rf.me, serverIndex, reply.Term, rf.currentTerm, reply.VoteGranted)
	}
}

func (rf *Raft) election() {
	DPrintf("[%d] start election", rf.me)

	var peerLen int
	voteRequest := &RequestVoteArgs{
		CandidateId:rf.me}

	preparedCheck := func() bool {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.raftState != PeerRaftStateFollower {
			return false
		}
		rf.switchToCandidate()
		peerLen = len(rf.peers)
		voteRequest.Term = rf.currentTerm
		voteRequest.LastLogTerm, voteRequest.LastLogIndex = rf.getLastLogTermAndIndex()

		return true
	}()

	if !preparedCheck {
		return
	}

	for peerIndex := 0; peerIndex < peerLen; peerIndex++ {
		if peerIndex == rf.me {
			continue
		}

		go func(peerIndex int) {
			var reply RequestVoteReply
			DPrintf("start send request to [%d]", peerIndex)
			rf.sendRequestVote(peerIndex, voteRequest, &reply)
			DPrintf("recv reply from [%d]", peerIndex)
			rf.handleVoteReply(peerIndex, voteRequest.Term, reply)
		}(peerIndex)
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

	rf.lastLogIndex = -1
	rf.lastLogTerm = -1
	rf.lastApplied = -1
	rf.leaderIndex = InvalidPeerNodeIndex
	rf.currentTerm = 0
	rf.votedFor = InvalidPeerNodeIndex
	rf.switchFollower()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
