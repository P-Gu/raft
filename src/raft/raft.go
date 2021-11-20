package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"kvstore/labgob"
	"kvstore/labrpc"
)

type LogEntry struct {
	term int
	command interface{}
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


type AppendEntriesArgs struct {
	Term         int;
	LeadId       int;
	PrevLogIndex int;
	PrevLogTerm  LogEntry;
	Entries      [] LogEntry;
	LeaderCommit int;
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}
//
// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	//// For 2D:
	//SnapshotValid bool
	//Snapshot      []byte
	//SnapshotTerm  int
	//SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// persistent state
	currentTerm int
	votedFor int
	logs []LogEntry
	// volatile state on all servers
	commitIndex int
	lastApplied int
	// volatile state on leaders
	nextIndex []int
	matchIndex []int

	// other
	state  string// "leader" "candidate" "follower"
	applyCh         chan ApplyMsg
	electionTimeout time.Time // last event time
	heartbeatTimeout time.Duration // the time interval to send heartbeats
	voteCnt         int
	majorityCnt int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) getState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == "leader"{
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil || e.Encode(rf.votedFor) != nil || e.Encode(rf.logs) != nil {
		panic("failed to persist")
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var tmpCurrentTerm, tmpVotedFor int
	var tmpLogs []LogEntry
	if d.Decode(&tmpCurrentTerm) != nil ||
	   d.Decode(&tmpVotedFor) != nil || d.Decode(&tmpLogs) != nil{
	  panic("failed to readPersist")
	} else {
	  rf.currentTerm = tmpCurrentTerm
	  rf.votedFor = tmpVotedFor
	  rf.logs = tmpLogs
	}
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) getLastIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) getLastTerm() int {
	return rf.logs[rf.getLastIndex()].term
}

func (rf * Raft) getLastIndexAndTerm() (int, int) {
	lastIndex := rf.getLastIndex()
	lastTerm := rf.logs[rf.getLastIndex()].term
	return lastIndex, lastTerm
}

// thread-safe
func (rf * Raft) ValidateLatest(lastTerm int, lastIndex int) bool{
	if lastTerm > rf.currentTerm || (lastTerm == rf.currentTerm && lastIndex >= len(rf.logs)){
		return true
	} else {
		return false
	}
}

func (rf * Raft) getElectionTimeout() time.Duration {
	return time.Duration(150 + rand.Intn(150))
}

func (rf *Raft) getHeartbeatTime() time.Time{
	return rf.electionTimeout
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm{
		reply.VoteGranted = false
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// check if the candidate's log is at least as up-to-date as receiver's log
		lastIndex, lastTerm := rf.getLastIndexAndTerm()
		if rf.ValidateLatest(lastIndex, lastTerm){
			reply.VoteGranted = true
			rf.electionTimeout = time.Now()
		} else {
			reply.VoteGranted = false
		}
	}

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if !ok {
		return
	}

	// get the reply from peer
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != "candidate" || reply.Term < rf.currentTerm || !reply.VoteGranted {
		return
	}

	if reply.Term > rf.currentTerm{
		rf.becomeFollower(reply.Term)
		return
	}


	if reply.VoteGranted{
		rf.voteCnt += 1
		if rf.voteCnt >= rf.majorityCnt{
			rf.becomeLeader()
		}
	}
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
// Term. the third return value is true if this server believes it is
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
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf * Raft) becomeLeader(){
	if rf.state != "candidate"{
		panic("non-candidate cannot be elected as leader")
	}
	rf.state = "leader"
}

func (rf * Raft) becomeFollower(term int){
	if rf.state == "follower" {
		return
	}

	if term <= rf.currentTerm {
		panic("only greater term can override currentTerm")
		return
	}

	rf.currentTerm = term
	rf.state = "follower"
	rf.votedFor = -1
}

func (rf *Raft) becomeCandidate(){
	if rf.state == "leader"{
		return
	}

	rf.state = "candiate"
}

func (rf * Raft) broadcastRequestVote(){
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	me := rf.me
	lastIndex := rf.getLastIndex()
	lastTerm := rf.getLastTerm()
	rf.mu.Unlock()

	var args = &RequestVoteArgs{currentTerm, me, lastIndex, lastTerm}

	for i:=0; i<len(rf.peers); i+=1 {
		if i != me{
			var reply = &RequestVoteReply{}
			go rf.sendRequestVote(i, args, reply)
		}
	}
}

func (rf * Raft) broadcastAppendEntries(){
	rf.mu.Lock()
	me := rf.me
	args := &AppendEntriesArgs{rf.currentTerm, rf.me, -1,
		LogEntry{}, []LogEntry{}, rf.commitIndex}
	rf.mu.Unlock()

	for i:=0;i<len(rf.peers);i+=1{
		if i!= me{
			reply := &AppendEntriesReply{}
			go rf.sendAppendEntries(i, args, reply)
		}
	}

}
func (rf * Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.electionTimeout = time.Now()
	rf.mu.Unlock()

	go rf.broadcastRequestVote()
}

func (rf * Raft) leaderHeartBeatTicker(){
	for rf.killed() == false {
		select {
		case <-time.After(rf.heartbeatTimeout):
			go rf.broadcastAppendEntries()
		}
	}
}

func (rf *Raft) candiateElectionTicker(){
	for rf.killed() == false {
		electionTimeout := rf.getElectionTimeout()

		select {
		case <-time.After(electionTimeout * time.Millisecond):
			{
				rf.mu.Lock()
				duration := time.Since(rf.getHeartbeatTime())
				if duration > electionTimeout { // note that you should only reset hearbeat timeout upon receiving RPC from current leader, not from other peers
					rf.becomeCandidate()
					go rf.startElection()
				} else {
					continue
				}

				rf.mu.Unlock()
			}
		}
	}

}


//
// create a Raft server. the ports
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

	// persistent state
	rf.readPersist(persister.ReadRaftState())
	//currentTerm int
	//votedFor int
	//logs []LogEntry

	// volatile state on all servers
	rf.commitIndex = 0
	rf.lastApplied = 0
	// volatile state on leaders
	lastIndex := rf.getLastIndex()
	for i:=0;i<len(rf.peers); i+=1{
		rf.nextIndex[i] = lastIndex + 1
		rf.matchIndex[i] = 0
	}

	// other
	rf.state = "follower"
	rf.applyCh = applyCh
	rf.electionTimeout = time.Now()
	rf.votedFor = -1

	numPeers := len(rf.peers)
	if numPeers % 2 == 0{
		rf.majorityCnt = numPeers / 2 + 1
	} else {
		rf.majorityCnt = (numPeers + 1) / 2
	}

	// start ticker goroutine to start elections
	go rf.leaderHeartBeatTicker()
	go rf.candiateElectionTicker()

	return rf
}

func (rf * Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply * AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm{
		rf.becomeFollower(reply.Term)
	}

}

func (rf * Raft) AppendEntries(args AppendEntriesArgs, reply * AppendEntriesReply){
	if len(args.Entries) != 0{
		panic("Currently AppendEntries doesnot support non-empty entries")
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.electionTimeout = time.Now()

	reply.Success = true
	return
}

