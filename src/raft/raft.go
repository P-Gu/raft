package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"kvstore/labgob"
	"kvstore/labrpc"
)

type LogEntry struct {
	Term    int
	Command interface{}
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
	Term         int
	LeadId       int
	PrevLogIndex int // We will send the corresponding index in nextIndex[] here, but minus one
	PrevLogTerm  int
	Entries      *[]LogEntry
	LeaderCommit int
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
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
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
	prevTerm    int
	votedFor    int
	logs        []LogEntry
	// volatile state on all servers
	commitIndex int
	lastApplied int
	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// other
	state           string // "leader" "candidate" "follower"
	applyCh         chan ApplyMsg
	electionTimeout time.Time // last event time
	voteCnt         int
	majorityCnt     int
	lockVersion     int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	lockVersion := rf.lockVersion
	DPrintf("[node %d]: GetState() acquires lock %d", rf.me, lockVersion)
	rf.lockVersion++

	defer DPrintf("[node %d]: GetState() releases lock %d", rf.me, lockVersion)
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if rf.state == "leader" {
		isleader = true
	} else {
		isleader = false
	}

	DPrintf("[node %d]: GetState: [Term=%d, isLeader=%v]", rf.me, rf.currentTerm, isleader)
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
		d.Decode(&tmpVotedFor) != nil || d.Decode(&tmpLogs) != nil {
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
	return rf.logs[rf.getLastIndex()].Term
}

func (rf *Raft) getLastIndexAndTerm() (int, int) {
	lastIndex := rf.getLastIndex()
	lastTerm := rf.logs[rf.getLastIndex()].Term
	return lastIndex, lastTerm
}

//
func (rf *Raft) ValidateLatest(lastTerm int, lastIndex int) bool {
	myLastIndex, myLastTerm := rf.getLastIndexAndTerm()
	DPrintf("ValidateLatest: lastIndex=%d, lastTerm=%d, myLastIndex=%d, myLastTerm=%d",
		lastIndex, lastTerm, myLastIndex, myLastTerm)
	if lastTerm > myLastTerm ||
		(lastTerm == myLastTerm && lastIndex >= myLastIndex) {
		return true
	} else {
		return false
	}
}

func (rf *Raft) getElectionTimeout() time.Duration {
	return time.Duration(200 + rand.Intn(250))
}

func (rf *Raft) getHeartbeatTimeout() time.Duration {
	return time.Duration(80)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("[node %d]: receives Vote request from node %d: [term=%d, lastLogIndex=%d]", rf.me, args.CandidateId,
		args.Term, args.LastLogIndex)
	rf.mu.Lock()
	lockVersion := rf.lockVersion
	DPrintf("[node %d]: RequestVote() acquires lock %d", rf.me, lockVersion)
	rf.lockVersion++

	defer DPrintf("[node %d]: RequestVote() releases lock %d", rf.me, lockVersion)
	defer rf.mu.Unlock()

	DPrintf("[node %d]: receives Vote request from node %d: [term=%d, lastLogIndex=%d], [currentTerm=%d, votedFor=%d]", rf.me, args.CandidateId,
		args.Term, args.LastLogIndex, rf.currentTerm, rf.votedFor)

	// TODO: maybe delete the below codes
	if args.LastLogTerm > rf.getLastTerm() ||
		(args.LastLogTerm == rf.getLastTerm() && args.LastLogIndex >= rf.getLastIndex()) {
		rf.becomeFollower(rf.currentTerm)
	}

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("[node %d]: the Vote request from node %d is old, reject", rf.me, args.CandidateId)

	} else if args.Term > rf.currentTerm ||
		(args.Term == rf.currentTerm && rf.votedFor == -1 || rf.votedFor == args.CandidateId) {

		if rf.state == "leader" {
			rf.becomeFollower(args.Term)
		} else {
			rf.currentTerm = args.Term
		}
		reply.Term = rf.currentTerm

		// if receive a larger term, update currentTerm
		// check if the candidate's log is at least as up-to-date as receiver's log
		lastIndex, lastTerm := rf.getLastIndexAndTerm()
		if rf.ValidateLatest(lastIndex, lastTerm) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId

			rf.electionTimeout = time.Now()
			DPrintf("[node %d]: granted vote to node %d", rf.me, args.CandidateId)
		} else {
			reply.VoteGranted = false
			DPrintf("[node %d]: node %d doesn't contain latest LogEntries, reject", rf.me, args.CandidateId)
		}
	} else {
		DPrintf("[node %d]: already voteFor other peer %d, reject peer %d", rf.me, rf.votedFor, args.CandidateId)
	}
	return
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
	t1 := time.Now()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	duration := time.Since(t1)

	if !ok {
		DPrintf("sendRequestVote RPC failed")
		return
	}

	DPrintf("[node %d]: [%v ms] of RPC call", rf.me, duration)
	DPrintf("[node %d]: receives vote reply from node %d: [term=%d, voteGranted=%v]",
		rf.me, server, reply.Term, reply.VoteGranted)

	// get the reply from peer
	rf.mu.Lock()
	lockVersion := rf.lockVersion
	DPrintf("[node %d]: sendRequestVote() acquires lock %d", rf.me, lockVersion)
	rf.lockVersion++
	defer DPrintf("[node %d]: sendRequestVote() releases lock %d", rf.me, lockVersion)
	defer rf.mu.Unlock()

	if rf.state != "candidate" || reply.Term < rf.currentTerm || !reply.VoteGranted {
		DPrintf("[node %d]: cannot accept this vote: [state=%v, reply.Term=%d, currentTerm=%d, reply.VoteGranted=%v]",
			rf.me, rf.state, reply.Term, rf.currentTerm, reply.VoteGranted)
		return
	}

	if reply.Term > rf.currentTerm {
		DPrintf("[node %d]: received a higher term from RequestVote, step down to follower", rf.me)
		rf.becomeFollower(reply.Term)
		return
	}

	// reply.Term == rf.currentTerm
	if reply.VoteGranted {
		DPrintf("[node %d]: vote ++ from node %d", rf.me, server)
		fmt.Printf("[node %d]: vote ++ from node %d", rf.me, server)
		rf.voteCnt += 1

		if rf.voteCnt >= rf.majorityCnt {
			DPrintf("[node %d]: voteCnt %d >= majorityCnt %d", rf.me, rf.voteCnt, rf.majorityCnt)
			fmt.Printf("[node %d]: voteCnt %d >= majorityCnt %d", rf.me, rf.voteCnt, rf.majorityCnt)
			rf.becomeLeader()
			return
		} else {
			DPrintf("[node %d]: voteCnt %d < majorityCnt %d", rf.me, rf.voteCnt, rf.majorityCnt)
		}
	} else {
		DPrintf("[node %d]: doesnot know how to deal with the vote reply", rf.me)
	}

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	//rf.logs = append(rf.logs, LogEntry{123, 321})

	// Your code here (2B).
	if rf.state != "leader" {
		isLeader = false
	}
	term = rf.currentTerm
	index = rf.lastApplied + 1
	//time.Sleep(5 * time.Second)

	//fmt.Printf("command here %v\n", command)
	//fmt.Println(term)
	//fmt.Println(isLeader)
	if isLeader {
		go rf.broadcastAppendEntries(command)
	}

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

func (rf *Raft) becomeLeader() {
	if rf.state == "leader" {
		return
	}
	if rf.state != "candidate" {
		panic("non-candidate cannot be elected as leader")
	}

	fmt.Printf("A leader\n")
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	lastIndex := rf.getLastIndex()
	for i := 0; i < len(rf.peers); i += 1 {
		rf.nextIndex[i] = lastIndex + 1
		rf.matchIndex[i] = 0
	}
	rf.state = "leader"
	DPrintf("[node %d]: becomes leader in Term %d", rf.me, rf.currentTerm)
	// TODO: bug: you can never stop it even if me is not leader any more

	go rf.leaderHeartBeatTicker() // periodically send heartbeats to followers
	// DPrintf("[node %d]: becomeLeader created a go routine", rf.me)
}

// lock must be held before calling this function
func (rf *Raft) becomeFollower(term int) {
	if term < rf.currentTerm {
		panic("only greater Term can override currentTerm")
		return
	}

	rf.currentTerm = term
	rf.state = "follower"
	DPrintf("[node %d]: becomes follower", rf.me)
	rf.votedFor = -1
}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	lockVersion := rf.lockVersion
	DPrintf("[node %d]: becomeCandidate() acquires lock %d", rf.me, lockVersion)
	rf.lockVersion++
	defer DPrintf("[node %d]: becomeCandidate() releases lock %d", rf.me, lockVersion)
	defer rf.mu.Unlock()

	rf.state = "candidate"
	DPrintf("[node %d]: becomes candidate in term %d", rf.me, rf.currentTerm)
}

func (rf *Raft) broadcastRequestVote() {
	rf.mu.Lock()
	lockVersion := rf.lockVersion
	DPrintf("[node %d]: broadcastRequestVote() acquires lock %d", rf.me, lockVersion)
	rf.lockVersion++
	defer DPrintf("[node %d]: broadcastRequestVote() releases lock %d", rf.me, lockVersion)
	defer rf.mu.Unlock()

	lastIndex, lastTerm := rf.getLastIndexAndTerm()
	n_peers := len(rf.peers)

	for i := 0; i < n_peers; i += 1 {
		if i != rf.me {
			var args = &RequestVoteArgs{rf.currentTerm, rf.me, lastIndex, lastTerm}
			DPrintf("[node %d]: broadcast RequestVote ...", rf.me)
			reply := &RequestVoteReply{}
			// race
			go rf.sendRequestVote(i, args, reply)
			// DPrintf("[node %d]: sendRequestVote created a go routine", rf.me)
			DPrintf("[node %d]: sendRequestVote to node %d", rf.me, i)
		}
	}
}

func (rf *Raft) broadcastAppendEntries(command interface{}) {
	DPrintf("[node %d]: broadcasting heartbeats ...", rf.me)
	rf.mu.Lock()
	lockVersion := rf.lockVersion
	DPrintf("[node %d]: broadcastAppendEntries() acquires lock %d", rf.me, lockVersion)
	rf.lockVersion++

	if command != nil {
		//fmt.Printf("command %v\n", command)
		rf.logs = append(rf.logs, LogEntry{rf.currentTerm, command})
		rf.nextIndex[rf.me]++
		rf.lastApplied++
		var success_count uint32
		var wg sync.WaitGroup
		for i := 0; i < len(rf.peers); i += 1 {
			//rf.nextIndex = append(rf.nextIndex, rf.commitIndex) // TODO: ?
			if i != rf.me {
				wg.Add(1)
				go rf.sendOneLogEntry(command, i, &success_count, &wg)
				// DPrintf("[node %d]: broadcastAppendEntries created a go routine", rf.me)
			}
		}
		rf.mu.Unlock()

		wg.Wait()
		//fmt.Printf("success count %d\n", int(success_count))
		//fmt.Printf("limit %f\n", float32(len(rf.peers))/2-1)
		if float32(success_count) > float32(len(rf.peers))/2-1 {
			rf.commitIndex++
			//fmt.Printf("commit index %d\n", rf.commitIndex)
		} else if len(rf.logs) > 1 {
			rf.logs = rf.logs[:len(rf.logs)-1]
			rf.lastApplied--
		}
	} else {
		defer DPrintf("[node %d]: broadcastAppendEntries() releases lock %d", rf.me, lockVersion)
		defer rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i += 1 {
			if i != rf.me {
				var pnt *[]LogEntry
				args := &AppendEntriesArgs{rf.currentTerm, rf.me, -1,
					-1, pnt, rf.commitIndex}
				reply := &AppendEntriesReply{-1, false}
				go rf.sendAppendEntries(i, args, reply)
				// DPrintf("[node %d]: broadcastAppendEntries created a go routine", rf.me)
			}
		}
	}
}

func (rf *Raft) sendOneLogEntry(command interface{}, i int, success_count_pt *uint32, wg *sync.WaitGroup) {
	var tmpLogs []LogEntry
	rf.mu.Lock()
	if rf.nextIndex[i] < len(rf.logs) {
		tmpLogs = append(tmpLogs, rf.logs[rf.nextIndex[i]:]...)
	}
	//tmpLogs = append(tmpLogs, LogEntry{rf.currentTerm, command})
	prevLogIndex := rf.nextIndex[i] - 1 // the index to add entries in the follower's log
	if prevLogIndex >= len(rf.logs) {
		return
	}
	prevLogTerm := rf.logs[prevLogIndex].Term
	rf.mu.Unlock()

	//logIndexToAdd := rf.lastApplied // the index of entry in this leader to be sent to follower
	for {
		rf.mu.Lock()
		rf.lockVersion++
		//fmt.Printf("prev log index before send %d\n", prevLogIndex)
		//fmt.Printf("prev log term before send %d\n", prevLogTerm)
		args := &AppendEntriesArgs{rf.currentTerm, rf.me, prevLogIndex,
			prevLogTerm, &tmpLogs, rf.commitIndex}
		if len(tmpLogs) > 1 {
			fmt.Printf("int i = %d\n", i)
			fmt.Printf("next index %d\n", rf.nextIndex[i])
			fmt.Printf("tmp log before send %v\n", args.Entries)
		}
		reply := &AppendEntriesReply{-1, true}
		rf.mu.Unlock()
		rf.sendAppendEntries(i, args, reply)
		//fmt.Printf("after send peer %d\n", i)
		if reply.Success {
			atomic.AddUint32(success_count_pt, 1)
			rf.mu.Lock()
			rf.nextIndex[i] += len(tmpLogs)
			rf.mu.Unlock()
			break
		} else {
			fmt.Printf("Failed with follower term %d\n", reply.Term)
			//logIndexToAdd--
			rf.mu.Lock()
			rf.nextIndex[i]--
			prevLogIndex = rf.nextIndex[i] - 1
			prevLogTerm = rf.logs[prevLogIndex].Term
			tmpLogs = append([]LogEntry{rf.logs[rf.nextIndex[i]]}, tmpLogs...)
			rf.mu.Unlock()
		}
	}
	wg.Done()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	lockVersion := rf.lockVersion
	DPrintf("[node %d]: startElection() acquires lock %d", rf.me, lockVersion)
	rf.lockVersion++

	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.voteCnt = 1
	rf.electionTimeout = time.Now()

	DPrintf("[node %d]: startElection: current Term=%d, votedFor=%d", rf.me, rf.currentTerm, rf.votedFor)
	rf.mu.Unlock()
	DPrintf("[node %d]: startElection() releases lock %d", rf.me, lockVersion)
	rf.broadcastRequestVote()
}

func (rf *Raft) leaderHeartBeatTicker() {
	heartbeatTimeout := rf.getHeartbeatTimeout()

	for rf.killed() == false {
		time.Sleep(heartbeatTimeout * time.Millisecond)

		rf.mu.Lock()
		lockVersion := rf.lockVersion
		DPrintf("[node %d]: leaderHeartBeatTicker() acquires lock %d", rf.me, lockVersion)
		rf.lockVersion++

		if rf.state == "leader" {
			rf.mu.Unlock()
			DPrintf("[node %d]: leaderHeartBeatTicker() releases lock %d", rf.me, lockVersion)

			rf.broadcastAppendEntries(nil)
		} else {
			rf.mu.Unlock()
			DPrintf("[node %d]: leaderHeartBeatTicker() releases lock %d", rf.me, lockVersion)
			break
		}

	}
}

func (rf *Raft) candiateElectionTicker() {
	for rf.killed() == false {
		nowTime := time.Now()

		electionTimeout := rf.getElectionTimeout() * time.Millisecond
		DPrintf("[node %d]: initialized election out = [%v]", rf.me, electionTimeout)

		time.Sleep(electionTimeout)

		rf.mu.Lock()
		lockVersion := rf.lockVersion
		DPrintf("[node %d]: candiateElectionTicker() acquires lock %d", rf.me, lockVersion)
		rf.lockVersion++

		if rf.electionTimeout.Before(nowTime) && rf.state != "leader" {
			rf.mu.Unlock()
			DPrintf("[node %d]: candiateElectionTicker() releases lock %d", rf.me, lockVersion)

			DPrintf("[node %d]: [%v ms] election time out", rf.me, electionTimeout)
			rf.becomeCandidate()
			rf.startElection()
		} else {
			rf.mu.Unlock()
			DPrintf("[node %d]: candiateElectionTicker() releases lock %d", rf.me, lockVersion)
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.voteCnt = 0
	rf.logs = append(rf.logs, LogEntry{Term: 0})
	// currentTerm=0, votedFOr=-1, logs: [{0 <nil>}]
	//DPrintf("currentTerm=%d, votedFOr=%d, logs: %v", rf.currentTerm, rf.votedFor, rf.logs)
	rf.readPersist(persister.ReadRaftState())

	// volatile state on all servers
	rf.commitIndex = 0
	rf.lastApplied = 0
	// volatile state on leaders

	// other
	rf.state = "follower"
	rf.applyCh = applyCh
	rf.electionTimeout = time.Now()
	rf.lockVersion = 0

	numPeers := len(rf.peers)
	if numPeers%2 == 0 {
		rf.majorityCnt = numPeers/2 + 1
	} else {
		rf.majorityCnt = (numPeers + 1) / 2
	}

	// start ticker goroutine to start elections
	go rf.candiateElectionTicker() // check whether the election timeouts and start a new election

	DPrintf("[node %d]: raft %d created", rf.me, rf.me)
	return rf
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("[node %d]: send a heartbeat to peer %d", rf.me, server)
	if args.Entries != nil && len(*args.Entries) > 1 {
		fmt.Printf("tmp log to node %d before send 2 %v\n", server, *args.Entries)
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//fmt.Printf("after rpc\n")

	if !ok {
		return
	}

	DPrintf("[node %d]: receive hearbeat ack from peer %d: [Term=%d, Success=%v]",
		rf.me, server, reply.Term, reply.Success)

	rf.mu.Lock()
	lockVersion := rf.lockVersion
	DPrintf("[node %d]: sendAppendEntries() acquires lock %d", rf.me, lockVersion)
	rf.lockVersion++
	defer DPrintf("[node %d]: sendAppendEntries() releases lock %d", rf.me, lockVersion)
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		DPrintf("[node %d]: become follower because of higher term received: %d", rf.me, reply.Term)
		if reply.Success {
			rf.currentTerm = reply.Term
		} else {
			rf.becomeFollower(reply.Term)
		}
	}

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//DPrintf("[node %d]: receives heartbeat from peer %d: [Term=%d]", rf.me, args.LeadId, args.Term)
	//DPrintf("[node %d]: Current term %d", rf.me, rf.currentTerm)
	if args.Entries != nil && len(*args.Entries) > 1 {
		fmt.Printf("node %d, tmp log AFTER send %v\n", rf.me, *args.Entries)
		fmt.Printf("prev log index %d and prev log term %d\n", args.PrevLogIndex, args.PrevLogTerm)
		fmt.Printf("args term %d and current term %d\n", args.Term, rf.currentTerm)
	}

	rf.mu.Lock()

	//lockVersion := rf.lockVersion
	//DPrintf("[node %d]: AppendEntries() acquires lock %d", rf.me, lockVersion)
	rf.lockVersion++
	//defer DPrintf("[node %d]: AppendEntries() releases lock %d", rf.me, lockVersion)
	defer rf.mu.Unlock()

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > rf.lastApplied {
			rf.commitIndex = rf.lastApplied
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	} else {
		rf.commitIndex = args.LeaderCommit
	}

	// TODO: maybe delete the below codes
	if args.PrevLogTerm > rf.getLastTerm() ||
		(args.PrevLogTerm == rf.getLastTerm() && args.PrevLogIndex >= rf.getLastIndex()) {
		rf.becomeFollower(rf.currentTerm)
		args.Term = rf.currentTerm
	}

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm { // rf.currentTerm, false
		reply.Success = false
		return
	} else if args.Term == rf.currentTerm { // rf.currentTerm=args.Term, false
		// reset election timeout
		rf.electionTimeout = time.Now()

		reply.Success = true

		if rf.state != "follower" {
			rf.becomeFollower(args.Term)
		}

	} else { // args.Term, true
		rf.becomeFollower(args.Term)
		reply.Success = true
	}

	if args.Entries == nil {
		/*if rf.state != "follower" {
			rf.becomeFollower(args.Term)
		}*/
	} else {
		if args.PrevLogIndex < len(rf.logs) {
			if args.PrevLogIndex == 0 || (rf.logs[args.PrevLogIndex].Term == args.PrevLogTerm) {
				rf.logs = append(rf.logs, *args.Entries...)
				rf.lastApplied += len(*args.Entries)
			} else {
				rf.logs = rf.logs[:args.PrevLogIndex]
				rf.lastApplied = args.PrevLogIndex - 1
				reply.Success = false
			}
		} else {
			reply.Success = false
		}
	}
	if args.Entries != nil && len(*args.Entries) > 1 {
		fmt.Printf("log after append %v\n", rf.logs)
	}

	DPrintf("[node %d]: sends heartbeat ack back to peer %d: [Term=%d, Sucess=%v]",
		rf.me, args.LeadId,
		reply.Term, reply.Success)
	return
}
