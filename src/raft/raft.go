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
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

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

// log id is uint64
// each peer's ID is int
// term id is int

type RaftState int32

const (
	Follower RaftState = iota
	Leader
	Candidate
)

var RaftStateNames = []string{"Follower", "Leader", "Candidate"}

func (r RaftState) GetName() string {
	return RaftStateNames[int(r)]
}

type ActionType uint8

const (
	Set ActionType = iota
	Del
)

type LogEntry struct {
	Term    uint32
	Command interface{}
}

type AppendEntriesArgs struct {
	Term         uint32
	LeaderID     int
	PrevLogIndex uint64
	PrevLogTerm  uint32
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    uint32
	Success bool
}

type DebugLock struct {
	mu    sync.Mutex
	ident int
}

func (l *DebugLock) Lock() {
	// log.Println("Waiting for lock", l.ident)
	l.mu.Lock()
	// log.Println("Locked", l.ident)
}

func (l *DebugLock) Unlock() {
	l.mu.Unlock()
	// log.Println("Unlocked", l.ident)
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        DebugLock           // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	currentTerm   uint32
	votedFor      int // candidate id
	log           []LogEntry
	logUpdateCond *sync.Cond // this signal should be broadcasted

	// volatile state on all servers
	commitIndex           uint64
	commitIndexUpdateCond *sync.Cond
	lastApplied           uint64

	// state variable & its broadcast watcher
	state              RaftState
	stateMutex         sync.RWMutex
	stateBroadcastChan chan struct{}

	// volatile state on leaders
	nextIndex           []uint64
	matchIndex          []uint64
	lastUpdateTime      []time.Time
	matchIndexWatchChan chan struct{}
	leaderReady         bool // flag to check if leader's routines have started

	// other runtime channels
	heartbeatChan chan struct{} // Follower
	newEntryChan  chan struct{} // Leader

	// send items back
	applyChan chan ApplyMsg
}

// GetState returns currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.getState() == Leader
	return int(term), isLeader
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

func (rf *Raft) getState() RaftState {
	rf.stateMutex.RLock()
	defer rf.stateMutex.RUnlock()
	return rf.state
}

// setState sets a raft object's current state
func (rf *Raft) setState(newState RaftState) {
	rf.stateMutex.Lock()
	defer rf.stateMutex.Unlock()
	rf.state = newState
	if rf.stateBroadcastChan != nil {
		close(rf.stateBroadcastChan)
		rf.stateBroadcastChan = nil
	}
}

func (rf *Raft) watchState() chan struct{} {
	rf.stateMutex.Lock()
	defer rf.stateMutex.Unlock()
	if rf.stateBroadcastChan == nil {
		rf.stateBroadcastChan = make(chan struct{})
	}
	return rf.stateBroadcastChan
}

// triggerHeartbeat triggers heart beat signal in a non-blocking manner.
func (rf *Raft) triggerHeartbeat() {
	select {
	case rf.heartbeatChan <- struct{}{}:
	default:
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         uint32
	CandidateID  int
	LastLogIndex uint64
	LastLogTerm  uint32
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        uint32
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		// RequestVote: Reply false if term < currentTerm
		return
	} else if args.Term > rf.currentTerm {
		// All servers: If RPC request or response contains term T > currentTerm:
		// set currentTerm = T, convert to follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.setState(Follower)
	}
	// votedFor is null or current candidate and
	// candidate's log is up-to-date as receiver's log
	var logTerm uint32 = 0
	var logIndex uint64 = rf.commitIndex
	if len(rf.log) > 0 {
		logTerm = rf.log[len(rf.log)-1].Term
	}
	if rf.votedFor < 0 || rf.votedFor == args.CandidateID {
		if args.LastLogTerm >= logTerm && args.LastLogIndex >= logIndex {
			// RequestVote: If votedFor is null or candidateId, and candidate’s log is at
			// least as up-to-date as receiver’s log, grant vote
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateID
			rf.triggerHeartbeat()
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
		}
	}
}

// updateCommitIndex calculate the latest commit index by latest match indexes
// This function should be called with mutex held.
func (rf *Raft) updateCommitIndex() {
	indexes := make([]uint64, len(rf.matchIndex))
	copy(indexes, rf.matchIndex)
	sort.Slice(indexes, func(i int, j int) bool { return indexes[i] < indexes[j] })
	newCommitIdx := indexes[(len(indexes)-1)/2]
	log.Printf("Indexs are now: %v\n", indexes)
	if newCommitIdx != rf.commitIndex {
		log.Printf("Leader's commit index updated to %d\n", newCommitIdx)
		rf.commitIndex = newCommitIdx
		rf.commitIndexUpdateCond.Signal()
	}
}

// heartbeatRoutine sends heartbeat to the peer periodically
func (rf *Raft) heartbeatRoutine(server int) {
	for !rf.killed() && rf.getState() == Leader {
		timeout := GetHeartbeatTimeout()
		timeToWait := rf.lastUpdateTime[server].Add(timeout).Sub(time.Now())
		log.Printf("Peer %d -> %d heartbeat routine waiting for %v\n", rf.me, server, timeToWait)
		time.Sleep(timeToWait)
		rf.mu.Lock()
		if rf.killed() || rf.getState() != Leader {
			rf.mu.Unlock()
			return
		}
		if rf.lastUpdateTime[server].Add(timeout).After(time.Now().Add(time.Duration(5) * time.Millisecond)) {
			log.Printf("Peer %d -> %d not now!", rf.me, server)
			rf.mu.Unlock()
			continue
		}
		prevLogTerm := uint32(0)
		prevLogIndex := rf.nextIndex[server] - 1
		if rf.nextIndex[server] > 1 {
			prevLogTerm = rf.log[prevLogIndex-1].Term
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			// empty entries stands for heartbeat
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}
		// reset timer
		rf.lastUpdateTime[server] = time.Now()
		rf.mu.Unlock()
		reply := AppendEntriesReply{}
		log.Printf("Peer %d -> %d sending heartbeat, leadercommit=%d", rf.me, server, args.LeaderCommit)
		// send RPC in its own goroutine so that timeout won't be blocked
		go rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	}
}

// logReplicationRoutine maintains log consistency with one peer
func (rf *Raft) logReplicationRoutine(server int) {
	log.Printf("Peer %d -> %d replication routine started.\n", rf.me, server)
	for !rf.killed() && rf.getState() == Leader {
		rf.mu.Lock()
		lastLogIndex := uint64(len(rf.log))
		for rf.nextIndex[server] > lastLogIndex {
			log.Printf("Peer %d -> %d waiting for log update...\n", rf.me, server)
			rf.logUpdateCond.Wait()
			lastLogIndex = uint64(len(rf.log))
		}
		log.Printf("Peer %d -> %d rf.nextIndex[server] = %d lastLogIndex = %d\n", rf.me, server, rf.nextIndex[server], lastLogIndex)
		// replicate log
		prevLogTerm := uint32(0)
		prevLogIndex := rf.nextIndex[server] - 1
		if rf.nextIndex[server] > 1 {
			prevLogTerm = rf.log[prevLogIndex-1].Term
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      rf.log[rf.nextIndex[server]-1:],
			LeaderCommit: rf.commitIndex,
		}
		reply := AppendEntriesReply{}
		rf.lastUpdateTime[server] = time.Now()
		rf.mu.Unlock()
		ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
		rf.mu.Lock()
		if ok {
			if reply.Success {
				rf.matchIndex[server] = lastLogIndex
				rf.nextIndex[server] = lastLogIndex + 1
				rf.updateCommitIndex()
			} else {
				log.Printf("Peer %d - %d append entries failed\n", rf.me, server)
				rf.nextIndex[server]-- // retry
			}
		} // retry when next request comes in
		rf.mu.Unlock()
	}
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("Peer %d received AppendEntries with %d entries.\n", rf.me, len(args.Entries))
	reply.Success = false

	if args.Term < rf.currentTerm {
		// AppendEntries: Reply false if term < currentTerm.
		return
	} else if args.Term >= rf.currentTerm {
		// Candidate: If AppendEntries RPC received from new leader: convert to follower.
		// All servers: If RPC request contains term T > currentTerm: set currentTerm = T, convert to follower.
		state := rf.getState()
		if (state == Candidate || args.Term > rf.currentTerm) && state != Follower {
			rf.setState(Follower)
		}
		rf.currentTerm = args.Term
	}

	rf.triggerHeartbeat()
	// AppendEntries: Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if args.PrevLogIndex > 0 {
		if uint64(len(rf.log))+1 <= args.PrevLogIndex || rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			return
		}
	}
	// AppendEntries: If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that follow it.
	// AppendEntries: Append any new entries not already in the lo
	if args.PrevLogIndex == 0 {
		rf.log = args.Entries
	} else {
		rf.log = append(rf.log[:args.PrevLogIndex], args.Entries...)
	}
	if args.LeaderCommit > rf.commitIndex {
		lastEntryIdx := uint64(len(rf.log))
		// rf.commitIndex = min(args.LeaderCommit, idx of last entry)
		if args.LeaderCommit < lastEntryIdx {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastEntryIdx
		}
		log.Printf("Peer %d commitIndex is now at %d\n", rf.me, rf.commitIndex)
		rf.commitIndexUpdateCond.Signal()
	}

	reply.Success = true
}

// applyRoutine sends back commited entries back to server via applyChan
func (rf *Raft) applyRoutine() {
	rf.mu.Lock()
	lastCommitIndex := uint64(0)
	rf.mu.Unlock()
	for !rf.killed() {
		rf.mu.Lock()
		if lastCommitIndex == rf.commitIndex {
			rf.commitIndexUpdateCond.Wait()
		}
		newLogs := rf.log[lastCommitIndex:rf.commitIndex]
		startIndex := lastCommitIndex + 1
		rf.mu.Unlock()
		for off, l := range newLogs {
			idx := int(startIndex + uint64(off))
			log.Printf("Peer %d applying log index %v\n", rf.me, idx)
			rf.applyChan <- ApplyMsg{
				CommandValid: true,
				Command:      l.Command,
				CommandIndex: idx,
			}
		}
		rf.mu.Lock()
		lastCommitIndex = rf.commitIndex
		rf.mu.Unlock()
	}
}

// sendRequestVote send a request vote request to specified peer synchronously
// this function will acquire mutex, and since it's synchronous
// it should be called in a separate goroutine
func (rf *Raft) sendRequestVote(server int, lastLogTerm uint32) (ok bool, granted bool, term uint32) {
	rf.mu.Lock()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.lastApplied,
		LastLogTerm:  lastLogTerm,
	}
	rf.mu.Unlock()
	reply := RequestVoteReply{}
	log.Printf("Peer %d sending request vote to %d, term = %d\n", rf.me, server, rf.currentTerm)
	ok = rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	log.Printf("Peer %d received from %d, voteGranted = %v, term = %v\n", rf.me, server, reply.VoteGranted, reply.Term)
	return ok, reply.VoteGranted, reply.Term
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
	if rf.getState() != Leader {
		return 0, 0, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("Peer %d(Leader) received new command", rf.me)
	index := rf.nextIndex[rf.me]
	rf.nextIndex[rf.me]++
	term := int(rf.currentTerm)
	entry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	// signal & leave all other jobs to log replication routines
	rf.logUpdateCond.Broadcast()
	log.Printf("Start returning index = %d\n", index)
	return int(index), term, true
}

func GetElectionTimeout() time.Duration {
	return time.Duration(500+rand.Int()%500) * time.Millisecond
}

func GetHeartbeatTimeout() time.Duration {
	// heartbeat timeout should be way lower than election timeout
	// we set it to the lab's limit 10Hz(100ms)
	return time.Duration(100) * time.Millisecond
}

func (rf *Raft) mainRoutine() {
	// from time.After's doc:
	// The underlying Timer is not recovered by the garbage collector
	// until the timer fires. If efficiency is a concern,
	// use NewTimer instead and call Timer.Stop if the timer is no longer needed.
	go rf.applyRoutine()
	for !rf.killed() {
		state := rf.getState()
		log.Printf("Peer %d now %v\n", rf.me, state.GetName())
		switch state {
		case Follower:
			// wait for heartbeat, or become a new candidate
			t := time.NewTimer(GetElectionTimeout())
			select {
			case <-rf.heartbeatChan:
				// continue
			case <-rf.watchState():
				// continue
			case <-t.C:
				rf.setState(Candidate)
			}
			t.Stop()
		case Candidate:
			// prepare for leader election
			rf.mu.Lock()
			lastLogTerm := uint32(0)
			if len(rf.log) > 0 {
				lastLogTerm = rf.log[len(rf.log)-1].Term
			}
			rf.currentTerm++
			newTerm := rf.currentTerm
			// vote for myself first
			rf.votedFor = rf.me
			rf.mu.Unlock()
			// send request vote RPCs
			voteChan := make(chan bool)
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(idx int) {
					ok, granted, term := rf.sendRequestVote(idx, lastLogTerm)
					if ok {
						voteChan <- (granted && term == newTerm)
					}
				}(i)
			}
			// wait for responses
			voteCount := 1
			timeOutChan := time.After(GetElectionTimeout())
		outer:
			for {
				select {
				case granted := <-voteChan:
					if granted {
						voteCount++
						if voteCount >= (len(rf.peers)+1)/2 {
							rf.setState(Leader)
							break outer
						}
					}
				case <-rf.watchState():
					// restart everything
					break outer
				case <-timeOutChan:
					// restart election
					break outer
				}
			}
		case Leader:
			// check & start all log replication routines
			if !rf.leaderReady {
				lastLogIndex := uint64(len(rf.log))
				rf.nextIndex = make([]uint64, len(rf.peers))
				rf.matchIndex = make([]uint64, len(rf.peers))
				rf.lastUpdateTime = make([]time.Time, len(rf.peers))
				// set update time to a value far before
				updateTime := time.Now().Add(-GetHeartbeatTimeout() - time.Duration(1000)*time.Millisecond)
				for i := range rf.peers {
					rf.nextIndex[i] = lastLogIndex + 1
					rf.matchIndex[i] = 0
					rf.lastUpdateTime[i] = updateTime
				}
				for i := range rf.peers {
					if i != rf.me {
						go rf.heartbeatRoutine(i)
						go rf.logReplicationRoutine(i)
					}
				}
				rf.leaderReady = true
			}
			// wait on state change
			<-rf.watchState()
		}
	}
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
	log.Printf("Killing peer %d...", rf.me)
	// close(rf.appendReceiveChan)
	close(rf.heartbeatChan)
	// this indicates that the raft object is closed
	if rf.stateBroadcastChan != nil {
		close(rf.stateBroadcastChan)
	}
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)

	return z == 1
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
	rf := &Raft{
		peers:         peers,
		persister:     persister,
		me:            me,
		state:         Follower, // this is default for new servers
		currentTerm:   0,
		votedFor:      -1,
		heartbeatChan: make(chan struct{}),
		newEntryChan:  make(chan struct{}),
		applyChan:     applyCh,
	}
	rf.mu = DebugLock{
		mu:    sync.Mutex{},
		ident: me,
	}
	rf.logUpdateCond = sync.NewCond(&rf.mu)
	rf.commitIndexUpdateCond = sync.NewCond(&rf.mu)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.mainRoutine()
	return rf
}
