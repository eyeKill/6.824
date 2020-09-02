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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	currentTerm uint32
	votedFor    int // candidate id
	log         []LogEntry

	// volatile state on all servers
	commitIndex uint64
	lastApplied uint64

	// state variable & its watcher
	state          RaftState
	stateWatchChan chan struct{}
	stateMutex     sync.Mutex

	// volatile state on leaders
	nextIndex  []uint64
	matchIndex []uint64

	// runtime channels
	heartbeatChan     chan struct{}
	appendReceiveChan chan struct{}
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
	rf.stateMutex.Lock()
	defer rf.stateMutex.Unlock()
	return rf.state
}

// setState sets a raft object's current state
func (rf *Raft) setState(newState RaftState) {
	rf.stateMutex.Lock()
	defer rf.stateMutex.Unlock()
	rf.state = newState
	if rf.stateWatchChan != nil {
		close(rf.stateWatchChan)
		rf.stateWatchChan = nil
	}
}

// watchState returns a channel that watches the changes of state variable
// this implementation is not ideal performance-wise, but it enables us to watch in select statement
func (rf *Raft) watchState() chan struct{} {
	rf.stateMutex.Lock()
	defer rf.stateMutex.Unlock()
	if rf.stateWatchChan == nil {
		rf.stateWatchChan = make(chan struct{})
	}
	return rf.stateWatchChan
}

// triggerHeartbeat triggers heart beat signal in a non-blocking manner.
// Since it does not block, it won't introduce dead locks.
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

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
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

	if len(args.Entries) == 0 {
		rf.triggerHeartbeat()
	} else {
		if uint64(len(rf.log)) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			// AppendEntries: Reply false if log doesn’t contain an entry at prevLogIndex
			// whose term matches prevLogTerm
			return
		}
		// AppendEntries: If an existing entry conflicts with a new one (same index
		// but different terms), delete the existing entry and all that follow it.
		// AppendEntries: Append any new entries not already in the log
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	}

	if args.LeaderCommit > rf.commitIndex {
		// rf.commitIndex = min(args.LeaderCommit, rf.lastApplied)
		if args.LeaderCommit < rf.lastApplied {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.lastApplied
		}
	}

	reply.Success = true
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

// sendHeartbeat send a heartbeat to given server synchronously,
// and return whether the heartbeat is accepted and the server's term
func (rf *Raft) sendHeartbeat(server int) (ok bool, success bool, term uint32) {
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: rf.lastApplied,
		PrevLogTerm:  rf.currentTerm,
		// empty entries stands for heartbeat
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	ok = rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	return ok, reply.Success, reply.Term
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
	// TODO leader function
	index, term := rf.ExecuteRequest(command)
	return int(index), int(term), true
}

func (rf *Raft) ExecuteRequest(command interface{}) (index uint64, term uint32) {
	// append to local log
	rf.mu.Lock()
	rf.nextIndex[rf.me]++
	index = rf.nextIndex[rf.me]
	term = rf.currentTerm
	rf.mu.Unlock()
	go func() {
		// append to local log first
		entry := LogEntry{}
		rf.log = append(rf.log)
	}()
	return
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
	for !rf.killed() {
		state := rf.getState()
		log.Printf("Peer %d now state=[Follower, Leader, Candidate][%v]\n", rf.me, state)
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
			t := time.NewTimer(GetHeartbeatTimeout())
			select {
			case <-rf.appendReceiveChan:
				// do nothing, continue to next cycle
			case <-t.C:
				// send heartbeat
				log.Printf("Peer %d sending heartbeat...\n", rf.me)
				for i := range rf.peers {
					if i == rf.me {
						continue
					}
					go rf.sendHeartbeat(i)
				}
				// TODO maybe add some retry logic?
			}
			t.Stop()
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
	if rf.stateWatchChan != nil {
		close(rf.stateWatchChan)
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
		peers:             peers,
		persister:         persister,
		me:                me,
		state:             Follower, // this is default for new servers
		currentTerm:       0,
		votedFor:          -1,
		heartbeatChan:     make(chan struct{}),
		appendReceiveChan: make(chan struct{}),
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.mainRoutine()
	return rf
}
