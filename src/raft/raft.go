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
	"log"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"
import "../labgob"

var ElectionInterval = 100
// import "bytes"
// import "../labgob"

// ApplyMsg
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

const(
	Follower	int = 0
	Candidate	= 1
	Leader		= 2
)

type LogEntry struct{
	Command 	interface{}
	Term		int
}

// Raft
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	leaderId int
	state int
	votedFor  int
	currentTerm	int
	log 	[]LogEntry

	applyCond	*sync.Cond

	applyCh		chan ApplyMsg
	electionTimeout int
	heartbeatPeriod	int

	latestIssueTime int64
	latestHeardTime int64

	commitIndex int
	lastApplied int

	nextIndex []int
	matchIndex []int

	leaderCond	*sync.Cond
	nonLeaderCond	*sync.Cond

	electionTimeoutChan	chan bool
	heartbeatPeriodChan	chan bool
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type AppendEntriesArgs struct{
	Term		 int
	LeaderId 	 int
	PrevLogIndex int
	PrevLogTerm	 int
	Entries 	 []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct{
	Term	int
	ConflictTerm	int
	ConflictFirstIndex	int
	Success	bool
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == Leader{
		isleader = true
	}else{
		isleader = false
	}
	rf.mu.Unlock()
	// Your code here (2A).
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
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	DPrintf("[persist]: Id %d Term %d State %s\t||\tsave persistent state\n",
		rf.me, rf.currentTerm, state2name(rf.state))
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
	var currentTerm	int
	var voteFor	int
	var logs	[]LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil{
		log.Fatal("[readPersist]: decode error!\n")
	}else{
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.log = logs
	}
}

// RequestVoteArgs
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

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

// RequestVote
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm <= args.Term{
		if rf.currentTerm < args.Term {
			DPrintf("[RequestVote]: Id %d Term %d State %s\t||\targs's term %d is larger\n",
				rf.me, rf.currentTerm, state2name(rf.state), args.Term)
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.switchTo(Follower)

			rf.persist()
		}

		if rf.votedFor == -1 || rf.votedFor == args.CandidateId{
			lastLogIndex := len(rf.log) - 1
			if lastLogIndex < 0{
				DPrintf("[RequestVote]: Id %d Term %d State %s\t||\tinvalid lastLogIndex: %d\n",
					rf.me, rf.currentTerm, state2name(rf.state), lastLogIndex)
			}
			lastLogTerm := rf.log[lastLogIndex].Term

			DPrintf("[RequestVote]: Id %d Term %d State %s\t||\tlastLogIndex %d and lastLogTerm %d" +
				" while args's lastLogIndex %d lastLogTerm %d\n", rf.me, rf.currentTerm, state2name(rf.state),
				lastLogIndex, lastLogTerm, args.LastLogIndex, args.LastLogTerm)

			if lastLogTerm < args.LastLogTerm ||
				(lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex){
				rf.votedFor = args.CandidateId
				rf.resetElectionTimer()

				rf.switchTo(Follower)
				DPrintf("[RequestVote]: Id %d Term %d State %s\t||\tlastLogIndex %d and lastLogTerm %d" +
					" while args's lastLogIndex %d lastLogTerm %d\n", rf.me, rf.currentTerm, state2name(rf.state),
					lastLogIndex, lastLogTerm, args.LastLogIndex, args.LastLogTerm)
				rf.persist()

				reply.Term = rf.currentTerm
				reply.VoteGranted = true
				return
			}
		}
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

func(rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()

	if rf.currentTerm <= args.Term{
		if rf.currentTerm < args.Term{
			DPrintf("[AppendEntries]: Id %d Term %d State %s\t||\targs's term %d is newer\n",
				rf.me, rf.currentTerm, state2name(rf.state), args.Term)

			rf.currentTerm = args.Term
			rf.resetElectionTimer()
			rf.votedFor = -1
			rf.switchTo(Follower)
			rf.persist()
		}

		if len(rf.log) > args.PrevLogIndex &&
			rf.log[args.PrevLogIndex].Term == args.PrevLogTerm{
			rf.switchTo(Follower)

			isMatch := true
			nextIndex := args.PrevLogIndex + 1
			end := len(rf.log) - 1
			for i := 0; isMatch && i < len(args.Entries); i++{
				if end < nextIndex + i{
					isMatch = false
				}else if rf.log[nextIndex + i].Term != args.Entries[i].Term{
					isMatch = false
				}
			}
			if isMatch == false{
				entries := make([]LogEntry, len(args.Entries))
				copy(entries, args.Entries)
				rf.log = append(rf.log[:nextIndex], entries...)
			}

			DPrintf("[AppendEntries]: Id %d Term %d State %s\t||\tcommitIndex %d while leaderCommit %d" +
				" for leader %d\n", rf.me, rf.currentTerm, state2name(rf.state), rf.commitIndex,
				args.LeaderCommit, args.LeaderId)

			indexOfLastOfNewEntry := args.PrevLogIndex + len(args.Entries)
			if args.LeaderCommit > rf.commitIndex{
				rf.commitIndex = args.LeaderCommit
				if rf.commitIndex > indexOfLastOfNewEntry{
					rf.commitIndex = indexOfLastOfNewEntry
				}
				rf.applyCond.Broadcast()
			}
			DPrintf("[AppendEntries]: Id %d Term %d State %s\t||\tconsistency check pass for index %d" +
				" with args's prevLogIndex %d args's prevLogTerm %d\n", rf.me, rf.currentTerm, state2name(rf.state),
				indexOfLastOfNewEntry, args.PrevLogIndex, args.PrevLogTerm)

			rf.resetElectionTimer()
			rf.leaderId = args.LeaderId
			rf.persist()
			reply.Term = rf.currentTerm
			reply.Success = true
			rf.mu.Unlock()
			return
		}else{
			nextIndex := args.PrevLogIndex + 1
			index := nextIndex + len(args.Entries) - 1
			DPrintf("[AppendEntries]: Id %d Term %d State %s\t||\tconsistency check failed for index %d" +
				" with args's prevLogIndex %d args's prevLogTerm %d\n",
				rf.me, rf.currentTerm, state2name(rf.state), index, args.PrevLogIndex, args.PrevLogTerm)
			if len(rf.log) < nextIndex{
				reply.ConflictTerm = 0
				reply.ConflictFirstIndex = len(rf.log)
				DPrintf("[AppendEntries]: Id %d Term %d State %s\t||\tLog's len %d" +
					" is shorter than args's prevLogIndex %d\n",
					rf.me, rf.currentTerm, state2name(rf.state), len(rf.log), args.PrevLogIndex)
				DPrintf("[AppnedEntries]: Id %d Term %d State %s\t||\treply's conflictFirstIndex %d" +
					" and conflictTerm is None\n", rf.me, rf.currentTerm, state2name(rf.state), reply.ConflictFirstIndex)

			}else{
				reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
				reply.ConflictFirstIndex = args.PrevLogIndex
				DPrintf("[AppendEntries]: Id %d Term %d State %s\t||\tconsistency check failed" +
					" with args's prevLogIndex %d args's prevLogTerm %d while it's prevLogIndex %d in" +
					" prevLogTerm %d\n", rf.me, rf.currentTerm, state2name(rf.state),
					args.PrevLogIndex, args.PrevLogTerm, args.PrevLogIndex, rf.log[args.PrevLogIndex].Term)
				for i:= reply.ConflictFirstIndex - 1; i>=0; i--{
					if rf.log[i].Term != reply.ConflictTerm{
						break
					}else{
						reply.ConflictFirstIndex -= 1
					}
				}
				DPrintf("[AppendEntries]: Id %d Term %d State %s\t||\treply's conflictFirstIndex %d" +
					" and conflictTerm %d\n", rf.me, rf.currentTerm, state2name(rf.state),
					reply.ConflictFirstIndex, reply.ConflictTerm)
			}
		}
	}
	reply.Term = rf.currentTerm
	reply.Success = false
	rf.mu.Unlock()
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

//TODO: call timeout handle

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

func(rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool{
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Start
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
	if term, isLeader = rf.GetState(); isLeader{
		rf.mu.Lock()
		logEntry := LogEntry{Command: command, Term: rf.currentTerm}
		rf.log = append(rf.log, logEntry)
		index = len(rf.log) - 1
		DPrintf("[Start]: Id %d Term %d State %s\t||\treplicate the command to Log index %d\n",
			rf.me, rf.currentTerm, state2name(rf.state), index)
		nReplica := 1
		rf.persist()
		rf.mu.Unlock()

		rf.mu.Lock()
		rf.latestIssueTime = time.Now().UnixNano()
		go rf.broadcastAppendEntries(index, rf.currentTerm, rf.commitIndex, nReplica, "Start")
		rf.mu.Unlock()
	}
	return index, term, isLeader
}

// Kill
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

// Make
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
	rf.applyCh = applyCh
	rf.leaderId = -1
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.leaderCond = sync.NewCond(&rf.mu)
	rf.nonLeaderCond = sync.NewCond(&rf.mu)
	rf.heartbeatPeriod = 120
	rf.resetElectionTimer()
	rf.electionTimeoutChan = make(chan bool)
	rf.heartbeatPeriodChan = make(chan bool)

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.commitIndex = 0
	rf.lastApplied = 0

	size := len(rf.peers)
	rf.nextIndex = make([]int, size)
	rf.matchIndex = make([]int, size)

	rf.state = Follower

	go rf.electionTimeoutTick()
	go rf.heartbeatPeriodTick()
	go rf.eventLoop()
	go rf.applyEntries()

	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()


	return rf
}

func(rf *Raft) electionTimeoutTick(){
	for{
		if term, isLeader := rf.GetState(); isLeader{
			rf.mu.Lock()
			rf.nonLeaderCond.Wait()
			rf.mu.Unlock()
		}else{
			rf.mu.Lock()
			elapseTime := time.Now().UnixNano() - rf.latestHeardTime
			if int(elapseTime/int64(time.Millisecond)) >= rf.electionTimeout{
				_, _ = DPrintf("[ElectionTimeoutTick]: Id %d Term %d State %s\t||\ttimeout,"+
					" convert to Candidate\n", rf.me, term, state2name(rf.state))
				rf.electionTimeoutChan <- true
			}
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 10)
		}
	}
}

func(rf *Raft) heartbeatPeriodTick(){
	for{
		if term, isLeader := rf.GetState(); isLeader == false{
			rf.mu.Lock()
			rf.leaderCond.Wait()
			rf.mu.Unlock()
		}else{
			rf.mu.Lock()
			elapseTime := time.Now().UnixNano() - rf.latestIssueTime
			if int(elapseTime/int64(time.Millisecond)) >= rf.heartbeatPeriod{
				DPrintf("[HeartbeatPeriodTick]: Id %d Term %d State %s\t||\theartbeat period elapsed," +
					" issue heartbeat\n", rf.me, term, state2name(rf.state))
				rf.heartbeatPeriodChan <- true
			}
			rf.mu.Unlock()
			time.Sleep(time.Millisecond*10)
		}
	}
}

func(rf *Raft) eventLoop(){
	for{
		select{
		case <- rf.electionTimeoutChan:
			rf.mu.Lock()
			DPrintf("[EventLoop]: Id %d Term %d State %s\t||\telection timeout, start an election\n",
				rf.me, rf.currentTerm, state2name(rf.state))
			rf.mu.Unlock()
			go rf.startElection()
		case <- rf.heartbeatPeriodChan:
			rf.mu.Lock()
			DPrintf("[EventLoop]: Id %d Term %d State %s\t||\theartbeat period occurs, broadcast heartbeats\n",
				rf.me, rf.currentTerm, state2name(rf.state))
			rf.mu.Unlock()
			go rf.broadcastHeartbeat()
		}
	}
}

func(rf *Raft) startElection(){
	rf.mu.Lock()
	rf.switchTo(Candidate)
	rf.currentTerm += 1
	rf.votedFor = rf.me
	nVotes := 1
	rf.resetElectionTimer()

	rf.persist()
	DPrintf("[StartElection]: Id %d Term %d State %s\t||\tstart an election\n",
		rf.me, rf.currentTerm, state2name(rf.state))
	rf.mu.Unlock()

	go func(nVotes *int, rf *Raft){
		var wg sync.WaitGroup
		winThreshold := len(rf.peers)/2 + 1

		for i,_ := range rf.peers{
			if i == rf.me{
				continue
			}
			rf.mu.Lock()
			wg.Add(1)
			lastLogIndex := len(rf.log) -1
			if lastLogIndex < 0{
				DPrintf("[StartElection]: Id %d Term %d State %s\t||\tinvalid lastLogIndex %d\n",
					rf.me, rf.currentTerm, state2name(rf.state), lastLogIndex)
			}
			args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: lastLogIndex,
				LastLogTerm: rf.log[lastLogIndex].Term}
			DPrintf("[StartElection]: Id %d Term %d State %s\t||\tissue RequestVote RPC"+
				" to peer %d\n", rf.me, rf.currentTerm, state2name(rf.state), i)
			rf.mu.Unlock()
			var reply RequestVoteReply

			go func(i int, rf *Raft, args *RequestVoteArgs, reply *RequestVoteReply){
				defer wg.Done()
				ok := rf.sendRequestVote(i, args, reply)
				if ok == false{
					rf.mu.Lock()
					DPrintf("[StartElection]: Id %d Term %d State %s\t||\tsend RequestVote"+
						" Request to peer %d failed\n", rf.me, rf.currentTerm, state2name(rf.state), i)
					rf.mu.Unlock()
					return
				}
				if reply.VoteGranted == false{
					rf.mu.Lock()
					defer rf.mu.Unlock()
					DPrintf("[StartElection]: Id %d Term %d State %s\t||\tRequestVote is"+
						" rejected by peer %d\n", rf.me, rf.currentTerm, state2name(rf.state), i)

					if rf.currentTerm < reply.Term{
						DPrintf("[StartElection]: Id %d Term %d State %s\t||\tless than"+
							" peer %d Term %d\n", rf.me, rf.currentTerm, state2name(rf.state), i, reply.Term)
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.switchTo(Follower)
						rf.persist()
					}
				}else{
					rf.mu.Lock()
					DPrintf("[StartElection]: Id %d Term %d State %s\t||\tpeer %d grants vote\n",
						rf.me, rf.currentTerm, state2name(rf.state), i)
					*nVotes += 1
					DPrintf("[StartElection]: Id %d Term %d State %s\t||\tnVotes %d\n",
						rf.me, rf.currentTerm, state2name(rf.state), *nVotes)
					if rf.state == Candidate && *nVotes >= winThreshold{
						DPrintf("[StartElection]: Id %d Term %d State %s\t||\twin election with nVotes %d\n",
							rf.me, rf.currentTerm, state2name(rf.state), *nVotes)
						rf.switchTo(Leader)
						rf.leaderId = rf.me
						for i := 0; i<len(rf.peers); i++{
							rf.nextIndex[i] = len(rf.log)
						}
						go rf.broadcastHeartbeat()
						rf.persist()
					}
					rf.mu.Unlock()
				}
			}(i, rf, &args, &reply)
		}
		wg.Wait()
	}(&nVotes, rf)
}

func(rf *Raft) broadcastHeartbeat(){
	if _, isLeader := rf.GetState(); isLeader == false{
		return
	}
	rf.mu.Lock()
	rf.latestIssueTime = time.Now().UnixNano()
	rf.mu.Unlock()
	//TODO: need to know if necessary to lock twice
	rf.mu.Lock()
	index := len(rf.log)-1
	nReplica := 1
	go rf.broadcastAppendEntries(index, rf.currentTerm, rf.commitIndex, nReplica, "Broadcast")
	rf.mu.Unlock()
}

func(rf *Raft) broadcastAppendEntries(index int, term int, commitIndex int, nReplica int, name string){
	var wg sync.WaitGroup
	majority := len(rf.peers)/2 + 1
	isAgree := false

	if _, isLeader := rf.GetState(); isLeader == false{
		return
	}

	rf.mu.Lock()
	if rf.currentTerm != term{
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	rf.mu.Lock()
	DPrintf("[%s]: Id %d Term %d State %s\t||\tcreate an goroutine for index %d term %d commitIndex %d" +
		" to issue parallel and wait\n", name, rf.me, rf.currentTerm, state2name(rf.state), index, term, commitIndex)
	rf.mu.Unlock()

	for i, _ := range rf.peers{
		if i == rf.me{
			continue
		}
		wg.Add(1)
		go func(i int, rf *Raft){
			defer wg.Done()
		retry:
			if _, isLeader := rf.GetState(); isLeader == false{
				return
			}

			rf.mu.Lock()
			if rf.currentTerm != term{
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			rf.mu.Lock()
			nextIndex := rf.nextIndex[i]
			prevLogIndex := nextIndex - 1
			if prevLogIndex < 0 {
				DPrintf("[%s]: Id %d Term %d State %s\t||\tinvalid prevLogIndex %d for index %d" +
					" peer %d\n", name, rf.me, rf.currentTerm, state2name(rf.state), prevLogIndex, index, i)
			}
			prevLogTerm := rf.log[prevLogIndex].Term

			entries := make([]LogEntry, 0)
			if nextIndex < index+1{
				entries = rf.log[nextIndex:index+1]
			}

			if nextIndex > index + 1{
				DPrintf("[%s]: Id %d Term %d State %s\t||\tinvalid nextIndex %d while index %d\n",
					name, rf.me, rf.currentTerm, state2name(rf.state), nextIndex, index)
			}
			args := AppendEntriesArgs{
				Term:term,
				LeaderId:rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm: prevLogTerm,
				Entries: entries,
				LeaderCommit: commitIndex,
			}
			DPrintf("[%s]: Id %d Term %d State %s\t||\tissue AppendEntries RPC for index %d" +
				" to peer %d with commitIndex %d nextIndex %d\n", name, rf.me, rf.currentTerm, state2name(rf.state), index, i, commitIndex, nextIndex)
			rf.mu.Unlock()
			var reply AppendEntriesReply

			ok := rf.sendAppendEntries(i, &args, &reply)
			if ok == false{
				rf.mu.Lock()
				DPrintf("[%s]: Id %d Term %d State %s\t||\tissue AppendEntries RPC for index %d" +
					" to peer %d failed\n", name, rf.me, rf.currentTerm, state2name(rf.state), index, i)
				rf.mu.Unlock()
				return
			}

			rf.mu.Lock()
			if rf.currentTerm != args.Term{
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			if reply.Success == false{
				rf.mu.Lock()
				DPrintf("[%s]: Id %d Term %d State %s\t||\tAppendEntries RPC for index %d" +
					" is rejected by peer %d\n", name, rf.me, rf.currentTerm, state2name(rf.state), index, i)

				if args.Term < reply.Term{
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.switchTo(Follower)
					DPrintf("[%s]: Id %d Term %d State %s\t||\tAppendEntires RPC for index %d is rejected" +
						" by peer %d due to newer peer's term %d\n", name, rf.me, rf.currentTerm, state2name(rf.state),
						index, i, reply.Term)

					rf.persist()

					rf.mu.Unlock()
					return
				}else{
					nextIndex := rf.getNextIndex(reply, nextIndex)
					rf.nextIndex[i] = nextIndex
					DPrintf("[%s]: Id %d Term %d State %s\t||\tAppendEntries RPC for index %d is rejected by" +
						" peer %d due to the consistency check failed\n", name, rf.me, rf.currentTerm, state2name(rf.state),
						index, i)
					DPrintf("[%s]: Id %d Term %d State %s\t||\treply's conflictFirstIndex %d and conflictTerm %d\n",
						name, rf.me, rf.currentTerm, state2name(rf.state), reply.ConflictFirstIndex, reply.ConflictTerm)
					DPrintf("[%s]: Id %d Term %d State %s\t||\tretry AppendEntries RPC with nextIndex %d," +
						" so prevLogIndex %d and prevLogTerm %d\n", name, rf.me, rf.currentTerm, state2name(rf.state),
						nextIndex, nextIndex-1, rf.log[nextIndex-1].Term)
					rf.mu.Unlock()
					goto retry
				}
			}else {
				rf.mu.Lock()
				DPrintf("[%s]: Id %d Term %d State %s\t||\tsend AppendEntries RPC for index %d to peer %d success\n",
					name, rf.me, rf.currentTerm, state2name(rf.state), index, i)
				if rf.nextIndex[i] < index + 1{
					rf.nextIndex[i] = index + 1
					rf.matchIndex[i] = index
				}
				nReplica += 1
				DPrintf("[%s]: Id %d Term %d State %s\t||\tnReplica %d for index %d\n",
					name, rf.me, rf.currentTerm, state2name(rf.state), nReplica, index)
				if isAgree == false && rf.state == Leader && nReplica >= majority{
					isAgree = true
					DPrintf("[%s]: Id %d Term %d State %s\t||\thas replicated the entry with index %d" +
						" to the majority with nReplica %d\n", name, rf.me, rf.currentTerm, state2name(rf.state),
						index, nReplica)
					if rf.commitIndex < index && rf.log[index].Term == rf.currentTerm{
						DPrintf("[%s]: Id %d Term %d State %s\t||\tadvance the commitIndex to %d\n",
							name, rf.me, rf.currentTerm, state2name(rf.state), index)
						rf.commitIndex = index
						go rf.broadcastHeartbeat()
						rf.applyCond.Broadcast()
						DPrintf("[%s]: Id %d Term %d State %s\t||\tapply updated commitIndex %d to applyCh\n",
							name, rf.me, rf.currentTerm, state2name(rf.state), rf.commitIndex)
					}
				}
				rf.mu.Unlock()
			}
		}(i, rf)
	}
	wg.Wait()
}

func(rf *Raft) switchTo(newState int){
	oldState := rf.state
	rf.state = newState
	if oldState == Leader && newState == Follower{
		rf.nonLeaderCond.Broadcast()
	}else if oldState == Candidate && newState == Leader{
		rf.leaderCond.Broadcast()
	}
}

func(rf *Raft) resetElectionTimer(){
	rand.Seed(time.Now().UnixNano())
	rf.electionTimeout = rf.heartbeatPeriod * 5 + rand.Intn(300 - 150)
	rf.latestHeardTime = time.Now().UnixNano()
}

func(rf *Raft) getNextIndex(reply AppendEntriesReply, nextIndex int) int{
	if reply.ConflictTerm == 0{
		nextIndex = reply.ConflictFirstIndex
	}else{
		conflictIndex := reply.ConflictFirstIndex
		conflictTerm := rf.log[conflictIndex].Term

		if conflictTerm >= reply.ConflictTerm{
			for i := conflictIndex; i>0; i--{
				if rf.log[i].Term == reply.ConflictTerm{
					break
				}
				conflictIndex -= 1
			}
			if conflictIndex != 0{
				//TODO: Is this step necessary?
				for i := conflictIndex + 1; i<nextIndex; i++{
					if rf.log[i].Term != reply.ConflictTerm{
						break
					}
					conflictIndex += 1
				}
				nextIndex = conflictIndex + 1
			}else{
				nextIndex = reply.ConflictFirstIndex
			}

		}else{
			nextIndex = reply.ConflictFirstIndex
		}
	}
	return nextIndex
}

//func(rf *Raft) getNextIndex(reply AppendEntriesReply, nextIndex int) int{
//	if reply.ConflictTerm == 0{
//		nextIndex = reply.ConflictFirstIndex
//	}else{
//		nextIndex = reply.ConflictFirstIndex
//	}
//	return nextIndex
//}

func(rf *Raft) applyEntries(){
	for{
		rf.mu.Lock()
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		DPrintf("[applyEntries]: Id %d Term %d State %s\t||\tlastApplied %d and commitIndex %d\n",
			rf.me, rf.currentTerm, state2name(rf.state), lastApplied, commitIndex)
		rf.mu.Unlock()

		if lastApplied == commitIndex{
			rf.mu.Lock()
			rf.applyCond.Wait()
			rf.mu.Unlock()
		}else{
			for i := lastApplied + 1; i<=commitIndex; i++{
				rf.mu.Lock()
				applyMsg := ApplyMsg{CommandValid: true, Command: rf.log[i].Command, CommandIndex: i}
				rf.lastApplied = i
				DPrintf("[applyEntries]: Id %d Term %d State %s\t||\tapply command %v of index %d and term %d to applyCh\n",
					rf.me, rf.currentTerm, state2name(rf.state), applyMsg.Command, applyMsg.CommandIndex, rf.log[i].Term)
				rf.mu.Unlock()
				rf.applyCh <- applyMsg
			}
		}
	}
}

func state2name(state int) string{
	var name string
	if state == Follower{
		name = "follower"
	}else if state == Candidate{
		name = "candidate"
	}else if state == Leader{
		name = "leader"
	}
	return name
}
