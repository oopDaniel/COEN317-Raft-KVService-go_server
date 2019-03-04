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
	"labrpc"
	"math/rand"
	"sort"
	"sync"
	"time"
)

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	leaderId          int           // Id of current lead
	electionTimer     *time.Timer   // Timer for leader election, different in peers
	resetTimer        chan bool     // Chan to reset electionTimer if received a heartbeat
	heartbeatInterval time.Duration // When reached, start an election
	State             ServerState   // Leader, Candidate, or Follower

	// Persistent state on all servers
	CurrentTerm int        // Current term of this server (initialized to 0)
	VotedFor    int        // CandidateId that received vote in current term (null if none)
	Logs        []LogEntry // Log entries; each entry contains command for state machine, and term when entry was received by leader

	// Volatile state on all servers
	CommitIndex int // Index of highest log entry known to be committed (initialized to 0
	LastApplied int // index of highest log entry applied to state machine (initialized to 0)

	// Volatile state on leaders
	NextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	MatchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0)

	applyCh       chan ApplyMsg // channel to notify tester(service) that log has been committed and ready to apply
	notifyApplyCh chan bool     // channel to have daemon notify applyCh
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.CurrentTerm
	isLeader := rf.State == Leader
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)

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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var logs []LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Logs = logs
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	_, isLeader := rf.GetState()

	if isLeader {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		term = rf.CurrentTerm
		isLeader = true
		index = len(rf.Logs)

		log := LogEntry{
			Term:     term,
			LogIndex: index,
			Command:  command,
		}
		rf.Logs = append(rf.Logs, log)

		rf.NextIndex[rf.me] = index + 1
		rf.MatchIndex[rf.me] = index

		rf.persist()
	}

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

	rf.leaderId = -1
	rf.electionTimer = time.NewTimer(getRandElectionTimeout())
	rf.resetTimer = make(chan bool)
	rf.heartbeatInterval = 40 * time.Millisecond

	rf.toFollower()
	rf.CurrentTerm = 0
	rf.Logs = []LogEntry{LogEntry{0, 0, nil}} // Placeholder. Log entry starts from 1

	rf.CommitIndex = 0
	rf.LastApplied = 0

	peerLen := len(rf.peers)
	rf.NextIndex = make([]int, peerLen)
	rf.MatchIndex = make([]int, peerLen)

	rf.applyCh = applyCh
	rf.notifyApplyCh = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.leaderElectionDaemon()
	go rf.applyLogToStateDaemon()
	return rf
}

// Daemon to start leader election if failed to receive heartbeat from latest leader
func (rf *Raft) leaderElectionDaemon() {
	for {
		select {
		case <-rf.resetTimer:
			if !rf.electionTimer.Stop() {
				<-rf.electionTimer.C // Force timeout by draining the value
			}
			rf.electionTimer.Reset(getRandElectionTimeout())
		case <-rf.electionTimer.C:
			go rf.proposeElection()
			// This reset ensures propose new round of election if the current round failed
			rf.electionTimer.Reset(getRandElectionTimeout())
		}
	}
}

// Start leader election; vote for self
func (rf *Raft) proposeElection() {
	rf.mu.Lock()

	rf.toCandidate()
	peers := len(rf.peers)
	index := len(rf.Logs) - 1
	voteArgs := RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: index,
		LastLogTerm:  rf.Logs[index].Term,
	}

	rf.mu.Unlock()

	majority := peers/2 + 1
	votes := 1

	for i := 0; i < peers; i++ {
		go func(n int) {
			if n != rf.me {
				var reply RequestVoteReply
				if rf.sendRequestVote(n, &voteArgs, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					if rf.State == Candidate { // Still a candidate
						if reply.Term > voteArgs.Term { // Found a higher term; become follower
							rf.CurrentTerm = reply.Term
							rf.toFollower()
							rf.persist()
							rf.resetTimer <- true
							return
						}
						// May receive reply from previous terms because of latency
						if reply.VoteGranted && rf.CurrentTerm == voteArgs.Term {
							votes++
							if votes == majority && rf.State != Leader {
								rf.State = Leader
								rf.resetIndex()
								go rf.appendEntriesDaemon()
								return
							}
						}
					}

				}
			}
		}(i)
	}
}

// Initialize NextIndex and MatchIndex for any new elected leader.
// Should be called when holding lock.
func (rf *Raft) resetIndex() {
	logLen := len(rf.Logs)
	for i := 0; i < len(rf.peers); i++ {
		rf.NextIndex[i] = logLen
		rf.MatchIndex[i] = 0
	}
	rf.MatchIndex[rf.me] = logLen - 1
}

// Call appendEntries periodically for heartbeats or log replication. Leader only.
func (rf *Raft) appendEntriesDaemon() {
	for {
		// No longer a leader
		if _, isLeader := rf.GetState(); !isLeader {
			return
		}
		// Reset leader's own timer
		rf.resetTimer <- true

		// Send heartbeat to all the peers
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go rf.appendEntriesToFollowers(i)
			}
		}
		// Sleep until next interval
		time.Sleep(rf.heartbeatInterval)
	}
}

func (rf *Raft) appendEntriesToFollowers(follower int) {
	// Cound find a follower who has higher term and turn to follower
	if _, isLeader := rf.GetState(); !isLeader {
		return
	}

	rf.mu.Lock()

	newLogIndex := rf.NextIndex[follower] // Index of new entries sending to follower
	prevLogIndex := newLogIndex - 1       // Index just preceding the new entries. Aka. last known log of follower
	args := AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.Logs[prevLogIndex].Term,
		Entries:      []LogEntry{},
		LeaderCommit: rf.CommitIndex,
	}

	// Some logs are missing in the follower's side
	if newLogIndex < len(rf.Logs) {
		args.Entries = rf.Logs[newLogIndex:]
	}

	rf.mu.Unlock()

	var reply AppendEntriesReply
	if rf.sendAppendEntries(follower, &args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		// NOTE: May receive a reply from a long time ago
		if rf.State != Leader {
			return
		}
		if reply.Success {
			commitIndexOfFollower := args.PrevLogIndex + len(args.Entries)
			rf.MatchIndex[follower] = commitIndexOfFollower
			rf.NextIndex[follower] = commitIndexOfFollower + 1
			rf.updateCommitIndex()
		} else if reply.Term > rf.CurrentTerm {
			// Found a follower with higher term. Convert to follower.
			rf.toFollower()
			rf.persist()
			rf.resetTimer <- true

		} else {
			// There's a conflict. Adopt speed up log replication (see step 3 in `raft_handlers.go`)
			nextIndex := len(rf.Logs) - 1 // search begins from the end of all logs

			if reply.ConflictTerm != 0 {
				for ; nextIndex >= 0 && rf.Logs[nextIndex].Term != reply.ConflictTerm; nextIndex-- {
				}
				// No matching conflict term, use conflict index
				if nextIndex == -1 {
					// NextIndex will be at least 1. May receive duplicate `reply.ConflictIndex` after updated `rf.NextIndex[follower]`
					rf.NextIndex[follower] = Max(1, Min(reply.ConflictIndex, rf.NextIndex[follower]))
				} else {
					rf.NextIndex[follower] = nextIndex + 1
				}
			} else {
				// No conflict term but has conflict index
				rf.NextIndex[follower] = Max(1, Min(reply.ConflictIndex, rf.NextIndex[follower]))
			}
		}
	}
}

func (rf *Raft) applyLogToStateDaemon() {
	for {
		select {
		case <-rf.notifyApplyCh:
			rf.mu.Lock()

			lastApplied, commitIdx := rf.LastApplied, rf.CommitIndex
			diff := commitIdx - lastApplied
			logs := make([]LogEntry, diff)
			// Skip the first placeholding log entry
			copy(logs, rf.Logs[lastApplied+1:lastApplied+diff+1])

			rf.LastApplied = commitIdx

			rf.mu.Unlock()
			for i := 0; i < diff; i++ {
				msg := ApplyMsg{
					CommandValid: true,
					Command:      logs[i].Command,
					CommandIndex: lastApplied + i + 1,
				}
				rf.applyCh <- msg
			}
		}
	}
}

// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
// and log[N].term == currentTerm: set commitIndex = N.
// Should be called when holding lock
func (rf *Raft) updateCommitIndex() {
	matchIndex := make([]int, len(rf.MatchIndex))
	copy(matchIndex, rf.MatchIndex)
	sort.Ints(matchIndex)

	oldCommitIdx := rf.CommitIndex
	newCommitIdx := matchIndex[len(rf.peers)/2]
	// NOTE: Leader can only update commit index of CURRENT term
	if newCommitIdx > oldCommitIdx && rf.Logs[newCommitIdx].Term == rf.CurrentTerm {
		rf.CommitIndex = newCommitIdx
		go func() { rf.notifyApplyCh <- true }()
	}
}

func (rf *Raft) toCandidate() {
	rf.VotedFor = rf.me
	rf.CurrentTerm++
	rf.State = Candidate
	return
}

func (rf *Raft) toFollower() {
	rf.VotedFor = -1
	rf.State = Follower
	return
}

// Randomly return 500 - 900 ms
func getRandElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Millisecond * time.Duration(rand.Intn(5)*100+500)
}
