package raft

//
// RequestVote RPC handler.
//
// Didn't implement 'disregard RequestVote RPCs' mentioned in Section 6
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
	} else {
		if args.Term > rf.CurrentTerm {
			rf.CurrentTerm = args.Term
			rf.toFollower()
		}

		// The follower hasn't voted to any candidate yet
		if rf.VotedFor == -1 {
			lastIndex := len(rf.Logs) - 1
			lastTerm := rf.Logs[lastIndex].Term

			// Only grant the vote if candidate is at least as up-to-date
			if isUpToDate := args.LastLogTerm > lastTerm ||
				args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex; isUpToDate {
				// Reset the timer of leader election
				rf.resetTimer <- true
				rf.toFollower()
				rf.VotedFor = args.CandidateId
				reply.VoteGranted = true
			}
		}

		rf.persist()
	}
}

//
// AppendEntries RPC handler. Handles heartbeat and log replication for followers
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Should reject in this case
	if args.Term < rf.CurrentTerm {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		return
	}

	// Receive heartbeat from new leader
	if args.Term > rf.CurrentTerm || rf.State == Leader {
		rf.CurrentTerm = args.Term
		rf.toFollower()
		rf.persist()
	}
	rf.leaderId = args.LeaderId

	// Reset the timer of leader election
	rf.resetTimer <- true

	// Log Replication

	/*
		To speed up log replication, instead of decreasing one step at a time, we'll
		have followers to return where the conflict happened obeying the following rules:

		1. If a follower does not have prevLogIndex in its log, it should return with
			conflictIndex = len(log) and conflictTerm = None.

		2. If a follower does have prevLogIndex in its log, but the term does not match,
			it should return conflictTerm = log[prevLogIndex].Term, and then
			search its log for the FIRST index whose entry has term EQUAL to conflictTerm.

		3. Upon receiving a conflict response, the leader should first
			search its log for conflictTerm. If it finds an entry in its log with that term,
			it should set nextIndex to be the one beyond the index of the last entry
			in that term in its log (the index of entry of next term following the conflictTerm).

		4. If it does not find an entry with that term, it should set
			nextIndex = conflictIndex.
	*/

	// They may change later if there's no conflict
	reply.Success = false
	reply.ConflictTerm = 0
	reply.ConflictIndex = -1

	// Conflict index - there's no such entry
	if args.PrevLogIndex >= len(rf.Logs) {
		reply.ConflictIndex = len(rf.Logs)
		return
	}

	// Index exists but it's a conflict term
	if rf.Logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		conflictTerm := rf.Logs[args.PrevLogIndex].Term
		conflictIndex := args.PrevLogIndex - 1
		// Find the index of any term that is prior to the conflict term
		for ; rf.Logs[conflictIndex].Term == conflictTerm; conflictIndex-- {
		}
		reply.ConflictTerm = conflictTerm
		// Set ConflictIndex as first entry of conflict term, which will be replaced
		reply.ConflictIndex = conflictIndex + 1

		// Truncate all log entries after prevLogindex
		rf.Logs = rf.Logs[:args.PrevLogIndex]
		return
	}

	// At this point, all logs preceding PrevLogIndex are identical, but still
	// need to check existent entries that conflicts according to rule 3,
	// and truncate them accordingly.
	untrackedLogsStart := args.PrevLogIndex + 1
	untrackedLogs := rf.Logs[untrackedLogsStart:]
	if mustCheckConflict := len(args.Entries) > 0 && len(untrackedLogs) > 0; mustCheckConflict {
		mustTruncate := len(untrackedLogs) > len(args.Entries)
		for i := 0; !mustTruncate && i < len(untrackedLogs); i++ {
			hasTermConflict := untrackedLogs[i].Term != args.Entries[i].Term
			// Duplicate happens in unreliable network when receiving outdated heartbeats
			isDuplicate := untrackedLogs[i].LogIndex == args.Entries[i].LogIndex
			mustTruncate = hasTermConflict || isDuplicate
		}
		// Truncate all log entries after the last known log entry
		if mustTruncate {
			rf.Logs = rf.Logs[:untrackedLogsStart]
		}
	}

	// Now, it's safe to append new entries
	rf.Logs = append(rf.Logs, args.Entries...)

	reply.Success = true
	if args.LeaderCommit > rf.CommitIndex {
		oldCommitIdx := rf.CommitIndex
		rf.CommitIndex = Min(args.LeaderCommit, len(rf.Logs))
		if rf.CommitIndex > oldCommitIdx {
			// Need to apply log entries according to the new commitIndex
			go func() { rf.notifyApplyCh <- true }()
		}
	}

	rf.persist()
}
