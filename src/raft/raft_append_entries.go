package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictIndex int
	ConflictTerm  int
}

const (
	_continue int = iota
	_return
)

func (rf *Raft) makeHeartBeat(term, index int) AppendEntriesArgs {
	rf.lock("makeHeartBeat")
	prevIndex := rf.nextIndex[index] - 1
	if prevIndex < rf.lastIncludedIndex {
		prevIndex = rf.lastIncludedIndex
		go func() {
			rf.updateFollowerLogCh[index] <- 1
		}()
	}
	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PreLogIndex:  prevIndex,
		PreLogTerm:   rf.getLogByIndex(prevIndex).Term,
		Entries:      []LogEntry{},
		LeaderCommit: rf.commitIndex,
	}
	rf.unlock("makeHeartBeat")
	return args
}

func (rf *Raft) broadcastHeartBeats(term int) {
	for index := range rf.peers {
		select {
		case <-rf.killedCh:
			return
		case <-rf.stopChSendHB:
			return
		default:
		}
		if index != rf.me {
			go rf.broadcastHB(term, index)
		}
	}
}

func (rf *Raft) broadcastHB(term, index int) {
	go rf.sendHB(term, index)
	timer := time.NewTimer(rf.getHBTime())
	defer timer.Stop()
	for {
		timer.Reset(rf.getHBTime())
		select {
		case <-rf.killedCh:
			return
		case <-rf.stopChSendHB:
			return
		case <-rf.sendHBCh[index]:
			continue
		case <-timer.C:
			go rf.sendHB(term, index)
		}
	}
}

func (rf *Raft) getHBTime() time.Duration {
	return time.Millisecond * time.Duration(rf.heartBeatTime)
}

func (rf *Raft) sendHB(term, index int) {
	args := rf.makeHeartBeat(term, index)
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(index, &args, &reply)
	if !ok || args.Term != rf.currentTerm {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.changeRole(follower, reply.Term)
		rf.persist()
	}
	if !reply.Success {
		rf.updateFollowerLogCh[index] <- 1
	}
	return
}

// heartbeats: 100 ms
// election time elapse: 300~500 ms
func (rf *Raft) checkHeartBeats() {
	timer := time.NewTimer(rf.getElectionTime())
	defer timer.Stop()
	for {
		timer.Reset(rf.getElectionTime())
		select {
		case <-rf.killedCh:
			return
		case <-rf.stopChCheckHB:
			return
		case <-rf.heartBeatsCh:
			continue
		case <-timer.C:
			rf.changeRole(candidate, -1)
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock("AE")
	defer rf.unlock("AE")
	// rf.DPrintf("sr %v term %v with log %v receive AE %v", rf.me, rf.currentTerm, rf.log, *args)

	reply.Success = false
	reply.Term = rf.currentTerm
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1
	if args.Term < rf.currentTerm { // self is newer
		return
	}
	if args.Term > rf.currentTerm {
		rf.changeRole(follower, args.Term)
		rf.votedFor = -1
		rf.persist()
	}
	rf.heartBeatsCh <- 1
	if rf.getLastLogIndex() < args.PreLogIndex {
		reply.ConflictIndex = rf.getLastLogIndex() + 1
		reply.ConflictTerm = -1
		return
	}
	if args.PreLogIndex < rf.lastIncludedIndex {
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		return
	}
	if rf.getLogByIndex(args.PreLogIndex).Term != args.PreLogTerm {
		reply.ConflictTerm = rf.getLogByIndex(args.PreLogIndex).Term
		var i int
		for i = args.PreLogIndex - 1; i >= 0; i-- {
			if i < rf.lastIncludedIndex {
				break
			}
			if rf.getLogByIndex(i).Term != reply.ConflictTerm {
				break
			}
		}
		reply.ConflictIndex = i + 1
		return
	} else {
		endIndex := args.PreLogIndex + len(args.Entries)
		reply.Success = true
		if len(args.Entries) > 0 {
			if rf.getLastLogIndex() >= endIndex &&
				rf.getLogByIndex(endIndex).Term == args.Entries[len(args.Entries)-1].Term {
			} else {
				rf.log = append(rf.log[0:rf.convertIndex(args.PreLogIndex+1)], args.Entries...)
				rf.persist()
			}
		}
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit > endIndex {
				rf.commitIndex = endIndex
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			go func() {
				rf.checkAppliedCh <- 1
			}()
		}
		return
	}
}

func (rf *Raft) startUpdateFollowersLog(term int) {
	for index := range rf.peers {
		if index != rf.me {
			go rf.updateFollowerLog(index, term)
		}
	}
}

func (rf *Raft) updateFollowerLog(index, term int) {
	for {
		select {
		case <-rf.killedCh:
			return
		case <-rf.stopChUpdateFollowers:
			return
		case <-rf.updateFollowerLogCh[index]:
			// rf.DPrintf("leader %v last %v next %v match %v", rf.me, rf.getLastLogIndex(), rf.nextIndex, rf.matchIndex)
			if rf.getLastLogIndex() == rf.matchIndex[index] {
				continue
			}
			// rf.DPrintf("leader %v update %v", rf.me, index)
			for {
				select {
				case <-rf.killedCh:
					return
				case <-rf.stopChUpdateFollowers:
					return
				default:
				}
				rf.lock("updateFollower")
				if rf.nextIndex[index]-1 < rf.lastIncludedIndex {
					rf.unlock("updateFollower")
					break
				}
				prevLog := rf.getLogByIndex(rf.nextIndex[index] - 1)
				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     rf.me,
					PreLogIndex:  rf.nextIndex[index] - 1,
					PreLogTerm:   prevLog.Term,
					Entries:      []LogEntry{},
					LeaderCommit: rf.commitIndex,
				}
				rf.unlock("updateFollower")
				reply := AppendEntriesReply{}
				// rf.DPrintf("sr %v send check %v to %v", rf.me, args, index)
				for {
					select {
					case <-rf.killedCh:
						return
					case <-rf.stopChUpdateFollowers:
						return
					default:
					}
					rf.sendHBCh[index] <- 1
					ok := rf.sendAppendEntries(index, &args, &reply)
					if ok || term != rf.currentTerm {
						break
					}
				}
				if term != rf.currentTerm {
					return
				}
				if reply.Term > rf.currentTerm {
					rf.changeRole(follower, reply.Term)
					rf.votedFor = -1
					rf.persist()
					return
				}
				// rf.DPrintf("sr %v from %v %v", rf.me, index, reply)
				if reply.Success {
					break
				} else {
					rf.nextIndex[index] = rf.getNextIndex(reply.ConflictIndex, reply.ConflictTerm, rf.nextIndex[index])
				}
			}
			var control int
			if rf.nextIndex[index] > rf.lastIncludedIndex {
				control = rf.updateByAppendEntries(index, term)
			} else {
				control = rf.updateBySnapshot(index, term)
			}
			if control == _continue {
				continue
			} else if control == _return {
				return
			}
		}
	}
}

func (rf *Raft) updateByAppendEntries(index, term int) int {
	// rf.DPrintf("update by AE lastIn %v next %v", rf.lastIncludedIndex, rf.nextIndex[index])
	rf.lock("updateByAE")
	if rf.nextIndex[index] <= rf.lastIncludedIndex {
		go func() {
			rf.updateFollowerLogCh[index] <- 1
		}()
		return _continue
	}
	prevLog := rf.getLogByIndex(rf.nextIndex[index] - 1)
	lastLogIndex := rf.getLastLogIndex()
	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PreLogIndex:  rf.nextIndex[index] - 1,
		PreLogTerm:   prevLog.Term,
		Entries:      rf.getLogByIndexRange(rf.nextIndex[index], lastLogIndex+1),
		LeaderCommit: rf.commitIndex,
	}
	rf.unlock("updateByAE")
	reply := AppendEntriesReply{}
	rf.sendHBCh[index] <- 1
	ok := rf.sendAppendEntries(index, &args, &reply)
	if !ok || term != rf.currentTerm {
		return _continue
	}
	if reply.Term > rf.currentTerm {
		rf.changeRole(follower, reply.Term)
		rf.votedFor = -1
		rf.persist()
		return _return
	}
	if reply.Success {
		rf.lock("updateMatchIndex")
		rf.nextIndex[index] = lastLogIndex + 1
		rf.matchIndex[index] = lastLogIndex
		rf.unlock("updateMatchIndex")
		rf.checkCommitUpdateCh <- 1
		go func() {
			rf.updateFollowerLogCh[index] <- 1
		}()
	} else {
		panic("something wrong")
	}
	// rf.DPrintf("sr %v update %v finish", rf.me, index)
	return -1
}

func (rf *Raft) updateBySnapshot(index, term int) int {
	args := InstallSnapshotArgs{
		Term:              term,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	// rf.DPrintf("update by SS with %v", args)
	reply := InstallSnapshotReply{}
	rf.sendHBCh[index] <- 1
	ok := rf.sendInstallSnapshot(index, &args, &reply)
	if !ok || term != rf.currentTerm {
		return _continue
	}
	if reply.Term > rf.currentTerm {
		rf.changeRole(follower, reply.Term)
		rf.votedFor = -1
		rf.persist()
		return _return
	}
	rf.lock("updateMatchIndex")
	if args.LastIncludedIndex > rf.matchIndex[index] {
		rf.matchIndex[index] = args.LastIncludedIndex
	}
	if args.LastIncludedIndex > rf.nextIndex[index] {
		rf.nextIndex[index] = args.LastIncludedIndex + 1
	}
	rf.unlock("updateMatchIndex")
	go func() {
		rf.updateFollowerLogCh[index] <- 1
	}()
	// rf.DPrintf("sr %v update %v finish", rf.me, index)
	return -1
}

func (rf *Raft) getNextIndex(conflictIndex, conflictTerm, nextIndex int) int {
	rf.lock("getNextIndex")
	defer rf.unlock("getNextIndex")
	if conflictTerm == -1 {
		return conflictIndex
	}
	indexTerm := rf.getLogByIndex(conflictIndex).Term
	if indexTerm == conflictTerm {
		index := conflictIndex
		for ; index < nextIndex; index++ {
			if rf.getLogByIndex(index).Term != indexTerm {
				break
			}
		}
		return index
	} else if indexTerm > conflictTerm {
		index := conflictIndex
		for ; index > 0; index-- {
			if rf.getLogByIndex(index-1).Term == indexTerm {
				break
			}
		}
		if index == 0 {
			return conflictIndex
		}
		return index
	} else {
		return conflictIndex
	}
}

func (rf *Raft) checkCommitUpdate(term int) {
	for {
		select {
		case <-rf.killedCh:
			return
		case <-rf.stopChCommitUpdate:
			return
		case <-rf.checkCommitUpdateCh:
			// rf.lock("checkCommitUpdate")
			rf.DPrintf("[%v] check commit commitIndex %v, match %v lastIn %v", rf.me, rf.commitIndex, rf.matchIndex, rf.lastIncludedIndex)
			var i int
			lastLogIndex := rf.getLastLogIndex()
			for i = lastLogIndex; i > rf.commitIndex; i-- {
				matches := 0
				for j := 0; j < len(rf.peers); j++ {
					if rf.matchIndex[j] >= i {
						matches++
					}
				}
				if i <= rf.lastIncludedIndex {
					break
				}
				if matches > len(rf.peers)/2 && rf.getLogByIndex(i).Term == rf.currentTerm {
					break
				}
			}
			rf.commitIndex = i
			// rf.unlock("checkCommitUpdate")
			rf.triggerHB(term)
			rf.checkAppliedCh <- 1
		}
	}
}

func (rf *Raft) triggerUpdateFollowers() {
	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
				rf.updateFollowerLogCh[i] <- 1
			}(i)
		}
	}
}

func (rf *Raft) triggerHB(term int) {
	for i := range rf.peers {
		if i != rf.me {
			rf.sendHBCh[i] <- 1
			go rf.sendHB(term, i)
		}
	}
}
