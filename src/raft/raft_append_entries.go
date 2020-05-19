package raft

import (
	// "sync"
	// "log"
	"time"
)

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PreLogIndex int
	PreLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool

	ConflictIndex int
	ConflictTerm int
}

func (rf *Raft) makeHeartBeat(term int) AppendEntriesArgs {
	return AppendEntriesArgs{
		Term: term,
		LeaderId: rf.me,
		PreLogIndex: rf.getLastLogIndex(),
		PreLogTerm: rf.getLastLogTerm(),
		Entries: []LogEntry{},
		LeaderCommit: rf.commitIndex,
	}
}

func (rf *Raft) broadcastHeartBeats(term int) {
	rf.DPrintf("sr %v begins send HB", rf.me)
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

func (rf *Raft) broadcastHB(term, index int)  {
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
		case <-timer.C:
			select {
			case <-rf.sendHBCh[index]:
				continue
			default:
			}
			go rf.sendHB(term, index)
		}
	}
}

func (rf *Raft) getHBTime() time.Duration {
	return time.Millisecond*time.Duration(rf.heartBeatTime)
}

func (rf *Raft) sendHB(term, index int) {
	// rf.DPrintf("sr %v send HB to %v", rf.me, index)
	args := rf.makeHeartBeat(term)
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
		// rf.nextIndex[index]--
		rf.updateFollowerLogCh[index] <- 1
	}
	return
}

// heartbeats: 100 ms
// election time elapse: 300~500 ms
func (rf *Raft) checkHeartBeats() {
	rf.DPrintf("sr %v start checkHB", rf.me)
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
			// rf.DPrintf("sr %v plan to change to candidate", rf.me)
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

	// if len(args.Entries) > 0 {
	rf.DPrintf("sr %v term %v with log %v receive AE %v", rf.me, rf.currentTerm, rf.log, *args)
	// }

	reply.Success = false
	reply.Term = rf.currentTerm
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1
	if args.Term < rf.currentTerm { // self is newer
		return
	}
	// rf.DPrintf("sr %v rec HB", rf.me)
	// rf.DPrintf("sr %v rec HB2", rf.me)
	if args.Term > rf.currentTerm {
		// log.Printf("sr %v flag 2", rf.me)
		rf.changeRole(follower, args.Term)
		rf.votedFor = -1
		rf.persist()
		// log.Printf("sr %v flag 2.1", rf.me)
	}
	rf.heartBeatsCh <- 1
	if rf.getLastLogIndex() < args.PreLogIndex {
		// log.Printf("sr %v flag 3.1", rf.me)
		reply.ConflictIndex = rf.getLastLogIndex() + 1
		reply.ConflictTerm = -1
		return
	}
	if rf.getLogByIndex(args.PreLogIndex).Term != args.PreLogTerm {
		// log.Printf("sr %v flag 4", rf.me)
		reply.ConflictTerm = rf.getLogByIndex(args.PreLogIndex).Term
		var i int
		for i = args.PreLogIndex-1; i >= 0; i-- {
			if rf.getLogByIndex(i).Term != reply.ConflictTerm {
				break
			}
		}
		reply.ConflictIndex = i + 1

		rf.log = rf.getLogByIndexRange(0, args.PreLogIndex)
		// log.Printf("sr %v flag 4.1", rf.me)
		rf.persist()
		return
	} else {
		endIndex := args.PreLogIndex + len(args.Entries)
		reply.Success = true
		if len(args.Entries) > 0 {
			if rf.getLastLogIndex() >= endIndex && 
			   rf.getLogByIndex(endIndex).Term == args.Entries[len(args.Entries)-1].Term {
				// log.Printf("already exists")
			} else {
				rf.log = append(rf.getLogByIndexRange(0, args.PreLogIndex + 1), args.Entries...)
				rf.persist()
			}
		}
		if args.LeaderCommit > rf.commitIndex {
			// log.Printf("sr %v flag 6.1", rf.me)
			if args.LeaderCommit > endIndex {
				rf.commitIndex = endIndex
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			go func(){
				rf.checkAppliedCh <- 1
			}()
		}
		// log.Printf("sr %v flag 7", rf.me)
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
			rf.DPrintf("leader %v lastLog %v nextIndex %v", rf.me, rf.getLastLogIndex(), rf.nextIndex)
			// if rf.getLastLogIndex() >= rf.nextIndex[index] {
			flag := false
			// rf.lock("updateFollower")
			// rf.DPrintf("leader %v send %v log %v", rf.me, index, rf.getLogByIndexRange(rf.nextIndex[index], -1))
			for {
				select {
				case <-rf.killedCh:
					return
				case <-rf.stopChUpdateFollowers:
					return
				default:
				}
				prevLog := rf.getLogByIndex(rf.nextIndex[index]-1)
				args := AppendEntriesArgs{
					Term: term,
					LeaderId: rf.me,
					PreLogIndex: rf.nextIndex[index]-1,
					PreLogTerm: prevLog.Term,
					Entries: []LogEntry{},
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesReply{}
				// log.Printf("sr %v send check to %v", rf.me, index)
				rf.sendHBCh[index] <- 1
				ok := rf.sendAppendEntries(index, &args, &reply)
				// log.Printf("sr %v send check to %v 2", rf.me, index)
				if !ok || term != rf.currentTerm {
					flag = true
					break
				}
				if reply.Term > rf.currentTerm {
					rf.changeRole(follower, reply.Term)
					rf.votedFor = -1
					rf.persist()
					return
				}
				rf.DPrintf("sr %v from %v %v", rf.me, index, reply)
				if reply.Success {
					break
				} else {
					rf.nextIndex[index] = rf.getNextIndex(reply.ConflictIndex, reply.ConflictTerm, rf.nextIndex[index])
				}
			}
			if flag {
				continue
			}
			prevLog := rf.getLogByIndex(rf.nextIndex[index]-1)
			lastLogIndex := rf.getLastLogIndex()
			args := AppendEntriesArgs{
				Term: term,
				LeaderId: rf.me,
				PreLogIndex: rf.nextIndex[index]-1,
				PreLogTerm: prevLog.Term,
				Entries: rf.getLogByIndexRange(rf.nextIndex[index], lastLogIndex+1),
				LeaderCommit: rf.commitIndex,
			}
			reply := AppendEntriesReply{}
			// log.Printf("sr %v send update log to sr %v with %v", rf.me, index, args)
			// log.Printf("sr %v send check to %v 3", rf.me, index)
			rf.sendHBCh[index] <- 1
			ok := rf.sendAppendEntries(index, &args, &reply)
			// log.Printf("sr %v send check to %v 4", rf.me, index)
			if !ok || term != rf.currentTerm {
				continue
			}
			if reply.Term > rf.currentTerm {
				rf.changeRole(follower, reply.Term)
				rf.votedFor = -1
				rf.persist()
				return
			}
			if reply.Success {
				rf.lock("updateMatchIndex")
				rf.nextIndex[index] = lastLogIndex + 1
				rf.matchIndex[index] = lastLogIndex
				rf.unlock("updateMatchIndex")
				rf.checkCommitUpdateCh <- 1
			} else {
				panic("something wrong")
			}
			// mu.unlock("updateFollower")
			rf.DPrintf("sr %v update %v finish", rf.me, index)
		// }
		}
	}
}

func(rf *Raft) getNextIndex(conflictIndex, conflictTerm, nextIndex int) int {
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

func (rf *Raft) checkCommitUpdate() {
	for {
		select {
		case <-rf.killedCh:
			return
		case <-rf.stopChCommitUpdate:
			return
		case <-rf.checkCommitUpdateCh:
			rf.DPrintf("sr %v check commit update commitIndex %v, match %v", rf.me, rf.commitIndex, rf.matchIndex)
			rf.mu.Lock()
			var i int
			for i = rf.getLastLogIndex(); i > rf.commitIndex; i-- {
				matches := 0
				for j := 0; j < len(rf.peers); j++ {
					if rf.matchIndex[j] >= i {
						matches++
					}
				}
				if matches > len(rf.peers)/2 && rf.getLogByIndex(i).Term == rf.currentTerm {
					break
				}
			}
			// log.Printf("new commitIndex %v", i)
			rf.commitIndex = i
			rf.triggerHB(rf.currentTerm)
			// go rf.broadcastHeartBeats(rf.currentTerm)
			rf.mu.Unlock()
			rf.checkAppliedCh <- 1
		}
	}
}

func (rf *Raft) triggerUpdateFollowers() {
	for i := range rf.peers {
		if i != rf.me {
			go func(i int){
				rf.updateFollowerLogCh[i] <- 1
				// log.Printf("sr %v trigger %v", rf.me, i)
			}(i)
		}
	}
}

func (rf *Raft) triggerHB(term int) {
	for i := range rf.peers {
		if i != rf.me {
			go rf.sendHB(term, i)
		}
	}
}