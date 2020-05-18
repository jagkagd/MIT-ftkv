package raft

import (
	"sync"
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
	return time.Millisecond*(time.Duration)(rf.heartBeatTime)
}

func (rf *Raft) sendHB(term, index int) {
	rf.DPrintf("sr %v send HB to %v", rf.me, index)
	args := rf.makeHeartBeat(term)
	reply := AppendEntriesReply{}
	rf.sendAppendEntries(index, &args, &reply)
	if reply.Term > term {
		rf.changeRoleCh <- follower
		rf.currentTerm = reply.Term
	}
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
			rf.changeRoleCh <- candidate
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(args.Entries) > 0 {
		rf.DPrintf("sr %v term %v with log %v receive AE %v", rf.me, rf.currentTerm, rf.log, *args)
	}

	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm { // self is newer
		return
	}
	rf.heartBeatsCh <- 1
	if args.Term > rf.currentTerm {
		// log.Printf("sr %v flag 2", rf.me)
		rf.currentTerm = args.Term
		rf.changeRoleCh <- follower
		// log.Printf("sr %v flag 2.1", rf.me)
	}
	if rf.getLastLogIndex() < args.PreLogIndex {
		// log.Printf("sr %v flag 3.1", rf.me)
		return
	}
	if rf.getLogByIndex(args.PreLogIndex).Term != args.PreLogTerm {
		// log.Printf("sr %v flag 4", rf.me)
		rf.log = rf.getLogByIndexRange(0, args.PreLogIndex)
		// log.Printf("sr %v flag 4.1", rf.me)
		return
	} else {
		reply.Success = true
		if len(args.Entries) > 0 {
			rf.log = append(rf.getLogByIndexRange(0, args.PreLogIndex + 1), args.Entries...)
		} else {
			rf.log = rf.getLogByIndexRange(0, args.PreLogIndex + 1)
		}
		if args.LeaderCommit > rf.commitIndex {
			// log.Printf("sr %v flag 6.1", rf.me)
			if args.LeaderCommit > rf.getLastLogIndex() {
				rf.commitIndex = rf.getLastLogIndex()
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
			if rf.getLastLogIndex() >= rf.nextIndex[index] {
				mu := sync.Mutex{}
				mu.Lock()
				// log.Printf("leader %v next %v log %v", rf.me, rf.nextIndex, rf.log)
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
					ok := rf.peers[index].Call("Raft.AppendEntries", &args, &reply)
					// log.Printf("sr %v send check to %v 2", rf.me, index)
					if !ok {
						// rf.DPrintf("sr %v send %v to sr %v fail", rf.me, args, index)
						// log.Printf("rf.nextIndex %v", rf.nextIndex)
						continue
					}
					if reply.Term > term {
						rf.currentTerm = reply.Term
						rf.changeRoleCh <- follower
						return
					}
					if reply.Success {
						break
					} else {
						for ; rf.getLogByIndex(rf.nextIndex[index]-1).Term == prevLog.Term; {
							rf.nextIndex[index]--
						}
					}
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
				ok := rf.peers[index].Call("Raft.AppendEntries", &args, &reply)
				// log.Printf("sr %v send check to %v 4", rf.me, index)
				if !ok {
					// log.Printf("sr %v send %v to sr %v fail", rf.me, args, index)
					continue
				}
				if reply.Success {
					rf.nextIndex[index] = lastLogIndex + 1
					rf.matchIndex[index] = lastLogIndex
					rf.checkCommitUpdateCh <- 1
				} else {
					panic("something wrong")
				}
				mu.Unlock()
				rf.DPrintf("sr %v update %v finish", rf.me, index)
			}
		}
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