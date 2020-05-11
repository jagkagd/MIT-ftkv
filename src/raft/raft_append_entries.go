package raft

import (
	"sync"
	"log"
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

func (rf *Raft) makeHeartBeat() AppendEntriesArgs {
	return AppendEntriesArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
		PreLogIndex: rf.getLastLogIndex(),
		PreLogTerm: rf.getLastLogTerm(),
		Entries: []LogEntry{},
		LeaderCommit: rf.commitIndex,
	}
}

func (rf *Raft) sendHeartBeats() {
	log.Printf("server %v begins send HB", rf.me)
	rf.stopChs["sendHB"] = make(chan int)
	go rf.broadCastHB()
	for {
		select {
		case <-rf.killedCh:
			return
		case <-rf.stopChs["sendHB"]:
			return
		case <-time.After(rf.getHBTime()):
			go rf.broadCastHB()
		}
	}
}

func (rf *Raft) getHBTime() time.Duration {
	return time.Millisecond*(time.Duration)(rf.heartBeatTime)
}

func (rf *Raft) broadCastHB() {
	log.Printf("server %v send HB with matchIndex %v", rf.me, rf.matchIndex)
	args := rf.makeHeartBeat()
	for index := range rf.peers {
		select {
		case <-rf.stopChs["sendHB"]:
			return
		default:
		}
		if index != rf.me {
			go func(index int) {
				reply := AppendEntriesReply{}
				// log.Printf("server %v send HB to %v index", rf.me, index)
				rf.sendAppendEntries(index, &args, &reply)
				if reply.Term > rf.currentTerm {
					rf.changeRoleCh <- follower
					rf.currentTerm = reply.Term
				}
			}(index)
		}
	}
}

// heartbeats: 100 ms
// election time elapse: 300~500 ms
func (rf *Raft) checkHeartBeats() {
	log.Printf("server %v start checkHB", rf.me)
	rf.stopChs["checkHB"] = make(chan int)
	for {
		select {
		case <-rf.killedCh:
			return
		case <-rf.stopChs["checkHB"]:
			return
		case <-rf.heartBeatsCh:
			// log.Printf("server %v get HB", rf.me)
			continue
		case <-time.After(rf.getElectionTime()):
			log.Printf("server %v doesn't get HB", rf.me)
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
	log.Printf("server %v receive AE %v", rf.me, *args)
	reply.Success = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm { // self is newer
		return
	}
	rf.heartBeatsCh <- 1
	// log.Printf("server %v flag 2", rf.me)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.changeRoleCh <- follower
	}
	// log.Printf("server %v flag 3", rf.me)
	if rf.getLastLogIndex() < args.PreLogIndex {
		// log.Printf("server %v flag 3.1", rf.me)
		return
	}
	// log.Printf("server %v flag 4", rf.me)
	if rf.getLogByIndex(args.PreLogIndex).Term != args.PreLogTerm {
		rf.log = rf.getLogByIndexRange(1, args.PreLogIndex)
		// log.Printf("server %v flag 4.1", rf.me)
		return
	}
	// log.Printf("server %v flag 5", rf.me)
	reply.Success = true
	rf.log = append(rf.log, args.Entries...)
	// log.Printf("server %v flag 6", rf.me)
	if args.LeaderCommit > rf.commitIndex {
		// log.Printf("server %v flag 6.1", rf.me)
		if args.LeaderCommit > rf.getLastLogIndex() {
			rf.commitIndex = rf.getLastLogIndex()
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.checkAppliedCh <- 1
	}
	// log.Printf("server %v flag 7", rf.me)
	return
}

func (rf *Raft) startUpdateFollowersLog() {
	rf.stopChs["updateFollowers"] = make(chan int)
	for index := range rf.peers {
		if index != rf.me {
			go rf.updateFollowerLog(index)
		}
	}
}

func (rf *Raft) updateFollowerLog(index int) {
	for {
		select {
		case <-rf.killedCh:
			return
		case <-rf.stopChs["updateFollowers"]:
			return
		case <-rf.updateFollowerLogCh[index]:
			if rf.getLastLogIndex() >= rf.nextIndex[index] {
				mu := sync.Mutex{}
				mu.Lock()
				log.Printf("leader last log index %v, server %v next index %v", rf.getLastLogIndex(), index, rf.nextIndex[index])
				log.Printf("leader logs %v", rf.log)
				log.Printf("leader send log %v", rf.getLogByIndexRange(rf.nextIndex[index], -1))
				for {
					prevLog := rf.getLogByIndex(rf.nextIndex[index]-1)
					args := AppendEntriesArgs{
						Term: rf.currentTerm,
						LeaderId: rf.me,
						PreLogIndex: prevLog.Index,
						PreLogTerm: prevLog.Term,
						Entries: []LogEntry{},
						LeaderCommit: rf.commitIndex,
					}
					reply := AppendEntriesReply{}
					rf.peers[index].Call("Raft.AppendEntries", &args, &reply)
					if reply.Success {
						break
					} else {
						rf.nextIndex[index]--
					}
				}
				prevLog := rf.getLogByIndex(rf.nextIndex[index]-1)
				args := AppendEntriesArgs{
					Term: rf.currentTerm,
					LeaderId: rf.me,
					PreLogIndex: prevLog.Index,
					PreLogTerm: prevLog.Term,
					Entries: rf.getLogByIndexRange(rf.nextIndex[index], -1),
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesReply{}
				rf.peers[index].Call("Raft.AppendEntries", &args, &reply)
				if reply.Success {
					rf.nextIndex[index] = rf.getLastLogIndex() + 1
					rf.matchIndex[index] = rf.getLastLogIndex()
					rf.checkCommitUpdateCh <- 1
				} else {
					panic("something wrong")
				}
				mu.Unlock()
			}
		}
	}
}

func (rf *Raft) checkCommitUpdate() {
	rf.stopChs["commitUpdate"] = make(chan int)
	for {
		select {
		case <-rf.killedCh:
			return
		case <-rf.stopChs["commitUpdate"]:
			return
		case <-rf.checkCommitUpdateCh:
			log.Printf("Check Commit Update with commitIndex %v, match %v", rf.commitIndex, rf.matchIndex)
			rf.mu.Lock()
			i := rf.commitIndex+1
			for {
				matches := 0
				for j := 0; j < len(rf.peers); j++ {
					if rf.matchIndex[j] >= i {
						matches++
					}
				}
				if matches > len(rf.peers)/2 && rf.getLogByIndex(i).Term == rf.currentTerm {
					i++
				} else {
					break
				}
			}
			log.Printf("new commitIndex %v", i-1)
			rf.commitIndex = i-1
			rf.checkAppliedCh <- 1
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) triggerUpdateFollowers() {
	for i := range rf.peers {
		if i != rf.me {
			rf.updateFollowerLogCh[i] <- 1
		}
	}
}