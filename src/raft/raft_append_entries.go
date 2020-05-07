package raft

import (
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
	args := rf.makeHeartBeat()
	for index := range rf.peers {
		if index != rf.me {
			go func(index int) {
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(index, &args, &reply)
			}(index)
		}
	}
}

// heartbeats: 100 ms
// election time elapse: 300~500 ms
func (rf *Raft) checkHeartBeats() {
	rf.stopChs["checkHB"] = make(chan int)
	for {
		select {
		case <-rf.stopChs["checkHB"]:
			return
		case <-rf.heartBeatsCh:
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
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	rf.heartBeatsCh <- 1
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.changeRoleCh <- follower
	}
	if args.PreLogIndex == rf.getLastLogIndex() && args.PreLogTerm == rf.getLastLogTerm() {
		reply.Success = true
		reply.Term = rf.currentTerm
		return
	}
	if rf.getLastLogIndex() < args.PreLogIndex {
		reply.Success = false
		return
	}
	if rf.log[args.PreLogIndex].Term != args.Term {
		rf.log = rf.log[:args.PreLogIndex-1]
		reply.Success = false
	}
	rf.log = append(rf.log, args.Entries...)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > rf.getLastLogIndex() {
			rf.commitIndex = rf.getLastLogIndex()
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.checkAppliedCh <- 1
	}
	return
}

func (rf *Raft) updateFollowersLog() {
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
		case <-rf.stopChs["updateFollowers"]:
			return
		case <-rf.updateFollowerLogCh[index]:
			if rf.getLastLogIndex() >= rf.nextIndex[index] {
				args := AppendEntriesArgs{
					Term: rf.currentTerm,
					LeaderId: rf.me,
					PreLogIndex: rf.getLastLogIndex(),
					PreLogTerm: rf.getLastLogTerm(),
					Entries: rf.log[rf.nextIndex[index]:],
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesReply{}
				rf.peers[index].Call("Raft.AppendEntries", &args, &reply)
				if reply.Success {
					rf.nextIndex[index] = rf.getLastLogIndex() + 1
					rf.matchIndex[index] = rf.getLastLogIndex()
					rf.checkCommitUpdateCh <- 1
				} else {
					rf.nextIndex[index]--
					rf.updateFollowerLogCh[index] <- 1
				}
			}
		}
	}
}

func (rf *Raft) checkCommitUpdate() {
	rf.stopChs["commitUpdate"] = make(chan int)
	for {
		select {
		case <-rf.stopChs["commitUpdate"]:
			return
		case <-rf.checkCommitUpdateCh:
			rf.mu.Lock()
			i := rf.commitIndex+1
			for {
				// TODO leader.matchindex
				matches := 0
				for j := 0; j < len(rf.peers); j++ {
					if rf.matchIndex[j] >= i {
						matches++
					}
				}
				if matches > len(rf.peers)/2 && rf.log[i].Term == rf.currentTerm {
					i++
				} else {
					break
				}
			}
			rf.commitIndex = i-1
			rf.checkAppliedCh <- 1
			rf.mu.Unlock()
		}
	}
}