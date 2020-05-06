package raft

import (
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
	rf.stopChs["sendHB"] = make(chan int)
	for {
		select {
		case <-rf.stopChs["sendHB"]:
			return
		case <-time.After(rf.getHBTime()):
			rf.broadCastHB()
		}
	}
}

func (rf *Raft) getHBTime() time.Duration {
	return time.Millisecond*(time.Duration)(rf.heartBeatTime)
}

func (rf *Raft) broadCastHB() {
	args := rf.makeHeartBeat()
	for index := range rf.peers {
		go func(index int) {
			reply := AppendEntriesReply{}
			rf.sendAppendEntries(index, &args, &reply)
		}(index)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.mu.Unlock()
		rf.changeRoleCh <- follower
	}
	if args.PreLogIndex == rf.getLastLogIndex() && args.PreLogTerm == rf.getLastLogTerm() {
		reply.Success = true
		reply.Term = rf.currentTerm
		return
	}
	reply.Term = rf.currentTerm
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	rf.mu.Lock()
	if rf.getLastLogIndex() < args.PreLogIndex {
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	rf.mu.Lock()
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
	rf.mu.Unlock()
	return
}

func (rf *Raft) updateFollowersLog() {
	rf.stopChs["updateFollowers"] = make(chan int)
	for index := range rf.peers {
		go rf.updateFollowerLog(index)
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
				} else {
					rf.nextIndex[index]--
					rf.updateFollowerLogCh[index] <- 1
				}
			}
		}
	}
}