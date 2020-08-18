package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) SavePersistAndSnapshot(logIndex int, data []byte) {
	rf.lock("SavePersistAndSnapshot")
	defer rf.unlock("SavePersistAndSnapshot")

	if logIndex <= rf.lastIncludedIndex {
		return
	}
	// rf.DPrintf("origin log %v", rf.log)
	rf.log = rf.log[rf.convertIndex(logIndex):] // log[0] for guard
	rf.lastIncludedIndex = logIndex
	rf.lastIncludedTerm = rf.getLogByIndex(logIndex).Term
	if rf.lastApplied < rf.lastIncludedIndex {
		rf.lastApplied = rf.lastIncludedIndex
	}
	rf.DPrintf("[%v] lastIncludedIndex %v lastTerm %v log %v", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.log)
	stateData := rf.getRaftState()
	rf.persister.SaveStateAndSnapshot(stateData, data)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.lock("IS")
	defer rf.unlock("IS")

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm { // self is newer
		return
	}
	if args.Term > rf.currentTerm {
		rf.changeRole(follower, args.Term)
		rf.votedFor = -1
		rf.persist()
	}
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}
	if rf.getLastLogIndex() >= args.LastIncludedIndex {
		return
	}
	if args.LastIncludedIndex > rf.lastIncludedIndex {
		if rf.getLastLogIndex() > args.LastIncludedIndex {
			rf.log = rf.getLogByIndexRange(args.LastIncludedIndex, -1)
		} else {
			rf.log = []LogEntry{
				LogEntry{
					Term:    args.LastIncludedTerm,
					Command: 0,
				},
			}
		}
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.lastApplied = args.LastIncludedIndex
	rf.persister.SaveStateAndSnapshot(rf.getRaftState(), args.Data)
	rf.DPrintf("[%v] IS lastIncludedIndex %v", rf.me, rf.lastIncludedIndex)
	go func() {
		rf.applyCh <- ApplyMsg{
			Command:      "InstallSnapshot",
			CommandIndex: rf.lastApplied,
			CommandValid: false,
		}
	}()
}
