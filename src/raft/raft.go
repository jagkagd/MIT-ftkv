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
	"log"
	"sync"
	"sync/atomic"
	"../labrpc"
)

// import "bytes"
// import "../labgob"



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

type LogEntry struct {
	Term int
	Index int
	Command interface{}
}
//
// A Go object implementing a single Raft peer.
//

type ServerState int

const (
	follower ServerState = iota
	candidate
	leader
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	killedCh chan int
	state ServerState
	currentTerm int
	votedFor int
	log []LogEntry
	
	commitIndex int
	lastApplied int
	applyCh chan ApplyMsg

	nextIndex []int
	matchIndex []int
	updateFollowerLogCh [](chan int)

	electionTimeRange []int
	heartBeatTime int

	changeRoleCh chan ServerState
	changeTermCh chan int
	heartBeatsCh chan int
	checkAppliedCh chan int
	checkCommitUpdateCh chan int
	stopChs map[string](chan int)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == leader
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
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//


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
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.state == leader
	if isLeader {
		rf.log = append(rf.log, LogEntry{
			Command: command,
			Term: rf.currentTerm,
			Index: rf.getLastLogIndex() + 1,
		})
		rf.matchIndex[rf.me] = rf.getLastLogIndex()
	}

	return index, term, isLeader
}

//
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
	log.Printf("Make a raft server No %v.", rf.me)

	// Your initialization code here (2A, 2B, 2C).
	rf.state = -1
	rf.log = []LogEntry{}
	rf.electionTimeRange = []int{300, 500}
	rf.heartBeatTime = 100
	rf.stopChs = map[string](chan int){
		"checkHB": make(chan int),
		"election": make(chan int),
		"sendHB": make(chan int),
		"updateFollowers" : make(chan int),
		"commitUpdate": make(chan int),
	}
	rf.changeRoleCh = make(chan ServerState)
	rf.changeTermCh = make(chan int)
	rf.heartBeatsCh = make(chan int)
	rf.killedCh = make(chan int)
	go rf.changeRole()
	rf.changeRoleCh <- follower
	rf.checkAppliedCh = make(chan int)
	go rf.checkApplied()
	rf.checkCommitUpdateCh = make(chan int)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) changeRole() {
	for role := range rf.changeRoleCh {
		preRole := rf.state
		rf.state = role
		log.Printf("server %v change role from %v to %v", rf.me, preRole, role)
		if preRole == role {
			continue
		}
		switch preRole {
		case leader:
			close(rf.stopChs["sendHB"])
			close(rf.stopChs["updateFollowers"])
			close(rf.stopChs["commitUpdate"])
		}

		switch role {
		case follower:
			switch preRole {
			case candidate:
				close(rf.stopChs["election"])
			}
			go rf.checkHeartBeats()
		case candidate:
			go rf.tryWinElection()
		case leader:
			close(rf.stopChs["checkHB"])
			close(rf.stopChs["election"])
			go rf.sendHeartBeats()
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			rf.updateFollowerLogCh = make([](chan int), len(rf.peers))
			go rf.updateFollowersLog()
			for i := range rf.nextIndex {
				if i != rf.me {
					rf.nextIndex[i] = rf.getLastLogIndex() + 1
					rf.matchIndex[i] = 0
					rf.updateFollowerLogCh[i] <- 1
				}
			}
		}
	}
}

func (rf *Raft) checkApplied() {
	for {
		<-rf.checkAppliedCh
		if rf.commitIndex > rf.lastApplied {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.getLastLogIndex(),
				Command: rf.log[rf.getLastLogIndex()].Command,
			}
			rf.lastApplied++
			rf.checkAppliedCh <- 1
		}
	}
}


func (rf *Raft) getLastLogIndex() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].Term
}