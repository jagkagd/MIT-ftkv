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
	debug bool
	killedCh chan int
	state ServerState
	currentTerm int
	votedFor int
	log []LogEntry
	
	commitIndex int
	lastApplied int
	applyCh chan ApplyMsg
	commandChs chan interface{}
	startFinish chan int

	nextIndex []int
	matchIndex []int
	updateFollowerLogCh [](chan int)

	electionTimeRange []int
	heartBeatTime int

	changeRoleCh chan ServerState
	heartBeatsCh chan int
	checkAppliedCh chan int
	checkCommitUpdateCh chan int
	stopChCheckHB chan int
	stopChElection chan int
	stopChSendHB chan int
	stopChUpdateFollowers chan int
	stopChCommitUpdate chan int
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
	term = rf.currentTerm
	isLeader = rf.state == leader
	rf.mu.Unlock()
	if isLeader {
		rf.commandChs <- command
		rf.broadCastHB(term)
		<-rf.startFinish
		defer rf.triggerUpdateFollowers()
		index = rf.getLastLogIndex()
		return index, term, isLeader
	}
	return rf.getLastLogIndex(), term, isLeader
}

func (rf *Raft) applyStart(term int) {
	var command interface{}
	for {
		command = <-rf.commandChs
		rf.log = append(rf.log, LogEntry{
			Command: command,
			Term: term,
		})
		rf.DPrintf("Start: sr %v gets %v index %v", rf.me, command, rf.getLastLogIndex())
		rf.matchIndex[rf.me] = rf.getLastLogIndex()
		rf.startFinish <- 1
	}
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
	close(rf.killedCh)
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
	rf.applyCh = applyCh
	rf.debug = false
	// log.Printf("Make a raft server No %v.", rf.me)

	// Your initialization code here (2A, 2B, 2C).
	rf.state = -1
	rf.votedFor = -1
	rf.log = []LogEntry{LogEntry{
		Term: 0,
		Command: struct{}{},
	}}
	rf.electionTimeRange = []int{400, 600}
	rf.heartBeatTime = 120

	rf.stopChCheckHB = make(chan int)
	rf.stopChElection = make(chan int)
	rf.stopChSendHB = make(chan int)
	rf.stopChUpdateFollowers = make(chan int)
	rf.stopChCommitUpdate = make(chan int)

	rf.heartBeatsCh = make(chan int)
	rf.killedCh = make(chan int)
	rf.checkAppliedCh = make(chan int)
	go rf.checkApplied()
	rf.changeRoleCh = make(chan ServerState)
	go rf.changeRole()
	rf.changeRoleCh <- follower

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) changeRole() {
	var role ServerState
	for {
		select {
		case <-rf.killedCh:
			return
		case role = <-rf.changeRoleCh:
			preRole := rf.state
			rf.DPrintf("server %v change role from %v to %v", rf.me, preRole, role)
			if preRole == role {
				continue
			}
			switch role {
			case follower:
				switch preRole {
				case candidate:
					close(rf.stopChElection)
				case leader:
					close(rf.stopChSendHB)
					close(rf.stopChUpdateFollowers)
					close(rf.stopChCommitUpdate)
				}
				go rf.checkHeartBeats()
			case candidate:
				switch preRole {
				case leader:
					continue
				}
				go rf.tryWinElection()
			case leader:
				switch preRole {
				case follower:
					continue
				}
				close(rf.stopChCheckHB)
				close(rf.stopChElection)
				// log.Printf("new leader %v log %v", rf.me, rf.log)
				go rf.sendHeartBeats(rf.currentTerm)
				rf.checkCommitUpdateCh = make(chan int)
				go rf.checkCommitUpdate()
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				rf.matchIndex[rf.me] = rf.getLastLogIndex()
				rf.updateFollowerLogCh = make([](chan int), len(rf.peers))
				for i := range rf.peers {
					rf.updateFollowerLogCh[i] = make(chan int)
				}
				rf.startUpdateFollowersLog(rf.currentTerm)
				for i := range rf.peers {
					if i != rf.me {
						rf.nextIndex[i] = rf.getLastLogIndex() + 1
					}
				}
				rf.triggerUpdateFollowers()
				rf.commandChs = make(chan interface{})
				rf.startFinish = make(chan int)
				go rf.applyStart(rf.currentTerm)
			}
			rf.state = role
		}
	}
}

func (rf *Raft) getLastLogIndex() int {
	if len(rf.log) == 0 {
		return 0
	}
	return len(rf.log)-1
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.getLogByIndex(-1).Term
}

func (rf *Raft) checkApplied() {
	for {
		select {
		case <-rf.killedCh:
			return
		case <-rf.checkAppliedCh:
			rf.mu.Lock()
			// log.Printf("server %v commitIndex %v lastApplied %v", rf.me, rf.commitIndex, rf.lastApplied)
			for ; rf.commitIndex > rf.lastApplied; {
				rf.lastApplied++
				appliedLog := rf.getLogByIndex(rf.lastApplied)
				applyMsg := ApplyMsg{
					Command: appliedLog.Command,
					CommandIndex: rf.lastApplied,
					CommandValid: true,
				}
				rf.applyCh <- applyMsg
				// log.Printf("Apply: sr %v log %v", rf.me, applyMsg)
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) convertIndex(i int) int {
	if i >= 0 {
		return i
	} 
	return len(rf.log)+i
}

func (rf *Raft) getLogByIndex(i int) LogEntry {
	mu := sync.Mutex{}
	mu.Lock()
	defer mu.Unlock()
	return rf.log[rf.convertIndex(i)]
}

func (rf *Raft) getLogByIndexRange(i, j int) []LogEntry {
	mu := sync.Mutex{}
	mu.Lock()
	defer mu.Unlock()
	ii := rf.convertIndex(i)
	jj := rf.convertIndex(j)
	if ii == jj {
		return []LogEntry{rf.log[ii]}
	} else if jj == -0 {
		return rf.log[ii:]
	} else if ii > jj {
		log.Fatalf("%v < %v", ii, jj)
		return []LogEntry{}
	} else {
		return rf.log[ii:jj]
	}
}