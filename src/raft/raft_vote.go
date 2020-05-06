package raft

import (
	"math/rand"
	"time"
	// "../labrpc"
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	term int
	candidatedId int
	lastLogIndex int
	lastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	term int
	voteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if args.term < rf.currentTerm {
		reply.term = rf.currentTerm
		reply.voteGranted = false
		rf.mu.Unlock()
		return
	}
	if rf.currentTerm < args.term {
		rf.currentTerm = args.term
		rf.changeRoleCh <- follower
	}
	if (rf.votedFor == -1 || rf.votedFor == args.candidatedId) && rf.getLastLogIndex() <= args.lastLogIndex {
		rf.mu.Unlock()
		reply.term = args.term
		reply.voteGranted = true
		return
	}
	rf.mu.Unlock()
	reply.term = args.term
	reply.voteGranted = false
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) getElectionTime() time.Duration {
	return time.Millisecond*(time.Duration)(rf.electionTimeRange[0]+rand.Intn(rf.electionTimeRange[1]-rf.electionTimeRange[0]))
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	args := RequestVoteArgs{
		term: rf.currentTerm,
		candidatedId: rf.me,
		lastLogIndex: rf.getLastLogIndex(),
		lastLogTerm: rf.currentTerm-1,
	}
	rf.mu.Unlock()
	votesCh := make(chan int, len(rf.peers))
	stopCh := make(chan int)
	go func() {
		for i := range votesCh {
			i++
			if i > len(rf.peers) {
				rf.changeRoleCh <- leader
				close(stopCh)
				return
			}
		}
	}()
	for index := range rf.peers {
		go func(index int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(index, &args, &reply)
			if reply.voteGranted {
				select {
				case <-stopCh:
					return
				default:
				}
				votesCh <- 1
			}
		}(index)
	}
}

func (rf *Raft) tryWinElection() {
	rf.stopChs["election"] = make(chan int)
	for {
		select {
		case <- rf.stopChs["election"]:
			return
		case <- time.After(rf.getElectionTime()):
			rf.startElection()
		}
	}
}