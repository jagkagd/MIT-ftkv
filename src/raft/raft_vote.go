package raft

import (
	// "sync"
	// "log"
	"math/rand"
	"time"
	// "../labrpc"
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.DPrintf("RV %v to %v term %v log %v", *args, rf.me, rf.currentTerm, rf.log)
	rf.lock("RV")
	rf.DPrintf("lock: RV %v to %v term %v log %v", *args, rf.me, rf.currentTerm, rf.log)
	defer rf.unlock("RV")
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		// log.Printf("server %v return false for args.Term %v < rf.currentTerm %v", rf.me, args.Term, rf.currentTerm)
		return
	}
	// log.Printf("sr %v currentTerm %v args.Term %v", rf.me, rf.currentTerm, args.Term)
	if rf.currentTerm < args.Term {
		// log.Printf("sr %v currentTerm %v < args.Term %v", rf.me, rf.currentTerm, args.Term)
		rf.votedFor = -1
		rf.changeRole(follower, args.Term)
		rf.persist()
		rf.DPrintf("sr %v change to follower2", rf.me)
	}
	var upToDate bool
	termDiff := rf.getLastLogTerm() - args.LastLogTerm
	switch {
	case termDiff < 0:
		upToDate = true
	case termDiff == 0:
		upToDate = rf.getLastLogIndex() <= args.LastLogIndex
	case termDiff > 0:
		upToDate = false
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		// log.Printf("server %v votedFor %v for request from server %v", rf.me, rf.votedFor, args.CandidateId)
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.heartBeatsCh <- 1
		rf.DPrintf("sr %v voted %v finish", rf.me, args.CandidateId)
		rf.persist()
		return
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// log.Printf("server %v return false for other reason: term %v votedFor %v", rf.me, rf.currentTerm, rf.votedFor)
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
	return time.Millisecond*time.Duration(rf.electionTimeRange[0]+rand.Intn(rf.electionTimeRange[1]-rf.electionTimeRange[0]))
}

func (rf *Raft) startElection() {
	rf.heartBeatsCh <- 1
	rf.lock("election")
	rf.currentTerm++
	term := rf.currentTerm
	rf.votedFor = rf.me
	rf.persist()
	// log.Printf("server %v start a election with term %v", rf.me, rf.currentTerm)
	args := RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm: rf.getLastLogTerm(),
	}
	rf.unlock("election")
	votesCh := make(chan int, len(rf.peers))
	votesCh <- 1
	// stopCh := make(chan int)
	getVotes := 0
	go func(term int, votesCh chan int, getVotes int) {
		for range votesCh {
			select {
			case <-rf.stopChElection:
				// close(stopCh)
				return
			default:
			}
			getVotes++
			if getVotes > len(rf.peers)/2 {
				// close(stopCh)
				close(rf.stopChElection)
				rf.changeRole(leader, term)
				return
			}
		}
	}(term, votesCh, getVotes)
	for index := range rf.peers {
		select {
		case <- rf.killedCh:
			return
		case <- rf.stopChElection:
			return
		default:
		}
		if index != rf.me {
			go func(term, index int, votesCh chan int) {
				reply := RequestVoteReply{}
				rf.DPrintf("sr %v send RV %v", rf.me, args)
				ok := rf.sendRequestVote(index, &args, &reply)
				rf.DPrintf("sr %v from %v RV reply %v", rf.me, index, reply)
				if !ok || term != rf.currentTerm {
					return
				}
				if reply.Term > rf.currentTerm {
					rf.changeRole(follower, reply.Term)
					rf.persist()
					return
				}
				if reply.VoteGranted {
					select {
					case <-rf.stopChElection:
						return
					default:
					}
					// log.Printf("server %v receives vote from server %v", rf.me, index)
					votesCh <- 1
				}
				return
			}(term, index, votesCh)
		}
	}
}

func (rf *Raft) tryWinElection() {
	// log.Printf("server %v try to win election", rf.me)
	timer := time.NewTimer(rf.getElectionTime())
	go rf.startElection()
	defer timer.Stop()
	for {
		timer.Reset(rf.getElectionTime())
		select {
		case <- rf.killedCh:
			return
		case <- rf.stopChElection:
			return
		case <- timer.C:
			go rf.startElection()
		}
	}
}