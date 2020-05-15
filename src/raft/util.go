package raft

import "log"

// Debugging
// const Debug = 1

func (rf *Raft) DPrintf(format string, a ...interface{}) (n int, err error) {
	if rf.debug {
		log.Printf(format, a...)
	}
	return
}
