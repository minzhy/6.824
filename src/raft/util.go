package raft

import (
	"log"
)

// Debugging
const Debug1 = true

func DPrintf(format string, a ...interface{}) {
	if Debug1 {
		log.Printf(format, a...)
	}
}
