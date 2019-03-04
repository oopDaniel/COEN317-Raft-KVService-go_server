package raft

import "log"

func Min(a int, b int) int {
	if a > b {
		return b
	}
	return a
}
func Max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
