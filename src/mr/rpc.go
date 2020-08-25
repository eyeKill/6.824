package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type JobIdentifier string

// MapJob represents a map job. File content is retrieved by worker.
type MapJob struct {
	TaskNum  int
	Filename string
	NReduce  int
}

// ReduceJob represents a reduce job. File content is retrieved by worker.
type ReduceJob struct {
	TaskNum int
	NMapJob int
}

type ResponseCode int

// Response code
const (
	OK ResponseCode = iota
	Fail
)

type MapResponse struct {
	Code ResponseCode
	Job  MapJob
}

type ReduceResponse struct {
	Code ResponseCode
	Job  ReduceJob
}

// Empty represents the empty type, for handiness.
type Empty struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
