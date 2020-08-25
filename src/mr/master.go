package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
)

type Master struct {
	// Your definitions here.
	nReduce             int
	nJob                int
	mapJobs             chan MapJob
	reduceJobs          chan ReduceJob
	completedMapJobs    int32
	completedReduceJobs int32
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// GetMapJob returns new map job, or empty job entry if there is no map job left.
func (m *Master) GetMapJob(args Empty, reply *MapJob) error {
	job, closed := <-m.mapJobs
	if !closed {
		*reply = job
	} else {
		reply = &MapJob{TaskNum: -1}
	}
	return nil
}

var once sync.Once

// CompleteMapJob is called when worker completes one map job.
func (m *Master) CompleteMapJob(args *MapResponse, reply *Empty) error {
	if args.Code == OK {
		atomic.AddInt32(&m.completedMapJobs, 1)
		if int(m.completedMapJobs) == m.nJob {
			// get into reduce phase
			once.Do(func() {
				for i := 0; i < m.nReduce; i++ {
					m.reduceJobs <- ReduceJob{
						TaskNum: i,
						NMapJob: int(m.nJob),
					}
				}
			})
		}
	} else {
		m.mapJobs <- args.Job // retry
	}
	return nil
}

// GetReduceJob returns new reduce job, or empty job entry if there is no reduce job left.
func (m *Master) GetReduceJob(args *Empty, reply *ReduceJob) error {
	job, closed := <-m.reduceJobs
	if !closed {
		*reply = job
	} else {
		*reply = ReduceJob{TaskNum: -1}
	}
	return nil
}

// CompleteReduceJob is called when worker completes one reduce job.
func (m *Master) CompleteReduceJob(args *ReduceResponse, reply *Empty) error {
	if args.Code == OK {
		atomic.AddInt32(&m.completedReduceJobs, 1)
	} else {
		m.reduceJobs <- args.Job
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	return m.completedReduceJobs == int32(m.nReduce)
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		nReduce:             nReduce,
		nJob:                len(files),
		mapJobs:             make(chan MapJob, len(files)),
		reduceJobs:          make(chan ReduceJob, nReduce),
		completedMapJobs:    0,
		completedReduceJobs: 0,
	}
	// enqueue all map jobs
	for i, f := range files {
		m.mapJobs <- MapJob{
			TaskNum:  i,
			Filename: f,
			NReduce:  m.nReduce,
		}
	}
	log.Println("Master init completed, begin to serve...")
	m.server()
	return &m
}
