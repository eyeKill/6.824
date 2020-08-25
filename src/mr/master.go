package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type JobStatus int32

const (
	notStarted JobStatus = iota
	scheduled
	running
	finished
	// timeout
)

// Timeout specifies the default timeout for worker to respond after the job is completed.
const Timeout = 15 * time.Second

type Master struct {
	// Your definitions here.
	nReduce             int
	nJob                int
	mapJobs             chan MapJob
	reduceJobs          chan ReduceJob
	closeLock           sync.Mutex
	closeCond           *sync.Cond
	mapJobStatus        []JobStatus
	reduceJobStatus     []JobStatus
	completedMapJobs    int32
	completedReduceJobs int32
}

func (m *Master) sleep(timeout time.Duration) bool {
	stopped := make(chan struct{})
	go func() {
		m.closeLock.Lock()
		m.closeCond.Wait()
		m.closeLock.Unlock()
		close(stopped)
	}()
	select {
	case <-stopped:
		return false
	case <-time.After(timeout):
		return true
	}
}

// Your code here -- RPC handlers for the worker to call.

// GetMapJob returns new map job, or empty job entry if there is no map job left.
func (m *Master) GetMapJob(args Empty, reply *MapJob) error {
	job, ok := <-m.mapJobs
	if ok {
		*reply = job
		m.mapJobStatus[job.TaskNum] = running
		go func() {
			if m.sleep(Timeout) == false {
				// stop signal received, close immediately
				return
			}
			// TODO use atomic functions here
			if m.mapJobStatus[job.TaskNum] == running {
				fmt.Printf("Map job %d failed, rescheduling...", job.TaskNum)
				m.mapJobs <- job
				m.mapJobStatus[job.TaskNum] = scheduled
			}
		}()
		return nil
	}
	return errors.New("no more map job")
}

var once sync.Once

// CompleteMapJob is called when worker completes one map job.
func (m *Master) CompleteMapJob(args *MapResponse, reply *Empty) error {
	if args.Code == OK {
		if m.mapJobStatus[args.Job.TaskNum] != running {
			log.Fatalf("Ran into some weird data race bug here, taskNum = %d\n", args.Job.TaskNum)
		}
		// mark it as finished
		m.mapJobStatus[args.Job.TaskNum] = finished
		atomic.AddInt32(&m.completedMapJobs, 1)
		if int(m.completedMapJobs) == m.nJob {
			close(m.mapJobs)
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
	job, ok := <-m.reduceJobs
	if ok {
		*reply = job
		// similar thing
		m.reduceJobStatus[job.TaskNum] = running
		go func() {
			if m.sleep(Timeout) == false {
				// stop signal received, close immediately
				return
			}
			// TODO use atomic functions here
			if m.reduceJobStatus[job.TaskNum] == running {
				fmt.Printf("Reduce job %d failed, rescheduling...", job.TaskNum)
				m.reduceJobs <- job
				m.reduceJobStatus[job.TaskNum] = scheduled
			}
		}()
		return nil
	}
	return errors.New("no more reduce job")
}

// CompleteReduceJob is called when worker completes one reduce job.
func (m *Master) CompleteReduceJob(args *ReduceResponse, reply *Empty) error {
	if args.Code == OK {
		if m.reduceJobStatus[args.Job.TaskNum] != running {
			log.Fatalf("Ran into some weird data race bug here, taskNum = %d\n", args.Job.TaskNum)
		}
		atomic.AddInt32(&m.completedReduceJobs, 1)
		if int(m.completedReduceJobs) == m.nReduce {
			close(m.reduceJobs)
			// also close every sleep goroutine
			m.closeCond.Broadcast()
		}
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
	nJob := len(files)
	m := Master{
		nReduce:    nReduce,
		nJob:       nJob,
		mapJobs:    make(chan MapJob, nJob),
		reduceJobs: make(chan ReduceJob, nReduce),
		// default is notStarted = 0
		mapJobStatus:        make([]JobStatus, nJob),
		reduceJobStatus:     make([]JobStatus, nReduce),
		completedMapJobs:    0,
		completedReduceJobs: 0,
	}
	m.closeLock = sync.Mutex{}
	m.closeCond = sync.NewCond(&m.closeLock)
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
