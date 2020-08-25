package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		job := GetMapJobFromMaster()
		if len(job.filename) == 0 {
			break
		}
		// do map
		file, err := os.Open(job.filename)
		if err != nil {
			log.Fatalf("cannot open %v", job.filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", job.filename)
		}
		file.Close()
		kva := mapf(job.filename, string(content))
		// create reduce output files
		outFiles := make([]*os.File, job.nReduce)
		for i := 0; i < job.nReduce; i++ {
			fn := fmt.Sprintf("mr-%d-%d", job.taskNum, i)
			f, err := os.Create(fn)
			if err != nil {
				log.Fatalf("cannot open %v", fn)
			}
			outFiles[i] = f
		}
		// write all mapped results into separate buckets
		for _, kv := range kva {
			h := ihash(kv.Key)
			fmt.Fprintf(outFiles[h], "%v %v\n", kv.Key, kv.Value)
		}
		// close all files
		for _, f := range outFiles {
			if f.Close() != nil {
				log.Fatalf("failed to close %v", f.Name())
			}
		}
		// respond
		CompleteMapJob(job, true)
	}

	// uncomment to send the Example RPC to the master.
	// now receive reduce jobs
	for {
		job := GetReduceJobFromMaster()
		if len(job.filename) == 0 {
			break
		}
		// do reduce

	}
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// GetMapJobFromMaster get one map job from master, if possible
func GetMapJobFromMaster() MapJob {
	args := Empty{}
	reply := MapJob{}
	call("Master.GetMapJob", &args, &reply)
	return reply
}

// CompleteMapJob notifies the master that the job has been completed.
func CompleteMapJob(job MapJob, ok bool) {
	args := MapResponse{}
	if ok {
		args.code = OK
	} else {
		args.code = Fail
	}
	args.job = job
	reply := Empty{}
	call("Master.CompleteMapJob", &args, &reply)
}

// GetReduceJobFromMaster get one reduce job from master, if possible
func GetReduceJobFromMaster() ReduceJob {
	args := Empty{}
	reply := ReduceJob{}
	call("Master.GetReduceJob", &args, &reply)
	return reply
}

// CompleteReduceJob notifies the master that the job has been completed.
func CompleteReduceJob(job ReduceJob, ok bool) {
	args := ReduceResponse{}
	if ok {
		args.code = OK
	} else {
		args.code = Fail
	}
	args.job = job
	reply := Empty{}
	call("Master.CompleteReduceJob", &args, &reply)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
