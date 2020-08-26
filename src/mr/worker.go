package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
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

type MapFunc func(string, string) []KeyValue
type ReduceFunc func(string, []string) string

//
// main/mrworker.go calls this function.
//
func Worker(mapf MapFunc, reducef ReduceFunc) {
	// query & run map jobs
	for {
		job, ok := GetMapJobFromMaster()
		if !ok {
			break
		}
		ok = doMap(job, mapf)
		CompleteMapJob(job, ok) // respond
	}

	// query & run reduce jobs
	for {
		job, ok := GetReduceJobFromMaster()
		if !ok {
			break
		}
		ok = doReduce(job, reducef)
		CompleteReduceJob(job, ok) // respond
	}
}

func doMap(job MapJob, mapf MapFunc) bool {
	file, err := os.Open(job.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", job.Filename)
	}
	content, err := ioutil.ReadAll(file)
	file.Close()
	if err != nil {
		log.Fatalf("cannot read %v", job.Filename)
	}
	kva := mapf(job.Filename, string(content))
	// create reduce output files
	outFiles := make([]*os.File, job.NReduce)
	for i := 0; i < job.NReduce; i++ {
		fn := fmt.Sprintf("mr-%d-%d", job.TaskNum, i)
		f, err := os.Create(fn)
		if err != nil {
			log.Fatalf("cannot open %v", fn)
		}
		outFiles[i] = f
	}
	// write all mapped results into separate buckets
	for _, kv := range kva {
		h := ihash(kv.Key) % job.NReduce
		fmt.Fprintf(outFiles[h], "%v %v\n", kv.Key, kv.Value)
	}
	// close all files
	for _, f := range outFiles {
		if f.Close() != nil {
			log.Fatalf("failed to close %v", f.Name())
		}
	}
	return true
}

func doReduce(job ReduceJob, reducef ReduceFunc) bool {
	values := make(map[string][]string)
	for i := 0; i < job.NMapJob; i++ {
		fn := fmt.Sprintf("mr-%d-%d", i, job.TaskNum)
		f, err := os.Open(fn)
		if err != nil {
			log.Fatalf("failed to open intermediate file %v", fn)
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			v := strings.Fields(scanner.Text())
			if len(v) != 2 {
				f.Close()
				log.Fatalf("invalid intermediate output: %v", scanner.Text())
			}
			values[v[0]] = append(values[v[0]], v[1])
		}
		f.Close()
	}
	// do reduce
	outFn := fmt.Sprintf("mr-out-%d", job.TaskNum)
	outFile, err := os.Create(outFn)
	defer outFile.Close()
	if err != nil {
		log.Fatalf("cannot open %v", outFn)
	}
	// In golang iterating over maps is randomized,
	// so we have to manually specify the permutation
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		result := reducef(k, values[k])
		fmt.Fprintf(outFile, "%v %v\n", k, result)
	}
	return true
}

// GetMapJobFromMaster get one map job from master, if possible.
func GetMapJobFromMaster() (job MapJob, ok bool) {
	log.Println("Inquiring map job from master...")
	args := Empty{}
	reply := MapJob{}
	ok = call("Master.GetMapJob", &args, &reply)
	return reply, ok
}

// CompleteMapJob notifies the master that the job has been completed.
func CompleteMapJob(job MapJob, ok bool) {
	log.Printf("Map job %d completed...\n", job.TaskNum)
	args := MapResponse{}
	if ok {
		args.Code = OK
	} else {
		args.Code = Fail
	}
	args.Job = job
	reply := Empty{}
	call("Master.CompleteMapJob", &args, &reply)
}

// GetReduceJobFromMaster get one reduce job from master, if possible.
func GetReduceJobFromMaster() (job ReduceJob, ok bool) {
	log.Println("Inquiring reduce job from master...")
	args := Empty{}
	reply := ReduceJob{}
	ok = call("Master.GetReduceJob", &args, &reply)
	return reply, ok
}

// CompleteReduceJob notifies the master that the job has been completed.
func CompleteReduceJob(job ReduceJob, ok bool) {
	log.Printf("Reduce job %d completed...", job.TaskNum)
	args := ReduceResponse{}
	if ok {
		args.Code = OK
	} else {
		args.Code = Fail
	}
	args.Job = job
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
