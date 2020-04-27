package mr

import (
	"encoding/json"
	"sync"
	"io/ioutil"
	"os"
	"fmt"
	"log"
	"net/rpc"
	"hash/fnv"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	// Your worker implementation here.
	log.Printf("Worker start.")
	var config ConfigReply
	arg := 1
	call("Master.Register", &arg, &config)
	id := config.WorkerId
	for {
		var job JobReply
		log.Printf("Ask for a job")
		call("Master.DispatchJob", &id, &job)
		var reply int
		switch job.Kind {
		case "map":
			doMap(mapf, job, config)
			call("Master.WorkerFinished", &id, &reply)
		case "reduce":
			doReduce(reducef, job, config)
			call("Master.WorkerFinished", &id, &reply)
		}
	}
}

func doMap(mapf func(string, string) []KeyValue, job JobReply, config ConfigReply) {
	intermediate := make([][]KeyValue, config.NReduce)
	for i := range intermediate {
		intermediate[i] = make([]KeyValue, 0)
	}
	filename := config.MapFiles[job.TaskId]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	for _, kv := range mapf(filename, string(content)) {
		reduceNo := ihash(kv.Key) % config.NReduce
		intermediate[reduceNo] = append(intermediate[reduceNo], kv)
	}
	var done sync.WaitGroup
	for ii := 0; ii < len(intermediate); ii++ {
		done.Add(1)
		go func(i int) {
			defer done.Done()
			sort.Sort(ByKey(intermediate[i]))

			tmpFile := fmt.Sprintf("mr-%v-%v-tmp", job.TaskId, i)
			file, err := ioutil.TempFile("", tmpFile)
			if err != nil {
				log.Fatal(err)
			}
			enc := json.NewEncoder(file)
			err = enc.Encode(intermediate[i])
			if err != nil {
				log.Fatal(err)
			}
			if err = file.Close(); err != nil {
				log.Fatal(err)
			}
			os.Rename(fmt.Sprintf("%v", tmpFile), fmt.Sprintf("mr-%v-%v", job.TaskId, i))
		}(ii)
	}
	done.Wait()
}

func doReduce(reducef func(string, []string) string, job JobReply, config ConfigReply) {
	kva := make([]KeyValue, 0)
	for mapId := 0; mapId < len(config.MapFiles); mapId++ {
		filename := fmt.Sprintf("mr-%v-%v", mapId, job.TaskId)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			log.Fatalf("cannot write %v", filename)
			break
		}
		kva = append(kva, kv)
	}
	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("mr-out-%v", job.TaskId)
	ofile, _ := os.Create(oname)
	lock := sync.Mutex{}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		go func(i int) {
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := reducef(kva[i].Key, values)

			lock.Lock()
			fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
			defer lock.Unlock()
		}(i)

		i = j
	}
	ofile.Close()
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
