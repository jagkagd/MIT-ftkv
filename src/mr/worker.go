package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "sort"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []mr.KeyValue

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
	var id int
	if ok := call("Master.register", &arg, &id); !ok {
		log('Registration fail.')
		return
	}
	for {
		var job jobReply
		if ok := call("Master.getJob", &id, &job); !ok {
			continue
		}
		switch job.kind {
		case 'map':
			doMap(mapf, job)
			var reply id
			call("Master.mapFinished", &id, &reply)
		case 'reduce':
			doReduce(reducef, job)
			var reply id
			call("Master.reduceFinished", &id, &reply)
		}
	}
}

func domap(mapf func(string, string) []KeyValue, job jobReply) {
	intermediate := make([][]mr.KeyValue, job.reduceNum)
	for inter := range intermediate {
		inter = []mr.KeyValue{}
	}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	for key, value := range mapf(filename, string(content)) {
		reduceNo = ihash(key)
		intermediate[reduceNo] = append(intermediate[reduceNo], map[string]string{key: value})
	}
	var done sync.WaitGroup
	for inter := range intermediate {
		done.Add(1)
		go func(u &[]KeyValue) {
			defer done.Done()
			sort.Sort(ByKey(u))

			file, err := ioutil.TempFile("mr-tmp", fmt.Sprintf("mr-%v-%v-tmp", id, job.reduceNum))
			if err != nil {
				log.Fatal(err)
			}
			enc := json.NewEncoder(file)
			err := enc.Encode(u)
			if err != nil {
				log.Fatal(err)
			}
			file.Close()
			if err := tmpfile.Close(); err != nil {
				log.Fatal(err)
			}
			os.Rename()
		}(&inter)
	}
	done.Wait()
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
