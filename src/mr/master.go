package mr

import (
	"time"
	"log"
	"net"
	"os"
	"net/rpc"
	"net/http"
	"sync"
)

type WorkerState struct {
	kind string
	state string
	taskId int
}

type Master struct {
	// Your definitions here.
	tasksCh map[string](chan int)
	mapFiles []string
	tasksState map[string]([]bool)
	nReduce int
	lastWorkerId int
	workersState []WorkerState
	numRemainedTask map[string]int
	sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) Register(arg *int, reply *ConfigReply) error {
	m.Lock()
	defer m.Unlock()
	reply.WorkerId = m.lastWorkerId
	log.Printf("Worker No. %v register", reply.WorkerId)
	m.lastWorkerId++
	m.workersState = append(m.workersState, WorkerState{})
	reply.MapFiles = m.mapFiles
	reply.NReduce = m.nReduce
	return nil
}

func (m *Master) DispatchJob(arg *int, job *JobReply) error {
	log.Printf("In dispatchjob")
	m.Lock()
	log.Printf("In dispatchjob lock")
	var kind string
	if !m.taskDone("map") {
		kind = "map"
		job.Kind = kind
	} else {
		kind = "reduce"
		job.Kind = kind
	}
	job.TaskId = <-m.tasksCh[kind]
	m.workersState[*arg] = WorkerState{kind, "in processing", job.TaskId}
	log.Printf("Dispatch job to worker %v with %v No. %v", *arg, kind, job.TaskId)
	
	m.Unlock()
	log.Printf("Out dispatchjob")
	go func(taskId int, kind string) {
		time.Sleep(time.Second)
		m.Lock()
		defer m.Unlock()
		log.Printf("%v task states: %v", kind, m.tasksState[kind])
		if m.tasksState[kind][taskId] != true {
			m.tasksCh[kind] <- taskId
			log.Printf("%v task No. %v timeout.", kind, taskId)
		}
	}(job.TaskId, kind)
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
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
	return m.taskDone("reduce")
}

func (m *Master) taskDone(kind string) bool {
	m.Lock()
	log.Printf("in Done lock")
	defer m.Unlock()
	if m.numRemainedTask[kind] == 0 {
		return true
	} else {
		return false
	}
}

func (m *Master) WorkerFinished(arg *int, reply *int) error {
	m.Lock()
	defer m.Unlock()
	m.workersState[*arg].state = "finished"
	temp := m.workersState[*arg]
	m.tasksState[temp.kind][temp.taskId] = true
	m.numRemainedTask[temp.kind]--
	log.Printf("%v task No. %v done", temp.kind, temp.taskId)
	*reply = 1
	return nil
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	nMap := len(files)
	m := Master{
		tasksCh: map[string](chan int){
			"map": make(chan int, 2*nMap),
			"reduce": make(chan int, 2*nReduce),
		},
		mapFiles: files,
		tasksState: map[string]([]bool){
			"map": make([]bool, nMap),
			"reduce": make([]bool, nReduce),
		},
		lastWorkerId: 0,
		workersState: make([]WorkerState, 0),
		numRemainedTask: map[string]int{
			"map": nMap,
			"reduce": nReduce,
		},
		nReduce: nReduce,
	}
	for i := 0; i < nMap; i++ {
		m.tasksCh["map"] <- i
	}
	for i := 0; i < nReduce; i++ {
		m.tasksCh["reduce"] <- i
	}
	// Your code here.
	m.server()
	log.Printf("Make Master")
	return &m
}
