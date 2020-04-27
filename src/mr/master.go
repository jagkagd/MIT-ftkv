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
	taskId int
}

type TaskInfo WorkerState

type Master struct {
	// Your definitions here.
	tasksCh chan TaskInfo
	mapFiles []string
	tasksState map[string]([]bool)
	nReduce int
	lastWorkerId int
	workersState []WorkerState
	numRemainedTask map[string]int
	sync.Mutex
	numTasks map[string]int
	addReduce bool
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
	temp := <-m.tasksCh
	job.Kind = temp.kind
	job.TaskId = temp.taskId
	m.Lock()
	m.workersState[*arg] = WorkerState{temp.kind, temp.taskId}
	log.Printf("Dispatch job to worker %v with %v No. %v", *arg, job.Kind, job.TaskId)
	m.Unlock()

	go func(taskId int, kind string) {
		time.Sleep(time.Second)
		if m.tasksState[kind][taskId] != true {
			m.tasksCh <- TaskInfo{kind, taskId}
			log.Printf("%v task No. %v timeout.", kind, taskId)
		}
	}(job.TaskId, job.Kind)
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
	if m.numRemainedTask[kind] == 0 {
		return true
	} else {
		return false
	}
}

func (m *Master) WorkerFinished(arg *int, reply *int) error {
	m.Lock()
	temp := m.workersState[*arg]
	if m.tasksState[temp.kind][temp.taskId] == false {
		m.tasksState[temp.kind][temp.taskId] = true
		m.numRemainedTask[temp.kind]--
	}
	m.Unlock()
	log.Printf("%v task No. %v done", temp.kind, temp.taskId)
	*reply = 1
	if m.addReduce {
		return nil
	}
	m.Lock()
	if m.taskDone("map") && !m.addReduce {
		for i := 0; i < m.nReduce; i++ {
			m.tasksCh <- TaskInfo{"reduce", i}
		}
		m.addReduce = true
	}
	m.Unlock()
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
		tasksCh: make(chan TaskInfo, nMap+nReduce),
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
		numTasks: map[string]int{
			"map": nMap,
			"reduce": nReduce,
		},
	}
	for i := 0; i < nMap; i++ {
		m.tasksCh <- TaskInfo{"map", i}
	}
	// Your code here.
	m.server()
	log.Printf("Make Master")
	return &m
}
