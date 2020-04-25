package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

type TaskState struct {
	file string
	state string
	workerId int
}

type Master struct {
	// Your definitions here.
	mapTasks, reduceTasks []TaskState
	m sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) register(arg *TODO, reply *int) {
	m.Lock()
	reply = m.lastWorkerId + 1
	m.lastWorkerId++
	m.workersState[reply] = WorkerState{}
	m.UnLock()
	return nil
}
	

func (m *Master) dispatchJob(arg *int, job *JobReply) {
	m.Lock()
	if !m.mapDone() {
		kind = 'map'
		job.kind = kind
		job.reduceNum = m.reduceNum
	} else {
		kind = 'reduce'
		job.kind = kind
		job.mapIds = m.mapIds
	}
	job.TaskId = <-m.tasksCh[kind]
	m.UnLock()
	go func(taskId int) {
		time.Sleep(10*1000)
		m.Lock()
		if m.tasks[kind][TaskId].state != 'finish' {
			tasksCh[kind] <- TaskId
		}
		m.UnLock()
	}(job.taskId)
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
	ret := true

	// Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{

	}

	// Your code here.
		m.server()

	return &m
}
