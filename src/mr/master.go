package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Master struct {
	// Your definitions here.
	mapTasks []MapTask
	reduceTasks []ReduceTask
	nReduce int
	masterLock sync.Mutex
}

type MapTask struct {
	inputFile string
	state string
	taskNum int
	pid int
}

type ReduceTask struct{
	state string
	taskNum int
	pid int
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

func (m *Master) ReplyTaskCall(args *CallArgs, reply *CallReply) error{

	m.masterLock.Lock()
	defer m.masterLock.Unlock()

	for index, mapTask := range m.mapTasks{
		if mapTask.state == "idle"{
			reply.Type = "map"
			reply.FileName = mapTask.inputFile
			reply.TaskNum = mapTask.taskNum
			reply.NReduce = m.nReduce
			m.mapTasks[index].state = "working"
			m.mapTasks[index].pid = args.Pid
			go m.MonitorMapTask(&(m.mapTasks[index]))
			return nil
		}
	}
	for _, mapTask := range m.mapTasks{
		if mapTask.state == "working"{
			reply.Type = "wait"
			return nil
		}
	}

	for index, reduceTask := range m.reduceTasks{
		if reduceTask.state == "idle"{
			reply.Type = "reduce"
			reply.TaskNum = reduceTask.taskNum
			m.reduceTasks[index].state = "working"
			m.reduceTasks[index].pid = args.Pid
			go m.MonitorReduceTask(&(m.reduceTasks[index]))
			return nil
		}
	}

	return nil
}

func (m *Master) AcceptTaskDone(args *DoneArgs, reply *DoneReply) error{
	taskNum := args.TaskNum

	fmt.Println(args.Type)
	fmt.Println(args.TaskNum)

	m.masterLock.Lock()
	defer m.masterLock.Unlock()

	if args.Type == "map"{
		for index, mapTask := range m.mapTasks{
			if mapTask.taskNum == taskNum {
				if mapTask.pid == args.Pid {
					m.mapTasks[index].state = "done"
					reply.DoneConfirm = true
				}else{
					reply.DoneConfirm = false
				}

			}
		}
	}else if args.Type == "reduce"{
		for index, reduceTask := range m.reduceTasks{
			if reduceTask.taskNum == taskNum{
				if reduceTask.pid == args.Pid {
					m.reduceTasks[index].state = "done"
					reply.DoneConfirm = true
				}else {
					reply.DoneConfirm = false
				}
			}
		}
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
	for _,reduceTask := range m.reduceTasks{
		if reduceTask.state != "done"{
			return false
		}
	}

	return true
}

func (m *Master) MonitorMapTask(mapTask *MapTask){
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <- t.C:
			m.masterLock.Lock()
			mapTask.state = "idle"
			m.masterLock.Unlock()
			return
		default:
			if mapTask.state == "done"{
				return
			}
		}
	}
}
func (m *Master) MonitorReduceTask(reduceTask *ReduceTask){
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <- t.C:
			m.masterLock.Lock()
			reduceTask.state = "idle"
			m.masterLock.Unlock()
			return
		default:
			if reduceTask.state == "done"{
				return
			}
		}
	}
}


//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	undoFile:= make( map[string]bool)
	var mapTasks []MapTask
	var reduceTasks []ReduceTask
	for index, f := range files{
		mapTasks = append(mapTasks, MapTask{inputFile: f, state: "idle", taskNum: index})
		undoFile[f] = true
	}
	for i := 0; i < nReduce; i ++{
		reduceTasks = append(reduceTasks, ReduceTask{state: "idle", taskNum: i})
	}
	m := Master{mapTasks, reduceTasks, nReduce, sync.Mutex{}}

	// Your code here.


	m.server()
	return &m
}
