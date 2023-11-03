package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Map functions return a slice of KeyValue.
type TaskStatus int

const (
	TaskStatusPending TaskStatus = iota
	TaskStatusAssigned
	TaskStatusFinish
)

type TaskType int

const (
	TaskTypeMap TaskType = iota
	TaskTypeReduce
	TaskTypeWait
)

type Task struct {
	Idx       int
	Filenames []string
	Status    TaskStatus
	Type      TaskType
	StartedAt time.Time
}

type Coordinator struct {
	mu          sync.Mutex
	nReduce     int
	mapTasks    []Task
	reduceTasks []Task
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.observe()
	reply.Ok = false

	now := time.Now()

	for i := 0; i < len(c.mapTasks); i++ {
		task := &c.mapTasks[i]
		if task.Status == TaskStatusPending {
			task.Status = TaskStatusAssigned
			task.StartedAt = now
			reply.Ok = true
			reply.NReduce = c.nReduce
			reply.Task = task
			return
		}
	}

	for _, task := range c.mapTasks {
		if task.Status != TaskStatusFinish {
			task := &Task{Type: TaskTypeWait}
			reply.Ok = true
			reply.NReduce = c.nReduce
			reply.Task = task
			return
		}
	}
	for i := 0; i < len(c.reduceTasks); i++ {
		task := &c.reduceTasks[i]
		if task.Status == TaskStatusPending {
			task.Status = TaskStatusAssigned
			task.StartedAt = now
			reply.Ok = true
			reply.NReduce = c.nReduce
			reply.Task = task
			return
		}
	}

	return
}

func (c *Coordinator) TaskFinish(args *TaskFinishArgs, reply *TaskFinishReply) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.OK = false

	switch args.Type {
	case TaskTypeMap:
		if args.Idx < 0 || args.Idx >= len(c.mapTasks) {
			return
		}

		task := &c.mapTasks[args.Idx]
		task.Status = TaskStatusFinish

		if len(args.ReduceFilenames) == c.nReduce {
			for i, filename := range args.ReduceFilenames {
				task := &c.reduceTasks[i]
				task.Filenames = append(task.Filenames, filename)
			}
		}

		reply.OK = true
	case TaskTypeReduce:
		if args.Idx < 0 || args.Idx >= len(c.reduceTasks) {
			return
		}

		task := &c.reduceTasks[args.Idx]
		task.Status = TaskStatusFinish

		reply.OK = true
	}

	return
}

const timeout = 10 * time.Second

func (c *Coordinator) observe() {
	ticker := time.NewTicker(1 * time.Second)

	go func() {
		for t := range ticker.C {
			c.mu.Lock()

			for i := 0; i < len(c.mapTasks); i++ {
				task := &c.mapTasks[i]
				if task.Status == TaskStatusAssigned && t.Sub(task.StartedAt) >= timeout {
					task.Status = TaskStatusPending
					task.StartedAt = time.Time{}
				}
			}

			for i := 0; i < len(c.reduceTasks); i++ {
				task := &c.reduceTasks[i]
				if task.Status == TaskStatusAssigned && t.Sub(task.StartedAt) >= timeout {
					task.Status = TaskStatusPending
					task.StartedAt = time.Time{}
				}
			}

			c.mu.Unlock()
		}
	}()
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, task := range c.mapTasks {
		if task.Status != TaskStatusFinish {
			return false
		}
	}

	for _, task := range c.reduceTasks {
		if task.Status != TaskStatusFinish {
			return false
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{nReduce: nReduce}

	// init map tasks
	for i, filename := range files {
		c.mapTasks = append(c.mapTasks, Task{
			Idx:       i,
			Filenames: []string{filename},
			Status:    TaskStatusPending,
			Type:      TaskTypeMap,
		})
	}

	// init reduce tasks
	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, Task{
			Idx:       i,
			Filenames: []string{},
			Status:    TaskStatusPending,
			Type:      TaskTypeReduce,
		})
	}

	c.server()
	c.observe()
	return &c
}
