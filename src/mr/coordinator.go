package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

type Coordinator struct {
	// Your definitions here.
	// the task has 3 states, 0 -> unstart, 1 -> start but not finished, 2 -> finished
	// do map task , wait for all map task finish  , do reduce task (read maps file) , end 
	// 0 - map, 1 - , 2 - reduce 
	filename 	 []string
	NReduce      int
	Num 		 int

	state    	 int 
	stmu		 sync.RWMutex  // rwlock will be better	

	mapfinish 	 []int
	maplast 	 []time.Time
	mapmutex	 []sync.Mutex

	mapnum 		 int
	mapmu		 sync.Mutex

	reducefinish []int
	reducelast   []time.Time
	reducemutex  []sync.Mutex

	reducenum 	 int
	reducemu 	 sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Real(args *RealArgs, reply* RealReply) error{
	num := len(c.filename)
	c.stmu.RLock()
	defer c.stmu.RUnlock()
	if c.state == 3 {
		reply.State = 3
		return nil
	}
	if c.state == 0 {
		// do map task
		for i, value := range c.filename {
			c.mapmutex[i].Lock()
			defer c.mapmutex[i].Unlock()
			if c.mapfinish[i] == 0 {
				c.mapfinish[i] = 1;
				reply.Id = i
				reply.Nfile = num
				reply.NReduce = c.NReduce
				reply.Filename = value
				reply.State = c.state
				c.maplast[i] = time.Now()
				return nil
			}
			// give the task to other worker 
			if c.mapfinish[i] == 1 {
				if time.Now().Sub(c.maplast[i]) >= 10 * time.Second {
					c.mapfinish[i] = 1;
					reply.Id = i
					reply.Nfile = num
					reply.NReduce = c.NReduce
					reply.Filename = value
					reply.State = c.state
					c.maplast[i] = time.Now()
					return nil				
				}
			}
		}
		reply.State = 1//睡眠等待
		return nil
	}
	//do reduce task
	for  i := 0; i < c.NReduce; i++ {
		c.reducemutex[i].Lock()
		defer c.reducemutex[i].Unlock()
		if c.reducefinish[i] == 0 {
			c.reducefinish[i] = 1;
			reply.Id = i
			reply.Nfile = num
			reply.NReduce = c.NReduce
			reply.State = 2
			c.reducelast[i] = time.Now()
			return nil
		}
		// give the task to other worker 
		if c.reducefinish[i] == 1 {
			if time.Now().Sub(c.reducelast[i]) >= 10 * time.Second {
				c.reducefinish[i] = 1;
				reply.Id = i
				reply.Nfile  = num
				reply.NReduce = c.NReduce
				reply.State = 2
				c.reducelast[i] = time.Now()
				return nil			
			}
		}
	}
	reply.State = 1
	return nil
}

// if do map and reduce in a func Unlock that with defer 
// it will has some problems
func (c *Coordinator) solvemap(id int)	error{
	c.mapmutex[id].Lock()
	defer c.mapmutex[id].Unlock()
	if c.mapfinish[id] != 2 {
		c.mapfinish[id] = 2
		c.mapmu.Lock()
		defer c.mapmu.Unlock()
		c.mapnum++
		if c.mapnum == len(c.filename) {
			c.stmu.Lock()
			defer c.stmu.Unlock()
			c.state = 2
			return nil
		}
	}
	return nil
}

func (c *Coordinator) solvereduce(id int)	error{
	c.reducemutex[id].Lock()
	defer c.reducemutex[id].Unlock()
	if c.reducefinish[id] != 2 {
		c.reducefinish[id] = 2
		c.reducemu.Lock()
		defer c.reducemu.Unlock()
		c.reducenum++
		if c.reducenum == c.NReduce {
			c.stmu.Lock()
			defer c.stmu.Unlock()
			c.state = 3
			return nil
		}
	}
	return nil
}

func (c *Coordinator) Unlock(args *LockArgs, reply* LockReply) error{
	id := args.Id
	check := args.Check
	if check == 0 {
		return c.solvemap(id)
	}
	if check == 1 {
		return c.solvereduce(id)
	}
	return nil
}
//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
// 轮询遍历job是否成功
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.state == 3 {
		ret = true
	}	
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	//init
	num := len(files)
	c.mapfinish = make([]int, num)
	c.mapmutex = make([]sync.Mutex, num)
	c.maplast = make([]time.Time, num)
	c.filename = make([]string, 0)
	c.filename = append(c.filename, files...)
	c.NReduce = nReduce
	c.reducefinish = make([]int, nReduce)
	c.reducemutex = make([]sync.Mutex, nReduce)
	c.reducelast = make([]time.Time, nReduce)
	c.state = 0
	c.mapnum = 0
	c.reducenum = 0
	c.server()
	return &c
}
