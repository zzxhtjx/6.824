package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "time"
import "encoding/json"
import "strconv"
import "sort"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	state := 0
	for state != 3 {//state == 3 will break
		reply 		:= CallReal()
		filename 	:= reply.Filename
		num 		:= reply.Nfile
		NReduce 	:= reply.NReduce 
		id 			:= reply.Id
		state 		 = reply.State
		switch state {
		case 1:
			//wait for all map task finish
			time.Sleep(time.Second)
		case 0:
			//do map task
			file, err 	:= os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			//存储一个slice数组,作为中间的结果输出
			//write to tmp json
			tmpfile := make([]*os.File, NReduce)
			enc := make([]*json.Encoder, NReduce)
			for i := 0; i < NReduce; i++ {
				tmpfile[i],err = ioutil.TempFile("", "map")
				if err != nil {
					fmt.Println(err)
				}
				// defer tmpfile[i].Close()
				enc[i] = json.NewEncoder(tmpfile[i])
			}
			for _, kv := range kva {
				id := ihash(kv.Key) % NReduce
				err = enc[id].Encode(&kv)
				// fmt.Println(id, kv)
			}
			// rename 
			for i := 0; i < NReduce; i++ {
				X := strconv.Itoa(id)
				Y := strconv.Itoa(i)
				newpath := "/root/6.5840/src/main/mr-tmp/mr-" + X + "-" + Y+".json"
				oldpath := tmpfile[i].Name()
				tmpfile[i].Close()
				err := os.Rename(oldpath, newpath)
				if err != nil {
					fmt.Println(oldpath, newpath)
					fmt.Println("map rename fail")
				}							
			}
			// call rpc tell the master
			CallUnlock(id, 0)	
		case 2:
			// do reduce task
			//first : read the json file 
			kva := make([]KeyValue, 0)
			for i := 0; i < num; i++ {
				X := strconv.Itoa(i)
				Y := strconv.Itoa(id)
				filename := "/root/6.5840/src/main/mr-tmp/mr-" + X + "-" + Y+".json"
				file, err := os.Open(filename)
				defer file.Close()		// this can read then close all
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}
			// second : sort and reduce the final file 
			sort.Sort(ByKey(kva))

			// third : do the reduce task and use tempory file in the first
			tmpfile, err := ioutil.TempFile("", "reduce")
			if err != nil {
				fmt.Println(err)
			}
			finalfile := "mr-out-" + strconv.Itoa(id)

			//
			// call Reduce on each distinct key in kva[],
			// and print the result to mr-out-id.
			//
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(tmpfile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			oldname := tmpfile.Name()
			tmpfile.Close()
			err = os.Rename(oldname, finalfile)
			if err != nil {
				fmt.Println("reduce Rename fail")
			}
			//tell the master that reduce has finish
			CallUnlock(id, 1)
		case 3:
			//job finish
			break;
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill inthe argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func CallReal() RealReply{
	args := RealArgs{}

	reply := RealReply{}

	ok := call("Coordinator.Real", &args, &reply)
	if ok {
		return reply
	} else {
		reply.State = 1 //进行循环
		return reply
	}
}


func CallUnlock(id int, check int) bool {
	args := LockArgs{}
	args.Id = id
	args.Check = check
	reply := LockReply{}
	ok := call("Coordinator.Unlock", &args, &reply)
	return ok
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
