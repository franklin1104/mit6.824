package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue
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


	// uncomment to send the Example RPC to the master.
	for {
		reply, succeed := CallForTask()
		if !succeed {
			return
		} else if reply.Type == "wait" {
			time.Sleep(2 * time.Second)
		} else if reply.Type == "map" {
			workerMap(mapf, reply)
		} else if reply.Type == "reduce" {
			workerReduce(reducef, reply)
		}
	}

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallForTask() (CallReply, bool){
	pid := os.Getpid()
	args := CallArgs{pid}
	reply := CallReply{}
	succeed := call("Master.ReplyTaskCall", &args, &reply)
	return reply, succeed
}

func TaskDone(args DoneArgs) (DoneReply, bool){
	reply := DoneReply{}
	succeed := call("Master.AcceptTaskDone", &args, &reply)
	return reply, succeed
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

func workerMap(mapf func(string, string) []KeyValue, reply CallReply){
	doneArgs := DoneArgs{Type:"map", TaskNum: reply.TaskNum, Pid: os.Getpid()}

	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", reply.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FileName)
	}
	file.Close()

	intermediate := mapf(reply.FileName, string(content))
	outputTempFiles := make(map[int]*os.File)
	outputTempFilesName := make(map[int]string)
	for _, interPair := range intermediate {
		interHash := ihash(interPair.Key) % reply.NReduce
		outputFileName := "mr-" + strconv.Itoa(reply.TaskNum) + "-" + strconv.Itoa(interHash)
		/*
		outputFile, err := os.OpenFile(outputFileName, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
		if err != nil {
			log.Fatal(err)
		}
		 */

		if outputTempFiles[interHash] == nil{
			outputTempFiles[interHash], err = ioutil.TempFile(".", outputFileName)
			outputTempFilesName[interHash] = outputFileName
			if err != nil {
				log.Fatal(err)
			}
		}
		outputTempFile, err := os.OpenFile(outputTempFiles[interHash].Name(), os.O_RDWR | os.O_APPEND, 0666)
		if err != nil{
			log.Fatal(err)
		}
		enc := json.NewEncoder(outputTempFile)
		err = enc.Encode(&interPair)
		if err != nil {
			log.Fatalf("encode error %v",err)
		}
		outputTempFile.Close()
		doneArgs.MapFiles = append(doneArgs.MapFiles, outputFileName)
	}

	doneReply, succeed := TaskDone(doneArgs)
	if succeed && doneReply.DoneConfirm{
		//TODO: rename temp file
		for index, tempFile := range outputTempFiles{
			err = os.Rename(tempFile.Name(), outputTempFilesName[index])
			if err != nil{
				log.Fatal(err)
			}
			tempFile.Close()
		}
	}else{
		for _, tempFile := range outputTempFiles{
			os.Remove(tempFile.Name())
		}
	}
}

func workerReduce(reducef func(string, []string) string, reply CallReply){
	doneArgs := DoneArgs{Type: "reduce",TaskNum: reply.TaskNum, Pid: os.Getpid()}
	interHash := reply.TaskNum

	pat := fmt.Sprintf("mr-%%d-%d", interHash)
	dd, err := os.Open(".")
	if err != nil {
		panic(err)
	}
	names, err := dd.Readdirnames(1000000)
	if err != nil {
		panic(err)
	}
	var intermediate []KeyValue
	for _, name := range names {
		var mapNum int
		n, err := fmt.Sscanf(name, pat, &mapNum)

		if n == 1 && err == nil {

			file, err := os.Open(name)
			if err != nil {
				panic(err)
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
			file.Close()
		}
	}

	sort.Sort(ByKey(intermediate))
	reduceFileName := "mr-out-" + strconv.Itoa(reply.TaskNum)
	doneArgs.ReduceFile = reduceFileName

	reduceTempFileName := strconv.Itoa(os.Getpid()) + "-mr-out-" + strconv.Itoa(reply.TaskNum)
	reduceTempFile, err := os.Create(reduceTempFileName)
	if err != nil{
		panic(err)
	}
	//reduceTempFile, _ := ioutil.TempFile(".", reduceFileName)
	//reduceFile, _ := os.Create(reduceFileName)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		_, err = fmt.Fprintf(reduceTempFile, "%v %v\n", intermediate[i].Key, output)
		if err != nil{
			panic(err)
		}
		i = j
	}
	reduceTempFile.Close()
	doneReply, succeed := TaskDone(doneArgs)
	if succeed && doneReply.DoneConfirm{
		//TODO: rename temp file
		err = os.Rename(reduceTempFileName, reduceFileName)
		if err != nil{
			panic(err)
		}
		for _, name := range names{
			var mapNum int
			n, err := fmt.Sscanf(name, pat, &mapNum)
			if n == 1 && err == nil {
				err = os.Remove(name)
				if err != nil{
					panic(err)
				}
			}
		}
	}else{
		err = os.Remove(reduceTempFileName)
		if err != nil{
			panic(err)
		}
	}
}
