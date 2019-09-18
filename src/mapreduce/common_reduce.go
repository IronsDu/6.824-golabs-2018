package mapreduce

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {

	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	for i := 0; i < nMap; i++ {
		func() {
			reduceFileName := reduceName(jobName, i, reduceTask)

			reduceFile, err := os.Open(reduceFileName)
			if err != nil {
				fmt.Printf("open file:%s cause error:%s", reduceFileName, err.Error())
				return
			}
			defer func() {
				if err := reduceFile.Close(); err != nil {
					fmt.Printf("close file:%s cause error:%s\n", reduceFileName, err.Error())
				}
			}()

			var keys []string
			kvList := make(map[string][]string)

			dec := json.NewDecoder(reduceFile)
			for {
				var kvPair KeyValue
				if err := dec.Decode(&kvPair); err != nil {
					fmt.Printf("decode error:%s\n", err.Error())
					break
				}

				if valueList , ok := kvList[kvPair.Key]; ok {
					kvList[kvPair.Key] = append(valueList, kvPair.Value)
				} else {
					kvList[kvPair.Key] = []string{kvPair.Value}
					keys = append(keys, kvPair.Key)
				}
			}

			outputFile, err := os.OpenFile(outFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
			defer func() {
				if err := outputFile.Close(); err != nil {
					fmt.Printf("close outFile:%s cause error:%s\n", outFile, err.Error())
				}
			}()

			enc := json.NewEncoder(outputFile)
			sort.Strings(keys)
			for _, key := range keys {
				valueList := kvList[key]
				result := reduceF(key, valueList)
				enc.Encode(KeyValue{key, result})
			}
		}()
	}
}
