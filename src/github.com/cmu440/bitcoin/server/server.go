package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"

	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
)

type requestStatus struct {
	clientID     int
	min_hash     uint64
	nounce       uint64
	completedNum uint64
	totalNum     uint64
}

type task struct {
	minerStatus string
	requestID   int
	msg         bitcoin.Message
}

type server struct {
	lspServer           lsp.Server
	minerNum            int
	curRequestID        int
	clientID2RequestMap map[int]int
	minersMap           map[int]*task
	requestStatusMap    map[int]*requestStatus
	taskPriorityMap     map[int]*bitcoin.Queue
}

const CHUNKSIZE = 10000

func startServer(port int) (*server, error) {
	// TODO: implement this!
	s := &server{
		lspServer:           nil,
		curRequestID:        0,
		minerNum:            0,
		clientID2RequestMap: make(map[int]int),
		minersMap:           make(map[int]*task),
		requestStatusMap:    make(map[int]*requestStatus),
		taskPriorityMap:     make(map[int]*bitcoin.Queue),
	}
	lspserver, err := lsp.NewServer(port, lsp.NewParams())
	if err != nil {
		return nil, err
	}
	s.lspServer = lspserver

	return s, nil
}

var LOGF *log.Logger

func main() {
	// You may need a logger for debug purpose
	const (
		name = "serverLog.txt"
		flag = os.O_RDWR | os.O_CREATE
		perm = os.FileMode(0666)
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}
	defer file.Close()

	LOGF = log.New(file, "", log.Lshortfile|log.Lmicroseconds)
	// Usage: LOGF.Println() or LOGF.Printf()

	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <port>", os.Args[0])
		return
	}

	port, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Port must be a number:", err)
		return
	}

	srv, err := startServer(port)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("Server listening on port", port)

	defer srv.lspServer.Close()

	// TODO: implement this!
	for {
		//read the request from server
		connID, requestByte, err := srv.lspServer.Read()
		//if disconnected
		if err != nil {
			//fmt.Println("Failed to read from server:", err)

			if task, ok := srv.minersMap[connID]; ok { //miner disconnected
				delete(srv.minersMap, connID)
				if task.minerStatus != "idle" {
					//iterate through minersmap to find an idle miner
					find_idle := false
					for connID, per_task := range srv.minersMap {
						if per_task.minerStatus == "idle" {
							srv.minersMap[connID] = task
							//send the task to the idle miner
							msgToSendMarshalled, err := json.Marshal(task.msg)
							if err != nil {
								//fmt.Println("Failed to marshal response:", err)
								return
							}
							srv.lspServer.Write(connID, msgToSendMarshalled)
							find_idle = true
							break
						}
					}
					if !find_idle {
						//cannot find an idle miner, put the task back to the queue
						srv.taskPriorityMap[task.requestID].EnqueueAtHead(task.msg)
					}

				}

			} else { //client disconnected
				//delete the request in maps
				delete(srv.requestStatusMap, srv.clientID2RequestMap[connID])
				delete(srv.taskPriorityMap, srv.clientID2RequestMap[connID])
			}
			continue

		}

		var msg bitcoin.Message
		json.Unmarshal(requestByte, &msg)
		fmt.Println("Server received a message")
		if msg.Type == bitcoin.Join {
			//new client join
			srv.minerNum++
			srv.minersMap[connID] = &task{"idle", 0, bitcoin.Message{}}
			fmt.Println("a miner joined now")
		} else if msg.Type == bitcoin.Request {
			//new request
			srv.curRequestID++
			srv.clientID2RequestMap[connID] = srv.curRequestID
			totalNum := (msg.Upper + CHUNKSIZE - 1) / CHUNKSIZE
			messageQueue := bitcoin.NewQueue()
			srv.taskPriorityMap[srv.curRequestID] = messageQueue
			msgToSend := bitcoin.NewRequest(msg.Data, uint64((totalNum-1)*CHUNKSIZE), msg.Upper)
			messageQueue.EnqueueAtBack(msgToSend)
			for i := uint64(0); (i + 1) < totalNum; i++ {
				msgToSend := bitcoin.NewRequest(msg.Data, uint64(i*CHUNKSIZE), uint64((i+1)*CHUNKSIZE))
				messageQueue.EnqueueAtBack(msgToSend)
			}
			srv.requestStatusMap[srv.curRequestID] = &requestStatus{connID, math.MaxUint64, 0, 0, totalNum}

		} else if msg.Type == bitcoin.Result {

			//result from miner
			requestID := srv.minersMap[connID].requestID
			srv.minersMap[connID] = &task{"idle", 0, bitcoin.Message{}}
			if _, ok := srv.requestStatusMap[requestID]; !ok {
				continue
			}

			srv.requestStatusMap[requestID].completedNum++
			fmt.Println("Server received completednum: ", srv.requestStatusMap[requestID].completedNum,
				" with totalnum: ", srv.requestStatusMap[requestID].totalNum)
			if msg.Hash < srv.requestStatusMap[requestID].min_hash {
				srv.requestStatusMap[requestID].min_hash = msg.Hash
				srv.requestStatusMap[requestID].nounce = msg.Nonce
			}
			if srv.requestStatusMap[requestID].completedNum == srv.requestStatusMap[requestID].totalNum {
				//all miners have completed the task, send the result back to client
				msgToSend := bitcoin.NewResult(srv.requestStatusMap[requestID].min_hash, srv.requestStatusMap[requestID].nounce)
				msgToSendMarshalled, err := json.Marshal(msgToSend)
				if err != nil {
					//fmt.Println("Failed to marshal response:", err)
					return
				}
				srv.lspServer.Write(srv.requestStatusMap[requestID].clientID, msgToSendMarshalled)

				//delete the request in maps
				delete(srv.requestStatusMap, requestID)
			}
		}

		//find idle miner
		if len(srv.taskPriorityMap) == 0 {
			continue
		}
		for connID, per_task := range srv.minersMap {
			if per_task.minerStatus == "idle" {
				minimum_lenth := math.MaxInt
				requestID := 0
				for curRequestID, queue := range srv.taskPriorityMap {
					// get queue with mimimum length, send the head task to the idle miner
					length := queue.Len()
					if length < minimum_lenth {
						minimum_lenth = length
						requestID = curRequestID
					} else if length == minimum_lenth {
						if curRequestID < requestID {
							requestID = curRequestID
						}
					}
				}
				if requestID == 0 {
					continue
				}
				msgtask := srv.taskPriorityMap[requestID].Dequeue().(*bitcoin.Message)
				if srv.taskPriorityMap[requestID].IsEmpty() {
					delete(srv.taskPriorityMap, requestID)
				}
				srv.minersMap[connID] = &task{"busy", requestID, *msgtask}
				msgToSendMarshalled, err := json.Marshal(*msgtask)
				if err != nil {
					//fmt.Println("Failed to marshal response:", err)
					return
				}
				srv.lspServer.Write(connID, msgToSendMarshalled)
			}
		}
	}
}
