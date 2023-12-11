// Contains the implementation of a LSP client.

// TODO: calculateChecksum and save it to message struct before marshalling it to []byte
package lsp

import (
	"encoding/json"
	"errors"
	"log"
	"math"
	"time"

	"github.com/cmu440/lspnet"
)

type massageWithEpoch struct {
	message        *Message
	epoch          int
	currentBackOff int
}

func (c *client) SendMessage(messageToSend *Message) {
	if messageToSend.Type == MsgData {
		unAckMessageMap := <-c.unAckMessageMap
		currentEpoch := <-c.currentEpoch
		c.currentEpoch <- currentEpoch
		unAckMessageMap[messageToSend.SeqNum] = &massageWithEpoch{
			message:        messageToSend,
			epoch:          currentEpoch + 1,
			currentBackOff: 1,
		}
		c.unAckMessageMap <- unAckMessageMap
	}
	jsonData, err := json.Marshal(messageToSend)
	if err != nil {
		log.Printf("Error encoding message: %v", err)
		return
	}
	if _, err := c.conn.Write(jsonData); err != nil {
		log.Printf("Error sending message: %v", err)
	}
	if messageToSend.Type == MsgData {
		log.Print("client ", c.connID, " finish writing: ", messageToSend.SeqNum)
	}

}

func (c *client) resendMessageRoutine() {
	for {
		select {
		case <-time.After(time.Duration(c.params.EpochMillis * 1000000)):

			currentEpoch := <-c.currentEpoch
			currentEpoch++
			c.currentEpoch <- currentEpoch

			//heartbeat
			ackMsg := NewAck(c.connID, 0)
			go c.SendMessage(ackMsg)

			go c.nonResponseEpochRoutine()

			unAckMessageMap := <-c.unAckMessageMap
			for seqNum, messageWithEpoch := range unAckMessageMap {
				if messageWithEpoch.epoch <= currentEpoch {
					log.Print("client resend!!!!")
					// send message
					messageToSend := messageWithEpoch.message
					jsonData, err := json.Marshal(messageToSend)
					if err != nil {
						log.Printf("Error encoding message: %v", err)
						return
					}
					if _, err := c.conn.Write(jsonData); err != nil {
						log.Printf("Error sending message: %v", err)
					}
					log.Print("client: ", c.connID, "resend message: ", messageToSend)
					if messageWithEpoch.currentBackOff > c.params.MaxBackOffInterval {
						messageWithEpoch.currentBackOff = c.params.MaxBackOffInterval
					}
					messageWithEpoch.epoch = messageWithEpoch.epoch + messageWithEpoch.currentBackOff + 1
					//update backoff
					messageWithEpoch.currentBackOff = messageWithEpoch.currentBackOff * 2
					unAckMessageMap[seqNum] = messageWithEpoch
				}
			}
			c.unAckMessageMap <- unAckMessageMap
		}
	}
}

type PendingMessageQueueOp struct {
	opType         string // "keepPop" or "insert"
	pendingMessage *Message
}

func (c *client) managePendingMessageQueueOps() {
	for {
		select {
		case op := <-c.pendingMessageQueueOpChan:
			switch op.opType {
			case "keepPop":

			case "insert":
				//TODO: just use op.pendingMessage
				c.pendingMessageQueue.Enqueue(op.pendingMessage)
			}
			temp_op := LeftWindowSizeOp{
				opType:   "read",
				respChan: make(chan int),
			}
			c.LeftWindowSizeOpChan <- temp_op
			curLeftWindowSize := <-temp_op.respChan

			// temp_op2 := currentWriteSeqNumOp{
			// 	opType:   "read",
			// 	respChan: make(chan int),
			// }
			// c.currentWriteSeqNumOpChan <- temp_op2
			// curWriteSeqNum := <-temp_op2.respChan

			temp_op3 := currentAckNumOp{
				opType:   "read",
				respChan: make(chan int),
			}
			c.currentAckNumOpChan <- temp_op3
			curAckNum := <-temp_op3.respChan
			if !c.pendingMessageQueue.IsEmpty() {
				log.Print(c.connID, ": is here with top peek seqnum:", c.pendingMessageQueue.Peek().(*Message).SeqNum, " and current unac ", (curLeftWindowSize + c.params.WindowSize - 1))
			}
			//log.Print(c.connID, ": is here with top peek seqnum:", c.pendingMessageQueue.Peek().(*Message).SeqNum, " and current rightsidewindow ", )
			for (!c.pendingMessageQueue.IsEmpty() && c.pendingMessageQueue.Peek().(*Message).SeqNum <= curLeftWindowSize+c.params.WindowSize-1) &&
				((c.pendingMessageQueue.Peek().(*Message).SeqNum - curAckNum) <= c.params.MaxUnackedMessages) {
				messageToSend := c.pendingMessageQueue.Dequeue().(*Message)
				go c.SendMessage(messageToSend)

			}
		}
	}
}

type LeftWindowSizeOp struct {
	opType   string // "plus" or "minus" or "read" or "set"
	value    int    //value of set
	respChan chan int
}

func (c *client) manageLeftWindowSizeOps() {
	for {
		select {
		case op := <-c.LeftWindowSizeOpChan:
			switch op.opType {
			case "plus":
				c.windowSizeLeft++
			case "minus":
				c.windowSizeLeft--
			case "read":
				op.respChan <- c.windowSizeLeft
			case "set":
				c.windowSizeLeft = op.value
			}
		}
	}
}

type currentWriteSeqNumOp struct {
	opType   string // "plus" or "read"
	respChan chan int
}

func (c *client) manageCurrentWriteSeqNumOps() {
	for {
		select {
		case op := <-c.currentWriteSeqNumOpChan:
			switch op.opType {
			case "plus":
				c.currentWriteSeqNum++
			case "read":
				op.respChan <- c.currentWriteSeqNum
			}
		}
	}
}

type currentReadSeqNumOp struct {
	opType   string // "plus" or "read"
	respChan chan int
	value    int //value of set
}

func (c *client) manageCurrentReadSeqNumOps() {
	for {
		select {
		case op := <-c.currentReadSeqNumOpChan:
			switch op.opType {
			case "plus":
				c.currentReadSeqNum++
			case "read":
				op.respChan <- c.currentReadSeqNum
			case "set":
				c.currentReadSeqNum = op.value
			}
		}
	}
}

type currentAckNumOp struct {
	opType   string // "plus" or "read" or "set"
	respChan chan int
	value    int //value of set
}

func (c *client) manageCurrentAckNumOps() {
	for {
		select {
		case op := <-c.currentAckNumOpChan:
			switch op.opType {
			case "plus":
				c.currentAckNum++
			case "read":
				op.respChan <- c.currentAckNum
			case "set":
				c.currentAckNum = op.value
			}
		}
	}
}

type connIDOp struct {
	opType   string // "read" or "set"
	respChan chan int
	value    int //value of set
}

func (c *client) manageConnIDOps() {
	for {
		select {
		case op := <-c.connIDOpChan:
			switch op.opType {
			case "read":
				op.respChan <- c.connID
			case "set":
				c.connID = op.value
			}

		}
	}
}

type client struct {
	conn                      *lspnet.UDPConn
	readChannel               chan *Message
	connID                    int
	readSigChan               chan struct{}
	params                    Params
	serverMessageChannel      chan Message
	closeChannel              chan struct{}
	windowSizeLeft            int
	currentReadSeqNum         int // should be the next one to be processed
	currentWriteSeqNum        int // should be the oldest that have been processed
	currentAckNum             int
	pendingMessageQueue       *Queue
	pqRead                    map[int]*Message
	pqACK                     *PriorityQueue // process writing to server
	pendingMessageQueueOpChan chan PendingMessageQueueOp
	PendingMessageQueueChan   chan *Message
	pendingMessageQueueLock   chan struct{}
	LeftWindowSizeOpChan      chan LeftWindowSizeOp
	LeftWindowSizeLock        chan struct{}
	currentWriteSeqNumOpChan  chan currentWriteSeqNumOp
	currentWriteSeqNumLock    chan struct{}
	currentReadSeqNumOpChan   chan currentReadSeqNumOp
	currentReadSeqNumLock     chan struct{}
	currentAckNumOpChan       chan currentAckNumOp
	currentAckNumLock         chan struct{}

	connIDOpChan chan connIDOp
	connIDLock   chan struct{}
	// pqACKOpChan               chan pqACKOp
	unAckMessageMap     chan map[int]*massageWithEpoch
	currentEpoch        chan int
	nonResponseEpochSum chan int
	connSetUpChan       chan int

	readChannelQueue chan *Queue
}

func (c *client) readChannelProcess() {
	log.Print("herhe!!!")
	readChannelQueue := <-c.readChannelQueue
	item := readChannelQueue.Peek().(*Message)
	readChannelQueue.Dequeue()
	c.readChannelQueue <- readChannelQueue
	log.Print("herh2222!!!")
	c.readChannel <- item
}

func (c *client) readProcessRoutine() {
	buf := make([]byte, 2048)
	for {
		n, _, err := c.conn.ReadFromUDP(buf)
		if err != nil {
			log.Println("Read Error: ", err)
			continue
		}

		var msg Message
		if err := json.Unmarshal(buf[:n], &msg); err != nil {
			log.Println("Decode Error: ", err)
			continue
		}

		if msg.Type == MsgData {
			ackMsg := NewAck(msg.ConnID, msg.SeqNum)
			go c.SendMessage(ackMsg)

		}
		log.Print("client ", c.connID, " do get message with msgtype", msg.Type, " with seqnum", msg.SeqNum)
		c.serverMessageChannel <- msg
		nonResponseEpochSum := <-c.nonResponseEpochSum
		nonResponseEpochSum = -1 // when increased it turns to 0 which means a new epoch calculation
		c.nonResponseEpochSum <- nonResponseEpochSum

	}
}

func (c *client) findMinSeqNum() *Message {
	minSeqNum := math.MaxInt64
	for key := range c.pqRead {
		if key < minSeqNum {
			minSeqNum = key
		}
	}
	return c.pqRead[minSeqNum]
}

func (c *client) mainRoutine() {
	for {
		select {
		case <-c.closeChannel:
			// 客户端已关闭，退出协程

			return
		case msg := <-c.serverMessageChannel:

			if msg.Type == MsgData {
				log.Print("am i here??")
				newPqReadMessage := Message{
					SeqNum:  msg.SeqNum,
					Payload: msg.Payload,
					ConnID:  msg.ConnID,
				}
				log.Print("my current readseqnun: ", c.currentReadSeqNum, " with msgseqnum ", msg.SeqNum)
				if msg.SeqNum >= (c.currentReadSeqNum + 1) {
					c.pqRead[msg.SeqNum] = &newPqReadMessage
				} else {
					continue
				}
				curPqReadTop := c.findMinSeqNum()

				for (c.currentReadSeqNum + 1) == (curPqReadTop.SeqNum) {
					delete(c.pqRead, curPqReadTop.SeqNum)
					readChannelQueue := <-c.readChannelQueue
					readChannelQueue.Enqueue(curPqReadTop)
					c.readChannelQueue <- readChannelQueue
					log.Print("am i here!@$@#??")
					go c.readChannelProcess()

					c.currentReadSeqNum++
					if len(c.pqRead) == 0 {
						break
					}
					curPqReadTop = c.findMinSeqNum()

				}

			} else if msg.Type == MsgAck {
				// temp_op00 := connIDOp{
				// 	opType: "set",
				// 	value:  msg.ConnID,
				// }
				// c.connIDOpChan <- temp_op00
				if msg.SeqNum == 0 {
					continue
				}

				temp_op := LeftWindowSizeOp{
					opType:   "read",
					respChan: make(chan int),
				}
				c.LeftWindowSizeOpChan <- temp_op
				curLeftWindowSize := <-temp_op.respChan

				unAckMessageMap := <-c.unAckMessageMap
				// 1) see if this is in the map
				// 2) if true: increase currntacknum
				//.    and delete

				if _, ok := unAckMessageMap[msg.SeqNum]; ok {
					temp_op0 := currentAckNumOp{
						opType: "plus",
					}
					c.currentAckNumOpChan <- temp_op0
					delete(unAckMessageMap, msg.SeqNum)
				}
				c.unAckMessageMap <- unAckMessageMap

				log.Print(c.connID, ": i get seqnum: ", msg.SeqNum, " curleft:", curLeftWindowSize)
				if msg.SeqNum == (curLeftWindowSize) {
					/*update leftWindow */
					newOp := LeftWindowSizeOp{
						opType: "plus",
					}
					c.LeftWindowSizeOpChan <- newOp

					/*update continuously from pqACK queue*/
					lastLeft := msg.SeqNum + 1

					for c.pqACK.Len() > 0 {
						if (lastLeft) == c.pqACK.Top().(int) {
							lastLeft++
							c.pqACK.Pop()
							c.LeftWindowSizeOpChan <- newOp
						} else {
							break
						}
					}

					pendMessageOp := PendingMessageQueueOp{
						opType: "keepPop",
					}
					c.pendingMessageQueueOpChan <- pendMessageOp
					//
				} else {
					c.pqACK.Insert(msg.SeqNum, float64(msg.SeqNum))
				}

			} else if msg.Type == MsgCAck {
				temp_op00 := connIDOp{
					opType: "set",
					value:  msg.ConnID,
				}
				c.connIDOpChan <- temp_op00
				temp_op := LeftWindowSizeOp{
					opType:   "read",
					respChan: make(chan int),
				}
				c.LeftWindowSizeOpChan <- temp_op
				curLeftWindowSize := <-temp_op.respChan

				temp_op1 := currentAckNumOp{
					opType: "set",
					value:  msg.SeqNum,
				}
				c.currentAckNumOpChan <- temp_op1

				unAckMessageMap := <-c.unAckMessageMap
				for seqNum, _ := range unAckMessageMap {
					if seqNum <= msg.SeqNum {
						delete(unAckMessageMap, seqNum)
					}
				}
				c.unAckMessageMap <- unAckMessageMap

				if msg.SeqNum >= (curLeftWindowSize) {

					for c.pqACK.Len() > 0 {
						if msg.SeqNum >= c.pqACK.Top().(int) {
							c.pqACK.Pop()
						} else {
							break
						}
					}

					plusOp := LeftWindowSizeOp{
						opType: "plus",
					}

					lastLeft := msg.SeqNum
					for c.pqACK.Len() > 0 {
						if (lastLeft) == c.pqACK.Top().(int) {
							lastLeft++
							c.pqACK.Pop()
							c.LeftWindowSizeOpChan <- plusOp
						} else {
							break
						}
					}

					temp_op1 := LeftWindowSizeOp{
						opType: "set",
						value:  lastLeft, //
					}
					c.LeftWindowSizeOpChan <- temp_op1

					pendMessageOp := PendingMessageQueueOp{
						opType: "keepPop",
					}
					c.pendingMessageQueueOpChan <- pendMessageOp
				}

			}

		}
	}
}

func (c *client) setUpConnection() {
	buf := make([]byte, 2048)
	for {
		n, _, err := c.conn.ReadFromUDP(buf)
		if err != nil {
			log.Println("Read Error: ", err)
			continue
		}

		var msg Message
		if err := json.Unmarshal(buf[:n], &msg); err != nil {
			log.Println("Decode Error: ", err)
			continue
		}

		c.connID = msg.ConnID //no need to protect here
		c.connSetUpChan <- c.connID
		break

	}

}

func (c *client) nonResponseEpochRoutine() {
	nonResponseEpoch := <-c.nonResponseEpochSum
	nonResponseEpoch++
	c.nonResponseEpochSum <- nonResponseEpoch
	if nonResponseEpoch > c.params.EpochLimit {
		// TODO: close the channel
		return
	}

}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// initialSeqNum is an int representing the Initial Sequence Number (ISN) this
// client must use. You may assume that sequence numbers do not wrap around.
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, initialSeqNum int, params *Params) (Client, error) {
	// 使用lspnet.ResolveUDPAddr解析服务器地址
	raddr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}

	// 使用lspnet.DialUDP连接到服务器
	conn, err := lspnet.DialUDP("udp", nil, raddr)
	if err != nil {
		return nil, err
	}

	connRequest := NewConnect(initialSeqNum)
	jsonData, err := json.Marshal(connRequest)
	if err != nil {
		log.Printf("Error encoding message: %v", err)
		return nil, err
	}
	if _, err := conn.Write(jsonData); err != nil {
		log.Printf("Error sending message: %v", err)
		return nil, err
	}

	c := &client{
		conn:                      conn,
		readChannel:               make(chan *Message, 1),
		readSigChan:               make(chan struct{}),
		params:                    *params,
		connID:                    0,
		serverMessageChannel:      make(chan Message),
		closeChannel:              make(chan struct{}),
		windowSizeLeft:            initialSeqNum + 1,
		currentReadSeqNum:         initialSeqNum,
		currentWriteSeqNum:        initialSeqNum,
		currentAckNum:             initialSeqNum,
		pendingMessageQueue:       NewQueue(),
		pqRead:                    make(map[int]*Message),
		pqACK:                     NewPriorityQueue(),
		PendingMessageQueueChan:   make(chan *Message),
		pendingMessageQueueOpChan: make(chan PendingMessageQueueOp),
		LeftWindowSizeOpChan:      make(chan LeftWindowSizeOp),
		currentWriteSeqNumOpChan:  make(chan currentWriteSeqNumOp),
		currentReadSeqNumOpChan:   make(chan currentReadSeqNumOp),
		currentAckNumOpChan:       make(chan currentAckNumOp),

		connIDOpChan: make(chan connIDOp),
		// pqACKOpChan:               make(chan pqACKOp),
		unAckMessageMap:     make(chan map[int]*massageWithEpoch, 1),
		currentEpoch:        make(chan int, 1),
		nonResponseEpochSum: make(chan int, 1),
		connSetUpChan:       make(chan int),

		readChannelQueue: make(chan *Queue, 1),
	}
	c.currentEpoch <- 0
	c.unAckMessageMap <- make(map[int]*massageWithEpoch)
	c.nonResponseEpochSum <- -1
	c.readChannelQueue <- NewQueue()
	go c.setUpConnection()
	for {
		select {
		case connId := <-c.connSetUpChan:
			log.Print("established connection with ", connId)
			c.connID = connId
			go c.managePendingMessageQueueOps()
			go c.manageLeftWindowSizeOps()
			go c.manageCurrentWriteSeqNumOps()
			go c.manageCurrentAckNumOps()
			go c.manageCurrentReadSeqNumOps()
			//go c.managePqReadOps()
			go c.readProcessRoutine()
			go c.mainRoutine()
			go c.manageConnIDOps()
			go c.resendMessageRoutine()

			return c, nil

		case <-time.After(time.Duration(params.EpochMillis * 1000000)):
			currentEpoch := <-c.currentEpoch
			currentEpoch++
			c.currentEpoch <- currentEpoch
			if currentEpoch > c.params.EpochLimit {
				return nil, errors.New("connection could not be made")
			}
			//send connection request again
			connRequest := NewConnect(initialSeqNum)
			jsonData, err := json.Marshal(connRequest)
			if err != nil {
				return nil, err
			}
			if _, err := conn.Write(jsonData); err != nil {
				return nil, err
			}
		}
	}

}

func (c *client) ConnID() int {
	temp_op := connIDOp{
		opType:   "read",
		respChan: make(chan int),
	}
	c.connIDOpChan <- temp_op
	connID := <-temp_op.respChan
	return connID
}

// it is ok if it blocks forever.
func (c *client) Read() ([]byte, error) {
	//c.readSigChan <- struct{}{}
	for {
		select {
		case data, ok := <-c.readChannel:

			if !ok {
				// readChannel 已关闭
				return nil, errors.New("readChannel has been closed")
			}
			log.Print("client do get data !!")
			return data.Payload, nil
		case <-c.closeChannel:
			// 客户端已关闭
			return nil, errors.New("client has been closed")
		}
	}
}

// the sequence number depends on the order of the Write calls
func (c *client) Write(payload []byte) error {
	temp_op := currentWriteSeqNumOp{
		opType: "plus",
	}
	c.currentWriteSeqNumOpChan <- temp_op

	temp_op1 := LeftWindowSizeOp{
		opType:   "read",
		respChan: make(chan int),
	}
	c.LeftWindowSizeOpChan <- temp_op1
	curLeftWindowSize := <-temp_op1.respChan

	temp_op2 := currentWriteSeqNumOp{
		opType:   "read",
		respChan: make(chan int),
	}
	c.currentWriteSeqNumOpChan <- temp_op2
	curWriteSeqNum := <-temp_op2.respChan

	temp_op3 := currentAckNumOp{
		opType:   "read",
		respChan: make(chan int),
	}
	c.currentAckNumOpChan <- temp_op3
	curAckNum := <-temp_op3.respChan

	if (curWriteSeqNum <= curLeftWindowSize+c.params.WindowSize-1) &&
		((curWriteSeqNum - curAckNum) <= c.params.MaxUnackedMessages) {

		temp_op4 := connIDOp{
			opType:   "read",
			respChan: make(chan int),
		}
		c.connIDOpChan <- temp_op4
		connID := <-temp_op4.respChan
		messageToSend := NewData(connID, curWriteSeqNum, int(len(payload)), payload,
			CalculateChecksum(connID, curWriteSeqNum, int(len(payload)), payload))
		go c.SendMessage(messageToSend)

	} else {
		temp_op0 := connIDOp{
			opType:   "read",
			respChan: make(chan int),
		}
		c.connIDOpChan <- temp_op0
		connID := <-temp_op0.respChan
		temp_op := PendingMessageQueueOp{
			opType: "insert",
			pendingMessage: NewData(connID, curWriteSeqNum, int(len(payload)), payload,
				CalculateChecksum(connID, curWriteSeqNum, int(len(payload)), payload)),
		}

		c.pendingMessageQueueOpChan <- temp_op

	}
	return nil
}

func (c *client) Close() error {
	c.closeChannel <- struct{}{}
	return c.conn.Close()
}
