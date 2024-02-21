// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/cmu440/lspnet"
)

// a structure called massageWithEpoch include the message, epoch and current backoff
type massageWithEpoch struct {
	message        *Message
	epoch          int
	currentBackOff int
}

// send message function
func (c *client) SendMessage(messageToSend *Message) {

	if messageToSend.Type == MsgData {
		unAckMessageMap := <-c.unAckMessageMap
		currentEpoch := <-c.currentEpoch
		c.currentEpoch <- currentEpoch // a int value, not need protect here
		unAckMessageMap[messageToSend.SeqNum] = &massageWithEpoch{
			message:        messageToSend,
			epoch:          currentEpoch + 1,
			currentBackOff: 1,
		}

		c.unAckMessageMap <- unAckMessageMap
	}
	jsonData, err := json.Marshal(messageToSend)
	if err != nil {
		return
	}
	c.conn.Write(jsonData)

}

// resend message routine
func (c *client) resendMessageRoutine() {
	for {

		select {
		case <-c.pendingMessageFinalClose:
			c.pendingMessageFinalClose <- struct{}{}
			return
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
					// send message
					messageToSend := messageWithEpoch.message
					jsonData, err := json.Marshal(messageToSend)
					if err != nil {
						return
					}
					c.conn.Write(jsonData)

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

// a structure called PendingMessageQueueOp include the operation type and pending message
type PendingMessageQueueOp struct {
	opType         string // "keepPop" or "insert"
	pendingMessage *Message
}

// manage pending message queue operations
func (c *client) managePendingMessageQueueOps() {
	isCloseCalled := false
	for {
		select {
		case <-c.pendingMessageFinalClose:
			c.pendingMessageFinalClose <- struct{}{}
			return
		case op := <-c.pendingMessageQueueOpChan:
			switch op.opType {
			case "keepPop":

			case "insert":
				c.pendingMessageQueue.Enqueue(op.pendingMessage)
			}
			temp_op := LeftWindowSizeOp{
				opType:   "read",
				respChan: make(chan int),
			}
			c.LeftWindowSizeOpChan <- temp_op
			curLeftWindowSize := <-temp_op.respChan

			temp_op3 := currentAckNumOp{
				opType:   "read",
				respChan: make(chan int),
			}
			c.currentAckNumOpChan <- temp_op3
			curAckNum := <-temp_op3.respChan
			for (!c.pendingMessageQueue.IsEmpty() && c.pendingMessageQueue.Peek().(*Message).SeqNum <= curLeftWindowSize+c.params.WindowSize-1) &&
				((c.pendingMessageQueue.Peek().(*Message).SeqNum - curAckNum) <= c.params.MaxUnackedMessages) {
				messageToSend := c.pendingMessageQueue.Dequeue().(*Message)
				c.SendMessage(messageToSend)
			}
			if isCloseCalled && c.pendingMessageQueue.IsEmpty() {
				select {
				case <-c.pendingMessageFinalClose:
					c.pendingMessageFinalClose <- struct{}{}
				default:
					c.pendingMessageFinalClose <- struct{}{}
				}
			}
		case <-c.isClosed:
			isCloseCalled = true
			if isCloseCalled && c.pendingMessageQueue.IsEmpty() {
				select {
				case <-c.pendingMessageFinalClose:
					c.pendingMessageFinalClose <- struct{}{}
				default:
					c.pendingMessageFinalClose <- struct{}{}
				}

			}
		}

	}
}

// a structure called LeftWindowSizeOp include the operation type, response channel and value
type LeftWindowSizeOp struct {
	opType   string // "plus" or "minus" or "read" or "set"
	value    int    //value of set
	respChan chan int
}

// manage left window size operations
func (c *client) manageLeftWindowSizeOps() {
	for {
		select {
		case <-c.pendingMessageFinalClose:
			c.pendingMessageFinalClose <- struct{}{}
			return
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

// a structure called currentWriteSeqNumOp include the operation type, response channel
type currentWriteSeqNumOp struct {
	opType   string // "plus" or "read"
	respChan chan int
}

// manage current write sequence number operations
func (c *client) manageCurrentWriteSeqNumOps() {
	for {
		select {
		case <-c.pendingMessageFinalClose:
			c.pendingMessageFinalClose <- struct{}{}
			return
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

// a structure called currentAckNumOp include the operation type, response channel and value
type currentAckNumOp struct {
	opType   string // "plus" or "read" or "set"
	respChan chan int
	value    int //value of set
}

// manage current ack number operations
func (c *client) manageCurrentAckNumOps() {
	for {
		select {
		case <-c.pendingMessageFinalClose:
			c.pendingMessageFinalClose <- struct{}{}
			return
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

// a structure called connIDOp include the operation type, response channel and value
type connIDOp struct {
	opType   string // "read" or "set"
	respChan chan int
	value    int //value of set
}

// manage connection id operations
func (c *client) manageConnIDOps() {
	for {
		select {
		case <-c.pendingMessageFinalClose:
			c.pendingMessageFinalClose <- struct{}{}
			return
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
	currentReadSeqNumLock     chan struct{}
	currentAckNumOpChan       chan currentAckNumOp
	currentAckNumLock         chan struct{}

	connIDOpChan        chan connIDOp
	connIDLock          chan struct{}
	unAckMessageMap     chan map[int]*massageWithEpoch
	currentEpoch        chan int
	nonResponseEpochSum chan int
	connSetUpChan       chan int

	readChannelQueue chan *Queue

	isClosed                 chan struct{} // a channel to indicate whether the client is closed
	pendingMessageFinalClose chan struct{}
}

// read channel process
func (c *client) readChannelProcess() {
	readChannelQueue := <-c.readChannelQueue
	item := readChannelQueue.Peek().(*Message)
	readChannelQueue.Dequeue()
	c.readChannelQueue <- readChannelQueue
	c.readChannel <- item
}

// read process routine
func (c *client) readProcessRoutine() {
	buf := make([]byte, 2048)
	for {
		n, _, err := c.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}

		var msg Message
		if err := json.Unmarshal(buf[:n], &msg); err != nil {
			continue
		}

		if msg.Type == MsgData {
			ackMsg := NewAck(msg.ConnID, msg.SeqNum)
			go c.SendMessage(ackMsg)

		}

		c.serverMessageChannel <- msg
		nonResponseEpochSum := <-c.nonResponseEpochSum
		nonResponseEpochSum = -1 // when increased it turns to 0 which means a new epoch calculation
		c.nonResponseEpochSum <- nonResponseEpochSum

	}
}

// find the minimum sequence number in the read queue
func (c *client) findMinSeqNum() *Message {
	if len(c.pqRead) == 0 {
		return nil
	}
	minSeqNum := math.MaxInt
	for key := range c.pqRead {
		if key < minSeqNum {
			minSeqNum = key
		}
	}
	return c.pqRead[minSeqNum]
}

// main routine
func (c *client) mainRoutine() {
	for {
		select {
		case <-c.pendingMessageFinalClose:
			c.pendingMessageFinalClose <- struct{}{}
			return
		case msg := <-c.serverMessageChannel:
			if msg.Type == MsgData {
				//check the checksum
				//if the data get is longer than said, truncate it first

				if len(msg.Payload) > msg.Size {
					msg.Payload = msg.Payload[:msg.Size]
				}
				if CalculateChecksum(msg.ConnID, msg.SeqNum, msg.Size, msg.Payload) != msg.Checksum {
					continue
				}

				newPqReadMessage := Message{
					SeqNum:  msg.SeqNum,
					Payload: msg.Payload,
					ConnID:  msg.ConnID,
				}

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
					go c.readChannelProcess()
					c.currentReadSeqNum++
					if len(c.pqRead) == 0 {
						break
					}
					curPqReadTop = c.findMinSeqNum()
				}

			} else if msg.Type == MsgAck { //process ack

				if msg.SeqNum == 0 { //process heartbeat
					continue
				}

				temp_op := LeftWindowSizeOp{
					opType:   "read",
					respChan: make(chan int),
				}
				c.LeftWindowSizeOpChan <- temp_op
				curLeftWindowSize := <-temp_op.respChan

				unAckMessageMap := <-c.unAckMessageMap
				if _, ok := unAckMessageMap[msg.SeqNum]; ok {
					temp_op0 := currentAckNumOp{
						opType: "plus",
					}
					c.currentAckNumOpChan <- temp_op0
					delete(unAckMessageMap, msg.SeqNum)
				}
				c.unAckMessageMap <- unAckMessageMap

				if msg.SeqNum == curLeftWindowSize {
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

				} else {
					c.pqACK.Insert(msg.SeqNum, float64(msg.SeqNum))
				}

			} else if msg.Type == MsgCAck { //process cack
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
						value:  lastLeft,
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

// set up connection function
func (c *client) setUpConnection() {
	buf := make([]byte, 2048)
	for {
		n, _, err := c.conn.ReadFromUDP(buf)
		if err != nil {
			continue
		}

		var msg Message
		if err := json.Unmarshal(buf[:n], &msg); err != nil {
			continue
		}

		c.connID = msg.ConnID
		c.connSetUpChan <- c.connID
		break

	}
}

// non response epoch routine
func (c *client) nonResponseEpochRoutine() {
	nonResponseEpoch := <-c.nonResponseEpochSum
	nonResponseEpoch++
	c.nonResponseEpochSum <- nonResponseEpoch
	if nonResponseEpoch > c.params.EpochLimit { //if the epoch limit is reached, close the connection
		select {
		case <-c.pendingMessageFinalClose:
			c.pendingMessageFinalClose <- struct{}{}
		default:
			c.pendingMessageFinalClose <- struct{}{}
		}
		c.conn.Close()
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

	raddr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}

	conn, err := lspnet.DialUDP("udp", nil, raddr)
	if err != nil {
		return nil, err
	}

	connRequest := NewConnect(initialSeqNum)
	jsonData, err := json.Marshal(connRequest)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Write(jsonData); err != nil {
		return nil, err
	}
	c := &client{
		conn:                      conn,
		readChannel:               make(chan *Message, 1),
		readSigChan:               make(chan struct{}),
		params:                    *params,
		connID:                    0,
		serverMessageChannel:      make(chan Message),
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
		currentAckNumOpChan:       make(chan currentAckNumOp),

		connIDOpChan:        make(chan connIDOp),
		unAckMessageMap:     make(chan map[int]*massageWithEpoch, 1),
		currentEpoch:        make(chan int, 1),
		nonResponseEpochSum: make(chan int, 1),
		connSetUpChan:       make(chan int),
		readChannelQueue:    make(chan *Queue, 1),

		isClosed:                 make(chan struct{}, 1),
		pendingMessageFinalClose: make(chan struct{}, 1),
	}
	c.currentEpoch <- 0
	c.readChannelQueue <- NewQueue()
	c.unAckMessageMap <- make(map[int]*massageWithEpoch)
	c.nonResponseEpochSum <- -1
	go c.setUpConnection()
	for {
		select {
		case connId := <-c.connSetUpChan:
			c.connID = connId
			go c.managePendingMessageQueueOps()
			go c.manageLeftWindowSizeOps()
			go c.manageCurrentWriteSeqNumOps()
			go c.manageCurrentAckNumOps()
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

// get connection id
func (c *client) ConnID() int {
	temp_op := connIDOp{
		opType:   "read",
		respChan: make(chan int),
	}
	c.connIDOpChan <- temp_op
	connID := <-temp_op.respChan
	return connID
}

func (c *client) Read() ([]byte, error) {
	for {
		select {
		case <-c.pendingMessageFinalClose:
			c.pendingMessageFinalClose <- struct{}{}
			return nil, errors.New("pendingMessageFinalClose")
		case data, ok := <-c.readChannel:
			if !ok {
				// readChannel has been closed
				return nil, errors.New("readChannel has been closed")
			}
			return data.Payload, nil
		}
	}
}

func (c *client) Write(payload []byte) error {
	// the sequence number depends on the order of the Write calls
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
	select {
	case <-c.isClosed:
		c.isClosed <- struct{}{}
	default:
		c.isClosed <- struct{}{}
	}

	<-c.pendingMessageFinalClose
	c.pendingMessageFinalClose <- struct{}{}

	return c.conn.Close()
}
