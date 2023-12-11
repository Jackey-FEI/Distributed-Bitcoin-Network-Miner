// Contains the implementation of a LSP client.

// TODO: calculateChecksum and save it to message struct before marshalling it to []byte
package lsp

import (
	"bytes"
	"encoding/gob"
	"errors"
	"log"

	"github.com/cmu440/lspnet"
)

type PendingMessage struct {
	SequenceNumber int
	Payload        []byte
	connID         int
	udpAddr        *lspnet.UDPAddr
}

type PendingMessageQueueOp struct {
	opType         string // "keepPop" or "insert"
	pendingMessage *PendingMessage
}

func (c *client) managePendingMessageQueueOps() {
	for {
		select {
		case op := <-c.pendingMessageQueueOpChan:
			switch op.opType {
			case "keepPop":
				temp_op := LeftWindowSizeOp{
					opType:   "read",
					respChan: make(chan int),
				}
				c.LeftWindowSizeOpChan <- temp_op
				curLeftWindowSize := <-temp_op.respChan

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

				// if (curWriteSeqNum <= curLeftWindowSize+c.params.WindowSize-1) ||
				//    ((curAckNum - curWriteSeqNum) > c.params.MaxUnackedMessages) {
				if c.pendingMessageQueue.IsEmpty() {
					continue
				}
				for (c.pendingMessageQueue.Peek().(*PendingMessage).SequenceNumber <= curLeftWindowSize+c.params.WindowSize-1) &&
					((curWriteSeqNum - curAckNum) <= c.params.MaxUnackedMessages) {
					messageToSend := c.pendingMessageQueue.Dequeue().(*PendingMessage).Payload
					// 序列化消息
					var buffer bytes.Buffer
					enc := gob.NewEncoder(&buffer)
					if err := enc.Encode(messageToSend); err != nil {
						// Handle error
						log.Printf("Error encoding message: %v", err)
						continue
					}
					// 发送消息
					if _, err := c.conn.Write(buffer.Bytes()); err != nil {
						// Handle error
						log.Printf("Error sending message: %v", err)
					}

				}
			case "insert":
				c.pendingMessageQueue.Enqueue(&PendingMessage{op.pendingMessage.SequenceNumber, op.pendingMessage.Payload, op.pendingMessage.connID, op.pendingMessage.udpAddr})
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

type pqReadMessage struct {
	SequenceNumber int
	Payload        []byte
	connID         int
	udpAddr        *lspnet.UDPAddr
}
type pqReadOp struct {
	opType          string // "length" or "peek" or "insert" or "pop"
	respChan        chan int
	respChanLen     chan int
	respChanMessage chan pqReadMessage
	value           pqReadMessage //value of insert
}

func (c *client) managePqReadOps() {
	for {
		select {
		case op := <-c.PqReadOpChan:
			switch op.opType {
			case "length":
				op.respChanLen <- c.pqRead.Len()
			case "peek":
				op.respChanMessage <- c.pqRead.Top().(pqReadMessage)
			case "insert":
				c.pqRead.Insert(op.value, float64(op.value.SequenceNumber))
			case "pop":
				c.pqRead.Pop()
			}
		}
	}
}

// type pqACKOp struct {
// 	opType   string // "insert" or "pop" or "peek"
// 	value    int
// 	respChan chan int
// }

// func (c *client) managePQACKOps() {
// 	for {
// 		select {
// 		case op := <-c.pqACKOpChan:
// 			switch op.opType {
// 			case "insert":
// 				c.pqACK.Insert(op.value, float64(op.value))
// 			case "peek":
// 				op.respChan <- first(c.pqACK.Top()).(int)
// 			case "pop":
// 				c.pqACK.Pop()
// 			}
// 		}
// 	}
// }

type client struct {
	conn                      *lspnet.UDPConn
	udpAddrr                  *lspnet.UDPAddr
	readChannel               chan []byte
	connID                    int
	readSigChan               chan struct{}
	params                    Params
	serverMessageChannel      chan Message
	closeChannel              chan struct{}
	windowSizeLeft            int
	currentReadSeqNum         int // should be the next one to be processed
	currentWriteSeqNum        int // should be the oldest that have been processed
	currentAckNum             int
	pendingMessageQueue       Queue
	pqRead                    PriorityQueue // process reading from server
	pqACK                     PriorityQueue // process writing to server
	pendingMessageQueueOpChan chan PendingMessageQueueOp
	LeftWindowSizeOpChan      chan LeftWindowSizeOp
	currentWriteSeqNumOpChan  chan currentWriteSeqNumOp
	currentReadSeqNumOpChan   chan currentReadSeqNumOp
	currentAckNumOpChan       chan currentAckNumOp
	PqReadOpChan              chan pqReadOp
	// pqACKOpChan               chan pqACKOp
}

func (c *client) readProcessRoutine() {
	buf := make([]byte, 2048)
	log.Printf("client conn number is: %d", c.connID)
	for {
		n, _, err := c.conn.ReadFromUDP(buf)
		if err != nil {
			log.Println("Read Error: ", err)
			continue
		}

		var msg Message
		dec := gob.NewDecoder(bytes.NewBuffer(buf[:n]))
		if err := dec.Decode(&msg); err != nil {
			log.Println("Decode Error: ", err)
			continue
		}
		log.Printf("receive in client: %d", msg.Type)
		c.serverMessageChannel <- msg
	}
}

func (c *client) mainRoutine() {
	for {
		select {
		case <-c.closeChannel:
			// 客户端已关闭，退出协程

			return
		case msg := <-c.serverMessageChannel:

			if msg.Type == MsgData {
				// ACK back to server
				log.Printf("type == MsgData in client")
				ackMsg := NewAck(msg.ConnID, msg.SeqNum)
				var buffer bytes.Buffer
				enc := gob.NewEncoder(&buffer)
				if err := enc.Encode(ackMsg); err != nil {
					// error of encode
					continue
				}
				// send back to server encoded buffer
				if _, err := c.conn.Write(buffer.Bytes()); err != nil {
					// deal with error
					continue
				}

				newPqReadMessage := pqReadMessage{
					SequenceNumber: msg.SeqNum,
					Payload:        msg.Payload,
					connID:         msg.ConnID,
					udpAddr:        c.udpAddrr,
				}
				pqReadOp := pqReadOp{
					opType: "insert",
					value:  newPqReadMessage,
				}
				log.Printf("i am insert into pqreadopchan")
				c.PqReadOpChan <- pqReadOp

			} else if msg.Type == MsgAck {
				c.connID = msg.ConnID
				log.Print(c.connID, "oooooooo")
				temp_op0 := currentAckNumOp{
					opType: "plus",
				}
				c.currentAckNumOpChan <- temp_op0
				log.Printf("type == Msgack in client")
				temp_op := LeftWindowSizeOp{
					opType:   "read",
					respChan: make(chan int),
				}
				c.LeftWindowSizeOpChan <- temp_op
				curLeftWindowSize := <-temp_op.respChan

				if msg.SeqNum == (curLeftWindowSize) {
					/*update leftWindow */
					newOp := LeftWindowSizeOp{
						opType: "plus",
					}
					c.LeftWindowSizeOpChan <- newOp

					/*update continuously from pqACK queue*/
					lastLeft := msg.SeqNum
					if c.pqACK.Len() != 0 {
						for {
							if c.pqACK.Len() != 0 && (lastLeft) == c.pqACK.Top().(int) {
								lastLeft++
								c.pqACK.Pop()
								c.LeftWindowSizeOpChan <- newOp
							} else {
								break
							}
						}
					}

					//TODO:
					/*update pending Message queue, pending message can be sent now*/
					pendMessageOp := PendingMessageQueueOp{
						opType: "keepPop",
					}
					c.pendingMessageQueueOpChan <- pendMessageOp
					//
				} else {
					c.pqACK.Insert(msg.SeqNum, float64(msg.SeqNum))
				}
				log.Printf("out of msgACK in main routine in client")

			} else if msg.Type == MsgCAck {
				c.connID = msg.ConnID
				log.Print(c.connID, "ppppppp")
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

				if msg.SeqNum >= (curLeftWindowSize) {
					for {
						if c.pqACK.Len() != 0 && msg.SeqNum >= c.pqACK.Top().(int) {
							c.pqACK.Pop()
						} else {
							break
						}
					}

					plusOp := LeftWindowSizeOp{
						opType: "plus",
					}

					lastLeft := msg.SeqNum
					for {
						if c.pqACK.Len() != 0 && (lastLeft) == c.pqACK.Top().(int) {
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
		case <-c.readSigChan:
			// implement pqread here
			// a reader comes
			log.Print("the routine in client starts now")
			go c.readServerProcess()
		default:
			continue
		}
	}
}

func (c *client) readServerProcess() {
	readPqOp0 := pqReadOp{
		opType:      "length",
		respChanLen: make(chan int),
	}
	c.PqReadOpChan <- readPqOp0
	curPqReadLen := <-readPqOp0.respChanLen
	for curPqReadLen == 0 {
		readPqOp1 := pqReadOp{
			opType:      "length",
			respChanLen: make(chan int),
		}
		c.PqReadOpChan <- readPqOp1
		curPqReadLen = <-readPqOp1.respChanLen
	}
	readPqOp2 := pqReadOp{
		opType:          "peek",
		respChanMessage: make(chan pqReadMessage),
	}
	log.Print("finish checking the pqread")
	c.PqReadOpChan <- readPqOp2
	curPqReadTop := <-readPqOp2.respChanMessage
	temp_op := currentReadSeqNumOp{
		opType:   "read",
		respChan: make(chan int),
	}
	c.currentReadSeqNumOpChan <- temp_op
	curReadSeqNum := <-temp_op.respChan
	log.Printf("Reader client input into readChannel before check")
	for (curReadSeqNum + 1) != (curPqReadTop.SequenceNumber) {
		temp_op := currentReadSeqNumOp{
			opType:   "read",
			respChan: make(chan int),
		}
		c.currentReadSeqNumOpChan <- temp_op
		curReadSeqNum = <-temp_op.respChan
	}
	if (curReadSeqNum + 1) == (curPqReadTop.SequenceNumber) {
		readPqOp3 := pqReadOp{
			opType: "pop",
		}
		c.PqReadOpChan <- readPqOp3
		item := curPqReadTop.Payload
		log.Printf("Reader input into readChannel now...")
		c.readChannel <- item
		temp_op := currentReadSeqNumOp{
			opType: "plus",
		}
		c.currentReadSeqNumOpChan <- temp_op

		ackMsg := NewAck(c.connID, curPqReadTop.SequenceNumber)
		var buffer bytes.Buffer
		enc := gob.NewEncoder(&buffer)
		if err := enc.Encode(ackMsg); err != nil {
			// error of encode
		}
		if _, err := c.conn.Write(buffer.Bytes()); err != nil {
			// deal with error
			log.Printf("error of writing back ack after connect")
		}
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
	//send connection request
	connRequest := NewConnect(initialSeqNum)
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	if err := enc.Encode(connRequest); err != nil {
		// Handle error and return
		log.Printf("Error encoding message: %v", err)
		return nil, err
	}
	if _, err := conn.Write(buffer.Bytes()); err != nil {
		log.Printf("Error sending message: %v", err)
		return nil, err
	}

	c := &client{
		conn:                      conn,
		udpAddrr:                  raddr,
		readChannel:               make(chan []byte),
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
		pqRead:                    NewPriorityQueue(),
		pqACK:                     NewPriorityQueue(),
		pendingMessageQueueOpChan: make(chan PendingMessageQueueOp),
		LeftWindowSizeOpChan:      make(chan LeftWindowSizeOp),
		currentWriteSeqNumOpChan:  make(chan currentWriteSeqNumOp),
		currentReadSeqNumOpChan:   make(chan currentReadSeqNumOp),
		currentAckNumOpChan:       make(chan currentAckNumOp),
		PqReadOpChan:              make(chan pqReadOp),
		// pqACKOpChan:               make(chan pqACKOp),
	}

	buf := make([]byte, 2048)
	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Println("Read Error: ", err)
			continue
		}

		var msg Message
		dec := gob.NewDecoder(bytes.NewBuffer(buf[:n]))
		if err := dec.Decode(&msg); err != nil {
			log.Println("Decode Error: ", err)
			continue
		}
		log.Printf("receive in client: %d", msg.Type)
		c.connID = msg.ConnID
		break
		//c.serverMessageChannel <- msg
	}

	go c.managePendingMessageQueueOps()
	go c.manageLeftWindowSizeOps()
	go c.manageCurrentWriteSeqNumOps()
	go c.manageCurrentAckNumOps()
	go c.manageCurrentReadSeqNumOps()
	go c.managePqReadOps()
	go c.readProcessRoutine()
	go c.mainRoutine()
	return c, nil
}

func (c *client) ConnID() int {
	return c.connID
}

// it is ok if it blocks forever.
func (c *client) Read() ([]byte, error) {
	c.readSigChan <- struct{}{}
	log.Println("readSigChan insert in client Read() is ok")
	for {
		select {
		case data, ok := <-c.readChannel:

			if !ok {
				// readChannel 已关闭
				return nil, errors.New("readChannel has been closed")
			}

			return data, nil
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

	log.Print(curWriteSeqNum, " client write seqnum")
	log.Print(curLeftWindowSize+c.params.WindowSize-1, " client write right window")
	if (curWriteSeqNum <= curLeftWindowSize+c.params.WindowSize-1) &&
		((curWriteSeqNum - curAckNum) <= c.params.MaxUnackedMessages) {
		messageToSend := NewData(c.ConnID(), curWriteSeqNum, int(len(payload)), payload,
			CalculateChecksum(c.ConnID(), curWriteSeqNum, int(len(payload)), payload))
		var buffer bytes.Buffer
		enc := gob.NewEncoder(&buffer)
		if err := enc.Encode(messageToSend); err != nil {
			// Handle error and return
			log.Printf("Error encoding message: %v", err)
			return err
		}
		log.Printf("client write with connid: %d", c.ConnID())
		if _, err := c.conn.Write(buffer.Bytes()); err != nil {
			// Handle error and return
			log.Printf("Error sending message: %v", err)
			return err
		}
	} else {
		temp_op := PendingMessageQueueOp{
			opType: "insert",
			pendingMessage: &PendingMessage{
				SequenceNumber: curWriteSeqNum,
				Payload:        payload,
				connID:         c.ConnID(),
				udpAddr:        c.udpAddrr,
			},
		}

		c.pendingMessageQueueOpChan <- temp_op
	}
	return nil
}

func (c *client) Close() error {
	c.closeChannel <- struct{}{}
	return c.conn.Close()
}
