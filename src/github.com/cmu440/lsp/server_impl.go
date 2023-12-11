// Contains the implementation of a LSP server.

package lsp

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"log"

	"github.com/cmu440/lspnet"
)

type s_PendingMessage struct {
	SequenceNumber int
	Payload        []byte
	connID         int
	udpAddr        *lspnet.UDPAddr
}

type s_pqReadMessage struct {
	SequenceNumber int
	Payload        []byte
	connID         int
	udpAddr        *lspnet.UDPAddr
}

type s_PendingMessageQueueOp struct {
	opType           string // "keepPop" or "insert"
	s_pendingMessage *s_PendingMessage
}

func (s *server) managePendingMessageQueueOps() {
	for {
		select {
		case op := <-s.pendingMessageQueueOpChan:
			switch op.opType {
			case "keepPop":
				temp_op := s_LeftWindowSizeOp{
					opType:   "read",
					respChan: make(chan int),
				}
				s.LeftWindowSizeOpChan <- temp_op
				curLeftWindowSize := <-temp_op.respChan

				temp_op2 := s_currentWriteSeqNumOp{
					opType:   "read",
					respChan: make(chan int),
				}
				s.currentWriteSeqNumOpChan <- temp_op2
				curWriteSeqNum := <-temp_op2.respChan

				temp_op3 := s_currentAckNumOp{
					opType:   "read",
					respChan: make(chan int),
				}
				s.currentAckNumOpChan <- temp_op3
				curAckNum := <-temp_op3.respChan

				// if (curWriteSeqNum <= curLeftWindowSize+c.params.WindowSize-1) ||
				//    ((curAckNum - curWriteSeqNum) > c.params.MaxUnackedMessages) {
				if s.pendingMessageQueue.IsEmpty() {
					continue
				}
				for (s.pendingMessageQueue.Peek().(*s_PendingMessage).SequenceNumber <= curLeftWindowSize+s.params.WindowSize-1) &&
					((curWriteSeqNum - curAckNum) <= s.params.MaxUnackedMessages) {
					messageToSend := s.pendingMessageQueue.Dequeue().(*s_PendingMessage).Payload
					var buffer bytes.Buffer
					enc := gob.NewEncoder(&buffer)
					if err := enc.Encode(messageToSend); err != nil {
						// Handle error
						log.Printf("Error encoding message: %v", err)
						continue
					}
					// send message
					if _, err := s.conn.WriteToUDP(buffer.Bytes(), s.pendingMessageQueue.Peek().(*s_PendingMessage).udpAddr); err != nil {
						// Handle error
						log.Printf("Error sending message: %v", err)
					}

				}
			case "insert":
				s.pendingMessageQueue.Enqueue(&s_PendingMessage{op.s_pendingMessage.SequenceNumber, op.s_pendingMessage.Payload, op.s_pendingMessage.connID, op.s_pendingMessage.udpAddr})
			}
		}
	}
}

type s_LeftWindowSizeOp struct {
	opType   string // "plus" or "minus" or "read" or "set"
	value    int    // value of set
	respChan chan int
}

func (s *server) manageLeftWindowSizeOps() {
	for {
		select {
		case op := <-s.LeftWindowSizeOpChan:
			switch op.opType {
			case "plus":
				s.windowSizeLeft++
			case "minus":
				s.windowSizeLeft--
			case "read":
				op.respChan <- s.windowSizeLeft
			case "set":
				s.windowSizeLeft = op.value
			}
		}
	}
}

type s_currentWriteSeqNumOp struct {
	opType   string // "plus" or "read"
	value    int    //value of set
	respChan chan int
}

func (s *server) manageCurrentWriteSeqNumOps() {
	for {
		select {
		case op := <-s.currentWriteSeqNumOpChan:
			switch op.opType {
			case "plus":
				s.currentWriteSeqNum++
			case "read":
				op.respChan <- s.currentWriteSeqNum
			case "set":
				s.currentWriteSeqNum = op.value
			}
		}
	}
}

type s_currentReadSeqNumOp struct {
	opType   string // "plus" or "read" or "set"
	respChan chan int
	value    int //value of set
}

func (s *server) manageCurrentReadSeqNumOps() {
	for {
		select {
		case op := <-s.currentReadSeqNumOpChan:
			switch op.opType {
			case "plus":
				s.currentReadSeqNum++
			case "read":
				op.respChan <- s.currentReadSeqNum
			case "set":
				s.currentReadSeqNum = op.value
			}
		}
	}
}

type s_currentAckNumOp struct {
	opType   string // "plus" or "read" or "set"
	respChan chan int
	value    int //value of set
}

func (s *server) manageCurrentAckNumOps() {
	for {
		select {
		case op := <-s.currentAckNumOpChan:
			switch op.opType {
			case "plus":
				s.currentAckNum++
			case "read":
				op.respChan <- s.currentAckNum
			case "set":
				s.currentAckNum = op.value
			}
		}
	}
}

type s_currentConnIDOp struct {
	opType   string // "plus" or "read"
	respChan chan int
}

func (s *server) manageCurrentConnIDOps() {
	for {
		select {
		case op := <-s.currentConnIDOpChan:
			switch op.opType {
			case "plus":
				s.currentConnID++
			case "read":
				op.respChan <- s.currentConnID
			}
		}
	}
}

type s_connIDToAddrOp struct {
	opType       string // "insert" or "read"
	valueConnID  int
	valueUDPAddr *lspnet.UDPAddr
	respChan     chan *lspnet.UDPAddr
}

func (s *server) manageConnIDToAddrOps() {
	for {
		select {
		case op := <-s.connIDToAddrOpChan:
			switch op.opType {
			case "insert":
				s.connIDToAddr[op.valueConnID] = op.valueUDPAddr
			case "read":
				op.respChan <- s.connIDToAddr[op.valueConnID]
			}
		}
	}
}

type s_pqReadOp struct {
	opType          string // "length" or "peek" or "insert" or "pop"
	respChanLen     chan int
	respChanMessage chan s_pqReadMessage
	value           s_pqReadMessage //value of insert
}

func (s *server) managePqReadOps() {
	for {
		select {
		case op := <-s.PqReadOpChan:
			switch op.opType {
			case "length":
				op.respChanLen <- s.pqRead.Len()
			case "peek":
				op.respChanMessage <- (s.pqRead.Top()).(s_pqReadMessage)
			case "insert":
				s.pqRead.Insert(op.value, float64(op.value.SequenceNumber))
			case "pop":
				s.pqRead.Pop()
			}
		}
	}
}

type messagewithUDP struct {
	udpAddr *lspnet.UDPAddr
	msg     Message //message
}

type server struct {
	// addrToConnID map[*lspnet.UDPAddr]int

	connIDToAddr map[int]*lspnet.UDPAddr

	conn                      *lspnet.UDPConn
	readChannel               chan s_pqReadMessage
	connID                    int
	readSigChan               chan struct{}
	params                    Params
	serverMessageChannel      chan messagewithUDP
	closeChannel              chan struct{}
	windowSizeLeft            int
	currentReadSeqNum         int // should be the next one to be processed
	currentWriteSeqNum        int // should be the oldest that have been processed
	currentAckNum             int
	currentConnID             int // generated by server, give to client, plus 1 every time
	pendingMessageQueue       Queue
	pqRead                    PriorityQueue // process reading from server
	pqACK                     PriorityQueue // process writing to server
	pendingMessageQueueOpChan chan s_PendingMessageQueueOp
	LeftWindowSizeOpChan      chan s_LeftWindowSizeOp
	currentWriteSeqNumOpChan  chan s_currentWriteSeqNumOp
	currentReadSeqNumOpChan   chan s_currentReadSeqNumOp
	currentAckNumOpChan       chan s_currentAckNumOp
	currentConnIDOpChan       chan s_currentConnIDOp
	connIDToAddrOpChan        chan s_connIDToAddrOp
	PqReadOpChan              chan s_pqReadOp
	// pqACKOpChan               chan pqACKOp

}

func (s *server) readProcessRoutine() {
	buf := make([]byte, 2048)
	for {
		n, addr, err := s.conn.ReadFromUDP(buf)
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
		// if msg.ConnID != 0 { //every update?
		// 	temp_op := s_connIDToAddrOp{
		// 		opType:       "insert",
		// 		valueConnID:  msg.ConnID,
		// 		valueUDPAddr: addr,
		// 	}
		// 	s.connIDToAddrOpChan <- temp_op
		// 	// s.connIDToAddr[msg.ConnID] = addr //TODO: how to not update it everytimes
		// }
		log.Printf("my connid: %d", msg.ConnID)
		// 可以使用 addr 来区分不同的客户端
		// log.Printf("Received a message from %s\n", addr.String())

		// 根据需要进行更多的处理，例如保存客户端状态等
		// ...
		msgWithUDP := messagewithUDP{
			udpAddr: addr,
			msg:     msg,
		}

		s.serverMessageChannel <- msgWithUDP
	}
}

func (s *server) mainRoutine() {
	for {
		select {
		case <-s.closeChannel:
			return
		case messagewithUDP := <-s.serverMessageChannel:
			msg := messagewithUDP.msg
			msgUDPaddr := messagewithUDP.udpAddr
			if msg.Type == MsgData {
				// ACK back to server
				ackMsg := NewAck(msg.ConnID, msg.SeqNum)
				var buffer bytes.Buffer
				enc := gob.NewEncoder(&buffer)
				if err := enc.Encode(ackMsg); err != nil {
					// error of encode
					continue
				}
				// send back to server encoded buffer
				if _, err := s.conn.WriteToUDP(buffer.Bytes(), msgUDPaddr); err != nil {
					// deal with error
					continue
				}
				newPqReadMessage := s_pqReadMessage{
					SequenceNumber: msg.SeqNum,
					Payload:        msg.Payload,
					connID:         msg.ConnID,
					udpAddr:        msgUDPaddr,
				}
				pqReadOp := s_pqReadOp{
					opType: "insert",
					value:  newPqReadMessage,
				}
				s.PqReadOpChan <- pqReadOp

			} else if msg.Type == MsgConnect {

				temp_op00 := s_currentConnIDOp{
					opType: "plus",
				}
				s.currentConnIDOpChan <- temp_op00

				temp_op := s_currentConnIDOp{
					opType:   "read",
					respChan: make(chan int),
				}
				s.currentConnIDOpChan <- temp_op
				currentConnID_toClient := <-temp_op.respChan

				temp_op11 := s_connIDToAddrOp{
					opType:       "insert",
					valueConnID:  currentConnID_toClient,
					valueUDPAddr: msgUDPaddr,
				}
				s.connIDToAddrOpChan <- temp_op11

				// ACK back to client
				temp_op0 := s_currentAckNumOp{
					opType: "set",
					value:  msg.SeqNum,
				}
				s.currentAckNumOpChan <- temp_op0

				temp_op1 := s_currentWriteSeqNumOp{
					opType: "set",
					value:  msg.SeqNum,
				}
				s.currentWriteSeqNumOpChan <- temp_op1

				temp_op2 := s_currentReadSeqNumOp{
					opType: "set",
					value:  msg.SeqNum,
				}
				s.currentReadSeqNumOpChan <- temp_op2

				temp_op3 := s_LeftWindowSizeOp{
					opType: "set",
					value:  msg.SeqNum + 1, // why you add +1 here?
				}
				s.LeftWindowSizeOpChan <- temp_op3
				// this connID should be somehow implemented
				ackMsg := NewAck(currentConnID_toClient, msg.SeqNum) //TODO: use own generate id
				var buffer bytes.Buffer
				enc := gob.NewEncoder(&buffer)
				if err := enc.Encode(ackMsg); err != nil {
					// error of encode
					continue
				}
				// send back to server encoded buffer
				if _, err := s.conn.WriteToUDP(buffer.Bytes(), msgUDPaddr); err != nil {
					// deal with error
					log.Printf("error of writing back ack after connect")
					continue
				}

			} else if msg.Type == MsgAck {
				//s.connID = msg.ConnID
				temp_op0 := s_currentAckNumOp{
					opType: "plus",
				}
				s.currentAckNumOpChan <- temp_op0

				temp_op := s_LeftWindowSizeOp{
					opType:   "read",
					respChan: make(chan int),
				}
				s.LeftWindowSizeOpChan <- temp_op
				curLeftWindowSize := <-temp_op.respChan

				if msg.SeqNum == (curLeftWindowSize) {
					/*update leftWindow */
					newOp := s_LeftWindowSizeOp{
						opType: "plus",
					}
					s.LeftWindowSizeOpChan <- newOp

					/*update continuously from pqACK queue*/
					lastLeft := msg.SeqNum
					if s.pqACK.Len() != 0 {
						for s.pqACK.Len() > 0 {
							if (lastLeft) == s.pqACK.Top().(int) {
								lastLeft++
								s.pqACK.Pop()
								s.LeftWindowSizeOpChan <- newOp
							} else {
								break
							}
						}
					}
					//TODO:
					/*update pending Message queue, pending message can be sent now*/
					pendMessageOp := s_PendingMessageQueueOp{
						opType: "keepPop",
					}
					s.pendingMessageQueueOpChan <- pendMessageOp
					//
				} else {
					s.pqACK.Insert(msg.SeqNum, float64(msg.SeqNum))
				}

			} else if msg.Type == MsgCAck {
				//s.connID = msg.ConnID

				temp_op := s_LeftWindowSizeOp{
					opType:   "read",
					respChan: make(chan int),
				}
				s.LeftWindowSizeOpChan <- temp_op
				curLeftWindowSize := <-temp_op.respChan

				temp_op1 := s_currentAckNumOp{
					opType: "set",
					value:  msg.SeqNum,
				}
				s.currentAckNumOpChan <- temp_op1

				if msg.SeqNum >= (curLeftWindowSize) {
					for {
						if s.pqACK.Len() > 0 && msg.SeqNum >= s.pqACK.Top().(int) {
							s.pqACK.Pop()
						} else {
							break
						}
					}

					plusOp := s_LeftWindowSizeOp{
						opType: "plus",
					}

					lastLeft := msg.SeqNum
					for {
						if s.pqACK.Len() != 0 && (lastLeft) == s.pqACK.Top().(int) {
							lastLeft++
							s.pqACK.Pop()
							s.LeftWindowSizeOpChan <- plusOp
						} else {
							break
						}
					}

					temp_op1 := s_LeftWindowSizeOp{
						opType: "set",
						value:  lastLeft, //
					}
					s.LeftWindowSizeOpChan <- temp_op1

					pendMessageOp := s_PendingMessageQueueOp{
						opType: "keepPop",
					}
					s.pendingMessageQueueOpChan <- pendMessageOp
				}

			}
		case <-s.readSigChan:
			log.Print("Server start a new readServerProcess()")
			go s.readServerProcess()

		default:
			continue
		}
	}
}

// func (s *server) manageConnections() {
// 	for {
// 		buf := make([]byte, 1024)
// 		_, addr, err := s.conn.ReadFromUDP(buf)
// 		if err != nil {
// 			// 处理错误
// 			continue
// 		}

// 		// 检查这个地址是否已经有了一个连接
// 		_, exists := s.addrToConnID[addr]
// 		if !exists {
// 			s.acceptNewConn(addr)
// 		}
// 	}
// }

func (s *server) readServerProcess() {
	readPqOp0 := s_pqReadOp{
		opType:      "length",
		respChanLen: make(chan int),
	}
	s.PqReadOpChan <- readPqOp0
	curPqReadLen := <-readPqOp0.respChanLen
	for curPqReadLen == 0 {
		readPqOp1 := s_pqReadOp{
			opType:      "length",
			respChanLen: make(chan int),
		}
		s.PqReadOpChan <- readPqOp1
		curPqReadLen = <-readPqOp1.respChanLen
	}
	readPqOp2 := s_pqReadOp{
		opType:          "peek",
		respChanMessage: make(chan s_pqReadMessage),
	}
	s.PqReadOpChan <- readPqOp2

	curPqReadTop := <-readPqOp2.respChanMessage

	temp_op := s_currentReadSeqNumOp{
		opType:   "read",
		respChan: make(chan int),
	}
	s.currentReadSeqNumOpChan <- temp_op
	curReadSeqNum := <-temp_op.respChan
	log.Print(curReadSeqNum, ", serverReadseqNum")
	log.Print(curPqReadTop.SequenceNumber, ", serverTopSeqNum")
	log.Print((curReadSeqNum + 1) == (curPqReadTop.SequenceNumber))

	for (curReadSeqNum + 1) != (curPqReadTop.SequenceNumber) {
		temp_op1 := s_currentReadSeqNumOp{
			opType:   "read",
			respChan: make(chan int),
		}
		s.currentReadSeqNumOpChan <- temp_op1
		curReadSeqNum = <-temp_op1.respChan

		readPqOp3 := s_pqReadOp{
			opType:          "peek",
			respChanMessage: make(chan s_pqReadMessage),
		}
		s.PqReadOpChan <- readPqOp3

		curPqReadTop = <-readPqOp3.respChanMessage
	}
	if (curReadSeqNum + 1) == (curPqReadTop.SequenceNumber) {

		readPqOp3 := s_pqReadOp{
			opType: "pop",
		}
		s.PqReadOpChan <- readPqOp3
		//item := curPqReadTop.Payload

		s.readChannel <- curPqReadTop
		//log.Print(item)
		log.Print("Server has input item into readChannel.")
		temp_op := s_currentReadSeqNumOp{
			opType: "plus",
		}
		s.currentReadSeqNumOpChan <- temp_op

		ackMsg := NewAck(1, curPqReadTop.SequenceNumber)
		var buffer bytes.Buffer
		enc := gob.NewEncoder(&buffer)
		if err := enc.Encode(ackMsg); err != nil {
			// error of encode
		}
		if _, err := s.conn.WriteToUDP(buffer.Bytes(), curPqReadTop.udpAddr); err != nil {
			// deal with error
			log.Printf("error of writing back ack after connect")
		}
	}
	log.Print("Server finish readServerProcess")
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	log.Print("NewServer9999")
	laddr, err := lspnet.ResolveUDPAddr("udp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}
	conn, err := lspnet.ListenUDP("udp", laddr)
	if err != nil {
		return nil, err
	}

	// go s.manageConnections()
	// return s, nil
	s := &server{
		conn: conn,
		// addrToConnID:              make(map[*lspnet.UDPAddr]int),
		readChannel:               make(chan s_pqReadMessage),
		readSigChan:               make(chan struct{}),
		params:                    *params,
		connID:                    0,
		serverMessageChannel:      make(chan messagewithUDP),
		closeChannel:              make(chan struct{}),
		windowSizeLeft:            params.WindowSize,
		currentReadSeqNum:         0,
		currentWriteSeqNum:        0,
		currentAckNum:             0,
		pendingMessageQueue:       NewQueue(),
		pqRead:                    NewPriorityQueue(),
		pqACK:                     NewPriorityQueue(),
		pendingMessageQueueOpChan: make(chan s_PendingMessageQueueOp),
		LeftWindowSizeOpChan:      make(chan s_LeftWindowSizeOp),
		currentWriteSeqNumOpChan:  make(chan s_currentWriteSeqNumOp),
		currentReadSeqNumOpChan:   make(chan s_currentReadSeqNumOp),
		currentAckNumOpChan:       make(chan s_currentAckNumOp),
		currentConnIDOpChan:       make(chan s_currentConnIDOp),
		connIDToAddrOpChan:        make(chan s_connIDToAddrOp),
		PqReadOpChan:              make(chan s_pqReadOp),
		connIDToAddr:              make(map[int]*lspnet.UDPAddr),
		// pqACKOpChan:               make(chan pqACKOp),
	}

	go s.managePendingMessageQueueOps()
	go s.manageLeftWindowSizeOps()
	go s.manageCurrentWriteSeqNumOps()
	go s.manageCurrentAckNumOps()
	go s.manageCurrentConnIDOps()
	go s.manageConnIDToAddrOps()
	go s.manageCurrentReadSeqNumOps()
	go s.managePqReadOps()
	go s.readProcessRoutine()
	go s.mainRoutine()

	return s, nil
}

// func (s *server) acceptNewConn(addr *lspnet.UDPAddr) int {
// 	connID := len(s.addrToConnID) + 1
// 	s.addrToConnID[addr] = connID
// 	s.connIDToAddr[connID] = addr
// 	return connID
// }

func (s *server) Read() (int, []byte, error) {
	log.Print("Server Read() is called")
	// buf := make([]byte, 1024)
	// n, addr, err := s.conn.ReadFromUDP(buf)
	// if err != nil {
	// 	return -1, nil, err
	// }

	// connID, exists := s.addrToConnID[addr]
	// if !exists {
	// 	return -1, nil, errors.New("unknown connection")
	// }

	// return connID, buf[:n], nil
	s.readSigChan <- struct{}{}
	for {
		log.Print("enter for loop in server Read()")
		select {
		case data, ok := <-s.readChannel:
			log.Print("Server readChannel do get data.")
			if !ok {
				// readChannel 已关闭
				return data.connID, nil, errors.New("readChannel has been closed")
			}
			return data.connID, data.Payload, nil
		case <-s.closeChannel:
			// 客户端已关闭
			return s.connID, nil, errors.New("server has been closed")
		}
	}
}

// func (s *server) findAddrByConnID(connID int) *lspnet.UDPAddr {
// 	// s.mutex.Lock()
// 	addr := s.connIDToAddr[connID]
// 	// s.mutex.Unlock()
// 	return addr
// }

func (s *server) Write(connId int, payload []byte) error {
	log.Printf("Server 66666")
	// addr := s.findAddrByConnID(connId)
	// if addr == nil {
	// 	return errors.New("invalid connection ID")
	// }

	// _, err := s.conn.WriteToUDP(payload, addr)
	// return err
	temp_op := s_currentWriteSeqNumOp{
		opType: "plus",
	}
	s.currentWriteSeqNumOpChan <- temp_op

	temp_op1 := s_LeftWindowSizeOp{
		opType:   "read",
		respChan: make(chan int),
	}
	s.LeftWindowSizeOpChan <- temp_op1
	curLeftWindowSize := <-temp_op1.respChan

	temp_op2 := s_currentWriteSeqNumOp{
		opType:   "read",
		respChan: make(chan int),
	}
	s.currentWriteSeqNumOpChan <- temp_op2
	curWriteSeqNum := <-temp_op2.respChan

	temp_op3 := s_currentAckNumOp{
		opType:   "read",
		respChan: make(chan int),
	}
	s.currentAckNumOpChan <- temp_op3
	curAckNum := <-temp_op3.respChan

	log.Print(curWriteSeqNum, " server write seqnum")
	log.Print(curLeftWindowSize+s.params.WindowSize-1, " server write right window")
	if (curWriteSeqNum <= curLeftWindowSize+s.params.WindowSize-1) &&
		((curWriteSeqNum - curAckNum) <= s.params.MaxUnackedMessages) {
		messageToSend := NewData(connId, curWriteSeqNum, int(len(payload)), payload,
			CalculateChecksum(connId, curWriteSeqNum, int(len(payload)), payload))
		var buffer bytes.Buffer
		enc := gob.NewEncoder(&buffer)
		if err := enc.Encode(messageToSend); err != nil {
			// Handle error and return
			log.Printf("Error encoding message: %v", err)
			return err
		}

		temp_op := s_connIDToAddrOp{
			opType:      "read",
			valueConnID: connId,
			respChan:    make(chan *lspnet.UDPAddr),
		}
		s.connIDToAddrOpChan <- temp_op
		clientAddr := <-temp_op.respChan

		if _, err := s.conn.WriteToUDP(buffer.Bytes(), clientAddr); err != nil {
			// Handle error and return
			log.Printf("Error sending message: %v", err)
			return err
		}
		log.Printf("Server finished write")
	} else {
		temp_op0 := s_connIDToAddrOp{
			opType:      "read",
			valueConnID: connId,
			respChan:    make(chan *lspnet.UDPAddr),
		}
		s.connIDToAddrOpChan <- temp_op0
		clientAddr := <-temp_op0.respChan
		temp_op := s_PendingMessageQueueOp{
			opType: "insert",
			s_pendingMessage: &s_PendingMessage{
				SequenceNumber: curWriteSeqNum,
				Payload:        payload,
				connID:         connId,
				udpAddr:        clientAddr,
			},
		}

		s.pendingMessageQueueOpChan <- temp_op
		log.Printf("Server input into pendingMessgaeQueue")
	}
	return nil
}

func (s *server) CloseConn(connId int) error {
	return errors.New("not yet implemented")
}

func (s *server) Close() error {
	s.closeChannel <- struct{}{}
	return s.conn.Close()
}
