// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/cmu440/lspnet"
)

// massageWithEpoch is a struct that contains operation type, Message and connection ID
type s_PendingMessageQueueOp struct {
	opType           string // "keepPop"
	s_pendingMessage *Message
	connID           int
}

// manage pending message queue operations
func (s *server) managePendingMessageQueueOps() {
	isCloseCalled := false
	for {
		select {
		case <-s.pendingMessageFinalClose:
			s.pendingMessageFinalClose <- struct{}{}
			return
		case op := <-s.pendingMessageQueueOpChan:
			switch op.opType {
			case "keepPop":
				s.LeftWindowSizeLock <- struct{}{}
				curLeftWindowSize := s.windowSizeLeft[op.connID]
				<-s.LeftWindowSizeLock

				s.currentAckNumLock <- struct{}{}
				curAckNum := s.currentAckNum[op.connID]
				<-s.currentAckNumLock

				s.PendingMessageQueueLock <- struct{}{}
				pendMessageQueue := s.pendingMessageQueue[op.connID]
				<-s.PendingMessageQueueLock
				if pendMessageQueue == nil {
					continue
				}

				for (!pendMessageQueue.IsEmpty() && pendMessageQueue.Peek().(*Message).SeqNum <= curLeftWindowSize+s.params.WindowSize-1) &&
					((pendMessageQueue.Peek().(*Message).SeqNum - curAckNum) <= s.params.MaxUnackedMessages) {

					pendingMessage := pendMessageQueue.Peek().(*Message)

					pendMessageQueue.Dequeue() // Now it is safe to dequeue, we already have all needed info

					s.SendMessage(pendingMessage)
				}
				if isCloseCalled && pendMessageQueue.IsEmpty() {
					s.PendingMessageQueueLock <- struct{}{}
					delete(s.pendingMessageQueue, op.connID)
					length := len(s.pendingMessageQueue)
					<-s.PendingMessageQueueLock
					if length == 0 {
						select {
						case <-s.pendingMessageFinalClose:
							s.pendingMessageFinalClose <- struct{}{}
						default:
							s.pendingMessageFinalClose <- struct{}{}
						}
					}
				}
			}
		case <-s.isClosed:
			isCloseCalled = true
			s.PendingMessageQueueLock <- struct{}{}
			for connID, clientPendingMessageQueue := range s.pendingMessageQueue {
				if clientPendingMessageQueue.IsEmpty() {
					delete(s.pendingMessageQueue, connID)
				}
				if len(s.pendingMessageQueue) == 0 {
					select {
					case <-s.pendingMessageFinalClose:
						s.pendingMessageFinalClose <- struct{}{}
					default:
						s.pendingMessageFinalClose <- struct{}{}
					}
					break
				}
			}
			<-s.PendingMessageQueueLock
		}
	}
}

// send message function
func (s *server) SendMessage(messageToSend *Message) {

	if messageToSend.Type == MsgData {
		unAckMessageMap := <-s.unAckMessageMap
		currentEpoch := <-s.currentEpoch
		unAckMessageMap[messageToSend.ConnID][messageToSend.SeqNum] = &massageWithEpoch{
			message:        messageToSend,
			epoch:          currentEpoch[messageToSend.ConnID] + 1,
			currentBackOff: 1,
		}
		s.currentEpoch <- currentEpoch
		s.unAckMessageMap <- unAckMessageMap
	}

	s.connIDToAddrLock <- struct{}{}
	udpAddr := s.connIDToAddr[messageToSend.ConnID]
	<-s.connIDToAddrLock

	jsonData, err := json.Marshal(messageToSend)
	if err != nil {
		return
	}

	s.conn.WriteToUDP(jsonData, udpAddr)

}

// A routine processing non response epoch
func (s *server) nonResponseEpochRoutine(ConnID int) {
	nonResponseEpoch := <-s.nonResponseEpochSum
	nonResponseEpoch[ConnID]++
	curEpoch := nonResponseEpoch[ConnID]
	s.nonResponseEpochSum <- nonResponseEpoch
	if curEpoch > s.params.EpochLimit {
		s.PendingMessageQueueLock <- struct{}{}
		delete(s.pendingMessageQueue, ConnID)
		length := len(s.pendingMessageQueue)
		<-s.PendingMessageQueueLock
		if length == 0 {
			select {
			case <-s.pendingMessageFinalClose:
				s.pendingMessageFinalClose <- struct{}{}
			default:
				s.pendingMessageFinalClose <- struct{}{}
			}
			s.conn.Close()
		}
		return
	}

}

// A routine that try to resend message per epoch
func (s *server) resendMessageRoutine() {
	for {
		select {
		case <-s.pendingMessageFinalClose:
			s.pendingMessageFinalClose <- struct{}{}
			return
		case <-time.After(time.Duration(s.params.EpochMillis * 1000000)):
			currentEpoch := <-s.currentEpoch

			for connID := range currentEpoch {

				currentEpoch[connID]++
				ackMsg := NewAck(connID, 0)
				go s.SendMessage(ackMsg)

				go s.nonResponseEpochRoutine(connID)
			}
			unAckMessageMap := <-s.unAckMessageMap
			for connID, messageMap := range unAckMessageMap {

				for seqNum, messageWithEpoch := range messageMap {
					if messageWithEpoch.epoch <= currentEpoch[connID] {
						// send message
						messageToSend := messageWithEpoch.message
						jsonData, err := json.Marshal(messageToSend)
						s.connIDToAddrLock <- struct{}{}
						udpAddr := s.connIDToAddr[messageToSend.ConnID]
						<-s.connIDToAddrLock
						if err != nil {
							return
						}
						s.conn.WriteToUDP(jsonData, udpAddr)

						if messageWithEpoch.currentBackOff > s.params.MaxBackOffInterval {
							messageWithEpoch.currentBackOff = s.params.MaxBackOffInterval
						}
						messageWithEpoch.epoch = messageWithEpoch.epoch + messageWithEpoch.currentBackOff + 1
						//update backoff
						messageWithEpoch.currentBackOff = messageWithEpoch.currentBackOff * 2
						unAckMessageMap[connID][seqNum] = messageWithEpoch
					}
				}
			}
			s.currentEpoch <- currentEpoch
			s.unAckMessageMap <- unAckMessageMap
		}
	}
}

// a struct that contains message and UDP address
type messagewithUDP struct {
	udpAddr *lspnet.UDPAddr
	msg     Message //message
}

type server struct {
	connIDToAddr map[int]*lspnet.UDPAddr

	windowSizeLeft map[int]int

	LeftWindowSizeLock chan struct{}

	currentReadSeqNum map[int]int // read seq num for each connection

	currentReadSeqNumLock chan struct{}

	currentWriteSeqNum map[int]int

	currentWriteSeqNumOpLock chan struct{}

	currentAckNum map[int]int

	currentAckNumLock chan struct{}

	pendingMessageQueue       map[int]*Queue // a map from connID to pending message queue
	pendingMessageQueueOpChan chan s_PendingMessageQueueOp
	PendingMessageQueueLock   chan struct{}

	pqRead map[int]map[int]*Message

	pqACK map[int]*PriorityQueue // a map from connID to priority queue for ACK

	conn        *lspnet.UDPConn
	readChannel chan *Message
	connID      int

	params               Params
	serverMessageChannel chan messagewithUDP
	closeChannel         chan struct{}

	currentConnID     int
	currentConnIDLock chan struct{}

	connIDToAddrLock chan struct{}

	unAckMessageMap     chan map[int]map[int]*massageWithEpoch // first key int is connID, second key int is sequence number in that connection
	currentEpoch        chan map[int]int
	nonResponseEpochSum chan map[int]int

	readChannelQueue chan *Queue

	closedClients chan map[int]bool

	isClosed                 chan struct{} // a channel to indicate whether the server is closed
	pendingMessageFinalClose chan struct{}
}

// A routine that process readChannelQueue
func (s *server) readChannelProcess() {
	readChannelQueue := <-s.readChannelQueue
	item := readChannelQueue.Peek().(*Message)
	readChannelQueue.Dequeue()
	s.readChannelQueue <- readChannelQueue

	s.readChannel <- item
}

// A routine that process read from ReadFromUDP
func (s *server) readProcessRoutine() {
	buf := make([]byte, 2048)
	for {
		n, addr, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		var msg Message
		// use json.Unmarshal
		if err := json.Unmarshal(buf[:n], &msg); err != nil {
			continue
		}
		msgWithUDP := messagewithUDP{
			udpAddr: addr,
			msg:     msg,
		}
		if msg.Type == MsgData {
			ackMsg := NewAck(msg.ConnID, msg.SeqNum)
			s.SendMessage(ackMsg)

		}

		s.serverMessageChannel <- msgWithUDP

		nonResponseEpochSum := <-s.nonResponseEpochSum
		nonResponseEpochSum[msg.ConnID] = -1 // when increased it turns to 0 which means a new epoch calculation
		s.nonResponseEpochSum <- nonResponseEpochSum

	}
}

// find the minimum sequence number in the map with given connection ID
func (s *server) findMinSeqNum(connID int) *Message {
	if len(s.pqRead[connID]) == 0 {
		return nil
	}
	minSeqNum := math.MaxInt
	for key := range s.pqRead[connID] {
		if key < minSeqNum {
			minSeqNum = key
		}
	}
	return s.pqRead[connID][minSeqNum]
}

// main routine for server
func (s *server) mainRoutine() {
	for {
		select {
		case <-s.pendingMessageFinalClose:
			s.pendingMessageFinalClose <- struct{}{}
			return
		case messagewithUDP := <-s.serverMessageChannel:
			msg := messagewithUDP.msg
			msgUDPaddr := messagewithUDP.udpAddr
			if msg.Type == MsgConnect {
				s.currentConnID++

				currentConnID_toClient := s.currentConnID

				s.PendingMessageQueueLock <- struct{}{}
				s.pendingMessageQueue[currentConnID_toClient] = NewQueue()
				<-s.PendingMessageQueueLock

				s.pqRead[currentConnID_toClient] = make(map[int]*Message)

				s.pqACK[currentConnID_toClient] = NewPriorityQueue()

				s.connIDToAddrLock <- struct{}{}
				s.connIDToAddr[currentConnID_toClient] = msgUDPaddr
				<-s.connIDToAddrLock

				s.currentAckNumLock <- struct{}{}
				s.currentAckNum[currentConnID_toClient] = msg.SeqNum
				<-s.currentAckNumLock

				s.currentWriteSeqNumOpLock <- struct{}{}
				s.currentWriteSeqNum[currentConnID_toClient] = msg.SeqNum
				<-s.currentWriteSeqNumOpLock

				s.currentReadSeqNum[currentConnID_toClient] = msg.SeqNum

				s.LeftWindowSizeLock <- struct{}{}
				s.windowSizeLeft[currentConnID_toClient] = msg.SeqNum + 1
				<-s.LeftWindowSizeLock

				ackMsg := NewAck(currentConnID_toClient, msg.SeqNum)

				unAckMessageMap := <-s.unAckMessageMap
				unAckMessageMap[currentConnID_toClient] = make(map[int]*massageWithEpoch)
				s.unAckMessageMap <- unAckMessageMap

				currentEpoch := <-s.currentEpoch
				currentEpoch[currentConnID_toClient] = 0
				s.currentEpoch <- currentEpoch

				nonResponseEpochSum := <-s.nonResponseEpochSum
				nonResponseEpochSum[currentConnID_toClient] = -1
				s.nonResponseEpochSum <- nonResponseEpochSum

				s.SendMessage(ackMsg)

			} else if msg.Type == MsgData {

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

				if msg.SeqNum >= (s.currentReadSeqNum[msg.ConnID] + 1) {
					s.pqRead[msg.ConnID][msg.SeqNum] = &newPqReadMessage
				} else {
					continue
				}

				curPqReadTop := s.findMinSeqNum(msg.ConnID)

				for (s.currentReadSeqNum[msg.ConnID] + 1) == (curPqReadTop.SeqNum) {

					delete(s.pqRead[msg.ConnID], curPqReadTop.SeqNum)
					readChannelQueue := <-s.readChannelQueue
					readChannelQueue.Enqueue(curPqReadTop)
					s.readChannelQueue <- readChannelQueue
					go s.readChannelProcess()

					s.currentReadSeqNum[msg.ConnID]++

					if len(s.pqRead[msg.ConnID]) == 0 {
						break
					}
					curPqReadTop = s.findMinSeqNum(msg.ConnID)

				}

			} else if msg.Type == MsgAck {
				if msg.SeqNum == 0 {
					continue
				}

				s.LeftWindowSizeLock <- struct{}{}
				curLeftWindowSize := s.windowSizeLeft[msg.ConnID]
				<-s.LeftWindowSizeLock

				unAckMessageMap := <-s.unAckMessageMap
				if _, ok := unAckMessageMap[msg.ConnID][msg.SeqNum]; ok {
					s.currentAckNumLock <- struct{}{}
					s.currentAckNum[msg.ConnID]++
					<-s.currentAckNumLock

					delete(unAckMessageMap[msg.ConnID], msg.SeqNum)
				}
				s.unAckMessageMap <- unAckMessageMap

				if msg.SeqNum == (curLeftWindowSize) {
					/*update leftWindow */
					s.LeftWindowSizeLock <- struct{}{}
					s.windowSizeLeft[msg.ConnID]++
					<-s.LeftWindowSizeLock

					/*update continuously from pqACK queue*/
					lastLeft := msg.SeqNum + 1
					pqACK := s.pqACK[msg.ConnID]

					for pqACK.Len() > 0 {
						if (lastLeft) == pqACK.Top().(int) {
							lastLeft++
							pqACK.Pop()
							s.LeftWindowSizeLock <- struct{}{}
							s.windowSizeLeft[msg.ConnID]++
							<-s.LeftWindowSizeLock
						} else {
							break
						}
					}

					/*update pending Message queue, pending message can be sent now*/
					pendMessageOp := s_PendingMessageQueueOp{
						opType: "keepPop",
						connID: msg.ConnID,
					}
					s.pendingMessageQueueOpChan <- pendMessageOp
				} else {
					pqACK := s.pqACK[msg.ConnID]
					pqACK.Insert(msg.SeqNum, float64(msg.SeqNum))
				}

			} else if msg.Type == MsgCAck {

				s.LeftWindowSizeLock <- struct{}{}
				curLeftWindowSize := s.windowSizeLeft[msg.ConnID]
				<-s.LeftWindowSizeLock

				s.currentAckNumLock <- struct{}{}
				s.currentAckNum[msg.ConnID] = msg.SeqNum
				<-s.currentAckNumLock

				unAckMessageMap := <-s.unAckMessageMap
				for seqNum := range unAckMessageMap[msg.ConnID] {
					if seqNum <= msg.SeqNum {
						delete(unAckMessageMap[msg.ConnID], seqNum)
					}
				}
				s.unAckMessageMap <- unAckMessageMap

				if msg.SeqNum >= (curLeftWindowSize) {
					pqACK := s.pqACK[msg.ConnID]
					for pqACK.Len() > 0 {
						if msg.SeqNum >= pqACK.Top().(int) {
							pqACK.Pop()
						} else {
							break
						}
					}

					lastLeft := msg.SeqNum
					for pqACK.Len() > 0 {
						if (lastLeft) == pqACK.Top().(int) {
							lastLeft++
							pqACK.Pop()
							s.LeftWindowSizeLock <- struct{}{}
							s.windowSizeLeft[msg.ConnID]++
							<-s.LeftWindowSizeLock
						} else {
							break
						}
					}

					s.LeftWindowSizeLock <- struct{}{}
					s.windowSizeLeft[msg.ConnID] = lastLeft
					<-s.LeftWindowSizeLock

					pendMessageOp := s_PendingMessageQueueOp{
						opType: "keepPop",
						connID: msg.ConnID,
					}
					s.pendingMessageQueueOpChan <- pendMessageOp
				}

			}
		}
	}
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {

	laddr, err := lspnet.ResolveUDPAddr("udp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}
	conn, err := lspnet.ListenUDP("udp", laddr)
	if err != nil {
		return nil, err
	}
	s := &server{
		conn:        conn,
		readChannel: make(chan *Message, 1),

		params:               *params,
		connID:               0,
		serverMessageChannel: make(chan messagewithUDP),
		closeChannel:         make(chan struct{}),

		pendingMessageQueue:       make(map[int]*Queue, 1),
		pendingMessageQueueOpChan: make(chan s_PendingMessageQueueOp),
		PendingMessageQueueLock:   make(chan struct{}, 1),

		windowSizeLeft: make(map[int]int),

		LeftWindowSizeLock: make(chan struct{}, 1),

		currentWriteSeqNum: make(map[int]int),

		currentWriteSeqNumOpLock: make(chan struct{}, 1),

		currentReadSeqNum: make(map[int]int),

		currentReadSeqNumLock: make(chan struct{}, 1),

		currentAckNum: make(map[int]int),

		currentAckNumLock: make(chan struct{}, 1),

		currentConnID: 0,

		currentConnIDLock: make(chan struct{}, 1),

		connIDToAddr: make(map[int]*lspnet.UDPAddr),

		connIDToAddrLock: make(chan struct{}, 1),

		pqRead: make(map[int]map[int]*Message),

		pqACK: make(map[int]*PriorityQueue),

		unAckMessageMap:     make(chan map[int]map[int]*massageWithEpoch, 1),
		currentEpoch:        make(chan map[int]int, 1),
		nonResponseEpochSum: make(chan map[int]int, 1),

		readChannelQueue: make(chan *Queue, 1),

		closedClients: make(chan map[int]bool, 1),

		isClosed:                 make(chan struct{}, 1),
		pendingMessageFinalClose: make(chan struct{}, 1),
	}
	s.currentEpoch <- make(map[int]int)
	s.unAckMessageMap <- make(map[int]map[int]*massageWithEpoch)
	s.nonResponseEpochSum <- make(map[int]int)
	s.readChannelQueue <- NewQueue()
	s.closedClients <- make(map[int]bool)
	go s.managePendingMessageQueueOps()

	go s.readProcessRoutine()
	go s.mainRoutine()
	go s.resendMessageRoutine()

	return s, nil
}

func (s *server) Read() (int, []byte, error) {
	for {
		select {
		case <-s.pendingMessageFinalClose:
			s.pendingMessageFinalClose <- struct{}{}
			return s.connID, nil, errors.New("server has been closed")
		case data, ok := <-s.readChannel:
			if !ok {
				// readChannel has closed
				return data.ConnID, nil, errors.New("readChannel has been closed")
			}
			return data.ConnID, data.Payload, nil
		case <-s.closeChannel:
			return s.connID, nil, errors.New("server has been closed")
		}
	}
}

func (s *server) Write(connId int, payload []byte) error {
	s.currentWriteSeqNumOpLock <- struct{}{}
	s.currentWriteSeqNum[connId]++
	<-s.currentWriteSeqNumOpLock
	s.LeftWindowSizeLock <- struct{}{}
	curLeftWindowSize := s.windowSizeLeft[connId]
	<-s.LeftWindowSizeLock

	s.currentWriteSeqNumOpLock <- struct{}{}
	curWriteSeqNum := s.currentWriteSeqNum[connId]
	<-s.currentWriteSeqNumOpLock

	s.currentAckNumLock <- struct{}{}
	curAckNum := s.currentAckNum[connId]
	<-s.currentAckNumLock

	if (curWriteSeqNum <= curLeftWindowSize+s.params.WindowSize-1) &&
		((curWriteSeqNum - curAckNum) <= s.params.MaxUnackedMessages) {
		messageToSend := NewData(connId, curWriteSeqNum, int(len(payload)), payload,
			CalculateChecksum(connId, curWriteSeqNum, int(len(payload)), payload))

		s.SendMessage(messageToSend)

	} else {
		s.PendingMessageQueueLock <- struct{}{}
		s.pendingMessageQueue[connId].Enqueue(NewData(connId, curWriteSeqNum, int(len(payload)), payload,
			CalculateChecksum(connId, curWriteSeqNum, int(len(payload)), payload)))
		<-s.PendingMessageQueueLock
	}
	return nil
}

func (s *server) CloseConn(connId int) error {

	s.PendingMessageQueueLock <- struct{}{}
	delete(s.pendingMessageQueue, connId)
	length := len(s.pendingMessageQueue)

	<-s.PendingMessageQueueLock
	if length == 0 {
		select {
		case <-s.pendingMessageFinalClose:
			s.pendingMessageFinalClose <- struct{}{}
		default:
			s.pendingMessageFinalClose <- struct{}{}
		}
		s.conn.Close()
	}
	return nil

}

func (s *server) Close() error {

	select {
	case <-s.isClosed:
		s.isClosed <- struct{}{}
	default:
		s.isClosed <- struct{}{}
	}
	<-s.pendingMessageFinalClose
	s.pendingMessageFinalClose <- struct{}{}

	return s.conn.Close()
}
