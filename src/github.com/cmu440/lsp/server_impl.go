// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/cmu440/lspnet"
)

type s_PendingMessageQueueOp struct {
	opType           string // "keepPop" or "insert" or "init"
	s_pendingMessage *Message
	connID           int
}

func (s *server) managePendingMessageQueueOps() {
	for {
		select {
		case op := <-s.pendingMessageQueueOpChan:
			switch op.opType {
			case "keepPop":
				s.LeftWindowSizeLock <- struct{}{}
				curLeftWindowSize := s.windowSizeLeft[op.connID]
				<-s.LeftWindowSizeLock

				// s.currentWriteSeqNumOpLock <- struct{}{}
				// curWriteSeqNum := s.currentWriteSeqNum[op.connID]
				// <-s.currentWriteSeqNumOpLock

				s.currentAckNumLock <- struct{}{}
				curAckNum := s.currentAckNum[op.connID]
				<-s.currentAckNumLock

				s.PendingMessageQueueLock <- struct{}{}
				pendMessageQueue := s.pendingMessageQueue[op.connID]
				<-s.PendingMessageQueueLock

				for (!pendMessageQueue.IsEmpty() && pendMessageQueue.Peek().(*Message).SeqNum <= curLeftWindowSize+s.params.WindowSize-1) &&
					((pendMessageQueue.Peek().(*Message).SeqNum - curAckNum) <= s.params.MaxUnackedMessages) {

					pendingMessage := pendMessageQueue.Peek().(*Message)

					pendMessageQueue.Dequeue() // Now it is safe to dequeue, we already have all needed info

					go s.SendMessage(pendingMessage)
				}
			}
		}
	}
}

func (s *server) SendMessage(messageToSend *Message) {
	//check if the client is closed
	// closedClient := <-s.closedClients
	// <-s.closedClients
	// if closedClient[messageToSend.ConnID] {
	// 	return
	// }

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
		log.Printf("Error encoding message: %v", err)
		return
	}

	if _, err := s.conn.WriteToUDP(jsonData, udpAddr); err != nil {
		log.Printf("Error sending message: %v", err)
	}
	log.Print("server finished writing to client: ", messageToSend.ConnID, "with type: ",
		messageToSend.Type, " and seqnun: ", messageToSend.SeqNum)
}

func (s *server) nonResponseEpochRoutine(ConnID int) {
	nonResponseEpoch := <-s.nonResponseEpochSum
	nonResponseEpoch[ConnID]++
	curEpoch := nonResponseEpoch[ConnID]
	s.nonResponseEpochSum <- nonResponseEpoch
	if curEpoch > s.params.EpochLimit {
		// TODO: close the channel
		closedClient := <-s.closedClients
		closedClient[ConnID] = true
		s.closedClients <- closedClient
		return
	}

}

func (s *server) resendMessageRoutine() {
	for {
		select {
		case <-time.After(time.Duration(s.params.EpochMillis * 1000000)):
			currentEpoch := <-s.currentEpoch

			for connID := range currentEpoch {
				// closedClient := <-s.closedClients
				// if closedClient[connID] {
				// 	continue
				// }
				// <-s.closedClients

				currentEpoch[connID]++
				//heartbeat
				ackMsg := NewAck(connID, 0)
				go s.SendMessage(ackMsg)

				go s.nonResponseEpochRoutine(connID)
			}
			unAckMessageMap := <-s.unAckMessageMap
			for connID, messageMap := range unAckMessageMap {

				// closedClient := <-s.closedClients
				// if closedClient[connID] {
				// 	continue
				// }
				// <-s.closedClients

				for seqNum, messageWithEpoch := range messageMap {
					if messageWithEpoch.epoch <= currentEpoch[connID] {
						log.Print("server resend!!!!")
						// send message
						messageToSend := messageWithEpoch.message
						jsonData, err := json.Marshal(messageToSend)
						s.connIDToAddrLock <- struct{}{}
						udpAddr := s.connIDToAddr[messageToSend.ConnID]
						<-s.connIDToAddrLock
						if err != nil {
							log.Printf("Error encoding message: %v", err)
							return
						}
						if _, err := s.conn.WriteToUDP(jsonData, udpAddr); err != nil {
							log.Printf("Error sending message: %v", err)
						}

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

type messagewithUDP struct {
	udpAddr *lspnet.UDPAddr
	msg     Message //message
}

type server struct {
	// addrToConnID map[*lspnet.UDPAddr]int

	connIDToAddr map[int]*lspnet.UDPAddr

	windowSizeLeft map[int]int
	// LeftWindowSizeOpChan chan s_LeftWindowSizeOp
	LeftWindowSizeLock chan struct{} //88

	currentReadSeqNum map[int]int //88
	// currentReadSeqNumOpChan chan s_currentReadSeqNumOp
	currentReadSeqNumLock chan struct{}

	currentWriteSeqNum map[int]int //88
	// currentWriteSeqNumOpChan chan s_currentWriteSeqNumOp
	currentWriteSeqNumOpLock chan struct{}

	currentAckNum map[int]int //88
	// currentAckNumOpChan chan s_currentAckNumOp
	currentAckNumLock chan struct{}

	pendingMessageQueue       map[int]*Queue //888
	pendingMessageQueueOpChan chan s_PendingMessageQueueOp
	PendingMessageQueueLock   chan struct{}

	pqRead map[int]map[int]*Message

	pqACK map[int]*PriorityQueue //**
	// pqACKOpChan               chan pqACKOp

	conn        *lspnet.UDPConn
	readChannel chan *Message
	connID      int

	params               Params
	serverMessageChannel chan messagewithUDP
	closeChannel         chan struct{}

	currentConnID int // generated by server, give to client, plus 1 every time
	// currentConnIDOpChan chan s_currentConnIDOp
	currentConnIDLock chan struct{}

	// connIDToAddrOpChan chan s_connIDToAddrOp
	connIDToAddrLock chan struct{}

	unAckMessageMap     chan map[int]map[int]*massageWithEpoch // first key int is connID, second key int is sequence number in that connection
	currentEpoch        chan map[int]int
	nonResponseEpochSum chan map[int]int

	readChannelQueue chan *Queue

	closedClients chan map[int]bool
}

func (s *server) readChannelProcess() {
	readChannelQueue := <-s.readChannelQueue
	item := readChannelQueue.Peek().(*Message)
	readChannelQueue.Dequeue()
	s.readChannelQueue <- readChannelQueue

	s.readChannel <- item
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
		// use json.Unmarshal
		if err := json.Unmarshal(buf[:n], &msg); err != nil {
			log.Println("Decode Error: ", err)
			continue
		}
		msgWithUDP := messagewithUDP{
			udpAddr: addr,
			msg:     msg,
		}
		if msg.Type == MsgData {
			ackMsg := NewAck(msg.ConnID, msg.SeqNum)
			go s.SendMessage(ackMsg)

		}

		s.serverMessageChannel <- msgWithUDP

		nonResponseEpochSum := <-s.nonResponseEpochSum
		nonResponseEpochSum[msg.ConnID] = -1 // when increased it turns to 0 which means a new epoch calculation
		s.nonResponseEpochSum <- nonResponseEpochSum
		log.Print("server do get message with msgtype", msg.Type, " with seqnum", msg.SeqNum)

	}
}

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

func (s *server) mainRoutine() {
	for {
		select {
		case <-s.closeChannel:
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

				s.currentWriteSeqNumOpLock <- struct{}{} // might delete
				s.currentWriteSeqNum[currentConnID_toClient] = msg.SeqNum
				<-s.currentWriteSeqNumOpLock

				s.currentReadSeqNum[currentConnID_toClient] = msg.SeqNum

				s.LeftWindowSizeLock <- struct{}{} // might delete
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

				go s.SendMessage(ackMsg)

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

				//s.pqReadLock <- struct{}{}

				//<-s.pqReadLock
				//s.currentReadSeqNumLock <- struct{}{}
				//<-s.currentReadSeqNumLock
				if msg.SeqNum >= (s.currentReadSeqNum[msg.ConnID] + 1) {
					s.pqRead[msg.ConnID][msg.SeqNum] = &newPqReadMessage
				} else {
					continue
				}

				// s.pqReadLock <- struct{}{}
				curPqReadTop := s.findMinSeqNum(msg.ConnID)
				log.Print("In msgData: server get messgae seqnum: ", msg.SeqNum, " with cureadsqenum: ", s.currentReadSeqNum[msg.ConnID],
					" and pqreadtopseqnum: ", curPqReadTop.SeqNum)
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
				// <-s.pqReadLock

			} else if msg.Type == MsgAck {
				if msg.SeqNum == 0 {
					continue
				}

				s.LeftWindowSizeLock <- struct{}{}
				curLeftWindowSize := s.windowSizeLeft[msg.ConnID]
				<-s.LeftWindowSizeLock

				log.Print("server receive ack message with seqnum: ", msg.SeqNum, " and client num: ", msg.ConnID)

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
							// s.LeftWindowSizeOpChan <- newOp
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
		// LeftWindowSizeOpChan: make(chan s_LeftWindowSizeOp),
		LeftWindowSizeLock: make(chan struct{}, 1),

		currentWriteSeqNum: make(map[int]int),
		// currentWriteSeqNumOpChan: make(chan s_currentWriteSeqNumOp),
		currentWriteSeqNumOpLock: make(chan struct{}, 1),

		currentReadSeqNum: make(map[int]int),
		// currentReadSeqNumOpChan: make(chan s_currentReadSeqNumOp),
		currentReadSeqNumLock: make(chan struct{}, 1),

		currentAckNum: make(map[int]int),
		// currentAckNumOpChan: make(chan s_currentAckNumOp),
		currentAckNumLock: make(chan struct{}, 1),

		currentConnID: 0,
		// currentConnIDOpChan: make(chan s_currentConnIDOp),
		currentConnIDLock: make(chan struct{}, 1),

		connIDToAddr: make(map[int]*lspnet.UDPAddr),
		// connIDToAddrOpChan: make(chan s_connIDToAddrOp),
		connIDToAddrLock: make(chan struct{}, 1),

		pqRead: make(map[int]map[int]*Message),

		pqACK: make(map[int]*PriorityQueue),
		// pqACKOpChan:               make(chan pqACKOp),
		unAckMessageMap:     make(chan map[int]map[int]*massageWithEpoch, 1),
		currentEpoch:        make(chan map[int]int, 1),
		nonResponseEpochSum: make(chan map[int]int, 1),

		readChannelQueue: make(chan *Queue, 1),

		closedClients: make(chan map[int]bool, 1),
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
		case data, ok := <-s.readChannel:
			if !ok {
				// readChannel 已关闭
				return data.ConnID, nil, errors.New("readChannel has been closed")
			}
			log.Print("server get message seqnum", data.SeqNum, " from client: ", data.ConnID)
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
	log.Print("server on connid: ", connId, " : I have  curwriteseq: ", curWriteSeqNum, " with curacknum: ", curAckNum,
		" withrightwindowside: ", (curLeftWindowSize + s.params.WindowSize - 1))
	if (curWriteSeqNum <= curLeftWindowSize+s.params.WindowSize-1) &&
		((curWriteSeqNum - curAckNum) <= s.params.MaxUnackedMessages) {
		messageToSend := NewData(connId, curWriteSeqNum, int(len(payload)), payload,
			CalculateChecksum(connId, curWriteSeqNum, int(len(payload)), payload))
		go s.SendMessage(messageToSend)

	} else {
		s.PendingMessageQueueLock <- struct{}{}
		s.pendingMessageQueue[connId].Enqueue(NewData(connId, curWriteSeqNum, int(len(payload)), payload,
			CalculateChecksum(connId, curWriteSeqNum, int(len(payload)), payload)))
		<-s.PendingMessageQueueLock
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
