package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"time"

	"github.com/cmu440/bitcoin"
	"github.com/cmu440/lsp"
)

// Attempt to connect miner as a client to the server.
func joinWithServer(hostport string) (lsp.Client, error) {
	// You will need this for randomized isn
	seed := rand.NewSource(time.Now().UnixNano())
	isn := rand.New(seed).Intn(int(math.Pow(2, 8)))

	// TODO: implement this!
	client, err := lsp.NewClient(hostport, isn, lsp.NewParams())
	//send join
	join := bitcoin.NewJoin()
	joinMarshalled, err := json.Marshal(join)
	err = client.Write(joinMarshalled)
	if err != nil {
		fmt.Println("Failed to write to server:", err)
	}

	return client, err
}

var LOGF *log.Logger

// brute force mining
func mining(data string, lower, upper uint64) (uint64, uint64) {
	var hash uint64
	var nounce uint64
	var minimum uint64 = math.MaxUint64
	for i := lower; i <= upper; i++ {
		hash = bitcoin.Hash(data, i)
		if hash < minimum {
			nounce = i
			minimum = hash
		}
	}
	return minimum, nounce
}

func main() {
	// You may need a logger for debug purpose
	const (
		name = "minerLog.txt"
		flag = os.O_RDWR | os.O_CREATE
		perm = os.FileMode(0666)
	)

	file, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return
	}
	defer file.Close()

	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Printf("Usage: ./%s <hostport>", os.Args[0])
		return
	}

	hostport := os.Args[1]
	miner, err := joinWithServer(hostport)
	if err != nil {
		fmt.Println("Failed to join with server:", err)
		return
	}

	defer miner.Close()

	// TODO: implement this!

	for {
		//read the request from server
		requestByte, err := miner.Read()
		if err != nil {
			fmt.Println("Failed to read from server:", err)
			return
		}
		var msg bitcoin.Message
		json.Unmarshal(requestByte, &msg)
		data, lower, upper := msg.Data, msg.Lower, msg.Upper

		//mining
		hash, nounce := mining(data, lower, upper)

		//send the result back to server
		result := bitcoin.NewResult(hash, nounce)
		resultMarshalled, err := json.Marshal(result)
		err = miner.Write(resultMarshalled)
		if err != nil {
			fmt.Println("Failed to write to server:", err)
			return
		}
	}

}
