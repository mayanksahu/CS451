// A server is a node in the cord ring.
// change finger indices to 0 - m-1
// close conn
// handle err, check access to node.Finger

package main

import (
	"bufio"
	"fmt"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	CREATE = "create"
	JOIN   = "join"
)

func main() {

	if len(os.Args) < 3 {
		fmt.Println("Usage: ", os.Args[0], "create <port> or join <port> <joinIP> <joinport>")
		log.Fatal(1)
	}

	var node Node

	fmt.Println("Setting up configuration.")
	config := DefaultConfiguration()

	var ip = GetAddress()
	var port = os.Args[2]
	myNodeIdentifier := &NodeIdentifier{ip, port}
	joined := false

	switch os.Args[1] {

	case CREATE:
		fmt.Println("Creating new chord ring.")
		// TODO: add config in a proper way to Node if possible.
		node = create(myNodeIdentifier, config)

	case JOIN:
		fmt.Println("Joining new node to the chord ring.")
		var joinIP = os.Args[3]
		var joinPort = os.Args[4]
		joinManagerNodeIdentifier := &NodeIdentifier{joinIP, joinPort}
		// TODO: add config in a proper way to Node if possible.
		node = join(myNodeIdentifier, joinManagerNodeIdentifier, config)
		joined = true

	default:
		fmt.Println("Wrong selection !!!")
	}

	// TCP server running in the main thread.
	rpc.Register(&node)

	fmt.Println("listening on port:" + port)

	go node.listen()
	go node.checkPredecessor()
	go node.stabilize()
	go node.fixFingers()

	if joined {
		go func() {
			time.Sleep(TIME_WAIT_BEFORE_KV_ADJUSTMENT)
			node.transferKV()
		}()
	}

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Println("1:Predecessor; 2:Successor; 3:FingerTable; 4:Read; 5:Write; 6:Remove; 7:AllKVPairs; 8:SuccessorList")
		scanner.Scan()
		command, err := strconv.Atoi(scanner.Text())

		checkError(err)

		switch command {
		case 1:
			// Predecessor
			fmt.Println(node.Predecessor.getAddress() + "\n")
		case 2:
			// Successor
			fmt.Println(node.SuccessorList[0].getAddress() + "\n")
		case 3:
			// FingerTable
			for i, elem := range node.Finger {
				fmt.Println(strconv.Itoa(int(i)) + " " + elem.getAddress())
			}
		case 4:
			// read
			fmt.Println("Enter key:")
			scanner.Scan()
			key := scanner.Text()
			fmt.Println("Value: " + node.read(key) + "\n")
		case 5:
			// write
			fmt.Println("Enter key:")
			scanner.Scan()
			key := scanner.Text()
			fmt.Println("Enter value:")
			scanner.Scan()
			value := scanner.Text()
			node.write(key, value)
			fmt.Println()
		case 6:
			// remove
			fmt.Println("Enter key:")
			scanner.Scan()
			key := scanner.Text()
			node.remove(key)
			fmt.Println()
		case 7:
			for i := 0; i < config.k+1; i++ {
				fmt.Printf("\n%dth predecessor keyvalues:\n", i)
				for k, v := range node.Data[i] {
					fmt.Println(k + " : " + v)
				}
			}
			fmt.Println()
		case 8:
			for _, elem := range node.SuccessorList {
				fmt.Println(elem.getAddress())
			}
			fmt.Println()
		default:
			fmt.Println("Wrong choice !!!")
		}

	}

}

// Creating a new chord ring.
func create(myNodeIdentifier *NodeIdentifier, config Configuration) Node {
	//myKey := config.getKey(myNodeIdentifier)
	//fmt.Println(strconv.Itoa(int(myKey)))

	var successor = NodeIdentifier{myNodeIdentifier.IP, myNodeIdentifier.Port}
	successorList := make([]*NodeIdentifier, config.r, config.r)
	successorList[0] = &successor

	data := make([]map[string]string, config.k+1)
	for i := 0; i < config.k+1; i++ {
		data[i] = make(map[string]string)
	}

	file, err := os.OpenFile("logs\\"+myNodeIdentifier.IP+"-"+myNodeIdentifier.Port+".log", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalln("Failed to open log file", ":", err)
	}

	logger := log.New(file, "Log: ", log.Lshortfile)

	return Node{myNodeIdentifier, successorList, nil, config, make([]*NodeIdentifier, config.m), 0, data, &sync.Mutex{}, logger}
}

// Joining an existing chord ring by contacting a joinManagerNode.
func join(myNodeIdentifier *NodeIdentifier, joinManagerNodeIdentifier *NodeIdentifier, config Configuration) Node {
	myKey := config.getKey(myNodeIdentifier)

	successorNodeIdentifier := getSuccessor(myKey, joinManagerNodeIdentifier)

	successorList := make([]*NodeIdentifier, config.r, config.r)
	successorList[0] = successorNodeIdentifier

	data := make([]map[string]string, config.k+1)
	for i := 0; i < config.k+1; i++ {
		data[i] = make(map[string]string)
	}

	file, err := os.OpenFile("logs\\"+myNodeIdentifier.IP+"-"+myNodeIdentifier.Port+".log", os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalln("Failed to open log file", ":", err)
	}

	logger := log.New(file, "Log: ", log.Lshortfile)

	return Node{myNodeIdentifier, successorList, nil, config, make([]*NodeIdentifier, config.m), 0, data, &sync.Mutex{}, logger}
}
