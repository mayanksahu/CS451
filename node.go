// The node struct represents a node (by its IP, port etc.) in the ring.

package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"strconv"
	"sync"
	"time"
)

const (
	TIME_WAIT_STABILIZE            = 500 * time.Millisecond // Periodic time wait for stabilize method (millisecond).
	TIME_WAIT_FIX_FINGERS          = 500 * time.Millisecond // Periodic time wait for fixFinger method (millisecond).
	TIME_WAIT_CHECK_PREDECESSOR    = 500 * time.Millisecond // Interval after which a predecessor is checked for its live condition.
	TIMEOUT_QUERY_TO_COMMIT        = 1000 * time.Millisecond
	WRITE_DELAY                    = false
	TIME_WAIT_WRITE_DELAY          = 10 * time.Second
	TIME_WAIT_BEFORE_KV_ADJUSTMENT = 2500 * time.Millisecond
)

// The unique identifier of a node in the chord ring.
type NodeIdentifier struct {
	IP   string // The IP address of the node.
	Port string // The port on which the node is listening.
}

func (t *NodeIdentifier) getAddress() string {
	return t.IP + ":" + t.Port
}

// The struct that represents a node in the ring.
type Node struct {
	myNodeIdentifier *NodeIdentifier
	SuccessorList    []*NodeIdentifier
	Predecessor      *NodeIdentifier
	Config           Configuration
	Finger           []*NodeIdentifier
	Next             int                 // Next index in the finger table to be fixed.
	Data             []map[string]string // The i'th index will store all the key value pairs of predecessor i. (0 is itself)
	mutex            *sync.Mutex
	logger           *(log.Logger)
}

// The method which continuously listen for incomming connections.
func (t *Node) listen() {
	listener, err := net.Listen("tcp", t.myNodeIdentifier.getAddress())
	checkError(err)

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handleClient(conn)
	}
}

////////////////////////////////////////////////////////////////////////////////////////////
// Methods which are run periodically.

// Returns the current predecessor node.
// Self if no predecessor is set currently.
func (t *Node) GetPredecessor(inp *int, reply *NodeIdentifier) error {
	if t.Predecessor == nil {
		*reply = *t.myNodeIdentifier
	} else {
		*reply = *t.Predecessor
	}

	return nil
}

// Notify a node about it's new predecessor.
// Returns its updated successor list.
func (t *Node) Notify(newPred *NodeIdentifier, reply *[]*NodeIdentifier) error {
	if t.Predecessor == nil {
		t.Predecessor = newPred
	} else {
		curPredKey := t.Config.getKey(t.Predecessor)
		newPredKey := t.Config.getKey(newPred)
		myKey := t.Config.getKey(t.myNodeIdentifier)

		if inbetween(curPredKey, newPredKey, myKey) {
			t.Predecessor = newPred
		}
	}

	*reply = t.SuccessorList
	return nil
}

// Periodically varifies it's immediate successor and tells the successor about it.
func (t *Node) stabilize() {
	for {
		time.Sleep(TIME_WAIT_STABILIZE)

		var client *rpc.Client
		var err error
		var i int
		var succ *NodeIdentifier

		service := ""

		// Gets the first successor which is alive.
		for i = 0; i < len(t.SuccessorList); i++ {
			if t.SuccessorList[i] != nil {
				service = t.SuccessorList[i].getAddress()
				succ = t.SuccessorList[i]

				client, err = jsonrpc.Dial("tcp", service)
				if err == nil {
					break
				} else {
					service = ""
				}
			}
		}
		if service == "" {
			continue
		}

		var dummy int
		var newPredecessor NodeIdentifier

		// Get the possibly new predecessor of the successor.
		err = client.Call("Node.GetPredecessor", &dummy, &newPredecessor)
		if err != nil {
			continue
		}

		client.Close()

		newPredKey := t.Config.getKey(&newPredecessor)
		myKey := t.Config.getKey(t.myNodeIdentifier)
		curSuccKey := t.Config.getKey(succ)

		if inbetween(myKey, newPredKey, curSuccKey) {
			service = newPredecessor.getAddress()
			succ = &newPredecessor
		}

		client, err = jsonrpc.Dial("tcp", service)
		if err != nil {
			fmt.Println("error in dial")
			continue
		}

		var ret []*NodeIdentifier

		// Notify the new successor.
		err = client.Call("Node.Notify", t.myNodeIdentifier, &ret)
		client.Close()

		if err != nil {
			continue
		}

		// Update the successor list on successfully notifying the new successor.
		t.SuccessorList[0] = succ

		copy(t.SuccessorList[1:], ret[0:len(ret)-1])

		if i != 0 {
			go func() {
				time.Sleep(TIME_WAIT_BEFORE_KV_ADJUSTMENT)
				t.AdjustKVLeave(i)
			}()
		}

	}
}

// The method periodically fixes the finger table.
func (t *Node) fixFingers() {
	for {
		time.Sleep(TIME_WAIT_FIX_FINGERS)

		myId := t.Config.getKey(t.myNodeIdentifier)

		t.Finger[t.Next] = t.findSuccessor((myId + pow2(t.Next)) % pow2(t.Config.m))

		t.Next++
		if t.Next == t.Config.m {
			t.Next = 0
		}
	}
}

// The method periodically checks whether predecessor has failed or not.
func (t *Node) checkPredecessor() {
	for {
		time.Sleep(TIME_WAIT_CHECK_PREDECESSOR)

		if t.Predecessor != nil {
			_, err := jsonrpc.Dial("tcp", t.Predecessor.getAddress())
			// If not able to call => predecessor has failed.
			if err != nil {
				t.Predecessor = nil
			}
		}
	}
}

func (t *Node) askclosestPreceedingNode(key uint32) *NodeIdentifier {
	myId := t.Config.getKey(t.myNodeIdentifier)
	var reply *NodeIdentifier

	for i := t.Config.m - 1; i >= 0; i-- {
		if t.Finger[i] != nil {
			successorId := t.Config.getKey(t.Finger[i])
			if inbetween(myId, successorId, key) {
				reply = getSuccessor(key, t.Finger[i])
				if reply != nil {
					return reply
				}
			}
		}
	}

	for i := len(t.SuccessorList) - 1; i >= 0; i-- {
		if t.SuccessorList[i] != nil {
			successorId := t.Config.getKey(t.SuccessorList[i])
			if inbetween(myId, successorId, key) {
				reply = getSuccessor(key, t.SuccessorList[i])
				if reply != nil {
					return reply
				}
			}

		}
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////
// Methods which are exported for RPC calls.

// Method to find the immediate successor of a given node.
func (t *Node) findSuccessor(key uint32) *NodeIdentifier {
	myId := t.Config.getKey(t.myNodeIdentifier)
	successorId := t.Config.getKey(t.SuccessorList[0])

	var reply *NodeIdentifier

	if myId == key {
		reply = t.myNodeIdentifier
	} else if inbetweenUpperClosed(myId, key, successorId) {
		reply = t.SuccessorList[0]
	} else {
		reply = t.askclosestPreceedingNode(key)
	}

	return reply
}

// Exported method to find the immediate successor of a given node.
func (t *Node) FindSuccessor(key uint32, reply *NodeIdentifier) error {
	successor := t.findSuccessor(key)
	if successor != nil {
		*reply = *successor
	}

	return nil
}

// Makes an RPC call to the method FindSuccessor at the provided address.
func getSuccessor(myKey uint32, joinNode *NodeIdentifier) *NodeIdentifier {
	service := joinNode.getAddress()

	client, err := jsonrpc.Dial("tcp", service)
	if err != nil {
		return nil
	}

	var successorNodeIdentifier NodeIdentifier
	err = client.Call("Node.FindSuccessor", &myKey, &successorNodeIdentifier)
	if err != nil {
		return nil
	}

	client.Close()

	return &successorNodeIdentifier
}

//////////////////////////////////////////////////////////////////////////////////////
// Data specific operations along with methods which make RPC calls.

// Returns the value of the specified key from the data stored.
func (t *Node) GetValue(key *string, value *string) error {

	t.mutex.Lock()
	if t.Data[0] != nil {
		if val, ok := t.Data[0][*key]; ok {
			*value = val
			t.mutex.Unlock()
			return nil
		}
	}

	t.mutex.Unlock()
	*value = ""
	return errors.New("Key does not exist.")
}

// Returns the value of the provided key stored.
func (t *Node) read(key string) string {
	keyHash := t.Config.getKey1(key)
	fmt.Println(strconv.Itoa(int(keyHash)))

	firstSuccessor := t.findSuccessor(keyHash)

	service := firstSuccessor.getAddress()

	client, err := jsonrpc.Dial("tcp", service)
	if err != nil {

		t.logger.Print("Error connecting to data containing node. Returning Empty string.")
		return ""
	}

	var value string
	err = client.Call("Node.GetValue", &key, &value)
	if err != nil {
		t.logger.Print(err.Error())
	}
	client.Close()

	return value
}

// A key-value pair.
type KeyValue struct {
	Key   string
	Value string
}

// Stores a key-value pair along with the replica number in which it is stored.
type KeyValueReplica struct {
	KV         KeyValue
	Replicanum []int
}

// Locks the data and wait for query to commit.
func (t *Node) QueryToCommit(kv *KeyValue, result *bool) error {
	t.mutex.Lock()
	*result = true
	return nil
}

// Rollback if something goes wrong before actual commit happens.
func (t *Node) RollBack(kv *KeyValue, result *bool) error {
	t.mutex.Unlock()
	*result = true
	return nil
}

// Rollback if something goes wrong during final commit.
func (t *Node) CommitRollBack(kvr *KeyValueReplica, result *bool) error {
	for _, elem := range kvr.Replicanum {
		delete(t.Data[elem], kvr.KV.Key)
	}

	*result = true
	return nil
}

// Final commit call method.
func (t *Node) CommitWrite(kvr *KeyValueReplica, result *bool) error {
	// Uncomment to show the working of 2PC.
	if WRITE_DELAY {
		fmt.Println("Sleeping before commit write")
		time.Sleep(TIME_WAIT_WRITE_DELAY)
		fmt.Println("Slept before commit write")
	}

	for _, elem := range kvr.Replicanum {
		t.Data[elem][kvr.KV.Key] = kvr.KV.Value
	}

	t.mutex.Unlock()

	*result = true
	return nil
}

// Assuming #Nodes > k.
// TODO: have to handle the above case also. Might not be needed in the presence of Successor List.
func (t *Node) SetValue(kv *KeyValue, result *bool) error {

	successorsReplied := make(map[string]string)
	querySent := make(map[string][]int)

	myRepIndices := []int{0}
	canCommit := true

	// Sending QUERY TO COMMIT to all the redundant successors.
	for i := 0; i < t.Config.k; i++ {

		service := t.SuccessorList[i].getAddress()

		if service == t.myNodeIdentifier.getAddress() {
			myRepIndices = append(myRepIndices, int(i+1))
			continue
		} else if _, ok := querySent[service]; ok {
			querySent[service] = append(querySent[service], int(i+1))
			continue
		} else {
			querySent[service] = []int{i + 1}
		}

		client, err := jsonrpc.Dial("tcp", service)
		if err != nil {
			t.logger.Print("Error in setting up connection for QUERY TO COMMIT: " + err.Error())
			canCommit = false
			break
		}

		var res bool
		ret := client.Go("Node.QueryToCommit", &kv, &res, nil)

		select {
		case ret = <-ret.Done:
			if ret.Error != nil {
				t.logger.Print("Error in query to commit: " + err.Error())
				canCommit = false
				break
			}
		case <-time.After(TIMEOUT_QUERY_TO_COMMIT):
			t.logger.Print("Timeout in query to commit.")
			canCommit = false
			break
		}

		client.Close()
		successorsReplied[service] = service
	}

	// Rollback if cannot commit.
	if canCommit == false {
		for _, v := range successorsReplied {
			service := v

			client, err := jsonrpc.Dial("tcp", service)
			if err != nil {
				// Successor is dead.
				t.logger.Print("Error in dialing for ROLLBACK: " + err.Error())
				continue
			}

			var res bool
			err = client.Call("Node.RollBack", &kv, &res)

			if err != nil {
				t.logger.Print("Error in ROLLBACK call:" + err.Error())
			}

			client.Close()
		}

		*result = false
		return errors.New("Cannot write.")
	}

	var successfulCommit []string

	// Writing the values to my copy.
	t.mutex.Lock()
	for _, elem := range myRepIndices {

		t.Data[elem][kv.Key] = kv.Value
	}
	t.mutex.Unlock()

	// Sending COMMIT to all the redundant successors.
	res := true

	for service, listreplicas := range querySent {

		if _, ok := querySent[service]; !ok {
			continue
		}

		client, err := jsonrpc.Dial("tcp", service)
		if err != nil {
			t.logger.Print("Error in dial for COMMIT:" + err.Error())
			continue
		}

		kvr := KeyValueReplica{*kv, listreplicas}
		err = client.Call("Node.CommitWrite", &kvr, &res)

		client.Close()

		if err != nil || res == false {
			t.logger.Print("Error in COMMIT RPC:" + err.Error())
			res = false
			break
		}

		successfulCommit = append(successfulCommit, service)
	}

	if res == false {
		t.logger.Print("Error during COMMIT of some process. Rolling back.")
		for _, elem := range myRepIndices {
			delete(t.Data[elem], kv.Key)
		}

		for _, service := range successfulCommit {
			client, err := jsonrpc.Dial("tcp", service)
			if err != nil {
				t.logger.Print("Error in dialing:" + err.Error())
				continue
			}

			var dummy bool
			kvr := KeyValueReplica{*kv, querySent[service]}
			err = client.Call("Node.CommitRollBack", &kvr, &dummy)

			client.Close()
		}

		*result = false
	} else {
		*result = true
	}

	return nil
}

// Method to write a key value pair in the ring.
func (t *Node) write(key string, value string) bool {
	keyHash := t.Config.getKey1(key)

	firstSuccessor := t.findSuccessor(keyHash)
	service := firstSuccessor.getAddress()

	client, err := jsonrpc.Dial("tcp", service)
	if err != nil {
		t.logger.Print("Error in contacting the first successor of the key:" + key)
		t.logger.Print(err.Error())
	}

	kv := KeyValue{key, value}
	var result bool

	client.Call("Node.SetValue", &kv, &result)
	client.Close()

	return result
}

// Commit the delete of a key value pair.
func (t *Node) CommitDelete(kvr *KeyValueReplica, result *bool) error {
	for _, elem := range kvr.Replicanum {
		delete(t.Data[elem], kvr.KV.Key)
	}

	t.mutex.Unlock()

	*result = true
	return nil
}

// Assuming #Nodes > k.
// Ecported method to delete a key value pair.
func (t *Node) DeleteKV(kv *KeyValue, result *bool) error {

	successorsReplied := make(map[string]string)
	querySent := make(map[string][]int)

	myRepIndices := []int{0}
	canCommit := true

	// Sending QUERY TO COMMIT to all the redundant successors.
	for i := 0; i < t.Config.k; i++ {

		service := t.SuccessorList[i].getAddress()

		if service == t.myNodeIdentifier.getAddress() {
			myRepIndices = append(myRepIndices, int(i+1))
			continue
		} else if _, ok := querySent[service]; ok {
			querySent[service] = append(querySent[service], int(i+1))
			continue
		} else {
			querySent[service] = []int{i + 1}
		}

		client, err := jsonrpc.Dial("tcp", service)
		if err != nil {
			t.logger.Print("Error in dial QUERY TO COMMIT: " + err.Error())
			canCommit = false
			break
		}

		var result bool
		ret := client.Go("Node.QueryToCommit", &kv, &result, nil)

		select {
		case ret = <-ret.Done:
			if ret.Error != nil {
				t.logger.Print("Error in QUERY TO COMMIT: " + err.Error())
				canCommit = false
				break
			}
		case <-time.After(TIMEOUT_QUERY_TO_COMMIT):
			fmt.Println("Timeout in QUERY TO COMMIT.")
			canCommit = false
			break
		}

		client.Close()

		successorsReplied[service] = service

	}

	// Rollback if something goes wrong.
	if canCommit == false {
		for _, v := range successorsReplied {
			service := v

			client, err := jsonrpc.Dial("tcp", service)
			if err != nil {
				t.logger.Print("Error dial in ROLLBACK: " + err.Error())
				continue
				// Cannot commit.
			}

			var result bool
			err = client.Call("Node.RollBack", &kv, &result)

			if err != nil {
				t.logger.Print("Error in ROLLBACK:" + err.Error())
			}

			client.Close()
		}

		*result = false
		return errors.New("Error in delete.")
	}

	// Sending COMMIT to all the redundant successors.
	for service, listreplicas := range querySent {
		client, err := jsonrpc.Dial("tcp", service)
		if err != nil {
			t.logger.Print("Error in dial during COMMIT:" + err.Error())
			break
		}

		var result bool
		kvr := KeyValueReplica{*kv, listreplicas}

		err = client.Call("Node.CommitDelete", &kvr, &result)

		if err != nil || result == false {
			t.logger.Print("Error in COMMIT:" + err.Error())
			continue
		}

		client.Close()

		if result == false {
			// Rollback does not matter as if call to CommitDelete fails means that the node died.
		}
	}

	// Deleting the key-value pairs from my replica.
	t.mutex.Lock()
	for _, val := range myRepIndices {
		if _, ok := t.Data[val][kv.Key]; ok {
			delete(t.Data[val], kv.Key)
		}
	}
	t.mutex.Unlock()

	*result = true
	return nil

}

// Removes a key value pair from the ring.
func (t *Node) remove(key string) bool {
	keyHash := t.Config.getKey1(key)

	firstSuccessor := t.findSuccessor(keyHash)
	service := firstSuccessor.getAddress()

	client, err := jsonrpc.Dial("tcp", service)
	if err != nil {
		t.logger.Print("Error dialing: " + err.Error())
		return false
	}

	kv := KeyValue{key, ""}
	var result bool

	client.Call("Node.DeleteKV", &kv, &result)
	client.Close()

	return result
}

/////////////////////////////////////////////////////////////////////////////////////
// Handling of transfer of key-value pairs on node leave/join.

func (t *Node) transferKV() {

	service := t.SuccessorList[0].getAddress()
	client, err := jsonrpc.Dial("tcp", service)
	if err != nil {
		t.logger.Print("Error transfering KV dialing: " + err.Error())
		return
	}

	myhash := t.Config.getKey(t.myNodeIdentifier)

	t.mutex.Lock()
	err = client.Call("Node.AdjustKVJoin", myhash, &t.Data)
	t.mutex.Unlock()

	if err != nil {
		t.logger.Print("Error transfering KV RPC: " + err.Error())
		return
	}

	client.Close()
	fmt.Println("Key values assigned from successor")

}

type SplitDict struct {
	Dict1 map[string]string
	Dict2 map[string]string
	Num   int
}

func (t *Node) AdjustKVJoin(newnodehash uint32, reply *[]map[string]string) error {
	myhash := t.Config.getKey(t.myNodeIdentifier)
	var splitdict SplitDict
	splitdict.Dict1 = make(map[string]string)
	splitdict.Dict2 = make(map[string]string)
	mydict := t.Data

	for k, v := range mydict[0] {
		keyhash := t.Config.getKey1(k)
		if inbetweenUpperClosed(newnodehash, keyhash, myhash) {
			delete(mydict[0], k)
			splitdict.Dict1[k] = v
		}
	}
	splitdict.Dict2 = mydict[0]

	go func() {
		// Need to write time interval at the top.
		time.Sleep(500 * time.Millisecond)

		t.mutex.Lock()

		copy(t.Data[2:], t.Data[1:])
		t.Data[0] = splitdict.Dict1
		t.Data[1] = splitdict.Dict2

		t.mutex.Unlock()

		for i := 0; i < t.Config.k; i++ {
			splitdict.Num = i + 1
			service := t.SuccessorList[i].getAddress()
			client, err := jsonrpc.Dial("tcp", service)
			if err != nil {
				t.logger.Print("Error adjusting KV dialing: " + err.Error())
				break
			}

			myhash = t.Config.getKey(t.myNodeIdentifier)
			var reply bool
			err = client.Call("Node.AdjustKVJoinSucc", splitdict, &reply)
			if err != nil {
				t.logger.Print("Error adjusting KV RPC: " + err.Error())
				break
			}
			client.Close()
		}
	}()

	*reply = mydict

	return nil
}

func (t *Node) AdjustKVJoinSucc(splitdict SplitDict, reply *bool) error {
	t.mutex.Lock()

	if splitdict.Num+2 <= t.Config.k {
		copy(t.Data[splitdict.Num+2:], t.Data[splitdict.Num+1:])
	}

	if splitdict.Num+1 <= t.Config.k {
		t.Data[splitdict.Num+1] = splitdict.Dict2
	}

	t.Data[splitdict.Num] = splitdict.Dict1

	t.mutex.Unlock()

	t.logger.Print("Key value adjustment done on node join.")

	*reply = true
	return nil
}

type ArgKVLeave struct {
	Dictarr []map[string]string
	Start   int
	Club    int
}

func (t *Node) AdjustKVLeave(num int) {
	for i := 0; i < t.Config.k+1; i++ {

		service := t.SuccessorList[i].getAddress()
		givekv := t.Data[:t.Config.k-i]
		client, err := jsonrpc.Dial("tcp", service)
		if err != nil {
			t.logger.Print("Error adjusting KV leave dialing: " + err.Error())
			break
		}

		arg := ArgKVLeave{givekv, i, num}
		var reply bool
		err = client.Call("Node.AdjustKVLeaveSucc", arg, &reply)

		if err != nil {
			t.logger.Print("Error adjusting KV leave RPC: " + err.Error())
			break
		}
	}
}

func (t *Node) AdjustKVLeaveSucc(arg ArgKVLeave, reply *bool) error {
	t.mutex.Lock()

	if arg.Start+arg.Club > t.Config.k {
		service := t.Predecessor.getAddress()
		client, err := jsonrpc.Dial("tcp", service)
		if err != nil {
			t.logger.Print("Error pred dialing: " + err.Error())
			*reply = false
		}

		var dictarr []map[string]string
		err = client.Call("Node.GetDict", 0, &dictarr)

		if err != nil {
			t.logger.Print("Error pred RPC: " + err.Error())
			*reply = false
		}

		copy(t.Data[1:], dictarr[:t.Config.k])

	} else {
		for i := arg.Start + 1; i <= arg.Start+arg.Club; i++ {
			for k, v := range t.Data[i] {
				t.Data[arg.Start][k] = v
			}
		}

		if len(arg.Dictarr) != 0 {
			copy(t.Data[arg.Start+1:], arg.Dictarr)
		}
	}

	t.mutex.Unlock()

	t.logger.Print("Key value adjustment done on node leave.")

	*reply = true
	return nil
}

func (t *Node) GetDict(i int, reply *[]map[string]string) error {
	*reply = t.Data
	return nil
}
