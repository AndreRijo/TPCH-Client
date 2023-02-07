package client

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"potionDB/src/antidote"
	"potionDB/src/clocksi"
	"potionDB/src/crdt"
	"potionDB/src/proto"
	"strconv"
	"strings"
	"time"

	pb "github.com/golang/protobuf/proto"
)

const (
	TEST_PROTO, TEST_CLIENT, TEST_SERVER, TEST_SERVER_READ_ONLY = 1, 2, 3, 4
	TEST_CLIENT_REPLY, TEST_SERVER_REPLY                        = 5, 6
)

var (
	nProtosTarget, nElems, trashSize, nRoutines = 0, 0, 0, 0
	protoTestServer                             string
	testType                                    int
	//msgSize                                     int
)

func ProtoTest() {
	nProtosFlag := flag.String("p_n_protos", "10000000", "number of protos to create")
	nBytesFlag := flag.String("p_n_bytes", "100", "number entries in each protobuf")
	nRoutinesFlag := flag.String("p_n_routines", "1", "number of routines to create protobufs")
	nModeFlag := flag.String("p_n_mode", "proto", "modes available - 'proto', 'sender', 'receiver'")
	//dummyDataSizeString = flag.String("initialMem", "1000000000", "the size (bytes) of the initial block of data. This is used to avoid Go's GC to overcollect garbage and hinder system performance.")
	//flag.Parse()
	cfgs := loadFlags()
	servers = strings.Split(cfgs.GetConfig("servers"), " ")

	if isFlagValid(nProtosFlag) {
		nProtosTarget, _ = strconv.Atoi(*nProtosFlag)
	}
	if isFlagValid(nBytesFlag) {
		nElems, _ = strconv.Atoi(*nBytesFlag)
	}
	if isFlagValid(nRoutinesFlag) {
		nRoutines, _ = strconv.Atoi(*nRoutinesFlag)
	}
	if isFlagValid(nModeFlag) {
		testTypeString := strings.ToLower(strings.Trim(*nModeFlag, " "))
		fmt.Println("Test flag is valid. Values:", *nModeFlag, testTypeString)
		if testTypeString == "proto" {
			testType = TEST_PROTO
		} else if testTypeString == "client" {
			testType = TEST_CLIENT
		} else if testTypeString == "server" {
			testType = TEST_SERVER
		} else if testTypeString == "server_read_only" {
			testType = TEST_SERVER_READ_ONLY
		} else if testTypeString == "client_reply" {
			testType = TEST_CLIENT_REPLY
		} else if testTypeString == "server_reply" {
			testType = TEST_SERVER_REPLY
		} else {
			fmt.Println("Invalid test type. Exitting.")
			os.Exit(0)
		}
	}
	/*
		if isFlagValid(dummyDataSizeString) {
			trashSize, _ = strconv.Atoi(*dummyDataSizeString)
		}
	*/
	trashSize = 10000000000
	fmt.Println(*nProtosFlag)
	fmt.Println(*nBytesFlag)
	fmt.Println(*nRoutinesFlag)
	fmt.Println(*dummyDataSizeString)
	fmt.Println(*nModeFlag)
	fmt.Println(servers)
	fmt.Println(nProtosTarget, nElems, nRoutines, trashSize, testType, servers)
	if nProtosTarget == 0 || nElems == 0 || trashSize == 0 || nRoutines == 0 {
		fmt.Println("Warning - one of the flags is invalid (it is set but not valid), so default values will be used")
		nProtosTarget, nElems, trashSize, nRoutines = 10000000, 100, 1000000000, 1
	}
	if testType == 0 {
		fmt.Println("Warning - test type undefined, assuming proto.")
		testType = TEST_PROTO
	}

	trash = make([]byte, trashSize) //To prevent Go's GC from being weird
	chooseTest()
}

func chooseTest() {
	replyChan := make(chan int64, nRoutines)
	totalBytes := int64(0)
	var start int64
	crashedClients := 0
	if testType == TEST_PROTO {
		start = time.Now().UnixNano()
		for i := 0; i < nRoutines; i++ {
			go doProtoTest(replyChan)
		}
	} else if testType == TEST_CLIENT {
		conns := makeClientConns()
		start = time.Now().UnixNano()
		for i := 0; i < nRoutines; i++ {
			conn := conns[i]
			go doProtoSendTest(replyChan, conn, i)
		}
	} else if testType == TEST_CLIENT_REPLY {
		replySize := calculateReplySize()
		conns := makeClientConns()
		start = time.Now().UnixNano()
		for i := 0; i < nRoutines; i++ {
			conn := conns[i]
			go doProtoSendReceiveTest(replyChan, conn, i, replySize)
		}
	} else if testType == TEST_SERVER {
		msgSize := calculateMsgSize()
		server, _ := net.Listen("tcp", "0.0.0.0:"+"8087")
		defer server.Close()
		for i := 0; i < nRoutines; i++ {
			conn, _ := server.Accept()
			fmt.Printf("Starting server %d with %v\n", i, conn)
			go processProtoReceive(conn, replyChan, i, msgSize)
			time.Sleep(50 * time.Millisecond)
		}
		start = time.Now().UnixNano()

	} else if testType == TEST_SERVER_READ_ONLY {
		msgSize := calculateMsgSize()
		server, _ := net.Listen("tcp", "0.0.0.0:"+"8087")
		defer server.Close()
		for i := 0; i < nRoutines; i++ {
			conn, _ := server.Accept()
			fmt.Printf("Starting server %d with %v\n", i, conn)
			go processProtoReceiveRead(conn, replyChan, i, msgSize)
			time.Sleep(50 * time.Millisecond)
		}
		start = time.Now().UnixNano()
	} else if testType == TEST_SERVER_REPLY {
		msgSize := calculateMsgSize()
		server, _ := net.Listen("tcp", "0.0.0.0:"+"8087")
		defer server.Close()
		for i := 0; i < nRoutines; i++ {
			conn, _ := server.Accept()
			fmt.Printf("Starting server %d with %v (no wait)\n", i, conn)
			go processProtoReceiveSend(conn, replyChan, i, msgSize)
		}
		start = time.Now().UnixNano()
	}
	fmt.Println("Waiting for test to finish...")
	for i := 0; i < nRoutines; i++ {
		fmt.Println("Waiting reply from routine", i)
		bytes := <-replyChan
		if bytes == 0 {
			//Connection crashed. *sigh*
			crashedClients++
		}
		totalBytes += bytes
		fmt.Println("Received reply from routine", i, "totalBytes now:", totalBytes)
	}
	nRoutines -= crashedClients
	end := time.Now().UnixNano()
	fmt.Println("Printing statistics...")
	printProtoStatistics(totalBytes, start, end)
}

func makeClientConns() (conns []net.Conn) {
	conns = make([]net.Conn, nRoutines)
	var err error
	dialer := net.Dialer{KeepAlive: -1}
	for i := 0; i < nRoutines; i++ {
		conns[i], err = dialer.Dial("tcp", servers[0])
		if err != nil {
			fmt.Println("Error on connecting to server:", err)
			os.Exit(0)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return
}

func doProtoTest(replyChan chan int64) {
	elems := getDummyElemsSlice()
	replyChan <- createProtos(elems)
}

func doProtoSendTest(replyChan chan int64, conn net.Conn, client int) {
	elems := getDummyElemsSlice()
	replyChan <- createAndSendProtos(elems, conn, client)
}

func doProtoSendReceiveTest(replyChan chan int64, conn net.Conn, client, replySize int) {
	elems := getDummyElemsSlice()
	replyChan <- createAndSendReceiveProtos(elems, conn, client, replySize)
}

func getDummyElemsSlice() [][]byte {
	reply := make([][]byte, nElems)
	for i := range reply {
		reply[i] = make([]byte, 8)
		binary.LittleEndian.PutUint64(reply[i], rand.Uint64())
	}
	return reply
}

func createProtos(elems [][]byte) (bytes int64) {
	//proto := &proto.ApbGetSetResp{Value: elems}
	proto := makeProto(elems)
	//proto := makeCounterProto()
	var buf []byte
	bytes = int64(0)
	nProtos := 0
	//start := time.Now().UnixNano()
	//fmt.Println("Starting to create protos")
	printTarget := nProtosTarget / 5
	for nProtos < nProtosTarget {
		buf, _ = pb.Marshal(proto)
		bytes += int64(len(buf))
		nProtos++
		if nProtos%printTarget == 0 {
			fmt.Println("Current protos done:", nProtos)
		}
	}
	fmt.Println("Finished creating protos.")
	//finish := time.Now().UnixNano()
	return
}

func createAndSendProtos(elems [][]byte, conn net.Conn, client int) (bytes int64) {
	proto := makeProto(elems)
	//proto := makeCounterProto()
	var buf []byte
	bytes = int64(0)
	var err error
	nProtos, nWritten := 0, 0
	//start := time.Now().UnixNano()
	//fmt.Println("Starting to create protos")
	printTarget := nProtosTarget / 5
	for nProtos < nProtosTarget {
		buf, err = pb.Marshal(proto)
		if err != nil {
			fmt.Printf("[%d]Error on marshal: %s\n", client, err)
			return 0
		}
		bytes += int64(len(buf))
		nProtos++
		nWritten, err = conn.Write(buf)
		if nWritten != len(buf) || err != nil {
			fmt.Printf("[%d]Warning - written wrong number of bytes or err is not nil. "+
				"Written: %d; Supposed: %d; Error: %v\n", client, nWritten, len(buf), err)
			return 0
		}
		if nProtos%printTarget == 0 {
			fmt.Printf("[%d]Current protos done: %d (%d) (%d) %v\n", client, nProtos, bytes, len(buf), conn)
		}
	}
	fmt.Printf("[%d]Finished creating & sending protos.\n", client)
	//finish := time.Now().UnixNano()
	return
}

func createAndSendReceiveProtos(elems [][]byte, conn net.Conn, client, replySize int) (bytes int64) {
	sendProto := makeProto(elems)
	//sendProto := makeCounterProto()
	var buf []byte
	receiveBuf := make([]byte, replySize)
	bytes = int64(0)
	var err error
	var replyMsg pb.Message = &proto.ApbStaticReadObjects{}
	nProtos, nWritten, nRead, currRead := 0, 0, 0, 0
	//start := time.Now().UnixNano()
	//fmt.Println("Starting to create protos")
	printTarget := nProtosTarget / 5
	for nProtos < nProtosTarget {
		buf, err = pb.Marshal(sendProto)
		if err != nil {
			fmt.Printf("[%d]Error on marshal: %s\n", client, err)
			return 0
		}
		bytes += int64(len(buf))
		nProtos++
		nWritten, err = conn.Write(buf)
		if nWritten != len(buf) || err != nil {
			fmt.Printf("[%d]Warning - written wrong number of bytes or err is not nil. "+
				"Written: %d; Supposed: %d; Error: %v\n", client, nWritten, len(buf), err)
			return 0
		}
		for nRead = 0; nRead < replySize; nRead += currRead {
			currRead, err = conn.Read(receiveBuf[nRead:])
			if err != nil {
				fmt.Printf("[%d]Error on receive: %s\n", client, err)
				return 0
			}
		}
		bytes += int64(nRead)
		err = pb.Unmarshal(receiveBuf, replyMsg)
		if err != nil {
			fmt.Printf("[%d]Error on unmarshaling: %s\n", client, err)
			return 0
		}
		if nProtos%printTarget == 0 {
			fmt.Printf("[%d]Current protos done: %d (%d) (%d) %v\n", client, nProtos, bytes, len(buf), conn)
		}
	}
	fmt.Printf("[%d]Finished creating & sending & receiving protos.\n", client)
	//finish := time.Now().UnixNano()
	return
}

func makeCounterProto() (m pb.Message) {
	readRepProto := &proto.ApbReadObjectResp{Counter: &proto.ApbGetCounterResp{Value: pb.Int32(10)}}
	readRepProtoSlice := []*proto.ApbReadObjectResp{readRepProto}
	readObjsResp := &proto.ApbReadObjectsResp{Objects: readRepProtoSlice, Success: pb.Bool(true)}
	clock := clocksi.ClockSiTimestamp{VectorClock: make(map[int16]int64)}
	clock.VectorClock[1000] = 100
	clock.VectorClock[5000] = 50
	clock.VectorClock[-2000] = 20
	clock.VectorClock[10403] = 94
	clock.VectorClock[-20492] = 8
	staticReadResp := &proto.ApbStaticReadObjectsResp{
		Objects:    readObjsResp,
		Committime: antidote.CreateCommitOkResp(antidote.TransactionId(1000), clock),
	}
	return staticReadResp
}

func makeProto(elems [][]byte) (m pb.Message) {
	readRepProto := &proto.ApbReadObjectResp{Set: &proto.ApbGetSetResp{Value: elems}}
	readRepProtoSlice := []*proto.ApbReadObjectResp{readRepProto}
	readObjsResp := &proto.ApbReadObjectsResp{Objects: readRepProtoSlice, Success: pb.Bool(true)}
	clock := clocksi.ClockSiTimestamp{VectorClock: make(map[int16]int64)}
	clock.VectorClock[1000] = 100
	clock.VectorClock[5000] = 50
	clock.VectorClock[-2000] = 20
	clock.VectorClock[10403] = 94
	clock.VectorClock[-20492] = 8
	staticReadResp := &proto.ApbStaticReadObjectsResp{
		Objects:    readObjsResp,
		Committime: antidote.CreateCommitOkResp(antidote.TransactionId(1000), clock),
	}

	return staticReadResp
}

func printProtoStatistics(bytes, start, finish int64) {
	diffMs := (finish - start) / int64(time.Millisecond)
	bytesPerMs := bytes / diffMs
	bytesPerS := bytesPerMs * 1000
	MBytesPerS := bytesPerS / 1000000
	fmt.Printf("Total time: %dms. Total protobufs: %d. Total bytes: %d. MBs per second: %dMB/s\n", diffMs, nProtosTarget*nRoutines, bytes, MBytesPerS)
}

//proto.ApbGetSetResp{Value: ElementArrayToByteMatrix(crdtState.Elems)}
/*
func ElementArrayToByteMatrix(elements []Element) (converted [][]byte) {
	converted = make([][]byte, len(elements))
	for i, value := range elements {
		converted[i] = []byte(value)
	}
	return
}
*/

func processProtoReceiveSend(conn net.Conn, resultChan chan int64, client, msgSize int) {
	sendProto := makeReplyProto()
	nProtos, totalBytesRead, bytesRead, currRead, nWritten := 0, int64(0), 0, 0, 0
	var msg pb.Message = &proto.ApbStaticReadObjectsResp{}
	var err error
	size := msgSize
	buf := make([]byte, size)
	var sendBuf []byte
	elems := make([][]byte, size)
	printTarget := nProtosTarget / 5
	for nProtos < nProtosTarget {
		for bytesRead = 0; bytesRead < size; bytesRead += currRead {
			currRead, err = conn.Read(buf[bytesRead:])
			if err != nil {
				fmt.Printf("[%d]Error on receive: %s\n", client, err)
				resultChan <- 0
				return
			}
		}
		totalBytesRead += int64(bytesRead)
		err = pb.Unmarshal(buf, msg)
		if err != nil {
			fmt.Printf("[%d]Error on unmarshaling: %s\n", client, err)
			resultChan <- 0
			return
		}
		elems = extractData(msg)
		sendBuf, err = pb.Marshal(sendProto)
		if err != nil {
			fmt.Printf("[%d]Error on marshal: %s\n", client, err)
			resultChan <- 0
			return
		}
		totalBytesRead += int64(len(sendBuf))
		nWritten, err = conn.Write(sendBuf)
		if nWritten != len(sendBuf) || err != nil {
			fmt.Printf("[%d]Warning - written wrong number of bytes or err is not nil. "+
				"Written: %d; Supposed: %d; Error: %v\n", client, nWritten, len(sendBuf), err)
			resultChan <- 0
			return
		}
		nProtos++
		if nProtos%printTarget == 0 {
			fmt.Printf("[%d]Current protos done: %d\n", client, nProtos)
		}
	}
	ignore(elems)
	ignore(msg)
	fmt.Printf("[%d]Finished receiving protos.\n", client)
	resultChan <- totalBytesRead
}

func processProtoReceive(conn net.Conn, resultChan chan int64, client, msgSize int) {
	nProtos, totalBytesRead, bytesRead, currRead := 0, int64(0), 0, 0
	var msg pb.Message = &proto.ApbStaticReadObjectsResp{}
	var err error
	size := msgSize
	buf := make([]byte, size)
	elems := make([][]byte, size)
	printTarget := nProtosTarget / 5
	for nProtos < nProtosTarget {
		for bytesRead = 0; bytesRead < size; bytesRead += currRead {
			currRead, err = conn.Read(buf[bytesRead:])
			if err != nil {
				fmt.Printf("[%d]Error on receive: %s\n", client, err)
				resultChan <- 0
				return
			}
		}
		totalBytesRead += int64(bytesRead)
		err = pb.Unmarshal(buf, msg)
		if err != nil {
			fmt.Printf("[%d]Error on unmarshaling: %s\n", client, err)
			resultChan <- 0
			return
		}
		elems = extractData(msg)
		nProtos++
		if nProtos%printTarget == 0 {
			fmt.Printf("[%d]Current protos done: %d\n", client, nProtos)
		}
	}
	ignore(elems)
	ignore(msg)
	fmt.Printf("[%d]Finished receiving protos.\n", client)
	resultChan <- totalBytesRead
}

func processProtoReceiveRead(conn net.Conn, resultChan chan int64, client, msgSize int) {
	nProtos, totalBytesRead, bytesRead, currRead := 0, int64(0), 0, 0
	var msg pb.Message = &proto.ApbStaticReadObjectsResp{}
	var err error
	size := msgSize
	buf := make([]byte, size)
	elems := make([][]byte, size)
	printTarget := nProtosTarget / 5
	for nProtos < nProtosTarget {
		for bytesRead = 0; bytesRead < size; bytesRead += currRead {
			currRead, err = conn.Read(buf[bytesRead:])
			if err != nil {
				fmt.Printf("[%d]Error on receive: %s\n", client, err)
				resultChan <- 0
				return
			}
		}
		totalBytesRead += int64(bytesRead)
		nProtos++
		if nProtos%printTarget == 0 {
			fmt.Printf("[%d]Current protos done: %d\n", client, nProtos)
		}
	}
	ignore(elems)
	ignore(msg)
	fmt.Printf("[%d]Finished receiving protos.\n", client)
	resultChan <- totalBytesRead
}

func extractData(m pb.Message) (elems [][]byte) {
	staticReadResp := m.(*proto.ApbStaticReadObjectsResp)
	setProto := staticReadResp.Objects.Objects[0].Set
	return setProto.Value
}

func calculateMsgSize() int {
	//Creates temporary msg
	elems := getDummyElemsSlice()
	msg := makeProto(elems)
	//msg := makeCounterProto()
	buf, _ := pb.Marshal(msg)
	return len(buf)
}

func calculateReplySize() int {
	//Creates temporary msg
	replyProto := makeReplyProto()
	buf, _ := pb.Marshal(replyProto)
	return len(buf)
}

func makeReplyProto() *proto.ApbStaticReadObjects {
	readParams := []antidote.ReadObjectParams{antidote.ReadObjectParams{KeyParams: antidote.KeyParams{Key: "test",
		CrdtType: proto.CRDTType_RWSET, Bucket: "test"}, ReadArgs: crdt.StateReadArguments{}}}
	return antidote.CreateStaticReadObjs(nil, readParams)
}
