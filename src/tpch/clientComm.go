package tpch

import (
	"antidote"
	"fmt"
	"net"
	"proto"
	"runtime"
	"runtime/debug"
	"time"
	"tools"

	pb "github.com/golang/protobuf/proto"
)

//Handles client communication for initial data loading and updates.
//clientQueries handles the communication for queries, since replies need to be received during query execution.

type QueuedMsg struct {
	pb.Message
	code byte
}

type Channels struct {
	procTableChan chan int
	prepSendChan  chan int
	dataChans     []chan QueuedMsg
	indexChans    []chan QueuedMsg
}

const (
	QUEUE_COMPLETE  = byte(255)
	DEFAULT_REPLICA = 0
)

var (
	//servers = []string{"127.0.0.1:8087"}
	//servers = []string{"127.0.0.1:8087", "127.0.0.1:8087", "127.0.0.1:8087", "127.0.0.1:8087", "127.0.0.1:8087"}
	//servers = []string{"127.0.0.1:8087", "127.0.0.1:8088", "127.0.0.1:8089", "127.0.0.1:8090", "127.0.0.1:8091"}
	servers []string

	channels = Channels{
		//First two are used internally by clientDataLoad. The later two are initialized in tpchClient.go
		procTableChan: make(chan int, 10),
		prepSendChan:  make(chan int, 10),
	}

	//Filled automatically by configLoader
	MAX_BUFF_PROTOS, FORCE_PROTO_CLEAN int
)

func connectToServers() {
	for i, server := range servers {
		conn, err := net.Dial("tcp", server)
		tools.CheckErr("Network connection establishment err", err)
		conns[i] = conn
		if DOES_DATA_LOAD {
			go handleServerComm(i)
		}
	}
}

func handleServerComm(connIndex int) {
	conn := conns[connIndex]
	start := time.Now().UnixNano() / 1000000
	fmt.Println("Starting to send data protos for server", servers[connIndex], "...")
	complete := false
	channel := channels.dataChans[connIndex]
	i := 0
	for !complete {
		msg := <-channel
		if msg.code == QUEUE_COMPLETE {
			complete = true
		} else {
			//fmt.Println("Sending proto...")
			antidote.SendProto(msg.code, msg.Message, conn)
			//fmt.Println("Receiving proto...")
			antidote.ReceiveProto(conn)
			//fmt.Println("Proto received.")
			i++
			if i%FORCE_PROTO_CLEAN == 0 {
				fmt.Println("Sent data proto number", i)
				if connIndex == 0 {
					runtime.GC()
				}
			}
		}
	}
	fmt.Println("All data protos for server", servers[connIndex], "have been sent.")
	end := time.Now().UnixNano() / 1000000
	times.sendDataProtos[connIndex] = end - start
	fmt.Printf("Time to send dataProtos for %d: %d\n", connIndex, end-start)

	if !isIndexGlobal || splitIndexLoad || connIndex == 0 {
		handleIndexComm(connIndex)
	}
}

func handleIndexComm(connIndex int) {
	fmt.Println("Started index comm for", connIndex)
	conn := conns[connIndex]
	start := time.Now().UnixNano() / 1000000

	//Start a txn for the indexes
	startTxn := antidote.CreateStartTransaction(nil)
	antidote.SendProto(antidote.StartTrans, startTxn, conn)
	_, txnReplyProto, _ := antidote.ReceiveProto(conn)
	txnId := txnReplyProto.(*proto.ApbStartTransactionResp).GetTransactionDescriptor()
	channel := channels.indexChans[connIndex]

	complete := false
	for !complete {
		msg := <-channel
		if msg.code == QUEUE_COMPLETE {
			complete = true
		} else {
			msg.Message.(*proto.ApbUpdateObjects).TransactionDescriptor = txnId
			antidote.SendProto(msg.code, msg.Message, conn)
			antidote.ReceiveProto(conn)
		}
	}

	//Commit for indexes
	commitTxn := antidote.CreateCommitTransaction(txnId)
	antidote.SendProto(antidote.CommitTrans, commitTxn, conn)
	antidote.ReceiveProto(conn)
	fmt.Println("All index protos have been sent.")
	end := time.Now().UnixNano() / 1000000
	times.sendIndexProtos = end - start
	times.totalData = end - times.startTime/1000000
	fmt.Println("Time taken to send index protos: ", end-start)

	if connIndex == 0 {
		runtime.GC()
		debug.FreeOSMemory()
		/*
			if QUERY_BENCH {
				startQueriesBench()
			} else {
				sendQueries(conn)
			}
		*/
	}
	//sendQueriesNoIndex(conn)
	//printExecutionTimes()
}

//Note: Assumes updatesComm and serverComm aren't running at the same time!
//It's safe to run updates after data ends though.
func handleUpdatesComm(connIndex int) {
	fmt.Println("Started updates comm for", connIndex)
	conn := conns[connIndex]
	channel := channels.dataChans[connIndex]
	//var txnId []byte
	//var txnProto pb.Message
	complete := false
	for !complete {
		msg := <-channel
		if msg.code == QUEUE_COMPLETE {
			complete = true
		} else {
			/*	ignore(conn)
				}*/
			antidote.SendProto(msg.code, msg.Message, conn)
			antidote.ReceiveProto(conn)
		}

		/*else if msg.code == antidote.StartTrans {
			fmt.Println("Starting to send startTxn update proto")
			antidote.SendProto(msg.code, msg.Message, conn)
			fmt.Println("Sent startTxn update proto")
			_, txnProto, _ = antidote.ReceiveProto(conn)
			fmt.Println("Received startTxn proto reply")
			txnId = txnProto.(*proto.ApbStartTransactionResp).TransactionDescriptor
		} else if msg.code == antidote.CommitTrans {
			fmt.Println("Starting to send commitTxn update proto")
			antidote.SendProto(msg.code, msg.Message, conn)
			fmt.Println("Sent commitTxn update proto")
			antidote.ReceiveProto(conn)
			fmt.Println("Received commitTxn proto reply")
		} else {
			msg.Message.(*proto.ApbUpdateObjects).TransactionDescriptor = txnId
			fmt.Println("Starting to send update data proto...")
			antidote.SendProto(msg.code, msg.Message, conn)
			fmt.Println("Sent update data proto...")
			antidote.ReceiveProto(conn)
			fmt.Println("Received update proto reply...")
		}*/
	}

	fmt.Println("All update protos have been sent.")
}
