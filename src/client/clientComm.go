package client

import (
	"encoding/csv"
	"fmt"
	"net"
	"os"
	"potionDB/src/antidote"
	"potionDB/src/proto"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	pb "github.com/golang/protobuf/proto"
)

//Handles client communication for initial data loading and updates.
//clientQueries handles the communication for queries, since replies need to be received during query execution.

type QueuedMsg struct {
	pb.Message
	code byte
}

type QueuedMsgWithStat struct {
	QueuedMsg
	nData    int
	dataType int
}

type Channels struct {
	procTableChan chan int
	prepSendChan  chan int
	dataChans     []chan QueuedMsg
	indexChans    []chan QueuedMsg
	updateChans   []chan QueuedMsgWithStat
}

const (
	QUEUE_COMPLETE                    = byte(255)
	DEFAULT_REPLICA                   = 0
	NEW_TYPE, REMOVE_TYPE, INDEX_TYPE = 0, 1, 2 //Types for QueuedMsgWithStat.dataType
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
	NOTIFY_ADDRESS                     string
)

//Keeps trying to estabilish a connection until it succeeds.
func ConnectAndRetry(ip string) (conn net.Conn) {
	var err error
	for {
		conn, err = net.Dial("tcp", ip)
		if err == nil { //Success
			return
		}
	}
}

func connectToServers() {
	toRepeat, index := make([]string, 0), make([]int, 0)
	timeout, timeoutScale := 1000.0, 1.5 //ms
	for i, server := range servers {
		conn, err := net.Dial("tcp", server)
		//tools.CheckErr("Network connection establishment err", err)
		if err != nil {
			fmt.Println("[CC]Network connection establishment err", err, ". Trying again.")
			toRepeat = append(toRepeat, server)
			index = append(index, i)
		} else {
			conns[i] = conn
			if DOES_DATA_LOAD && LOAD_BASE_DATA {
				go handleServerComm(i)
			} else if DOES_DATA_LOAD {
				go handleIndexComm(i)
			}
		}
	}
	for len(toRepeat) > 0 {
		fmt.Println("[CC]Sleeping before retrying connection.")
		time.Sleep(time.Duration(timeout) * time.Millisecond)
		timeout *= timeoutScale
		oldLen := len(toRepeat)
		//Has to try reconnecting to some servers
		for i := 0; i < oldLen; i++ {
			server := toRepeat[i]
			//for i, server := range toRepeat {
			conn, err := net.Dial("tcp", server)
			//tools.CheckErr("Network connection establishment err", err)
			if err != nil {
				fmt.Println("[CC]Network connection establishment err", err, ". Trying again.")
				toRepeat = append(toRepeat, server)
				index = append(index, index[i])
			} else {
				conns[index[i]] = conn
				if DOES_DATA_LOAD && LOAD_BASE_DATA {
					go handleServerComm(index[i])
				} else if DOES_DATA_LOAD {
					go handleIndexComm(index[i])
				}
			}
		}
		toRepeat, index = toRepeat[oldLen:], index[oldLen:]
	}
	fmt.Println("[CC]Connection established to all servers")
}

func handleServerComm(connIndex int) {
	conn := conns[connIndex]
	ignore(conn)
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
			//TODO: Convert msg to bytes BEFORE sending to this channel.
			//That way the time spent generating the msg bytes isn't wasted here.
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
	timeSpent := end - start
	times.sendDataProtos[connIndex] = timeSpent
	dataloadStats.Lock()
	if timeSpent > dataloadStats.dataTimeSpent {
		dataloadStats.dataTimeSpent = timeSpent
	}
	dataloadStats.Unlock()
	fmt.Printf("Time to send dataProtos for %d: %d\n", connIndex, end-start)

	if LOAD_INDEX_DATA && (!isIndexGlobal || (splitIndexLoad && !SINGLE_INDEX_SERVER) || connIndex == 0) {
		handleIndexComm(connIndex)
	}
}

func handleIndexComm(connIndex int) {
	fmt.Println("Started index comm for", connIndex)
	conn := conns[connIndex]
	start := time.Now().UnixNano() / 1000000
	ignore(conn)
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
	timeSpent := end - start
	times.sendIndexProtos = timeSpent
	times.totalData = end - times.startTime/1000000
	fmt.Println("Time taken to send index protos: ", end-start)

	dataloadStats.Lock()
	dataloadStats.nSendersDone++
	if timeSpent > dataloadStats.indexTimeSpent {
		dataloadStats.indexTimeSpent = timeSpent
	}
	if dataloadStats.nSendersDone == len(conns) || !splitIndexLoad || SINGLE_INDEX_SERVER {
		writeDataloadStatsFile()
		if !DOES_QUERIES && !DOES_UPDATES {
			for _, conn := range conns {
				conn.Close()
			}
			os.Exit(0)
		}
	}
	dataloadStats.Unlock()

	/*
		if connIndex == 0 {
			runtime.GC()
			debug.FreeOSMemory()
			/*
				if QUERY_BENCH {
					startQueriesBench()
				} else {
					sendQueries(conn)
				}
	*/ /*
		}
	*/
	//sendQueriesNoIndex(conn)
	//printExecutionTimes()
}

//Note: Assumes updatesComm and serverComm aren't running at the same time!
//It's safe to run updates after data ends though.
func handleUpdatesComm(connIndex int) {
	fmt.Println("Started updates comm for", connIndex)
	conn := conns[connIndex]
	//channel := channels.dataChans[connIndex]
	channel := channels.updateChans[connIndex]
	//var txnId []byte
	//var txnProto pb.Message
	complete := false
	localUpdStats := make([]UpdatesStats, 0, 100)
	currUpdStats := UpdatesStats{}
	var startSend, finishReceive int64
	lastType, lastNUpds, lastTime := -1, 0, int64(0)
	ignore(finishReceive)
	//ignore(conn, localUpdStats, currUpdStats, startSend, finishReceive, lastTime, lastType, lastNUpds)

	for !complete {
		msg := <-channel
		if msg.code == QUEUE_COMPLETE {
			complete = true
		} else {
			//TODO: Make waiting time (except for first update of all) also count for whichever type of update is coming next.
			startSend = time.Now().UnixNano()
			antidote.SendProto(msg.code, msg.Message, conn)
			//Storing previous
			if lastType == NEW_TYPE {
				currUpdStats.newDataUpds += lastNUpds
				currUpdStats.newDataTimeSpent += (startSend - lastTime)
			} else if lastType == REMOVE_TYPE {
				currUpdStats.removeDataUpds += lastNUpds
				currUpdStats.removeDataTimeSpent += (startSend - lastTime)
			} else if lastType == INDEX_TYPE {
				currUpdStats.indexUpds += lastNUpds
				currUpdStats.indexTimeSpent += (startSend - lastTime)
			}
			if collectUpdStatsComm[connIndex] {
				currUpdStats.newDataTimeSpent /= 1000000 //convert to ms
				currUpdStats.removeDataTimeSpent /= 1000000
				currUpdStats.indexTimeSpent /= 1000000
				localUpdStats = append(localUpdStats, currUpdStats)
				currUpdStats = UpdatesStats{}
				collectUpdStatsComm[connIndex] = false
			}
			//Registering current
			lastType, lastNUpds = msg.dataType, msg.nData
			antidote.ReceiveProto(conn)
			//finishReceive = time.Now().UnixNano()
			//lastTime = finishReceive - startSend
			lastTime = startSend
		}
	}

	fmt.Println("All update protos have been sent.")

	if lastType == NEW_TYPE {
		currUpdStats.newDataUpds += lastNUpds
		currUpdStats.newDataTimeSpent += (startSend - lastTime)
	} else if lastType == REMOVE_TYPE {
		currUpdStats.removeDataUpds += lastNUpds
		currUpdStats.removeDataTimeSpent += (startSend - lastTime)
	} else {
		currUpdStats.indexUpds += lastNUpds
		currUpdStats.indexTimeSpent += (startSend - lastTime)
	}
	currUpdStats.newDataTimeSpent /= 1000000 //convert to ms
	currUpdStats.removeDataTimeSpent /= 1000000
	currUpdStats.indexTimeSpent /= 1000000
	localUpdStats = append(localUpdStats, currUpdStats)

	updStatsComm[connIndex] = localUpdStats
	nFinished := atomic.AddInt64(&nFinishedUpdComms, 1)
	if nFinished == int64(len(channels.updateChans)) {
		mergeCommUpdateStats()
		writeUpdsStatsFile()
		os.Exit(0)
	}
}

func notifyTestComplete() {
	if strings.Contains(NOTIFY_ADDRESS, ":") {
		conn, err := net.Dial("tcp", NOTIFY_ADDRESS)
		if err != nil {
			fmt.Println("[COMM]Error - couldn't connect to notification server at", NOTIFY_ADDRESS, ".", err)
		} else {
			conn.Write([]byte("TEST_COMPLETE"))
			fmt.Println("[COMM]Notified server of test end.")
		}
	}
}

/*
type DataloadStats struct {
	nDataUpds      int
	nIndexUpds     int
	dataTimeSpent  int64
	indexTimeSpent int64
	nSendersDone   int
	sync.Mutex     //Used for dataTimeSpent, indexTimeSpent and nServersDone, as there's one goroutine per server.
}
*/

func getStatsFileToWrite(filename string) (file *os.File) {
	if statisticsInterval == -1 {
		fmt.Println("Not writing stats file as statisticsInterval is -1.")
		return
	}
	//os.Mkdir(statsSaveLocation, os.ModeDir)
	os.MkdirAll(statsSaveLocation, 0777)
	fileCreated := false
	idLock.Lock()
	defer idLock.Unlock()
	//for i := int64(0); !fileCreated; i++ {
	for !fileCreated {
		_, err := os.Stat(statsSaveLocation + filename + id + ".csv")
		if err != nil {
			fileCreated = true
			file, err = os.Create(statsSaveLocation + filename + id + ".csv")
			if err != nil {
				fmt.Println("[DATASAVE][ERROR]Failed to create stats file with name "+filename+id+".csv. Error:", err)
				return
			}
		} else {
			id = string(id[0]) + id
		}
	}
	return
}

func writeDataloadStatsFile() {
	fmt.Println("Starting to write dataload statistics")
	file := getStatsFileToWrite("dataloadStats")
	if file == nil {
		return
	}

	dataUpdsPerSecond := float64(dataloadStats.nDataUpds) / float64(dataloadStats.dataTimeSpent) * 1000
	indexUpdsPerSecond := float64(dataloadStats.nIndexUpds) / float64(dataloadStats.indexTimeSpent) * 1000

	writer := csv.NewWriter(file)
	writer.Comma = ';'
	defer writer.Flush()
	//Data stats
	writer.Write([]string{strconv.FormatInt(int64(dataloadStats.nDataUpds), 10), strconv.FormatInt(dataloadStats.dataTimeSpent, 10),
		strconv.FormatFloat(dataUpdsPerSecond, 'f', 10, 64)})
	//Index stats
	writer.Write([]string{strconv.FormatInt(int64(dataloadStats.nIndexUpds), 10), strconv.FormatInt(dataloadStats.indexTimeSpent, 10),
		strconv.FormatFloat(indexUpdsPerSecond, 'f', 10, 64)})

	fmt.Println("Dataload statistics saved successfully to " + file.Name())
}
