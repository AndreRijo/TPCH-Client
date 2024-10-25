package client

import (
	"encoding/csv"
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"potionDB/crdt/crdt"
	"potionDB/crdt/proto"
	antidote "potionDB/potionDB/components"
	"runtime"
	"sort"
	"strconv"
	"time"

	//"tpch_client/src/tpch"
	tpch "tpch_data_processor/tpch"
)

//TODO: On queries, all requests can go to the "partial" slot. That should make things easier to manage/maintain
//      protoServer handles fine requests on the "partial" slot that are full reads
//TODO (if there's ever time to): make some struct that handles automatically readParams and updParams slices with bufI.

// Similar to TableInfo (clientIndex.go), but for doing updates an order at a time
type SingleTableInfo struct {
	*tpch.Tables
	rng            *rand.Rand
	id             int   //Just for debbuging purposes, can delete later
	waitFor        []int //NReplies to wait for each server.
	conns          []net.Conn
	currUpdStat    *UpdateStats
	indexServer    int                         //For global case
	reusableParams [][]crdt.UpdateObjectParams //This can be reused as a buffer when sending each update in its own transaction.
	debugStats     *DebugUpdStats
}

type MixClientResult struct {
	QueryClientResult
	updsDone int
	updStats []UpdateStats
	DebugUpdStats
	clientID int
}

type UpdateStats struct {
	nNews, nDels, nIndex, nTxns, nUpdBlocks int
	latency                                 int64 //Used when LATENCY_MODE is PER_BATCH
}

type DebugUpdStats struct {
	newOrders, newItems, deleteOrders, deleteItems int
}

//NOTE: THIS MOST LIKELY WON'T WORK WHEN INDEXES DON'T HAVE THE ALL DATA!
//The reason for this is that for such thing to work, we would need different (ID -> pos) methods
//which would take in consideration the existance of both old and updated data (and, worse, shared among clients)
//This would be specifically problematic when running clients in different machines.
//With the current setup, I don't think even with split update and query clients that it works.

//Also, golang's rand is not adequate for this, as it uses a globalLock to ensure concurrent access is safe.

//Note: some configs and variables are shared with clientQueries.go and clientUpdates.go

//New TODO:
//Check what happens when we run out of updates - rip
//Check if the requirements nClients < updateFiles is still true - it is not, just < nOrders
//Make code to split updates per region - done

var (
	UPD_RATE                             float64 //0 to 1.
	N_UPD_CLIENTS                        int     //if <= 0, then all clients can do updates. Otherwise, N_UPD_CLIENTS can do both updates and queries, and TEST_ROUTINES-N_UPD_CLIENTS can only do queries.
	START_UPD_FILE, FINISH_UPD_FILE      int     //Used by this client only. clientUpdates.go use N_UPDATE_FILES.
	SPLIT_UPDATES, SPLIT_UPDATES_NO_WAIT bool    //Whenever each update should be its own transaction or not.
	RECORD_LATENCY                       bool    //Filled automatically in startMixBench. When true, latency is measured every transaction.
	//MAX_CONNECTIONS                      int     = 64 //After this limit, clients start sharing connections.
	NON_RANDOM_SERVERS bool    //If true, servers are associated to clients by order instead of randomly. id is used to select the first server
	LIKEHOOD_ADD       float64 //Percentage of adds vs removes when doing non-standard TPC-H
	ADD_ONLY           bool    //Filled automatically in startMixBench. Is true when likehood_add == 1
)

const (
	STAT_TOTAL            = 0
	STAT_MIN              = 1
	STAT_MAX              = 2
	STAT_AVG              = 3
	STAT_MEAN             = 4
	ONE_CLIENT_STATISTICS = true
)

func startMixBench() {
	ADD_ONLY = (LIKEHOOD_ADD == 1)
	maxServers := len(servers)
	RECORD_LATENCY = (LATENCY_MODE == PER_BATCH)
	fmt.Println("TPC-H add rate:", LIKEHOOD_ADD)
	//fmt.Println("Is it add-only?", ADD_ONLY)
	fmt.Println("Reading updates...")
	readStart := time.Now().UnixNano()
	//ordersUpds, lineItemUpds, deleteKeys, lineItemSizes, itemSizesPerOrder := readUpdsByOrder()
	ordersUpds, lineItemUpds, deleteKeys, _, itemSizesPerOrder := readUpdsByOrder()
	readFinish := time.Now().UnixNano()
	fmt.Println("Finished reading updates. Time taken for read:", (readFinish-readStart)/1000000, "ms")

	fmt.Println("Splitting updates")
	N_UPDATE_FILES = FINISH_UPD_FILE - START_UPD_FILE + 1 //for splitUpdatesPerRoutine
	//_, tableInfos, routineOrders, routineItems, routineDelete, routineLineSizes :=
	//splitUpdatesPerRoutine(TEST_ROUTINES, ordersUpds, lineItemUpds, deleteKeys, itemSizesPerOrder)
	//tableInfos, routineOrders, routineItems, routineDelete, routineLineSizes := newSplitUpdatesPerRoutine(TEST_ROUTINES, ordersUpds, lineItemUpds, deleteKeys, itemSizesPerOrder)

	tableInfos, routineOrders, routineItems, routineDelete, routineLineSizes, regionsForClients := splitUpdatesPerRoutineAndRegionHelper(ordersUpds, lineItemUpds, deleteKeys, itemSizesPerOrder)
	fmt.Println("Updates split.")

	tableInfos[0].q22CalcHelper() //Prepare info for Q22.
	singleTableInfos, seed := make([]SingleTableInfo, len(tableInfos)), time.Now().UnixNano()
	//lastRoutineLineSize, currRoutineLineSize := 0, 0
	for i, info := range tableInfos {
		singleTableInfos[i] = SingleTableInfo{Tables: info.Tables, rng: rand.New(rand.NewSource(seed + int64(i))), id: i, waitFor: make([]int, len(servers))}
		if N_UPD_CLIENTS == 0 || i < N_UPD_CLIENTS {
			singleTableInfos[i].reusableParams = make([][]crdt.UpdateObjectParams, maxServers)
			for j := range singleTableInfos[i].reusableParams {
				singleTableInfos[i].reusableParams[j] = make([]crdt.UpdateObjectParams, 200)
			}
		}
	}

	selfRng := rand.New(rand.NewSource(seed + int64(2*TEST_ROUTINES)))
	resultChan := make(chan MixClientResult, TEST_ROUTINES)
	results := make([]MixClientResult, TEST_ROUTINES)
	serverPerClient := make([]int, TEST_ROUTINES)
	collectQueryStats = make([]bool, TEST_ROUTINES)
	for i := range results {
		//chans[i] = make(chan MixClientResult)
		collectQueryStats[i] = false
	}
	//Distribute query clients equally or randomly by servers
	if UPD_RATE == 0 {
		clientsDistributionHelper(0, maxServers, selfRng, serverPerClient)
	} else {
		fmt.Println("[CQU]Using order distribution as index distribution: ", regionsForClients)
		if N_UPD_CLIENTS == 0 {
			//Use the same distribution as the updates
			serverPerClient = regionsForClients
		} else {
			for i := 0; i < N_UPD_CLIENTS; i++ {
				serverPerClient[i] = regionsForClients[i]
			}
			clientsDistributionHelper(N_UPD_CLIENTS, maxServers, selfRng, serverPerClient)
		}
	}

	//nConns := tools.MinInt(MAX_CONNECTIONS, TEST_ROUTINES)
	nConns := TEST_ROUTINES
	conns := make([][]net.Conn, nConns)
	fmt.Println("Making", nConns, "connections.")
	startConnTime := time.Now().UnixNano()
	makeClientConns(nConns, conns)
	endConnTime := time.Now().UnixNano()

	//fmt.Printf("Making %d connections (%d, %d)\n", nConns, MAX_CONNECTIONS, TEST_ROUTINES)
	/*for i := 0; i < nConns; i++ {
		go func(i int) {
			list := make([]net.Conn, len(servers))
			for j := 0; j < len(servers); j++ {
				dialer := net.Dialer{KeepAlive: -1}
				list[j], _ = (&dialer).Dial("tcp", servers[j])
			}
			conns[i] = list
		}(i)
	}*/

	fmt.Printf("Times: %+v\n", times)
	fmt.Printf("Took %dms to connect to the servers.\n", (endConnTime-startConnTime)/int64(time.Millisecond))
	fmt.Println("[CQU]Using the following TopK size for Q15:", q15TopSize)
	fmt.Printf("Waiting to start queries & upds... (ops per txn: %d; batch mode: %d; latency mode: %d, isGlobal: %t, isSingle: %t, isDirect: %t, updateSpecificIndex: %t)\n",
		READS_PER_TXN, BATCH_MODE, LATENCY_MODE, isIndexGlobal, !isMulti, LOCAL_DIRECT, UPDATE_SPECIFIC_INDEX_ONLY)
	sleepTime := QUERY_WAIT*1000000 - time.Duration(time.Now().UnixNano()-times.startTime)
	connTime := time.Duration(endConnTime-startConnTime) / time.Second
	if connTime >= 5 {
		sleepTime += (connTime / 5) * 5 //Add a sleeping time that is a multiple of 5s.
	}
	/*if sleepTime < 1*time.Second { //Try to make a "synchronized" wait time with the other clients
		if sleepTime > 0 {
			sleepTime += 10 * time.Second
		} else {
			excessTime := time.Duration(int64(math.Abs(float64(sleepTime))) % (int64(10 * time.Second)))
			sleepTime = 10*time.Second - excessTime
		}
	}*/
	fmt.Println("Sleeping at", time.Now().String(), "for", sleepTime/time.Millisecond, "ms")
	time.Sleep(sleepTime)
	//time.Sleep(QUERY_WAIT*1000000 - time.Duration(time.Now().UnixNano()-times.startTime))
	fmt.Println("Queries & upds started at", time.Now().String())
	fmt.Println("Is using split: ", SPLIT_UPDATES)
	//fmt.Println("Server per client:", serverPerClient)

	if statisticsInterval > 0 {
		fmt.Println("Called mixedInterval at", time.Now().String())
		go doMixedStatsInterval()
	}

	if N_UPD_CLIENTS > 0 {
		for i := 0; i < N_UPD_CLIENTS; i++ {
			go mixBench(0, i, serverPerClient[i], conns[i], resultChan, singleTableInfos[i],
				routineOrders[i], routineItems[i], routineDelete[i], routineLineSizes[i])
		}
		for i := N_UPD_CLIENTS; i < TEST_ROUTINES; i++ {
			go queryOnlyBench(0, i, serverPerClient[i], conns[i], resultChan, singleTableInfos[i])
		}
	} else {
		for i := 0; i < TEST_ROUTINES; i++ {
			//for i := 0; i < TEST_ROUTINES; i++ {
			//fmt.Printf("Value of update stats for client %d: %t, started at: %s\n", i, collectQueryStats[i], time.Now().String())
			go mixBench(0, i, serverPerClient[i], conns[i], resultChan, singleTableInfos[i],
				routineOrders[i], routineItems[i], routineDelete[i], routineLineSizes[i])
		}
	}

	if TEST_ROUTINES > 4000 {
		TEST_DURATION += int64(5000 * TEST_ROUTINES / 4000) //Compensating for delayed start due to too many clients.
	}

	fmt.Println("Sleeping for: ", time.Duration(TEST_DURATION)*time.Millisecond, "at", time.Now().String())
	time.Sleep(time.Duration(TEST_DURATION) * time.Millisecond)
	STOP_QUERIES = true
	stopTimeFull := time.Now()
	stopTime := stopTimeFull.UnixNano()
	fmt.Println()
	fmt.Printf("Test time is over at %s.\n", stopTimeFull.Format("2006-01-02 15:04:05"))

	go func() {
		var result MixClientResult
		for range results {
			result = <-resultChan
			results[result.clientID] = result
		}
		/*for i, channel := range chans {
			results[i] = <-channel
		}*/
		//Notify update channels that there's no further updates
		/*
			for _, channel := range channels.updateChans {
				channel <- QueuedMsgWithStat{QueuedMsg: QueuedMsg{code: QUEUE_COMPLETE}}
			}
		*/
		fmt.Printf("Time (ms) from test end until all clients replied: %d (ms)\n", (time.Now().UnixNano()-stopTime)/1000000)
		fmt.Println("All query/upd clients have finished.")
		totalQueries, totalReads, avgDuration, totalUpds, nFuncs := 0.0, 0.0, 0.0, 0.0, float64(len(queryFuncs))
		totalNewOrders, totalNewItems, totalDeleteOrders, totalDeleteItems := 0, 0, 0, 0
		for i, result := range results {
			if len(results) < 300 { //Do not print each clients' results if too many.
				fmt.Printf("%d[%d]: QueryTxns: %f, Queries: %f, QueryTxns/s: %f, Query/s: %f, Reads: %f, Reads/s: %f, Upds: %d, Upds/s: %f, "+
					"New orders: %d, New items: %d, Deleted orders: %d, Deleted items: %d, New items/order %f, Delete items/order %f\n", i, serverPerClient[i],
					result.nQueries/nFuncs, result.nQueries, (result.nQueries/(result.duration*nFuncs))*1000,
					(result.nQueries/result.duration)*1000, result.nReads, (result.nReads/result.duration)*1000,
					result.updsDone, (float64(result.updsDone)/result.duration)*1000,
					result.newOrders, result.newItems, result.deleteOrders, result.deleteItems, float64(result.newItems)/float64(result.newOrders),
					float64(result.deleteItems)/float64(result.deleteOrders))
			}
			totalQueries += result.nQueries
			totalReads += result.nReads
			avgDuration += result.duration
			totalUpds += float64(result.updsDone)
			totalNewOrders += result.newOrders
			totalNewItems += result.newItems
			totalDeleteOrders += result.deleteOrders
			totalDeleteItems += result.deleteItems
		}
		avgDuration /= float64(len(results))
		fmt.Printf("Totals: QueryTxns: %f, Queries: %f, QueryTxns/s: %f, Query/s: %f, Reads: %f, Reads/s: %f, Upds: %f, Upds/s: %f, "+
			"New orders: %d, New items: %d, Deleted orders: %d, Deleted items: %d, New items/order %f, Delete items/order %f\n", totalQueries/nFuncs,
			totalQueries, (totalQueries/(avgDuration*nFuncs))*1000, totalQueries/avgDuration*1000,
			totalReads, (totalReads/avgDuration)*1000, totalUpds, (totalUpds/avgDuration)*1000,
			totalNewOrders, totalNewItems, totalDeleteOrders, totalDeleteItems, float64(totalNewItems)/float64(totalNewOrders), float64(totalDeleteItems)/float64(totalDeleteOrders))

		fmt.Println("Finished at", time.Now().Format("2006-01-02 15:04:05"))
		writeMixStatsFile(results)

		os.Exit(0)
	}()

	time.Sleep(10 * time.Second)
	for i, result := range results {
		if result.nQueries == 0 && result.updsDone == 0 {
			fmt.Printf("Client %d has not yet finished! Waiting for: %v. If all are zeros, then it's waiting for a query on server %d.\n", i, singleTableInfos[i].waitFor, serverPerClient[i])
		}
	}

}

func clientsDistributionHelper(startPos, maxServers int, selfRng *rand.Rand, serverPerClient []int) {
	if NON_RANDOM_SERVERS {
		j, _ := strconv.Atoi(id)
		j = j % maxServers
		for i := startPos; i < TEST_ROUTINES; i++ {
			serverPerClient[i] = j % maxServers
			j++
		}
	} else {
		for i := startPos; i < TEST_ROUTINES; i++ {
			serverPerClient[i] = selfRng.Intn(maxServers)
		}
	}
}

func makeClientConns(nConns int, conns [][]net.Conn) {
	nRoutines := runtime.NumCPU() * 5
	var doneChan chan bool
	if nConns <= nRoutines*2 {
		doneChan = make(chan bool, nConns)
		for i := 0; i < nConns; i++ {
			go func(i int) {
				var err error
				nAttempts := 0
				list := make([]net.Conn, len(servers))
				for j := 0; j < len(servers); j++ {
					dialer := net.Dialer{KeepAlive: -1}
					list[j], err = (&dialer).Dial("tcp", servers[j])
					if err != nil {
						fmt.Printf("[CQU][MakeClientConns]Error connecting to PotionDB, trying again. Error: %s\n", err.Error())
						j--
						nAttempts++
						if nAttempts > 3 {
							fmt.Printf("[CQU][MakeClientConns]Too many connection attempts failed. Exiting.\n")
							break
						}
					}
				}
				conns[i] = list
				doneChan <- true
			}(i)
		}
	} else {
		doneChan = make(chan bool, nRoutines)
		factor, remaining := nConns/nRoutines, nConns%nRoutines
		connFunc := func(i, fac int) {
			for k := 0; k < fac; k++ {
				list := make([]net.Conn, len(servers))
				for j := 0; j < len(servers); j++ {
					dialer := net.Dialer{KeepAlive: -1}
					list[j], _ = (&dialer).Dial("tcp", servers[j])
				}
				conns[i+k] = list
			}
			doneChan <- true
		}
		j := 0
		for i := 0; i < nRoutines; i++ {
			if i < remaining {
				go connFunc(j, factor+1)
				j++
			} else {
				go connFunc(j, factor)
			}
			j += factor
		}
		/*
			for i := 0; i < nConns; i += factor {
				if i < remaining {
					connFunc(i, factor+1)
					i++
				} else {
					connFunc(i, factor)
				}
			}
		*/
	}
	for i := 0; i < cap(doneChan); i++ {
		<-doneChan
	}
}

func readUpdsByOrder() (ordersUpds [][]string, lineItemUpds [][]string, deleteKeys []string, lineItemSizes []int, itemSizesPerOrder []int) {
	updPartsRead := [][]int8{tpchData.ToRead[tpch.ORDERS], tpchData.ToRead[tpch.LINEITEM]}
	return tpch.ReadUpdatesPerOrder(updCompleteFilename[:], tpch.UpdEntries[:], tpch.UpdParts[:], updPartsRead, START_UPD_FILE, FINISH_UPD_FILE)
}

func updateMixStats(queryStats []QueryStats, updStats []UpdateStats, nReads, lastStatReads, nQueries, lastStatQueries int,
	client *SingleTableInfo, lastStatTime, qTime, updTime int64, nTxnsQ, nTxnsU int) (newQStats []QueryStats, newUStats []UpdateStats,
	newNReads, newNQueries int, newLastStatTime int64) {
	currStatTime, currQStats, currUStats := time.Now().UnixNano()/1000000, QueryStats{}, *client.currUpdStat
	diffT, diffR, diffQ := currStatTime-lastStatTime, nReads-lastStatReads, nQueries-lastStatQueries
	if diffT < int64(statisticsInterval/8) && len(queryStats) > 0 {
		//Replace
		lastStatI := len(queryStats) - 1
		queryStats[lastStatI].nReads += diffR
		queryStats[lastStatI].nQueries += diffQ
		queryStats[lastStatI].timeSpent += diffT
		queryStats[lastStatI].latency += (qTime / 1000000)
		queryStats[lastStatI].nTxns += nTxnsQ
		updStats[lastStatI].nIndex += currUStats.nIndex
		updStats[lastStatI].nNews += currUStats.nNews
		updStats[lastStatI].nDels += currUStats.nDels
		updStats[lastStatI].latency += (updTime / 1000000)
		updStats[lastStatI].nTxns += nTxnsU
		updStats[lastStatI].nUpdBlocks += currUStats.nUpdBlocks
	} else if diffT < int64(statisticsInterval/4) && len(queryStats) > 0 {
		//Ignore
	} else {
		currQStats.nReads, currQStats.nQueries, currQStats.timeSpent, currQStats.latency, currQStats.nTxns = diffR, diffQ, diffT, qTime/1000000, nTxnsQ
		queryStats = append(queryStats, currQStats)
		currUStats.latency, currUStats.nTxns = updTime/1000000, nTxnsU
		updStats = append(updStats, currUStats)
	}
	client.currUpdStat = &UpdateStats{}
	return queryStats, updStats, nReads, nQueries, currStatTime
}

func getCorrectDiffTime(statDiff, perBatchTime int64) int64 {
	if RECORD_LATENCY {
		return perBatchTime
	}
	return statDiff
}

func mixBench(seed int64, clientN int, defaultServer int, conns []net.Conn, resultChan chan MixClientResult, tableInfo SingleTableInfo, orders, items [][]string,
	delete []string, lineSizes []int) {

	//fmt.Println("Mix bench started at", time.Now().String(), "for server", defaultServer)
	//fmt.Println("[DELETE]Client conns @ mixBench:", conns)
	isLocalDirect := localMode == LOCAL_DIRECT && !isIndexGlobal
	tableInfo.indexServer = defaultServer
	if SINGLE_INDEX_SERVER || !splitIndexLoad {
		tableInfo.indexServer = 0
		defaultServer = 0
	}

	//Counters and common preparation
	random := 0.0
	qDone, updsDone, reads := 0, 0, 0
	waitForUpds := 0 //number of times updProb must be met before doing the next update batch

	tableInfo.conns = conns
	tableInfo.currUpdStat = &UpdateStats{}
	tableInfo.debugStats = &DebugUpdStats{}

	//Update preparation
	orderI, lineI, lineF := 0, 0, lineSizes[0]

	//Query preparation
	client := QueryClient{serverConns: conns, indexServer: defaultServer, rng: tableInfo.rng, currQ20: &Q20QueryArgs{}, nEntries: new(int)}
	funcs := make([]func(QueryClient) int, len(queryFuncs))
	for i, fun := range queryFuncs {
		funcs[i] = fun
	}
	queryStats := make([]QueryStats, 0, TEST_DURATION/int64(statisticsInterval)+1)
	updStats := make([]UpdateStats, 0, TEST_DURATION/int64(statisticsInterval)+1)
	nQueries := len(funcs)
	lastStatQDone, lastStatReads := 0, 0
	startTime := time.Now().UnixNano() / 1000000
	lastStatTime := startTime
	startTxnTime, currUpdSpentTime, currQuerySpentTime := int64(0), int64(0), int64(0)
	readTxns, updTxns := 0, 0
	rngRegionsI := 0

	//fmt.Println("[CQU]NQueries:", nQueries)

	if READS_PER_TXN > 1 {
		queryPos, nOpsTxn, previousQueryPos, replyPos, nUpdsDone := 0, 0, 0, 0, 0
		var fullRBuf, partialRBuf []crdt.ReadObjectParams
		if isIndexGlobal {
			fullRBuf, partialRBuf = make([]crdt.ReadObjectParams, READS_PER_TXN+1),
				make([]crdt.ReadObjectParams, READS_PER_TXN+1) //+1 as one of the queries has 2 reads.
		} else if localMode == LOCAL_SERVER {
			fullRBuf, partialRBuf = make([]crdt.ReadObjectParams, (READS_PER_TXN+1)*len(conns)),
				make([]crdt.ReadObjectParams, (READS_PER_TXN+1)*len(conns))
		}
		//For LOCAL_DIRECT
		fullRBufS, partialRBufS := make([][]crdt.ReadObjectParams, len(conns)), make([][]crdt.ReadObjectParams, len(conns))
		copyFullS, copyPartialS := make([][]crdt.ReadObjectParams, len(conns)), make([][]crdt.ReadObjectParams, len(conns))

		updBuf := make([][]crdt.UpdateObjectParams, len(conns))
		updBufI, readBufI, localReadBufI, rngRegions := make([]int, len(conns)), make([]int, 2), make([][]int, len(conns)), make([]int, READS_PER_TXN)
		for i := range updBuf {
			updBuf[i] = make([]crdt.UpdateObjectParams, int(math.Max(200.0, float64(READS_PER_TXN*2)))) //200 is way more than enough for a set of updates.
			fmt.Printf("Size of updBuf for server %d: %d\n", i, len(updBuf[i]))
			fullRBufS[i], partialRBufS[i] = make([]crdt.ReadObjectParams, READS_PER_TXN+1), make([]crdt.ReadObjectParams, READS_PER_TXN+1)
			localReadBufI[i] = make([]int, 2)
		}
		for !STOP_QUERIES {
			if collectQueryStats[clientN] {
				/*fmt.Printf("[CQU][%d]New orders %d, new items %d, delete orders %d, delete items %d, new items/order %f, delete items/order %f\n",
				clientN, tableInfo.debugStats.newOrders, tableInfo.debugStats.newItems, tableInfo.debugStats.deleteOrders, tableInfo.debugStats.deleteItems,
				float64(tableInfo.debugStats.newItems)/float64(tableInfo.debugStats.newOrders), float64(tableInfo.debugStats.deleteItems)/float64(tableInfo.debugStats.deleteOrders))*/
				queryStats, updStats, lastStatReads, lastStatQDone, lastStatTime = updateMixStats(queryStats, updStats, reads,
					lastStatReads, qDone, lastStatQDone, &tableInfo, lastStatTime, currQuerySpentTime, currUpdSpentTime, readTxns, updTxns)
				currUpdSpentTime, currQuerySpentTime, readTxns, updTxns = 0, 0, 0, 0
				collectQueryStats[clientN] = false
			}
			random = tableInfo.rng.Float64()
			if random < UPD_RATE {
				if waitForUpds >= READS_PER_TXN {
					waitForUpds -= READS_PER_TXN
				} else {
					//*fmt.Println("[MIXBENCH]Preparing updates. ReadsPerTxn > 1. WaitForUpds, ReadsPerTxn:", waitForUpds, READS_PER_TXN)
					startTxnTime = recordStartLatency()
					for waitForUpds < READS_PER_TXN {
						//fmt.Printf("[CQU][%d]LineSizes for new order %s: %d\n", clientN, orders[orderI][O_ORDERKEY], lineSizes[orderI])
						//fmt.Println("[CQU]Start getSingleDataChange.")
						nUpdsDone = tableInfo.getSingleDataChange(updBuf, updBufI, orders[orderI], items[lineI:lineF], delete[orderI])
						//fmt.Println("[CQU]Finish getSingleDataChange. NUpdsDone:", nUpdsDone)
						waitForUpds += nUpdsDone
						orderI++
						if orderI == len(orders) {
							orderI, lineI, lineF = 0, 0, lineSizes[0]
						} else {
							lineI = lineF
							lineF += lineSizes[orderI]
						}
					}
					//*fmt.Println("[MIXBENCH]updBufI when calling sendReceiveMultipleUpdates")
					tableInfo.sendReceiveMultipleUpdates(updBuf, updBufI)
					for i := range updBufI {
						updBufI[i] = 0
					}
					currUTime := recordFinishLatency(startTxnTime)
					if currUTime/int64(time.Second) >= 1 {
						fmt.Printf("[CQU%d]Time spent on curr update: %d\n", clientN, currUTime)
					}
					currUpdSpentTime += currUTime
					updTxns++
					updsDone += waitForUpds
				}
			} else {
				startTxnTime = recordStartLatency()
				if BATCH_MODE == CYCLE {
					for nOpsTxn < READS_PER_TXN {
						if !isLocalDirect {
							nOpsTxn = getReadsFuncs[queryPos%nQueries](client, fullRBuf, partialRBuf, readBufI, nOpsTxn)
						} else {
							nOpsTxn, rngRegions[rngRegionsI] = getReadsLocalDirectFuncs[queryPos%nQueries](client, fullRBufS,
								partialRBufS, localReadBufI, rngRegions, rngRegionsI, nOpsTxn)
							rngRegionsI++
						}

						qDone++
						queryPos++
					}
				} else {
					queryPos = (queryPos + 1) % nQueries
					for nOpsTxn < READS_PER_TXN {
						if !isLocalDirect {
							nOpsTxn = getReadsFuncs[queryPos](client, fullRBuf, partialRBuf, readBufI, nOpsTxn)
						} else {
							nOpsTxn, rngRegions[rngRegionsI] = getReadsLocalDirectFuncs[queryPos](client, fullRBufS,
								partialRBufS, localReadBufI, rngRegions, rngRegionsI, nOpsTxn)
							rngRegionsI++
						}
						qDone++
					}
					//fmt.Printf("[CQU]Next query pos: %d. Last query pos: %d. Query done: %d. nQueries: %d.\n", queryPos+1, queryPos, qDone, nQueries)
					previousQueryPos = queryPos
				}

				if !isLocalDirect {
					//fmt.Printf("[ClientQueryUpd]Sizes full/part: %d %d\n", readBufI[0], readBufI[1])
					//Expected sizes for local_redirect
					//fmt.Printf("[ClientQueryUpd]Expected sizes: 8 15 (Q3: 0 5; Q5: 1 0; Q11: 2 0; Q14: 0 5; Q15: 0 5; Q18: 5 0)\n")
					readReplies := sendReceiveReadProto(client, fullRBuf[:readBufI[0]], partialRBuf[:readBufI[1]], client.indexServer).GetObjects().GetObjects()
					//sendReceiveReadProtoNoProcess(client, fullRBuf[:readBufI[0]], partialRBuf[:readBufI[1]], client.indexServer)
					readBufI[1] = readBufI[0] //partial reads start when full reads end
					readBufI[0] = 0
					//fmt.Printf("[CQU]Reply sizes: len is %d; readBufI[0] is %d; readBufI[1] is %d\n", len(readReplies), readBufI[0], readBufI[1])
					if BATCH_MODE == CYCLE {
						for replyPos < len(readReplies) {
							replyPos = processReadReplies[previousQueryPos%nQueries](client, readReplies, readBufI, replyPos)
							previousQueryPos++
						}
					} else {
						for replyPos < len(readReplies) {
							replyPos = processReadReplies[previousQueryPos](client, readReplies, readBufI, replyPos)
						}
					}
				} else {
					rngRegionsI = 0
					for i, size := range localReadBufI {
						copyFullS[i], copyPartialS[i] = fullRBufS[i][:size[0]], partialRBufS[i][:size[1]]
					}
					readReplies := sendReceiveMultipleReadProtos(client, copyFullS, copyPartialS)
					convReadReplies := make([][]*proto.ApbReadObjectResp, len(readReplies))
					for i, readReply := range readReplies {
						convReadReplies[i] = readReply.GetObjects().GetObjects()
						localReadBufI[i][1] = localReadBufI[i][0]
						localReadBufI[i][0] = 0
					}
					if BATCH_MODE == CYCLE {
						for replyPos < len(readReplies) {
							replyPos = processLocalDirectReadReplies[previousQueryPos%nQueries](client, convReadReplies, localReadBufI, rngRegions, rngRegionsI, replyPos)
							previousQueryPos++
							rngRegionsI++
						}
					} else {
						for replyPos < len(readReplies) {
							replyPos = processLocalDirectReadReplies[previousQueryPos](client, convReadReplies, localReadBufI, rngRegions, rngRegionsI, replyPos)
							rngRegionsI++
						}
					}
					for _, bufI := range localReadBufI {
						bufI[0], bufI[1] = 0, 0
					}
					rngRegionsI = 0
				}
				reads += nOpsTxn
				readTxns++
				//fmt.Printf("[CQU]Next query pos: %d. Last query pos: %d\n", queryPos, previousQueryPos)
				previousQueryPos, nOpsTxn, replyPos, readBufI[0], readBufI[1] = queryPos, 0, 0, 0, 0

				currQTime := recordFinishLatency(startTxnTime)
				if currQTime/int64(time.Second) >= 1 {
					fmt.Printf("[CQU%d]Time spent on curr query: %d\n", clientN, currQTime)
				}
				currQuerySpentTime += currQTime

				//fmt.Println("[CQU]Aborted after first read")
			}
			/*if readTxns == 2 {
				STOP_QUERIES = true
			}*/
		}
	} else {
		for !STOP_QUERIES {
			if collectQueryStats[clientN] {
				queryStats, updStats, lastStatReads, lastStatQDone, lastStatTime = updateMixStats(queryStats, updStats, reads,
					lastStatReads, qDone, lastStatQDone, &tableInfo, lastStatTime, currQuerySpentTime, currUpdSpentTime, readTxns, updTxns)
				currQuerySpentTime, currUpdSpentTime, readTxns, updTxns = 0, 0, 0, 0
				collectQueryStats[clientN] = false
			}
			random = tableInfo.rng.Float64()
			if random < UPD_RATE {
				//UPDATE
				if waitForUpds > 0 {
					waitForUpds--
				} else {
					startTxnTime = recordStartLatency()
					if SPLIT_UPDATES {
						waitForUpds += tableInfo.sendDataIndividually(orders[orderI], items[lineI:lineF], delete[orderI])
						updTxns += waitForUpds
					} else {
						waitForUpds += tableInfo.sendSingleDataChange(orders[orderI], items[lineI:lineF], delete[orderI])
						updTxns++
					}
					updsDone += waitForUpds
					orderI++
					if orderI == len(orders) {
						orderI, lineI, lineF = 0, 0, lineSizes[0]
					} else {
						lineI = lineF
						lineF += lineSizes[orderI]
					}
					currUTime := recordFinishLatency(startTxnTime)
					if currUTime/int64(time.Second) >= 1 {
						fmt.Printf("[CQU%d]Time spent on curr update: %d\n", clientN, currUTime)
					}
					currUpdSpentTime += currUTime
				}
			} else {
				//READ
				startTxnTime = recordStartLatency()
				reads += funcs[qDone%nQueries](client)
				qDone++
				currQTime := recordFinishLatency(startTxnTime)
				if currQTime/int64(time.Second) >= 1 {
					fmt.Printf("[CQU%d]Time spent on curr query: %d\n", clientN, currQTime)
				}
				currQuerySpentTime += currQTime
				readTxns++
			}
		}
	}

	endTime := time.Now().UnixNano() / 1000000
	queryStats, updStats, lastStatReads, lastStatQDone, lastStatTime = updateMixStats(queryStats, updStats, reads,
		lastStatReads, qDone, lastStatQDone, &tableInfo, lastStatTime, currQuerySpentTime, currUpdSpentTime, readTxns, updTxns)

	/*if clientN == 0 {
		regions := []string{"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"}
		years := []string{"1993", "1994", "1995", "1996", "1997"}
		idInt, _ := strconv.Atoi(id)
		key := "q5nr" + regions[idInt]
		readP := make([]crdt.ReadObjectParams, len(years))
		for i, year := range years {
			readP[i] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: key + year, Bucket: buckets[INDEX_BKT], CrdtType: proto.CRDTType_RRMAP}}
		}
		antidote.SendProto(antidote.StaticRead, antidote.CreateStaticRead(nil, readP, nil), conns[0])
		_, respProto, _ := antidote.ReceiveProto(conns[0])
		objs := respProto.(*proto.ApbStaticReadObjectsResp).GetObjects().GetObjects()
		for i, obj := range objs {
			mapState := crdt.ReadRespProtoToAntidoteState(obj, proto.CRDTType_RRMAP, proto.READType_FULL).(crdt.EmbMapEntryState)
			fmt.Println("Q5: Values for", key+years[i])
			for nation, valueState := range mapState.States {
				fmt.Printf("%s: %d\n", nation, valueState.(crdt.CounterState).Value)
			}
		}
	}*/

	for _, conn := range conns {
		conn.Close()
	}

	resultChan <- MixClientResult{clientID: clientN, QueryClientResult: QueryClientResult{duration: float64(endTime - startTime), nQueries: float64(qDone),
		nReads: float64(reads), intermediateResults: queryStats, nEntries: float64(*client.nEntries)}, updsDone: updsDone, updStats: updStats, DebugUpdStats: *tableInfo.debugStats}
}

func queryOnlyBench(seed int64, clientN int, defaultServer int, conns []net.Conn, resultChan chan MixClientResult, tableInfo SingleTableInfo) {
	isLocalDirect := localMode == LOCAL_DIRECT && !isIndexGlobal
	tableInfo.indexServer = defaultServer
	if SINGLE_INDEX_SERVER || !splitIndexLoad {
		tableInfo.indexServer = 0
		defaultServer = 0
	}

	//Counters and common preparation
	qDone, reads := 0, 0
	tableInfo.conns, tableInfo.currUpdStat, tableInfo.debugStats = conns, &UpdateStats{}, &DebugUpdStats{}

	//Query preparation
	client := QueryClient{serverConns: conns, indexServer: defaultServer, rng: tableInfo.rng, currQ20: &Q20QueryArgs{}, nEntries: new(int)}
	funcs := make([]func(QueryClient) int, len(queryFuncs))
	for i, fun := range queryFuncs {
		funcs[i] = fun
	}
	queryStats := make([]QueryStats, 0, TEST_DURATION/int64(statisticsInterval)+1)
	updStats := make([]UpdateStats, 0, TEST_DURATION/int64(statisticsInterval)+1)
	nQueries := len(funcs)
	lastStatQDone, lastStatReads := 0, 0
	startTime := time.Now().UnixNano() / 1000000
	lastStatTime := startTime
	startTxnTime, currQuerySpentTime := int64(0), int64(0)
	readTxns, rngRegionsI := 0, 0

	if READS_PER_TXN > 1 {
		queryPos, nOpsTxn, previousQueryPos, replyPos := 0, 0, 0, 0
		var fullRBuf, partialRBuf []crdt.ReadObjectParams
		if isIndexGlobal {
			fullRBuf, partialRBuf = make([]crdt.ReadObjectParams, READS_PER_TXN+1),
				make([]crdt.ReadObjectParams, READS_PER_TXN+1) //+1 as one of the queries has 2 reads.
		} else if localMode == LOCAL_SERVER {
			fullRBuf, partialRBuf = make([]crdt.ReadObjectParams, (READS_PER_TXN+1)*len(conns)),
				make([]crdt.ReadObjectParams, (READS_PER_TXN+1)*len(conns))
		}
		//For LOCAL_DIRECT
		fullRBufS, partialRBufS := make([][]crdt.ReadObjectParams, len(conns)), make([][]crdt.ReadObjectParams, len(conns))
		copyFullS, copyPartialS := make([][]crdt.ReadObjectParams, len(conns)), make([][]crdt.ReadObjectParams, len(conns))

		readBufI, localReadBufI, rngRegions := make([]int, 2), make([][]int, len(conns)), make([]int, READS_PER_TXN)
		for i := range fullRBufS {
			fullRBufS[i], partialRBufS[i] = make([]crdt.ReadObjectParams, READS_PER_TXN+1), make([]crdt.ReadObjectParams, READS_PER_TXN+1)
			localReadBufI[i] = make([]int, 2)
		}
		for !STOP_QUERIES {
			if collectQueryStats[clientN] {
				queryStats, updStats, lastStatReads, lastStatQDone, lastStatTime = updateMixStats(queryStats, updStats, reads,
					lastStatReads, qDone, lastStatQDone, &tableInfo, lastStatTime, currQuerySpentTime, 0, readTxns, 0)
				currQuerySpentTime, readTxns, collectQueryStats[clientN] = 0, 0, false
			}
			startTxnTime = recordStartLatency()
			if BATCH_MODE == CYCLE {
				for nOpsTxn < READS_PER_TXN {
					if !isLocalDirect {
						nOpsTxn = getReadsFuncs[queryPos%nQueries](client, fullRBuf, partialRBuf, readBufI, nOpsTxn)
					} else {
						nOpsTxn, rngRegions[rngRegionsI] = getReadsLocalDirectFuncs[queryPos%nQueries](client, fullRBufS,
							partialRBufS, localReadBufI, rngRegions, rngRegionsI, nOpsTxn)
						rngRegionsI++
					}
					qDone++
					queryPos++
				}
			} else {
				queryPos = (queryPos + 1) % nQueries
				for nOpsTxn < READS_PER_TXN {
					if !isLocalDirect {
						nOpsTxn = getReadsFuncs[queryPos](client, fullRBuf, partialRBuf, readBufI, nOpsTxn)
					} else {
						nOpsTxn, rngRegions[rngRegionsI] = getReadsLocalDirectFuncs[queryPos](client, fullRBufS,
							partialRBufS, localReadBufI, rngRegions, rngRegionsI, nOpsTxn)
						rngRegionsI++
					}
					qDone++
				}
				//fmt.Printf("[CQU]Next query pos: %d. Last query pos: %d. Query done: %d. nQueries: %d.\n", queryPos+1, queryPos, qDone, nQueries)
				previousQueryPos = queryPos
			}

			if !isLocalDirect {
				readReplies := sendReceiveReadProto(client, fullRBuf[:readBufI[0]], partialRBuf[:readBufI[1]], client.indexServer).GetObjects().GetObjects()
				readBufI[1] = readBufI[0] //partial reads start when full reads end
				readBufI[0] = 0
				if BATCH_MODE == CYCLE {
					for replyPos < len(readReplies) {
						replyPos = processReadReplies[previousQueryPos%nQueries](client, readReplies, readBufI, replyPos)
						previousQueryPos++
					}
				} else {
					for replyPos < len(readReplies) {
						replyPos = processReadReplies[previousQueryPos](client, readReplies, readBufI, replyPos)
					}
				}
			} else {
				rngRegionsI = 0
				for i, size := range localReadBufI {
					copyFullS[i], copyPartialS[i] = fullRBufS[i][:size[0]], partialRBufS[i][:size[1]]
				}
				readReplies := sendReceiveMultipleReadProtos(client, copyFullS, copyPartialS)
				convReadReplies := make([][]*proto.ApbReadObjectResp, len(readReplies))
				for i, readReply := range readReplies {
					convReadReplies[i] = readReply.GetObjects().GetObjects()
					localReadBufI[i][1] = localReadBufI[i][0]
					localReadBufI[i][0] = 0
				}
				if BATCH_MODE == CYCLE {
					for replyPos < len(readReplies) {
						replyPos = processLocalDirectReadReplies[previousQueryPos%nQueries](client, convReadReplies, localReadBufI, rngRegions, rngRegionsI, replyPos)
						previousQueryPos++
						rngRegionsI++
					}
				} else {
					for replyPos < len(readReplies) {
						replyPos = processLocalDirectReadReplies[previousQueryPos](client, convReadReplies, localReadBufI, rngRegions, rngRegionsI, replyPos)
						rngRegionsI++
					}
				}
				for _, bufI := range localReadBufI {
					bufI[0], bufI[1] = 0, 0
				}
				rngRegionsI = 0
			}
			reads += nOpsTxn
			readTxns++
			previousQueryPos, nOpsTxn, replyPos, readBufI[0], readBufI[1] = queryPos, 0, 0, 0, 0

			currQTime := recordFinishLatency(startTxnTime)
			if currQTime/int64(time.Second) >= 1 {
				fmt.Printf("[CQU%d]Time spent on curr query: %d\n", clientN, currQTime)
			}
			currQuerySpentTime += currQTime
		}
	} else {
		for !STOP_QUERIES {
			if collectQueryStats[clientN] {
				queryStats, updStats, lastStatReads, lastStatQDone, lastStatTime = updateMixStats(queryStats, updStats, reads,
					lastStatReads, qDone, lastStatQDone, &tableInfo, lastStatTime, currQuerySpentTime, 0, readTxns, 0)
				currQuerySpentTime, readTxns, collectQueryStats[clientN] = 0, 0, false
			}
			//READ
			startTxnTime = recordStartLatency()
			reads += funcs[qDone%nQueries](client)
			qDone++
			currQTime := recordFinishLatency(startTxnTime)
			if currQTime/int64(time.Second) >= 1 {
				fmt.Printf("[CQU%d]Time spent on curr query: %d\n", clientN, currQTime)
			}
			currQuerySpentTime += currQTime
			readTxns++
		}
	}

	endTime := time.Now().UnixNano() / 1000000
	queryStats, updStats, lastStatReads, lastStatQDone, lastStatTime = updateMixStats(queryStats, updStats, reads,
		lastStatReads, qDone, lastStatQDone, &tableInfo, lastStatTime, currQuerySpentTime, 0, readTxns, 0)

	for _, conn := range conns {
		conn.Close()
	}

	resultChan <- MixClientResult{clientID: clientN, QueryClientResult: QueryClientResult{duration: float64(endTime - startTime), nQueries: float64(qDone),
		nReads: float64(reads), intermediateResults: queryStats, nEntries: float64(*client.nEntries)}, updsDone: 0, updStats: updStats, DebugUpdStats: *tableInfo.debugStats}
}

func recordStartLatency() int64 {
	if RECORD_LATENCY {
		return time.Now().UnixNano()
	}
	return 0
}

func recordFinishLatency(startTime int64) int64 {
	if RECORD_LATENCY {
		return time.Now().UnixNano() - startTime
	}
	return 0
}

func (ti SingleTableInfo) sendReceiveMultipleUpdates(updBuf [][]crdt.UpdateObjectParams, bufI []int) {
	//*fmt.Println("Upds to send:", updBuf)
	//*fmt.Println("Upds to send:")
	/*
		for i, upds := range updBuf {
			fmt.Println(upds[:bufI[i]], "(", bufI[i], ")")
		}
	*/
	for i, upds := range updBuf {
		if bufI[i] > 0 {
			antidote.SendProto(antidote.StaticUpdateObjs, antidote.CreateStaticUpdateObjs(nil, upds[:bufI[i]]), ti.conns[i])
		}
	}
	for i, nUpds := range bufI {
		if nUpds > 0 {
			antidote.ReceiveProto(ti.conns[i])
		}
	}
}

func (ti SingleTableInfo) sendDataIndividually(order []string, lineItems [][]string, deleteKey string) (updsDone int) {
	updParams := ti.reusableParams
	bufI := make([]int, len(ti.reusableParams))
	nAdds, nDels := 0, 0

	deletedOrder, deletedItems := ti.getNextDelete(deleteKey)

	if UPDATE_BASE_DATA {
		dels, itemsDelPerServer, itemsIndex := ti.getSingleDelete(deletedOrder, deletedItems)
		ti.makeSingleDelete(deletedOrder, updParams, bufI, itemsDelPerServer, itemsIndex)
		ti.sendIndividualUpdates(updParams, bufI)

		adds, itemsNewPerServer := ti.getSingleUpd(order, lineItems)
		ti.makeSingleUpd(order, lineItems, updParams, bufI, itemsNewPerServer)
		ti.sendIndividualUpdates(updParams, bufI)
		nAdds, nDels = adds, dels
	}

	newOrder, newItems := ti.CreateOrder(order), ti.CreateLineitemsOfOrder(lineItems)
	nIndexUpds, nBlockIndexUpds /*, indexInfo*/ := 0, 0 /*, IndexInfo{}*/

	if UPDATE_INDEX {
		indexInfos := makeIndexInfos(ti.Tables, newOrder, deletedOrder, newItems, deletedItems)
		if len(indexInfos) > 0 {
			updS := updParams[0] //Doesn't matter which buffer is used
			if isIndexGlobal {
				for _, indexInfo := range indexInfos {
					ti.sendIndividualIndex(updS, indexInfo.makeNewUpdArgs(ti.Tables, newOrder, updS, 0, INDEX_BKT), ti.indexServer)
					ti.sendIndividualIndex(updS, indexInfo.makeRemUpdArgs(ti.Tables, deletedOrder, updS, 0, INDEX_BKT), ti.indexServer)
				}
			} else {
				regN, regR := ti.OrderToRegionkey(newOrder), ti.OrderToRegionkey(deletedOrder)
				newI, remI := int(regN)+INDEX_BKT, int(regR)+INDEX_BKT
				var newS, remS int
				if localMode == LOCAL_DIRECT {
					newS, remS = int(regN), int(regR)
				} else {
					newS, remS = ti.indexServer, ti.indexServer
				}
				//Note: if regN == regR, so is newI == remI, thus everything is sent to the same server and bucket (as it is desired)
				for _, indexInfo := range indexInfos {
					ti.sendIndividualIndex(updS, indexInfo.makeNewUpdArgs(ti.Tables, newOrder, updS, 0, newI), newS)
					ti.sendIndividualIndex(updS, indexInfo.makeRemUpdArgs(ti.Tables, deletedOrder, updS, 0, remI), remS)
				}
			}
			nIndexUpds, _, nBlockIndexUpds = countUpdsIndexInfo(indexInfos)
		}
	} /*else if UPDATE_INDEX {
		ti.sendSpecificIndexIndividual(deleteKey, newOrder, newItems, deletedOrder, deletedItems)
	}*/

	if SPLIT_UPDATES_NO_WAIT {
		for i, nWait := range ti.waitFor {
			if nWait > 0 {
				for j := 0; j < nWait; j++ {
					antidote.ReceiveProto(ti.conns[i])
				}
			}
			ti.waitFor[i] = 0
		}
	}
	ti.currUpdStat.nDels += nDels
	ti.currUpdStat.nNews += nAdds
	ti.currUpdStat.nIndex += nIndexUpds
	ti.currUpdStat.nUpdBlocks += nDels + nAdds + nBlockIndexUpds

	return nDels + nAdds + nIndexUpds
}

func (ti SingleTableInfo) sendIndividualUpdates(upds [][]crdt.UpdateObjectParams, bufI []int) {
	for i, serverUpds := range upds {
		for j := 0; j < bufI[i]; j++ {
			antidote.SendProto(antidote.StaticUpdateObjs, antidote.CreateStaticUpdateObjs(nil, []crdt.UpdateObjectParams{serverUpds[j]}), ti.conns[i])
			if !SPLIT_UPDATES_NO_WAIT {
				antidote.ReceiveProto(ti.conns[i])
			}
		}
		ti.waitFor[i] += bufI[i]
		bufI[i] = 0
	}
}

func (ti SingleTableInfo) sendIndividualIndex(upds []crdt.UpdateObjectParams, buf int, server int) {
	for j := 0; j < buf; j++ {
		antidote.SendProto(antidote.StaticUpdateObjs, antidote.CreateStaticUpdateObjs(nil, []crdt.UpdateObjectParams{upds[j]}), ti.conns[server])
		if !SPLIT_UPDATES_NO_WAIT {
			antidote.ReceiveProto(ti.conns[server])
		}
	}
	ti.waitFor[server] += buf
}

// This one only does either add or delete, but not both: it picks which one to do depending on odds
func (ti SingleTableInfo) getSingleOddsDataChange(updParams [][]crdt.UpdateObjectParams, bufI []int, order []string, lineItems [][]string, deleteKey string) (updsDone int) {
	if ADD_ONLY || ti.rng.Float64() <= LIKEHOOD_ADD { //Add
		return ti.addAndIndexOnly(updParams, bufI, order, lineItems)
	}
	return ti.deleteAndIndexOnly(updParams, bufI, deleteKey)
}

func (ti SingleTableInfo) addAndIndexOnly(updParams [][]crdt.UpdateObjectParams, bufI []int, order []string, lineItems [][]string) (updsDone int) {
	newOrder, newItems := ti.CreateOrder(order), ti.CreateLineitemsOfOrder(lineItems)
	nAdds, itemsNewPerServer := ti.getSingleUpd(order, lineItems)
	nIndexUpds, nBlockIndexUpds := 0, 0
	regN := ti.OrderToRegionkey(newOrder)

	if UPDATE_BASE_DATA {
		ti.makeSingleUpd(order, lineItems, updParams, bufI, itemsNewPerServer)
	}

	if UPDATE_INDEX {
		indexInfos := makeAddIndexInfos(ti.Tables, newOrder, newItems)
		if len(indexInfos) > 0 {
			nIndexUpds, _, nBlockIndexUpds = countAddsIndexInfo(indexInfos)
			if isIndexGlobal {
				updS, bufS := updParams[ti.indexServer], bufI[ti.indexServer]
				for _, indexInfo := range indexInfos {
					bufS = indexInfo.makeNewUpdArgs(ti.Tables, newOrder, updS, bufS, INDEX_BKT)
				}
				bufI[ti.indexServer] = bufS
			} else {
				newI, updN, bufN := int(regN)+INDEX_BKT, updParams[regN], bufI[regN]
				if localMode == LOCAL_SERVER {
					updN, bufN = updParams[ti.indexServer], bufI[ti.indexServer]
				}
				for _, indexInfo := range indexInfos {
					bufN = indexInfo.makeNewUpdArgs(ti.Tables, newOrder, updN, bufN, newI)
				}
				if localMode == LOCAL_SERVER {
					bufI[ti.indexServer] = bufN
				} else {
					bufI[regN] = bufN
				}
			}
		}
	}

	if UPDATE_BASE_DATA {
		ti.currUpdStat.nNews += nAdds
		ti.currUpdStat.nUpdBlocks += nAdds
	}
	ti.currUpdStat.nIndex += nIndexUpds
	ti.currUpdStat.nUpdBlocks += nBlockIndexUpds

	if UPDATE_BASE_DATA {
		return nAdds + nIndexUpds
	} else {
		return nIndexUpds
	}
}

func (ti SingleTableInfo) deleteAndIndexOnly(updParams [][]crdt.UpdateObjectParams, bufI []int, deleteKey string) (updsDone int) {
	deletedOrder, deletedItems := ti.getNextDelete(deleteKey)
	nDels, itemsDelPerServer, itemsIndex := ti.getSingleDelete(deletedOrder, deletedItems)
	nIndexUpds, nBlockIndexUpds := 0, 0
	regR := ti.OrderToRegionkey(deletedOrder)

	if UPDATE_BASE_DATA {
		ti.makeSingleDelete(deletedOrder, updParams, bufI, itemsDelPerServer, itemsIndex)
	}

	if UPDATE_INDEX {
		indexInfos := makeRemIndexInfos(ti.Tables, deletedOrder, deletedItems)
		if len(indexInfos) > 0 {
			nIndexUpds, _, nBlockIndexUpds = countRemsIndexInfo(indexInfos)
			if isIndexGlobal {
				updS, bufS := updParams[ti.indexServer], bufI[ti.indexServer]
				for _, indexInfo := range indexInfos {
					bufS = indexInfo.makeRemUpdArgs(ti.Tables, deletedOrder, updS, bufS, INDEX_BKT)
				}
				bufI[ti.indexServer] = bufS
			} else {
				remI, updR, bufR := int(regR)+INDEX_BKT, updParams[regR], bufI[regR]
				if localMode == LOCAL_SERVER {
					updR, bufR = updParams[ti.indexServer], bufI[ti.indexServer]
				}
				for _, indexInfo := range indexInfos {
					bufR = indexInfo.makeRemUpdArgs(ti.Tables, deletedOrder, updR, bufR, remI)
				}
				if localMode == LOCAL_SERVER {
					bufI[ti.indexServer] = bufR
				} else {
					bufI[regR] = bufR
				}
			}
		}
	}

	if UPDATE_BASE_DATA {
		ti.currUpdStat.nDels += nDels
		ti.currUpdStat.nUpdBlocks += nDels
	}
	ti.currUpdStat.nIndex += nIndexUpds
	ti.currUpdStat.nUpdBlocks += nBlockIndexUpds

	if UPDATE_BASE_DATA {
		return nDels + nIndexUpds
	} else {
		return nIndexUpds
	}
}

func (ti SingleTableInfo) getSingleDataChange(updParams [][]crdt.UpdateObjectParams, bufI []int, order []string, lineItems [][]string, deleteKey string) (updsDone int) {
	if LIKEHOOD_ADD != 0.5 {
		return ti.getSingleOddsDataChange(updParams, bufI, order, lineItems, deleteKey)
	}
	/*for i, updP := range updParams {
		fmt.Printf("[CQU]Size of buffer for server %d at start of getSingleDataChange: %d\n", i, len(updP))
	}*/
	deletedOrder, deletedItems := ti.getNextDelete(deleteKey)
	newOrder, newItems := ti.CreateOrder(order), ti.CreateLineitemsOfOrder(lineItems)
	nDels, itemsDelPerServer, itemsIndex := ti.getSingleDelete(deletedOrder, deletedItems)
	nAdds, itemsNewPerServer := ti.getSingleUpd(order, lineItems)
	nIndexUpds, nBlockIndexUpds /*, indexInfo*/ := 0, 0 /*, IndexInfo{}*/
	regN, regR := ti.OrderToRegionkey(newOrder), ti.OrderToRegionkey(deletedOrder)
	//fmt.Printf("Number of new items: %d. Removed items: %d. Total: %d\n", len(newItems), len(deletedItems), len(newItems)+len(deletedItems))
	//fmt.Println(bufI)
	if UPDATE_BASE_DATA {
		ti.makeSingleDelete(deletedOrder, updParams, bufI, itemsDelPerServer, itemsIndex)
		//fmt.Println(bufI)
		ti.makeSingleUpd(order, lineItems, updParams, bufI, itemsNewPerServer)
		//fmt.Println(bufI)
	}

	if UPDATE_INDEX {
		indexInfos := makeIndexInfos(ti.Tables, newOrder, deletedOrder, newItems, deletedItems)
		if len(indexInfos) > 0 { //Can happen if e.g., we're only doing Q11.
			nIndexUpds, _, nBlockIndexUpds = countUpdsIndexInfo(indexInfos)
			//fmt.Printf("[CQU]Finished making indexInfos. NIndexInfos: %d. Number of upds: %d (block %d)\n", len(indexInfos), nIndexUpds, nBlockIndexUpds)
			if isIndexGlobal {
				updS, bufS := updParams[ti.indexServer], bufI[ti.indexServer]
				//fmt.Printf("[CQU]BufS is at %d before we start making the index updates. Len, Capacity of buffer: %d, %d\n", bufS, len(updS), cap(updS))
				for _, indexInfo := range indexInfos {
					bufS = indexInfo.makeNewUpdArgs(ti.Tables, newOrder, updS, bufS, INDEX_BKT)
					//fmt.Printf("[CQU]Finished making indexInfo %T *new* upds. BufS is at %d.\n", indexInfo, bufS)
					bufS = indexInfo.makeRemUpdArgs(ti.Tables, deletedOrder, updS, bufS, INDEX_BKT)
					//fmt.Printf("[CQU]Finished making indexInfo %T *rem* upds. BufS is at %d.\n", indexInfo, bufS)
				}
				bufI[ti.indexServer] = bufS
			} else {
				newI, remI := int(regN)+INDEX_BKT, int(regR)+INDEX_BKT
				updN, bufN := updParams[regN], bufI[regN]
				if localMode == LOCAL_SERVER {
					updN, bufN = updParams[ti.indexServer], bufI[ti.indexServer]
				}
				for _, indexInfo := range indexInfos {
					//First, newUpds to updN
					bufN = indexInfo.makeNewUpdArgs(ti.Tables, newOrder, updN, bufN, newI)
				}
				updR, bufR := updParams[regR], bufI[regR]
				if localMode == LOCAL_SERVER {
					updR, bufR = updParams[ti.indexServer], bufI[ti.indexServer]
				}
				for _, indexInfo := range indexInfos {
					//Now, remUpds to updR. Note that under LOCAL_SERVER mode, or if both regions are equal, it goes to the previous buffer (as intended)
					bufR = indexInfo.makeRemUpdArgs(ti.Tables, deletedOrder, updR, bufR, remI)
				}
				if localMode == LOCAL_SERVER {
					bufI[ti.indexServer] = bufR
				} else if regN == regR {
					bufI[regN] = bufR
				} else {
					bufI[regN], bufI[regR] = bufN, bufR
				}
			}
			//fmt.Printf("Making index updates. NIndexUpds: %d, nBlockIndexUpds: %d, test: %d, bufI[index]: %d, indexServer: %d\n", nIndexUpds, nBlockIndexUpds, test, bufI[ti.indexServer], ti.indexServer)
		}
	}
	/*
		if UPDATE_INDEX && !UPDATE_SPECIFIC_INDEX_ONLY {
			_, _, nIndexUpds, nBlockIndexUpds, indexInfo = ti.getSingleIndexUpds(deleteKey, newOrder, newItems, deletedOrder, deletedItems)
			if isIndexGlobal {
				//*fmt.Println("[GetSingleDataChange]", "making index updates (isGlobal=true, updateIndex=true)")
				updS, bufS := updParams[ti.indexServer], bufI[ti.indexServer]
				bufS = ti.makeSingleQ3UpdArgs(indexInfo.q3, deletedOrder, newOrder, updS, bufS, INDEX_BKT)
				bufS = ti.makeSingleQ5UpdArgs(indexInfo.q5, deletedOrder, newOrder, updS, bufS, INDEX_BKT)
				bufS = ti.makeSingleQ14UpdArgs(indexInfo.q14, deletedOrder, newOrder, updS, bufS, INDEX_BKT)
				if !useTopSum {
					bufS = ti.makeSingleQ15UpdArgs(indexInfo.q15, deletedOrder, newOrder, updS, bufS, INDEX_BKT)
				} else {
					bufS = ti.makeSingleQ15TopSumUpdArgs(indexInfo.q15TopSum, deletedOrder, newOrder, updS, bufS, INDEX_BKT)
				}
				bufS = ti.makeSingleQ18UpdArgs(indexInfo.q18, deletedOrder, newOrder, updS, bufS, INDEX_BKT)
				bufI[ti.indexServer] = bufS
				//*fmt.Println("Number of updates for index server:", bufI[ti.indexServer], "Index server:", ti.indexServer)
			} else {
				//*fmt.Println("[GetSingleDataChange]", "making index updates (isGlobal=false, updateIndex=true)")
				newI, remI := int(regN)+INDEX_BKT, int(regR)+INDEX_BKT
				updN, bufN := updParams[regN], bufI[regN]
				if localMode == LOCAL_SERVER {
					updN, bufN = updParams[ti.indexServer], bufI[ti.indexServer]
				}
				if regN == regR {
					//Single msg for both new & rem
					bufN = ti.makeSingleQ3UpdArgs(indexInfo.q3, deletedOrder, newOrder, updN, bufN, newI)
					bufN = ti.makeSingleQ5UpdArgs(indexInfo.q5, deletedOrder, newOrder, updN, bufN, newI)
					bufN = ti.makeSingleQ14UpdArgs(SingleQ14{mapPromo: indexInfo.lq14.mapPromoRem, mapTotal: indexInfo.lq14.mapTotalRem}, deletedOrder, newOrder, updN, bufN, newI)
					bufN = ti.makeSingleQ14UpdArgs(SingleQ14{mapPromo: indexInfo.lq14.mapPromoUpd, mapTotal: indexInfo.lq14.mapTotalUpd}, deletedOrder, newOrder, updN, bufN, newI)
					if !useTopSum {
						bufN = ti.makeSingleLocalQ15UpdArgs(SingleQ15{updEntries: indexInfo.lq15.remEntries}, deletedOrder, newOrder, updN, bufN, newI, regN)
						bufN = ti.makeSingleLocalQ15UpdArgs(SingleQ15{updEntries: indexInfo.lq15.updEntries}, deletedOrder, newOrder, updN, bufN, newI, regN)
					} else {
						bufN = ti.makeSingleLocalQ15TopSumUpdArgs(SingleQ15TopSum{diffEntries: indexInfo.lq15TopSum.remEntries}, deletedOrder, newOrder, updN, bufN, newI, regN)
						bufN = ti.makeSingleLocalQ15TopSumUpdArgs(SingleQ15TopSum{diffEntries: indexInfo.lq15TopSum.updEntries}, deletedOrder, newOrder, updN, bufN, newI, regN)
					}
					bufN = ti.makeSingleQ18UpdArgs(indexInfo.q18, deletedOrder, newOrder, updN, bufN, newI)
					if localMode == LOCAL_DIRECT {
						bufI[regN] = bufN
					} else {
						bufI[ti.indexServer] = bufN
					}
				} else {
					updR, bufR := updParams[regR], bufI[regR]
					bufN = ti.makeSingleQ3UpdsArgsNews(indexInfo.q3, newOrder, updN, bufN, newI)
					bufN = ti.makeSingleQ5UpdArgsHelper(*indexInfo.q5.updValue, 1.0, newOrder, updN, bufN, newI)
					bufN = ti.makeSingleQ14UpdArgs(SingleQ14{mapPromo: indexInfo.lq14.mapPromoUpd, mapTotal: indexInfo.lq14.mapTotalUpd}, deletedOrder, newOrder, updN, bufN, newI)
					if !useTopSum {
						bufN = ti.makeSingleLocalQ15UpdArgs(SingleQ15{updEntries: indexInfo.lq15.updEntries}, deletedOrder, newOrder, updN, bufN, newI, regN)
					} else {
						bufN = ti.makeSingleLocalQ15TopSumUpdArgs(SingleQ15TopSum{diffEntries: indexInfo.lq15TopSum.updEntries}, deletedOrder, newOrder, updN, bufN, newI, regN)
					}
					bufN = ti.makeSingleQ18UpdsArgsNews(indexInfo.q18, newOrder, updN, bufN, newI)
					if localMode == LOCAL_SERVER {
						//Override with serverIndex (N)
						updR, bufR = updN, bufN
					}
					bufR = ti.makeSingleQ3UpdsArgsRems(indexInfo.q3, deletedOrder, updR, bufR, remI)
					bufR = ti.makeSingleQ5UpdArgsHelper(*indexInfo.q5.remValue, -1.0, deletedOrder, updR, bufR, remI)
					bufR = ti.makeSingleQ14UpdArgs(SingleQ14{mapPromo: indexInfo.lq14.mapPromoRem, mapTotal: indexInfo.lq14.mapTotalRem}, deletedOrder, newOrder, updR, bufR, remI)
					if !useTopSum {
						bufR = ti.makeSingleLocalQ15UpdArgs(SingleQ15{updEntries: indexInfo.lq15.remEntries}, deletedOrder, newOrder, updR, bufR, remI, regR)
					} else {
						bufR = ti.makeSingleLocalQ15TopSumUpdArgs(SingleQ15TopSum{diffEntries: indexInfo.lq15TopSum.remEntries}, deletedOrder, newOrder, updR, bufR, remI, regR)
					}
					bufR = ti.makeSingleQ18UpdsArgsRems(indexInfo.q18, deletedOrder, updR, bufR, remI)
					if localMode == LOCAL_DIRECT {
						bufI[regN], bufI[regR] = bufN, bufR
					} else {
						bufI[ti.indexServer] = bufR
					}
				}
			}
		} else if UPDATE_INDEX {
			//TODO
			if isIndexGlobal {
				bufI[ti.indexServer], nIndexUpds, nBlockIndexUpds = ti.getSpecificIndexesArgs(newOrder, deletedOrder, newItems, deletedItems, updParams[ti.indexServer], bufI[ti.indexServer])
			} else {
				//Local
				newI := int(regN) + INDEX_BKT
				if regN == regR {
					bufToUse := 0
					if localMode == LOCAL_DIRECT {
						bufToUse = int(regN)
					} else {
						bufToUse = ti.indexServer
					}
					bufI[bufToUse], nIndexUpds, nBlockIndexUpds = ti.getSpecificIndexesLocalSameRArgs(newOrder, deletedOrder, newItems, deletedItems, updParams[bufToUse], bufI[bufToUse], newI)
				} else {
					remI := int(regR) + INDEX_BKT
					if localMode == LOCAL_DIRECT {
						ti.getSpecificIndexesLocalArgs(newOrder, deletedOrder, newItems, deletedItems, updParams[regN], updParams[regR], bufI[regN], bufI[regR], newI, remI)
					} else {
						ti.getSpecificIndexesLocalRedirectArgs(newOrder, deletedOrder, newItems, deletedItems, updParams[ti.indexServer], bufI[ti.indexServer], newI, remI)
					}
				}
			}
		}
	*/

	if UPDATE_BASE_DATA {
		ti.currUpdStat.nDels += nDels
		ti.currUpdStat.nNews += nAdds
		ti.currUpdStat.nUpdBlocks += nDels + nAdds
	}
	ti.currUpdStat.nIndex += nIndexUpds
	ti.currUpdStat.nUpdBlocks += nBlockIndexUpds

	//*fmt.Println("BufI before returning (index server:", ti.indexServer, ") - ", bufI)

	if UPDATE_BASE_DATA {
		return nDels + nAdds + nIndexUpds
	} else {
		return nIndexUpds
	}
}

func (ti SingleTableInfo) sendSingleDataChange(order []string, lineItems [][]string, deleteKey string) (updsDone int) {
	//Idea: re-use the reusable parameters, always writing from 0, and then using bufI to know until where to read
	bufI := make([]int, 5)
	/*if ti.id == 0 {
		fmt.Printf("[CQU%d]Started single data change\n", ti.id)
	}*/
	updsDone = ti.getSingleDataChange(ti.reusableParams, bufI, order, lineItems, deleteKey)
	/*if ti.id == 0 {
		fmt.Printf("[CQU%d]Finished preparing single data change\n", ti.id)
	}*/

	for i, upds := range ti.reusableParams {
		if bufI[i] > 0 {
			//fmt.Println("Sending proto to region", i)
			antidote.SendProto(antidote.StaticUpdateObjs, antidote.CreateStaticUpdateObjs(nil, upds[:bufI[i]]), ti.conns[i])
		}
	}
	/*if ti.id == 0 {
		fmt.Printf("[CQU%d]Finished sending proto of data change\n", ti.id)
	}*/
	for i, nUpds := range bufI {
		if nUpds > 0 {
			antidote.ReceiveProto(ti.conns[i])
		}
	}
	/*if ti.id == 0 {
		fmt.Printf("[CQU%d]Finished receiving reply proto of data change\n", ti.id)
	}*/
	return
}

/*
func (ti SingleTableInfo) sendSingleDataChange(order []string, lineItems [][]string, deleteKey string) (updsDone int) {
	deletedOrder, deletedItems := ti.getNextDelete(deleteKey)
	newOrder, newItems := ti.CreateOrder(order), ti.CreateLineitemsOfOrder(lineItems)
	nDels, itemsDelPerServer, itemsIndex := ti.getSingleDelete(deletedOrder, deletedItems)
	nAdds, itemsNewPerServer := ti.getSingleUpd(order, lineItems)
	nUpdsN, nUpdsR, nIndexUpds, nBlockIndexUpds, indexInfo := 0, 0, 0, 0, IndexInfo{}

	if UPDATE_INDEX {
		nUpdsN, nUpdsR, nIndexUpds, nBlockIndexUpds, indexInfo = ti.getSingleIndexUpds(deleteKey, newOrder, newItems, deletedOrder, deletedItems)
	}
	regN, regR := ti.OrderToRegionkey(newOrder), ti.OrderToRegionkey(deletedOrder)

	updParams := make([][]crdt.UpdateObjectParams, len(channels.dataChans))
	//fmt.Println("Number of regions in updParams:", len(updParams), regN, regR)
	bufI := make([]int, len(updParams))

	if UPDATE_BASE_DATA {
		if isIndexGlobal {
			for i := range updParams {
				total := 0
				//new are all done in a single upd in the case of indexGlobal
				if len(itemsNewPerServer[i]) > 0 {
					total++
				}
				//So are removes
				if itemsIndex[i] > 0 {
					total++
				}
				if i == ti.indexServer && UPDATE_INDEX {
					total += nUpdsN
				}
				if i == int(regN) {
					if len(itemsNewPerServer[i]) == 0 {
						total += 2
					} else {
						total += 1 //new order
					}
				}
				if i == int(regR) {
					total += 1 //del order
				}
				updParams[i] = make([]crdt.UpdateObjectParams, total)
			}
		} else {
			for i := range updParams {
				total := 0
				if len(itemsNewPerServer[i]) > 0 {
					total++
				}
				//So are removes
				if itemsIndex[i] > 0 {
					total++
				}
				//total := itemsIndex[i] + len(itemsNewPerServer[i])
				if localMode == LOCAL_DIRECT {
					if UPDATE_INDEX && i == int(regN) {
						total += nUpdsN + 1
					} else if i == int(regN) {
						total++
					}
					if UPDATE_INDEX && i == int(regR) {
						total += nUpdsR + 1
					} else if i == int(regR) {
						total++
					}
				} else {
					if UPDATE_INDEX && i == ti.indexServer {
						total += nUpdsN + 1 + nUpdsR + 1
					} else if i == int(regN) || i == int(regR) {
						total++
					}
				}
				updParams[i] = make([]crdt.UpdateObjectParams, total)
			}
		}
	} else {
		if isIndexGlobal {
			for i := range updParams {
				if i == ti.indexServer && UPDATE_INDEX {
					updParams[i] = make([]crdt.UpdateObjectParams, nUpdsN)
				} else {
					updParams[i] = make([]crdt.UpdateObjectParams, 0)
				}
			}
		} else if localMode == LOCAL_DIRECT {
			for i := range updParams {
				total := 0
				if i == int(regN) && UPDATE_INDEX {
					total += nUpdsN + 1
				}
				if i == int(regR) && UPDATE_INDEX {
					total += nUpdsR + 1
				}
				updParams[i] = make([]crdt.UpdateObjectParams, total)
			}
		} else {
			for i := range updParams {
				if i == ti.indexServer && UPDATE_INDEX {
					updParams[i] = make([]crdt.UpdateObjectParams, nUpdsN+nUpdsR)
				} else {
					updParams[i] = make([]crdt.UpdateObjectParams, 0)
				}
			}
		}
	}

	//fmt.Println(bufI)
	if UPDATE_BASE_DATA {
		ti.makeSingleDelete(deletedOrder, updParams, bufI, itemsDelPerServer, itemsIndex)
		//fmt.Println(bufI)
		ti.makeSingleUpd(order, lineItems, updParams, bufI, itemsNewPerServer)
		//fmt.Println(bufI)
	}
	if UPDATE_INDEX {

		if isIndexGlobal {
			updS, bufS := updParams[ti.indexServer], bufI[ti.indexServer]
			bufS = ti.makeSingleQ3UpdArgs(indexInfo.q3, deletedOrder, newOrder, updS, bufS, INDEX_BKT)
			bufS = ti.makeSingleQ5UpdArgs(indexInfo.q5, deletedOrder, newOrder, updS, bufS, INDEX_BKT)
			bufS = ti.makeSingleQ14UpdArgs(indexInfo.q14, deletedOrder, newOrder, updS, bufS, INDEX_BKT)
			if !useTopSum {
				bufS = ti.makeSingleQ15UpdArgs(indexInfo.q15, deletedOrder, newOrder, updS, bufS, INDEX_BKT)
			} else {
				bufS = ti.makeSingleQ15TopSumUpdArgs(indexInfo.q15TopSum, deletedOrder, newOrder, updS, bufS, INDEX_BKT)
			}
			bufS = ti.makeSingleQ18UpdArgs(indexInfo.q18, deletedOrder, newOrder, updS, bufS, INDEX_BKT)
			bufI[ti.indexServer] = bufS
		} else {
			newI, remI := int(regN)+INDEX_BKT, int(regR)+INDEX_BKT
			updN, bufN := updParams[regN], bufI[regN]
			if localMode == LOCAL_SERVER {
				updN, bufN = updParams[ti.indexServer], bufI[ti.indexServer]
			}
			if regN == regR {
				//Single msg for both new & rem
				bufN = ti.makeSingleQ3UpdArgs(indexInfo.q3, deletedOrder, newOrder, updN, bufN, newI)
				bufN = ti.makeSingleQ5UpdArgs(indexInfo.q5, deletedOrder, newOrder, updN, bufN, newI)
				bufN = ti.makeSingleQ14UpdArgs(SingleQ14{mapPromo: indexInfo.lq14.mapPromoRem, mapTotal: indexInfo.lq14.mapTotalRem}, deletedOrder, newOrder, updN, bufN, newI)
				bufN = ti.makeSingleQ14UpdArgs(SingleQ14{mapPromo: indexInfo.lq14.mapPromoUpd, mapTotal: indexInfo.lq14.mapTotalUpd}, deletedOrder, newOrder, updN, bufN, newI)
				if !useTopSum {
					bufN = ti.makeSingleLocalQ15UpdArgs(SingleQ15{updEntries: indexInfo.lq15.remEntries}, deletedOrder, newOrder, updN, bufN, newI, regN)
					bufN = ti.makeSingleLocalQ15UpdArgs(SingleQ15{updEntries: indexInfo.lq15.updEntries}, deletedOrder, newOrder, updN, bufN, newI, regN)
				} else {
					bufN = ti.makeSingleLocalQ15TopSumUpdArgs(SingleQ15TopSum{diffEntries: indexInfo.lq15TopSum.remEntries}, deletedOrder, newOrder, updN, bufN, newI, regN)
					bufN = ti.makeSingleLocalQ15TopSumUpdArgs(SingleQ15TopSum{diffEntries: indexInfo.lq15TopSum.updEntries}, deletedOrder, newOrder, updN, bufN, newI, regN)
				}
				bufN = ti.makeSingleQ18UpdArgs(indexInfo.q18, deletedOrder, newOrder, updN, bufN, newI)
				if localMode == LOCAL_DIRECT {
					bufI[regN] = bufN
				} else {
					bufI[ti.indexServer] = bufN
				}
			} else {
				updR, bufR := updParams[regR], bufI[regR]
				bufN = ti.makeSingleQ3UpdsArgsNews(indexInfo.q3, newOrder, updN, bufN, newI)
				bufN = ti.makeSingleQ5UpdArgsHelper(*indexInfo.q5.updValue, 1.0, newOrder, updN, bufN, newI)
				bufN = ti.makeSingleQ14UpdArgs(SingleQ14{mapPromo: indexInfo.lq14.mapPromoUpd, mapTotal: indexInfo.lq14.mapTotalUpd}, deletedOrder, newOrder, updN, bufN, newI)
				if !useTopSum {
					bufN = ti.makeSingleLocalQ15UpdArgs(SingleQ15{updEntries: indexInfo.lq15.updEntries}, deletedOrder, newOrder, updN, bufN, newI, regN)
				} else {
					bufN = ti.makeSingleLocalQ15TopSumUpdArgs(SingleQ15TopSum{diffEntries: indexInfo.lq15TopSum.updEntries}, deletedOrder, newOrder, updN, bufN, newI, regN)
				}
				bufN = ti.makeSingleQ18UpdsArgsNews(indexInfo.q18, newOrder, updN, bufN, newI)
				if localMode == LOCAL_SERVER {
					//Point remove to index server too (N)
					updR, bufR = updN, bufN
				}
				bufR = ti.makeSingleQ3UpdsArgsRems(indexInfo.q3, deletedOrder, updR, bufR, remI)
				bufR = ti.makeSingleQ5UpdArgsHelper(*indexInfo.q5.remValue, -1.0, deletedOrder, updR, bufR, remI)
				bufR = ti.makeSingleQ14UpdArgs(SingleQ14{mapPromo: indexInfo.lq14.mapPromoRem, mapTotal: indexInfo.lq14.mapTotalRem}, deletedOrder, newOrder, updR, bufR, remI)
				if !useTopSum {
					bufR = ti.makeSingleLocalQ15UpdArgs(SingleQ15{updEntries: indexInfo.lq15.remEntries}, deletedOrder, newOrder, updR, bufR, remI, regR)
				} else {
					bufR = ti.makeSingleLocalQ15TopSumUpdArgs(SingleQ15TopSum{diffEntries: indexInfo.lq15TopSum.remEntries}, deletedOrder, newOrder, updR, bufR, remI, regR)
				}
				bufR = ti.makeSingleQ18UpdsArgsRems(indexInfo.q18, deletedOrder, updR, bufR, remI)
				if localMode == LOCAL_DIRECT {
					bufI[regN], bufI[regR] = bufN, bufR
				} else {
					bufI[ti.indexServer] = bufR
				}
			}
		}
	}

	for i, upds := range updParams {
		if bufI[i] > 0 {
			//fmt.Println("Sending proto to region", i)
			antidote.SendProto(antidote.StaticUpdateObjs, antidote.CreateStaticUpdateObjs(nil, upds), ti.conns[i])
		}
	}
	for i, nUpds := range bufI {
		if nUpds > 0 {
			antidote.ReceiveProto(ti.conns[i])
		}
	}
	if UPDATE_BASE_DATA {
		ti.currUpdStat.nDels += nDels
		ti.currUpdStat.nNews += nAdds
		ti.currUpdStat.nUpdBlocks += nDels + nAdds
	}
	ti.currUpdStat.nIndex += nIndexUpds
	ti.currUpdStat.nUpdBlocks += nBlockIndexUpds

	if UPDATE_BASE_DATA {
		return nDels + nAdds + nIndexUpds
	} else {
		return nIndexUpds
	}
}
*/

func (ti SingleTableInfo) getSingleDelete(orderDel *tpch.Orders, itemsDel []*tpch.LineItem) (nDels int, itemsPerServer [][]string, itemsIndex []int) {
	orderReg := ti.Tables.Custkey32ToRegionkey(orderDel.O_CUSTKEY)
	//Items may need to go to multiple servers (cust + supplier)
	itemsPerServer, itemsIndex = make([][]string, len(ti.Tables.Regions)), make([]int, len(ti.Tables.Regions))
	for i := range itemsPerServer {
		itemsPerServer[i] = make([]string, len(itemsDel))
	}

	var suppReg int8
	var itemS string
	key := strconv.FormatInt(int64(orderDel.O_ORDERKEY), 10)

	//TODO: Remove temporary counters
	nItems, nItemUpds := 0, 0
	for _, item := range itemsDel {
		itemS = key + "_" + strconv.FormatInt(int64(item.L_PARTKEY), 10) + "_" +
			strconv.FormatInt(int64(item.L_SUPPKEY), 10) + "_" + strconv.FormatInt(int64(item.L_LINENUMBER), 10)
		itemsPerServer[orderReg][itemsIndex[orderReg]] = itemS
		itemsIndex[orderReg]++
		nDels++
		ti.debugStats.deleteItems++
		suppReg = ti.Tables.SuppkeyToRegionkey(int64(item.L_SUPPKEY))
		nItems++
		nItemUpds++
		if orderReg != suppReg {
			itemsPerServer[suppReg][itemsIndex[suppReg]] = itemS
			itemsIndex[suppReg]++
			nDels++
			ti.debugStats.deleteItems++
			nItemUpds++
		}
	}
	//Order
	nDels++
	ti.debugStats.deleteOrders++
	//fmt.Printf("[CQU]Delete order %d has %d items and %d item deletes\n", orderDel.O_ORDERKEY, nItems, nItemUpds)
	return
}

func (ti SingleTableInfo) makeSingleDelete(orderDel *tpch.Orders, updParams [][]crdt.UpdateObjectParams, bufI []int, itemsPerServer [][]string, itemsIndex []int) {
	orderReg, key := ti.Tables.Custkey32ToRegionkey(orderDel.O_CUSTKEY), strconv.FormatInt(int64(orderDel.O_ORDERKEY), 10)
	orderUpdParams := updParams[orderReg]
	if !CRDT_PER_OBJ {
		orderParams := getSingleDeleteParams(tpch.TableNames[tpch.ORDERS], buckets[orderReg], key)
		itemParams := getDeleteParams(tpch.TableNames[tpch.LINEITEM], buckets[orderReg], itemsPerServer[orderReg], itemsIndex[orderReg])
		orderUpdParams[bufI[orderReg]] = orderParams
		orderUpdParams[bufI[orderReg]+1] = itemParams
		bufI[orderReg] += 2
		for i, items := range itemsPerServer {
			if int8(i) == orderReg || itemsIndex[i] == 0 {
				continue
			}
			itemParams = getDeleteParams(tpch.TableNames[tpch.LINEITEM], buckets[i], items, itemsIndex[i])
			updParams[i][bufI[i]] = itemParams
			bufI[i]++
			//ti.sendUpdate([]crdt.UpdateObjectParams{itemParams}, i, itemsIndex[i], REMOVE_TYPE)
		}
	} else {
		//upds := make([]crdt.UpdateObjectParams, itemsIndex[orderReg]+1)
		updParams[orderReg][bufI[orderReg]] = getSinglePerObjDeleteParams(tpch.TableNames[tpch.ORDERS], buckets[orderReg], key)
		bufI[orderReg] = getPerObjDeleteParams(tpch.TableNames[tpch.LINEITEM], buckets[orderReg], itemsPerServer[orderReg], itemsIndex[orderReg], updParams[orderReg], bufI[orderReg]+1)
		//ti.sendUpdate(upds, int(orderReg), len(upds), REMOVE_TYPE)
		for i, items := range itemsPerServer {
			if int8(i) == orderReg || itemsIndex[i] == 0 {
				continue
			}
			//upds = make([]crdt.UpdateObjectParams, itemsIndex[i])
			bufI[i] = getPerObjDeleteParams(tpch.TableNames[tpch.LINEITEM], buckets[i], items, itemsIndex[i], updParams[i], bufI[i])
			//ti.sendUpdate(upds, i, len(upds), REMOVE_TYPE)
		}
	}
}

/*
func (ti SingleTableInfo) sendSingleDelete(key string) (orderDel tpch.Orders, itemsDel []tpch.LineItem, nDels int) {
	orderDel, itemsDel = ti.getNextDelete(key)

	orderReg := ti.Tables.Custkey32ToRegionkey(orderDel.O_CUSTKEY)
	//Items may need to go to multiple servers (cust + supplier)
	itemsPerServer, itemsIndex := make([][]string, len(channels.dataChans)), make([]int, len(channels.dataChans))
	for i := range itemsPerServer {
		itemsPerServer[i] = make([]string, len(itemsDel))
	}

	var suppReg int8
	var itemS string
	for _, item := range itemsDel {
		//TODO: is itemS correct? Considering on getEntryUpd/getEntryKey they add "_" (and, possibly, tableName)
		itemS = key + strconv.FormatInt(int64(item.L_PARTKEY), 10) +
			strconv.FormatInt(int64(item.L_SUPPKEY), 10) + strconv.FormatInt(int64(item.L_LINENUMBER), 10)
		itemsPerServer[orderReg][itemsIndex[orderReg]] = itemS
		itemsIndex[orderReg]++
		nDels++
		suppReg = ti.Tables.SuppkeyToRegionkey(int64(item.L_SUPPKEY))
		if orderReg != suppReg {
			itemsPerServer[suppReg][itemsIndex[suppReg]] = itemS
			itemsIndex[suppReg]++
			nDels++
		}
	}
	//Order
	nDels++

	//Different handling for order region and supplier regions
	if !CRDT_PER_OBJ {
		orderParams := getSingleDeleteParams(TableNames[tpch.ORDERS], buckets[orderReg], key)
		itemParams := getDeleteParams(TableNames[LINEITEM], buckets[orderReg], itemsPerServer[orderReg], itemsIndex[orderReg])
		ti.sendUpdate([]crdt.UpdateObjectParams{orderParams, itemParams}, int(orderReg), itemsIndex[orderReg]+1, REMOVE_TYPE)
		// channels.updateChans[orderReg] <- QueuedMsgWithStat{QueuedMsg: QueuedMsg{
		// 	code:    antidote.StaticUpdateObjs,
		// 	Message: antidote.CreateStaticUpdateObjs(nil, []crdt.UpdateObjectParams{orderParams, itemParams})},
		// 	nData:    itemsIndex[orderReg] + 1,
		// 	dataType: REMOVE_TYPE,
		// }
		for i, items := range itemsPerServer {
			if int8(i) == orderReg || itemsIndex[i] == 0 {
				continue
			}
			itemParams = getDeleteParams(TableNames[LINEITEM], buckets[i], items, itemsIndex[i])
			ti.sendUpdate([]crdt.UpdateObjectParams{itemParams}, i, itemsIndex[i], REMOVE_TYPE)
			// channels.updateChans[i] <- QueuedMsgWithStat{QueuedMsg: QueuedMsg{
			// 	code:    antidote.StaticUpdateObjs,
			// 	Message: antidote.CreateStaticUpdateObjs(nil, []crdt.UpdateObjectParams{itemParams})},
			// 	nData:    itemsIndex[i],
			// 	dataType: REMOVE_TYPE,
			// }
		}
	} else {
		upds := make([]crdt.UpdateObjectParams, itemsIndex[orderReg]+1)
		upds[0] = getSinglePerObjDeleteParams(TableNames[tpch.ORDERS], buckets[orderReg], key)
		getPerObjDeleteParams(TableNames[LINEITEM], buckets[orderReg], itemsPerServer[orderReg], itemsIndex[orderReg], upds, 1)
		//Note: If we go back to sending upds via the update chans, use queueMsgWithStat.
		// channels.updateChans[orderReg] <- QueuedMsgWithStat{QueuedMsg: QueuedMsg{
		// 	code:    antidote.StaticUpdateObjs,
		// 	Message: antidote.CreateStaticUpdateObjs(nil, upds)},
		// 	nData:    len(upds),
		// 	dataType: REMOVE_TYPE,
		// }
		ti.sendUpdate(upds, int(orderReg), len(upds), REMOVE_TYPE)
		for i, items := range itemsPerServer {
			if int8(i) == orderReg || itemsIndex[i] == 0 {
				continue
			}
			upds = make([]crdt.UpdateObjectParams, itemsIndex[i])
			getPerObjDeleteParams(TableNames[LINEITEM], buckets[i], items, itemsIndex[i], upds, 0)
			// channels.updateChans[i] <- QueuedMsgWithStat{QueuedMsg: QueuedMsg{
			// 	code:    antidote.StaticUpdateObjs,
			// 	Message: antidote.CreateStaticUpdateObjs(nil, upds)},
			// 	nData:    len(upds),
			// 	dataType: REMOVE_TYPE,
			// }
			ti.sendUpdate(upds, i, len(upds), REMOVE_TYPE)
		}
	}
	return
}

*/

func (ti SingleTableInfo) sendUpdate(updates []crdt.UpdateObjectParams, channel int, nUpds int, dataType int) {
	antidote.SendProto(antidote.StaticUpdateObjs, antidote.CreateStaticUpdateObjs(nil, updates), ti.conns[channel])
	ti.waitFor[channel]++
	//TODO: Store nUpds/dataType
}

func getSingleDeleteParams(tableName string, bkt string, key string) crdt.UpdateObjectParams {
	return crdt.UpdateObjectParams{
		KeyParams:  crdt.KeyParams{Key: tableName, CrdtType: proto.CRDTType_RRMAP, Bucket: bkt},
		UpdateArgs: crdt.MapRemove{Key: key},
	}
}

func getSinglePerObjDeleteParams(tableName string, bkt string, key string) crdt.UpdateObjectParams {
	return crdt.UpdateObjectParams{
		KeyParams:  crdt.KeyParams{Key: tableName + key, CrdtType: proto.CRDTType_RRMAP, Bucket: bkt},
		UpdateArgs: crdt.ResetOp{},
	}
}

func (ti SingleTableInfo) getNextDelete(key string) (orderToDelete *tpch.Orders, itemsToDelete []*tpch.LineItem) {
	//delPos := ti.Tables.LastDeletedPos
	keyInt, _ := strconv.ParseInt(key, 10, 32)
	delPos := ti.Tables.GetOrderIndex(int32(keyInt))
	return ti.Tables.Orders[delPos], ti.Tables.LineItems[delPos-1]
}

func (ti SingleTableInfo) getSingleUpd(orderUpd []string, lineItemUpds [][]string) (nAdds int, itemsPerServer []map[string]crdt.UpdateArguments) {
	itemFunc := specialLineItemFunc
	itemsPerServer = make([]map[string]crdt.UpdateArguments, len(ti.Tables.Regions))
	for i := 0; i < len(ti.Tables.Regions); i++ {
		itemsPerServer[i] = make(map[string]crdt.UpdateArguments)
	}
	var key string
	var upd crdt.EmbMapUpdateAll
	var itemRegions []int8

	//nItems, nItemUpds := 0, 0
	for _, item := range lineItemUpds {
		key, upd = GetInnerMapEntry(tpchData.Headers[tpch.LINEITEM], tpchData.Keys[tpch.LINEITEM], item, tpchData.ToRead[tpch.LINEITEM])
		key = getEntryKey(tpch.TableNames[tpch.LINEITEM], key)
		itemRegions = itemFunc(orderUpd, item)
		for _, region := range itemRegions {
			itemsPerServer[region][key] = upd
			nAdds++
			ti.debugStats.newItems++
			//nItemUpds++
		}
		//nItems++
	}
	//order
	nAdds++
	ti.debugStats.newOrders++
	//fmt.Printf("[CQU]New order %s has %d items and %d item updates\n", orderUpd[O_ORDERKEY], nItems, nItemUpds)

	return
}

func (ti SingleTableInfo) makeSingleUpd(orderUpd []string, lineItemUpds [][]string, updParams [][]crdt.UpdateObjectParams, bufI []int,
	itemsPerServer []map[string]crdt.UpdateArguments) {
	orderReg := regionFuncs[tpch.ORDERS](orderUpd)

	orderKey, orderMapUpd := GetInnerMapEntry(tpchData.Headers[tpch.ORDERS], tpchData.Keys[tpch.ORDERS], orderUpd, tpchData.ToRead[tpch.ORDERS])
	orderKey = getEntryKey(tpch.TableNames[tpch.ORDERS], orderKey)
	//fmt.Println("startSingleUpd", bufI)
	//currUpdParams := make([]crdt.UpdateObjectParams, getUpdSize(itemsPerServer[orderReg])+1)
	updParams[orderReg][bufI[orderReg]] = getSingleDataUpdateParams(orderKey, orderMapUpd, tpch.TableNames[tpch.ORDERS], buckets[orderReg])
	//fmt.Println("1SingleUpd", bufI)
	//currUpdParams[0] = getSingleDataUpdateParams(orderKey, orderMapUpd, tpch.TableNames[tpch.ORDERS], buckets[orderReg])
	bufI[orderReg] = getDataUpdateParamsWithBuf(itemsPerServer[orderReg], tpch.TableNames[tpch.LINEITEM], buckets[orderReg], updParams[orderReg], bufI[orderReg]+1)
	//fmt.Println("2SingleUpd", bufI)
	//ti.sendUpdate(currUpdParams, int(orderReg), len(itemsPerServer[orderReg])+1, NEW_TYPE)

	for i := 0; i < len(ti.Tables.Regions); i++ {
		if int8(i) != orderReg && len(itemsPerServer[i]) > 0 {
			//currUpdParams = getDataUpdateParams(itemsPerServer[i], tpch.TableNames[tpch.LINEITEM], buckets[i])
			bufI[i] = getDataUpdateParamsWithBuf(itemsPerServer[i], tpch.TableNames[tpch.LINEITEM], buckets[i], updParams[i], bufI[i])
			//ti.sendUpdate(currUpdParams, i, len(itemsPerServer[i]), NEW_TYPE)
			//fmt.Println("CycleSingleUpd", bufI)
		}
	}
}

/*
func (ti SingleTableInfo) sendSingleUpd(orderUpd []string, lineItemUpds [][]string) (nAdds int) {
	orderReg := regionFuncs[tpch.ORDERS](orderUpd)
	itemFunc := specialLineItemFunc
	itemsPerServer := make([]map[string]crdt.UpdateArguments, len(ti.Tables.Regions))
	for i := 0; i < len(ti.Tables.Regions); i++ {
		itemsPerServer[i] = make(map[string]crdt.UpdateArguments)
	}
	var key string
	var upd *crdt.EmbMapUpdateAll
	var itemRegions []int8

	for _, item := range lineItemUpds {
		key, upd = getEntryUpd(headers[LINEITEM], keys[LINEITEM], item, read[LINEITEM])
		key = getEntryKey(TableNames[LINEITEM], key)
		itemRegions = itemFunc(orderUpd, item)
		for _, region := range itemRegions {
			itemsPerServer[region][key] = *upd
			nAdds++
		}
	}
	//order
	nAdds++

	orderKey, orderMapUpd := getEntryUpd(headers[tpch.ORDERS], keys[tpch.ORDERS], orderUpd, read[tpch.ORDERS])
	orderKey = getEntryKey(TableNames[tpch.ORDERS], orderKey)
	currUpdParams := make([]crdt.UpdateObjectParams, getUpdSize(itemsPerServer[orderReg])+1)
	currUpdParams[0] = getSingleDataUpdateParams(orderKey, orderMapUpd, TableNames[tpch.ORDERS], buckets[orderReg])
	getDataUpdateParamsWithBuf(itemsPerServer[orderReg], TableNames[LINEITEM], buckets[orderReg], currUpdParams, 1)
	ti.sendUpdate(currUpdParams, int(orderReg), len(itemsPerServer[orderReg])+1, NEW_TYPE)
		// channels.updateChans[orderReg] <- QueuedMsgWithStat{QueuedMsg: QueuedMsg{
		// 	code:    antidote.StaticUpdateObjs,
		// 	Message: antidote.CreateStaticUpdateObjs(nil, currUpdParams)},
		// 	nData:    len(itemsPerServer[orderReg]) + 1,
		// 	dataType: NEW_TYPE,
		// }

	for i := 0; i < len(ti.Tables.Regions); i++ {
		if int8(i) != orderReg && len(itemsPerServer[i]) > 0 {
			currUpdParams = getDataUpdateParams(itemsPerServer[i], TableNames[LINEITEM], buckets[i])
			ti.sendUpdate(currUpdParams, i, len(itemsPerServer[i]), NEW_TYPE)
				// channels.updateChans[i] <- QueuedMsgWithStat{QueuedMsg: QueuedMsg{
				// 	code:    antidote.StaticUpdateObjs,
				// 	Message: antidote.CreateStaticUpdateObjs(nil, currUpdParams)},
				// 	nData:    len(itemsPerServer[i]),
				// 	dataType: NEW_TYPE,
				// }
		}
	}
	return
}
*/

func getUpdSize(upds map[string]crdt.UpdateArguments) int {
	if CRDT_PER_OBJ {
		return len(upds)
	}
	return 1
}

func getSingleDataUpdateParams(key string, updArgs crdt.UpdateArguments, name string, bucket string) (updParams crdt.UpdateObjectParams) {
	if CRDT_PER_OBJ {
		return crdt.UpdateObjectParams{KeyParams: crdt.MakeKeyParams(key, proto.CRDTType_RRMAP, bucket), UpdateArgs: updArgs}
	} else {
		return crdt.UpdateObjectParams{KeyParams: crdt.MakeKeyParams(name, proto.CRDTType_RRMAP, bucket), UpdateArgs: crdt.EmbMapUpdate{Key: key, Upd: updArgs}}
	}
}

/*
func (ti SingleTableInfo) getSingleIndexUpds(deleteKey string, newOrder *tpch.Orders, newItems []*tpch.LineItem,
	remOrder *tpch.Orders, remItems []*tpch.LineItem) (nUpdsN, nUpdsR, nUpdsStats, nBlockUpds int, indexInfo IndexInfo) {
	if isIndexGlobal {
		q15, q15TopSum := SingleQ15{}, SingleQ15TopSum{}
		q3, q5, q14, q18 := ti.getSingleQ3Upds(remOrder, remItems, newOrder, newItems), ti.getSingleQ5Upds(remOrder, remItems, newOrder, newItems),
			ti.getSingleQ14Upds(remOrder, remItems, newOrder, newItems), ti.getSingleQ18Upds(remOrder, remItems, newOrder, newItems)
		if !useTopSum {
			q15 = ti.getSingleQ15Upds(remOrder, remItems, newOrder, newItems)
		} else {
			q15TopSum = ti.getSingleQ15TopSumUpds(remOrder, remItems, newOrder, newItems)
		}

		nUpdsN, nUpdsStats, nBlockUpds = ti.getNUpdObjs(q3, q5, q14, q15, q15TopSum, q18)
		indexInfo = IndexInfo{q3: q3, q5: q5, q14: q14, q15: q15, q15TopSum: q15TopSum, q18: q18}
	} else {
		newR, remR := ti.OrderToRegionkey(newOrder), ti.OrderToRegionkey(remOrder)
		q15, q15TopSum := SingleLocalQ15{}, SingleLocalQ15TopSum{}
		q3, q5, q14, q18 := ti.getSingleQ3Upds(remOrder, remItems, newOrder, newItems),
			ti.getSingleQ5Upds(remOrder, remItems, newOrder, newItems), ti.getSingleLocalQ14Upds(remOrder, remItems, newOrder, newItems),
			ti.getSingleQ18Upds(remOrder, remItems, newOrder, newItems)
		if !useTopSum {
			q15 = ti.getSingleLocalQ15Upds(remOrder, remItems, newOrder, newItems, remR, newR)
		} else {
			q15TopSum = ti.getSingleLocalQ15TopSumUpds(remOrder, remItems, newOrder, newItems, remR, newR)
		}
		nUpdsR, nUpdsN, nUpdsStats, nBlockUpds = ti.getNUpdLocalObjs(q3, q5, q14, q15, q15TopSum, q18, remR, newR)
		indexInfo = IndexInfo{q3: q3, q5: q5, lq14: q14, lq15: q15, lq15TopSum: q15TopSum, q18: q18}
	}
	return
}
*/

/*
func (ti SingleTableInfo) sendSingleIndexUpds(deleteKey string, orderUpd []string, lineItemUpds [][]string,
	remOrder tpch.Orders, remItems []tpch.LineItem) (nUpdsStats int) {
	if isIndexGlobal {
		return ti.sendSingleGlobalIndexUpdates(deleteKey, orderUpd, lineItemUpds, remOrder, remItems)
	}
	return ti.sendSingleLocalIndexUpdates(deleteKey, orderUpd, lineItemUpds, remOrder, remItems)
}

func (ti SingleTableInfo) sendSingleLocalIndexUpdates(deleteKey string, orderUpd []string, lineItemUpds [][]string,
	remOrder tpch.Orders, remItems []tpch.LineItem) (nUpdsStats int) {

	newOrder, newItems := ti.CreateOrder(orderUpd), ti.CreateLineitemsOfOrder(lineItemUpds)

	newR, remR := ti.OrderToRegionkey(newOrder), ti.OrderToRegionkey(remOrder)
	q3, q5, q14, q15, q18 := ti.getSingleQ3Upds(remOrder, remItems, newOrder, newItems),
		ti.getSingleQ5Upds(remOrder, remItems, newOrder, newItems), ti.getSingleLocalQ14Upds(remOrder, remItems, newOrder, newItems),
		ti.getSingleLocalQ15Upds(remOrder, remItems, newOrder, newItems, remR, newR), ti.getSingleQ18Upds(remOrder, remItems, newOrder, newItems)

	//Q3: single order -> single region -> single server. Ok to call getSingleQ3Upds as it splits in two maps.
	//I'll need to split makeSingleQ3UpdArgs in two tho (one for upds, one for rems)
	//Q5: albeit based on items, it's for orders in which all items' suppliers are in the same nation. Thus, again, single server.
	//Again, I'll need to split makeSingleQ5UpdsArgs in two (upds, rems)
	//Q14: also single server as the index location is based on the order's.
	//However, getSingleQ14Upds mixes both newOrder and remOrder in the same maps!
	//I can actually use makeSingleQ14Upds: all I need is to call it for each map, with the correct index.
	//Q15: albeit based on items, the indexes are partitioned according to the order's region. Thus, single server.
	//Since getSingleQ15Upds mixes both new and rem in the same map, a local version is needed.
	//Due to the map split, makeSingleQ15Upds can be used - a call per map, with the correct index.
	//Q18: Single order -> single customer, thus single server. As usual, need to split makeSingleQ18UpdsArgs in two (upds, rems)

	nUpdsR, nUpdsN, nUpdsStats := ti.getNUpdLocalObjs(q3, q5, q14, q15, q18, remR, newR)
	newI, remI := int(newR)+INDEX_BKT, int(remR)+INDEX_BKT
	if newR == remR {
		//Single msg for both new & rem
		upds, bufI := make([]crdt.UpdateObjectParams, nUpdsR+nUpdsN), 0
		bufI = ti.makeSingleQ3UpdArgs(q3, remOrder, newOrder, upds, bufI, newI)
		bufI = ti.makeSingleQ5UpdArgs(q5, remOrder, newOrder, upds, bufI, newI)
		bufI = ti.makeSingleQ14UpdArgs(SingleQ14{mapPromo: q14.mapPromoRem, mapTotal: q14.mapTotalRem}, remOrder, newOrder, upds, bufI, newI)
		bufI = ti.makeSingleQ14UpdArgs(SingleQ14{mapPromo: q14.mapPromoUpd, mapTotal: q14.mapTotalUpd}, remOrder, newOrder, upds, bufI, newI)
		bufI = ti.makeSingleLocalQ15UpdArgs(SingleQ15{updEntries: q15.remEntries}, remOrder, newOrder, upds, bufI, newI, newR)
		bufI = ti.makeSingleLocalQ15UpdArgs(SingleQ15{updEntries: q15.updEntries}, remOrder, newOrder, upds, bufI, newI, newR)
		bufI = ti.makeSingleQ18UpdArgs(q18, remOrder, newOrder, upds, bufI, newI)

		//queueStatMsg(int(newR), upds, nUpdsStats, INDEX_TYPE)
		ti.sendUpdate(upds, int(newR), nUpdsStats, INDEX_TYPE)
	} else {
		newUpds, remUpds, newBufI, remBufI := make([]crdt.UpdateObjectParams, nUpdsN), make([]crdt.UpdateObjectParams, nUpdsR), 0, 0
		newBufI = ti.makeSingleQ3UpdsArgsNews(q3, newOrder, newUpds, newBufI, newI)
		newBufI = ti.makeSingleQ5UpdArgsHelper(*q5.updValue, 1.0, newOrder, newUpds, newBufI, newI)
		newBufI = ti.makeSingleQ14UpdArgs(SingleQ14{mapPromo: q14.mapPromoUpd, mapTotal: q14.mapTotalUpd}, remOrder, newOrder, newUpds, newBufI, newI)
		newBufI = ti.makeSingleLocalQ15UpdArgs(SingleQ15{updEntries: q15.updEntries}, remOrder, newOrder, newUpds, newBufI, newI, newR)
		newBufI = ti.makeSingleQ18UpdsArgsNews(q18, newOrder, newUpds, newBufI, newI)
		remBufI = ti.makeSingleQ3UpdsArgsRems(q3, remOrder, remUpds, remBufI, remI)
		remBufI = ti.makeSingleQ5UpdArgsHelper(*q5.remValue, -1.0, remOrder, remUpds, remBufI, remI)
		remBufI = ti.makeSingleQ14UpdArgs(SingleQ14{mapPromo: q14.mapPromoRem, mapTotal: q14.mapTotalRem}, remOrder, newOrder, remUpds, remBufI, remI)
		remBufI = ti.makeSingleLocalQ15UpdArgs(SingleQ15{updEntries: q15.remEntries}, remOrder, newOrder, remUpds, remBufI, remI, remR)
		remBufI = ti.makeSingleQ18UpdsArgsRems(q18, remOrder, remUpds, remBufI, remI)

		//queueStatMsg(int(newR), newUpds, nUpdsStats/2, INDEX_TYPE)
		ti.sendUpdate(newUpds, int(newR), nUpdsStats, INDEX_TYPE)
		remStats := nUpdsStats - nUpdsStats/2 //We subtract to avoid loss in the case of not being divisible by 2
		//queueStatMsg(int(remR), remUpds, remStats, INDEX_TYPE)
		ti.sendUpdate(remUpds, int(remR), remStats, INDEX_TYPE)
	}

	return
}

func (ti SingleTableInfo) sendSingleGlobalIndexUpdates(deleteKey string, orderUpd []string, lineItemUpds [][]string,
	remOrder tpch.Orders, remItems []tpch.LineItem) (nUpdsStats int) {

	//fmt.Printf("[%d]Preparing index upds for order %s. Number of new lineItems: %d, delete lineItems: %d\n", ti.id,
	//orderUpd[0], len(lineItemUpds), len(remItems))

	newOrder, newItems := ti.CreateOrder(orderUpd), ti.CreateLineitemsOfOrder(lineItemUpds)
	q3, q5, q14, q15, q18 := ti.getSingleQ3Upds(remOrder, remItems, newOrder, newItems),
		ti.getSingleQ5Upds(remOrder, remItems, newOrder, newItems), ti.getSingleQ14Upds(remOrder, remItems, newOrder, newItems),
		ti.getSingleQ15Upds(remOrder, remItems, newOrder, newItems), ti.getSingleQ18Upds(remOrder, remItems, newOrder, newItems)

	nUpds, nUpdsStats := ti.getNUpdObjs(q3, q5, q14, q15, q18)
	upds, bufI := make([]crdt.UpdateObjectParams, nUpds), 0
	bufI = ti.makeSingleQ3UpdArgs(q3, remOrder, newOrder, upds, bufI, INDEX_BKT)
	bufI = ti.makeSingleQ5UpdArgs(q5, remOrder, newOrder, upds, bufI, INDEX_BKT)
	bufI = ti.makeSingleQ14UpdArgs(q14, remOrder, newOrder, upds, bufI, INDEX_BKT)
	bufI = ti.makeSingleQ15UpdArgs(q15, remOrder, newOrder, upds, bufI, INDEX_BKT)
	bufI = ti.makeSingleQ18UpdArgs(q18, remOrder, newOrder, upds, bufI, INDEX_BKT)

	if len(upds) > bufI {
		//Shouldn't happen anymore, but just in case
		upds = upds[:bufI]
	}
	//IndexChans is on purpose, to take in consideration the index split and SINGLE_INDEX_SERVER configs.
	//queueStatMsg(ti.rng.Intn(len(channels.indexChans)), upds, nUpdsStats, INDEX_TYPE)
	ti.sendUpdate(upds, ti.rng.Intn(len(channels.indexChans)), nUpdsStats, INDEX_TYPE)
	return
}

func (ti SingleTableInfo) getNUpdLocalObjs(q3 SingleQ3, q5 SingleQ5, q14 SingleLocalQ14, q15 SingleLocalQ15, q15TopSum SingleLocalQ15TopSum, q18 SingleQ18, remR,
	newR int8) (nUpdsR, nUpdsN, nUpdsStats, nBlockUpds int) {

	if q3.updStartPos > 0 {
		nUpdsN += int(MAX_MONTH_DAY - q3.updStartPos + 1)
	}
	nUpdsR += len(q3.remMap)
	if q3.updStartPos > 0 || len(q3.remMap) > 0 {
		nBlockUpds++
	}
	q5Rem, q5Upd := *q5.remValue, *q5.updValue
	if q5Rem != 0 {
		nUpdsR += 1
	}
	if q5Upd != 0 {
		nUpdsN += 1
	}
	if q5Rem != 0 || q5Upd != 0 {
		nBlockUpds++
	}

	nUpdsR += len(q14.mapTotalRem)
	nUpdsN += len(q14.mapTotalUpd)
	if len(q14.mapTotalRem) > 0 || len(q14.mapTotalUpd) > 0 {
		nBlockUpds++
	}
	nUpdsR += ti.countQ18Upds(q18.remQuantity)
	nUpdsN += ti.countQ18Upds(q18.updQuantity)
	if q18.remQuantity >= 312 || q18.updQuantity >= 312 {
		nBlockUpds++
	}
	nUpdsStats = nUpdsN + nUpdsR
	var q15UpdsR, q15UpdsN, q15UpdsStatsR, q15UpdsStatsN int
	if !useTopSum {
		q15UpdsR, q15UpdsStatsR = ti.countQ15Upds(q15.remEntries)
		q15UpdsN, q15UpdsStatsN = ti.countQ15Upds(q15.updEntries)
	} else {
		q15UpdsR, q15UpdsStatsR = ti.countQ15TopSumUpds(q15TopSum.remEntries)
		q15UpdsN, q15UpdsStatsN = ti.countQ15TopSumUpds(q15TopSum.updEntries)
	}
	nUpdsR += q15UpdsR
	nUpdsN += q15UpdsN
	nUpdsStats += q15UpdsStatsN + q15UpdsStatsR
	if q15UpdsR > 0 || q15UpdsN > 0 {
		nBlockUpds++
	}
	return
}

func (ti SingleTableInfo) getNUpdObjs(q3 SingleQ3, q5 SingleQ5, q14 SingleQ14, q15 SingleQ15, q15TopSum SingleQ15TopSum, q18 SingleQ18) (nUpds, nUpdsStats, nBlockUpds int) {
	nUpds += q3.totalUpds
	if q3.totalUpds > 0 {
		nBlockUpds++
	}
	q5Rem, q5Add := *q5.remValue, *q5.updValue
	if q5Rem != 0 {
		nUpds += 1
	}
	if q5Add != 0 {
		nUpds += 1
	}
	if q5Rem != 0 || q5Add != 0 {
		nBlockUpds++
	}
	nUpds += len(q14.mapTotal)
	if len(q14.mapTotal) > 0 {
		nBlockUpds++
	}
	nUpds += ti.countQ18Upds(q18.remQuantity)
	nUpds += ti.countQ18Upds(q18.updQuantity)
	if q18.remQuantity >= 312 || q18.updQuantity >= 312 {
		nBlockUpds++
	}
	nUpdsStats = nUpds
	var q15Upds, q15UpdsStats int
	if !useTopSum {
		q15Upds, q15UpdsStats = ti.countQ15Upds(q15.updEntries)
	} else {
		q15Upds, q15UpdsStats = ti.countQ15TopSumUpds(q15TopSum.diffEntries)
	}
	nUpds += q15Upds
	nUpdsStats += q15UpdsStats
	if q15Upds > 0 {
		nBlockUpds++
	}
	return
}

func (ti SingleTableInfo) countQ15TopSumUpds(diffEntries map[int16]map[int8]map[int32]float64) (nUpds, nPosUpd int) {
	if !useTopKAll {
		for _, monthMap := range diffEntries {
			for _, suppMap := range monthMap {
				nUpds += len(suppMap)
			}
		}
		return nUpds, nUpds
	} else {
		var suppMapValues map[int32]*float64
		foundInc, foundDec := 0, 0
		for year, monthMap := range diffEntries {
			for month, suppMap := range monthMap {
				if len(suppMap) == 1 {
					nUpds++
					nPosUpd++
				} else {
					suppMapValues = q15Map[year][month]
					for _, value := range suppMapValues {
						if *value > 0 {
							foundInc++
						} else {
							foundDec++
						}
					}
					nPosUpd += foundInc + foundDec
					if foundInc > 0 {
						nUpds++
						foundInc = 0
					}
					if foundDec > 0 {
						nUpds++
						foundDec = 0
					}
				}
			}
		}
	}
	return
}

func (ti SingleTableInfo) countQ15Upds(updEntries map[int16]map[int8]map[int32]struct{}) (nUpds, nPosUpd int) {
	if !useTopKAll {
		for _, monthMap := range updEntries {
			for _, suppMap := range monthMap {
				nUpds += len(suppMap)
			}
		}
		return nUpds, nUpds
	} else {
		var suppMapValues map[int32]*float64
		foundA, foundR := 0, 0
		for year, monthMap := range updEntries {
			for month, suppMap := range monthMap {
				if len(suppMap) == 1 {
					nUpds++
					nPosUpd++
				} else {
					suppMapValues = q15Map[year][month]
					for _, value := range suppMapValues {
						if *value > 0 {
							foundA++
						} else {
							foundR++
						}
					}
					nPosUpd += foundA + foundR
					if foundA > 0 {
						nUpds++
						foundA = 0
					}
					if foundR > 0 {
						nUpds++
						foundR = 0
					}
				}
			}
		}
	}
	return
}

func (ti SingleTableInfo) countQ18Upds(value int64) (nUpds int) {
	if value >= 312 {
		nUpds = int(value) - 312 + 1
		if nUpds > 4 {
			return 4
		}
		return
	}
	return 0
}*/

/*
type SingleQueryInfo interface {
	buildUpdInfo(newOrder, remOrder *tpch.Orders, newItems, remItems *tpch.LineItem) SingleQueryInfo
	countUpds() (nUpds, nUpdsStats, nBlockUpds int)
	makeNewUpdArgs(newOrder *tpch.Orders, updN []crdt.UpdateObjectParams, bufN int, indexN int) (newBufN int)
	makeRemUpdArgs(remOrder *tpch.Orders, updR []crdt.UpdateObjectParams, bufR int, indexR int) (newBufR int)
}
*/
/*
func (ti SingleTableInfo) getSingleQ3Upds(remOrder *tpch.Orders, remItems []*tpch.LineItem, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleQ3 {

	//Single segment, but multiple days (days are cumulative, and *possibly?* items may have different days)

	var remMap map[int8]struct{}
	var updMap map[int8]*float64
	updStart, nUpds := int8(0), 0

	if newOrder.O_ORDERDATE.isLowerOrEqual(MAX_DATE_Q3) {
		//Need to do add
		updMap, updStart = ti.q3SingleUpdsCalcHelper(newOrder, newItems)
		nUpds += int(MAX_MONTH_DAY - updStart + 1)
	}
	//Single segment, but multiple days (as days are cumulative).
	if remOrder.O_ORDERDATE.isLowerOrEqual(MAX_DATE_Q3) {
		//Need to do rem
		remMap = ti.q3SingleRemsCalcHelper(remOrder, remItems)
		nUpds += len(remMap)
	}

	return SingleQ3{remMap: remMap, updMap: updMap, updStartPos: updStart, totalUpds: nUpds,
		newSeg: ti.Tables.Customers[newOrder.O_CUSTKEY].C_MKTSEGMENT, oldSeg: ti.Tables.Customers[remOrder.O_CUSTKEY].C_MKTSEGMENT}

}

func (ti SingleTableInfo) q3SingleRemsCalcHelper(order *tpch.Orders, items []*tpch.LineItem) (remMap map[int8]struct{}) {
	remMap = make(map[int8]struct{})
	minDay, j := MIN_MONTH_DAY, int8(0)
	for _, item := range items {
		if item.L_SHIPDATE.IsHigherOrEqual(MIN_DATE_Q3) {
			if item.L_SHIPDATE.MONTH > 3 || item.L_SHIPDATE.YEAR > 1995 {
				//All days
				minDay = 1
			} else {
				minDay = item.L_SHIPDATE.DAY + 1
			}
			//Make a for from minDay to 31 to fill the map
			for j = minDay; j <= MAX_MONTH_DAY; j++ {
				remMap[j] = struct{}{}
			}
		}
	}
	return
}

func (ti SingleTableInfo) q3SingleUpdsCalcHelper(order *tpch.Orders, items []*tpch.LineItem) (updMap map[int8]*float64, dayStart int8) {
	updMap = make(map[int8]*float64)
	i, j, minDay := int8(1), int8(1), int8(1)
	for ; i <= 31; i++ {
		updMap[i] = new(float64)
	}
	dayStart = MAX_MONTH_DAY + 1 //+1 so that the sum on getSingleQ3Upds ends as 0 if no upd is to be done.
	for _, item := range items {
		if item.L_SHIPDATE.IsHigherOrEqual(MIN_DATE_Q3) {
			if item.L_SHIPDATE.MONTH > 3 || item.L_SHIPDATE.YEAR > 1995 {
				//All days
				minDay = 1
			} else {
				minDay = item.L_SHIPDATE.DAY + 1
			}
			//Make a for from minDay to 31 to fill the map
			for j = minDay; j <= MAX_MONTH_DAY; j++ {
				*updMap[j] += item.L_EXTENDEDPRICE * (1.0 - item.L_DISCOUNT)
			}
			if minDay < dayStart {
				dayStart = minDay
			}
		}
	}
	return
}

func (ti SingleTableInfo) getSingleQ5Upds(remOrder *tpch.Orders, remItems []*tpch.LineItem, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleQ5 {
	//One nation (supplier & customer in the same nation). Thus, one decrement and one increment?
	//That if the supplier and nation match.

	remValue := new(float64)
	*remValue -= ti.q5SingleUpdsCalcHelper(remOrder, remItems)

	updValue := new(float64)
	*updValue += ti.q5SingleUpdsCalcHelper(newOrder, newItems)

	return SingleQ5{remValue: remValue, updValue: updValue}
}

func (ti SingleTableInfo) q5SingleUpdsCalcHelper(order *tpch.Orders, items []*tpch.LineItem) (value float64) {
	orderNationKey := ti.Tables.Customers[order.O_CUSTKEY].C_NATIONKEY
	var suppNationKey int8
	for _, lineItem := range items {
		suppNationKey = ti.Tables.Suppliers[lineItem.L_SUPPKEY].S_NATIONKEY
		if suppNationKey == orderNationKey {
			value += lineItem.L_EXTENDEDPRICE * (1 - lineItem.L_DISCOUNT)
		}
	}
	return
}

func (ti SingleTableInfo) getSingleLocalQ14Upds(remOrder *tpch.Orders, remItems []*tpch.LineItem, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleLocalQ14 {
	mapPromoUpd, mapTotalUpd, mapPromoRem, mapTotalRem := make(map[string]*float64), make(map[string]*float64),
		make(map[string]*float64), make(map[string]*float64)

	ti.q14SingleUpdsCalcHelper(-1, remOrder, remItems, mapPromoRem, mapTotalRem, ti.Tables.PromoParts)
	ti.q14SingleUpdsCalcHelper(1, newOrder, newItems, mapPromoUpd, mapTotalUpd, ti.Tables.PromoParts)

	return SingleLocalQ14{mapPromoRem: mapPromoRem, mapTotalRem: mapTotalRem, mapPromoUpd: mapPromoUpd, mapTotalUpd: mapTotalUpd}
}

func (ti SingleTableInfo) getSingleQ14Upds(remOrder *tpch.Orders, remItems []*tpch.LineItem, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleQ14 {
	//May have multiple updates, as not all items in an order may have been shipped in the same date
	mapPromo, mapTotal := make(map[string]*float64), make(map[string]*float64)

	ti.q14SingleUpdsCalcHelper(-1, remOrder, remItems, mapPromo, mapTotal, ti.Tables.PromoParts)
	ti.q14SingleUpdsCalcHelper(1, newOrder, newItems, mapPromo, mapTotal, ti.Tables.PromoParts)

	return SingleQ14{mapPromo: mapPromo, mapTotal: mapTotal}
}

func (ti SingleTableInfo) q14SingleUpdsCalcHelper(multiplier float64, order *tpch.Orders, items []*tpch.LineItem,
	mapPromo map[string]*float64, mapTotal map[string]*float64, inPromo map[int32]struct{}) {

	var year int16
	revenue := 0.0
	date := ""
	for _, lineItem := range items {
		year = lineItem.L_SHIPDATE.YEAR
		if year >= 1993 && year <= 1997 {
			revenue = multiplier * lineItem.L_EXTENDEDPRICE * (1.0 - lineItem.L_DISCOUNT)
			date = strconv.FormatInt(int64(year), 10) + strconv.FormatInt(int64(lineItem.L_SHIPDATE.MONTH), 10)
			totalValue, has := mapTotal[date]
			if !has {
				totalValue = new(float64)
				mapTotal[date] = totalValue
				mapPromo[date] = new(float64)
			}
			if _, has := inPromo[lineItem.L_PARTKEY]; has {
				*mapPromo[date] += revenue
			}
			*mapTotal[date] += revenue
		}
	}
}

func (ti SingleTableInfo) getSingleLocalQ15TopSumUpds(remOrder *tpch.Orders, remItems []*tpch.LineItem, newOrder *tpch.Orders, newItems []*tpch.LineItem,
	remKey, updKey int8) SingleLocalQ15TopSum {
	remEntries, updEntries := createQ15TopSumEntriesMap(), createQ15TopSumEntriesMap()
	q15TopSumUpdsRemCalcHelper(remItems, q15LocalMap[remKey], remEntries)
	q15TopSumUpdsNewCalcHelper(newItems, q15LocalMap[updKey], updEntries)
	return SingleLocalQ15TopSum{remEntries: remEntries, updEntries: updEntries}
}

func (ti SingleTableInfo) getSingleQ15TopSumUpds(remOrder *tpch.Orders, remItems []*tpch.LineItem, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleQ15TopSum {

	diffEntries := createQ15TopSumEntriesMap()
	q15TopSumUpdsRemCalcHelper(remItems, q15Map, diffEntries)
	q15TopSumUpdsNewCalcHelper(newItems, q15Map, diffEntries)

	return SingleQ15TopSum{diffEntries: diffEntries}
}

func (ti SingleTableInfo) getSingleLocalQ15Upds(remOrder *tpch.Orders, remItems []*tpch.LineItem, newOrder *tpch.Orders, newItems []*tpch.LineItem,
	remKey, updKey int8) SingleLocalQ15 {
	remEntries, updEntries := createQ15EntriesMap(), createQ15EntriesMap()
	q15UpdsRemCalcHelper(remItems, q15LocalMap[remKey], remEntries)
	q15UpdsNewCalcHelper(newItems, q15LocalMap[updKey], updEntries)
	return SingleLocalQ15{remEntries: remEntries, updEntries: updEntries}
}

func (ti SingleTableInfo) getSingleQ15Upds(remOrder *tpch.Orders, remItems []*tpch.LineItem, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleQ15 {
	//Different items may have different suppliers... thus, multiple updates.
	//Also shipdates are potencially different.

	updEntries := createQ15EntriesMap()
	q15UpdsRemCalcHelper(remItems, q15Map, updEntries)
	q15UpdsNewCalcHelper(newItems, q15Map, updEntries)

	return SingleQ15{updEntries: updEntries}
}

func (ti SingleTableInfo) getSingleQ18Upds(remOrder *tpch.Orders, remItems []*tpch.LineItem, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleQ18 {

	//Single order -> single customer. However, it may have to potencially update the 4 quantity indexes
	remQuantity, updQuantity := int64(0), int64(0)
	for _, item := range remItems {
		remQuantity += int64(item.L_QUANTITY)
	}
	for _, item := range newItems {
		updQuantity += int64(item.L_QUANTITY)
	}

	return SingleQ18{remQuantity: remQuantity, updQuantity: updQuantity}
}

func (ti SingleTableInfo) makeSingleQ3UpdsArgsRems(q3Info SingleQ3, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams,
	bufI, bucketI int) (newBufI int) {

	var keyArgs crdt.KeyParams
	//Rems
	if len(q3Info.remMap) > 0 {
		remSegKey := SEGM_DELAY + q3Info.oldSeg
		for day := range q3Info.remMap {
			keyArgs = crdt.KeyParams{
				Key:      remSegKey + strconv.FormatInt(int64(day), 10),
				CrdtType: proto.CRDTType_TOPK_RMV,
				Bucket:   buckets[bucketI],
			}
			//useTopKAll not relevant as it is only one order
			var currUpd crdt.UpdateArguments = crdt.TopKRemove{Id: remOrder.O_ORDERKEY}
			buf[bufI] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
			bufI++
		}
	}
	return bufI
}

func (ti SingleTableInfo) makeSingleQ3UpdsArgsNews(q3Info SingleQ3, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams,
	bufI, bucketI int) (newBufI int) {

	var keyArgs crdt.KeyParams
	//Adds. Due to the date restriction, there may be no adds.
	if q3Info.updStartPos != 0 && q3Info.updStartPos <= MAX_MONTH_DAY {
		updSegKey := SEGM_DELAY + q3Info.newSeg
		for day := q3Info.updStartPos; day <= MAX_MONTH_DAY; day++ {
			keyArgs = crdt.KeyParams{
				Key:      updSegKey + strconv.FormatInt(int64(day), 10),
				CrdtType: proto.CRDTType_TOPK_RMV,
				Bucket:   buckets[bucketI],
			}
			var currUpd crdt.UpdateArguments
			if INDEX_WITH_FULL_DATA {
				currUpd = crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: newOrder.O_ORDERKEY, Score: int32(*q3Info.updMap[day]), Data: packQ3IndexExtraData(newOrder)}}
			} else {
				currUpd = crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: newOrder.O_ORDERKEY, Score: int32(*q3Info.updMap[day])}}
			}
			buf[bufI] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
			bufI++
		}
	}
	return bufI
}

func (ti SingleTableInfo) makeSingleQ3UpdArgs(q3Info SingleQ3, newOrder *tpch.Orders, remOrder *tpch.Orders,
	buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {

	bufI = ti.makeSingleQ3UpdsArgsRems(q3Info, remOrder, buf, bufI, bucketI)
	bufI = ti.makeSingleQ3UpdsArgsNews(q3Info, newOrder, buf, bufI, bucketI)

	return bufI
}

func (ti SingleTableInfo) makeSingleQ5UpdArgsHelper(value float64, signal float64, remOrder *tpch.Orders,
	buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {

	if value != 0 {
		remYear, remNation := remOrder.O_ORDERDATE.YEAR, ti.Tables.Nations[ti.Tables.Customers[remOrder.O_CUSTKEY].C_NATIONKEY]
		embMapUpd := crdt.EmbMapUpdate{Key: remNation.N_NAME, Upd: crdt.Increment{Change: int32(signal * value)}}
		var args crdt.UpdateArguments = embMapUpd
		buf[bufI] = crdt.UpdateObjectParams{
			KeyParams: crdt.KeyParams{Key: NATION_REVENUE + ti.Tables.Regions[remNation.N_REGIONKEY].R_NAME + strconv.FormatInt(int64(remYear-1993), 10),
				CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]},
			UpdateArgs: &args,
		}
		return bufI + 1
	}
	return bufI
}

func (ti SingleTableInfo) makeSingleQ5UpdArgs(q5Info SingleQ5, newOrder *tpch.Orders, remOrder *tpch.Orders,
	buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {

	bufI = ti.makeSingleQ5UpdArgsHelper(*q5Info.remValue, -1.0, remOrder, buf, bufI, bucketI)
	bufI = ti.makeSingleQ5UpdArgsHelper(*q5Info.updValue, 1.0, newOrder, buf, bufI, bucketI)

	return bufI
}

func (ti SingleTableInfo) makeSingleQ14UpdArgs(q14Info SingleQ14, newOrder *tpch.Orders, remOrder *tpch.Orders,
	buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {

	for key, totalP := range q14Info.mapTotal {
		promo := *q14Info.mapPromo[key]
		var currUpd crdt.UpdateArguments = crdt.AddMultipleValue{
			SumValue: int64(100.0 * promo),
			NAdds:    int64(*totalP),
		}
		buf[bufI] = crdt.UpdateObjectParams{
			KeyParams:  crdt.KeyParams{Key: PROMO_PERCENTAGE + key, CrdtType: proto.CRDTType_AVG, Bucket: buckets[bucketI]},
			UpdateArgs: &currUpd,
		}
		bufI++
	}

	return bufI
}

func (ti SingleTableInfo) makeSingleLocalQ15TopSumUpdArgs(q15Info SingleQ15TopSum, newOrder *tpch.Orders, remOrder *tpch.Orders,
	buf []crdt.UpdateObjectParams, bufI int, bucketI int, regKey int8) (newBufI int) {

	newBufI, _ = makeQ15TopSumIndexUpdsDeletesHelper(q15LocalMap[regKey], q15Info.diffEntries, bucketI, buf, bufI)
	return newBufI
}

func (ti SingleTableInfo) makeSingleQ15TopSumUpdArgs(q15Info SingleQ15TopSum, newOrder *tpch.Orders, remOrder *tpch.Orders,
	buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {

	newBufI, _ = makeQ15TopSumIndexUpdsDeletesHelper(q15Map, q15Info.diffEntries, INDEX_BKT, buf, bufI)
	return newBufI
}

func (ti SingleTableInfo) makeSingleLocalQ15UpdArgs(q15Info SingleQ15, newOrder *tpch.Orders, remOrder *tpch.Orders,
	buf []crdt.UpdateObjectParams, bufI int, bucketI int, regKey int8) (newBufI int) {

	newBufI, _ = makeQ15IndexUpdsDeletesHelper(q15LocalMap[regKey], q15Info.updEntries, bucketI, buf, bufI)
	return newBufI
}

func (ti SingleTableInfo) makeSingleQ15UpdArgs(q15Info SingleQ15, newOrder *tpch.Orders, remOrder *tpch.Orders,
	buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {

	newBufI, _ = makeQ15IndexUpdsDeletesHelper(q15Map, q15Info.updEntries, INDEX_BKT, buf, bufI)
	return newBufI
}

func (ti SingleTableInfo) makeSingleQ18UpdsArgsRems(q18Info SingleQ18, remOrder *tpch.Orders,
	buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {

	remQ := q18Info.remQuantity
	var keyArgs crdt.KeyParams

	if remQ >= 312 {
		if remQ > 315 {
			remQ = 315
		}
		for i := int64(312); i <= remQ; i++ {
			keyArgs = crdt.KeyParams{
				Key:      LARGE_ORDERS + strconv.FormatInt(i, 10),
				CrdtType: proto.CRDTType_TOPK_RMV,
				Bucket:   buckets[bucketI],
			}
			//useTopKAll is irrelevant since it is only 1 order
			var currUpd crdt.UpdateArguments = crdt.TopKRemove{Id: remOrder.O_ORDERKEY}
			buf[bufI] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
			bufI++
		}
	}
	return bufI
}

func (ti SingleTableInfo) makeSingleQ18UpdsArgsNews(q18Info SingleQ18, newOrder *tpch.Orders,
	buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {

	updQ := q18Info.updQuantity
	var keyArgs crdt.KeyParams

	if updQ >= 312 {
		//fmt.Println("[Q18]Upd quantity is actually >= 312", updQ, newOrder.O_ORDERKEY)
		if updQ > 315 {
			updQ = 315
		}
		for i := int64(312); i <= updQ; i++ {
			keyArgs = crdt.KeyParams{
				Key:      LARGE_ORDERS + strconv.FormatInt(i, 10),
				CrdtType: proto.CRDTType_TOPK_RMV,
				Bucket:   buckets[bucketI],
			}
			//useTopKAll is irrelevant since it is only 1 order
			var currUpd crdt.UpdateArguments
			if !INDEX_WITH_FULL_DATA {
				currUpd = crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: newOrder.O_ORDERKEY, Score: int32(q18Info.updQuantity)}}
			} else {
				currUpd = crdt.TopKAdd{TopKScore: crdt.TopKScore{
					Id:    newOrder.O_ORDERKEY,
					Score: int32(q18Info.updQuantity),
					Data:  packQ18IndexExtraData(newOrder, ti.Tables.Customers[newOrder.O_CUSTKEY])}}
			}
			buf[bufI] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
			bufI++
		}
	}
	return bufI
}

func (ti SingleTableInfo) makeSingleQ18UpdArgs(q18Info SingleQ18, newOrder *tpch.Orders, remOrder *tpch.Orders,
	buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {

	bufI = ti.makeSingleQ18UpdsArgsRems(q18Info, remOrder, buf, bufI, bucketI)
	bufI = ti.makeSingleQ18UpdsArgsNews(q18Info, remOrder, buf, bufI, bucketI)

	return bufI
}*/

func doMixedStatsInterval() {
	i := 1
	for {
		newBoolSlice := make([]bool, TEST_ROUTINES)
		for i := range newBoolSlice {
			newBoolSlice[i] = true
		}
		time.Sleep(statisticsInterval * time.Millisecond)
		collectUpdStats = true
		collectUpdStatsComm = []bool{true, true, true, true, true}
		collectQueryStats = newBoolSlice
		fmt.Println("Time elapsed (aproximately):", time.Duration(i)*statisticsInterval, "ms.")
		i++
	}
}

// Returns the stats in format [time][nclients] and prepares headers.
func mixStatsFileHelper(stats []MixClientResult) (qStatsPerPart [][]QueryStats, uStatsPerPart [][]UpdateStats, header []string) {
	qStatsPerPart, uStatsPerPart = convertMixStats(stats)

	//Cutting out first entry as "warmup"
	qStatsPerPart, uStatsPerPart = qStatsPerPart[1:], uStatsPerPart[1:]

	var latencyString []string

	if LATENCY_MODE == AVG_OP {
		latencyString = []string{"Average latency (ms)(AO)", "Average latency (ms)(AOw/I)"}
	} else if LATENCY_MODE == AVG_BATCH {
		latencyString = []string{"Average latency (ms)(AB)", "Average latency (ms)(AO)", "Average latency (ms)(AOw/I)"}
	} else { //PER_OP
		latencyString = []string{"AvgQ latency", "AvgU latency", "AvgAll latency (ms)", "Avg latency (ms)(AO)", "Average latency (ms)(AOw/I)"}
	}

	header = append([]string{"Total time", "Section time", "Queries cycles", "Queries", "Reads", "Query cycles/s", "Query/s", "Read/s",
		"Query txns", "Query txns/s", "Updates", "Updates/s", "New upds", "Del upds", "Index upds", "Update blocks", "Update blocks/s",
		"New+del upds", "New+del upd/s", "Update txns", "Update txns/s", "Ops_all", "Ops_all/s", "Ops", "Ops/s", "Txns", "Txn/s"}, latencyString...)

	return
}

// TODO: One day improve the statistics. Namely I need an auxiliary method on creating the slices of data/finalData.
// And also for the intermediate calculations.
func writeMixStatsFile(stats []MixClientResult) {
	//I should write the total, avg, best and worse for each part.
	//Also write the total, avg, best and worse for the "final"
	//TODO: I think I need to prepare the final part before sorting.
	//TODO: Maybe I should write all clients' statistics too. In case later I need to access something.
	qStatsPerPart, uStatsPerPart, header := mixStatsFileHelper(stats)
	totalQClientStats, totalUClientStats := sumAndSortPartStats(qStatsPerPart, uStatsPerPart)
	for i := range qStatsPerPart {
		qStatsPerPart[i], uStatsPerPart[i] = sortByOpsSecond(qStatsPerPart[i], uStatsPerPart[i])
	}

	writeMixTotalStatsFile(stats, qStatsPerPart, uStatsPerPart, header) //This one already calls the avg one
	writeMixExtraStatsFile(stats, qStatsPerPart, uStatsPerPart, totalQClientStats, totalUClientStats, header, STAT_MIN)
	writeMixExtraStatsFile(stats, qStatsPerPart, uStatsPerPart, totalQClientStats, totalUClientStats, header, STAT_MAX)
	writeMixExtraStatsFile(stats, qStatsPerPart, uStatsPerPart, totalQClientStats, totalUClientStats, header, STAT_MEAN)
	writeMixRawStatsFile(totalQClientStats, totalUClientStats, header)
	writeNEntriesFile(stats)
}

// This is only used to help sort the query and update results by order of ops/s.
type OpClientIdPair struct {
	opsPerSecond float64
	clientID     int
}

// Returns an entry per client, with clients sorted by increasing ops/s
func sumAndSortPartStats(qStatsPerPart [][]QueryStats, uStatsPerPart [][]UpdateStats) (sumQ []QueryStats, sumU []UpdateStats) {
	sumQ, sumU = make([]QueryStats, TEST_ROUTINES), make([]UpdateStats, TEST_ROUTINES)

	for i, partStats := range qStatsPerPart {
		for j, clientQStat := range partStats {
			sumQ[j].nQueries += clientQStat.nQueries
			sumQ[j].nReads += clientQStat.nReads
			sumQ[j].timeSpent += clientQStat.timeSpent
			sumQ[j].latency += clientQStat.latency
			sumQ[j].nTxns += clientQStat.nTxns
		}
		for j, clientUStat := range uStatsPerPart[i] {
			sumU[j].nNews += clientUStat.nNews
			sumU[j].nDels += clientUStat.nDels
			sumU[j].nIndex += clientUStat.nIndex
			sumU[j].nTxns += clientUStat.nTxns
			sumU[j].nUpdBlocks += clientUStat.nUpdBlocks
		}
	}

	return sortByOpsSecond(sumQ, sumU)
}

func sortByOpsSecond(qStats []QueryStats, uStats []UpdateStats) (sortedQStats []QueryStats, sortedUStats []UpdateStats) {
	opSlice := make([]OpClientIdPair, TEST_ROUTINES)
	for j := range opSlice {
		opSlice[j].clientID, opSlice[j].opsPerSecond = j, float64(qStats[j].nQueries+uStats[j].nNews+uStats[j].nDels+uStats[j].nIndex)/float64(qStats[j].timeSpent)
	}
	sort.Slice(opSlice, func(a, b int) bool { return opSlice[a].opsPerSecond < opSlice[b].opsPerSecond })
	/*fmt.Println("[CQU]SortByOpsS")
	fmt.Print("[")
	for _, op := range opSlice {
		fmt.Printf("(%d, %.5f), ", op.clientID, op.opsPerSecond)
	}
	fmt.Println("]")*/
	sortedQStats, sortedUStats = make([]QueryStats, len(qStats)), make([]UpdateStats, len(uStats))
	for i, opIdPair := range opSlice {
		sortedQStats[i], sortedUStats[i] = qStats[opIdPair.clientID], uStats[opIdPair.clientID]
	}
	/*
		fmt.Println("Sorted qStats (sorted by opsS, showcasing nQueries):")
		fmt.Print("[")
		for _, qStat := range sortedQStats {
			fmt.Printf("%d, ", qStat.nQueries)
		}
		fmt.Println("]")
	*/
	return sortedQStats, sortedUStats
}

func writeNEntriesFile(stats []MixClientResult) {
	header := []string{"Total time", "Queries", "Reads", "nEntries", "Queries/s", "Reads/s", "nEntries/s", "nEntries/query"}
	totalTime, totalQueries, totalReads, totalEntries := 0.0, 0.0, 0.0, 0.0

	for _, stat := range stats {
		totalTime += stat.duration
		totalQueries += stat.nQueries
		totalReads += stat.nReads
		totalEntries += stat.nEntries
	}

	totalTime /= float64(len(stats))
	totalQueries /= float64(len(stats))
	totalReads /= float64(len(stats))
	totalEntries /= float64(len(stats))

	data := [][]string{{strconv.FormatFloat(totalTime, 'f', 0, 64), strconv.FormatFloat(totalQueries, 'f', 0, 64),
		strconv.FormatFloat(totalReads, 'f', 0, 64), strconv.FormatFloat(totalEntries, 'f', 0, 64), strconv.FormatFloat((totalQueries*1000)/totalTime, 'f', 10, 64),
		strconv.FormatFloat((totalReads*1000)/totalTime, 'f', 10, 64), strconv.FormatFloat((totalEntries*1000)/totalTime, 'f', 10, 64),
		strconv.FormatFloat((totalEntries)/totalQueries, 'f', 10, 64)}}

	writeDataToFile("nEntries", header, data)
	//data := [][]string{{}}
	/*
		file := getStatsFileToWrite("nEntries.txt")
		if file == nil {
			return
		}
		defer file.Close()
		writer := csv.NewWriter(file)
		writer.Comma = ';'
		defer writer.Flush()

		writer.Write(header)
		for _, line := range data {
			writer.Write(line)
		}
		fmt.Println("Mix statistics saved successfully to " + file.Name())
	*/

}

func writeMixTotalStatsFile(stats []MixClientResult, qStatsPerPart [][]QueryStats, uStatsPerPart [][]UpdateStats, header []string) {
	totalData := make([][]string, len(qStatsPerPart)+1) //space for final data as well

	partQueries, partReads, partTime, partNews, partDels, partIndexes, partQTime, partUTime := 0, 0, int64(0), 0, 0, 0, int64(0), int64(0)
	partQTxns, partUTxns, partUBlocks, totalUBlocks := 0, 0, 0, 0
	totalQueries, totalReads, totalTime, totalNews, totalDels, totalIndexes, totalQTime, totalUTime, totalQTxns, totalUTxns := 0, 0, int64(0), 0, 0, 0, int64(0), int64(0), 0, 0

	//Use these later for average. These ones are by part (i.e., sum of all clients)
	sumQStats, sumUStats := make([]QueryStats, len(qStatsPerPart)+1), make([]UpdateStats, len(qStatsPerPart)+1)

	for i, partStats := range qStatsPerPart {
		for _, clientQStat := range partStats {
			partQueries += clientQStat.nQueries
			partReads += clientQStat.nReads
			partTime += clientQStat.timeSpent
			partQTime += clientQStat.latency
			partQTxns += clientQStat.nTxns
		}
		for _, clientUStat := range uStatsPerPart[i] {
			partNews += clientUStat.nNews
			partDels += clientUStat.nDels
			partIndexes += clientUStat.nIndex
			partUTime += clientUStat.latency
			partUTxns += clientUStat.nTxns
			partUBlocks += clientUStat.nUpdBlocks
		}
		sumQStats[i] = QueryStats{nQueries: partQueries, nReads: partReads, timeSpent: partTime, latency: partQTime, nTxns: partQTxns}
		sumUStats[i] = UpdateStats{nNews: partNews, nDels: partDels, nIndex: partIndexes, latency: partUTime, nTxns: partUTxns, nUpdBlocks: partUBlocks}

		partTime /= int64(TEST_ROUTINES)

		totalQueries += partQueries
		totalReads += partReads
		totalTime += partTime
		totalNews += partNews
		totalDels += partDels
		totalIndexes += partIndexes
		totalQTime += partQTime
		totalUTime += partUTime
		totalQTxns += partQTxns
		totalUTxns += partUTxns
		totalUBlocks += partUBlocks

		totalData[i] = calculateMixStats(partQueries, partReads, partQTxns, partNews, partDels, partIndexes, partUTxns, partUBlocks,
			totalTime, partTime, partQTime, partUTime, !ONE_CLIENT_STATISTICS)

		partQueries, partReads, partTime, partNews, partDels, partIndexes, partQTime, partQTxns, partUTime, partUTxns, partUBlocks = 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
	}

	totalData[len(totalData)-1] = calculateMixStats(totalQueries, totalReads, totalQTxns, totalNews, totalDels, totalIndexes, totalUTxns, totalUBlocks,
		totalTime, totalTime, totalQTime, totalUTime, !ONE_CLIENT_STATISTICS)

	writeDataToFile("mixStats", header, totalData)
	sumQStats[len(sumQStats)-1] = QueryStats{nQueries: totalQueries, nReads: totalReads, timeSpent: totalTime, latency: totalQTime, nTxns: totalQTxns}
	sumUStats[len(sumUStats)-1] = UpdateStats{nNews: totalNews, nDels: totalDels, nIndex: totalIndexes, latency: partUTime, nTxns: totalUTxns, nUpdBlocks: totalUBlocks}
	writeMixAvgStatsFile(sumQStats, sumUStats, header)
}

func writeMixRawStatsFile(totalQStats []QueryStats, totalUStats []UpdateStats, header []string) {
	//Only writes the total of each client.
	totalData := make([][]string, TEST_ROUTINES)

	for i, qStat := range totalQStats {
		uStat := totalUStats[i]
		totalData[i] = calculateMixStats(qStat.nQueries, qStat.nReads, qStat.nTxns, uStat.nNews, uStat.nDels, uStat.nIndex,
			uStat.nTxns, uStat.nUpdBlocks, qStat.timeSpent, qStat.timeSpent, qStat.latency, uStat.latency, ONE_CLIENT_STATISTICS)
	}

	writeDataToFile("raw/rawStats", header, totalData)
}

// The stats are an array of parts (i.e., each position is the sum of all clients)
// Thus, we need to divide many totals by the number of clients
func writeMixAvgStatsFile(sumQStats []QueryStats, sumUStats []UpdateStats, header []string) {
	data := make([][]string, len(sumQStats)+1)

	//Calculating the averages
	for i, qStat := range sumQStats {
		uStat := sumUStats[i]
		qStat.nQueries /= TEST_ROUTINES
		qStat.nReads /= TEST_ROUTINES
		qStat.latency /= int64(TEST_ROUTINES)
		qStat.nTxns /= TEST_ROUTINES
		uStat.nNews /= TEST_ROUTINES
		uStat.nDels /= TEST_ROUTINES
		uStat.nIndex /= TEST_ROUTINES
		uStat.latency /= int64(TEST_ROUTINES)
		uStat.nTxns /= TEST_ROUTINES
		uStat.nUpdBlocks /= TEST_ROUTINES
		if i < len(sumQStats)-1 { //On the total (last one), we do not have to divide by TEST_ROUTINES on timeSpent.
			qStat.timeSpent /= int64(TEST_ROUTINES)
		}
		sumQStats[i], sumUStats[i] = qStat, uStat
	}

	totalTime, pTime := int64(0), int64(0)

	for i, qStat := range sumQStats {
		uStat := sumUStats[i]
		pTime = qStat.timeSpent
		if i < len(sumQStats)-1 { //On the total (last one), no need to sum pTime
			totalTime += pTime
		}

		data[i] = calculateMixStats(qStat.nQueries, qStat.nReads, qStat.nTxns, uStat.nNews, uStat.nDels, uStat.nIndex, uStat.nTxns, uStat.nUpdBlocks,
			totalTime, pTime, qStat.latency, uStat.latency, ONE_CLIENT_STATISTICS)
	}

	//Note: the last position of sumQStats and sumUStats is already the total (thus it is already done in the cycle above)
	writeDataToFile("avgStats", header, data)
}

// Writes min, max and mean.
func writeMixExtraStatsFile(stats []MixClientResult, qStatsPerPart [][]QueryStats, uStatsPerPart [][]UpdateStats,
	totalQClientStats []QueryStats, totalUClientStats []UpdateStats, header []string, statsType int) {
	data := make([][]string, len(qStatsPerPart)+1) //space for final data as well

	totalTime := int64(0)
	for i, qPartStats := range qStatsPerPart {
		uPartStats := uStatsPerPart[i]
		posQ, posU := getPosOfStat(qPartStats, uPartStats, statsType)
		qStat, uStat := qPartStats[posQ], uPartStats[posU]
		totalTime += qStat.timeSpent

		data[i] = calculateMixStats(qStat.nQueries, qStat.nReads, qStat.nTxns, uStat.nNews, uStat.nDels, uStat.nIndex, uStat.nTxns, uStat.nUpdBlocks,
			totalTime, qStat.timeSpent, qStat.latency, uStat.latency, ONE_CLIENT_STATISTICS)
	}

	posQ, posU := getPosOfStat(totalQClientStats, totalUClientStats, statsType)
	tQStats, tUStats := totalQClientStats[posQ], totalUClientStats[posU]

	data[len(data)-1] = calculateMixStats(tQStats.nQueries, tQStats.nReads, tQStats.nTxns, tUStats.nNews, tUStats.nDels, tUStats.nIndex, tUStats.nTxns, tUStats.nUpdBlocks,
		tQStats.timeSpent, tQStats.timeSpent, tQStats.latency, tUStats.latency, ONE_CLIENT_STATISTICS)

	switch statsType {
	case STAT_MIN:
		writeDataToFile("minStats", header, data)
	case STAT_MAX:
		writeDataToFile("maxStats", header, data)
	case STAT_MEAN:
		writeDataToFile("meanStats", header, data)
	}
}

func calculateMixStats(nQueries, nReads, nQTxns, nNews, nDels, nIndex, nUTxns, nUpdBlocks int, totalTime, partTime, qLatency, uLatency int64, isSingleClient bool) (data []string) {
	queryCycles, nUpds := nQueries/len(queryFuncs), nNews+nDels+nIndex
	nClients := int64(1)
	if !isSingleClient {
		nClients = int64(TEST_ROUTINES)
	}

	queryCycleS, queryS := (float64(queryCycles)/float64(partTime))*1000, (float64(nQueries)/float64(partTime))*1000
	updS, readS := (float64(nUpds)/float64(partTime))*1000, (float64(nReads)/float64(partTime))*1000
	qTxnsS, uTxnsS := (float64(nQTxns)/float64(partTime))*1000, (float64(nUTxns)/float64(partTime))*1000
	uBlocksS := (float64(nUpdBlocks) / float64(partTime)) * 1000
	latencyPerOp := float64(partTime*nClients) / float64(nQueries+nUpds)
	latencyPerOpNoIndex := float64(partTime*nClients) / float64(nQueries+nNews+nDels)
	latency := getStatsLatency(partTime, qLatency, uLatency, nClients, nQueries, nUpds, nQTxns, nUTxns)
	updSNoIndex := (float64(nNews+nDels) / float64(partTime)) * 1000

	data = []string{strconv.FormatInt(totalTime, 10), strconv.FormatInt(partTime, 10),
		strconv.FormatInt(int64(queryCycles), 10), strconv.FormatInt(int64(nQueries), 10), strconv.FormatInt(int64(nReads), 10),
		strconv.FormatFloat(queryCycleS, 'f', 10, 64), strconv.FormatFloat(queryS, 'f', 10, 64),
		strconv.FormatFloat(readS, 'f', 10, 64), strconv.FormatInt(int64(nQTxns), 10), strconv.FormatFloat(qTxnsS, 'f', 10, 64),
		strconv.FormatInt(int64(nUpds), 10), strconv.FormatFloat(updS, 'f', 10, 64),
		strconv.FormatInt(int64(nNews), 10), strconv.FormatInt(int64(nDels), 10), strconv.FormatInt(int64(nIndex), 10),
		strconv.FormatInt(int64(nUpdBlocks), 10), strconv.FormatFloat(uBlocksS, 'f', 10, 64),
		strconv.FormatInt(int64(nNews+nDels), 10), strconv.FormatFloat(updSNoIndex, 'f', 10, 64),
		strconv.FormatInt(int64(nUTxns), 10), strconv.FormatFloat(uTxnsS, 'f', 10, 64),
		strconv.FormatInt(int64(nQueries+nUpds), 10), strconv.FormatFloat(queryS+updS, 'f', 10, 64),
		strconv.FormatInt(int64(nQueries+nNews+nDels), 10), strconv.FormatFloat(queryS+updSNoIndex, 'f', 10, 64),
		strconv.FormatInt(int64(nQTxns+nUTxns), 10), strconv.FormatFloat(qTxnsS+uTxnsS, 'f', 10, 64)}

	if LATENCY_MODE == PER_BATCH {
		qLatency, uLatency := float64(qLatency)/float64(nQTxns), float64(uLatency)/float64(nUTxns)
		data = append(data, strconv.FormatFloat(qLatency, 'f', 10, 64), strconv.FormatFloat(uLatency, 'f', 10, 64), strconv.FormatFloat(latency, 'f', 10, 64))
		//fmt.Printf("Time, qTime, uTime, qTxns, uTxns, qLatency, uLatency: %d %d %d %d %d %f %f\n", totalTime, totalQTime, totalUTime, totalQTxns, totalUTxns, qLatency, uLatency)
	} else if LATENCY_MODE == AVG_BATCH {
		data = append(data, strconv.FormatFloat(latency, 'f', 10, 64))
	}
	//All modes use AVG_OP
	data = append(data, strconv.FormatFloat(latencyPerOp, 'f', 10, 64), strconv.FormatFloat(latencyPerOpNoIndex, 'f', 10, 64))

	return
}

// For min/max/mean/avg situations, nClient must be 1.
func getStatsLatency(partTime, qTime, uTime, nClients int64, nQueries, nUpds, nQueryTxns, nUpdTxns int) float64 {
	if LATENCY_MODE == AVG_OP {
		return float64(partTime*nClients) / float64(nQueries+nUpds)
	} else if LATENCY_MODE == AVG_BATCH {
		return float64(partTime*nClients) / float64(nQueryTxns+nUpdTxns)
	}
	return float64(qTime+uTime) / float64(nQueryTxns+nUpdTxns)
}

// [nClients] -> [time][clients]
func convertMixStats(stats []MixClientResult) (qStats [][]QueryStats, uStats [][]UpdateStats) {
	sizeToUse := int(math.MaxInt32)
	for _, mixStats := range stats {
		if len(mixStats.intermediateResults) < sizeToUse {
			sizeToUse = len(mixStats.intermediateResults)
		}
	}
	qStats, uStats = make([][]QueryStats, sizeToUse), make([][]UpdateStats, sizeToUse)
	var currQSlice []QueryStats
	var currUSlice []UpdateStats

	for i := range qStats {
		currQSlice, currUSlice = make([]QueryStats, len(stats)), make([]UpdateStats, len(stats))
		for j, stat := range stats {
			currQSlice[j], currUSlice[j] = stat.intermediateResults[i], stat.updStats[i]
		}
		qStats[i], uStats[i] = currQSlice, currUSlice
	}

	return
}

// Stats of a time for all clients, sorted from the client with minimum latency to the maximum latency (increasing order)
func getPosOfStat(qStats []QueryStats, uStats []UpdateStats, statsType int) (qPos, uPos int) {
	qPos, uPos = 0, 0
	switch statsType {
	case STAT_MIN:
		return 0, 0
	case STAT_MAX:
		return len(qStats) - 1, len(qStats) - 1
	case STAT_MEAN:
		return len(qStats) / 2, len(qStats) / 2
	}
	return
}

func writeDataToFile(filename string, header []string, data [][]string) {
	file := getStatsFileToWrite(filename)
	if file == nil {
		return
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	writer.Comma = ';'
	defer writer.Flush()

	writer.Write(header)
	for _, line := range data {
		writer.Write(line)
	}
	fmt.Println("Mix statistics saved successfully to " + file.Name())
}

//TODO: If everything works fine, delete this code
/*
func (ti SingleTableInfo) getSpecificIndexesArgs(newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem, upds []crdt.UpdateObjectParams,
	buf int) (newBuf, nIndex, nBlock int) {
	queryInfos := make([]interface{}, len(indexesToUpd))
	for i, queryN := range indexesToUpd {
		queryInfos[i] = ti.genericGetSingleUpds(queryN, newOrder, newItems, remOrder, remItems)
	}
	oldBuf, nIndex, nBlock := buf, 0, 0

	for i, queryN := range indexesToUpd {
		switch queryN {
		case 3:
			buf = ti.makeSingleQ3UpdArgs(queryInfos[i].(SingleQ3), remOrder, newOrder, upds, buf, INDEX_BKT)
		case 5:
			buf = ti.makeSingleQ5UpdArgs(queryInfos[i].(SingleQ5), remOrder, newOrder, upds, buf, INDEX_BKT)
		case 14:
			buf = ti.makeSingleQ14UpdArgs(queryInfos[i].(SingleQ14), remOrder, newOrder, upds, buf, INDEX_BKT)
		case 15:
			if !useTopSum {
				buf = ti.makeSingleQ15UpdArgs(queryInfos[i].(SingleQ15), remOrder, newOrder, upds, buf, INDEX_BKT)
			} else {
				buf = ti.makeSingleQ15TopSumUpdArgs(queryInfos[i].(SingleQ15TopSum), remOrder, newOrder, upds, buf, INDEX_BKT)
			}
		case 18:
			buf = ti.makeSingleQ18UpdArgs(queryInfos[i].(SingleQ18), remOrder, newOrder, upds, buf, INDEX_BKT)
		}
		if buf != oldBuf {
			nBlock++
		}
		nIndex += (buf - oldBuf)
		oldBuf = buf
	}
	return buf, nIndex, nBlock
}

func (ti SingleTableInfo) getSpecificIndexesLocalArgs(newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem, updN, updR []crdt.UpdateObjectParams,
	bufN, bufR, indexN, indexR int) (newBufN, newBufR, nIndex, nBlock int) {
	queryInfos := make([]interface{}, len(indexesToUpd))
	for i, queryN := range indexesToUpd {
		queryInfos[i] = ti.genericGetSingleUpds(queryN, newOrder, newItems, remOrder, remItems)
	}
	oldBufN, oldBufR, nIndex, nBlock := bufN, bufR, 0, 0

	for i, queryN := range indexesToUpd {
		switch queryN {
		case 3:
			q3 := queryInfos[i].(SingleQ3)
			bufN = ti.makeSingleQ3UpdsArgsNews(q3, newOrder, updN, bufN, indexN)
			bufR = ti.makeSingleQ3UpdsArgsRems(q3, remOrder, updR, bufR, indexR)
		case 5:
			q5 := queryInfos[i].(SingleQ5)
			bufN = ti.makeSingleQ5UpdArgsHelper(*q5.updValue, 1.0, newOrder, updN, bufN, indexN)
			bufR = ti.makeSingleQ5UpdArgsHelper(*q5.remValue, -1.0, remOrder, updR, bufR, indexR)
		case 14:
			lq14 := queryInfos[i].(SingleLocalQ14)
			remQ14 := SingleQ14{mapPromo: lq14.mapPromoRem, mapTotal: lq14.mapTotalRem}
			newQ14 := SingleQ14{mapPromo: lq14.mapPromoUpd, mapTotal: lq14.mapTotalUpd}
			bufN = ti.makeSingleQ14UpdArgs(newQ14, remOrder, newOrder, updN, bufN, indexN)
			bufR = ti.makeSingleQ14UpdArgs(remQ14, remOrder, newOrder, updR, bufR, indexR)
		case 15:
			if !useTopSum {
				lq15 := queryInfos[i].(SingleLocalQ15)
				remQ15, newQ15 := SingleQ15{updEntries: lq15.remEntries}, SingleQ15{updEntries: lq15.updEntries}
				bufN = ti.makeSingleLocalQ15UpdArgs(newQ15, remOrder, newOrder, updN, bufN, indexN, int8(indexN-INDEX_BKT))
				bufR = ti.makeSingleLocalQ15UpdArgs(remQ15, remOrder, newOrder, updR, bufR, indexR, int8(indexR-INDEX_BKT))
			} else {
				lq15 := queryInfos[i].(SingleLocalQ15TopSum)
				remQ15, newQ15 := SingleQ15TopSum{diffEntries: lq15.remEntries}, SingleQ15TopSum{diffEntries: lq15.updEntries}
				bufN = ti.makeSingleLocalQ15TopSumUpdArgs(newQ15, remOrder, newOrder, updN, bufN, indexN, int8(indexN-INDEX_BKT))
				bufR = ti.makeSingleLocalQ15TopSumUpdArgs(remQ15, remOrder, newOrder, updR, bufR, indexR, int8(indexR-INDEX_BKT))
			}
		case 18:
			q18 := queryInfos[i].(SingleQ18)
			bufN = ti.makeSingleQ18UpdsArgsNews(q18, newOrder, updN, bufN, indexN)
			bufR = ti.makeSingleQ18UpdsArgsRems(q18, remOrder, updR, bufR, indexR)
		}
		if bufN != oldBufN || bufR != oldBufR {
			nBlock++
		}
		nIndex += (bufN - oldBufN) + (bufR - oldBufR)
		oldBufN, oldBufR = bufN, bufR
	}
	return bufN, bufR, nIndex, nBlock
}

func (ti SingleTableInfo) getSpecificIndexesLocalRedirectArgs(newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem, upd []crdt.UpdateObjectParams,
	buf, indexN, indexR int) (newBuf, nIndex, nBlock int) {
	queryInfos := make([]interface{}, len(indexesToUpd))
	for i, queryN := range indexesToUpd {
		queryInfos[i] = ti.genericGetSingleUpds(queryN, newOrder, newItems, remOrder, remItems)
	}
	oldBuf, nIndex, nBlock := buf, 0, 0

	for i, queryN := range indexesToUpd {
		switch queryN {
		case 3:
			q3 := queryInfos[i].(SingleQ3)
			buf = ti.makeSingleQ3UpdsArgsNews(q3, newOrder, upd, buf, indexN)
			buf = ti.makeSingleQ3UpdsArgsRems(q3, remOrder, upd, buf, indexR)
		case 5:
			q5 := queryInfos[i].(SingleQ5)
			buf = ti.makeSingleQ5UpdArgsHelper(*q5.updValue, 1.0, newOrder, upd, buf, indexN)
			buf = ti.makeSingleQ5UpdArgsHelper(*q5.remValue, -1.0, remOrder, upd, buf, indexR)
		case 14:
			lq14 := queryInfos[i].(SingleLocalQ14)
			remQ14 := SingleQ14{mapPromo: lq14.mapPromoRem, mapTotal: lq14.mapTotalRem}
			newQ14 := SingleQ14{mapPromo: lq14.mapPromoUpd, mapTotal: lq14.mapTotalUpd}
			buf = ti.makeSingleQ14UpdArgs(newQ14, remOrder, newOrder, upd, buf, indexN)
			buf = ti.makeSingleQ14UpdArgs(remQ14, remOrder, newOrder, upd, buf, indexR)
		case 15:
			if !useTopSum {
				lq15 := queryInfos[i].(SingleLocalQ15)
				remQ15, newQ15 := SingleQ15{updEntries: lq15.remEntries}, SingleQ15{updEntries: lq15.updEntries}
				buf = ti.makeSingleLocalQ15UpdArgs(newQ15, remOrder, newOrder, upd, buf, indexN, int8(indexN-INDEX_BKT))
				buf = ti.makeSingleLocalQ15UpdArgs(remQ15, remOrder, newOrder, upd, buf, indexR, int8(indexR-INDEX_BKT))
			} else {
				lq15 := queryInfos[i].(SingleLocalQ15TopSum)
				remQ15, newQ15 := SingleQ15TopSum{diffEntries: lq15.remEntries}, SingleQ15TopSum{diffEntries: lq15.updEntries}
				buf = ti.makeSingleLocalQ15TopSumUpdArgs(newQ15, remOrder, newOrder, upd, buf, indexN, int8(indexN-INDEX_BKT))
				buf = ti.makeSingleLocalQ15TopSumUpdArgs(remQ15, remOrder, newOrder, upd, buf, indexR, int8(indexR-INDEX_BKT))
			}
		case 18:
			q18 := queryInfos[i].(SingleQ18)
			buf = ti.makeSingleQ18UpdsArgsNews(q18, newOrder, upd, buf, indexN)
			buf = ti.makeSingleQ18UpdsArgsRems(q18, remOrder, upd, buf, indexR)
		}
		if buf != oldBuf {
			nBlock++
		}
		nIndex += (buf - oldBuf)
		oldBuf = buf
	}
	return buf, nIndex, nBlock
}

func (ti SingleTableInfo) getSpecificIndexesLocalSameRArgs(newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem, upds []crdt.UpdateObjectParams,
	buf, indexN int) (newBuf, nIndex, nBlock int) {
	queryInfos := make([]interface{}, len(indexesToUpd))
	for i, queryN := range indexesToUpd {
		queryInfos[i] = ti.genericGetSingleUpds(queryN, newOrder, newItems, remOrder, remItems)
	}
	oldBuf, nIndex, nBlock := buf, 0, 0

	for i, queryN := range indexesToUpd {
		switch queryN {
		case 3:
			buf = ti.makeSingleQ3UpdArgs(queryInfos[i].(SingleQ3), remOrder, newOrder, upds, buf, indexN)
		case 5:
			buf = ti.makeSingleQ5UpdArgs(queryInfos[i].(SingleQ5), remOrder, newOrder, upds, buf, indexN)
		case 14:
			lq14 := queryInfos[i].(SingleLocalQ14)
			remQ14 := SingleQ14{mapPromo: lq14.mapPromoRem, mapTotal: lq14.mapTotalRem}
			newQ14 := SingleQ14{mapPromo: lq14.mapPromoUpd, mapTotal: lq14.mapTotalUpd}
			buf = ti.makeSingleQ14UpdArgs(remQ14, remOrder, newOrder, upds, buf, indexN)
			buf = ti.makeSingleQ14UpdArgs(newQ14, remOrder, newOrder, upds, buf, indexN)
		case 15:
			if !useTopSum {
				lq15 := queryInfos[i].(SingleLocalQ15)
				remQ15, newQ15 := SingleQ15{updEntries: lq15.remEntries}, SingleQ15{updEntries: lq15.updEntries}
				buf = ti.makeSingleLocalQ15UpdArgs(newQ15, remOrder, newOrder, upds, buf, indexN, int8(indexN-INDEX_BKT))
				buf = ti.makeSingleLocalQ15UpdArgs(remQ15, remOrder, newOrder, upds, buf, indexN, int8(indexN-INDEX_BKT))
			} else {
				lq15 := queryInfos[i].(SingleLocalQ15TopSum)
				remQ15, newQ15 := SingleQ15TopSum{diffEntries: lq15.remEntries}, SingleQ15TopSum{diffEntries: lq15.updEntries}
				buf = ti.makeSingleLocalQ15TopSumUpdArgs(newQ15, remOrder, newOrder, upds, buf, indexN, int8(indexN-INDEX_BKT))
				buf = ti.makeSingleLocalQ15TopSumUpdArgs(remQ15, remOrder, newOrder, upds, buf, indexN, int8(indexN-INDEX_BKT))
			}
		case 18:
			buf = ti.makeSingleQ18UpdArgs(queryInfos[i].(SingleQ18), remOrder, newOrder, upds, buf, indexN)
		}
		if buf != oldBuf {
			nBlock++
		}
		nIndex += (buf - oldBuf)
		oldBuf = buf
	}
	return buf, nIndex, nBlock
}

func (ti SingleTableInfo) genericGetSingleUpds(queryN int, newOrder *tpch.Orders, newItems []*tpch.LineItem,
	remOrder *tpch.Orders, remItems []*tpch.LineItem) (info interface{}) {

	switch queryN {
	case 3:
		return ti.getSingleQ3Upds(remOrder, remItems, newOrder, newItems)
	case 5:
		return ti.getSingleQ5Upds(remOrder, remItems, newOrder, newItems)
	case 14:
		if isIndexGlobal {
			return ti.getSingleQ14Upds(remOrder, remItems, newOrder, newItems)
		} else {
			return ti.getSingleLocalQ14Upds(remOrder, remItems, newOrder, newItems)
		}
	case 15:
		if isIndexGlobal {
			if !useTopSum {
				return ti.getSingleQ15Upds(remOrder, remItems, newOrder, newItems)
			}
			return ti.getSingleQ15TopSumUpds(remOrder, remItems, newOrder, newItems)
		} else {
			newR, remR := ti.OrderToRegionkey(newOrder), ti.OrderToRegionkey(remOrder)
			if !useTopSum {
				return ti.getSingleLocalQ15Upds(remOrder, remItems, newOrder, newItems, remR, newR)
			}
			return ti.getSingleLocalQ15TopSumUpds(remOrder, remItems, newOrder, newItems, remR, newR)
		}
	case 18:
		if isIndexGlobal {
			return ti.getSingleQ18Upds(remOrder, remItems, newOrder, newItems)
		} else {
			return ti.getSingleQ18Upds(remOrder, remItems, newOrder, newItems)
		}
	}

	return nil
}

func (ti SingleTableInfo) genericSendIndividualIndex(info interface{}, queryN int, newOrder, deletedOrder *tpch.Orders) {
	updS := ti.reusableParams[0] //Doesn't matter which buffer is used

	switch queryN {
	case 3:
		ti.sendIndividualIndex(updS, ti.makeSingleQ3UpdArgs(info.(SingleQ3), deletedOrder, newOrder, updS, 0, INDEX_BKT), ti.indexServer)
	case 5:
		ti.sendIndividualIndex(updS, ti.makeSingleQ5UpdArgs(info.(SingleQ5), deletedOrder, newOrder, updS, 0, INDEX_BKT), ti.indexServer)
	case 14:
		ti.sendIndividualIndex(updS, ti.makeSingleQ14UpdArgs(info.(SingleQ14), deletedOrder, newOrder, updS, 0, INDEX_BKT), ti.indexServer)
	case 15:
		if !useTopSum {
			ti.sendIndividualIndex(updS, ti.makeSingleQ15UpdArgs(info.(SingleQ15), deletedOrder, newOrder, updS, 0, INDEX_BKT), ti.indexServer)
		} else {
			ti.sendIndividualIndex(updS, ti.makeSingleQ15TopSumUpdArgs(info.(SingleQ15TopSum), deletedOrder, newOrder, updS, 0, INDEX_BKT), ti.indexServer)
		}
	case 18:
		ti.sendIndividualIndex(updS, ti.makeSingleQ18UpdArgs(info.(SingleQ18), deletedOrder, newOrder, updS, 0, INDEX_BKT), ti.indexServer)
	}
}

func (ti SingleTableInfo) genericLocalSameRSendIndividualIndex(info interface{}, queryN int, newOrder, deletedOrder *tpch.Orders, indexN int) {
	updS := ti.reusableParams[0] //Doesn't matter which buffer is used
	var serverN int
	if localMode == LOCAL_DIRECT {
		serverN = indexN - INDEX_BKT
	} else {
		serverN = ti.indexServer
	}

	switch queryN {
	case 3:
		ti.sendIndividualIndex(updS, ti.makeSingleQ3UpdArgs(info.(SingleQ3), deletedOrder, newOrder, updS, 0, indexN), serverN)
	case 5:
		ti.sendIndividualIndex(updS, ti.makeSingleQ5UpdArgs(info.(SingleQ5), deletedOrder, newOrder, updS, 0, indexN), serverN)
	case 14:
		lq14 := info.(SingleLocalQ14)
		remQ14 := SingleQ14{mapPromo: lq14.mapPromoRem, mapTotal: lq14.mapTotalRem}
		newQ14 := SingleQ14{mapPromo: lq14.mapPromoUpd, mapTotal: lq14.mapTotalUpd}
		ti.sendIndividualIndex(updS, ti.makeSingleQ14UpdArgs(remQ14, deletedOrder, newOrder, updS, 0, indexN), serverN)
		ti.sendIndividualIndex(updS, ti.makeSingleQ14UpdArgs(newQ14, deletedOrder, newOrder, updS, 0, indexN), serverN)
	case 15:
		if !useTopSum {
			lq15 := info.(SingleLocalQ15)
			remQ15, newQ15 := SingleQ15{updEntries: lq15.remEntries}, SingleQ15{updEntries: lq15.updEntries}
			ti.sendIndividualIndex(updS, ti.makeSingleLocalQ15UpdArgs(remQ15, deletedOrder, newOrder, updS, 0, indexN, int8(indexN-INDEX_BKT)), serverN)
			ti.sendIndividualIndex(updS, ti.makeSingleLocalQ15UpdArgs(newQ15, deletedOrder, newOrder, updS, 0, indexN, int8(indexN-INDEX_BKT)), serverN)
		} else {
			lq15 := info.(SingleLocalQ15TopSum)
			remQ15, newQ15 := SingleQ15TopSum{diffEntries: lq15.remEntries}, SingleQ15TopSum{diffEntries: lq15.updEntries}
			ti.sendIndividualIndex(updS, ti.makeSingleLocalQ15TopSumUpdArgs(remQ15, deletedOrder, newOrder, updS, 0, indexN, int8(indexN-INDEX_BKT)), serverN)
			ti.sendIndividualIndex(updS, ti.makeSingleLocalQ15TopSumUpdArgs(newQ15, deletedOrder, newOrder, updS, 0, indexN, int8(indexN-INDEX_BKT)), serverN)
		}
	case 18:
		ti.sendIndividualIndex(updS, ti.makeSingleQ18UpdArgs(info.(SingleQ18), deletedOrder, newOrder, updS, 0, indexN), serverN)
	}
}

func (ti SingleTableInfo) genericLocalSendIndividualIndex(info interface{}, queryN int, newOrder, deletedOrder *tpch.Orders, indexN, indexR int) {
	updS := ti.reusableParams[0] //Doesn't matter which buffer is used
	var serverN, serverR int
	if localMode == LOCAL_DIRECT {
		serverN, serverR = indexN-INDEX_BKT, indexR-INDEX_BKT
	} else {
		serverN, serverR = ti.indexServer, ti.indexServer
	}

	switch queryN {
	case 3:
		q3 := info.(SingleQ3)
		ti.sendIndividualIndex(updS, ti.makeSingleQ3UpdsArgsNews(q3, newOrder, updS, 0, indexN), serverN)
		ti.sendIndividualIndex(updS, ti.makeSingleQ3UpdsArgsRems(q3, deletedOrder, updS, 0, indexR), serverR)
	case 5:
		q5 := info.(SingleQ5)
		ti.sendIndividualIndex(updS, ti.makeSingleQ5UpdArgsHelper(*q5.updValue, 1.0, newOrder, updS, 0, indexN), serverN)
		ti.sendIndividualIndex(updS, ti.makeSingleQ5UpdArgsHelper(*q5.remValue, -1.0, deletedOrder, updS, 0, indexR), serverR)
	case 14:
		lq14 := info.(SingleLocalQ14)
		remQ14 := SingleQ14{mapPromo: lq14.mapPromoRem, mapTotal: lq14.mapTotalRem}
		newQ14 := SingleQ14{mapPromo: lq14.mapPromoUpd, mapTotal: lq14.mapTotalUpd}
		ti.sendIndividualIndex(updS, ti.makeSingleQ14UpdArgs(remQ14, deletedOrder, newOrder, updS, 0, indexR), serverR)
		ti.sendIndividualIndex(updS, ti.makeSingleQ14UpdArgs(newQ14, deletedOrder, newOrder, updS, 0, indexN), serverN)
	case 15:
		if !useTopSum {
			lq15 := info.(SingleLocalQ15)
			remQ15, newQ15 := SingleQ15{updEntries: lq15.remEntries}, SingleQ15{updEntries: lq15.updEntries}
			ti.sendIndividualIndex(updS, ti.makeSingleLocalQ15UpdArgs(remQ15, deletedOrder, newOrder, updS, 0, indexR, int8(serverR)), serverR)
			ti.sendIndividualIndex(updS, ti.makeSingleLocalQ15UpdArgs(newQ15, deletedOrder, newOrder, updS, 0, indexN, int8(serverN)), serverN)
		} else {
			lq15 := info.(SingleLocalQ15TopSum)
			remQ15, newQ15 := SingleQ15TopSum{diffEntries: lq15.remEntries}, SingleQ15TopSum{diffEntries: lq15.updEntries}
			ti.sendIndividualIndex(updS, ti.makeSingleLocalQ15TopSumUpdArgs(remQ15, deletedOrder, newOrder, updS, 0, indexR, int8(serverR)), serverR)
			ti.sendIndividualIndex(updS, ti.makeSingleLocalQ15TopSumUpdArgs(newQ15, deletedOrder, newOrder, updS, 0, indexN, int8(serverN)), serverN)
		}

	case 18:
		q18 := info.(SingleQ18)
		ti.sendIndividualIndex(updS, ti.makeSingleQ18UpdsArgsNews(q18, newOrder, updS, 0, indexN), serverN)
		ti.sendIndividualIndex(updS, ti.makeSingleQ18UpdsArgsRems(q18, deletedOrder, updS, 0, indexR), serverR)
	}
}

func (ti SingleTableInfo) sendSpecificIndexIndividual(deleteKey string, newOrder *tpch.Orders, newItems []*tpch.LineItem,
	remOrder *tpch.Orders, remItems []*tpch.LineItem) {

	queryInfos := make([]interface{}, len(indexesToUpd))
	for i, queryN := range indexesToUpd {
		queryInfos[i] = ti.genericGetSingleUpds(queryN, newOrder, newItems, remOrder, remItems)
	}

	if isIndexGlobal {
		for i, queryN := range indexesToUpd {
			ti.genericSendIndividualIndex(queryInfos[i], queryN, newOrder, remOrder)
		}
	} else {
		regN, regR := ti.OrderToRegionkey(newOrder), ti.OrderToRegionkey(remOrder)
		newI, remI := int(regN)+INDEX_BKT, int(regR)+INDEX_BKT
		if regN == regR {
			for i, queryN := range indexesToUpd {
				ti.genericLocalSameRSendIndividualIndex(queryInfos[i], queryN, newOrder, remOrder, newI)
			}
		} else {
			for i, queryN := range indexesToUpd {
				ti.genericLocalSendIndividualIndex(queryInfos[i], queryN, newOrder, remOrder, newI, remI)
			}
		}
	}
}
*/
