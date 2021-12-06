package tpch

import (
	"encoding/csv"
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"potionDB/src/antidote"
	"potionDB/src/crdt"
	"potionDB/src/proto"
	"strconv"
	"time"
)

//TODO: On queries, all requests can go to the "partial" slot. That should make things easier to manage/maintain
//      protoServer handles fine requests on the "partial" slot that are full reads
//TODO (if there's ever time to): make some struct that handles automatically readParams and updParams slices with bufI.

//Similar to TableInfo (clientIndex.go), but for doing updates an order at a time
type SingleTableInfo struct {
	*Tables
	rng            *rand.Rand
	id             int   //Just for debbuging purposes, can delete later
	waitFor        []int //NReplies to wait for each server.
	conns          []net.Conn
	currUpdStat    *UpdateStats
	indexServer    int                             //For global case
	reusableParams [][]antidote.UpdateObjectParams //This can be reused as a buffer when sending each update in its own transaction.
}

type MixClientResult struct {
	QueryClientResult
	updsDone int
	updStats []UpdateStats
}

type UpdateStats struct {
	nNews, nDels, nIndex, nTxns int
	latency                     int64 //Used when LATENCY_MODE is PER_BATCH
}

type IndexInfo struct {
	q3         SingleQ3
	q5         SingleQ5
	q14        SingleQ14
	q15        SingleQ15
	q15TopSum  SingleQ15TopSum
	q18        SingleQ18
	lq14       SingleLocalQ14
	lq15       SingleLocalQ15
	lq15TopSum SingleLocalQ15TopSum
}

//NOTE: THIS MOST LIKELY WON'T WORK WHEN INDEXES DON'T HAVE THE ALL DATA!
//The reason for this is that for such thing to work, we would need different (ID -> pos) methods
//which would take in consideration the existance of both old and updated data (and, worse, shared among clients)
//This would be specifically problematic when running clients in different machines.
//With the current setup, I don't think even with split update and query clients that it works.

//Also, golang's rand is not adequate for this, as it uses a globalLock to ensure concurrent access is safe.

//Another relevant assumption: the number of clients must not be above the number of update files.

//Note: some configs and variables are shared with clientQueries.go and clientUpdates.go
var (
	UPD_RATE                             float64 //0 to 1.
	START_UPD_FILE, FINISH_UPD_FILE      int     //Used by this client only. clientUpdates.go use N_UPDATE_FILES.
	SPLIT_UPDATES, SPLIT_UPDATES_NO_WAIT bool    //Whenever each update should be its own transaction or not.
	RECORD_LATENCY                       bool    //Filled automatically in startMixBench. When true, latency is measured every transaction.
	//MAX_CONNECTIONS                      int     = 64 //After this limit, clients start sharing connections.
	NON_RANDOM_SERVERS bool //If true, servers are associated to clients by order instead of randomly. id is used to select the first server
)

func startMixBench() {
	maxServers := len(servers)
	RECORD_LATENCY = (LATENCY_MODE == PER_BATCH)

	fmt.Println("Reading updates...")
	readStart := time.Now().UnixNano()
	//ordersUpds, lineItemUpds, deleteKeys, lineItemSizes, itemSizesPerOrder := readUpdsByOrder()
	ordersUpds, lineItemUpds, deleteKeys, _, itemSizesPerOrder := readUpdsByOrder()
	readFinish := time.Now().UnixNano()
	fmt.Println("Finished reading updates. Time taken for read:", (readFinish-readStart)/1000000, "ms")

	//connectToServers()
	//Start server communication for update sending
	/*
		for i := range channels.dataChans {
			go handleUpdatesComm(i)
		}
	*/
	/*
		if N_UPDATE_FILES < TEST_ROUTINES {
			fmt.Println("Error - not enough update files for all clients. Aborting.")
			os.Exit(0)
		}
	*/

	fmt.Println("Splitting updates")
	N_UPDATE_FILES = FINISH_UPD_FILE - START_UPD_FILE + 1 //for splitUpdatesPerRoutine
	//_, tableInfos, routineOrders, routineItems, routineDelete, routineLineSizes :=
	//splitUpdatesPerRoutine(TEST_ROUTINES, ordersUpds, lineItemUpds, deleteKeys, itemSizesPerOrder)
	tableInfos, routineOrders, routineItems, routineDelete, routineLineSizes := newSplitUpdatesPerRoutine(TEST_ROUTINES, ordersUpds, lineItemUpds, deleteKeys, itemSizesPerOrder)
	fmt.Println("Updates split.")

	singleTableInfos, seed := make([]SingleTableInfo, len(tableInfos)), time.Now().UnixNano()
	//lastRoutineLineSize, currRoutineLineSize := 0, 0
	for i, info := range tableInfos {
		//Fixing routineLineSizes to be per order
		//currRoutineLineSize += len(routineOrders[i])
		//routineLineSizes[i] = itemSizesPerOrder[lastRoutineLineSize:currRoutineLineSize]
		//lastRoutineLineSize = currRoutineLineSize
		singleTableInfos[i] = SingleTableInfo{Tables: info.Tables, rng: rand.New(rand.NewSource(seed + int64(i))), id: i, waitFor: make([]int, len(servers))}
		if SPLIT_UPDATES {
			singleTableInfos[i].reusableParams = make([][]antidote.UpdateObjectParams, maxServers)
			for j := range singleTableInfos[i].reusableParams {
				singleTableInfos[i].reusableParams[j] = make([]antidote.UpdateObjectParams, 100)
			}
		}
	}

	selfRng := rand.New(rand.NewSource(seed + int64(2*TEST_ROUTINES)))
	chans := make([]chan MixClientResult, TEST_ROUTINES)
	results := make([]MixClientResult, TEST_ROUTINES)
	serverPerClient := make([]int, TEST_ROUTINES)
	collectQueryStats = make([]bool, TEST_ROUTINES)
	for i := range chans {
		chans[i] = make(chan MixClientResult)
		collectQueryStats[i] = false
	}
	if NON_RANDOM_SERVERS {
		j, _ := strconv.Atoi(id)
		j = j % maxServers
		for i := range chans {
			serverPerClient[i] = j % maxServers
			j++
		}
	} else {
		for i := range chans {
			serverPerClient[i] = selfRng.Intn(maxServers)
		}
	}

	//nConns := tools.MinInt(MAX_CONNECTIONS, TEST_ROUTINES)
	nConns := TEST_ROUTINES
	conns := make([][]net.Conn, nConns)
	fmt.Println("Making", nConns, "connections.")
	//fmt.Printf("Making %d connections (%d, %d)\n", nConns, MAX_CONNECTIONS, TEST_ROUTINES)
	for i := 0; i < nConns; i++ {
		go func(i int) {
			list := make([]net.Conn, len(servers))
			for j := 0; j < len(servers); j++ {
				dialer := net.Dialer{KeepAlive: -1}
				list[j], _ = (&dialer).Dial("tcp", servers[j])
			}
			conns[i] = list
		}(i)
	}

	fmt.Printf("Waiting to start queries & upds... (ops per txn: %d; batch mode: %d; latency mode: %d, isGlobal: %t, isSingle: %t, isDirect: %t)\n",
		READS_PER_TXN, BATCH_MODE, LATENCY_MODE, isIndexGlobal, !isMulti, LOCAL_DIRECT)
	fmt.Println("Sleeping at", time.Now().String())
	time.Sleep(QUERY_WAIT*1000000 - time.Duration(time.Now().UnixNano()-times.startTime))
	fmt.Println("Starting queries & upds at", time.Now().String())
	fmt.Println("Is using split: ", SPLIT_UPDATES)

	if statisticsInterval > 0 {
		fmt.Println("Called mixedInterval at", time.Now().String())
		go doMixedStatsInterval()
	}

	for i := 0; i < TEST_ROUTINES; i++ {
		//for i := 0; i < TEST_ROUTINES; i++ {
		//fmt.Printf("Value of update stats for client %d: %t, started at: %s\n", i, collectQueryStats[i], time.Now().String())
		go mixBench(0, i, serverPerClient[i], conns[i], chans[i], singleTableInfos[i],
			routineOrders[i], routineItems[i], routineDelete[i], routineLineSizes[i])
	}

	fmt.Println("Sleeping for: ", time.Duration(TEST_DURATION)*time.Millisecond, "at", time.Now().String())
	time.Sleep(time.Duration(TEST_DURATION) * time.Millisecond)
	STOP_QUERIES = true
	stopTime := time.Now().UnixNano()
	fmt.Println()
	fmt.Println("Test time is over.")

	go func() {
		for i, channel := range chans {
			results[i] = <-channel
		}
		//Notify update channels that there's no further updates
		/*
			for _, channel := range channels.updateChans {
				channel <- QueuedMsgWithStat{QueuedMsg: QueuedMsg{code: QUEUE_COMPLETE}}
			}
		*/
		fmt.Printf("Time (ms) from test end until all clients replied: %d (ms)\n", (time.Now().UnixNano()-stopTime)/1000000)
		fmt.Println("All query/upd clients have finished.")
		totalQueries, totalReads, avgDuration, totalUpds, nFuncs := 0.0, 0.0, 0.0, 0.0, float64(len(queryFuncs))
		for i, result := range results {
			fmt.Printf("%d[%d]: QueryTxns: %f, Queries: %f, QueryTxns/s: %f, Query/s: %f, Reads: %f, Reads/s: %f, Upds: %d, Upds/s: %f\n", i, serverPerClient[i],
				result.nQueries/nFuncs, result.nQueries, (result.nQueries/(result.duration*nFuncs))*1000,
				(result.nQueries/result.duration)*1000, result.nReads, (result.nReads/result.duration)*1000,
				result.updsDone, (float64(result.updsDone)/result.duration)*1000)
			totalQueries += result.nQueries
			totalReads += result.nReads
			avgDuration += result.duration
			totalUpds += float64(result.updsDone)
		}
		avgDuration /= float64(len(results))
		fmt.Printf("Totals: QueryTxns: %f, Queries: %f, QueryTxns/s: %f, Query/s: %f, Reads: %f, Reads/s: %f, Upds: %f, Upds/s: %f\n", totalQueries/nFuncs,
			totalQueries, (totalQueries/(avgDuration*nFuncs))*1000, totalQueries/avgDuration*1000,
			totalReads, (totalReads/avgDuration)*1000, totalUpds, (totalUpds/avgDuration)*1000)

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

func readUpdsByOrder() (ordersUpds [][]string, lineItemUpds [][]string, deleteKeys []string, lineItemSizes []int, itemSizesPerOrder []int) {
	updPartsRead := [][]int8{read[ORDERS], read[LINEITEM]}
	return ReadUpdatesPerOrder(updCompleteFilename[:], updEntries[:], updParts[:], updPartsRead, START_UPD_FILE, FINISH_UPD_FILE)
}

func updateMixStats(queryStats []QueryStats, updStats []UpdateStats, nReads, lastStatReads, nQueries, lastStatQueries int,
	client *SingleTableInfo, lastStatTime, qTime, updTime int64, nTxnsQ, nTxnsU int) (newQStats []QueryStats, newUStats []UpdateStats,
	newNReads, newNQueries int, newLastStatTime int64) {
	currStatTime, currQStats, currUStats := time.Now().UnixNano()/1000000, QueryStats{}, *client.currUpdStat
	diffT, diffR, diffQ := currStatTime-lastStatTime, nReads-lastStatReads, nQueries-lastStatQueries
	if diffT < int64(statisticsInterval/20) && len(queryStats) > 0 {
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

	/*
		conns := make([]net.Conn, len(servers))
		for i := range conns {
			dialer := net.Dialer{KeepAlive: -1}
			conns[i], _ = (&dialer).Dial("tcp", servers[i])
		}
	*/

	tableInfo.conns = conns
	tableInfo.currUpdStat = &UpdateStats{}

	//Update preparation
	orderI, lineI, lineF := 0, 0, lineSizes[0]

	//Query preparation
	client := QueryClient{serverConns: conns, indexServer: defaultServer, rng: tableInfo.rng}
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

	if READS_PER_TXN > 1 {
		queryPos, nOpsTxn, previousQueryPos, replyPos, nUpdsDone := 0, 0, 0, 0, 0
		var fullRBuf, partialRBuf []antidote.ReadObjectParams
		if isIndexGlobal {
			fullRBuf, partialRBuf = make([]antidote.ReadObjectParams, READS_PER_TXN+1),
				make([]antidote.ReadObjectParams, READS_PER_TXN+1) //+1 as one of the queries has 2 reads.
		} else if localMode == LOCAL_SERVER {
			fullRBuf, partialRBuf = make([]antidote.ReadObjectParams, (READS_PER_TXN+1)*len(conns)),
				make([]antidote.ReadObjectParams, (READS_PER_TXN+1)*len(conns))
		}
		//For LOCAL_DIRECT
		fullRBufS, partialRBufS := make([][]antidote.ReadObjectParams, len(conns)), make([][]antidote.ReadObjectParams, len(conns))
		copyFullS, copyPartialS := make([][]antidote.ReadObjectParams, len(conns)), make([][]antidote.ReadObjectParams, len(conns))

		updBuf := make([][]antidote.UpdateObjectParams, len(conns))
		updBufI, readBufI, localReadBufI, rngRegions := make([]int, len(conns)), make([]int, 2), make([][]int, len(conns)), make([]int, READS_PER_TXN)
		for i := range updBuf {
			updBuf[i] = make([]antidote.UpdateObjectParams, int(math.Max(100.0, float64(READS_PER_TXN*2)))) //100 is way more than enough for a set of updates.
			fullRBufS[i], partialRBufS[i] = make([]antidote.ReadObjectParams, READS_PER_TXN+1), make([]antidote.ReadObjectParams, READS_PER_TXN+1)
			localReadBufI[i] = make([]int, 2)
		}
		for !STOP_QUERIES {
			if collectQueryStats[clientN] {
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
						nUpdsDone = tableInfo.getSingleDataChange(updBuf, updBufI, orders[orderI], items[lineI:lineF], delete[orderI])
						waitForUpds += nUpdsDone
						orderI++
						if orderI == len(orders) {
							orderI, lineI, lineF = 0, 0, lineSizes[0]
						} else {
							lineI += lineSizes[orderI]
							lineF += lineSizes[orderI]
						}
					}
					//*fmt.Println("[MIXBENCH]updBufI when calling sendReceiveMultipleUpdates")
					tableInfo.sendReceiveMultipleUpdates(updBuf, updBufI)
					for i := range updBufI {
						updBufI[i] = 0
					}
					currUpdSpentTime += recordFinishLatency(startTxnTime)
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
					previousQueryPos = queryPos
				}

				if !isLocalDirect {
					//fmt.Printf("[ClientQueryUpd]Sizes full/part: %d %d\n", readBufI[0], readBufI[1])
					//Expected sizes for local_redirect
					//fmt.Printf("[ClientQueryUpd]Expected sizes: 8 15 (Q3: 0 5; Q5: 1 0; Q11: 2 0; Q14: 0 5; Q15: 0 5; Q18: 5 0)\n")
					readReplies := sendReceiveReadProto(client, fullRBuf[:readBufI[0]], partialRBuf[:readBufI[1]], client.indexServer).GetObjects().GetObjects()
					readBufI[1] = readBufI[0] //partial reads start when full reads end
					readBufI[0] = 0
					//fmt.Printf("[ClientQueryUpd]Reply sizes: len is %d; readBufI[0] is %d; readBufI[1] is %d\n", len(readReplies), readBufI[0], readBufI[1])
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

				currQuerySpentTime += recordFinishLatency(startTxnTime)

				//fmt.Println("[CQU]Aborted after first read")
			}
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
						lineI += lineSizes[orderI]
						lineF += lineSizes[orderI]
					}
					currUpdSpentTime += recordFinishLatency(startTxnTime)
				}
			} else {
				//READ
				startTxnTime = recordStartLatency()
				reads += funcs[qDone%nQueries](client)
				qDone++
				currQuerySpentTime += recordFinishLatency(startTxnTime)
				readTxns++
			}
		}
	}

	endTime := time.Now().UnixNano() / 1000000
	queryStats, updStats, lastStatReads, lastStatQDone, lastStatTime = updateMixStats(queryStats, updStats, reads,
		lastStatReads, qDone, lastStatQDone, &tableInfo, lastStatTime, currQuerySpentTime, currUpdSpentTime, readTxns, updTxns)

	for _, conn := range conns {
		conn.Close()
	}

	resultChan <- MixClientResult{QueryClientResult: QueryClientResult{duration: float64(endTime - startTime), nQueries: float64(qDone),
		nReads: float64(reads), intermediateResults: queryStats}, updsDone: updsDone, updStats: updStats}
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

//getReadsFuncs[queryPos%nQueries](client, fullRBuf, partialRBuf, readBufI, nOpsTxn)
//fullRBuf, partialRBuf := make([]antidote.ReadObjectParams, READS_PER_TXN+1), make([]antidote.ReadObjectParams, READS_PER_TXN+1)

/*
func prepareNextQuery(client QueryClient, fullRBuf, partialRBuf []antidote.ReadObjectParams, readBufI []int, nOpsTxn int) {

}
*/

/*
func (ti SingleTableInfo) sendSingleDataChange(order []string, lineItems [][]string, deleteKey string) (updsDone int) {
	deletedOrder, deletedItems, nDels := ti.sendSingleDelete(deleteKey)
	updsDone += ti.sendSingleUpd(order, lineItems)
	updsDone += ti.sendSingleIndexUpds(deleteKey, order, lineItems, deletedOrder, deletedItems)
	updsDone += nDels

	//Wait for update replies
	for i, nMsgs := range ti.waitFor {
		if nMsgs > 0 {
			conn := ti.conns[i]
			for j := 0; j < nMsgs; j++ {
				antidote.ReceiveProto(conn)
			}
			ti.waitFor[i] = 0
		}
	}
	return
}
*/

func (ti SingleTableInfo) sendReceiveMultipleUpdates(updBuf [][]antidote.UpdateObjectParams, bufI []int) {
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
	nIndexUpds, indexInfo := 0, IndexInfo{}

	if UPDATE_INDEX && !UPDATE_SPECIFIC_INDEX_ONLY {
		_, _, nIndexUpds, indexInfo = ti.getSingleIndexUpds(deleteKey, newOrder, newItems, deletedOrder, deletedItems)
		regN, regR := ti.OrderToRegionkey(newOrder), ti.OrderToRegionkey(deletedOrder)
		updS := updParams[0] //Doesn't matter which buffer is used

		if isIndexGlobal {
			ti.sendIndividualIndex(updS, ti.makeSingleQ3UpdArgs(indexInfo.q3, deletedOrder, newOrder, updS, 0, INDEX_BKT), ti.indexServer)
			ti.sendIndividualIndex(updS, ti.makeSingleQ5UpdArgs(indexInfo.q5, deletedOrder, newOrder, updS, 0, INDEX_BKT), ti.indexServer)
			ti.sendIndividualIndex(updS, ti.makeSingleQ14UpdArgs(indexInfo.q14, deletedOrder, newOrder, updS, 0, INDEX_BKT), ti.indexServer)
			if !useTopSum {
				ti.sendIndividualIndex(updS, ti.makeSingleQ15UpdArgs(indexInfo.q15, deletedOrder, newOrder, updS, 0, INDEX_BKT), ti.indexServer)
			} else {
				ti.sendIndividualIndex(updS, ti.makeSingleQ15TopSumUpdArgs(indexInfo.q15TopSum, deletedOrder, newOrder, updS, 0, INDEX_BKT), ti.indexServer)
			}
			ti.sendIndividualIndex(updS, ti.makeSingleQ18UpdArgs(indexInfo.q18, deletedOrder, newOrder, updS, 0, INDEX_BKT), ti.indexServer)
		} else {
			newI, remI := int(regN)+INDEX_BKT, int(regR)+INDEX_BKT
			var newS, remS int
			if localMode == LOCAL_DIRECT {
				newS, remS = int(regN), int(regR)
			} else {
				newS, remS = ti.indexServer, ti.indexServer
			}

			if regN == regR {
				//Single msg for both new & rem
				ti.sendIndividualIndex(updS, ti.makeSingleQ3UpdArgs(indexInfo.q3, deletedOrder, newOrder, updS, 0, newI), newS)
				ti.sendIndividualIndex(updS, ti.makeSingleQ5UpdArgs(indexInfo.q5, deletedOrder, newOrder, updS, 0, newI), newS)
				ti.sendIndividualIndex(updS, ti.makeSingleQ14UpdArgs(SingleQ14{mapPromo: indexInfo.lq14.mapPromoRem, mapTotal: indexInfo.lq14.mapTotalRem}, deletedOrder, newOrder, updS, 0, newI), newS)
				ti.sendIndividualIndex(updS, ti.makeSingleQ14UpdArgs(SingleQ14{mapPromo: indexInfo.lq14.mapPromoUpd, mapTotal: indexInfo.lq14.mapTotalUpd}, deletedOrder, newOrder, updS, 0, newI), newS)
				if !useTopSum {
					ti.sendIndividualIndex(updS, ti.makeSingleLocalQ15UpdArgs(SingleQ15{updEntries: indexInfo.lq15.remEntries}, deletedOrder, newOrder, updS, 0, newI, regN), newS)
					ti.sendIndividualIndex(updS, ti.makeSingleLocalQ15UpdArgs(SingleQ15{updEntries: indexInfo.lq15.updEntries}, deletedOrder, newOrder, updS, 0, newI, regN), newS)
				} else {
					ti.sendIndividualIndex(updS, ti.makeSingleLocalQ15TopSumUpdArgs(SingleQ15TopSum{diffEntries: indexInfo.lq15TopSum.remEntries}, deletedOrder, newOrder, updS, 0, newI, regN), newS)
					ti.sendIndividualIndex(updS, ti.makeSingleLocalQ15TopSumUpdArgs(SingleQ15TopSum{diffEntries: indexInfo.lq15TopSum.updEntries}, deletedOrder, newOrder, updS, 0, newI, regN), newS)
				}
				ti.sendIndividualIndex(updS, ti.makeSingleQ18UpdArgs(indexInfo.q18, deletedOrder, newOrder, updS, 0, newI), newS)
			} else {
				ti.sendIndividualIndex(updS, ti.makeSingleQ3UpdsArgsNews(indexInfo.q3, newOrder, updS, 0, newI), newS)
				ti.sendIndividualIndex(updS, ti.makeSingleQ5UpdArgsHelper(*indexInfo.q5.updValue, 1.0, newOrder, updS, 0, newI), newS)
				ti.sendIndividualIndex(updS, ti.makeSingleQ14UpdArgs(SingleQ14{mapPromo: indexInfo.lq14.mapPromoUpd, mapTotal: indexInfo.lq14.mapTotalUpd}, deletedOrder, newOrder, updS, 0, newI), newS)
				if !useTopSum {
					ti.sendIndividualIndex(updS, ti.makeSingleLocalQ15UpdArgs(SingleQ15{updEntries: indexInfo.lq15.updEntries}, deletedOrder, newOrder, updS, 0, newI, regN), newS)
				} else {
					ti.sendIndividualIndex(updS, ti.makeSingleLocalQ15TopSumUpdArgs(SingleQ15TopSum{diffEntries: indexInfo.lq15TopSum.updEntries}, deletedOrder, newOrder, updS, 0, newI, regN), newS)
				}
				ti.sendIndividualIndex(updS, ti.makeSingleQ18UpdsArgsNews(indexInfo.q18, newOrder, updS, 0, newI), newS)
				ti.sendIndividualIndex(updS, ti.makeSingleQ3UpdsArgsRems(indexInfo.q3, deletedOrder, updS, 0, remI), remS)
				ti.sendIndividualIndex(updS, ti.makeSingleQ5UpdArgsHelper(*indexInfo.q5.remValue, -1.0, deletedOrder, updS, 0, remI), remS)
				ti.sendIndividualIndex(updS, ti.makeSingleQ14UpdArgs(SingleQ14{mapPromo: indexInfo.lq14.mapPromoRem, mapTotal: indexInfo.lq14.mapTotalRem}, deletedOrder, newOrder, updS, 0, remI), remS)
				if !useTopSum {
					ti.sendIndividualIndex(updS, ti.makeSingleLocalQ15UpdArgs(SingleQ15{updEntries: indexInfo.lq15.remEntries}, deletedOrder, newOrder, updS, 0, remI, regR), remS)
				} else {
					ti.sendIndividualIndex(updS, ti.makeSingleLocalQ15TopSumUpdArgs(SingleQ15TopSum{diffEntries: indexInfo.lq15TopSum.remEntries}, deletedOrder, newOrder, updS, 0, remI, regR), remS)
				}
				ti.sendIndividualIndex(updS, ti.makeSingleQ18UpdsArgsRems(indexInfo.q18, deletedOrder, updS, 0, remI), remS)
			}
		}
	} else if UPDATE_INDEX {
		ti.sendSpecificIndexIndividual(deleteKey, newOrder, newItems, deletedOrder, deletedItems)
	}

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

	return nDels + nAdds + nIndexUpds
}

func (ti SingleTableInfo) genericGetSingleUpds(queryN int, newOrder *Orders, newItems []*LineItem,
	remOrder *Orders, remItems []*LineItem) (info interface{}) {

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

func (ti SingleTableInfo) genericSendIndividualIndex(info interface{}, queryN int, newOrder, deletedOrder *Orders) {
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

func (ti SingleTableInfo) genericLocalSameRSendIndividualIndex(info interface{}, queryN int, newOrder, deletedOrder *Orders, indexN int) {
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

func (ti SingleTableInfo) genericLocalSendIndividualIndex(info interface{}, queryN int, newOrder, deletedOrder *Orders, indexN, indexR int) {
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

func (ti SingleTableInfo) sendSpecificIndexIndividual(deleteKey string, newOrder *Orders, newItems []*LineItem,
	remOrder *Orders, remItems []*LineItem) {

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

func (ti SingleTableInfo) sendIndividualUpdates(upds [][]antidote.UpdateObjectParams, bufI []int) {
	for i, serverUpds := range upds {
		for j := 0; j < bufI[i]; j++ {
			antidote.SendProto(antidote.StaticUpdateObjs, antidote.CreateStaticUpdateObjs(nil, []antidote.UpdateObjectParams{serverUpds[j]}), ti.conns[i])
			if !SPLIT_UPDATES_NO_WAIT {
				antidote.ReceiveProto(ti.conns[i])
			}
		}
		ti.waitFor[i] += bufI[i]
		bufI[i] = 0
	}
}

func (ti SingleTableInfo) sendIndividualIndex(upds []antidote.UpdateObjectParams, buf int, server int) {
	for j := 0; j < buf; j++ {
		antidote.SendProto(antidote.StaticUpdateObjs, antidote.CreateStaticUpdateObjs(nil, []antidote.UpdateObjectParams{upds[j]}), ti.conns[server])
		if !SPLIT_UPDATES_NO_WAIT {
			antidote.ReceiveProto(ti.conns[server])
		}
	}
	ti.waitFor[server] += buf
}

func (ti SingleTableInfo) getSingleDataChange(updParams [][]antidote.UpdateObjectParams, bufI []int, order []string, lineItems [][]string, deleteKey string) (updsDone int) {
	deletedOrder, deletedItems := ti.getNextDelete(deleteKey)
	newOrder, newItems := ti.CreateOrder(order), ti.CreateLineitemsOfOrder(lineItems)
	nDels, itemsDelPerServer, itemsIndex := ti.getSingleDelete(deletedOrder, deletedItems)
	nAdds, itemsNewPerServer := ti.getSingleUpd(order, lineItems)
	nIndexUpds, indexInfo := 0, IndexInfo{}
	if UPDATE_INDEX {
		_, _, nIndexUpds, indexInfo = ti.getSingleIndexUpds(deleteKey, newOrder, newItems, deletedOrder, deletedItems)
	}
	regN, regR := ti.OrderToRegionkey(newOrder), ti.OrderToRegionkey(deletedOrder)

	//fmt.Println(bufI)
	if UPDATE_BASE_DATA {
		ti.makeSingleDelete(deletedOrder, updParams, bufI, itemsDelPerServer, itemsIndex)
		//fmt.Println(bufI)
		ti.makeSingleUpd(order, lineItems, updParams, bufI, itemsNewPerServer)
		//fmt.Println(bufI)
	}
	if UPDATE_INDEX {
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
	}

	if UPDATE_BASE_DATA {
		ti.currUpdStat.nDels += nDels
		ti.currUpdStat.nNews += nAdds
	}
	ti.currUpdStat.nIndex += nIndexUpds

	//*fmt.Println("BufI before returning (index server:", ti.indexServer, ") - ", bufI)

	if UPDATE_BASE_DATA {
		return nDels + nAdds + nIndexUpds
	} else {
		return nIndexUpds
	}
}

func (ti SingleTableInfo) sendSingleDataChange(order []string, lineItems [][]string, deleteKey string) (updsDone int) {
	deletedOrder, deletedItems := ti.getNextDelete(deleteKey)
	newOrder, newItems := ti.CreateOrder(order), ti.CreateLineitemsOfOrder(lineItems)
	nDels, itemsDelPerServer, itemsIndex := ti.getSingleDelete(deletedOrder, deletedItems)
	nAdds, itemsNewPerServer := ti.getSingleUpd(order, lineItems)
	nUpdsN, nUpdsR, nIndexUpds, indexInfo := 0, 0, 0, IndexInfo{}
	if UPDATE_INDEX {
		nUpdsN, nUpdsR, nIndexUpds, indexInfo = ti.getSingleIndexUpds(deleteKey, newOrder, newItems, deletedOrder, deletedItems)
	}
	regN, regR := ti.OrderToRegionkey(newOrder), ti.OrderToRegionkey(deletedOrder)

	updParams := make([][]antidote.UpdateObjectParams, len(channels.dataChans))
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
				updParams[i] = make([]antidote.UpdateObjectParams, total)
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
				updParams[i] = make([]antidote.UpdateObjectParams, total)
			}
		}
	} else {
		if isIndexGlobal {
			for i := range updParams {
				if i == ti.indexServer && UPDATE_INDEX {
					updParams[i] = make([]antidote.UpdateObjectParams, nUpdsN)
				} else {
					updParams[i] = make([]antidote.UpdateObjectParams, 0)
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
				updParams[i] = make([]antidote.UpdateObjectParams, total)
			}
		} else {
			for i := range updParams {
				if i == ti.indexServer && UPDATE_INDEX {
					updParams[i] = make([]antidote.UpdateObjectParams, nUpdsN+nUpdsR)
				} else {
					updParams[i] = make([]antidote.UpdateObjectParams, 0)
				}
			}
		}
	}

	/*
		fmt.Printf("ItemIndexes: %v, itemsNewPerServer: %v\n", itemsIndex, itemsNewPerServer)
		fmt.Print("Upd sizes: ")
		for _, updP := range updParams {
			fmt.Print(len(updP), ",")
		}
		fmt.Println()
	*/

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
	}
	ti.currUpdStat.nIndex += nIndexUpds

	if UPDATE_BASE_DATA {
		return nDels + nAdds + nIndexUpds
	} else {
		return nIndexUpds
	}
}

func (ti SingleTableInfo) getSingleDelete(orderDel *Orders, itemsDel []*LineItem) (nDels int, itemsPerServer [][]string, itemsIndex []int) {
	orderReg := ti.Tables.Custkey32ToRegionkey(orderDel.O_CUSTKEY)
	//Items may need to go to multiple servers (cust + supplier)
	itemsPerServer, itemsIndex = make([][]string, len(channels.dataChans)), make([]int, len(channels.dataChans))
	for i := range itemsPerServer {
		itemsPerServer[i] = make([]string, len(itemsDel))
	}

	var suppReg int8
	var itemS string
	key := strconv.FormatInt(int64(orderDel.O_ORDERKEY), 10)
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
	return
}

func (ti SingleTableInfo) makeSingleDelete(orderDel *Orders, updParams [][]antidote.UpdateObjectParams, bufI []int, itemsPerServer [][]string, itemsIndex []int) {
	orderReg, key := ti.Tables.Custkey32ToRegionkey(orderDel.O_CUSTKEY), strconv.FormatInt(int64(orderDel.O_ORDERKEY), 10)
	orderUpdParams := updParams[orderReg]
	if !CRDT_PER_OBJ {
		orderParams := getSingleDeleteParams(tableNames[ORDERS], buckets[orderReg], key)
		itemParams := getDeleteParams(tableNames[LINEITEM], buckets[orderReg], itemsPerServer[orderReg], itemsIndex[orderReg])
		orderUpdParams[bufI[orderReg]] = orderParams
		orderUpdParams[bufI[orderReg]+1] = itemParams
		bufI[orderReg] += 2
		for i, items := range itemsPerServer {
			if int8(i) == orderReg || itemsIndex[i] == 0 {
				continue
			}
			itemParams = getDeleteParams(tableNames[LINEITEM], buckets[i], items, itemsIndex[i])
			updParams[i][bufI[i]] = itemParams
			bufI[i]++
			//ti.sendUpdate([]antidote.UpdateObjectParams{itemParams}, i, itemsIndex[i], REMOVE_TYPE)
		}
	} else {
		//upds := make([]antidote.UpdateObjectParams, itemsIndex[orderReg]+1)
		updParams[orderReg][bufI[orderReg]] = getSinglePerObjDeleteParams(tableNames[ORDERS], buckets[orderReg], key)
		bufI[orderReg] = getPerObjDeleteParams(tableNames[LINEITEM], buckets[orderReg], itemsPerServer[orderReg], itemsIndex[orderReg], updParams[orderReg], bufI[orderReg]+1)
		//ti.sendUpdate(upds, int(orderReg), len(upds), REMOVE_TYPE)
		for i, items := range itemsPerServer {
			if int8(i) == orderReg || itemsIndex[i] == 0 {
				continue
			}
			//upds = make([]antidote.UpdateObjectParams, itemsIndex[i])
			bufI[i] = getPerObjDeleteParams(tableNames[LINEITEM], buckets[i], items, itemsIndex[i], updParams[i], bufI[i])
			//ti.sendUpdate(upds, i, len(upds), REMOVE_TYPE)
		}
	}
}

/*
func (ti SingleTableInfo) sendSingleDelete(key string) (orderDel *Orders, itemsDel []*LineItem, nDels int) {
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
		orderParams := getSingleDeleteParams(tableNames[ORDERS], buckets[orderReg], key)
		itemParams := getDeleteParams(tableNames[LINEITEM], buckets[orderReg], itemsPerServer[orderReg], itemsIndex[orderReg])
		ti.sendUpdate([]antidote.UpdateObjectParams{orderParams, itemParams}, int(orderReg), itemsIndex[orderReg]+1, REMOVE_TYPE)
		// channels.updateChans[orderReg] <- QueuedMsgWithStat{QueuedMsg: QueuedMsg{
		// 	code:    antidote.StaticUpdateObjs,
		// 	Message: antidote.CreateStaticUpdateObjs(nil, []antidote.UpdateObjectParams{orderParams, itemParams})},
		// 	nData:    itemsIndex[orderReg] + 1,
		// 	dataType: REMOVE_TYPE,
		// }
		for i, items := range itemsPerServer {
			if int8(i) == orderReg || itemsIndex[i] == 0 {
				continue
			}
			itemParams = getDeleteParams(tableNames[LINEITEM], buckets[i], items, itemsIndex[i])
			ti.sendUpdate([]antidote.UpdateObjectParams{itemParams}, i, itemsIndex[i], REMOVE_TYPE)
			// channels.updateChans[i] <- QueuedMsgWithStat{QueuedMsg: QueuedMsg{
			// 	code:    antidote.StaticUpdateObjs,
			// 	Message: antidote.CreateStaticUpdateObjs(nil, []antidote.UpdateObjectParams{itemParams})},
			// 	nData:    itemsIndex[i],
			// 	dataType: REMOVE_TYPE,
			// }
		}
	} else {
		upds := make([]antidote.UpdateObjectParams, itemsIndex[orderReg]+1)
		upds[0] = getSinglePerObjDeleteParams(tableNames[ORDERS], buckets[orderReg], key)
		getPerObjDeleteParams(tableNames[LINEITEM], buckets[orderReg], itemsPerServer[orderReg], itemsIndex[orderReg], upds, 1)
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
			upds = make([]antidote.UpdateObjectParams, itemsIndex[i])
			getPerObjDeleteParams(tableNames[LINEITEM], buckets[i], items, itemsIndex[i], upds, 0)
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

func (ti SingleTableInfo) sendUpdate(updates []antidote.UpdateObjectParams, channel int, nUpds int, dataType int) {
	antidote.SendProto(antidote.StaticUpdateObjs, antidote.CreateStaticUpdateObjs(nil, updates), ti.conns[channel])
	ti.waitFor[channel]++
	//TODO: Store nUpds/dataType
}

func getSingleDeleteParams(tableName string, bkt string, key string) antidote.UpdateObjectParams {
	var mapRemove crdt.UpdateArguments = crdt.MapRemove{Key: key}
	return antidote.UpdateObjectParams{
		KeyParams:  antidote.KeyParams{Key: tableName, CrdtType: proto.CRDTType_RRMAP, Bucket: bkt},
		UpdateArgs: &mapRemove,
	}
}

func getSinglePerObjDeleteParams(tableName string, bkt string, key string) antidote.UpdateObjectParams {
	var delete crdt.UpdateArguments = crdt.ResetOp{}
	return antidote.UpdateObjectParams{
		KeyParams:  antidote.KeyParams{Key: tableName + key, CrdtType: proto.CRDTType_RRMAP, Bucket: bkt},
		UpdateArgs: &delete,
	}
}

func (ti SingleTableInfo) getNextDelete(key string) (orderToDelete *Orders, itemsToDelete []*LineItem) {
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
	var upd *crdt.EmbMapUpdateAll
	var itemRegions []int8

	for _, item := range lineItemUpds {
		key, upd = getEntryUpd(headers[LINEITEM], keys[LINEITEM], item, read[LINEITEM])
		key = getEntryKey(tableNames[LINEITEM], key)
		itemRegions = itemFunc(orderUpd, item)
		for _, region := range itemRegions {
			itemsPerServer[region][key] = *upd
			nAdds++
		}
	}
	//order
	nAdds++

	return
}

func (ti SingleTableInfo) makeSingleUpd(orderUpd []string, lineItemUpds [][]string, updParams [][]antidote.UpdateObjectParams, bufI []int,
	itemsPerServer []map[string]crdt.UpdateArguments) {
	orderReg := regionFuncs[ORDERS](orderUpd)

	orderKey, orderMapUpd := getEntryUpd(headers[ORDERS], keys[ORDERS], orderUpd, read[ORDERS])
	orderKey = getEntryKey(tableNames[ORDERS], orderKey)
	//fmt.Println("startSingleUpd", bufI)
	//currUpdParams := make([]antidote.UpdateObjectParams, getUpdSize(itemsPerServer[orderReg])+1)
	updParams[orderReg][bufI[orderReg]] = getSingleDataUpdateParams(orderKey, orderMapUpd, tableNames[ORDERS], buckets[orderReg])
	//fmt.Println("1SingleUpd", bufI)
	//currUpdParams[0] = getSingleDataUpdateParams(orderKey, orderMapUpd, tableNames[ORDERS], buckets[orderReg])
	bufI[orderReg] = getDataUpdateParamsWithBuf(itemsPerServer[orderReg], tableNames[LINEITEM], buckets[orderReg], updParams[orderReg], bufI[orderReg]+1)
	//fmt.Println("2SingleUpd", bufI)
	//ti.sendUpdate(currUpdParams, int(orderReg), len(itemsPerServer[orderReg])+1, NEW_TYPE)

	for i := 0; i < len(ti.Tables.Regions); i++ {
		if int8(i) != orderReg && len(itemsPerServer[i]) > 0 {
			//currUpdParams = getDataUpdateParams(itemsPerServer[i], tableNames[LINEITEM], buckets[i])
			bufI[i] = getDataUpdateParamsWithBuf(itemsPerServer[i], tableNames[LINEITEM], buckets[i], updParams[i], bufI[i])
			//ti.sendUpdate(currUpdParams, i, len(itemsPerServer[i]), NEW_TYPE)
			//fmt.Println("CycleSingleUpd", bufI)
		}
	}
}

/*
func (ti SingleTableInfo) sendSingleUpd(orderUpd []string, lineItemUpds [][]string) (nAdds int) {
	orderReg := regionFuncs[ORDERS](orderUpd)
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
		key = getEntryKey(tableNames[LINEITEM], key)
		itemRegions = itemFunc(orderUpd, item)
		for _, region := range itemRegions {
			itemsPerServer[region][key] = *upd
			nAdds++
		}
	}
	//order
	nAdds++

	orderKey, orderMapUpd := getEntryUpd(headers[ORDERS], keys[ORDERS], orderUpd, read[ORDERS])
	orderKey = getEntryKey(tableNames[ORDERS], orderKey)
	currUpdParams := make([]antidote.UpdateObjectParams, getUpdSize(itemsPerServer[orderReg])+1)
	currUpdParams[0] = getSingleDataUpdateParams(orderKey, orderMapUpd, tableNames[ORDERS], buckets[orderReg])
	getDataUpdateParamsWithBuf(itemsPerServer[orderReg], tableNames[LINEITEM], buckets[orderReg], currUpdParams, 1)
	ti.sendUpdate(currUpdParams, int(orderReg), len(itemsPerServer[orderReg])+1, NEW_TYPE)
		// channels.updateChans[orderReg] <- QueuedMsgWithStat{QueuedMsg: QueuedMsg{
		// 	code:    antidote.StaticUpdateObjs,
		// 	Message: antidote.CreateStaticUpdateObjs(nil, currUpdParams)},
		// 	nData:    len(itemsPerServer[orderReg]) + 1,
		// 	dataType: NEW_TYPE,
		// }

	for i := 0; i < len(ti.Tables.Regions); i++ {
		if int8(i) != orderReg && len(itemsPerServer[i]) > 0 {
			currUpdParams = getDataUpdateParams(itemsPerServer[i], tableNames[LINEITEM], buckets[i])
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

func getSingleDataUpdateParams(key string, updArgs crdt.UpdateArguments, name string, bucket string) (updParams antidote.UpdateObjectParams) {
	if CRDT_PER_OBJ {
		return antidote.UpdateObjectParams{
			KeyParams: antidote.KeyParams{Key: key, CrdtType: proto.CRDTType_RRMAP, Bucket: bucket}, UpdateArgs: &updArgs}
	} else {
		var mapUpd crdt.UpdateArguments = crdt.EmbMapUpdate{Key: key, Upd: updArgs}
		return antidote.UpdateObjectParams{
			KeyParams: antidote.KeyParams{Key: name, CrdtType: proto.CRDTType_RRMAP, Bucket: bucket}, UpdateArgs: &mapUpd}
	}
}

func (ti SingleTableInfo) getSingleIndexUpds(deleteKey string, newOrder *Orders, newItems []*LineItem,
	remOrder *Orders, remItems []*LineItem) (nUpdsN, nUpdsR, nUpdsStats int, indexInfo IndexInfo) {
	if isIndexGlobal {
		q15, q15TopSum := SingleQ15{}, SingleQ15TopSum{}
		q3, q5, q14, q18 := ti.getSingleQ3Upds(remOrder, remItems, newOrder, newItems), ti.getSingleQ5Upds(remOrder, remItems, newOrder, newItems),
			ti.getSingleQ14Upds(remOrder, remItems, newOrder, newItems), ti.getSingleQ18Upds(remOrder, remItems, newOrder, newItems)
		if !useTopSum {
			q15 = ti.getSingleQ15Upds(remOrder, remItems, newOrder, newItems)
		} else {
			q15TopSum = ti.getSingleQ15TopSumUpds(remOrder, remItems, newOrder, newItems)
		}

		nUpdsN, nUpdsStats = ti.getNUpdObjs(q3, q5, q14, q15, q15TopSum, q18)
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
		nUpdsR, nUpdsN, nUpdsStats = ti.getNUpdLocalObjs(q3, q5, q14, q15, q15TopSum, q18, remR, newR)
		indexInfo = IndexInfo{q3: q3, q5: q5, lq14: q14, lq15: q15, lq15TopSum: q15TopSum, q18: q18}
	}
	return
}

/*
func (ti SingleTableInfo) sendSingleIndexUpds(deleteKey string, orderUpd []string, lineItemUpds [][]string,
	remOrder *Orders, remItems []*LineItem) (nUpdsStats int) {
	if isIndexGlobal {
		return ti.sendSingleGlobalIndexUpdates(deleteKey, orderUpd, lineItemUpds, remOrder, remItems)
	}
	return ti.sendSingleLocalIndexUpdates(deleteKey, orderUpd, lineItemUpds, remOrder, remItems)
}

func (ti SingleTableInfo) sendSingleLocalIndexUpdates(deleteKey string, orderUpd []string, lineItemUpds [][]string,
	remOrder *Orders, remItems []*LineItem) (nUpdsStats int) {

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
		upds, bufI := make([]antidote.UpdateObjectParams, nUpdsR+nUpdsN), 0
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
		newUpds, remUpds, newBufI, remBufI := make([]antidote.UpdateObjectParams, nUpdsN), make([]antidote.UpdateObjectParams, nUpdsR), 0, 0
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
	remOrder *Orders, remItems []*LineItem) (nUpdsStats int) {

	//fmt.Printf("[%d]Preparing index upds for order %s. Number of new lineItems: %d, delete lineItems: %d\n", ti.id,
	//orderUpd[0], len(lineItemUpds), len(remItems))

	newOrder, newItems := ti.CreateOrder(orderUpd), ti.CreateLineitemsOfOrder(lineItemUpds)
	q3, q5, q14, q15, q18 := ti.getSingleQ3Upds(remOrder, remItems, newOrder, newItems),
		ti.getSingleQ5Upds(remOrder, remItems, newOrder, newItems), ti.getSingleQ14Upds(remOrder, remItems, newOrder, newItems),
		ti.getSingleQ15Upds(remOrder, remItems, newOrder, newItems), ti.getSingleQ18Upds(remOrder, remItems, newOrder, newItems)

	nUpds, nUpdsStats := ti.getNUpdObjs(q3, q5, q14, q15, q18)
	upds, bufI := make([]antidote.UpdateObjectParams, nUpds), 0
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
*/

func (ti SingleTableInfo) getNUpdLocalObjs(q3 SingleQ3, q5 SingleQ5, q14 SingleLocalQ14, q15 SingleLocalQ15, q15TopSum SingleLocalQ15TopSum, q18 SingleQ18, remR,
	newR int8) (nUpdsR, nUpdsN, nUpdsStats int) {

	if q3.updStartPos > 0 {
		nUpdsN += int(MAX_MONTH_DAY - q3.updStartPos + 1)
	}
	nUpdsR += len(q3.remMap)
	if *q5.remValue != 0 {
		nUpdsR += 1
	}
	if *q5.updValue != 0 {
		nUpdsN += 1
	}
	nUpdsR += len(q14.mapTotalRem)
	nUpdsN += len(q14.mapTotalUpd)
	nUpdsR += ti.countQ18Upds(q18.remQuantity)
	nUpdsN += ti.countQ18Upds(q18.updQuantity)
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
	//TODO: Remove
	/*
		q5N, q5R := 0, 0
		if *q5.remValue != 0 {
			q5R = 1
		}
		if *q5.updValue != 0 {
			q5N = 1
		}
		fmt.Println(q3.updStartPos, q3.totalUpds)
		fmt.Println(nUpdsN, nUpdsStats, MAX_MONTH_DAY-q3.updStartPos+1, q5N, len(q14.mapTotalUpd), q15UpdsN, q15UpdsStatsN, ti.countQ18Upds(q18.remQuantity))
		fmt.Println(nUpdsR, nUpdsStats, len(q3.remMap), q5R, len(q14.mapTotalRem), q15UpdsR, q15UpdsStatsR, ti.countQ18Upds(q18.updQuantity))
		fmt.Println()
	*/
	return
}

func (ti SingleTableInfo) getNUpdObjs(q3 SingleQ3, q5 SingleQ5, q14 SingleQ14, q15 SingleQ15, q15TopSum SingleQ15TopSum, q18 SingleQ18) (nUpds int, nUpdsStats int) {
	nUpds += q3.totalUpds
	if *q5.remValue != 0 {
		nUpds += 1
	}
	if *q5.updValue != 0 {
		nUpds += 1
	}
	nUpds += len(q14.mapTotal)
	nUpds += ti.countQ18Upds(q18.remQuantity)
	nUpds += ti.countQ18Upds(q18.updQuantity)
	nUpdsStats = nUpds
	var q15Upds, q15UpdsStats int
	if !useTopSum {
		q15Upds, q15UpdsStats = ti.countQ15Upds(q15.updEntries)
	} else {
		q15Upds, q15UpdsStats = ti.countQ15TopSumUpds(q15TopSum.diffEntries)
	}
	nUpds += q15Upds
	nUpdsStats += q15UpdsStats

	//Remove this
	/*
		q5Upds := 0
		if *q5.remValue != 0 {
			q5Upds++
		}
		if *q5.updValue != 0 {
			q5Upds++
		}
		fmt.Println(nUpds, nUpdsStats, q3.totalUpds, q5Upds, len(q14.mapTotal), q15Upds, q15UpdsStats, ti.countQ18Upds(q18.remQuantity), ti.countQ18Upds(q18.updQuantity))
	*/
	return
}

func (ti SingleTableInfo) countQ15TopSumUpds(diffEntries map[int16]map[int8]map[int32]float64) (nUpds, nPosUpd int) {
	if !useTopKAll {
		for _, monthMap := range diffEntries {
			for _, suppMap := range monthMap {
				nUpds += len(suppMap)
			}
		}
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
	return nUpds, nUpds
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
}

type SingleQ3 struct {
	remMap      map[int8]struct{}
	updMap      map[int8]*float64
	updStartPos int8
	totalUpds   int
	newSeg      string
	oldSeg      string
}

func (ti SingleTableInfo) getSingleQ3Upds(remOrder *Orders, remItems []*LineItem, newOrder *Orders, newItems []*LineItem) SingleQ3 {

	//Single segment, but multiple days (days are cumulative, and *possibly?* items may have different days)

	var remMap map[int8]struct{}
	var updMap map[int8]*float64
	updStart, nUpds := int8(0), 0

	if newOrder.O_ORDERDATE.isSmallerOrEqual(MAX_DATE_Q3) {
		//Need to do add
		updMap, updStart = ti.q3SingleUpdsCalcHelper(newOrder, newItems)
		nUpds += int(MAX_MONTH_DAY - updStart + 1)
	}
	//Single segment, but multiple days (as days are cumulative).
	if remOrder.O_ORDERDATE.isSmallerOrEqual(MAX_DATE_Q3) {
		//Need to do rem
		remMap = ti.q3SingleRemsCalcHelper(remOrder, remItems)
		nUpds += len(remMap)
	}

	return SingleQ3{remMap: remMap, updMap: updMap, updStartPos: updStart, totalUpds: nUpds,
		newSeg: ti.Tables.Customers[newOrder.O_CUSTKEY].C_MKTSEGMENT, oldSeg: ti.Tables.Customers[remOrder.O_CUSTKEY].C_MKTSEGMENT}

}

func (ti SingleTableInfo) q3SingleRemsCalcHelper(order *Orders, items []*LineItem) (remMap map[int8]struct{}) {
	remMap = make(map[int8]struct{})
	minDay, j := MIN_MONTH_DAY, int8(0)
	for _, item := range items {
		if item.L_SHIPDATE.isHigherOrEqual(MIN_DATE_Q3) {
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

func (ti SingleTableInfo) q3SingleUpdsCalcHelper(order *Orders, items []*LineItem) (updMap map[int8]*float64, dayStart int8) {
	updMap = make(map[int8]*float64)
	i, j, minDay := int8(1), int8(1), int8(1)
	for ; i <= 31; i++ {
		updMap[i] = new(float64)
	}
	dayStart = MAX_MONTH_DAY + 1 //+1 so that the sum on getSingleQ3Upds ends as 0 if no upd is to be done.
	for _, item := range items {
		if item.L_SHIPDATE.isHigherOrEqual(MIN_DATE_Q3) {
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

type SingleQ5 struct {
	remValue, updValue *float64
}

func (ti SingleTableInfo) getSingleQ5Upds(remOrder *Orders, remItems []*LineItem, newOrder *Orders, newItems []*LineItem) SingleQ5 {
	//One nation (supplier & customer in the same nation). Thus, one decrement and one increment?
	//That if the supplier and nation match.

	remValue := new(float64)
	*remValue -= ti.q5SingleUpdsCalcHelper(remOrder, remItems)

	updValue := new(float64)
	*updValue += ti.q5SingleUpdsCalcHelper(newOrder, newItems)

	return SingleQ5{remValue: remValue, updValue: updValue}
}

func (ti SingleTableInfo) q5SingleUpdsCalcHelper(order *Orders, items []*LineItem) (value float64) {
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

type SingleQ14 struct {
	mapPromo, mapTotal map[string]*float64
}

type SingleLocalQ14 struct {
	mapPromoRem, mapTotalRem, mapPromoUpd, mapTotalUpd map[string]*float64
}

func (ti SingleTableInfo) getSingleLocalQ14Upds(remOrder *Orders, remItems []*LineItem, newOrder *Orders, newItems []*LineItem) SingleLocalQ14 {
	mapPromoUpd, mapTotalUpd, mapPromoRem, mapTotalRem := make(map[string]*float64), make(map[string]*float64),
		make(map[string]*float64), make(map[string]*float64)

	ti.q14SingleUpdsCalcHelper(-1, remOrder, remItems, mapPromoRem, mapTotalRem, ti.Tables.PromoParts)
	ti.q14SingleUpdsCalcHelper(1, newOrder, newItems, mapPromoUpd, mapTotalUpd, ti.Tables.PromoParts)

	return SingleLocalQ14{mapPromoRem: mapPromoRem, mapTotalRem: mapTotalRem, mapPromoUpd: mapPromoUpd, mapTotalUpd: mapTotalUpd}
}

func (ti SingleTableInfo) getSingleQ14Upds(remOrder *Orders, remItems []*LineItem, newOrder *Orders, newItems []*LineItem) SingleQ14 {
	//May have multiple updates, as not all items in an order may have been shipped in the same date
	mapPromo, mapTotal := make(map[string]*float64), make(map[string]*float64)

	ti.q14SingleUpdsCalcHelper(-1, remOrder, remItems, mapPromo, mapTotal, ti.Tables.PromoParts)
	ti.q14SingleUpdsCalcHelper(1, newOrder, newItems, mapPromo, mapTotal, ti.Tables.PromoParts)

	return SingleQ14{mapPromo: mapPromo, mapTotal: mapTotal}
}

func (ti SingleTableInfo) q14SingleUpdsCalcHelper(multiplier float64, order *Orders, items []*LineItem,
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

type SingleLocalQ15TopSum struct {
	updEntries, remEntries map[int16]map[int8]map[int32]float64
}

type SingleQ15TopSum struct {
	diffEntries map[int16]map[int8]map[int32]float64
}

func (ti SingleTableInfo) getSingleLocalQ15TopSumUpds(remOrder *Orders, remItems []*LineItem, newOrder *Orders, newItems []*LineItem,
	remKey, updKey int8) SingleLocalQ15TopSum {
	remEntries, updEntries := createQ15TopSumEntriesMap(), createQ15TopSumEntriesMap()
	q15TopSumUpdsRemCalcHelper(remItems, q15LocalMap[remKey], remEntries)
	q15TopSumUpdsNewCalcHelper(newItems, q15LocalMap[updKey], updEntries)
	return SingleLocalQ15TopSum{remEntries: remEntries, updEntries: updEntries}
}

func (ti SingleTableInfo) getSingleQ15TopSumUpds(remOrder *Orders, remItems []*LineItem, newOrder *Orders, newItems []*LineItem) SingleQ15TopSum {

	diffEntries := createQ15TopSumEntriesMap()
	q15TopSumUpdsRemCalcHelper(remItems, q15Map, diffEntries)
	q15TopSumUpdsNewCalcHelper(newItems, q15Map, diffEntries)

	return SingleQ15TopSum{diffEntries: diffEntries}
}

type SingleLocalQ15 struct {
	remEntries, updEntries map[int16]map[int8]map[int32]struct{}
}

type SingleQ15 struct {
	updEntries map[int16]map[int8]map[int32]struct{}
}

func (ti SingleTableInfo) getSingleLocalQ15Upds(remOrder *Orders, remItems []*LineItem, newOrder *Orders, newItems []*LineItem,
	remKey, updKey int8) SingleLocalQ15 {
	remEntries, updEntries := createQ15EntriesMap(), createQ15EntriesMap()
	q15UpdsRemCalcHelper(remItems, q15LocalMap[remKey], remEntries)
	q15UpdsNewCalcHelper(newItems, q15LocalMap[updKey], updEntries)
	return SingleLocalQ15{remEntries: remEntries, updEntries: updEntries}
}

func (ti SingleTableInfo) getSingleQ15Upds(remOrder *Orders, remItems []*LineItem, newOrder *Orders, newItems []*LineItem) SingleQ15 {
	//Different items may have different suppliers... thus, multiple updates.
	//Also shipdates are potencially different.

	updEntries := createQ15EntriesMap()
	q15UpdsRemCalcHelper(remItems, q15Map, updEntries)
	q15UpdsNewCalcHelper(newItems, q15Map, updEntries)

	return SingleQ15{updEntries: updEntries}
}

type SingleQ18 struct {
	remQuantity, updQuantity int64
}

func (ti SingleTableInfo) getSingleQ18Upds(remOrder *Orders, remItems []*LineItem, newOrder *Orders, newItems []*LineItem) SingleQ18 {

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

func (ti SingleTableInfo) makeSingleQ3UpdsArgsRems(q3Info SingleQ3, remOrder *Orders, buf []antidote.UpdateObjectParams,
	bufI, bucketI int) (newBufI int) {

	var keyArgs antidote.KeyParams
	//Rems
	if len(q3Info.remMap) > 0 {
		remSegKey := SEGM_DELAY + q3Info.oldSeg
		for day := range q3Info.remMap {
			keyArgs = antidote.KeyParams{
				Key:      remSegKey + strconv.FormatInt(int64(day), 10),
				CrdtType: proto.CRDTType_TOPK_RMV,
				Bucket:   buckets[bucketI],
			}
			//useTopKAll not relevant as it is only one order
			var currUpd crdt.UpdateArguments = crdt.TopKRemove{Id: remOrder.O_ORDERKEY}
			buf[bufI] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
			bufI++
		}
	}
	return bufI
}

func (ti SingleTableInfo) makeSingleQ3UpdsArgsNews(q3Info SingleQ3, newOrder *Orders, buf []antidote.UpdateObjectParams,
	bufI, bucketI int) (newBufI int) {

	var keyArgs antidote.KeyParams
	//Adds. Due to the date restriction, there may be no adds.
	if q3Info.updStartPos != 0 && q3Info.updStartPos <= MAX_MONTH_DAY {
		updSegKey := SEGM_DELAY + q3Info.newSeg
		for day := q3Info.updStartPos; day <= MAX_MONTH_DAY; day++ {
			keyArgs = antidote.KeyParams{
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
			buf[bufI] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
			bufI++
		}
	}
	return bufI
}

func (ti SingleTableInfo) makeSingleQ3UpdArgs(q3Info SingleQ3, newOrder *Orders, remOrder *Orders,
	buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {

	bufI = ti.makeSingleQ3UpdsArgsRems(q3Info, remOrder, buf, bufI, bucketI)
	bufI = ti.makeSingleQ3UpdsArgsNews(q3Info, newOrder, buf, bufI, bucketI)

	return bufI
}

func (ti SingleTableInfo) makeSingleQ5UpdArgsHelper(value float64, signal float64, remOrder *Orders,
	buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {

	if value != 0 {
		remYear, remNation := remOrder.O_ORDERDATE.YEAR, ti.Tables.Nations[ti.Tables.Customers[remOrder.O_CUSTKEY].C_NATIONKEY]
		embMapUpd := crdt.EmbMapUpdate{Key: remNation.N_NAME, Upd: crdt.Increment{Change: int32(signal * value)}}
		var args crdt.UpdateArguments = embMapUpd
		buf[bufI] = antidote.UpdateObjectParams{
			KeyParams: antidote.KeyParams{Key: NATION_REVENUE + ti.Tables.Regions[remNation.N_REGIONKEY].R_NAME + strconv.FormatInt(int64(remYear-1993), 10),
				CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]},
			UpdateArgs: &args,
		}
		return bufI + 1
	}
	return bufI
}

func (ti SingleTableInfo) makeSingleQ5UpdArgs(q5Info SingleQ5, newOrder *Orders, remOrder *Orders,
	buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {

	bufI = ti.makeSingleQ5UpdArgsHelper(*q5Info.remValue, -1.0, remOrder, buf, bufI, bucketI)
	bufI = ti.makeSingleQ5UpdArgsHelper(*q5Info.updValue, 1.0, newOrder, buf, bufI, bucketI)

	return bufI
}

func (ti SingleTableInfo) makeSingleQ14UpdArgs(q14Info SingleQ14, newOrder *Orders, remOrder *Orders,
	buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {

	for key, totalP := range q14Info.mapTotal {
		promo := *q14Info.mapPromo[key]
		var currUpd crdt.UpdateArguments = crdt.AddMultipleValue{
			SumValue: int64(100.0 * promo),
			NAdds:    int64(*totalP),
		}
		buf[bufI] = antidote.UpdateObjectParams{
			KeyParams:  antidote.KeyParams{Key: PROMO_PERCENTAGE + key, CrdtType: proto.CRDTType_AVG, Bucket: buckets[bucketI]},
			UpdateArgs: &currUpd,
		}
		bufI++
	}

	return bufI
}

func (ti SingleTableInfo) makeSingleLocalQ15TopSumUpdArgs(q15Info SingleQ15TopSum, newOrder *Orders, remOrder *Orders,
	buf []antidote.UpdateObjectParams, bufI int, bucketI int, regKey int8) (newBufI int) {

	newBufI, _ = makeQ15TopSumIndexUpdsDeletesHelper(q15LocalMap[regKey], q15Info.diffEntries, bucketI, buf, bufI)
	return newBufI
}

func (ti SingleTableInfo) makeSingleQ15TopSumUpdArgs(q15Info SingleQ15TopSum, newOrder *Orders, remOrder *Orders,
	buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {

	newBufI, _ = makeQ15TopSumIndexUpdsDeletesHelper(q15Map, q15Info.diffEntries, INDEX_BKT, buf, bufI)
	return newBufI
}

func (ti SingleTableInfo) makeSingleLocalQ15UpdArgs(q15Info SingleQ15, newOrder *Orders, remOrder *Orders,
	buf []antidote.UpdateObjectParams, bufI int, bucketI int, regKey int8) (newBufI int) {

	newBufI, _ = makeQ15IndexUpdsDeletesHelper(q15LocalMap[regKey], q15Info.updEntries, bucketI, buf, bufI)
	return newBufI
}

func (ti SingleTableInfo) makeSingleQ15UpdArgs(q15Info SingleQ15, newOrder *Orders, remOrder *Orders,
	buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {

	newBufI, _ = makeQ15IndexUpdsDeletesHelper(q15Map, q15Info.updEntries, INDEX_BKT, buf, bufI)
	return newBufI
}

func (ti SingleTableInfo) makeSingleQ18UpdsArgsRems(q18Info SingleQ18, remOrder *Orders,
	buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {

	remQ := q18Info.remQuantity
	var keyArgs antidote.KeyParams

	if remQ >= 312 {
		if remQ > 315 {
			remQ = 315
		}
		for i := int64(312); i <= remQ; i++ {
			keyArgs = antidote.KeyParams{
				Key:      LARGE_ORDERS + strconv.FormatInt(i, 10),
				CrdtType: proto.CRDTType_TOPK_RMV,
				Bucket:   buckets[bucketI],
			}
			//useTopKAll is irrelevant since it is only 1 order
			var currUpd crdt.UpdateArguments = crdt.TopKRemove{Id: remOrder.O_ORDERKEY}
			buf[bufI] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
			bufI++
		}
	}
	return bufI
}

func (ti SingleTableInfo) makeSingleQ18UpdsArgsNews(q18Info SingleQ18, newOrder *Orders,
	buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {

	updQ := q18Info.updQuantity
	var keyArgs antidote.KeyParams

	if updQ >= 312 {
		//fmt.Println("[Q18]Upd quantity is actually >= 312", updQ, newOrder.O_ORDERKEY)
		if updQ > 315 {
			updQ = 315
		}
		for i := int64(312); i <= updQ; i++ {
			keyArgs = antidote.KeyParams{
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
			buf[bufI] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
			bufI++
		}
	}
	return bufI
}

func (ti SingleTableInfo) makeSingleQ18UpdArgs(q18Info SingleQ18, newOrder *Orders, remOrder *Orders,
	buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {

	bufI = ti.makeSingleQ18UpdsArgsRems(q18Info, remOrder, buf, bufI, bucketI)
	bufI = ti.makeSingleQ18UpdsArgsNews(q18Info, remOrder, buf, bufI, bucketI)

	return bufI
}

func doMixedStatsInterval() {
	for {
		newBoolSlice := make([]bool, TEST_ROUTINES)
		for i := range newBoolSlice {
			newBoolSlice[i] = true
		}
		time.Sleep(statisticsInterval * time.Millisecond)
		collectUpdStats = true
		collectUpdStatsComm = []bool{true, true, true, true, true}
		collectQueryStats = newBoolSlice
	}
}

//TODO: Need to update the conversions most likely
func writeMixStatsFile(stats []MixClientResult) {
	//I should write the total, avg, best and worse for each part.
	//Also write the total, avg, best and worse for the "final"

	//TODO: Latency

	qStatsPerPart, uStatsPerPart := convertMixStats(stats)
	nFuncs := len(queryFuncs)

	//Cutting out first entry as "warmup"
	qStatsPerPart, uStatsPerPart = qStatsPerPart[1:len(qStatsPerPart)], uStatsPerPart[1:len(uStatsPerPart)]

	totalData := make([][]string, len(qStatsPerPart)+1) //space for final data as well
	var latencyString []string

	if LATENCY_MODE == AVG_OP {
		latencyString = []string{"Average latency (ms)(AO)"}
	} else if LATENCY_MODE == AVG_BATCH {
		latencyString = []string{"Average latency (ms)(AB)", "Average latency (ms)(AO)"}
	} else { //PER_OP
		latencyString = []string{"AvgQ latency", "AvgU latency", "AvgAll latency (ms)", "Avg latency (ms)(AO)"}
	}

	header := append([]string{"Total time", "Section time", "Queries cycles", "Queries", "Reads", "Query cycles/s", "Query/s", "Read/s",
		"Query txns", "Query txns/s", "Updates", "Updates/s", "New upds", "Del upds", "Index upds", "Update txns", "Update txns/s", "Ops", "Ops/s",
		"Txns", "Txn/s"}, latencyString...)

	partQueries, partReads, partTime, partQueryCycles, partUpds, partNews, partDels, partIndexes, partQTime, partUTime := 0, 0, int64(0), 0, 0, 0, 0, 0, int64(0), int64(0)
	partQTxns, partUTxns := 0, 0
	totalQueries, totalReads, totalTime, totalNews, totalDels, totalIndexes, totalQTime, totalUTime, totalQTxns, totalUTxns := 0, 0, int64(0), 0, 0, 0, int64(0), int64(0), 0, 0
	queryCycleS, queryS, readS, updS, latency, latencyPerOp, qTxnsS, uTxnsS := 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0

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
		}
		partQueryCycles, partUpds = partQueries/nFuncs, partNews+partDels+partIndexes
		partTime /= int64(TEST_ROUTINES)
		queryCycleS, queryS = (float64(partQueryCycles)/float64(partTime))*1000, (float64(partQueries)/float64(partTime))*1000
		updS, readS = (float64(partUpds)/float64(partTime))*1000, (float64(partReads)/float64(partTime))*1000
		qTxnsS, uTxnsS = (float64(partQTxns)/float64(partTime))*1000, (float64(partUTxns)/float64(partTime))*1000
		latencyPerOp = float64(partTime*int64(TEST_ROUTINES)) / float64(partQueries+partUpds)
		latency = getStatsLatency(partTime, partQTime, partUTime, partQueries, partUpds, partQTxns, partUTxns)
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

		//header := append([]string{"Total time", "Section time", "Queries cycles", "Queries", "Reads", "Query cycles/s", "Query/s", "Read/s",
		//"Query txns", "Query txns/s", "Updates", "Updates/s", "New upds", "Del upds", "Index upds", "Update txns", "Update txns/s", "Ops", "Ops/s",
		//"Txns", "Txn/s"}, latencyString...)

		totalData[i] = []string{strconv.FormatInt(totalTime, 10), strconv.FormatInt(partTime, 10),
			strconv.FormatInt(int64(partQueryCycles), 10), strconv.FormatInt(int64(partQueries), 10), strconv.FormatInt(int64(partReads), 10),
			strconv.FormatFloat(queryCycleS, 'f', 10, 64), strconv.FormatFloat(queryS, 'f', 10, 64),
			strconv.FormatFloat(readS, 'f', 10, 64), strconv.FormatInt(int64(partQTxns), 10), strconv.FormatFloat(qTxnsS, 'f', 10, 64),
			strconv.FormatInt(int64(partUpds), 10), strconv.FormatFloat(updS, 'f', 10, 64),
			strconv.FormatInt(int64(partNews), 10), strconv.FormatInt(int64(partDels), 10), strconv.FormatInt(int64(partIndexes), 10),
			strconv.FormatInt(int64(partUTxns), 10), strconv.FormatFloat(uTxnsS, 'f', 10, 64),
			strconv.FormatInt(int64(partQueries+partUpds), 10), strconv.FormatFloat(queryS+updS, 'f', 10, 64),
			strconv.FormatInt(int64(partQTxns+partUTxns), 10), strconv.FormatFloat(qTxnsS+uTxnsS, 'f', 10, 64)}

		if LATENCY_MODE == PER_BATCH {
			qLatency, uLatency := float64(partQTime)/float64(partQTxns), float64(partUTime)/float64(partUTxns)
			totalData[i] = append(totalData[i], strconv.FormatFloat(qLatency, 'f', 10, 64), strconv.FormatFloat(uLatency, 'f', 10, 64), strconv.FormatFloat(latency, 'f', 10, 64))
			//fmt.Printf("Time, qTime, uTime, qTxns, uTxns, qLatency, uLatency: %d %d %d %d %d %f %f\n", totalTime, totalQTime, totalUTime, totalQTxns, totalUTxns, qLatency, uLatency)
		} else if LATENCY_MODE == AVG_BATCH {
			totalData[i] = append(totalData[i], strconv.FormatFloat(latency, 'f', 10, 64))
		}
		//All modes use AVG_OP
		totalData[i] = append(totalData[i], strconv.FormatFloat(latencyPerOp, 'f', 10, 64))
		//qLatency, uLatency = float64(partQTxns)/float64(partQTime), float64(partUTxns)/float64(partUTime)

		partQueryCycles, partQueries, partReads, partTime, partUpds, partNews, partDels, partIndexes, partQTime, partQTxns, partUTime, partUTxns = 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
	}

	totalQueryTxns, totalUpds := totalQueries/nFuncs, totalNews+totalDels+totalIndexes
	queryCycleS, queryS, updS = (float64(totalQueryTxns)/float64(totalTime))*1000, (float64(totalQueries)/float64(totalTime))*1000, (float64(totalUpds)/float64(totalTime))*1000
	readS = (float64(totalReads) / float64(totalTime)) * 1000
	qTxnsS, uTxnsS = (float64(totalQTxns)/float64(totalTime))*1000, (float64(totalUTxns)/float64(totalTime))*1000
	latencyPerOp = float64(totalTime*int64(TEST_ROUTINES)) / float64(totalQueries+totalUpds)
	latency = getStatsLatency(totalTime, totalQTime, totalUTime, totalQueries, totalUpds, totalQTxns, totalUTxns)

	finalData := []string{strconv.FormatInt(totalTime, 10), strconv.FormatInt(totalTime, 10),
		strconv.FormatInt(int64(totalQueryTxns), 10), strconv.FormatInt(int64(totalQueries), 10), strconv.FormatInt(int64(totalReads), 10),
		strconv.FormatFloat(queryCycleS, 'f', 10, 64), strconv.FormatFloat(queryS, 'f', 10, 64),
		strconv.FormatFloat(readS, 'f', 10, 64), strconv.FormatInt(int64(totalQTxns), 10), strconv.FormatFloat(qTxnsS, 'f', 10, 64),
		strconv.FormatInt(int64(totalUpds), 10), strconv.FormatFloat(updS, 'f', 10, 64),
		strconv.FormatInt(int64(totalNews), 10), strconv.FormatInt(int64(totalDels), 10), strconv.FormatInt(int64(totalIndexes), 10),
		strconv.FormatInt(int64(totalUTxns), 10), strconv.FormatFloat(uTxnsS, 'f', 10, 64),
		strconv.FormatInt(int64(totalQueries+totalUpds), 10), strconv.FormatFloat(queryS+updS, 'f', 10, 64),
		strconv.FormatInt(int64(totalQTxns+totalUTxns), 10), strconv.FormatFloat(qTxnsS+uTxnsS, 'f', 10, 64)}

	if LATENCY_MODE == PER_BATCH {
		qLatency, uLatency := float64(totalQTime)/float64(totalQTxns), float64(totalUTime)/float64(totalUTxns)
		finalData = append(finalData, strconv.FormatFloat(qLatency, 'f', 10, 64), strconv.FormatFloat(uLatency, 'f', 10, 64), strconv.FormatFloat(latency, 'f', 10, 64))
	} else if LATENCY_MODE == AVG_BATCH {
		finalData = append(finalData, strconv.FormatFloat(latency, 'f', 10, 64))
	}
	//All modes use AVG_OP
	finalData = append(finalData, strconv.FormatFloat(latencyPerOp, 'f', 10, 64))

	totalData[len(totalData)-1] = finalData

	file := getStatsFileToWrite("mixStats")
	if file == nil {
		return
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	writer.Comma = ';'
	defer writer.Flush()

	writer.Write(header)
	for _, line := range totalData {
		writer.Write(line)
	}

	fmt.Println("Mix statistics saved successfully to " + file.Name())
}

func getStatsLatency(partTime, qTime, uTime int64, nQueries, nUpds, nQueryTxns, nUpdTxns int) float64 {
	if LATENCY_MODE == AVG_OP {
		return float64(partTime*int64(TEST_ROUTINES)) / float64(nQueries+nUpds)
	} else if LATENCY_MODE == AVG_BATCH {
		return float64(partTime*int64(TEST_ROUTINES)) / float64(nQueryTxns+nUpdTxns)
	}
	return float64(qTime+uTime) / float64(nQueryTxns+nUpdTxns)
}

//[nClients] -> [time][clients]
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
