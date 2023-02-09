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
)

func startMixBench() {
	maxServers := len(servers)
	RECORD_LATENCY = (LATENCY_MODE == PER_BATCH)

	fmt.Println("Reading updates...")
	readStart := time.Now().UnixNano()
	ordersUpds, lineItemUpds, deleteKeys, lineItemSizes, itemSizesPerOrder := readUpdsByOrder()
	readFinish := time.Now().UnixNano()
	fmt.Println("Finished reading updates. Time taken for read:", (readFinish-readStart)/1000000, "ms")

	//connectToServers()
	//Start server communication for update sending
	/*
		for i := range channels.dataChans {
			go handleUpdatesComm(i)
		}
	*/

	if N_UPDATE_FILES < TEST_ROUTINES {
		fmt.Println("Error - not enough update files for all clients. Aborting.")
		os.Exit(0)
	}

	fmt.Println("Splitting updates")
	N_UPDATE_FILES = FINISH_UPD_FILE - START_UPD_FILE + 1 //for splitUpdatesPerRoutine
	filesPerRoutine, tableInfos, routineOrders, routineItems, routineDelete, routineLineSizes :=
		splitUpdatesPerRoutine(TEST_ROUTINES, ordersUpds, lineItemUpds, deleteKeys, lineItemSizes)
	fmt.Println("Updates split.")

	/*
		for i := 0; i < TEST_ROUTINES; i++ {
			fmt.Printf("%d: %d %d %d %d %d\n", i, filesPerRoutine, len(routineOrders[i]), len(routineItems[i]),
				len(routineDelete[i]), len(routineLineSizes[i]))
		}
	*/

	/*
		fmt.Println("Last goroutine updates info:")
		last := TEST_ROUTINES - 1
		fmt.Printf("Files: %d, orders: %d, items: %d, deletes: %d, lineSizes: %d\n", filesPerRoutine, len(routineOrders[last]),
			len(routineItems[last]), len(routineDelete[last]), len(routineLineSizes[last]))
		fmt.Println("Before last:")
		fmt.Printf("Files: %d, orders: %d, items: %d, deletes: %d, lineSizes: %d\n", filesPerRoutine, len(routineOrders[last-1]),
			len(routineItems[last-1]), len(routineDelete[last-1]), len(routineLineSizes[last-1]))
	*/

	singleTableInfos, seed := make([]SingleTableInfo, len(tableInfos)), time.Now().UnixNano()
	lastRoutineLineSize, currRoutineLineSize := 0, 0
	for i, info := range tableInfos {
		//Fixing routineLineSizes to be per order
		currRoutineLineSize += len(routineOrders[i])
		routineLineSizes[i] = itemSizesPerOrder[lastRoutineLineSize:currRoutineLineSize]
		lastRoutineLineSize = currRoutineLineSize
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
		serverPerClient[i] = selfRng.Intn(maxServers)
		collectQueryStats[i] = false
	}

	fmt.Printf("Waiting to start queries & upds... (ops per txn: %d; batch mode: %d; latency mode: %d)\n", READS_PER_TXN, BATCH_MODE, LATENCY_MODE)
	time.Sleep(QUERY_WAIT*1000000 - time.Duration(time.Now().UnixNano()-times.startTime))
	fmt.Println("Starting queries & upds...")
	fmt.Println("Is using split: ", SPLIT_UPDATES)

	if statisticsInterval > 0 {
		fmt.Println("Called mixedInterval at", time.Now().String())
		go doMixedStatsInterval()
	}

	for i := 0; i < TEST_ROUTINES; i++ {
		fmt.Printf("Value of update stats for client %d: %t, started at: %s\n", i, collectQueryStats[i], time.Now().String())
		go mixBench(0, i, serverPerClient[i], chans[i], filesPerRoutine, singleTableInfos[i],
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
	if diffT < int64(statisticsInterval/100) && len(queryStats) > 0 {
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

func mixBench(seed int64, clientN int, defaultServer int, resultChan chan MixClientResult,
	routineFiles int, tableInfo SingleTableInfo, orders, items [][]string,
	delete []string, lineSizes []int) {

	fmt.Println("Mix bench started at", time.Now().String())

	tableInfo.indexServer = defaultServer
	if SINGLE_INDEX_SERVER || !splitIndexLoad {
		tableInfo.indexServer = 0
		defaultServer = 0
	}

	//Counters and common preparation
	random := 0.0
	qDone, updsDone, reads := 0, 0, 0
	waitForUpds := 0 //number of times updProb must be met before doing the next update batch
	conns := make([]net.Conn, len(servers))
	for i := range conns {
		conns[i], _ = net.Dial("tcp", servers[i])
	}
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

	if READS_PER_TXN > 1 {
		queryPos, nOpsTxn, previousQueryPos, replyPos, nUpdsDone := 0, 0, 0, 0, 0
		fullRBuf, partialRBuf := make([]antidote.ReadObjectParams, READS_PER_TXN+1),
			make([]antidote.ReadObjectParams, READS_PER_TXN+1) //+1 as one of the queries has 2 reads.
		updBuf := make([][]antidote.UpdateObjectParams, len(conns))
		updBufI, readBufI := make([]int, len(conns)), make([]int, 2)
		for i := range updBuf {
			updBuf[i] = make([]antidote.UpdateObjectParams, int(math.Max(100.0, float64(READS_PER_TXN*2)))) //100 is way more than enough for a set of updates.
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
				if waitForUpds > READS_PER_TXN {
					waitForUpds -= READS_PER_TXN
				} else {
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
						nOpsTxn = getReadsFuncs[queryPos%nQueries](client, fullRBuf, partialRBuf, readBufI, nOpsTxn)
						qDone++
						queryPos++
					}
				} else {
					queryPos = (queryPos + 1) % nQueries
					for nOpsTxn < READS_PER_TXN {
						nOpsTxn = getReadsFuncs[queryPos](client, fullRBuf, partialRBuf, readBufI, nOpsTxn)
						qDone++
					}
					previousQueryPos = queryPos
				}

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
				reads += nOpsTxn
				readTxns++
				previousQueryPos, nOpsTxn, replyPos, readBufI[0], readBufI[1] = queryPos, 0, 0, 0, 0

				currQuerySpentTime += recordFinishLatency(startTxnTime)
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

func (ti SingleTableInfo) sendReceiveMultipleUpdates(updBuf [][]antidote.UpdateObjectParams, bufI []int) {
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

	return nDels + nAdds
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

func (ti SingleTableInfo) getSingleDataChange(updParams [][]antidote.UpdateObjectParams, bufI []int, order []string, lineItems [][]string, deleteKey string) (updsDone int) {
	deletedOrder, deletedItems := ti.getNextDelete(deleteKey)
	nDels, itemsDelPerServer, itemsIndex := ti.getSingleDelete(deletedOrder, deletedItems)
	nAdds, itemsNewPerServer := ti.getSingleUpd(order, lineItems)
	nIndexUpds := 0

	//fmt.Println(bufI)
	if UPDATE_BASE_DATA {
		ti.makeSingleDelete(deletedOrder, updParams, bufI, itemsDelPerServer, itemsIndex)
		//fmt.Println(bufI)
		ti.makeSingleUpd(order, lineItems, updParams, bufI, itemsNewPerServer)
		//fmt.Println(bufI)
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

func (ti SingleTableInfo) sendSingleDataChange(order []string, lineItems [][]string, deleteKey string) (updsDone int) {
	deletedOrder, deletedItems := ti.getNextDelete(deleteKey)
	newOrder := ti.CreateOrder(order)
	nDels, itemsDelPerServer, itemsIndex := ti.getSingleDelete(deletedOrder, deletedItems)
	nAdds, itemsNewPerServer := ti.getSingleUpd(order, lineItems)
	nUpdsN, nUpdsR := 0, 0
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
				if i == ti.indexServer {
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
		} else {
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
		}
	}

	//fmt.Println(bufI)
	if UPDATE_BASE_DATA {
		ti.makeSingleDelete(deletedOrder, updParams, bufI, itemsDelPerServer, itemsIndex)
		//fmt.Println(bufI)
		ti.makeSingleUpd(order, lineItems, updParams, bufI, itemsNewPerServer)
		//fmt.Println(bufI)
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

	if UPDATE_BASE_DATA {
		return nDels + nAdds
	} else {
		return 0
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
	var upd *crdt.MapAddAll
	var itemRegions []int8

	for _, item := range lineItemUpds {
		key, upd = getEntryORMapUpd(headers[LINEITEM], keys[LINEITEM], item, read[LINEITEM])
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

	orderKey, orderMapUpd := getEntryORMapUpd(headers[ORDERS], keys[ORDERS], orderUpd, read[ORDERS])
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
