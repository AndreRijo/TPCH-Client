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
	"strings"
	"time"

	pb "github.com/golang/protobuf/proto"
)

//In order to allow multiple independent clients executing queries, each client must
//have one instance of this object, which will contain his private variables (namelly,
//its own connections)
type QueryClient struct {
	serverConns []net.Conn
	indexServer int
	rng         *rand.Rand
}

type QueryClientResult struct {
	duration, nReads, nQueries float64
	intermediateResults        []QueryStats
}

const (
	CYCLE, SINGLE = 0, 1 //BATCH_MODE
)

var (
	//Filled automatically by configLoader
	PRINT_QUERY, QUERY_BENCH bool
	TEST_ROUTINES            int
	TEST_DURATION            int64
	STOP_QUERIES             bool //Becomes true when the execution time for the queries is over
	READS_PER_TXN            int
	QUERY_WAIT               time.Duration
	BATCH_MODE               int                     //CYCLE or SINGLE. Only supported by mix clients atm.
	queryFuncs               []func(QueryClient) int //queries to execute
	collectQueryStats        []bool                  //Becomes true when enough time has passed to collect statistics again. One entry per queryClient
	getReadsFuncs            []func(QueryClient, []antidote.ReadObjectParams, []antidote.ReadObjectParams, []int, int) int
	processReadReplies       []func(QueryClient, []*proto.ApbReadObjectResp, []int, int) int //int[]: fullReadPos, partialReadPos. int: reads done
	q15CrdtType              proto.CRDTType                                                  //Q15 can be implemented with either TopK or TopSum.
)

func startQueriesBench() {
	printExecutionTimes()
	maxServers := len(servers)
	if SINGLE_INDEX_SERVER {
		maxServers = 1
	}
	fmt.Printf("Waiting to start for queries with %d clients...\n", TEST_ROUTINES)
	collectQueryStats = make([]bool, TEST_ROUTINES)
	for i := range collectQueryStats {
		collectQueryStats[i] = false
	}
	rngs := make([]*rand.Rand, TEST_ROUTINES)
	seed := time.Now().UnixNano()
	for i := range rngs {
		rngs[i] = rand.New(rand.NewSource(seed + int64(i)))
	}
	selfRng := rand.New(rand.NewSource(seed + int64(2*TEST_ROUTINES)))
	time.Sleep(QUERY_WAIT * time.Millisecond)
	fmt.Println("Starting queries")

	chans := make([]chan QueryClientResult, TEST_ROUTINES)
	results := make([]QueryClientResult, TEST_ROUTINES)
	serverPerClient := make([]int, TEST_ROUTINES)
	for i := 0; i < TEST_ROUTINES; i++ {
		serverN := selfRng.Intn(maxServers)
		//serverN := 0
		//fmt.Println("Starting query client", i, "with index server", servers[serverN])
		chans[i] = make(chan QueryClientResult)
		serverPerClient[i] = serverN
		go queryBench(i, serverN, rngs[i], chans[i])
	}
	fmt.Println("Query clients started...")
	if statisticsInterval > 0 {
		go doQueryStatsInterval()
	}
	time.Sleep(time.Duration(TEST_DURATION) * time.Millisecond)
	STOP_QUERIES = true

	/*
		j := int32(0)
		for i, channel := range chans {
			go func(x int, channel chan QueryClientResult) {
				results[x] = <-channel
				fmt.Printf("Query client %d finished |", x)
				atomic.AddInt32(&j, 1)
			}(i, channel)
			//fmt.Printf("Query client %d finished |", i)
			//results[i] = <-channel
		}
		for j < int32(len(results)) {
			time.Sleep(100 * time.Millisecond)
		}
	*/

	for i, channel := range chans {
		results[i] = <-channel
	}
	fmt.Println()
	fmt.Println("All query clients have finished.")
	totalQueries, totalReads, avgDuration, nFuns := 0.0, 0.0, 0.0, float64(len(queryFuncs))
	for i, result := range results {
		fmt.Printf("%d[%d]: QueryTxns: %f, Queries: %f, QueryTxns/s: %f, Query/s: %f, Reads: %f, Reads/s: %f\n", i, serverPerClient[i],
			result.nQueries/nFuns, result.nQueries, (result.nQueries/(nFuns*result.duration))*1000,
			(result.nQueries/result.duration)*1000, result.nReads, (result.nReads/result.duration)*1000)
		totalQueries += result.nQueries
		totalReads += result.nReads
		avgDuration += result.duration
	}
	avgDuration /= float64(len(results))
	fmt.Printf("Totals: QueryTxns: %f, Queries: %f, QueryTxns/s: %f, Query/s: %f, Reads: %f, Reads/s: %f\n", totalQueries/nFuns, totalQueries,
		(totalQueries/(avgDuration*nFuns))*1000, (totalQueries/avgDuration)*1000, totalReads, (totalReads/avgDuration)*1000)

	writeQueriesStatsFile(results)
	os.Exit(0)
}

func updateQueryStats(stats []QueryStats, nReads, lastStatReads, nQueries, lastStatQueries int, lastStatTime int64) (newStats []QueryStats,
	newNReads, newNQueries int, newLastStatTime int64) {
	currStatTime, currQueryStats := time.Now().UnixNano()/1000000, QueryStats{}
	diffT, diffR, diffQ := currStatTime-lastStatTime, nReads-lastStatReads, nQueries-lastStatQueries
	if diffT < int64(statisticsInterval/100) {
		//Replace
		lastStatI := len(stats) - 1
		stats[lastStatI].nReads += diffR
		stats[lastStatI].nQueries += diffQ
		stats[lastStatI].timeSpent += diffT
	} else {
		currQueryStats.nReads, currQueryStats.nQueries, currQueryStats.timeSpent = diffR, diffQ, diffT
		stats = append(stats, currQueryStats)
	}
	return stats, nReads, nQueries, currStatTime
}

func queryBench(clientN int, defaultServer int, rng *rand.Rand, resultChan chan QueryClientResult) {
	queryStats := make([]QueryStats, 0, TEST_DURATION/int64(statisticsInterval)+1)
	conns := make([]net.Conn, len(servers))
	for i := range conns {
		conns[i], _ = net.Dial("tcp", servers[i])
	}
	client := QueryClient{serverConns: conns, indexServer: defaultServer, rng: rng}
	lastStatQueries, lastStatReads, cycleQueries := 0, 0, len(queryFuncs)
	queries, reads := 0, 0
	startTime := time.Now().UnixNano() / 1000000
	lastStatTime := startTime

	funcs := make([]func(QueryClient) int, len(queryFuncs))
	for i, fun := range queryFuncs {
		funcs[i] = fun
	}

	if READS_PER_TXN > 1 {
		queryPos, nOpsTxn, previousQueryPos, replyPos, bufI := 0, 0, 0, 0, make([]int, 2)
		fullRBuf, partialRBuf := make([]antidote.ReadObjectParams, READS_PER_TXN+1),
			make([]antidote.ReadObjectParams, READS_PER_TXN+1) //+1 as one of the queries has 2 reads.
		for !STOP_QUERIES {
			if collectQueryStats[clientN] {
				queryStats, lastStatReads, lastStatQueries, lastStatTime = updateQueryStats(queryStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime)
				collectQueryStats[clientN] = false
			}
			for nOpsTxn < READS_PER_TXN {
				nOpsTxn = getReadsFuncs[queryPos%cycleQueries](client, fullRBuf, partialRBuf, bufI, nOpsTxn)
				queries++
				queryPos++
			}
			readReplies := sendReceiveReadProto(client, fullRBuf[:bufI[0]], partialRBuf[:bufI[1]], client.indexServer).GetObjects().GetObjects()
			bufI[1] = bufI[0] //partial reads start when full reads end
			bufI[0] = 0
			for replyPos < len(readReplies) {
				replyPos = processReadReplies[previousQueryPos%cycleQueries](client, readReplies, bufI, replyPos)
				previousQueryPos++
			}
			reads += nOpsTxn
			previousQueryPos, nOpsTxn, replyPos, bufI[0], bufI[1] = queryPos, 0, 0, 0, 0
		}
	} else {
		for !STOP_QUERIES {
			if collectQueryStats[clientN] {
				queryStats, lastStatReads, lastStatQueries, lastStatTime = updateQueryStats(queryStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime)
				collectQueryStats[clientN] = false
			}
			for _, query := range funcs {
				reads += query(client)
			}
			queries += cycleQueries
		}
	}

	endTime := time.Now().UnixNano() / 1000000
	queryStats, _, _, _ = updateQueryStats(queryStats, reads, lastStatReads, queries, lastStatReads, lastStatTime)
	resultChan <- QueryClientResult{duration: float64(endTime - startTime), nQueries: float64(queries), nReads: float64(reads),
		intermediateResults: queryStats}
}

func sendQueries(conn net.Conn) {
	client := QueryClient{serverConns: conns, indexServer: DEFAULT_REPLICA, rng: rand.New(rand.NewSource(time.Now().UnixNano()))}
	ignore(client)
	time.Sleep(QUERY_WAIT * time.Millisecond)
	fmt.Println("Starting to send queries")
	startTime := time.Now().UnixNano() / 1000000
	fmt.Println()
	//Here used to be calls to queries
	endTime := time.Now().UnixNano()
	times.queries[5] = endTime / 1000000

	for i := len(times.queries) - 1; i > 0; i-- {
		times.queries[i] -= times.queries[i-1]
	}
	times.queries[0] -= startTime
	times.totalQueries = endTime/1000000 - startTime
	times.finishTime = endTime

	fmt.Println()
	fmt.Println("All queries have been executed")
	printExecutionTimes()
	fmt.Println()
	//fmt.Println("Starting to prepare and send upds")
	//sendDataChangesV2(readUpds())
}

func getTableState(client QueryClient, mapArgs crdt.EmbMapPartialArguments, tableName int, ids []int32,
	toRegionKey func(int32) int8) (states map[string]crdt.EmbMapEntryState) {
	if isMulti {
		readParams, idsArgs := getReadArgsPerBucket(tableName)
		for _, id := range ids {
			idsArgs[toRegionKey(id)][strconv.FormatInt(int64(id), 10)] = mapArgs
		}
		for i, args := range idsArgs {
			if len(args) == 0 {
				delete(readParams, i)
			}
		}

		replies := sendReceiveBucketReadProtos(client, readParams)
		states = mergeMapReplies(replies)
	} else {
		idsArgs := make(map[string]crdt.ReadArguments)
		for _, id := range ids {
			idsArgs[strconv.FormatInt(int64(id), 10)] = mapArgs
		}
		readParam := []antidote.ReadObjectParams{getMapReadParams(tableName, idsArgs, client.indexServer)}
		reply := sendReceiveReadProto(client, nil, readParam, client.indexServer)
		states = mergeMapReplies(map[int8]*proto.ApbStaticReadObjectsResp{0: reply})
	}
	return
}

func getMapReadParams(tableIndex int, innerArgs map[string]crdt.ReadArguments, bktI int) (readParams antidote.ReadObjectParams) {
	return antidote.ReadObjectParams{
		KeyParams: antidote.KeyParams{Key: tableNames[tableIndex], CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bktI]},
		ReadArgs:  crdt.EmbMapPartialArguments{Args: innerArgs},
	}

}

func getReadArgsPerBucket(tableIndex int) (readParams map[int8]antidote.ReadObjectParams, args map[int8]map[string]crdt.ReadArguments) {
	args = make(map[int8]map[string]crdt.ReadArguments)
	var i, regionLen int8 = 0, int8(len(procTables.Regions))
	readParams = make(map[int8]antidote.ReadObjectParams)
	for ; i < regionLen; i++ {
		args[i] = make(map[string]crdt.ReadArguments)
		readParams[i] = getMapReadParams(tableIndex, args[i], int(i))
	}
	return
}

func sendReceiveReadObjsProto(client QueryClient, fullReads []antidote.ReadObjectParams, nConn int) (replyProto *proto.ApbStaticReadObjectsResp) {
	antidote.SendProto(antidote.StaticReadObjs, antidote.CreateStaticReadObjs(nil, fullReads), client.serverConns[nConn])
	_, tmpProto, _ := antidote.ReceiveProto(client.serverConns[nConn])
	return tmpProto.(*proto.ApbStaticReadObjectsResp)
}

func sendReceiveReadProto(client QueryClient, fullReads []antidote.ReadObjectParams, partReads []antidote.ReadObjectParams, nConn int) (replyProto *proto.ApbStaticReadObjectsResp) {
	antidote.SendProto(antidote.StaticRead, antidote.CreateStaticRead(nil, fullReads, partReads), client.serverConns[nConn])
	_, tmpProto, _ := antidote.ReceiveProto(client.serverConns[nConn])
	return tmpProto.(*proto.ApbStaticReadObjectsResp)
}

/*
func sendReceiveMultipleReads(client QueryClient, reads []antidote.ReadObjectParams, nConn int) (replyProto *proto.ApbStaticReadObjectsResp) {
	antidote.SendProto(antidote.StaticRead, antidote.CreateStaticRead(nil, nil, reads), client.serverConns[nConn])
	_, tmpProto, _ := antidote.ReceiveProto(client.serverConns[nConn])
	return tmpProto.(*proto.ApbStaticReadObjectsResp)
}
*/

//Note: assumes only partial reads, for simplicity
func sendReceiveBucketReadProtos(client QueryClient, partReads map[int8]antidote.ReadObjectParams) (replyProtos map[int8]*proto.ApbStaticReadObjectsResp) {
	servers := make(map[int8]struct{})
	fullR := []antidote.ReadObjectParams{}
	for i, partR := range partReads {
		//fmt.Println("Sending part read to", i, partR)
		antidote.SendProto(antidote.StaticRead, antidote.CreateStaticRead(nil, fullR, []antidote.ReadObjectParams{partR}), client.serverConns[i])
		servers[i] = struct{}{}
	}
	return receiveBucketReadReplies(client, servers)
}

func sendReceiveBucketFullReadProtos(client QueryClient, fullReads map[int8][]antidote.ReadObjectParams) (replyProtos map[int8]*proto.ApbStaticReadObjectsResp) {
	servers := make(map[int8]struct{})
	for i, params := range fullReads {
		//fmt.Println("Sending read to", i, params)
		antidote.SendProto(antidote.StaticReadObjs, antidote.CreateStaticReadObjs(nil, params), client.serverConns[i])
		servers[i] = struct{}{}
	}
	return receiveBucketReadReplies(client, servers)
}

func receiveBucketReadReplies(client QueryClient, reads map[int8]struct{}) (replyProtos map[int8]*proto.ApbStaticReadObjectsResp) {
	replyProtos = make(map[int8]*proto.ApbStaticReadObjectsResp)
	//Note: this receiving of replies could be optimized
	for i, _ := range reads {
		//fmt.Println("Receiving reply from", i)
		_, replyProto, _ := antidote.ReceiveProto(client.serverConns[i])
		replyProtos[i] = replyProto.(*proto.ApbStaticReadObjectsResp)
		//fmt.Println("Reply received:", replyProtos[i].GetObjects().GetObjects())
	}
	return
}

//Could be more generic as long as it is reasonable to return state instead of crdt.EmbMapEntryState
func mergeMapReplies(replies map[int8]*proto.ApbStaticReadObjectsResp) (merged map[string]crdt.EmbMapEntryState) {
	merged = make(map[string]crdt.EmbMapEntryState)
	for _, reply := range replies {
		bktState := crdt.ReadRespProtoToAntidoteState(reply.GetObjects().GetObjects()[0],
			proto.CRDTType_RRMAP, proto.READType_GET_VALUES).(crdt.EmbMapEntryState)
		//fmt.Println(bktState)
		for keyS, state := range bktState.States {
			merged[keyS] = state.(crdt.EmbMapEntryState)
		}
	}
	return
}

//Same as mergeMapReplies, but assumes that multiple embMap objects may be in readResp. Assumes that all replies have the same number of objects.
func mergeMultipleMapReplies(replies map[int8]*proto.ApbStaticReadObjectsResp, readType proto.READType) (merged []map[string]crdt.EmbMapEntryState) {
	fmt.Println("Received replies...")
	for _, reply := range replies {
		merged = make([]map[string]crdt.EmbMapEntryState, len(reply.GetObjects().GetObjects()))
		break
	}
	for i := 0; i < len(merged); i++ {
		merged[i] = make(map[string]crdt.EmbMapEntryState)
	}
	var currMap map[string]crdt.EmbMapEntryState
	//Now finally start merging the replies
	for _, reply := range replies {
		objsProto := reply.GetObjects().GetObjects()
		for i, obj := range objsProto {
			currMap = merged[i]
			bktState := crdt.ReadRespProtoToAntidoteState(obj, proto.CRDTType_RRMAP, readType).(crdt.EmbMapEntryState)
			for keyS, state := range bktState.States {
				currMap[keyS] = state.(crdt.EmbMapEntryState)
			}
		}
	}
	return
}

func embMapRegisterToString(key string, state crdt.EmbMapEntryState) string {
	return state.States[key].(crdt.RegisterState).Value.(string)
}

func sendReceiveIndexQuery(client QueryClient, fullReads []antidote.ReadObjectParams, partReads []antidote.ReadObjectParams) (replies []*proto.ApbStaticReadObjectsResp) {
	if isIndexGlobal {
		if len(partReads) == 0 {
			return []*proto.ApbStaticReadObjectsResp{sendReceiveReadObjsProto(client, fullReads, client.indexServer)}
		} else {
			return []*proto.ApbStaticReadObjectsResp{sendReceiveReadProto(client, fullReads, partReads, client.indexServer)}
		}
	} else {
		fullReadsS := make([][]antidote.ReadObjectParams, len(procTables.Regions))
		partReadsS := make([][]antidote.ReadObjectParams, len(procTables.Regions))
		var currFull, currPart []antidote.ReadObjectParams
		for i := range fullReadsS {
			currFull = make([]antidote.ReadObjectParams, len(fullReads))
			for j, param := range fullReads {
				currFull[j] = antidote.ReadObjectParams{
					KeyParams: antidote.KeyParams{Key: param.Key, CrdtType: param.CrdtType, Bucket: buckets[INDEX_BKT+i]},
				}
			}
			currPart = make([]antidote.ReadObjectParams, len(partReads))
			for j, param := range partReads {
				currPart[j] = antidote.ReadObjectParams{
					KeyParams: antidote.KeyParams{Key: param.Key, CrdtType: param.CrdtType, Bucket: buckets[INDEX_BKT+i]},
					ReadArgs:  param.ReadArgs,
				}
			}
			fullReadsS[i], partReadsS[i] = currFull, currPart
			if len(partReads) == 0 {
				antidote.SendProto(antidote.StaticReadObjs, antidote.CreateStaticReadObjs(nil, currFull), client.serverConns[i])
			} else {
				antidote.SendProto(antidote.StaticRead, antidote.CreateStaticRead(nil, currPart, currFull), client.serverConns[i])
			}
		}

		replyProtos := make([]*proto.ApbStaticReadObjectsResp, len(fullReadsS))
		var tmpProto pb.Message
		for i := range fullReadsS {
			_, tmpProto, _ = antidote.ReceiveProto(client.serverConns[i])
			replyProtos[i] = tmpProto.(*proto.ApbStaticReadObjectsResp)
		}
		return replyProtos
	}
}

func doQueryStatsInterval() {
	for {
		time.Sleep(statisticsInterval * time.Millisecond)
		newBoolSlice := make([]bool, TEST_ROUTINES)
		for i := range newBoolSlice {
			newBoolSlice[i] = true
		}
		collectQueryStats = newBoolSlice
	}
}

func writeQueriesStatsFile(stats []QueryClientResult) {
	//I should write the total, avg, best and worse for each part.
	//Also write the total, avg, best and worse for the "final"

	statsPerPart := convertQueryStats(stats)
	nClients, nFuncs := float64(len(stats)), len(queryFuncs)

	//+10: header(1), 5 spaces(5), finalStats(4)
	//data := make([][]string, len(statsPerPart)*4+10)
	totalData := make([][]string, len(statsPerPart)+1) //space for final data as well
	avgData := make([][]string, len(statsPerPart))
	bestData := make([][]string, len(statsPerPart))
	worseData := make([][]string, len(statsPerPart))

	header := []string{"Total time", "Section time", "Queries txns", "Queries", "Reads", "Query txns/s", "Query/s", "Read/s",
		"Average latency (ms)"}

	partQueries, partReads, partTime, partQueryTxns := 0, 0, int64(0), 0
	totalQueries, totalReads, totalTime := 0, 0, int64(0)
	queryTxnsS, queryS, readS, latency := 0.0, 0.0, 0.0, 0.0
	avgQueryTxn, avgQuery, avgRead, avgQueryTxnS, avgQueryS, avgReadS := 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
	maxClientPos, minClientPos := 0, 0
	maxClientQueryS, minClientQueryS, currClientQueryS := 0.0, math.MaxFloat64, 0.0
	var maxQueryTxnS, maxQueryS, maxReadS, minQueryTxnS, minQueryS, minReadS, maxLatency, minLatency float64
	var maxClient, minClient QueryStats

	for i, partStats := range statsPerPart {
		for j, clientStat := range partStats {
			partQueries += clientStat.nQueries
			partReads += clientStat.nReads
			partTime += clientStat.timeSpent
			currClientQueryS = float64(clientStat.nQueries) / float64(clientStat.timeSpent)
			if currClientQueryS > maxClientQueryS {
				maxClientPos = j
				maxClientQueryS = currClientQueryS
			}
			if currClientQueryS < minClientQueryS {
				minClientPos = j
				minClientQueryS = currClientQueryS
			}
		}
		partQueryTxns = partQueries / nFuncs
		partTime /= int64(TEST_ROUTINES)
		queryTxnsS, queryS = (float64(partQueryTxns)/float64(partTime))*1000, (float64(partQueries)/float64(partTime))*1000
		readS, latency = (float64(partReads)/float64(partTime))*1000, float64(partTime*int64(TEST_ROUTINES))/float64(partQueries)
		totalQueries += partQueries
		totalReads += partReads
		totalTime += partTime

		avgQueryTxn, avgQuery, avgRead = float64(partQueryTxns)/nClients, float64(partQueries)/nClients, float64(partReads)/nClients
		avgQueryTxnS, avgQueryS, avgReadS = float64(queryTxnsS)/nClients, float64(queryS)/nClients, float64(readS)/nClients
		maxClient, minClient = partStats[maxClientPos], partStats[minClientPos]
		maxQueryTxnS, maxQueryS = (float64(maxClient.nQueries/nFuncs)/float64(partTime))*1000, (float64(maxClient.nQueries)/float64(partTime))*1000
		maxReadS, minQueryTxnS = (float64(maxClient.nReads)/float64(partTime))*1000, (float64(minClient.nQueries/nFuncs)/float64(partTime))*1000
		minQueryS, minReadS = (float64(minClient.nQueries)/float64(partTime))*1000, (float64(minClient.nReads/nFuncs)/float64(partTime))*1000
		maxLatency, minLatency = float64(maxClient.timeSpent)/float64(maxClient.nQueries), float64(minClient.timeSpent)/float64(minClient.nQueries)

		totalData[i] = []string{strconv.FormatInt(totalTime, 10), strconv.FormatInt(partTime, 10),
			strconv.FormatInt(int64(partQueryTxns), 10), strconv.FormatInt(int64(partQueries), 10), strconv.FormatInt(int64(partReads), 10),
			strconv.FormatFloat(queryTxnsS, 'f', 10, 64), strconv.FormatFloat(queryS, 'f', 10, 64),
			strconv.FormatFloat(readS, 'f', 10, 64), strconv.FormatFloat(latency, 'f', 10, 64)}

		avgData[i] = []string{strconv.FormatInt(totalTime, 10), strconv.FormatInt(partTime, 10),
			strconv.FormatFloat(avgQueryTxn, 'f', 10, 64), strconv.FormatFloat(avgQuery, 'f', 10, 64), strconv.FormatFloat(avgRead, 'f', 10, 64),
			strconv.FormatFloat(avgQueryTxnS, 'f', 10, 64), strconv.FormatFloat(avgQueryS, 'f', 10, 64),
			strconv.FormatFloat(avgReadS, 'f', 10, 64), strconv.FormatFloat(latency, 'f', 10, 64)}

		bestData[i] = []string{strconv.FormatInt(totalTime, 10), strconv.FormatInt(maxClient.timeSpent, 10),
			strconv.FormatInt(int64(maxClient.nQueries/nFuncs), 10), strconv.FormatInt(int64(maxClient.nQueries), 10), strconv.FormatInt(int64(maxClient.nReads), 10),
			strconv.FormatFloat(maxQueryTxnS, 'f', 10, 64), strconv.FormatFloat(maxQueryS, 'f', 10, 64),
			strconv.FormatFloat(maxReadS, 'f', 10, 64), strconv.FormatFloat(maxLatency, 'f', 10, 64)}

		worseData[i] = []string{strconv.FormatInt(totalTime, 10), strconv.FormatInt(minClient.timeSpent, 10),
			strconv.FormatInt(int64(minClient.nQueries/nFuncs), 10), strconv.FormatInt(int64(minClient.nQueries), 10), strconv.FormatInt(int64(minClient.nReads), 10),
			strconv.FormatFloat(minQueryTxnS, 'f', 10, 64), strconv.FormatFloat(minQueryS, 'f', 10, 64),
			strconv.FormatFloat(minReadS, 'f', 10, 64), strconv.FormatFloat(minLatency, 'f', 10, 64)}

		partQueryTxns, partQueries, partReads, partTime = 0, 0, 0, 0
	}

	for _, clientFinal := range stats {
		partQueryTxns += int(clientFinal.nQueries)
		partReads += int(clientFinal.nReads)
		partTime += int64(clientFinal.duration)
	}
	partQueries, partQueryTxns = partQueryTxns, partQueryTxns/nFuncs
	partTime /= int64(TEST_ROUTINES)
	queryTxnsS, queryS = (float64(partQueryTxns)/float64(partTime))*1000, (float64(partQueries)/float64(partTime))*1000
	readS, latency = (float64(partReads)/float64(partTime))*1000, float64(partTime*int64(TEST_ROUTINES))/float64(partQueries)
	//TODO: Fix final data
	finalData := []string{strconv.FormatInt(partTime, 10), strconv.FormatInt(partTime, 10),
		strconv.FormatInt(int64(partQueryTxns), 10), strconv.FormatInt(int64(partQueries), 10), strconv.FormatInt(int64(partReads), 10),
		strconv.FormatFloat(queryTxnsS, 'f', 10, 64), strconv.FormatFloat(queryS, 'f', 10, 64),
		strconv.FormatFloat(readS, 'f', 10, 64), strconv.FormatFloat(latency, 'f', 10, 64)}
	totalData[len(totalData)-1] = finalData

	file := getStatsFileToWrite("queryStats")
	if file == nil {
		return
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	writer.Comma = ';'
	defer writer.Flush()

	writer.Write(header)
	toWrite := [][][]string{totalData, avgData, bestData, worseData}
	emptyLine := []string{"", "", "", "", "", "", "", "", ""}
	for _, data := range toWrite {
		for _, line := range data {
			writer.Write(line)
		}
		writer.Write(emptyLine)
	}
	writer.Write(finalData)

	fmt.Println("Query statistics saved successfully.")
}

func convertQueryStats(stats []QueryClientResult) (convStats [][]QueryStats) {
	sizeToUse := int(math.MaxInt32)
	for _, queryStats := range stats {
		if len(queryStats.intermediateResults) < sizeToUse {
			sizeToUse = len(queryStats.intermediateResults)
		}
	}
	convStats = make([][]QueryStats, sizeToUse)
	var currStatSlice []QueryStats

	for i := range convStats {
		currStatSlice = make([]QueryStats, len(stats))
		for j, stat := range stats {
			currStatSlice[j] = stat.intermediateResults[i]
		}
		convStats[i] = currStatSlice
	}

	return
}

func batchModeStringToInt(typeS string) int {
	switch strings.ToUpper(typeS) {
	case "SINGLE":
		return SINGLE
	case "CYCLE":
		return CYCLE
	default:
		fmt.Println("[ERROR]Unknown batch mode type. Exitting")
		os.Exit(0)
	}
	return CYCLE
}
