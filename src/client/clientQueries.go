package client

//TODO: Maybe refactor all the reply/process/query things as objects? Preferably in some other file?

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
	"sort"
	"strconv"
	"strings"
	"time"
	tpch "tpch_data_processor/tpch"

	//"tpch_client/src/tpch"

	//pb "github.com/golang/protobuf/proto"
	pb "google.golang.org/protobuf/proto"
)

/*
	Number of (actual) reads per query:
	Q3: 1
	Q5: 1
	Q11: 1 (used to be 2)
	Q14: 1
	Q15: 1
	Q18: 1
	Q1: 2
	Q2: 1
	Q4: 1
	Q6: 1
	Q7: 1
	Q8: 1
	Q9: 1
	Q10: 1
	Q12: 1
	Q13: 2
	Q16: 2
	Q17: 1
	Q19: 3
	Q20: 1
	Q21: 1
	Q22: 2
	READS_PER_TXN (full): 26
*/

// In order to allow multiple independent clients executing queries, each client must
// have one instance of this object, which will contain his private variables (namelly,
// its own connections)
type QueryClient struct {
	serverConns []net.Conn
	indexServer int
	rng         *rand.Rand
	currQ20     *Q20QueryArgs
	nEntries    *int
}

type Q20QueryArgs struct {
	color, nation, year string
}

type QueryClientResult struct {
	duration, nReads, nQueries, nEntries float64
	intermediateResults                  []QueryStats
}

const (
	CYCLE, SINGLE = 0, 1 //BATCH_MODE
)

var (
	//Filled automatically by configLoader
	PRINT_QUERY, QUERY_BENCH      bool
	TEST_ROUTINES                 int
	TEST_DURATION                 int64
	STOP_QUERIES                  bool //Becomes true when the execution time for the queries is over
	READS_PER_TXN                 int
	QUERY_WAIT                    time.Duration
	ONLY_LOCAL_DATA_QUERY         bool                    //if true, the client always asks for data of the server he's connected to
	BATCH_MODE                    int                     //CYCLE or SINGLE. Only supported by mix clients atm.
	queryFuncs                    []func(QueryClient) int //queries to execute
	collectQueryStats             []bool                  //Becomes true when enough time has passed to collect statistics again. One entry per queryClient
	getReadsFuncs                 []func(QueryClient, []crdt.ReadObjectParams, []crdt.ReadObjectParams, []int, int) int
	getReadsLocalDirectFuncs      []func(QueryClient, [][]crdt.ReadObjectParams, [][]crdt.ReadObjectParams, [][]int, []int, int, int) (int, int) //For local mode, LOCAL_DIRECT
	processReadReplies            []func(QueryClient, []*proto.ApbReadObjectResp, []int, int) int                                                //int[]: fullReadPos, partialReadPos. int: reads done
	processLocalDirectReadReplies []func(QueryClient, [][]*proto.ApbReadObjectResp, [][]int, []int, int, int) int
	q15CrdtType                   proto.CRDTType //Q15 can be implemented with either TopK or TopSum.
	q15TopSize                    int32          //Number of entries to ask on topK. By default, 1.
	Q17_QUERY_QUANTITIES          = map[string]crdt.ReadArguments{"1": crdt.StateReadArguments{}, "2": crdt.StateReadArguments{}, "3": crdt.StateReadArguments{}, "4": crdt.StateReadArguments{},
		"5": crdt.StateReadArguments{}, "6": crdt.StateReadArguments{}, "7": crdt.StateReadArguments{}, "8": crdt.StateReadArguments{}, "9": crdt.StateReadArguments{}, "10": crdt.StateReadArguments{}}
)

//mapKeys := map[string]crdt.ReadArguments{regIdS + "1995": fullArgs, regIdS + "1996": fullArgs, natIdS + "1995": fullArgs, natIdS + "1996": fullArgs}

func startQueriesBench() {
	printExecutionTimes()
	q22CustNatBalancesFiller() //Need to fill in q22CustNatBalances for Q22.
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
	client := QueryClient{serverConns: conns, indexServer: defaultServer, rng: rng, currQ20: &Q20QueryArgs{}, nEntries: new(int)}
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
		fullRBuf, partialRBuf := make([]crdt.ReadObjectParams, READS_PER_TXN+1),
			make([]crdt.ReadObjectParams, READS_PER_TXN+1) //+1 as one of the queries has 2 reads.
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
		intermediateResults: queryStats, nEntries: float64(*client.nEntries)}
}

func sendQueries(conn net.Conn) {
	client := QueryClient{serverConns: conns, indexServer: DEFAULT_REPLICA, rng: rand.New(rand.NewSource(time.Now().UnixNano())), currQ20: &Q20QueryArgs{}, nEntries: new(int)}
	q22CustNatBalancesFiller() //Need to fill in q22CustNatBalances for Q22.
	time.Sleep(QUERY_WAIT * time.Millisecond)
	fmt.Println("Starting to send queries")
	startTime := time.Now().UnixNano() / 1000000
	fmt.Println()
	sendQ3(client)
	times.queries[0] = time.Now().UnixNano() / 1000000
	fmt.Println()
	sendQ5(client)
	times.queries[1] = time.Now().UnixNano() / 1000000
	fmt.Println()
	sendQ11(client)
	times.queries[2] = time.Now().UnixNano() / 1000000
	fmt.Println()
	sendQ14(client)
	times.queries[3] = time.Now().UnixNano() / 1000000
	fmt.Println()
	sendQ15(client)
	times.queries[4] = time.Now().UnixNano() / 1000000
	fmt.Println()
	sendQ18(client)
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

// Converts [][]int into []int
func localDirectBufIToServerBufI(bufI [][]int, subIndex int) (newBufI []int) {
	newBufI = make([]int, len(bufI))
	for i, subBufI := range bufI {
		newBufI[i] = subBufI[subIndex]
	}
	return
}

func incrementAllBufILocalDirect(bufI [][]int, subIndex int, incValue int) {
	for _, subBufI := range bufI {
		subBufI[subIndex] += incValue
	}
}

// BufI: server -> full/partial
func getQ3LocalDirectReads(client QueryClient, fullR, partialR [][]crdt.ReadObjectParams, bufI [][]int, rngRegions []int, rngRegionsI, reads int) (newReads, rngRegion int) {
	rndSeg, rndDay, nRegions := tpchData.Tables.Segments[client.rng.Intn(5)], 1+client.rng.Int63n(31), len(fullR)
	for i := 0; i < nRegions; i++ {
		partialR[i][bufI[i][1]] = crdt.ReadObjectParams{
			KeyParams: crdt.KeyParams{Key: SEGM_DELAY + rndSeg + strconv.FormatInt(rndDay, 10), CrdtType: proto.CRDTType_TOPK_RMV, Bucket: buckets[INDEX_BKT+i]},
			ReadArgs:  crdt.GetTopNArguments{NumberEntries: 10},
		}
		bufI[i][1]++
	}
	return reads + nRegions, 0
}

func getQ3LocalServerReads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndSeg, rndDay := tpchData.Tables.Segments[client.rng.Intn(5)], 1+client.rng.Int63n(31)
	offset, nRegions := bufI[1], len(client.serverConns)
	for i := 0; i < nRegions; i++ {
		partialR[offset+i] = crdt.ReadObjectParams{
			KeyParams: crdt.KeyParams{Key: SEGM_DELAY + rndSeg + strconv.FormatInt(rndDay, 10), CrdtType: proto.CRDTType_TOPK_RMV, Bucket: buckets[INDEX_BKT+i]},
			ReadArgs:  crdt.GetTopNArguments{NumberEntries: 10},
		}
	}
	bufI[1] += nRegions
	return reads + nRegions
}

// Assuming global-only for now
func getQ3Reads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndSeg, rndDay := tpchData.Tables.Segments[client.rng.Intn(5)], 1+client.rng.Int63n(31)
	partialR[bufI[1]] = crdt.ReadObjectParams{
		KeyParams: crdt.KeyParams{Key: SEGM_DELAY + rndSeg + strconv.FormatInt(rndDay, 10), CrdtType: proto.CRDTType_TOPK_RMV, Bucket: buckets[INDEX_BKT]},
		ReadArgs:  crdt.GetTopNArguments{NumberEntries: 10},
	}
	bufI[1]++
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][Top10N]Q3 query: requesting day %d for segment %s\n", rndDay, rndSeg)
	}
	return reads + 1
}

func processQ3Reply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	processQ3ReplyProto(replies[bufI[1]], client)
	bufI[1]++
	return reads + 1
}

func processQ3LocalServerReply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newRead int) {
	nRegions := len(client.serverConns)
	start, end := bufI[1], bufI[1]+nRegions
	processQ3ReplyProto(mergeQ3IndexReply(replies[start:end])[0], client)
	bufI[1] += nRegions
	return reads + nRegions
}

func processQ3LocalDirectReply(client QueryClient, replies [][]*proto.ApbReadObjectResp, bufI [][]int, rngRegions []int, rngRegionsI int, reads int) (newReads int) {
	//fmt.Println("[ClientQuery][Q3Reply][direct]")
	nRegions := len(client.serverConns)
	bufI1 := localDirectBufIToServerBufI(bufI, 1)
	relevantReplies := readRepliesToOneSlice(replies, bufI1, 1)
	processQ3ReplyProto(mergeQ3IndexReply(relevantReplies)[0], client)
	incrementAllBufILocalDirect(bufI, 1, 1)
	return reads + nRegions
}

func processQ3ReplyProto(proto *proto.ApbReadObjectResp, client QueryClient) {
	values := proto.GetTopk().GetValues()
	if PRINT_QUERY {
		fmt.Println("[ClientQuery]Q3 results:")
		var orderData []string
		for _, pair := range values {
			//unpackIndexExtraData(pair.GetData(), Q3_N_EXTRA_DATA)
			orderData = unpackIndexExtraData(pair.GetData(), Q3_N_EXTRA_DATA)
			fmt.Printf("%d | %d | %s | %s\n", pair.GetPlayerId(), pair.GetScore(), orderData[0], orderData[1])
		}
	} else {
		//Still process data
		for _, pair := range values {
			unpackIndexExtraData(pair.GetData(), Q3_N_EXTRA_DATA)
		}
	}
	*client.nEntries += len(values)
}

func sendQ3(client QueryClient) (nRequests int) {
	rndSeg := tpchData.Tables.Segments[client.rng.Intn(5)]
	rndDay := 1 + client.rng.Int63n(31)

	readParam := []crdt.ReadObjectParams{{
		KeyParams: crdt.KeyParams{Key: SEGM_DELAY + rndSeg + strconv.FormatInt(rndDay, 10), CrdtType: proto.CRDTType_TOPK_RMV, Bucket: buckets[INDEX_BKT]},
		ReadArgs:  crdt.GetTopNArguments{NumberEntries: 10},
	}}
	//topKProto := sendReceiveReadProto(client, []crdt.ReadObjectParams{}, readParam).GetObjects().GetObjects()[0].GetTopk()
	topKProto := mergeIndex(sendReceiveIndexQuery(client, nil, readParam), 3)[0].GetTopk()
	values := topKProto.GetValues()
	*client.nEntries += len(values)

	if !INDEX_WITH_FULL_DATA {
		orderIDs := make([]int32, 10)
		written := 0
		for _, pair := range values {
			orderIDs[written] = pair.GetPlayerId()
			written++
		}

		orderIDs = orderIDs[:written]

		orderDate, shipPriority := tpchData.Headers[tpch.ORDERS][4], tpchData.Headers[tpch.ORDERS][7]
		registerArgs := crdt.StateReadArguments{}
		orderMapArgs := crdt.EmbMapPartialArguments{Args: map[string]crdt.ReadArguments{orderDate: registerArgs, shipPriority: registerArgs}}
		states := getTableState(client, orderMapArgs, tpch.ORDERS, orderIDs, tpchData.Tables.OrderkeyToRegionkey)

		if PRINT_QUERY {
			fmt.Printf("[ClientQuery][TopK10]Q3: top 10 orders for segment %s not delivered as of %d:03:1995:\n", rndSeg, rndDay)
			var orderID int32
			var order map[string]crdt.State
			for _, pair := range values {
				orderID = pair.GetPlayerId()
				order = states[strconv.FormatInt(int64(orderID), 10)].States
				fmt.Printf("%d | %d | %s | %s\n", orderID, pair.GetScore(),
					order[orderDate].(crdt.RegisterState).Value.(string), order[shipPriority].(crdt.RegisterState).Value.(string))
			}
		}
		if isIndexGlobal {
			return len(orderIDs) + 1
		}
		//indexes + orders
		return len(client.serverConns) + len(orderIDs)
	}

	//Extract extra data from topk and print
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][TopK10]Q3: top 10 orders for segment %s not delivered as of %d:03:1995:\n", rndSeg, rndDay)
		var orderData []string
		for _, pair := range values {
			orderData = unpackIndexExtraData(pair.GetData(), Q3_N_EXTRA_DATA)
			fmt.Printf("%d | %d | %s | %s\n", pair.GetPlayerId(), pair.GetScore(),
				orderData[0], orderData[1])
		}
	} else {
		//Still process the extra data
		for _, pair := range values {
			unpackIndexExtraData(pair.GetData(), Q3_N_EXTRA_DATA)
		}
	}
	if isIndexGlobal {
		return 1
	}
	return len(client.serverConns)
}

func getQ5Reads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndRegion, rndYear := client.getRngRegion(), 1993+client.rng.Int63n(5)
	rndRegionN := tpchData.Tables.Regions[rndRegion].R_NAME
	fullR[bufI[0]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{
		Key: NATION_REVENUE + rndRegionN + strconv.FormatInt(rndYear, 10), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]},
		ReadArgs: crdt.StateReadArguments{},
	}
	bufI[0]++
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][Map[Counters]StateRead]Q5 query: requesting all nation data for region %s for year %d\n", rndRegionN, rndYear)
	}
	return reads + 1
}

func getQ5LocalDirectReads(client QueryClient, fullR, partialR [][]crdt.ReadObjectParams, bufI [][]int, rngRegions []int, rngRegionsI, reads int) (newReads, rngRegion int) {
	rndRegion, rndYear := client.getRngRegion(), 1993+client.rng.Int63n(5)
	rndRegionN := tpchData.Tables.Regions[rndRegion].R_NAME
	fullR[rndRegion][bufI[rndRegion][0]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{
		Key: NATION_REVENUE + rndRegionN + strconv.FormatInt(rndYear, 10), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+rndRegion]},
		ReadArgs: crdt.StateReadArguments{},
	}
	bufI[rndRegion][0]++
	rngRegions[rngRegionsI] = rndRegion
	return reads + 1, rndRegion
}

func getQ5LocalServerReads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndRegion, rndYear := client.getRngRegion(), 1993+client.rng.Int63n(5)
	rndRegionN := tpchData.Tables.Regions[rndRegion].R_NAME
	fullR[bufI[0]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{
		Key: NATION_REVENUE + rndRegionN + strconv.FormatInt(rndYear, 10), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+rndRegion]},
		ReadArgs: crdt.StateReadArguments{},
	}
	bufI[0]++
	return reads + 1
}

func processQ5Reply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	//Just to force usual processing
	//ignore(crdt.ReadRespProtoToAntidoteState(replies[bufI[0]], proto.CRDTType_RRMAP, proto.READType_FULL).(crdt.EmbMapEntryState))
	mapState := crdt.ReadRespProtoToAntidoteState(replies[bufI[0]], proto.CRDTType_RRMAP, proto.READType_FULL).(crdt.EmbMapEntryState)
	bufI[0]++

	if PRINT_QUERY {
		fmt.Println("[ClientQuery]Q5 results:")
		for nation, state := range mapState.States {
			fmt.Printf("%s: %d\n", nation, state.(crdt.CounterState).Value)
		}
	}
	*client.nEntries += len(mapState.States)
	return reads + 1
}

func processQ5LocalDirectReply(client QueryClient, replies [][]*proto.ApbReadObjectResp, bufI [][]int, rngRegions []int, rngRegionsI int, reads int) (newReads int) {
	//fmt.Println("[ClientQuery][Q5Reply][direct]")
	index := rngRegions[rngRegionsI]
	mapState := crdt.ReadRespProtoToAntidoteState(replies[index][bufI[index][0]], proto.CRDTType_RRMAP, proto.READType_FULL).(crdt.EmbMapEntryState)
	*client.nEntries += len(mapState.States)
	bufI[index][0]++
	return reads + 1
}

func sendQ5(client QueryClient) (nRequests int) {
	rndRegion := client.getRngRegion()
	rndRegionN := tpchData.Tables.Regions[rndRegion].R_NAME
	rndYear := 1993 + client.rng.Int63n(5)
	bktI, serverI := getIndexOffset(client, rndRegion)

	readParam := []crdt.ReadObjectParams{{KeyParams: crdt.KeyParams{
		Key: NATION_REVENUE + rndRegionN + strconv.FormatInt(rndYear, 10), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bktI]}},
	}

	//replyProto := sendReceiveReadObjsProto(client, readParam, bktI-INDEX_BKT)
	replyProto := sendReceiveReadObjsProto(client, readParam, serverI)
	mapState := crdt.ReadRespProtoToAntidoteState(replyProto.GetObjects().GetObjects()[0], proto.CRDTType_RRMAP, proto.READType_FULL).(crdt.EmbMapEntryState)

	if PRINT_QUERY {
		fmt.Println("[ClientQuery][RRMap]Q5: Values for", rndRegion, "in year", rndYear)
		for nation, valueState := range mapState.States {
			fmt.Printf("%s: %d\n", nation, valueState.(crdt.CounterState).Value)
		}
	}

	*client.nEntries += len(mapState.States)
	return 1
}

func getQ11Reads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndNation := tpchData.Tables.Nations[client.getRngNation()]
	rndNationN := rndNation.N_NAME
	fullR[bufI[0]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q11_KEY + rndNationN, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}}
	bufI[0]++
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][TopK_StateRead+Counter]Q11 query: requesting topK and counter from nation %s\n", rndNationN)
	}
	return reads + 2
}

func getQ11LocalDirectReads(client QueryClient, fullR, partialR [][]crdt.ReadObjectParams, bufI [][]int, rngRegions []int, rngRegionsI, reads int) (newReads, rngNation int) {
	rndNation := tpchData.Tables.Nations[client.getRngNation()]
	rndNationN, rndNationR := rndNation.N_NAME, rndNation.N_REGIONKEY
	fullR[rndNationR][bufI[rndNationR][0]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q11_KEY + rndNationN, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+rndNationR]}}
	bufI[rndNationR][0]++
	rngRegions[rngRegionsI] = int(rndNationR)
	return reads + 2, int(rndNationR)
}

func getQ11LocalServerReads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndNation := tpchData.Tables.Nations[client.getRngNation()]
	rndNationN, rndNationR := rndNation.N_NAME, rndNation.N_REGIONKEY
	fullR[bufI[0]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q11_KEY + rndNationN, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+rndNationR]}}
	bufI[0]++
	return reads + 2
}

func processQ11Reply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	topKProto, counterProto := processQ11HelperGetProtos(replies[bufI[0]])

	if PRINT_QUERY {
		fmt.Printf("[ClientQuery]Q11 results:\n")
		values, minValue := topKProto.GetValues(), counterProto.GetValue()
		sort.Slice(values, func(i, j int) bool { return values[i].GetScore() > values[j].GetScore() })
		for _, pair := range values {
			if pair.GetScore() < minValue {
				fmt.Println("Break due to topK value being lower than", minValue)
				break
			}
			fmt.Printf("%d: %d.\n", pair.GetPlayerId(), pair.GetScore())
		}
	} /*else {
		values, minValue := topKProto.GetValues(), counterProto.GetValue()
		sort.Slice(values, func(i, j int) bool { return values[i].GetScore() > values[j].GetScore() })
		i, min, max := 0, int32(math.MaxInt32), int32(math.MinInt32)
		for _, pair := range values {
			i++
			if pair.GetScore() < minValue {
				break
			}
			if pair.GetScore() < min {
				min = pair.GetScore()
			}
			if pair.GetScore() > max {
				max = pair.GetScore()
			}
		}
		fmt.Printf("[ClientQuery][Q11]Q11 printed size: %d. minValue: %d. Lowest printed: %d. Highest printed: %d.\n", i, minValue, min, max)
	}*/
	*client.nEntries += len(topKProto.GetValues()) + 1

	return reads + 2
}

func processQ11LocalDirectReply(client QueryClient, replies [][]*proto.ApbReadObjectResp, bufI [][]int, rngRegions []int, rngRegionsI int, reads int) (newReads int) {
	index := rngRegions[rngRegionsI]
	topKProto, counterProto := processQ11HelperGetProtos(replies[index][bufI[index][0]])
	ignore(counterProto.GetValue(), topKProto)
	bufI[index][0]++
	return reads + 2
}

func processQ11HelperGetProtos(reply *proto.ApbReadObjectResp) (topKProto *proto.ApbGetTopkResp, counterProto *proto.ApbGetCounterResp) {
	mapProto := reply.GetMap().GetEntries()
	if mapProto[0].GetKey().GetType() == proto.CRDTType_COUNTER {
		counterProto, topKProto = mapProto[0].GetValue().GetCounter(), mapProto[1].GetValue().GetTopk()
	} else {
		counterProto, topKProto = mapProto[1].GetValue().GetCounter(), mapProto[0].GetValue().GetTopk()
	}
	return
}

func sendQ11(client QueryClient) (nRequests int) {
	//Unfortunatelly we can't use the topk query of "only values above min" here as we need to fetch the min from the database first.
	rndNation := tpchData.Tables.Nations[client.getRngNation()]
	rndNationN, rndNationR := rndNation.N_NAME, rndNation.N_REGIONKEY
	bktI, serverI := getIndexOffset(client, int(rndNationR))
	readParam := []crdt.ReadObjectParams{{KeyParams: crdt.KeyParams{Key: Q11_KEY + rndNationN, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bktI]}}}
	topkProto, counterProto := processQ11HelperGetProtos(sendReceiveReadObjsProto(client, readParam, serverI).GetObjects().GetObjects()[0])
	minValue := counterProto.GetValue()

	if PRINT_QUERY {
		fmt.Println("[ClientQuery][RRMap]Q11: Values for", rndNationN)
		values := topkProto.GetValues()
		sort.Slice(values, func(i, j int) bool { return values[i].GetScore() > values[j].GetScore() })
		for _, pair := range values {
			if pair.GetScore() < minValue {
				break
			}
			fmt.Printf("%d: %d.\n", pair.GetPlayerId(), pair.GetScore())
		}
	} /*else {
		values := topkProto.GetValues()
		sort.Slice(values, func(i, j int) bool { return values[i].GetScore() > values[j].GetScore() })
		i, min, max := 0, int32(math.MaxInt32), int32(math.MinInt32)
		for _, pair := range values {
			i++
			if pair.GetScore() < minValue {
				break
			}
			if pair.GetScore() < min {
				min = pair.GetScore()
			}
			if pair.GetScore() > max {
				max = pair.GetScore()
			}
		}
		fmt.Printf("[ClientQuery][Q11]Q11 printed size: %d. minValue: %d. Lowest printed: %d. Highest printed: %d.\n", i, minValue, min, max)
	}*/

	*client.nEntries += len(topkProto.GetValues()) + 1
	return 2
}

/*
func getQ11Reads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndNation := tpchData.Tables.Nations[client.getRngNation()]
	rndNationN := rndNation.N_NAME
	fullR[bufI[0]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: IMP_SUPPLY + rndNationN, CrdtType: proto.CRDTType_TOPK_RMV, Bucket: buckets[INDEX_BKT]},
		ReadArgs: crdt.StateReadArguments{},
	}
	fullR[bufI[0]+1] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: SUM_SUPPLY + rndNationN, CrdtType: proto.CRDTType_COUNTER, Bucket: buckets[INDEX_BKT]},
		ReadArgs: crdt.StateReadArguments{},
	}
	bufI[0] += 2
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][TopK_StateRead+Counter]Q11 query: requesting topK and counter from nation %s\n", rndNationN)
	}
	return reads + 2
}

func getQ11LocalDirectReads(client QueryClient, fullR, partialR [][]crdt.ReadObjectParams, bufI [][]int, rngRegions []int, rngRegionsI, reads int) (newReads, rngNation int) {
	rndNation := tpchData.Tables.Nations[client.getRngNation()]
	rndNationN, rndNationR := rndNation.N_NAME, rndNation.N_REGIONKEY
	fullR[rndNationR][bufI[rndNationR][0]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: IMP_SUPPLY + rndNationN, CrdtType: proto.CRDTType_TOPK_RMV, Bucket: buckets[INDEX_BKT+rndNationR]},
		ReadArgs: crdt.StateReadArguments{},
	}
	fullR[rndNationR][bufI[rndNationR][0]+1] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: SUM_SUPPLY + rndNationN, CrdtType: proto.CRDTType_COUNTER, Bucket: buckets[INDEX_BKT+rndNationR]},
		ReadArgs: crdt.StateReadArguments{},
	}
	bufI[rndNationR][0] += 2
	rngRegions[rngRegionsI] = int(rndNationR)
	return reads + 2, int(rndNationR)
}

func getQ11LocalServerReads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndNation := tpchData.Tables.Nations[client.getRngNation()]
	rndNationN, rndNationR := rndNation.N_NAME, rndNation.N_REGIONKEY
	fullR[bufI[0]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: IMP_SUPPLY + rndNationN, CrdtType: proto.CRDTType_TOPK_RMV, Bucket: buckets[INDEX_BKT+rndNationR]},
		ReadArgs: crdt.StateReadArguments{},
	}
	fullR[bufI[0]+1] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: SUM_SUPPLY + rndNationN, CrdtType: proto.CRDTType_COUNTER, Bucket: buckets[INDEX_BKT+rndNationR]},
		ReadArgs: crdt.StateReadArguments{},
	}
	bufI[0] += 2
	return reads + 2
}

func processQ11Reply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	topKProto, counterProto := replies[bufI[0]].GetTopk(), replies[bufI[0]+1].GetCounter()
	ignore(counterProto.GetValue(), topKProto)
	bufI[0] += 2

	if PRINT_QUERY {
		fmt.Printf("[ClientQuery]Q11 results:\n")
		values, minValue := topKProto.GetValues(), counterProto.GetValue()
		sort.Slice(values, func(i, j int) bool { return values[i].GetScore() > values[j].GetScore() })
		for _, pair := range values {
			if pair.GetScore() < minValue {
				fmt.Println("Break due to topK value being lower than", minValue)
				break
			}
			fmt.Printf("%d: %d.\n", pair.GetPlayerId(), pair.GetScore())
		}
	}

	return reads + 2
}

func processQ11LocalDirectReply(client QueryClient, replies [][]*proto.ApbReadObjectResp, bufI [][]int, rngRegions []int, rngRegionsI int, reads int) (newReads int) {
	//fmt.Println("[ClientQuery][Q11Reply][direct]")
	index := rngRegions[rngRegionsI]
	topKProto, counterProto := replies[index][bufI[index][0]].GetTopk(), replies[index][bufI[index][0]+1].GetCounter()
	ignore(counterProto.GetValue(), topKProto)
	bufI[index][0] += 2
	return reads + 2
}

func sendQ11(client QueryClient) (nRequests int) {
	//Unfortunatelly we can't use the topk query of "only values above min" here as we need to fetch the min from the database first.
	rndNation := tpchData.Tables.Nations[client.getRngNation()]
	//rndNation := tpchData.Tables.Nations[0]
	rndNationN, rndNationR := rndNation.N_NAME, rndNation.N_REGIONKEY
	bktI, serverI := getIndexOffset(client, int(rndNationR))

	readParam := []crdt.ReadObjectParams{
		crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: IMP_SUPPLY + rndNationN, CrdtType: proto.CRDTType_TOPK_RMV, Bucket: buckets[bktI]},
			ReadArgs: crdt.StateReadArguments{},
		},
		crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: SUM_SUPPLY + rndNationN, CrdtType: proto.CRDTType_COUNTER, Bucket: buckets[bktI]},
			ReadArgs: crdt.StateReadArguments{},
		},
	}

	//replyProto := sendReceiveReadObjsProto(client, readParam, bktI-INDEX_BKT)
	replyProto := sendReceiveReadObjsProto(client, readParam, serverI)
	objsProto := replyProto.GetObjects().GetObjects()
	topkProto, counterProto := objsProto[0].GetTopk(), objsProto[1].GetCounter()

	minValue := counterProto.GetValue()
	if PRINT_QUERY {
		fmt.Println("Q11: Values for", rndNationN)
		values := topkProto.GetValues()
		sort.Slice(values, func(i, j int) bool { return values[i].GetScore() > values[j].GetScore() })
		for _, pair := range values {
			if pair.GetScore() < minValue {
				break
			}
			fmt.Printf("%d: %d.\n", pair.GetPlayerId(), pair.GetScore())
		}
	}

	return 2
}
*/

func getQ14Reads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndForDate := client.rng.Int63n(60)
	year, month := 1993+rndForDate/12, 1+rndForDate%12
	date := strconv.FormatInt(year, 10) + strconv.FormatInt(month, 10)
	fullR[bufI[0]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: PROMO_PERCENTAGE + date, CrdtType: proto.CRDTType_AVG, Bucket: buckets[INDEX_BKT]},
		ReadArgs: crdt.StateReadArguments{},
	}
	bufI[0]++
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][Avg StateRead]Q14 query: requesting average for key %s (%d/%d).\n", PROMO_PERCENTAGE+date, month, year)
	}
	return reads + 1
}

// Local versions actually need an AvgFullRead instead of StateReadArguments (partial instead of full read)
func getQ14LocalDirectReads(client QueryClient, fullR, partialR [][]crdt.ReadObjectParams, bufI [][]int, rngRegions []int, rngRegionsI, reads int) (newReads, rngRegion int) {
	rndForDate, nRegions := client.rng.Int63n(60), len(client.serverConns)
	year, month := 1993+rndForDate/12, 1+rndForDate%12
	date := strconv.FormatInt(year, 10) + strconv.FormatInt(month, 10)
	for i := 0; i < nRegions; i++ {
		partialR[i][bufI[i][1]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: PROMO_PERCENTAGE + date, CrdtType: proto.CRDTType_AVG, Bucket: buckets[INDEX_BKT+i]},
			ReadArgs: crdt.AvgGetFullArguments{},
		}
		bufI[i][1]++
	}
	return reads + nRegions, 0
}

func getQ14LocalServerReads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndForDate, nRegions, offset := client.rng.Int63n(60), len(client.serverConns), bufI[1]
	year, month := 1993+rndForDate/12, 1+rndForDate%12
	date := strconv.FormatInt(year, 10) + strconv.FormatInt(month, 10)
	for i := 0; i < nRegions; i++ {
		partialR[offset+i] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: PROMO_PERCENTAGE + date, CrdtType: proto.CRDTType_AVG, Bucket: buckets[INDEX_BKT+i]},
			ReadArgs: crdt.AvgGetFullArguments{},
		}
	}
	bufI[1] += nRegions
	return reads + nRegions
}

func processQ14Reply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	avg := replies[bufI[0]].GetAvg().GetAvg()
	bufI[0]++
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery]Q14 result: %f.\n", avg)
	}
	*client.nEntries += 1
	return reads + 1
}

func processQ14LocalServerReply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	start, end := bufI[1], bufI[1]+nRegions
	/*
		fmt.Println("[ClientQuery][Q14Reply][server]")
		for i := start; i < end; i++ {
			//fmt.Println("[ClientQuery][Q14Reply]Avg proto received (unprocessed):", replies[i].GetAvg().GetAvg())
			currProto := replies[i].GetPartread().GetAvg().GetGetfull()
			fmt.Println("[ClientQuery][Q14Reply]Avg proto received (unprocessed). Sum:",
				currProto.GetSum(), "NAdds:", currProto.GetNAdds(), "Avg:", float64(currProto.GetSum())/float64(currProto.GetNAdds()))
		}
	*/
	//mergeQ14IndexReply(replies[start:end])[0].GetAvg().GetAvg()
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery]Q14 result: %f.\n", mergeQ14IndexReply(replies[start:end])[0].GetAvg().GetAvg())
	} else {
		mergeQ14IndexReply(replies[start:end])[0].GetAvg().GetAvg()
	}
	bufI[1] += nRegions
	return reads + nRegions
}

func processQ14LocalDirectReply(client QueryClient, replies [][]*proto.ApbReadObjectResp, bufI [][]int, rngRegions []int, rngRegionsI int, reads int) (newReads int) {
	//fmt.Println("[ClientQuery][Q14Reply][direct]")
	nRegions := len(client.serverConns)
	bufI1 := localDirectBufIToServerBufI(bufI, 1)
	relevantReplies := readRepliesToOneSlice(replies, bufI1, 1)
	mergeQ14IndexReply(relevantReplies)[0].GetAvg().GetAvg()
	incrementAllBufILocalDirect(bufI, 1, 1)
	return reads + nRegions
}

func sendQ14(client QueryClient) (nRequests int) {
	rndForDate := client.rng.Int63n(60) //60 months from 1993 to 1997 (5 years)
	year, month := 1993+rndForDate/12, 1+rndForDate%12
	date := strconv.FormatInt(year, 10) + strconv.FormatInt(month, 10)
	var avgProto *proto.ApbGetAverageResp

	//Need to send AvgFullRead instead of state reads in case the index is local. The merge function will return the reply as if it was a single average though.
	if isIndexGlobal {
		readParam := []crdt.ReadObjectParams{{
			KeyParams: crdt.KeyParams{Key: PROMO_PERCENTAGE + date, CrdtType: proto.CRDTType_AVG, Bucket: buckets[INDEX_BKT]},
		}}
		avgProto = sendReceiveReadObjsProto(client, readParam, client.indexServer).GetObjects().GetObjects()[0].GetAvg()
	} else {
		readParam := []crdt.ReadObjectParams{{
			KeyParams: crdt.KeyParams{Key: PROMO_PERCENTAGE + date, CrdtType: proto.CRDTType_AVG, Bucket: buckets[INDEX_BKT]},
			ReadArgs:  crdt.AvgGetFullArguments{},
		}}
		avgProto = mergeIndex(sendReceiveIndexQuery(client, nil, readParam), 14)[0].GetAvg()
	}

	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][Avg]Q14: %d_%d: %f.\n", year, month, avgProto.GetAvg())
	}

	*client.nEntries += 1
	if isIndexGlobal {
		return 1
	}
	return len(client.serverConns)
}

func getQ15Reads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndQuarter, rndYear := 1+3*client.rng.Int63n(4), 1993+client.rng.Int63n(5)
	date := strconv.FormatInt(rndYear, 10) + strconv.FormatInt(rndQuarter, 10)
	partialR[bufI[1]] = crdt.ReadObjectParams{
		KeyParams: crdt.KeyParams{Key: TOP_SUPPLIERS + date, CrdtType: q15CrdtType, Bucket: buckets[INDEX_BKT]},
		ReadArgs:  crdt.GetTopNArguments{NumberEntries: q15TopSize},
	}
	bufI[1]++
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][Top1]Q15 query: requesting top1 for %d/%d\n", rndQuarter, rndYear)
	}
	return reads + 1
}

func getQ15LocalDirectReads(client QueryClient, fullR, partialR [][]crdt.ReadObjectParams, bufI [][]int, rngRegions []int, rngRegionsI, reads int) (newReads, rngRegion int) {
	rndQuarter, rndYear, nRegions := 1+3*client.rng.Int63n(4), 1993+client.rng.Int63n(5), len(client.serverConns)
	date := strconv.FormatInt(rndYear, 10) + strconv.FormatInt(rndQuarter, 10)
	for i := 0; i < nRegions; i++ {
		partialR[i][bufI[i][1]] = crdt.ReadObjectParams{
			KeyParams: crdt.KeyParams{Key: TOP_SUPPLIERS + date, CrdtType: q15CrdtType, Bucket: buckets[INDEX_BKT+i]},
			ReadArgs:  crdt.GetTopNArguments{NumberEntries: q15TopSize},
		}
		bufI[i][1]++
	}
	return reads + nRegions, 0
}

func getQ15LocalServerReads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndQuarter, rndYear, nRegions, offset := 1+3*client.rng.Int63n(4), 1993+client.rng.Int63n(5), len(client.serverConns), bufI[1]
	date := strconv.FormatInt(rndYear, 10) + strconv.FormatInt(rndQuarter, 10)
	for i := 0; i < nRegions; i++ {
		partialR[offset+i] = crdt.ReadObjectParams{
			KeyParams: crdt.KeyParams{Key: TOP_SUPPLIERS + date, CrdtType: q15CrdtType, Bucket: buckets[INDEX_BKT+i]},
			ReadArgs:  crdt.GetTopNArguments{NumberEntries: q15TopSize},
		}
	}
	bufI[1] += nRegions
	return reads + nRegions
}

func processQ15Reply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	processQ15ReplyProto(replies[bufI[1]], client)
	bufI[1]++
	return reads + 1
}

func processQ15LocalServerReply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	start, end := bufI[1], bufI[1]+nRegions
	processQ15ReplyProto(mergeQ15IndexReply(replies[start:end])[0], client)
	bufI[1] += nRegions
	return reads + nRegions
}

func processQ15LocalDirectReply(client QueryClient, replies [][]*proto.ApbReadObjectResp, bufI [][]int, rngRegions []int, rngRegionsI int, reads int) (newReads int) {
	//fmt.Println("[ClientQuery][Q15Reply][direct]")
	nRegions := len(client.serverConns)
	bufI1 := localDirectBufIToServerBufI(bufI, 1)
	relevantReplies := readRepliesToOneSlice(replies, bufI1, 1)
	processQ15ReplyProto(mergeQ15IndexReply(relevantReplies)[0], client)
	incrementAllBufILocalDirect(bufI, 1, 1)
	return reads + nRegions
}

func processQ15ReplyProto(proto *proto.ApbReadObjectResp, client QueryClient) {
	values := proto.GetTopk().GetValues()
	//No sorting needed as it is a topN query.
	/*
		if len(values) > 1 {
			sort.Slice(values, func(i, j int) bool { return values[i].GetScore() > values[j].GetScore() })
		}
	*/
	//fmt.Println("[CQ]Received Q15 query has size:", len(values))
	if PRINT_QUERY {
		fmt.Println("[ClientQuery]Q15 results:")
		for _, pair := range values {
			fmt.Printf("%d: %d\n", pair.GetPlayerId(), pair.GetScore())
		}
	}
	*client.nEntries += 1
}

func sendQ15(client QueryClient) (nRequests int) {
	rndQuarter := 1 + 3*client.rng.Int63n(4)
	rndYear := 1993 + client.rng.Int63n(5)
	date := strconv.FormatInt(rndYear, 10) + strconv.FormatInt(rndQuarter, 10)

	readParam := []crdt.ReadObjectParams{{
		KeyParams: crdt.KeyParams{Key: TOP_SUPPLIERS + date, CrdtType: q15CrdtType, Bucket: buckets[INDEX_BKT]},
		ReadArgs:  crdt.GetTopNArguments{NumberEntries: q15TopSize},
	}}
	//topkProto := sendReceiveReadProto(client, []crdt.ReadObjectParams{}, readParam).GetObjects().GetObjects()[0].GetTopk()
	topkProto := mergeIndex(sendReceiveIndexQuery(client, nil, readParam), 15)[0].GetTopk()

	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][TopSum1]Q15: best supplier(s) for months [%d, %d] of year %d\n", rndQuarter, rndQuarter+2, rndYear)
	}
	values := topkProto.GetValues()
	/*
		if len(values) > 1 {
			sort.Slice(values, func(i, j int) bool { return values[i].GetScore() > values[j].GetScore() })
		}
	*/
	if PRINT_QUERY {
		for _, pair := range values {
			fmt.Printf("%d: %d\n", pair.GetPlayerId(), pair.GetScore())
		}
	}

	*client.nEntries += 1
	if isIndexGlobal {
		return 1
	}
	return len(client.serverConns)
}

func getQ18Reads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndQuantity := 312 + client.rng.Int31n(4)
	//fmt.Println("[ClientQuery]Asking for: ", rndQuantity)
	fullR[bufI[0]] = crdt.ReadObjectParams{
		KeyParams: crdt.KeyParams{Key: LARGE_ORDERS + strconv.FormatInt(int64(rndQuantity), 10), CrdtType: proto.CRDTType_TOPK_RMV, Bucket: buckets[INDEX_BKT]},
		ReadArgs:  crdt.StateReadArguments{},
	}
	bufI[0]++
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][TopK StateRead]Q18 query: requesting topK for quantity %d\n", rndQuantity)
	}
	return reads + 1
}

func getQ18LocalDirectReads(client QueryClient, fullR, partialR [][]crdt.ReadObjectParams, bufI [][]int, rngRegions []int, rngRegionsI, reads int) (newReads, rndRegion int) {
	rndQuantity, nRegions := 312+client.rng.Int31n(4), len(client.serverConns)
	for i := 0; i < nRegions; i++ {
		fullR[i][bufI[i][0]] = crdt.ReadObjectParams{
			KeyParams: crdt.KeyParams{Key: LARGE_ORDERS + strconv.FormatInt(int64(rndQuantity), 10), CrdtType: proto.CRDTType_TOPK_RMV, Bucket: buckets[INDEX_BKT+i]},
			ReadArgs:  crdt.StateReadArguments{},
		}
		bufI[i][0]++
	}
	return reads + nRegions, 0
}

func getQ18LocalServerReads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndQuantity, nRegions, offset := 312+client.rng.Int31n(4), len(client.serverConns), bufI[0]
	for i := 0; i < nRegions; i++ {
		fullR[offset+i] = crdt.ReadObjectParams{
			KeyParams: crdt.KeyParams{Key: LARGE_ORDERS + strconv.FormatInt(int64(rndQuantity), 10), CrdtType: proto.CRDTType_TOPK_RMV, Bucket: buckets[INDEX_BKT+i]},
			ReadArgs:  crdt.StateReadArguments{},
		}
	}
	bufI[0] += nRegions
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][TopK StateRead]Q18 query: requesting topK for quantity %d\n", rndQuantity)
	}
	return reads + nRegions
}

func processQ18Reply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	processQ18ReplyProto(replies[bufI[0]], client)
	bufI[0]++
	return reads + 1
}

func processQ18LocalServerReply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	start, end := bufI[0], bufI[0]+nRegions
	/*
		fmt.Println("[ClientQuery][Q18Reply][server]")
		for i := start; i < end; i++ {
			fmt.Println("[ClientQuery][Q18Reply]TopK proto received (unprocessed):")
			values := replies[i].GetTopk().GetValues()
			var sb strings.Builder
			for _, pair := range values {
				unpackIndexExtraData(pair.GetData(), Q18_N_EXTRA_DATA)
				sb.WriteString(strconv.Itoa(int(pair.GetPlayerId())))
				sb.WriteString(":")
				sb.WriteString(strconv.Itoa(int(pair.GetScore())))
				sb.WriteString(" ; ")
			}
			sb.WriteString("\n")
			fmt.Print(sb.String())
		}
	*/
	processQ18ReplyProto(mergeQ18IndexReply(replies[start:end])[0], client)
	bufI[0] += nRegions
	return reads + nRegions
}

func processQ18LocalDirectReply(client QueryClient, replies [][]*proto.ApbReadObjectResp, bufI [][]int, rngRegions []int, rngRegionsI int, reads int) (newReads int) {
	//fmt.Println("[ClientQuery][Q18Reply][direct]")
	nRegions := len(client.serverConns)
	bufI0 := localDirectBufIToServerBufI(bufI, 0)
	relevantReplies := readRepliesToOneSlice(replies, bufI0, 1)
	processQ18ReplyProto(mergeQ18IndexReply(relevantReplies)[0], client)
	incrementAllBufILocalDirect(bufI, 0, 1)
	return reads + nRegions
}

func processQ18ReplyProto(proto *proto.ApbReadObjectResp, client QueryClient) {
	values := proto.GetTopk().GetValues()
	sort.Slice(values, func(i, j int) bool { return values[i].GetScore() > values[j].GetScore() })
	*client.nEntries += len(values)
	/*
		var sb strings.Builder
		sb.WriteString("[ClientQuery]Q18 result:\n")
		for _, pair := range values {
			unpackIndexExtraData(pair.GetData(), Q18_N_EXTRA_DATA)
			sb.WriteString(strconv.Itoa(int(pair.GetPlayerId())))
			sb.WriteString(":")
			sb.WriteString(strconv.Itoa(int(pair.GetScore())))
			sb.WriteString(" ; ")
		}
		sb.WriteString("\n")
		fmt.Print(sb.String())
	*/
}

func sendQ18(client QueryClient) (nRequests int) {
	//Might be worth to optimize this to download customers simultaneously with orders
	//However, this requires for the ID in the topK to refer to both keys
	//Also, theoretically this should be a single transaction instead of a static.
	rndQuantity := 312 + client.rng.Int31n(4)

	//Should ask for a GetTopN if we ever change the topK to not be top 100
	readParam := []crdt.ReadObjectParams{{
		KeyParams: crdt.KeyParams{Key: LARGE_ORDERS + strconv.FormatInt(int64(rndQuantity), 10), CrdtType: proto.CRDTType_TOPK_RMV, Bucket: buckets[INDEX_BKT]},
	}}
	//topkProto := sendReceiveReadObjsProto(readParam).GetObjects().GetObjects()[0].GetTopk()
	topkProto := mergeIndex(sendReceiveIndexQuery(client, readParam, nil), 18)[0].GetTopk()
	values := topkProto.GetValues()
	sort.Slice(values, func(i, j int) bool { return values[i].GetScore() > values[j].GetScore() })
	*client.nEntries += len(values)

	if !INDEX_WITH_FULL_DATA {
		//TODO: I don't think it's necessary to verify < rndQuantity
		//Get orders
		orderIDs := make([]int32, 100, 100)
		written := 0
		for _, pair := range values {
			if pair.GetScore() < rndQuantity {
				break
			}
			orderIDs[written] = pair.GetPlayerId()
			written++
			if written == 100 {
				break
			}
		}
		if written == 0 {
			if PRINT_QUERY {
				fmt.Printf("[ClientQuery][TopK]Q18: top 100 customers for large quantity orders with quantity above %d: no match found.", rndQuantity)
			}
			return
		}
		orderIDs = orderIDs[:written]

		orderDate, orderTotalPrice, orderCustKey := tpchData.Headers[tpch.ORDERS][4], tpchData.Headers[tpch.ORDERS][3], tpchData.Headers[tpch.ORDERS][1]
		registerArgs := crdt.StateReadArguments{}
		orderMapArgs := crdt.EmbMapPartialArguments{Args: map[string]crdt.ReadArguments{orderDate: registerArgs, orderTotalPrice: registerArgs, orderCustKey: registerArgs}}
		orderStates := getTableState(client, orderMapArgs, tpch.ORDERS, orderIDs, tpchData.Tables.OrderkeyToRegionkey)
		/*
			if isMulti {
				readParams, orderIDsArgs := getReadArgsPerBucket(ORDERS)
				for _, orderID := range orderIDs {
					orderIDsArgs[tpchData.Tables.OrderkeyToRegionkey(orderID)][strconv.FormatInt(int64(orderID), 10)] = orderMapArgs
				}
				for i, args := range orderIDsArgs {
					if len(args) == 0 {
						delete(orderIDsArgs, i)
					}
				}

				replies := sendReceiveBucketReadProtos(readParams)
				orderStates := mergeMapReplies(replies)
			} else {

			}
		*/

		customerIDs := make([]int32, 100, 100)
		mapCustomerIDs := make(map[int32]struct{})
		var currCustomerID int64
		var int32CustomerID int32
		written = 0
		for _, orderID := range orderIDs {
			currCustomerID, _ = strconv.ParseInt(orderStates[strconv.FormatInt(int64(orderID), 10)].States[orderCustKey].(crdt.RegisterState).Value.(string), 10, 32)
			int32CustomerID = int32(currCustomerID)
			_, has := mapCustomerIDs[int32CustomerID]
			if !has {
				mapCustomerIDs[int32CustomerID] = struct{}{}
				customerIDs[written] = int32CustomerID
				written++
			}
		}
		customerIDs = customerIDs[:written]

		cName := tpchData.Headers[tpch.CUSTOMER][1]
		orderMapArgs = crdt.EmbMapPartialArguments{Args: map[string]crdt.ReadArguments{cName: registerArgs}}
		custStates := getTableState(client, orderMapArgs, tpch.CUSTOMER, customerIDs, tpchData.Tables.Custkey32ToRegionkey)

		if PRINT_QUERY {
			fmt.Printf("[ClientQuery][TopK]Q18 top 100 customers for large quantity orders with quantity above %d\n", rndQuantity)
			nPrinted := 0
			var order, customer map[string]crdt.State
			for _, pair := range values {
				//TODO: I don't think this is necessary
				if pair.GetScore() < rndQuantity {
					break
				}
				order = orderStates[strconv.FormatInt(int64(pair.GetPlayerId()), 10)].States
				currCustomerID, _ = strconv.ParseInt(order[orderCustKey].(crdt.RegisterState).Value.(string), 10, 32)
				customer = custStates[strconv.FormatInt(currCustomerID, 10)].States
				fmt.Printf("%s | %d | %d | %s | %s | %d\n",
					customer[cName], currCustomerID, pair.GetPlayerId(), order[orderDate], order[orderTotalPrice], pair.GetScore())
				nPrinted++
			}
		}

		if isIndexGlobal {
			return 1 + len(orderIDs) + len(customerIDs)
		}
		return len(client.serverConns) + len(orderIDs) + len(customerIDs)
	}

	if PRINT_QUERY {
		if len(values) == 0 {
			fmt.Printf("[ClientQuery][TopK]Q18: top 100 customers for large quantity orders with quantity above %d: no match found.", rndQuantity)
		} else {
			fmt.Printf("[ClientQuery][TopK]Q18 top 100 customers for large quantity orders with quantity above %d\n", rndQuantity)
			var extraData []string
			for _, pair := range values {
				extraData = unpackIndexExtraData(pair.GetData(), Q18_N_EXTRA_DATA)
				fmt.Printf("%s | %s | %d | %s | %s | %d\n",
					extraData[0], extraData[1], pair.GetPlayerId(), extraData[4]+"/"+extraData[3]+"/"+extraData[2], extraData[5], pair.GetScore())
			}
		}
	} else {
		//Still process the extra data
		for _, pair := range values {
			unpackIndexExtraData(pair.GetData(), Q18_N_EXTRA_DATA)
		}
	}
	if isIndexGlobal {
		return 1
	}
	return len(client.serverConns)
}

// For simplicity, this always queries 61-120 so that two entries need to be added
// Note that this is penalizing PotionDB.
func getQ1Reads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndDay := client.rng.Int63n(60) + 61 //0-59 + 61 = 61-120, as intended.
	partialR[bufI[1]] = q1ReadParamsHelper(strconv.FormatInt(rndDay, 10))
	bufI[1]++
	//partialR[bufI[1]], partialR[bufI[1]+1] = q1ReadParamsHelper(strconv.FormatInt(rndDay, 10)), q1ReadParamsHelper("120")
	//bufI[1] += 2
	if PRINT_QUERY {
		fmt.Println("[ClientQuery][MapFullRead]Q1 query: requesting delta days ", rndDay)
	}
	return reads + 2
}

func getQ1LocalDirectReads(client QueryClient, fullR, partialR [][]crdt.ReadObjectParams, bufI [][]int, rngRegions []int, rngRegionsI, reads int) (newReads, rngRegion int) {
	rndDay, nRegions := client.rng.Int63n(60)+61, len(fullR) //0-59 + 61 = 61-120, as intended.
	for i := 0; i < nRegions; i++ {
		//partialR[i][bufI[i][1]], partialR[i][bufI[i][1]+1] = q1ReadParamsHelper(strconv.FormatInt(rndDay, 10)), q1ReadParamsHelper("120")
		//bufI[i][1] += 2
		partialR[i][bufI[i][1]] = q1ReadParamsHelper(strconv.FormatInt(rndDay, 10))
	}
	if PRINT_QUERY {
		fmt.Println("[ClientQuery][MapFullRead]Q1 query: requesting delta days ", rndDay)
	}
	return reads + 2*nRegions, 0
}

func getQ1LocalServerReads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndDay, nRegions, offset := client.rng.Int63n(60)+61, len(fullR), bufI[1] //0-59 + 61 = 61-120, as intended.
	for i := 0; i < nRegions; i++ {
		//partialR[offset+i*2], partialR[offset+i*2+1] = q1ReadParamsHelper(strconv.FormatInt(rndDay, 10)), q1ReadParamsHelper("120")
		partialR[offset+i] = q1ReadParamsHelper(strconv.FormatInt(rndDay, 10))
	}
	bufI[1] += nRegions
	//bufI[1] += nRegions * 2
	if PRINT_QUERY {
		fmt.Println("[ClientQuery][MapFullRead]Q1 query: requesting delta days ", rndDay)
	}
	return reads + 2*nRegions
}

func processQ1Reply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	//processQ1ReplyProto(replies[bufI[1]], replies[bufI[1]+1], client)
	//bufI[1] += 2
	//return reads + 2
	processQ1ReplyProto(replies[bufI[1]], client)
	bufI[1]++
	return reads + 2
}

func processQ1LocalDirectReply(client QueryClient, replies [][]*proto.ApbReadObjectResp, bufI [][]int, rngRegions []int, rngRegionsI int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	bufI1 := localDirectBufIToServerBufI(bufI, 1)
	relevantReplies := readRepliesToOneSlice(replies, bufI1, 1 /*2*/)
	//No need to call processQ1ReplyProto as the merge already does the processing
	//printQ1Result(crdt.ReadRespProtoToAntidoteState(mergeQ1IndexReply(relevantReplies)[0], proto.CRDTType_RRMAP, proto.READType_GET_ALL_VALUES).(crdt.EmbMapGetValuesState))
	printQ1Result(crdt.ReadRespProtoToAntidoteState(mergeQ1IndexReply(relevantReplies)[0], proto.CRDTType_RRMAP, proto.READType_GET_VALUES).(crdt.EmbMapGetValuesState))
	incrementAllBufILocalDirect(bufI, 1, 1 /*2*/)
	return reads + nRegions*2
}

func processQ1LocalServerReply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	start, end := bufI[1] /*bufI[1]+2*nRegions*/, bufI[1]+nRegions
	//No need to call processQ1ReplyProto as the merge already does the processing
	printQ1Result(crdt.ReadRespProtoToAntidoteState(mergeQ1IndexReply(replies[start:end])[1], proto.CRDTType_RRMAP, proto.READType_GET_VALUES).(crdt.EmbMapGetValuesState))
	//printQ1Result(crdt.ReadRespProtoToAntidoteState(mergeQ1IndexReply(replies[start:end])[1], proto.CRDTType_RRMAP, proto.READType_GET_ALL_VALUES).(crdt.EmbMapGetValuesState))
	//bufI[1] += 2 * nRegions
	bufI[1] += nRegions
	return reads + 2*nRegions
}

func processQ1ReplyProto( /*askedReply, baseReply *proto.ApbReadObjectResp*/ reply *proto.ApbReadObjectResp, client QueryClient) {
	//fmt.Println("[ProcessQ1]Asked State (proto):", askedReply)
	//fmt.Println()
	//fmt.Println("[ProcessQ1]Base State (proto):", baseReply)
	//fmt.Println()
	//askedState, baseState := crdt.ReadRespProtoToAntidoteState(askedReply, proto.CRDTType_RRMAP, proto.READType_GET_ALL_VALUES).(crdt.EmbMapGetValuesState),
	//crdt.ReadRespProtoToAntidoteState(baseReply, proto.CRDTType_RRMAP, proto.READType_GET_ALL_VALUES).(crdt.EmbMapGetValuesState)
	fullState := crdt.ReadRespProtoToAntidoteState(reply, proto.CRDTType_RRMAP, proto.READType_GET_VALUES).(crdt.EmbMapGetValuesState)
	var askedState, baseState crdt.EmbMapGetValuesState
	//fmt.Printf("[ClientQuery][Q1]Full state: %+v\n", fullState)
	//fmt.Println()
	for key, state := range fullState.States {
		if key == "120" {
			//fmt.Println("[ClientQuery][Q1]Found state with 120")
			baseState = state.(crdt.EmbMapGetValuesState)
		} else {
			//fmt.Println("[ClientQuery][Q1]Found state with", key)
			askedState = state.(crdt.EmbMapGetValuesState)
		}
	}
	/*fmt.Println("[ProcessQ1]Asked State:", askedState)
	fmt.Println()
	fmt.Println("[ProcessQ1]Base State:", baseState)
	fmt.Println()*/
	mergedState := crdt.EmbMapGetValuesState{States: make(map[string]crdt.State, len(askedState.States))}
	for key, innerAsked := range askedState.States {
		mergedState.States[key] = q1MergeReadResult(innerAsked.(crdt.EmbMapGetValuesState), baseState.States[key].(crdt.EmbMapGetValuesState))
	}
	printQ1Result(mergedState)
	*client.nEntries += 2 * len(askedState.States)
}

// For simplicity, this always queries 61-120 so that two entries need to be added
// Note that this is penalizing PotionDB.
func sendQ1(client QueryClient) (nRequests int) {
	rndDay := client.rng.Int63n(60) + 61 //0-59 + 61 = 61-120, as intended.

	//readParam := []crdt.ReadObjectParams{q1ReadParamsHelper(strconv.FormatInt(rndDay, 10)), q1ReadParamsHelper("120")}
	readParam := []crdt.ReadObjectParams{q1ReadParamsHelper(strconv.FormatInt(rndDay, 10))}
	//fmt.Printf("%+v\n", readParam)
	//fmt.Println("Rnd day:", rndDay)
	replyProto := mergeIndex(sendReceiveIndexQuery(client, nil, readParam), 1)

	if PRINT_QUERY {
		date := (&tpch.Date{YEAR: 1998, MONTH: 12, DAY: 1}).CalculateDate(int(-1 * rndDay))
		fmt.Println("Date:", date.ToString())
	}
	/*fmt.Println("[SendQ1]First replyProto:", replyProto[0])
	fmt.Println()
	fmt.Println("[SendQ1]Second replyProto:", replyProto[1])
	fmt.Println()*/
	processQ1ReplyProto(replyProto[0], client)
	//processQ1ReplyProto(replyProto[0], replyProto[1], client)

	//mapStateMerged := processQ1ReplyProto(mapStateAsked, mapStateBased)

	if isIndexGlobal {
		return 2
	}
	return 2 * len(client.serverConns)
}

func q1MergeReadResult(askedState, baseState crdt.EmbMapGetValuesState) (mergedState crdt.EmbMapGetValuesState) {
	//fmt.Printf("[Queries][Q1MergeReadResult]AskedState: %+v\nBaseState: %+v\n", askedState, baseState)
	sq := askedState.States[Q1_SUM_QTY].(crdt.CounterState).Value + baseState.States[Q1_SUM_QTY].(crdt.CounterState).Value
	sbp := askedState.States[Q1_SUM_BASE_PRICE].(crdt.CounterFloatState).Value + baseState.States[Q1_SUM_BASE_PRICE].(crdt.CounterFloatState).Value
	sdc := askedState.States[Q1_SUM_DISC_PRICE].(crdt.CounterFloatState).Value + baseState.States[Q1_SUM_DISC_PRICE].(crdt.CounterFloatState).Value
	sc := askedState.States[Q1_SUM_CHARGE].(crdt.CounterFloatState).Value + baseState.States[Q1_SUM_CHARGE].(crdt.CounterFloatState).Value
	co := askedState.States[Q1_COUNT_ORDER].(crdt.CounterState).Value + baseState.States[Q1_COUNT_ORDER].(crdt.CounterState).Value
	aqA, aqB := askedState.States[Q1_AVG_QTY].(crdt.AvgFullState), baseState.States[Q1_AVG_QTY].(crdt.AvgFullState)
	apA, apB := askedState.States[Q1_AVG_PRICE].(crdt.AvgFullState), baseState.States[Q1_AVG_PRICE].(crdt.AvgFullState)
	adA, adB := askedState.States[Q1_AVG_DISC].(crdt.AvgFullState), baseState.States[Q1_AVG_DISC].(crdt.AvgFullState)

	states := make(map[string]crdt.State)
	states[Q1_SUM_QTY], states[Q1_SUM_BASE_PRICE], states[Q1_SUM_DISC_PRICE] = crdt.CounterState{Value: sq}, crdt.CounterFloatState{Value: sbp}, crdt.CounterFloatState{Value: sdc}
	states[Q1_SUM_CHARGE], states[Q1_COUNT_ORDER] = crdt.CounterFloatState{Value: sc}, crdt.CounterState{Value: co}
	states[Q1_AVG_QTY], states[Q1_AVG_PRICE], states[Q1_AVG_DISC] = crdt.AvgFullState{Sum: aqA.Sum + aqB.Sum, NAdds: aqA.NAdds + aqB.NAdds},
		crdt.AvgFullState{Sum: apA.Sum + apB.Sum, NAdds: apA.NAdds + apB.NAdds}, crdt.AvgFullState{Sum: adA.Sum + adB.Sum, NAdds: adA.NAdds + adB.NAdds}
	mergedState.States = states
	//fmt.Printf("[Queries][Q1MergeReadResult]MergedState: %+v\n", mergedState.States)
	//fmt.Println()
	return
}

/*func q1ReadParamsHelper(day string) crdt.ReadObjectParams {
	outerArgs := crdt.EmbMapPartialArguments{Args: make(map[string]crdt.ReadArguments)}
	cr, ar := crdt.StateReadArguments{}, crdt.AvgGetFullArguments{}
	outerArgs.Args[Q1_SUM_QTY], outerArgs.Args[Q1_SUM_BASE_PRICE], outerArgs.Args[Q1_SUM_DISC_PRICE], outerArgs.Args[Q1_SUM_CHARGE], outerArgs.Args[Q1_COUNT_ORDER] = cr, cr, cr, cr, cr
	outerArgs.Args[Q1_AVG_QTY], outerArgs.Args[Q1_AVG_PRICE], outerArgs.Args[Q1_AVG_DISC] = ar, ar, ar

	return crdt.ReadObjectParams{
		KeyParams: crdt.KeyParams{Key: Q1_KEY + day, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]},
		ReadArgs:  crdt.EmbMapPartialOnAllArguments{ArgForAll: outerArgs},
	}
}*/

func q1ReadParamsHelper(day string) crdt.ReadObjectParams {
	outerArgs := crdt.EmbMapPartialArguments{Args: make(map[string]crdt.ReadArguments)}
	cr, ar := crdt.StateReadArguments{}, crdt.AvgGetFullArguments{}
	outerArgs.Args[Q1_SUM_QTY], outerArgs.Args[Q1_SUM_BASE_PRICE], outerArgs.Args[Q1_SUM_DISC_PRICE], outerArgs.Args[Q1_SUM_CHARGE], outerArgs.Args[Q1_COUNT_ORDER] = cr, cr, cr, cr, cr
	outerArgs.Args[Q1_AVG_QTY], outerArgs.Args[Q1_AVG_PRICE], outerArgs.Args[Q1_AVG_DISC] = ar, ar, ar

	return crdt.ReadObjectParams{
		KeyParams: crdt.KeyParams{Key: Q1_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]},
		ReadArgs: crdt.EmbMapPartialArguments{Args: map[string]crdt.ReadArguments{
			day: crdt.EmbMapPartialOnAllArguments{ArgForAll: outerArgs}, "120": crdt.EmbMapPartialOnAllArguments{ArgForAll: outerArgs}}},
	}
}

func printQ1Result(mapState crdt.EmbMapGetValuesState) {
	//fmt.Println("[PrintQ1Result]State:", mapState)
	if PRINT_QUERY {
		fmt.Println("Q1: Summary report for shipped lineitems")
		//fmt.Printf("[ClientQuery][Q1Print]Map State: %+v\n", mapState)
		for key, insideState := range mapState.States {
			insideMap := insideState.(crdt.EmbMapGetValuesState)
			qtyState, priceState, discState := insideMap.States[Q1_AVG_QTY].(crdt.AvgFullState), insideMap.States[Q1_AVG_PRICE].(crdt.AvgFullState), insideMap.States[Q1_AVG_DISC].(crdt.AvgFullState)
			//fmt.Printf("Key: %v, State: %+v\n", key, insideMap)
			fmt.Printf("ReturnFlag: %v, LineStatus: %v, Sum_qty: %d, Sum_base_price: %.2f, Sum_disc_price: %.2f, Sum_charge: %.2f, Avg_qty: %.2f, Avg_price: %.2f, Avg_disc: %.2f, Count_order: %d\n",
				key[0:1], key[1:2], insideMap.States[Q1_SUM_QTY].(crdt.CounterState).Value, insideMap.States[Q1_SUM_BASE_PRICE].(crdt.CounterFloatState).Value,
				insideMap.States[Q1_SUM_DISC_PRICE].(crdt.CounterFloatState).Value, insideMap.States[Q1_SUM_CHARGE].(crdt.CounterFloatState).Value,
				float64(qtyState.Sum)/float64(qtyState.NAdds), (float64(priceState.Sum) / float64(priceState.NAdds) / 100.0),
				(float64(discState.Sum) / float64(discState.NAdds) / 100.0), insideMap.States[Q1_COUNT_ORDER].(crdt.CounterState).Value)
		}
	}
}

func getQ2Reads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndRegion, rndSize, rndType := int64(client.getRngRegion()), client.rng.Int63n(50)+1, client.rng.Int63n(5)
	rndTypeS := tpchData.Tables.ShortType[rndType]
	fullR[bufI[0]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q2_KEY + strconv.FormatInt(rndRegion, 10) + strconv.FormatInt(rndSize, 10) + rndTypeS, CrdtType: proto.CRDTType_TOPK, Bucket: buckets[INDEX_BKT]}}
	bufI[0]++
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][TopKFull]Q2 query: requesting region %d, size %d, type '%s'\n", rndRegion, rndSize, rndTypeS)
	}
	return reads + 1
}

func getQ2LocalDirectReads(client QueryClient, fullR, partialR [][]crdt.ReadObjectParams, bufI [][]int, rngRegions []int, rngRegionsI, reads int) (newReads, rngRegion int) {
	rndRegion, rndSize, rndType := client.getRngRegion(), client.rng.Int63n(50)+1, client.rng.Int63n(5)
	rndTypeS := tpchData.Tables.ShortType[rndType]
	fullR[rndRegion][bufI[rndRegion][0]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q2_KEY + strconv.FormatInt(int64(rndRegion), 10) + strconv.FormatInt(rndSize, 10) + rndTypeS, CrdtType: proto.CRDTType_TOPK, Bucket: buckets[INDEX_BKT]}}
	bufI[rndRegion][0]++
	rngRegions[rngRegionsI] = rndRegion
	return reads + 1, rndRegion
}

func getQ2LocalServerReads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndRegion, rndSize, rndType := client.getRngRegion(), client.rng.Int63n(50)+1, client.rng.Int63n(5)
	rndTypeS := tpchData.Tables.ShortType[rndType]
	fullR[bufI[0]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q2_KEY + strconv.FormatInt(int64(rndRegion), 10) + strconv.FormatInt(rndSize, 10) + rndTypeS, CrdtType: proto.CRDTType_TOPK, Bucket: buckets[INDEX_BKT]}}
	bufI[0]++
	return reads + 1
}

func processQ2Reply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	processQ2ReplyProto(replies[bufI[0]], client)
	bufI[0]++
	return reads + 1
}

func processQ2LocalDirectReply(client QueryClient, replies [][]*proto.ApbReadObjectResp, bufI [][]int, rngRegions []int, rngRegionsI int, reads int) (newReads int) {
	index := rngRegions[rngRegionsI]
	processQ2ReplyProto(replies[index][bufI[index][0]], client)
	bufI[index][0]++
	return reads + 1
}

func sendQ2(client QueryClient) (nRequests int) {
	rndRegion, rndSize, rndType := client.getRngRegion(), client.rng.Int63n(50)+1, client.rng.Int63n(5)
	rndTypeS := tpchData.Tables.ShortType[rndType]
	bktI, serverI := getIndexOffset(client, rndRegion)

	readParam := []crdt.ReadObjectParams{{KeyParams: crdt.KeyParams{
		Key: Q2_KEY + strconv.FormatInt(int64(rndRegion), 10) + strconv.FormatInt(rndSize, 10) + rndTypeS, CrdtType: proto.CRDTType_TOPK, Bucket: buckets[bktI],
	}}}

	replyProto := sendReceiveReadObjsProto(client, readParam, serverI)

	if PRINT_QUERY {
		fmt.Printf("Q2: Min cost in region %s for part size %d and part type %s\n", tpchData.Tables.Regions[rndRegion].R_NAME, rndSize, rndTypeS)
	}
	processQ2ReplyProto(replyProto.GetObjects().GetObjects()[0], client)
	return 1
}

func processQ2ReplyProto(proto *proto.ApbReadObjectResp, client QueryClient) {
	values := proto.GetTopk().GetValues()
	if PRINT_QUERY {
		fmt.Println("[ClientQuery]Q2 results:")
		sort.Slice(values, func(i, j int) bool { return values[i].GetScore() > values[j].GetScore() })
		//fmt.Println("[ClientQuery]Q2", values, len(values))
		var extraData []string
		fmt.Println("PartId: SupplyCost | SupplierBalance | SupplierName | NationName | P_MFGR | SupplierAddress | SupplierPhone | SupplierComment ")
		for _, pair := range values {
			extraData = unpackIndexExtraData(pair.GetData(), Q2_N_EXTRA_DATA)
			fmt.Printf("%d: %d", pair.GetPlayerId(), pair.GetScore())
			for _, data := range extraData {
				fmt.Printf("| %s", data)
			}
			fmt.Println()
		}
	} else {
		//Still process the extra data
		for _, pair := range values {
			unpackIndexExtraData(pair.GetData(), Q2_N_EXTRA_DATA)
		}
	}
	*client.nEntries += len(values)
}

func getQ4Reads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndYear, rndQuarter := client.rng.Int63n(5)+1993, (client.rng.Int63n(4)*3 + 1)
	key := Q4_KEY + strconv.FormatInt(rndYear, 10) + strconv.FormatInt(rndQuarter, 10)
	fullR[bufI[0]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: key, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}}
	bufI[0]++
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][EmbMap]Q4 query: requesting counters for year %d, quarter (month) %d\n", rndYear, rndQuarter)
	}
	return reads + 1
}

func getQ4LocalDirectReads(client QueryClient, fullR, partialR [][]crdt.ReadObjectParams, bufI [][]int, rngRegions []int, rngRegionsI, reads int) (newReads, rngRegion int) {
	rndYear, rndQuarter, nRegions := client.rng.Int63n(5)+1993, (client.rng.Int63n(4)*3 + 1), len(client.serverConns)
	key := Q4_KEY + strconv.FormatInt(rndYear, 10) + strconv.FormatInt(rndQuarter, 10)
	for i := 0; i < nRegions; i++ {
		fullR[i][bufI[i][0]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: key, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+i]}}
		bufI[i][0]++
	}
	return reads + nRegions, 0
}

func getQ4LocalServerReads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndYear, rndQuarter, nRegions, offset := client.rng.Int63n(5)+1993, (client.rng.Int63n(4)*3 + 1), len(client.serverConns), bufI[0]
	key := Q4_KEY + strconv.FormatInt(rndYear, 10) + strconv.FormatInt(rndQuarter, 10)
	for i := 0; i < nRegions; i++ {
		fullR[offset+i] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: key, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+i]}}
	}
	bufI[0] += nRegions
	return reads + nRegions
}

func processQ4Reply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	processQ4ReplyProto(replies[bufI[0]], client)
	bufI[0]++
	return reads + 1
}

func processQ4LocalDirectReply(client QueryClient, replies [][]*proto.ApbReadObjectResp, bufI [][]int, rngRegions []int, rngRegionsI int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	bufI0 := localDirectBufIToServerBufI(bufI, 0)
	relevantReplies := readRepliesToOneSlice(replies, bufI0, 1)
	processQ4ReplyProto(mergeQ4IndexReply(relevantReplies)[0], client)
	incrementAllBufILocalDirect(bufI, 0, 1)
	return reads + nRegions
}

// Queries that are naturally local (i.e., no merging needed) do not need processLocalServerReply
func processQ4LocalServerReply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	start, end := bufI[0], bufI[0]+nRegions
	processQ4ReplyProto(mergeQ4IndexReply(replies[start:end])[0], client)
	bufI[0] += nRegions
	return reads + nRegions
}

func processQ4ReplyProto(proto *proto.ApbReadObjectResp, client QueryClient) {
	entries := proto.GetMap().GetEntries()
	if PRINT_QUERY {
		fmt.Println("[ClientQuery]Q4 results:")
		for _, protoEntry := range entries {
			fmt.Printf("%s: %d\n", string(protoEntry.GetKey().GetKey()), protoEntry.GetValue().GetCounter().GetValue())
		}
	}
	*client.nEntries += len(entries)
}

func sendQ4(client QueryClient) (nRequests int) {
	rndYear, rndQuarter := client.rng.Int63n(5)+1993, (client.rng.Int63n(4)*3 + 1)
	key := Q4_KEY + strconv.FormatInt(rndYear, 10) + strconv.FormatInt(rndQuarter, 10)
	readParam := []crdt.ReadObjectParams{{
		KeyParams: crdt.KeyParams{Key: key, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}}}
	mapProto := mergeIndex(sendReceiveIndexQuery(client, readParam, nil), 4)[0]

	if PRINT_QUERY {
		fmt.Printf("Q4: Number of late orders for year %d, quarter (month) %d\n", rndYear, rndQuarter)
	}
	processQ4ReplyProto(mapProto, client)

	if isIndexGlobal {
		return 1
	}
	return len(client.serverConns)
}

func getQ6Reads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndYear, rndDiscount, rndQuantity := client.rng.Int63n(5)+1993, client.rng.Int63n(int64(MAX_Q6_DISCOUNT-MIN_Q6_DISCOUNT-1))+2, 24+client.rng.Int63n(2)
	key, fRead := Q6_KEY+strconv.FormatInt(rndYear, 10)+strconv.FormatInt(rndQuantity, 10), crdt.StateReadArguments{}
	quantityKeys := map[string]crdt.ReadArguments{strconv.FormatInt(rndDiscount-1, 10): fRead, strconv.FormatInt(rndDiscount, 10): fRead, strconv.FormatInt(rndDiscount+1, 10): fRead}
	partialR[bufI[1]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: key, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}, ReadArgs: crdt.EmbMapPartialArguments{Args: quantityKeys}}
	bufI[1]++
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][EmbMap]Q6 query: promotion revenue increase for year %d, discount %d%% to %d%%, for quantity <= %d, if no discount was applied.\n", rndYear, rndDiscount-1, rndDiscount+1, rndQuantity)
	}
	return reads + 3
}

func getQ6LocalDirectReads(client QueryClient, fullR, partialR [][]crdt.ReadObjectParams, bufI [][]int, rngRegions []int, rngRegionsI, reads int) (newReads, rngRegion int) {
	rndYear, rndDiscount, rndQuantity, nRegions := client.rng.Int63n(5)+1993, client.rng.Int63n(int64(MAX_Q6_DISCOUNT-MIN_Q6_DISCOUNT-1))+2, 24+client.rng.Int63n(2), len(client.serverConns)
	key, fRead := Q6_KEY+strconv.FormatInt(rndYear, 10)+strconv.FormatInt(rndQuantity, 10), crdt.StateReadArguments{}
	quantityKeys := map[string]crdt.ReadArguments{strconv.FormatInt(rndDiscount-1, 10): fRead, strconv.FormatInt(rndDiscount, 10): fRead, strconv.FormatInt(rndDiscount+1, 10): fRead}
	for i := 0; i < nRegions; i++ {
		partialR[i][bufI[i][1]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: key, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+i]}, ReadArgs: crdt.EmbMapPartialArguments{Args: quantityKeys}}
		bufI[i][1]++
	}
	return reads + nRegions*3, 0
}

func getQ6LocalServerReads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndYear, rndDiscount, rndQuantity := client.rng.Int63n(5)+1993, client.rng.Int63n(int64(MAX_Q6_DISCOUNT-MIN_Q6_DISCOUNT-1))+2, 24+client.rng.Int63n(2)
	nRegions, offset := len(client.serverConns), bufI[1]
	key, fRead := Q6_KEY+strconv.FormatInt(rndYear, 10)+strconv.FormatInt(rndQuantity, 10), crdt.StateReadArguments{}
	quantityKeys := map[string]crdt.ReadArguments{strconv.FormatInt(rndDiscount-1, 10): fRead, strconv.FormatInt(rndDiscount, 10): fRead, strconv.FormatInt(rndDiscount+1, 10): fRead}
	for i := 0; i < nRegions; i++ {
		partialR[offset+i] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: key, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+i]}, ReadArgs: crdt.EmbMapPartialArguments{Args: quantityKeys}}
	}
	bufI[1] += nRegions
	return reads + nRegions*3
}

func processQ6Reply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	processQ6ReplyProto(replies[bufI[1]], client)
	bufI[1]++
	return reads + 3
}

func processQ6LocalDirectReply(client QueryClient, replies [][]*proto.ApbReadObjectResp, bufI [][]int, rngRegions []int, rngRegionsI int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	bufI1 := localDirectBufIToServerBufI(bufI, 1)
	relevantReplies := readRepliesToOneSlice(replies, bufI1, 1)
	processQ6ReplyProto(mergeQ6IndexReply(relevantReplies)[0], client)
	incrementAllBufILocalDirect(bufI, 1, 1)
	return reads + 3*nRegions
}

// Queries that are naturally local (i.e., no merging needed) do not need processLocalServerReply
func processQ6LocalServerReply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	start, end := bufI[1], bufI[1]+nRegions
	processQ6ReplyProto(mergeQ6IndexReply(replies[start:end])[0], client)
	bufI[1] += nRegions
	return reads + 3*nRegions
}

func processQ6ReplyProto(proto *proto.ApbReadObjectResp, client QueryClient) {
	//entries := proto.GetMap().GetEntries()
	entries := proto.GetPartread().GetMap().GetGetvalues().GetValues()
	//Need to do the merging even if not printing, for fairness
	sum := 0.0
	for _, protoEntry := range entries {
		sum += protoEntry.GetValue().GetCounterfloat().GetValue()
	}
	if PRINT_QUERY {
		fmt.Println("[ClientQuery]Q6 results (increase of sales revenue if a certain % of discount was not applied): ", sum)
	}
	*client.nEntries += len(entries)
}

func sendQ6(client QueryClient) (nRequests int) {
	rndYear, rndDiscount, rndQuantity := client.rng.Int63n(5)+1993, client.rng.Int63n(int64(MAX_Q6_DISCOUNT-MIN_Q6_DISCOUNT-1))+2, 24+client.rng.Int63n(2)
	//rndYear, rndDiscount, rndQuantity = 1994, 2, 25
	key, fRead := Q6_KEY+strconv.FormatInt(rndYear, 10)+strconv.FormatInt(rndQuantity, 10), crdt.StateReadArguments{}
	quantityKeys := map[string]crdt.ReadArguments{strconv.FormatInt(rndDiscount-1, 10): fRead, strconv.FormatInt(rndDiscount, 10): fRead, strconv.FormatInt(rndDiscount+1, 10): fRead}
	readParam := []crdt.ReadObjectParams{{KeyParams: crdt.KeyParams{Key: key, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}, ReadArgs: crdt.EmbMapPartialArguments{Args: quantityKeys}}}
	mapProto := mergeIndex(sendReceiveIndexQuery(client, nil, readParam), 6)[0]

	if PRINT_QUERY {
		fmt.Printf("Q6: Revenue increase if sales in %d with at most %d units and with a discount between %d%% and %d%% had no discount.\n", rndYear, rndQuantity, rndDiscount-1, rndDiscount+1)
	}
	processQ6ReplyProto(mapProto, client)

	if isIndexGlobal {
		return 3
	}
	return 3 * len(tpchData.Tables.Regions)
}

func getQ7Reads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	partialR[bufI[1]] = getQ7ReadParams(client)
	bufI[1]++
	return reads + 2
}

func getQ7ReadParams(client QueryClient) crdt.ReadObjectParams {
	nNations := int64(len(tpchData.Tables.Nations))
	rndNat1, rndNat2 := client.rng.Int63n(nNations), client.rng.Int63n(nNations)
	for rndNat1 == rndNat2 {
		rndNat2 = client.rng.Int63n(nNations)
	}
	if rndNat1 > rndNat2 { //The first nation to query must be the one with the lower ID.
		tmp := rndNat1
		rndNat1 = rndNat2
		rndNat2 = tmp
	}
	rndNat1Name, rndNat2Name := tpchData.Tables.Nations[rndNat1].N_NAME, tpchData.Tables.Nations[rndNat2].N_NAME
	keyParams := crdt.KeyParams{Key: Q7_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}
	if PRINT_QUERY {
		fmt.Println("[ClientQuery][EmbMap]Q7 query: revenue of sales in which the customer and supplier are in different nations.")
		fmt.Printf("[ClientQuery][EmbMap]Q7 query: nation1, nation2: %s, %s\n", rndNat1Name, rndNat2Name)
	}
	return crdt.ReadObjectParams{KeyParams: keyParams, ReadArgs: crdt.EmbMapPartialOnAllArguments{ArgForAll: crdt.EmbMapGetValueArguments{Key: rndNat1Name, Args: crdt.EmbMapGetValueArguments{Key: rndNat2Name, Args: crdt.StateReadArguments{}}}}}
}

func getQ7LocalDirectReads(client QueryClient, fullR, partialR [][]crdt.ReadObjectParams, bufI [][]int, rngRegions []int, rngRegionsI, reads int) (newReads, rngRegion int) {
	rngRegion = client.getRngRegion()
	partialR[rngRegion][bufI[rngRegion][1]] = getQ7LocalReadParams(client, rngRegion)
	bufI[rngRegion][1]++
	return reads + 2, rngRegion
}

func getQ7LocalServerReads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rngRegion := client.getRngRegion()
	partialR[bufI[1]] = getQ7LocalReadParams(client, rngRegion)
	bufI[1]++
	return reads + 2
}

func getQ7LocalReadParams(client QueryClient, clientRegion int) crdt.ReadObjectParams {
	regionNations := tpchData.Tables.NationsByRegion[clientRegion]
	allCountries := tpchData.Tables.Nations
	nNations := len(allCountries)
	rndNat1, rndNat2 := int(regionNations[client.rng.Intn(len(regionNations))]), client.rng.Intn(nNations)
	for rndNat1 == rndNat2 {
		rndNat2 = client.rng.Intn(nNations)
	}
	rndNat1Name, rndNat2Name := allCountries[rndNat1].N_NAME, allCountries[rndNat2].N_NAME
	keyParams := crdt.KeyParams{Key: Q7_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+clientRegion]}
	return crdt.ReadObjectParams{KeyParams: keyParams, ReadArgs: crdt.EmbMapPartialOnAllArguments{ArgForAll: crdt.EmbMapGetValueArguments{Key: rndNat1Name, Args: crdt.EmbMapGetValueArguments{Key: rndNat2Name, Args: crdt.StateReadArguments{}}}}}
}

func processQ7Reply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	processQ7ReplyProto(replies[bufI[1]], client)
	bufI[1]++
	return reads + 2
}

func processQ7LocalDirectReply(client QueryClient, replies [][]*proto.ApbReadObjectResp, bufI [][]int, rngRegions []int, rngRegionsI int, reads int) (newReads int) {
	index := rngRegions[rngRegionsI]
	processQ7ReplyProto(replies[index][bufI[index][1]], client)
	bufI[index][1]++
	return reads + 2
}

func processQ7ReplyProto(proto *proto.ApbReadObjectResp, client QueryClient) {
	//yearsEntries := proto.GetMap().GetEntries()
	yearsEntries := proto.GetPartread().GetMap().GetGetvalues().GetValues()
	//fmt.Printf("base proto: %+v\n", yearsEntries)
	firstEntry, secondEntry := yearsEntries[0], yearsEntries[1]
	//fmt.Printf("1995 proto: %+v\n", firstEntry)
	firstMiddle := firstEntry.GetValue().GetPartread().GetMap().GetGetvalue()
	firstInside := firstMiddle.GetValue().GetPartread().GetMap().GetGetvalue()
	//countryOne, countryTwo := string(firstMiddle.GetKey().GetKey()), string(firstInside.GetKey().GetKey())
	secondInside := secondEntry.GetValue().GetPartread().GetMap().GetGetvalue().GetValue().GetPartread().GetMap().GetGetvalue()
	if PRINT_QUERY {
		fmt.Println("[ClientQuery]Q7 results (revenue between the two selected countries):")
		fmt.Printf("1995: %d; ", firstInside.GetValue().GetCounter().GetValue())
		fmt.Printf("1996: %d; ", secondInside.GetValue().GetCounter().GetValue())
		fmt.Println()
	}
	*client.nEntries += 2
}

func sendQ7(client QueryClient) (nRequests int) {
	var readParams crdt.ReadObjectParams
	var serverI int
	if isIndexGlobal {
		readParams = getQ7ReadParams(client)
		_, serverI = getIndexOffset(client, 0) //Region doesn't matter in this case
	} else {
		rngRegion := client.getRngRegion()
		_, serverI = getIndexOffset(client, rngRegion)
		readParams = getQ7LocalReadParams(client, rngRegion)
	}
	replyProto := sendReceiveReadProto(client, nil, []crdt.ReadObjectParams{readParams}, serverI).GetObjects().GetObjects()[0]
	processQ7ReplyProto(replyProto, client)
	return 2
}

func getQ8Reads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndType, rndNation := tpchData.Tables.Types[client.rng.Int63n(int64(len(tpchData.Tables.Types)))], client.getRngNation()
	key, rndRegion, fullArgs := Q8_KEY+rndType, tpchData.Tables.Nations[rndNation].N_REGIONKEY, crdt.StateReadArguments{}
	regIdS, natIdS := "R"+strconv.FormatInt(int64(rndRegion), 10), strconv.FormatInt(int64(rndNation), 10)
	mapKeys := map[string]crdt.ReadArguments{regIdS + "1995": fullArgs, regIdS + "1996": fullArgs, natIdS + "1995": fullArgs, natIdS + "1996": fullArgs}
	partialR[bufI[1]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: key, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}, ReadArgs: crdt.EmbMapPartialArguments{Args: mapKeys}}
	bufI[1]++
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][EmbMap]Q8 query: fraction of revenue for part type %s supplied from nation %s in region %s\n", rndType, tpchData.Tables.Nations[rndNation].N_NAME, tpchData.Tables.Regions[rndRegion].R_NAME)
	}
	return reads + 4
}

func getQ8LocalDirectReads(client QueryClient, fullR, partialR [][]crdt.ReadObjectParams, bufI [][]int, rngRegions []int, rngRegionsI, reads int) (newReads, rngRegion int) {
	rndType, rndNation := tpchData.Tables.Types[client.rng.Int63n(int64(len(tpchData.Tables.Types)))], client.getRngNation()
	key, rndRegion, fullArgs := Q8_KEY+rndType, tpchData.Tables.Nations[rndNation].N_REGIONKEY, crdt.StateReadArguments{}
	regIdS, natIdS := "R"+strconv.FormatInt(int64(rndRegion), 10), strconv.FormatInt(int64(rndNation), 10)
	mapKeys := map[string]crdt.ReadArguments{regIdS + "1995": fullArgs, regIdS + "1996": fullArgs, natIdS + "1995": fullArgs, natIdS + "1996": fullArgs}
	partialR[rndRegion][bufI[rndRegion][1]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: key, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}, ReadArgs: crdt.EmbMapPartialArguments{Args: mapKeys}}
	bufI[rndRegion][1]++
	rngRegions[rngRegionsI] = int(rndRegion)
	return reads + 4, int(rndRegion)
}

func getQ8LocalServerReads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndType, rndNation := tpchData.Tables.Types[client.rng.Int63n(int64(len(tpchData.Tables.Types)))], client.getRngNation()
	key, rndRegion, fullArgs := Q8_KEY+rndType, tpchData.Tables.Nations[rndNation].N_REGIONKEY, crdt.StateReadArguments{}
	regIdS, natIdS := "R"+strconv.FormatInt(int64(rndRegion), 10), strconv.FormatInt(int64(rndNation), 10)
	mapKeys := map[string]crdt.ReadArguments{regIdS + "1995": fullArgs, regIdS + "1996": fullArgs, natIdS + "1995": fullArgs, natIdS + "1996": fullArgs}
	partialR[bufI[1]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: key, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}, ReadArgs: crdt.EmbMapPartialArguments{Args: mapKeys}}
	bufI[1]++
	return reads + 4
}

func processQ8Reply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	processQ8ReplyProto(replies[bufI[1]], client)
	bufI[1]++
	return reads + 4
}

func processQ8LocalDirectReply(client QueryClient, replies [][]*proto.ApbReadObjectResp, bufI [][]int, rngRegions []int, rngRegionsI int, reads int) (newReads int) {
	index := rngRegions[rngRegionsI]
	processQ8ReplyProto(replies[index][bufI[index][1]], client)
	bufI[index][1]++
	return reads + 4
}

func sendQ8(client QueryClient) (nRequests int) {
	rndType, rndNation := tpchData.Tables.Types[client.rng.Int63n(int64(len(tpchData.Tables.Types)))], client.getRngNation()
	//rndType, rndNation = tpchData.Tables.Types[0], 0
	key, rndRegion, fullR := Q8_KEY+rndType, tpchData.Tables.Nations[rndNation].N_REGIONKEY, crdt.StateReadArguments{}
	regIdS, natIdS := "R"+strconv.FormatInt(int64(rndRegion), 10), strconv.FormatInt(int64(rndNation), 10)
	mapKeys := map[string]crdt.ReadArguments{regIdS + "1995": fullR, regIdS + "1996": fullR, natIdS + "1995": fullR, natIdS + "1996": fullR}
	bktI, serverI := getIndexOffset(client, int(rndRegion))

	//fmt.Println("[ClientQueries][SendQ8]Keys: ", mapKeys)
	readParam := []crdt.ReadObjectParams{{KeyParams: crdt.KeyParams{Key: key, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bktI]}, ReadArgs: crdt.EmbMapPartialArguments{Args: mapKeys}}}
	replyProto := sendReceiveReadProto(client, nil, readParam, serverI)

	if PRINT_QUERY {
		fmt.Printf("Q8: fraction of revenue for part type %s for country %s of region %s\n", rndType, tpchData.Tables.Nations[rndNation].N_NAME, tpchData.Tables.Regions[rndRegion].R_NAME)
	}
	processQ8ReplyProto(replyProto.GetObjects().GetObjects()[0], client)
	return 4
}

func processQ8ReplyProto(proto *proto.ApbReadObjectResp, client QueryClient) {
	entries := proto.GetPartread().GetMap().GetGetvalues()
	keys, values := entries.GetKeys(), entries.GetValues()
	//fmt.Println("[ClientQuery][ProcessQ8]Non part read:", proto.GetMap().GetEntries())
	//fmt.Printf("[ClientQuery][ProcessQ8]Entries: %+v\n", entries)
	//fmt.Printf("[ClientQuery][ProcessQ8]Keys: %v. Values: %+v\n", keys, values)
	var country95, country96, region95, region96 float64
	//Need to do processing for fairness
	for i, protoEntry := range values {
		key := string(keys[i])
		value := protoEntry.GetValue().GetCounterfloat().GetValue()
		//fmt.Printf("[ClientQuery][ProcessQ8]Key %s, Value %f\n", key, value)
		if key[len(key)-1] == '5' {
			if key[0] == 'R' {
				region95 = value
			} else {
				country95 = value
			}
		} else if key[0] == 'R' {
			region96 = value
		} else {
			country96 = value
		}
	}
	if PRINT_QUERY {
		fmt.Println("[ClientQuery]Q8 results (fraction of revenue of a nation in the region's supply of a certain part type)")
		fmt.Printf("1995: %.2f; 1996: %.2f\n", country95/region95, country96/region96)
	}
	*client.nEntries += len(values)
}

func getQ9Reads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	color := tpchData.Tables.Colors[client.rng.Int63n(int64(len(tpchData.Tables.Colors)))]
	partialR[bufI[1]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q9_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}, ReadArgs: crdt.EmbMapGetValueArguments{Key: color, Args: crdt.StateReadArguments{}}}
	bufI[1]++
	//fullR[bufI[0]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q9_KEY + color, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}}
	//bufI[0]++
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][EmbMap]Q9 query: requesting national supplier profits of products with `%s` in their name\n", color)
	}
	return reads + 1
}

func getQ9LocalDirectReads(client QueryClient, fullR, partialR [][]crdt.ReadObjectParams, bufI [][]int, rngRegions []int, rngRegionsI, reads int) (newReads, rngRegion int) {
	color, nRegions := tpchData.Tables.Colors[client.rng.Int63n(int64(len(tpchData.Tables.Colors)))], len(client.serverConns)
	//key := Q9_KEY + color
	for i := 0; i < nRegions; i++ {
		//fullR[i][bufI[i][0]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: key, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+i]}}
		partialR[i][bufI[i][1]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q9_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+i]}, ReadArgs: crdt.EmbMapGetValueArguments{Key: color, Args: crdt.StateReadArguments{}}}
		bufI[i][1]++
		//bufI[i][0]++
	}
	return reads + nRegions, 0
}

func getQ9LocalServerReads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	color, nRegions, offset := tpchData.Tables.Colors[client.rng.Int63n(int64(len(tpchData.Tables.Colors)))], len(client.serverConns), bufI[0]
	//key := Q9_KEY + color
	for i := 0; i < nRegions; i++ {
		partialR[offset+i] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q9_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+i]}, ReadArgs: crdt.EmbMapGetValueArguments{Key: color, Args: crdt.StateReadArguments{}}}
		//fullR[offset+i] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: key, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+i]}}
	}
	//bufI[0] += nRegions
	bufI[1] += nRegions
	return reads + nRegions
}

func processQ9Reply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	processQ9ReplyProto(replies[bufI[1]], tpchData.Tables.SortedNationsName, client)
	bufI[1]++
	return reads + 1
}

func processQ9LocalDirectReply(client QueryClient, replies [][]*proto.ApbReadObjectResp, bufI [][]int, rngRegions []int, rngRegionsI int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	bufI1 := localDirectBufIToServerBufI(bufI, 1)
	relevantReplies := readRepliesToOneSlice(replies, bufI1, 1)
	processQ9ReplyProto(mergeQ9IndexReply(relevantReplies)[0], tpchData.Tables.SortedNationsName, client)
	incrementAllBufILocalDirect(bufI, 1, 1)
	return reads + nRegions
}

// Queries that are naturally local (i.e., no merging needed) do not need processLocalServerReply
func processQ9LocalServerReply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	start, end := bufI[1], bufI[1]+nRegions
	processQ9ReplyProto(mergeQ9IndexReply(replies[start:end])[0], tpchData.Tables.SortedNationsName, client)
	bufI[1] += nRegions
	return reads + nRegions
}

func processQ9ReplyProto(readProto *proto.ApbReadObjectResp, sortedNatsNames []string, client QueryClient) {
	//states := crdt.ReadRespProtoToAntidoteState(readProto, proto.CRDTType_RRMAP, proto.READType_FULL).(crdt.EmbMapEntryState)
	states := crdt.ReadRespProtoToAntidoteState(readProto.GetPartread().GetMap().GetGetvalue().GetValue(), proto.CRDTType_RRMAP, proto.READType_FULL).(crdt.EmbMapEntryState)
	/*nEntries := 0
	for _, natN := range sortedNatsNames {
		yearsStates := states.States[natN].(crdt.EmbMapEntryState)
		for _, year := range Q9_YEARS {
			if _, has := yearsStates.States[year]; has {
				nEntries++
			}
		}
	}*/
	//fmt.Printf("[Q9ReplyProto]Number of entries: %d (%d outside entries)", nEntries, len(states.States))
	//fmt.Printf("[Q9ReplyProto]States: %+v\n", states)
	//fmt.Println("[Q9ReplyProto]Sorted nats names:", sortedNatsNames)
	if PRINT_QUERY {
		var yearsStates crdt.EmbMapEntryState
		var yearState crdt.State
		var has bool
		fmt.Println("[ClientQuery]Q9 results (national supplier profits of products with a given name pattern)")
		for _, natN := range sortedNatsNames {
			yearsStates = states.States[natN].(crdt.EmbMapEntryState)
			//fmt.Println()
			//fmt.Printf("[Q9ReplyProto]Year states: %+v\n", yearsStates)
			for _, year := range Q9_YEARS {
				if yearState, has = yearsStates.States[year]; has {
					fmt.Printf("Nation: %s, Year: %s, Profit: %.2f\n", natN, year, yearState.(crdt.CounterFloatState).Value)
				}
			}
		}
	}
	*client.nEntries += len(Q9_YEARS) * len(sortedNatsNames)
}

func sendQ9(client QueryClient) (nRequests int) {
	rndColor := tpchData.Tables.Colors[client.rng.Intn(len(tpchData.Tables.Colors))]
	//key := Q9_KEY + rndColor
	//fmt.Println("[SendQ9]Key:", key)
	//readParam := []crdt.ReadObjectParams{{KeyParams: crdt.KeyParams{Key: key, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}}}
	//mapProto := mergeIndex(sendReceiveIndexQuery(client, readParam, nil), 9)[0]
	readParam := []crdt.ReadObjectParams{{KeyParams: crdt.KeyParams{Key: Q9_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}, ReadArgs: crdt.EmbMapGetValueArguments{Key: rndColor, Args: crdt.StateReadArguments{}}}}
	mapProto := mergeIndex(sendReceiveIndexQuery(client, nil, readParam), 9)[0]

	if PRINT_QUERY {
		fmt.Printf("[ClientQuery]Q9 results: requesting national supplier profits of products with `%s` in their name\n", rndColor)
	}
	processQ9ReplyProto(mapProto, tpchData.Tables.SortedNationsName, client)

	if isIndexGlobal {
		return 1
	}
	return len(tpchData.Tables.Regions)
}

func getQ10Reads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndQuarter := client.rng.Int63n(8)
	keyParams := crdt.KeyParams{Key: Q10_KEY + strconv.FormatInt(rndQuarter, 10), CrdtType: proto.CRDTType_TOPSUM, Bucket: buckets[INDEX_BKT]}
	partialR[bufI[1]] = crdt.ReadObjectParams{KeyParams: keyParams, ReadArgs: crdt.GetTopNArguments{NumberEntries: 20}}
	bufI[1]++
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][TopSum]Q10 query: top customers from quarter %d that generated the most revenue loss due to returned parts.\n", rndQuarter)
	}
	return reads + 1
}

func getQ10LocalDirectReads(client QueryClient, fullR, partialR [][]crdt.ReadObjectParams, bufI [][]int, rngRegions []int, rngRegionsI, reads int) (newReads, rngRegion int) {
	rndQuarter, nRegions := client.rng.Int63n(8), len(client.serverConns)
	keyParams := crdt.KeyParams{Key: Q10_KEY + strconv.FormatInt(rndQuarter, 10), CrdtType: proto.CRDTType_TOPSUM, Bucket: buckets[INDEX_BKT]}
	for i := 0; i < nRegions; i++ {
		keyParams.Bucket = buckets[INDEX_BKT+i]
		partialR[i][bufI[i][1]] = crdt.ReadObjectParams{KeyParams: keyParams, ReadArgs: crdt.GetTopNArguments{NumberEntries: 20}}
		bufI[i][1]++
	}
	return reads + nRegions, 0
}

func getQ10LocalServerReads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndQuarter, nRegions, offset := client.rng.Int63n(8), len(client.serverConns), bufI[1]
	keyParams := crdt.KeyParams{Key: Q10_KEY + strconv.FormatInt(rndQuarter, 10), CrdtType: proto.CRDTType_TOPSUM, Bucket: buckets[INDEX_BKT]}
	for i := 0; i < nRegions; i++ {
		keyParams.Bucket = buckets[INDEX_BKT+i]
		partialR[offset+i] = crdt.ReadObjectParams{KeyParams: keyParams, ReadArgs: crdt.GetTopNArguments{NumberEntries: 20}}
	}
	bufI[1] += nRegions
	return reads + nRegions
}

func processQ10Reply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	processQ10ReplyProto(replies[bufI[1]], client)
	bufI[1]++
	return reads + 1
}

func processQ10LocalDirectReply(client QueryClient, replies [][]*proto.ApbReadObjectResp, bufI [][]int, rngRegions []int, rngRegionsI int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	bufI1 := localDirectBufIToServerBufI(bufI, 1)
	relevantReplies := readRepliesToOneSlice(replies, bufI1, 1)
	processQ10ReplyProto(mergeQ10IndexReply(relevantReplies)[0], client)
	incrementAllBufILocalDirect(bufI, 1, 1)
	return reads + nRegions
}

// Queries that are naturally local (i.e., no merging needed) do not need processLocalServerReply
func processQ10LocalServerReply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	start, end := bufI[1], bufI[1]+nRegions
	processQ10ReplyProto(mergeQ10IndexReply(replies[start:end])[0], client)
	bufI[1] += nRegions
	return reads + nRegions
}

func processQ10ReplyProto(protobuf *proto.ApbReadObjectResp, client QueryClient) {
	entries := protobuf.GetTopk().GetValues()
	if PRINT_QUERY {
		fmt.Println("[ClientQuery][TopSum20]Q10 results: top customers who generated the highest revenue loss due to returned items for a given quarter.")
		fmt.Println("CustId: Sum loss revenue | C_NAME | C_ACCTBAL | C_PHONE | N_NAME | C_ADDRESS | C_COMMENT")
		var extraData []string
		for _, pair := range entries {
			extraData = unpackIndexExtraData(pair.GetData(), Q10_N_EXTRA_DATA)
			fmt.Printf("%d: %d ", pair.GetPlayerId(), pair.GetScore())
			for _, data := range extraData {
				fmt.Printf("| %s", data)
			}
			fmt.Println()
		}
	} else {
		//Still process the extra data
		for _, pair := range entries {
			unpackIndexExtraData(pair.GetData(), Q10_N_EXTRA_DATA)
		}
	}
	/*
		var build strings.Builder
		build.WriteString(strconv.FormatInt(int64(cust.C_CUSTKEY), 10))
		build.WriteRune('-')
		build.WriteString(cust.C_NAME)
		build.WriteRune('-')
		build.WriteString(cust.C_ACCTBAL)
		build.WriteRune('-')
		build.WriteString(cust.C_PHONE)
		build.WriteRune('-')
		build.WriteString(nationName)
		build.WriteRune('-')
		build.WriteString(cust.C_ADDRESS)
		build.WriteRune('-')
		build.WriteString(cust.C_COMMENT)
		buf := []byte(build.String())
		return &buf
	*/
	*client.nEntries += len(entries)
}

func sendQ10(client QueryClient) (nRequests int) {
	rndQuarter := client.rng.Int63n(8)
	keyParams := crdt.KeyParams{Key: Q10_KEY + strconv.FormatInt(rndQuarter, 10), CrdtType: proto.CRDTType_TOPSUM, Bucket: buckets[INDEX_BKT]}
	readParam := []crdt.ReadObjectParams{{KeyParams: keyParams, ReadArgs: crdt.GetTopNArguments{NumberEntries: 20}}}
	readReply := mergeIndex(sendReceiveIndexQuery(client, nil, readParam), 10)[0]

	if PRINT_QUERY {
		fmt.Printf("Q10: top customers from quarter %d that generated the most revenue loss due to returned parts.\n", rndQuarter)
	}
	processQ10ReplyProto(readReply, client)

	if isIndexGlobal {
		return 1
	}
	return len(tpchData.Tables.Regions)
}

func getQ12Reads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	modes, nModes := tpchData.Tables.Modes, int64(len(tpchData.Tables.Modes))
	rndYear, rndShipM1, rndShipM2 := client.rng.Int63n(5)+1993, modes[client.rng.Int63n(nModes)], modes[client.rng.Int63n(nModes)]
	for rndShipM1 == rndShipM2 {
		rndShipM2 = modes[client.rng.Int63n(nModes)]
	}
	keyParams, fullArgs := crdt.KeyParams{Key: Q12_KEY + strconv.FormatInt(rndYear, 10), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}, crdt.StateReadArguments{}
	mapKeys := map[string]crdt.ReadArguments{rndShipM1 + Q12_PRIORITY[0]: fullArgs, rndShipM1 + Q12_PRIORITY[1]: fullArgs, rndShipM2 + Q12_PRIORITY[0]: fullArgs, rndShipM2 + Q12_PRIORITY[1]: fullArgs} //2x modes, 2x priority
	partialR[bufI[1]] = crdt.ReadObjectParams{KeyParams: keyParams, ReadArgs: crdt.EmbMapPartialArguments{Args: mapKeys}}
	bufI[1]++
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][RRMap]Q12 query: number of items delivered late (after commitdate) for year %d, by priority type and for shipmodes %s and %s.\n", rndYear, rndShipM1, rndShipM2)
	}
	return reads + 4
}

func getQ12LocalDirectReads(client QueryClient, fullR, partialR [][]crdt.ReadObjectParams, bufI [][]int, rngRegions []int, rngRegionsI, reads int) (newReads, rngRegion int) {
	modes, nModes, nRegions := tpchData.Tables.Modes, int64(len(tpchData.Tables.Modes)), len(tpchData.Tables.Regions)
	rndYear, rndShipM1, rndShipM2 := client.rng.Int63n(5)+1993, modes[client.rng.Int63n(nModes)], modes[client.rng.Int63n(nModes)]
	for rndShipM1 == rndShipM2 {
		rndShipM2 = modes[client.rng.Int63n(nModes)]
	}
	key, fullArgs := Q12_KEY+strconv.FormatInt(rndYear, 10), crdt.StateReadArguments{}
	mapKeys := map[string]crdt.ReadArguments{rndShipM1 + Q12_PRIORITY[0]: fullArgs, rndShipM1 + Q12_PRIORITY[1]: fullArgs, rndShipM2 + Q12_PRIORITY[0]: fullArgs, rndShipM2 + Q12_PRIORITY[1]: fullArgs} //2x modes, 2x priority
	for i := 0; i < nRegions; i++ {
		partialR[i][bufI[i][1]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: key, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+i]}, ReadArgs: crdt.EmbMapPartialArguments{Args: mapKeys}}
		bufI[i][1]++
	}
	return reads + nRegions*4, 0
}

func getQ12LocalServerReads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	modes, nModes, nRegions, offset := tpchData.Tables.Modes, int64(len(tpchData.Tables.Modes)), len(tpchData.Tables.Regions), bufI[1]
	rndYear, rndShipM1, rndShipM2 := client.rng.Int63n(5)+1993, modes[client.rng.Int63n(nModes)], modes[client.rng.Int63n(nModes)]
	for rndShipM1 == rndShipM2 {
		rndShipM2 = modes[client.rng.Int63n(nModes)]
	}
	key, fullArgs := Q12_KEY+strconv.FormatInt(rndYear, 10), crdt.StateReadArguments{}
	mapKeys := map[string]crdt.ReadArguments{rndShipM1 + Q12_PRIORITY[0]: fullArgs, rndShipM1 + Q12_PRIORITY[1]: fullArgs, rndShipM2 + Q12_PRIORITY[0]: fullArgs, rndShipM2 + Q12_PRIORITY[1]: fullArgs} //2x modes, 2x priority
	for i := 0; i < nRegions; i++ {
		partialR[offset+i] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: key, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+i]}, ReadArgs: crdt.EmbMapPartialArguments{Args: mapKeys}}
	}
	bufI[1] += nRegions
	return reads + nRegions*4
}

func processQ12Reply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	processQ12ReplyProto(replies[bufI[1]], client)
	bufI[1]++
	return reads + 4
}

func processQ12LocalDirectReply(client QueryClient, replies [][]*proto.ApbReadObjectResp, bufI [][]int, rngRegions []int, rngRegionsI int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	bufI1 := localDirectBufIToServerBufI(bufI, 1)
	relevantReplies := readRepliesToOneSlice(replies, bufI1, 1)
	processQ12ReplyProto(mergeQ12IndexReply(relevantReplies)[0], client)
	incrementAllBufILocalDirect(bufI, 1, 1)
	return reads + 4*nRegions
}

// Queries that are naturally local (i.e., no merging needed) do not need processLocalServerReply
func processQ12LocalServerReply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	start, end := bufI[1], bufI[1]+nRegions
	processQ12ReplyProto(mergeQ12IndexReply(replies[start:end])[0], client)
	bufI[1] += nRegions
	return reads + 4*nRegions
}

func processQ12ReplyProto(proto *proto.ApbReadObjectResp, client QueryClient) {
	entries := proto.GetPartread().GetMap().GetGetvalues()
	keys, values := entries.GetKeys(), entries.GetValues()
	if PRINT_QUERY {
		fmt.Println("[ClientQuery]Q12 results (number of items delivered after commitdate for a given year and given shipping modes): ")
		for i, entry := range values {
			key := string(keys[i])
			priorityType := key[len(key)-1:] //1-5, Priorities goes 0-4.
			shipMode := key[:len(key)-1]
			var priorityString string
			if priorityType == "p" {
				priorityString = "high_line_count"
			} else {
				priorityString = "low_line_count"
			}
			fmt.Printf("%s, %s: %v\n", shipMode, priorityString, entry.GetValue().GetCounter().GetValue())
		}
	}
	*client.nEntries += len(values)
}

func sendQ12(client QueryClient) (nRequests int) {
	modes, nModes := tpchData.Tables.Modes, int64(len(tpchData.Tables.Modes))
	rndYear, rndShipM1, rndShipM2 := client.rng.Int63n(5)+1993, modes[client.rng.Int63n(nModes)], modes[client.rng.Int63n(nModes)]
	for rndShipM1 == rndShipM2 {
		rndShipM2 = modes[client.rng.Int63n(nModes)]
	}
	keyParams, fullArgs := crdt.KeyParams{Key: Q12_KEY + strconv.FormatInt(rndYear, 10), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}, crdt.StateReadArguments{}
	mapKeys := map[string]crdt.ReadArguments{rndShipM1 + Q12_PRIORITY[0]: fullArgs, rndShipM1 + Q12_PRIORITY[1]: fullArgs, rndShipM2 + Q12_PRIORITY[0]: fullArgs, rndShipM2 + Q12_PRIORITY[1]: fullArgs} //2x modes, 2x priority
	readParam := []crdt.ReadObjectParams{{KeyParams: keyParams, ReadArgs: crdt.EmbMapPartialArguments{Args: mapKeys}}}
	mapProto := mergeIndex(sendReceiveIndexQuery(client, nil, readParam), 12)[0]

	if PRINT_QUERY {
		fmt.Printf("Q12: number of items delivered late (after commitdate) for year %d, by priority type and for shipmodes %s and %s.\n", rndYear, rndShipM1, rndShipM2)
	}
	processQ12ReplyProto(mapProto, client)

	if isIndexGlobal {
		return 4
	}
	return 4 * len(tpchData.Tables.Regions)
}

func getQ13Reads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	word1, word2 := Q13_Word1[client.rng.Intn(len(Q13_Word1))], Q13_Word2[client.rng.Intn(len(Q13_Word2))]
	readArgs := crdt.EmbMapGetValueArguments{Key: word1 + word2, Args: crdt.StateReadArguments{}}
	partialR[bufI[1]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q13_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}, ReadArgs: readArgs}
	bufI[1]++
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][EmbMap]Q13 query: distribution of customers by number of orders made, excluding orders with comments containing %s followed by %s.\n", word1, word2)
	}
	return reads + 1
}
func getQ13LocalDirectReads(client QueryClient, fullR, partialR [][]crdt.ReadObjectParams, bufI [][]int, rngRegions []int, rngRegionsI, reads int) (newReads, rngRegion int) {
	word1, word2, nRegions := Q13_Word1[client.rng.Intn(len(Q13_Word1))], Q13_Word2[client.rng.Intn(len(Q13_Word2))], len(client.serverConns)
	readArgs := crdt.EmbMapGetValueArguments{Key: word1 + word2, Args: crdt.StateReadArguments{}}
	for i := 0; i < nRegions; i++ {
		partialR[i][bufI[i][1]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q13_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+i]}, ReadArgs: readArgs}
		bufI[i][1]++
	}
	return reads + nRegions, 0
}

func getQ13LocalServerReads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	word1, word2, nRegions, offset := Q13_Word1[client.rng.Intn(len(Q13_Word1))], Q13_Word2[client.rng.Intn(len(Q13_Word2))], len(client.serverConns), bufI[1]
	readArgs := crdt.EmbMapGetValueArguments{Key: word1 + word2, Args: crdt.StateReadArguments{}}
	for i := 0; i < nRegions; i++ {
		partialR[offset+i] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q13_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+i]}, ReadArgs: readArgs}
	}
	bufI[1] += nRegions
	return reads + nRegions
}

func processQ13Reply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	processQ13ReplyProto(replies[bufI[1]], client)
	bufI[1]++
	return reads + 1
}

func processQ13LocalDirectReply(client QueryClient, replies [][]*proto.ApbReadObjectResp, bufI [][]int, rngRegions []int, rngRegionsI int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	bufI1 := localDirectBufIToServerBufI(bufI, 1)
	relevantReplies := readRepliesToOneSlice(replies, bufI1, 1)
	merged := mergeQ13IndexReply(relevantReplies)
	processQ13ReplyProto(merged[0], client)
	incrementAllBufILocalDirect(bufI, 0, 1)
	return reads + 2*nRegions
}

// Queries that are naturally local (i.e., no merging needed) do not need processLocalServerReply
func processQ13LocalServerReply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	start, end := bufI[1], bufI[1]+nRegions
	merged := mergeQ13IndexReply(replies[start:end])
	processQ13ReplyProto(merged[0], client)
	bufI[1] += nRegions
	return reads + nRegions
}

func processQ13ReplyProto(reply *proto.ApbReadObjectResp, client QueryClient) {
	entries := reply.GetPartread().GetMap().GetGetvalue().GetValue().GetMap().GetEntries()
	//fmt.Println("[Q13ReplyProto]NEntries returned:", len(entries))
	if PRINT_QUERY {
		fmt.Println("[ClientQuery]Q13 results (distribution of customers per number of orders except for orders with 2 certain words on it.")
		//nOrders, nCusts := make([]int, len(allEntries)), make([]int, len(allEntries))
		values := make([]int32, len(entries)+len(entries)/3)
		var nOrders int
		highest := 0
		for _, entry := range entries {
			nOrders, _ = strconv.Atoi(string(entry.GetKey().GetKey()))
			values[nOrders] = entry.GetValue().GetCounter().GetValue()
			if nOrders > highest {
				highest = nOrders
			}
		}
		for i, nCusts := range values[:highest] {
			fmt.Printf("%d orders: %d customers\n", i, nCusts)
		}
	}
	*client.nEntries += len(entries)
}

func sendQ13(client QueryClient) (nRequests int) {
	word1, word2 := Q13_Word1[client.rng.Intn(len(Q13_Word1))], Q13_Word2[client.rng.Intn(len(Q13_Word2))]
	//word1, word2 = Q13_Word1[0], Q13_Word2[0]
	keyP := crdt.KeyParams{Key: Q13_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}
	readP := []crdt.ReadObjectParams{{KeyParams: keyP, ReadArgs: crdt.EmbMapGetValueArguments{Key: word1 + word2, Args: crdt.StateReadArguments{}}}}
	//fmt.Println("[ClientQueries][SendQ13]Words:", word1, word2)
	protobf := mergeIndex(sendReceiveIndexQuery(client, nil, readP), 13)[0]

	if PRINT_QUERY {
		fmt.Printf("Q13: distribution of customers by number of orders made, excluding orders with comments containing '%s' followed by '%s'.\n", word1, word2)
	}
	processQ13ReplyProto(protobf, client)

	if isIndexGlobal {
		return 1
	}
	return len(tpchData.Tables.Regions)
}

/*
func getQ13Reads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	word1, word2 := Q13_Word1[client.rng.Intn(len(Q13_Word1))], Q13_Word2[client.rng.Intn(len(Q13_Word2))]
	fullR[bufI[0]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q13_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}}
	fullR[bufI[0]+1] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q13_KEY + word1 + word2, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}}
	bufI[0] += 2
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][EmbMap]Q13 query: distribution of customers by number of orders made, excluding orders with comments containing %s followed by %s.\n", word1, word2)
	}
	return reads + 2
}

func getQ13LocalDirectReads(client QueryClient, fullR, partialR [][]crdt.ReadObjectParams, bufI [][]int, rngRegions []int, rngRegionsI, reads int) (newReads, rngRegion int) {
	word1, word2, nRegions := Q13_Word1[client.rng.Intn(len(Q13_Word1))], Q13_Word2[client.rng.Intn(len(Q13_Word2))], len(client.serverConns)
	keyParamsAll, keyParamsWord := crdt.KeyParams{Key: Q13_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}, crdt.KeyParams{Key: Q13_KEY + word1 + word2, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}
	for i := 0; i < nRegions; i++ {
		keyParamsAll.Bucket, keyParamsWord.Bucket = buckets[INDEX_BKT+i], buckets[INDEX_BKT+i]
		fullR[i][bufI[i][0]], fullR[i][bufI[i][0]+1] = crdt.ReadObjectParams{KeyParams: keyParamsAll}, crdt.ReadObjectParams{KeyParams: keyParamsWord}
		bufI[i][0] += 2
	}
	return reads + nRegions*2, 0
}

func getQ13LocalServerReads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	word1, word2, nRegions, offset := Q13_Word1[client.rng.Intn(len(Q13_Word1))], Q13_Word2[client.rng.Intn(len(Q13_Word2))], len(client.serverConns), bufI[0]
	keyParamsAll, keyParamsWord := crdt.KeyParams{Key: Q13_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}, crdt.KeyParams{Key: Q13_KEY + word1 + word2, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}
	for i := 0; i < nRegions; i++ {
		keyParamsAll.Bucket, keyParamsWord.Bucket = buckets[INDEX_BKT+i], buckets[INDEX_BKT+i]
		fullR[offset+i*2], fullR[offset+i*2+1] = crdt.ReadObjectParams{KeyParams: keyParamsAll}, crdt.ReadObjectParams{KeyParams: keyParamsWord}
	}
	bufI[0] += nRegions * 2
	return reads + nRegions*2
}

func processQ13Reply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	processQ13ReplyProto(replies[bufI[0]], replies[bufI[0]+1])
	bufI[0] += 2
	return reads + 2
}

func processQ13LocalDirectReply(client QueryClient, replies [][]*proto.ApbReadObjectResp, bufI [][]int, rngRegions []int, rngRegionsI int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	bufI0 := localDirectBufIToServerBufI(bufI, 0)
	relevantReplies := readRepliesToOneSlice(replies, bufI0, 2)
	merged := mergeQ13IndexReply(relevantReplies)
	processQ13ReplyProto(merged[0], merged[1])
	incrementAllBufILocalDirect(bufI, 0, 2)
	return reads + 2*nRegions
}

// Queries that are naturally local (i.e., no merging needed) do not need processLocalServerReply
func processQ13LocalServerReply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	start, end := bufI[0], bufI[0]+nRegions*2
	merged := mergeQ13IndexReply(replies[start:end])
	processQ13ReplyProto(merged[0], merged[1])
	bufI[0] += nRegions * 2
	return reads + 2*nRegions
}

func processQ13ReplyProto(allReply, wordReply *proto.ApbReadObjectResp) {
	allEntries, allWords := allReply.GetMap().GetEntries(), wordReply.GetMap().GetEntries()
	if PRINT_QUERY {
		fmt.Println("[ClientQuery]Q13 results (distribution of customers per number of orders except for orders with 2 certain words on it.")
		//nOrders, nCusts := make([]int, len(allEntries)), make([]int, len(allEntries))
		values := make([]int32, len(allEntries)+(len(allEntries))/3)
		var nOrders int
		highest := 0
		for _, entryAll := range allEntries {
			nOrders, _ = strconv.Atoi(string(entryAll.GetKey().GetKey()))
			values[nOrders] = entryAll.GetValue().GetCounter().GetValue()
			if nOrders > highest {
				highest = nOrders
			}
		}
		for _, entryWords := range allWords {
			nOrders, _ = strconv.Atoi(string(entryWords.GetKey().GetKey()))
			values[nOrders] -= entryWords.GetValue().GetCounter().GetValue()
		}
		for i, nCusts := range values[:highest] {
			fmt.Printf("%d orders: %d customers\n", i, nCusts)
		}
	}
}

func sendQ13(client QueryClient) (nRequests int) {
	word1, word2 := Q13_Word1[client.rng.Intn(len(Q13_Word1))], Q13_Word2[client.rng.Intn(len(Q13_Word2))]
	//word1, word2 = Q13_Word1[0], Q13_Word2[0]
	keyPAll, keyPWord := crdt.KeyParams{Key: Q13_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}, crdt.KeyParams{Key: Q13_KEY + word1 + word2, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}
	readP := []crdt.ReadObjectParams{{KeyParams: keyPAll}, {KeyParams: keyPWord}}
	//fmt.Println("[ClientQueries][SendQ13]Words:", word1, word2)
	protos := mergeIndex(sendReceiveIndexQuery(client, readP, nil), 13)[:2]

	if PRINT_QUERY {
		fmt.Printf("Q13: distribution of customers by number of orders made, excluding orders with comments containing '%s' followed by '%s'.\n", word1, word2)
	}
	processQ13ReplyProto(protos[0], protos[1])

	if isIndexGlobal {
		return 2
	}
	return 2 * len(tpchData.Tables.Regions)
}
*/

func getQ16Reads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	readArgs := getQ16ReadsHelper(client)
	partialR[bufI[1]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q16_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}, ReadArgs: readArgs}
	bufI[1] += 1
	return reads + 8
}

func getQ16LocalDirectReads(client QueryClient, fullR, partialR [][]crdt.ReadObjectParams, bufI [][]int, rngRegions []int, rngRegionsI, reads int) (newReads, rngRegion int) {
	readArgs, nRegions := getQ16ReadsHelper(client), len(tpchData.Tables.Regions)
	for i := 0; i < nRegions; i++ {
		partialR[i][bufI[i][1]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q16_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+i]}, ReadArgs: readArgs}
		bufI[i][1]++
	}
	return reads + 8*nRegions, 0
}

func getQ16LocalServerReads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	readArgs, nRegions, offset := getQ16ReadsHelper(client), len(tpchData.Tables.Regions), bufI[1]
	for i := 0; i < nRegions; i++ {
		partialR[offset] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q16_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+i]}, ReadArgs: readArgs}
		offset++
	}
	bufI[1] = offset
	return reads + 8*nRegions
}

func getQ16ReadsHelper(client QueryClient) (readArgs crdt.EmbMapPartialArguments) {
	rndBrand, rndType, rndSizes := tpchData.Tables.Brands[client.rng.Intn(len(tpchData.Tables.Brands))], tpchData.Tables.MediumType[client.rng.Intn(len(tpchData.Tables.MediumType))], make(map[int64]string)
	var rndSize int64
	for len(rndSizes) < 8 {
		rndSize = client.rng.Int63n(50) + 1
		rndSizes[rndSize] = strconv.FormatInt(rndSize, 10)
	}
	//rndBrand, rndType, rndSizes := tpchData.Tables.Brands[0], tpchData.Tables.MediumType[0], []string{"1", "2", "3", "4", "5", "6", "7", "8"}
	readArgs = crdt.EmbMapPartialArguments{Args: make(map[string]crdt.ReadArguments)}
	//innerRead := crdt.EmbMapExceptArguments{ExceptKeys: map[string]struct{}{rndBrand: {}}, ArgForAll: crdt.EmbMapExceptArguments{ExceptKeys: map[string]struct{}{rndType: {}}}} //Inner-most is fullread
	innerRead := crdt.EmbMapExceptArguments{ExceptKeys: map[string]struct{}{rndBrand: {}}, ArgForAll: crdt.EmbMapConditionalReadExceptArguments{ExceptKeys: map[string]struct{}{rndType: {}}, CondArgForAll: &crdt.IntCompareArguments{Value: 0, CompType: crdt.H}}}
	for _, sizeS := range rndSizes {
		readArgs.Args[sizeS] = innerRead
	}
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][RRMap]Q16 query: number of (distinct) suppliers who can provide parts that satisfy a customer's requirements:"+
			"not of brand %s, not of type %s, and sizes in %v\n", rndBrand, rndType, rndSizes)
	}
	return
}

func processQ16Reply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	processQ16ReplyProto(replies[bufI[1]], client)
	bufI[1]++
	return reads + 8
}

func processQ16LocalDirectReply(client QueryClient, replies [][]*proto.ApbReadObjectResp, bufI [][]int, rngRegions []int, rngRegionsI int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	bufI1 := localDirectBufIToServerBufI(bufI, 1)
	relevantReplies := readRepliesToOneSlice(replies, bufI1, 1)
	processQ16ReplyProto(mergeQ16IndexReply(relevantReplies)[0], client)
	incrementAllBufILocalDirect(bufI, 1, 1)
	return reads + 8*nRegions
}

// Queries that are naturally local (i.e., no merging needed) do not need processLocalServerReply
func processQ16LocalServerReply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	start, end := bufI[1], bufI[1]+nRegions
	processQ16ReplyProto(mergeQ16IndexReply(replies[start:end])[0], client)
	bufI[1] += nRegions
	return reads + 8*nRegions
}

type Q16PrintInfo struct {
	count              int32
	brand, typeS, size string
}

// Needs to be sorted by count (desc), brand, type, size
func processQ16ReplyProto(reply *proto.ApbReadObjectResp, client QueryClient) {
	//Size -> brand -> type
	entries, pos := make([]Q16PrintInfo, 29*24*8), 0 //(nBrands-1)*(nTypes-1)*size
	//PRINT_QUERY {
	var sizeProto, brandProto *proto.ApbMapGetValuesResp
	var typeProto []*proto.ApbMapEntry
	var sizeKeys, brandKeys /*, typeKeys*/ [][]byte
	var sizeValues, brandValues /*, typeValues*/ []*proto.ApbMapGetValueResp
	//var sizeKey, brandKey, sizeBrandTypeKey string
	var sizeKey, brandKey string

	sizeProto = reply.GetPartread().GetMap().GetGetvalues()
	sizeKeys, sizeValues = sizeProto.GetKeys(), sizeProto.GetValues()
	//fmt.Printf("[Q16ReplyProto]SizeProto: %+v\n", sizeProto)
	for i, sizeV := range sizeValues {
		brandProto = sizeV.GetValue().GetPartread().GetMap().GetGetvalues()
		brandKeys, brandValues = brandProto.GetKeys(), brandProto.GetValues()
		sizeKey = string(sizeKeys[i])
		//fmt.Printf("[Q16ReplyProto]BrandProto: %+v\n", brandProto)
		for j, brandV := range brandValues {
			/*
				typeProto = brandV.GetValue().GetPartread().GetMap().GetGetvalues()
				typeKeys, typeValues = typeProto.GetKeys(), typeProto.GetValues()
				//brandKey = fmt.Sprintf("%s \t %s", string(brandKeys[j]), sizeKey)
				brandKey = string(brandKeys[j])
				fmt.Printf("[Q16ReplyProto]TypeProto: %+v\n", typeProto)
				for k, typeV := range typeValues {
					fmt.Printf("[Q16ReplyProto]Value: %d\n", *typeV.GetValue().GetCounter().Value)
					//fmt.Printf("Type %+v\n", *typeV)
					entries[pos] = Q16PrintInfo{count: *typeV.GetValue().GetCounter().Value, brand: brandKey, typeS: string(typeKeys[k]), size: sizeKey}
					pos++
					//sizeBrandTypeKey = fmt.Sprintf("%s \t %s\n")
					//fmt.Println(*typeV.GetValue().GetCounter().Value)
				}
			*/
			typeProto = brandV.GetValue().GetMap().GetEntries()
			brandKey = string(brandKeys[j])
			for _, entry := range typeProto {
				entries[pos] = Q16PrintInfo{count: *entry.GetValue().GetCounter().Value, brand: brandKey, typeS: string(entry.GetKey().GetKey()), size: sizeKey}
				pos++
			}
		}
	}
	//}
	sortedEntries := sortQ16Result(entries[:pos])
	if PRINT_QUERY {
		for _, entry := range sortedEntries {
			fmt.Printf("%s \t %s \t %s \t %d\n", entry.brand, entry.typeS, entry.size, entry.count)
		}
		fmt.Println("[Q16]Number of entries returned:", pos)
	}
	*client.nEntries += pos
}

func sortQ16Result(entries []Q16PrintInfo) (sortedEntries []Q16PrintInfo) {
	sort.Slice(entries, func(i, j int) bool {
		entryI, entryJ := entries[i], entries[j]
		if entryI.count > entryJ.count { //We want higher counts first in the slice
			return true
		}
		if entryI.count < entryJ.count {
			return false
		}
		if entryI.brand < entryJ.brand {
			return true
		}
		if entryI.brand > entryJ.brand {
			return false
		}
		if entryI.typeS < entryJ.typeS {
			return true
		}
		if entryI.typeS > entryJ.typeS {
			return false
		}
		//Comparing numbers in strings
		if len(entryI.size) < len(entryJ.size) {
			return true
		}
		if len(entryI.size) > len(entryJ.size) {
			return false
		}
		if entryI.size[0] < entryJ.size[0] {
			return true
		}
		if entryI.size[0] > entryJ.size[0] {
			return false
		}
		if len(entryI.size) == 2 {
			if entryI.size[1] < entryJ.size[1] {
				return true
			} else {
				return false
			}
		}
		return false
	})
	return entries
}

func sendQ16(client QueryClient) (nRequests int) {
	readArgs := getQ16ReadsHelper(client)
	readP := []crdt.ReadObjectParams{{KeyParams: crdt.KeyParams{Key: Q16_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}, ReadArgs: readArgs}}
	protos := mergeIndex(sendReceiveIndexQuery(client, nil, readP), 16)[0]
	processQ16ReplyProto(protos, client)

	if isIndexGlobal {
		return 8
	}
	return 8 * len(tpchData.Tables.Regions)
}

/*
func getQ16Reads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndBrand, rndType, rndSizes, readArgs := getQ16ReadsHelper(client)
	offset := bufI[1]
	partialR[offset] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q16_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}, ReadArgs: readArgs}
	partialR[offset+1] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q16_KEY + rndBrand + rndType, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}, ReadArgs: readArgs}
	bufI[1] += 2
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][RRMap]Q16 query: number of (distinct) suppliers who can provide parts that satisfy a customer's requirements:"+
			"not of brand %s, not of type %s, and sizes in %v\n", rndBrand, rndType, rndSizes)
	}
	return reads + 16 //2 reads per size
}

func getQ16ReadsHelper(client QueryClient) (rndBrand, rndType string, rndSizes map[int64]string, readArgs crdt.EmbMapPartialArguments) {
	rndBrand, rndType = tpchData.Tables.Brands[client.rng.Intn(len(tpchData.Tables.Brands))], tpchData.Tables.MediumType[client.rng.Intn(len(tpchData.Tables.MediumType))]
	rndSizes, setRead := make(map[int64]string), crdt.GetNElementsArguments{}
	var rndSize int64
	for len(rndSizes) < 8 {
		rndSize = client.rng.Int63n(50) + 1
		rndSizes[rndSize] = strconv.FormatInt(rndSize, 10)
	}
	readArgs = crdt.EmbMapPartialArguments{Args: make(map[string]crdt.ReadArguments)}
	for _, sizeS := range rndSizes {
		readArgs.Args[sizeS] = setRead
	}
	return
}

func getQ16LocalDirectReads(client QueryClient, fullR, partialR [][]crdt.ReadObjectParams, bufI [][]int, rngRegions []int, rngRegionsI, reads int) (newReads, rngRegion int) {
	rndBrand, rndType, _, readArgs := getQ16ReadsHelper(client)
	nRegions := len(tpchData.Tables.Regions)
	for i := 0; i < nRegions; i++ {
		offset, regP := bufI[i][1], partialR[i]
		regP[offset] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q16_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+i]}, ReadArgs: readArgs}
		regP[offset+1] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q16_KEY + rndBrand + rndType, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+1]}, ReadArgs: readArgs}
		bufI[i][1] += 2
	}
	return reads + 16*nRegions, 0
}

func getQ16LocalServerReads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndBrand, rndType, _, readArgs := getQ16ReadsHelper(client)
	nRegions, offset := len(tpchData.Tables.Regions), bufI[1]

	for i := 0; i < nRegions; i++ {
		partialR[offset] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q16_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+i]}, ReadArgs: readArgs}
		partialR[offset+1] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q16_KEY + rndBrand + rndType, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+1]}, ReadArgs: readArgs}
		offset += 2
	}
	bufI[1] = offset
	return reads + 16*nRegions
}

func processQ16Reply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	processQ16ReplyProto(replies[bufI[1] : bufI[1]+8])
	bufI[1] += 8
	return reads + 16
}

func processQ16LocalDirectReply(client QueryClient, replies [][]*proto.ApbReadObjectResp, bufI [][]int, rngRegions []int, rngRegionsI int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	bufI1 := localDirectBufIToServerBufI(bufI, 1)
	relevantReplies := readRepliesToOneSlice(replies, bufI1, 8)
	processQ16ReplyProto(mergeQ16IndexReply(relevantReplies))
	incrementAllBufILocalDirect(bufI, 1, 8)
	return reads + 16*nRegions
}

// Queries that are naturally local (i.e., no merging needed) do not need processLocalServerReply
func processQ16LocalServerReply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	start, end := bufI[1], bufI[1]+nRegions*8
	processQ16ReplyProto(mergeQ16IndexReply(replies[start:end]))
	bufI[1] += nRegions * 8
	return reads + 16*nRegions
}

func processQ16ReplyProto(protos []*proto.ApbReadObjectResp) {
	//Each entry in protos: pair (all, brand+type) for size s.
	if PRINT_QUERY {
		nSupps := make(map[string]int32)
		fmt.Println("[ClientQuery]Q16 query: number of (distinct) suppliers who can provide parts that satisfy a customer's requirements:" +
			"not of a certain brand, not of a certain type, and sizes in a set of values.")
		mapProto := protos[0].GetPartread().GetMap().GetGetvalues() //Sizes for "all"
		keys, entries := mapProto.GetKeys(), mapProto.GetValues()
		for i, entry := range entries {
			nSupps[string(keys[i])] = entry.GetValue().GetPartread().GetSet().GetNelems().GetCount()
		}
		mapProto = protos[1].GetPartread().GetMap().GetGetvalues() //Sizes for brand and type
		for i, entry := range entries {
			fmt.Println(nSupps[string(keys[i])] - entry.GetValue().GetPartread().GetSet().GetNelems().GetCount())
		}
	}
}

func sendQ16(client QueryClient) (nRequests int) {
	rndBrand, rndType, rndSizes, readArgs := getQ16ReadsHelper(client)
	keyPAll, keyPBT := crdt.KeyParams{Key: Q16_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}, crdt.KeyParams{Key: Q16_KEY + rndBrand + rndType, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}
	readP := []crdt.ReadObjectParams{{KeyParams: keyPAll, ReadArgs: readArgs}, {KeyParams: keyPBT, ReadArgs: readArgs}}
	protos := mergeIndex(sendReceiveIndexQuery(client, nil, readP), 16)

	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][RRMap]Q16 query: number of (distinct) suppliers who can provide parts that satisfy a customer's requirements:"+
			"not of brand %s, not of type %s, and sizes in %v\n", rndBrand, rndType, rndSizes)
	}
	processQ16ReplyProto(protos)

	if isIndexGlobal {
		return 16 //2 reads per size, 8 sizes
	}
	return 16 * len(tpchData.Tables.Regions)
}
*/

/*
func getQ16Reads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndBrand, rndType := tpchData.Tables.Brands[client.rng.Intn(len(tpchData.Tables.Brands))], tpchData.Tables.MediumType[client.rng.Intn(len(tpchData.Tables.MediumType))]
	rndSizes, setRead := make(map[int64]struct{}), crdt.GetNElementsArguments{}
	for len(rndSizes) < 8 {
		rndSizes[client.rng.Int63n(50)+1] = struct{}{}
	}
	readArgs := crdt.EmbMapPartialArguments{Args: map[string]crdt.ReadArguments{Q16_ALL: setRead, rndBrand + rndType: setRead}}
	offset := bufI[1]
	for size := range rndSizes {
		partialR[offset] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q16_KEY + strconv.FormatInt(size, 10), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}, ReadArgs: readArgs}
		offset++
	}
	bufI[1] = offset
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][RRMap]Q16 query: number of (distinct) suppliers who can provide parts that satisfy a customer's requirements:"+
			"not of brand %s, not of type %s, and sizes in %v\n", rndBrand, rndType, rndSizes)
	}
	return reads + 16 //2 reads per size
}

func getQ16LocalDirectReads(client QueryClient, fullR, partialR [][]crdt.ReadObjectParams, bufI [][]int, rngRegions []int, rngRegionsI, reads int) (newReads, rngRegion int) {
	rndBrand, rndType := tpchData.Tables.Brands[client.rng.Intn(len(tpchData.Tables.Brands))], tpchData.Tables.MediumType[client.rng.Intn(len(tpchData.Tables.MediumType))]
	rndSizes, nRegions, setRead := make(map[int64]string), 0, crdt.GetNElementsArguments{} //size -> key
	var rndSize int64
	for len(rndSizes) < 8 {
		rndSize = client.rng.Int63n(50) + 1
		rndSizes[rndSize] = Q16_KEY + strconv.FormatInt(rndSize, 10)
	}
	readArgs := crdt.EmbMapPartialArguments{Args: map[string]crdt.ReadArguments{Q16_ALL: setRead, rndBrand + rndType: setRead}}

	for i := 0; i < nRegions; i++ {
		offset, regP := bufI[i][1], partialR[i]
		for _, key := range rndSizes {
			regP[offset] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: key, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+i]}, ReadArgs: readArgs}
			offset++
		}
		bufI[i][1] = offset
	}
	return reads + 16*nRegions, 0
}

func getQ16LocalServerReads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndBrand, rndType := tpchData.Tables.Brands[client.rng.Intn(len(tpchData.Tables.Brands))], tpchData.Tables.MediumType[client.rng.Intn(len(tpchData.Tables.MediumType))]
	rndSizes, nRegions, offset, setRead := make(map[int64]string), 0, bufI[1], crdt.GetNElementsArguments{} //size -> key
	var rndSize int64
	for len(rndSizes) < 8 {
		rndSize = client.rng.Int63n(50) + 1
		rndSizes[rndSize] = Q16_KEY + strconv.FormatInt(rndSize, 10)
	}
	readArgs := crdt.EmbMapPartialArguments{Args: map[string]crdt.ReadArguments{Q16_ALL: setRead, rndBrand + rndType: setRead}}

	for i := 0; i < nRegions; i++ {
		for _, key := range rndSizes {
			partialR[offset] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: key, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+i]}, ReadArgs: readArgs}
			offset++
		}
	}
	bufI[1] = offset
	return reads + 16*nRegions
}

func processQ16Reply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	processQ16ReplyProto(replies[bufI[0] : bufI[0]+8])
	bufI[0] += 8
	return reads + 16
}

func processQ16LocalDirectReply(client QueryClient, replies [][]*proto.ApbReadObjectResp, bufI [][]int, rngRegions []int, rngRegionsI int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	bufI0 := localDirectBufIToServerBufI(bufI, 0)
	relevantReplies := readRepliesToOneSlice(replies, bufI0, 8)
	processQ16ReplyProto(mergeQ16IndexReply(relevantReplies))
	incrementAllBufILocalDirect(bufI, 0, 8)
	return reads + 16*nRegions
}

//Queries that are naturally local (i.e., no merging needed) do not need processLocalServerReply
func processQ16LocalServerReply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	start, end := bufI[0], bufI[0]+nRegions*8
	processQ16ReplyProto(mergeQ16IndexReply(replies[start:end]))
	bufI[0] += nRegions * 8
	return
}

func processQ16ReplyProto(protos []*proto.ApbReadObjectResp) {
	//Each entry in protos: pair (all, brand+type) for size s.
	if PRINT_QUERY {
		var nSupps int32
		fmt.Println("[ClientQuery]Q16 query: number of (distinct) suppliers who can provide parts that satisfy a customer's requirements:" +
			"not of a certain brand, not of a certain type, and sizes in a set of values.")
		for _, pb := range protos {
			entries := pb.GetMap().GetEntries()
			if string(entries[0].GetKey().GetKey()) == Q16_ALL {
				nSupps = entries[0].GetValue().GetPartread().GetSet().GetNelems().GetCount() - entries[1].GetValue().GetPartread().GetSet().GetNelems().GetCount()
			} else {
				nSupps = entries[1].GetValue().GetPartread().GetSet().GetNelems().GetCount() - entries[0].GetValue().GetPartread().GetSet().GetNelems().GetCount()
			}
			fmt.Println(nSupps)
		}
	}
}

func sendQ16(client QueryClient) (nRequests int) {
	rndBrand, rndType := tpchData.Tables.Brands[client.rng.Intn(len(tpchData.Tables.Brands))], tpchData.Tables.MediumType[client.rng.Intn(len(tpchData.Tables.MediumType))]
	rndSizes, nRegions, setRead := make(map[int64]string), 0, crdt.GetNElementsArguments{} //size -> key
	var rndSize int64
	for len(rndSizes) < 8 {
		rndSize = client.rng.Int63n(50) + 1
		rndSizes[rndSize] = Q16_KEY + strconv.FormatInt(rndSize, 10)
	}
	readArgs := crdt.EmbMapPartialArguments{Args: map[string]crdt.ReadArguments{Q16_ALL: setRead, rndBrand + rndType: setRead}}
	readP := make([]crdt.ReadObjectParams, 8)
	i := 0
	for size := range rndSizes {
		readP[i] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q16_KEY + strconv.FormatInt(size, 10), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}, ReadArgs: readArgs}
		i++
	}
	protos := mergeIndex(sendReceiveIndexQuery(client, nil, readP), 16)

	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][RRMap]Q16 query: number of (distinct) suppliers who can provide parts that satisfy a customer's requirements:"+
			"not of brand %s, not of type %s, and sizes in %v\n", rndBrand, rndType, rndSizes)
	}
	processQ16ReplyProto(protos)

	if isIndexGlobal {
		return 16 //2 reads per size
	}
	return 16 * nRegions
}
*/

func getQ17Reads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndBrand, rndContainer := tpchData.Tables.Brands[client.rng.Intn(len(tpchData.Tables.Brands))], tpchData.Tables.Containers[client.rng.Intn(len(tpchData.Tables.Containers))]
	readArgs := crdt.EmbMapPartialArguments{Args: map[string]crdt.ReadArguments{Q17_AVG: crdt.StateReadArguments{}, Q17_MAP: crdt.EmbMapPartialArguments{Args: Q17_QUERY_QUANTITIES}}}
	partialR[bufI[1]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q17_KEY + rndBrand + rndContainer, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}, ReadArgs: readArgs}
	bufI[1]++
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][RRMap]Q17 query: yearly lost revenue if small orders of parts of brand %s and container type %s were no longer taken.\n", rndBrand, rndContainer)
	}
	return reads + 2
}

func getQ17LocalDirectReads(client QueryClient, fullR, partialR [][]crdt.ReadObjectParams, bufI [][]int, rngRegions []int, rngRegionsI, reads int) (newReads, rngRegion int) {
	bc, nRegions := tpchData.Tables.Brands[client.rng.Intn(len(tpchData.Tables.Brands))]+tpchData.Tables.Containers[client.rng.Intn(len(tpchData.Tables.Containers))], len(tpchData.Tables.Regions)
	readArgs := crdt.EmbMapPartialArguments{Args: map[string]crdt.ReadArguments{Q17_AVG: crdt.AvgGetFullArguments{}, Q17_MAP: crdt.EmbMapPartialArguments{Args: Q17_QUERY_QUANTITIES}}}
	for i := 0; i < nRegions; i++ {
		partialR[i][bufI[i][1]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q17_KEY + bc, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+i]}, ReadArgs: readArgs}
		bufI[i][1]++
	}
	return reads + nRegions*2, 0
}

func getQ17LocalServerReads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	bc, nRegions, offset := tpchData.Tables.Brands[client.rng.Intn(len(tpchData.Tables.Brands))]+tpchData.Tables.Containers[client.rng.Intn(len(tpchData.Tables.Containers))], len(tpchData.Tables.Regions), bufI[1]
	readArgs := crdt.EmbMapPartialArguments{Args: map[string]crdt.ReadArguments{Q17_AVG: crdt.AvgGetFullArguments{}, Q17_MAP: crdt.EmbMapPartialArguments{Args: Q17_QUERY_QUANTITIES}}}
	for i := 0; i < nRegions; i++ {
		partialR[offset+i] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q17_KEY + bc, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+i]}, ReadArgs: readArgs}
	}
	bufI[1] += nRegions
	return reads + nRegions*2
}

func processQ17Reply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	processQ17ReplyProto(replies[bufI[0]], client)
	bufI[0]++
	return reads + 2
}

func processQ17LocalDirectReply(client QueryClient, replies [][]*proto.ApbReadObjectResp, bufI [][]int, rngRegions []int, rngRegionsI int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	bufI0 := localDirectBufIToServerBufI(bufI, 0)
	relevantReplies := readRepliesToOneSlice(replies, bufI0, 1)
	processQ17LocalReplyProto(mergeQ17IndexReply(relevantReplies)[0], client)
	incrementAllBufILocalDirect(bufI, 0, 1)
	return reads + 2*nRegions
}

// Queries that are naturally local (i.e., no merging needed) do not need processLocalServerReply
func processQ17LocalServerReply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	start, end := bufI[0], bufI[0]+nRegions
	processQ17LocalReplyProto(mergeQ17IndexReply(replies[start:end])[0], client)
	bufI[0] += nRegions
	return reads + 2*nRegions
}

// Due to averages, the organization of the protobufs is different in local direct and local server compared to global
func processQ17LocalReplyProto(protobuf *proto.ApbReadObjectResp, client QueryClient) {
	entries := protobuf.GetPartread().GetMap().GetGetvalues()
	values := entries.GetValues()
	if len(values) == 0 {
		//Can happen as some combinations have no low quantity order matching the BRAND and CONTAINER
		//Albeit unlikely on proper SFs, i.e., SFs >= 1.
		if PRINT_QUERY {
			fmt.Println("[ClientQuery]Q17 results (yearly lost revenue if small orders of parts of a certain brand and container type were no longer taken): 0")
		}
		*client.nEntries += 1 //Still "pays" as one download
		return
	}
	var avgProto *proto.ApbAvgGetFullReadResp
	var mapProto *proto.ApbMapGetValuesResp
	if values[0].GetCrdttype() == proto.CRDTType_AVG {
		avgProto = values[0].GetValue().GetPartread().GetAvg().GetGetfull()
		mapProto = values[1].GetValue().GetPartread().GetMap().GetGetvalues()
	} else {
		mapProto = values[0].GetValue().GetPartread().GetMap().GetGetvalues()
		avgProto = values[1].GetValue().GetPartread().GetAvg().GetGetfull()
	}

	if PRINT_QUERY {
		fmt.Println("[ClientQuery]Q17 results (yearly lost revenue if small orders of parts of a certain brand and container type were no longer taken):")
		profits, quantities, avgRef := mapProto.GetValues(), mapProto.GetKeys(), int((float64(avgProto.GetSum())/float64(avgProto.GetNAdds()))*0.2)
		sum := 0.0
		//fmt.Println("[ClientQuery]Q17 Average quantity:", avgRef)
		for i, profit := range profits {
			quantity, _ := strconv.Atoi(string(quantities[i]))
			if quantity <= avgRef {
				sum += *profit.GetValue().GetCounterfloat().Value
			}
		}
		sum /= 7
		fmt.Printf("Yearly average: %.2f\n", sum)
	}
	*client.nEntries += len(mapProto.GetValues()) + 1
}

func processQ17ReplyProto(protobuf *proto.ApbReadObjectResp, client QueryClient) {
	entries := protobuf.GetPartread().GetMap().GetGetvalues()
	values := entries.GetValues()
	if len(values) == 0 {
		//Can happen as some combinations have no low quantity order matching the BRAND and CONTAINER
		//Albeit unlikely on proper SFs, i.e., SFs >= 1.
		if PRINT_QUERY {
			fmt.Println("[ClientQuery]Q17 results (yearly lost revenue if small orders of parts of a certain brand and container type were no longer taken): 0")
		}
		*client.nEntries += 1 //Still pays as one download
		return
	}
	var avgProto *proto.ApbGetAverageResp
	var mapProto *proto.ApbMapGetValuesResp
	if values[0].GetCrdttype() == proto.CRDTType_AVG {
		avgProto = values[0].GetValue().GetAvg()
		mapProto = values[1].GetValue().GetPartread().GetMap().GetGetvalues()
	} else {
		mapProto = values[0].GetValue().GetPartread().GetMap().GetGetvalues()
		avgProto = values[1].GetValue().GetAvg()
	}

	if PRINT_QUERY {
		fmt.Println("[ClientQuery]Q17 results (yearly lost revenue if small orders of parts of a certain brand and container type were no longer taken):")
		profits, quantities, avgRef := mapProto.GetValues(), mapProto.GetKeys(), int(avgProto.GetAvg()*0.2)
		sum := 0.0
		//fmt.Println("[ClientQuery]Q17 Average quantity:", avgRef)
		for i, profit := range profits {
			quantity, _ := strconv.Atoi(string(quantities[i]))
			if quantity <= avgRef {
				sum += *profit.GetValue().GetCounterfloat().Value
			}
		}
		sum /= 7
		fmt.Printf("Yearly average: %.2f\n", sum)
	}
	*client.nEntries += len(mapProto.GetValues()) + 1
}

func sendQ17(client QueryClient) (nRequests int) {
	rndBrand, rndContainer := tpchData.Tables.Brands[client.rng.Intn(len(tpchData.Tables.Brands))], tpchData.Tables.Containers[client.rng.Intn(len(tpchData.Tables.Containers))]
	//rndBrand, rndContainer = tpchData.Tables.Brands[2], tpchData.Tables.Containers[0]
	var readReply *proto.ApbReadObjectResp
	keyP := crdt.KeyParams{Key: Q17_KEY + rndBrand + rndContainer, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}

	//For local reads, the average inside the map must be read with AvgFullRead
	if isIndexGlobal {
		readP := []crdt.ReadObjectParams{{KeyParams: keyP, ReadArgs: crdt.EmbMapPartialArguments{Args: map[string]crdt.ReadArguments{Q17_AVG: crdt.StateReadArguments{}, Q17_MAP: crdt.EmbMapPartialArguments{Args: Q17_QUERY_QUANTITIES}}}}}
		readReply = sendReceiveReadProto(client, nil, readP, client.indexServer).GetObjects().GetObjects()[0]
	} else {
		readP := []crdt.ReadObjectParams{{KeyParams: keyP, ReadArgs: crdt.EmbMapPartialArguments{Args: map[string]crdt.ReadArguments{Q17_AVG: crdt.AvgGetFullArguments{}, Q17_MAP: crdt.EmbMapPartialArguments{Args: Q17_QUERY_QUANTITIES}}}}}
		readReply = mergeIndex(sendReceiveIndexQuery(client, nil, readP), 17)[0]
	}

	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][RRMap]Q17 query: yearly lost revenue if small orders of parts of brand %s and container type %s were no longer taken.\n", rndBrand, rndContainer)
	}
	if isIndexGlobal {
		processQ17ReplyProto(readReply, client)
		return 2
	} else {
		processQ17LocalReplyProto(readReply, client)
		return len(client.serverConns) * 2
	}
}

/*
func getQ17Reads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndBrand, rndContainer := tpchData.Tables.Brands[client.rng.Intn(len(tpchData.Tables.Brands))], tpchData.Tables.Containers[client.rng.Intn(len(tpchData.Tables.Containers))]
	fullR[bufI[0]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q17_KEY + rndBrand + rndContainer, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}}
	bufI[0]++
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][RRMap]Q17 query: yearly lost revenue if small orders of parts of brand %s and container type %s were no longer taken.\n", rndBrand, rndContainer)
	}
	return reads + 2
}

func getQ17LocalDirectReads(client QueryClient, fullR, partialR [][]crdt.ReadObjectParams, bufI [][]int, rngRegions []int, rngRegionsI, reads int) (newReads, rngRegion int) {
	bc, nRegions := tpchData.Tables.Brands[client.rng.Intn(len(tpchData.Tables.Brands))]+tpchData.Tables.Containers[client.rng.Intn(len(tpchData.Tables.Containers))], len(tpchData.Tables.Regions)
	for i := 0; i < nRegions; i++ {
		partialR[i][bufI[i][1]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q17_KEY + bc, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+i]},
			ReadArgs: crdt.EmbMapPartialArguments{Args: map[string]crdt.ReadArguments{Q17_AVG: crdt.AvgGetFullArguments{}, Q17_TOP: crdt.StateReadArguments{}}}}
		bufI[i][1]++
	}
	return reads + nRegions*2, 0
}

func getQ17LocalServerReads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	bc, nRegions, offset := tpchData.Tables.Brands[client.rng.Intn(len(tpchData.Tables.Brands))]+tpchData.Tables.Containers[client.rng.Intn(len(tpchData.Tables.Containers))], len(tpchData.Tables.Regions), bufI[1]
	for i := 0; i < nRegions; i++ {
		partialR[offset+i] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q17_KEY + bc, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+i]},
			ReadArgs: crdt.EmbMapPartialArguments{Args: map[string]crdt.ReadArguments{Q17_AVG: crdt.AvgGetFullArguments{}, Q17_TOP: crdt.StateReadArguments{}}}}
	}
	bufI[1] += nRegions
	return reads + nRegions*2
}

func processQ17Reply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	processQ17ReplyProto(replies[bufI[0]])
	bufI[0]++
	return reads + 2
}

func processQ17LocalDirectReply(client QueryClient, replies [][]*proto.ApbReadObjectResp, bufI [][]int, rngRegions []int, rngRegionsI int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	bufI0 := localDirectBufIToServerBufI(bufI, 0)
	relevantReplies := readRepliesToOneSlice(replies, bufI0, 1)
	processQ17LocalReplyProto(mergeQ17IndexReply(relevantReplies)[0])
	incrementAllBufILocalDirect(bufI, 0, 1)
	return reads + 2*nRegions
}

// Queries that are naturally local (i.e., no merging needed) do not need processLocalServerReply
func processQ17LocalServerReply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	start, end := bufI[0], bufI[0]+nRegions
	processQ17LocalReplyProto(mergeQ17IndexReply(replies[start:end])[0])
	bufI[0] += nRegions
	return reads + 2*nRegions
}

// Due to averages, the organization of the protobufs is different in local direct and local server compared to global
func processQ17LocalReplyProto(protobuf *proto.ApbReadObjectResp) {
	entries := protobuf.GetPartread().GetMap().GetGetvalues()
	values := entries.GetValues()
	var avgProto *proto.ApbAvgGetFullReadResp
	var topkProto *proto.ApbGetTopkResp
	if values[0].GetCrdttype() == proto.CRDTType_AVG {
		avgProto = values[0].GetValue().GetPartread().GetAvg().GetGetfull()
		topkProto = values[1].GetValue().GetTopk()
	} else {
		topkProto = values[0].GetValue().GetTopk()
		avgProto = values[1].GetValue().GetPartread().GetAvg().GetGetfull()
	}

	if PRINT_QUERY {
		fmt.Println("[ClientQuery]Q17 results (yearly lost revenue if small orders of parts of a certain brand and container type were no longer taken):")
		values, avgRef := topkProto.GetValues(), int32((float64(avgProto.GetSum())/float64(avgProto.GetNAdds()))*0.2)
		sum, curr := 0.0, 0.0
		//fmt.Println("[ClientQuery]Q17 Average quantity:", avgRef)
		for _, pair := range values {
			if -(pair.GetScore()) <= avgRef {
				curr, _ = strconv.ParseFloat(string(pair.GetData()), 64)
				sum += curr
			}
		}
		sum /= 7
		fmt.Printf("Yearly average: %.2f\n", sum)
	}
}

func processQ17ReplyProto(protobuf *proto.ApbReadObjectResp) {
	entries := protobuf.GetMap().GetEntries()
	var avgProto *proto.ApbGetAverageResp
	var topkProto *proto.ApbGetTopkResp
	if entries[0].GetKey().GetType() == proto.CRDTType_AVG {
		avgProto = entries[0].GetValue().GetAvg()
		topkProto = entries[1].GetValue().GetTopk()
	} else {
		topkProto = entries[0].GetValue().GetTopk()
		avgProto = entries[1].GetValue().GetAvg()
	}

	if PRINT_QUERY {
		fmt.Println("[ClientQuery]Q17 results (yearly lost revenue if small orders of parts of a certain brand and container type were no longer taken):")
		values, avgRef := topkProto.GetValues(), int32(avgProto.GetAvg()*0.2)
		sum, curr := 0.0, 0.0
		//fmt.Println("[ClientQuery]Q17 Average quantity:", avgRef)
		fmt.Println("Len of values:", len(values))
		for _, pair := range values {
			if -(pair.GetScore()) <= avgRef {
				curr, _ = strconv.ParseFloat(string(pair.GetData()), 64)
				sum += curr
			}
		}
		sum /= 7
		fmt.Printf("Yearly average: %.2f\n", sum)
	}
}

func sendQ17(client QueryClient) (nRequests int) {
	rndBrand, rndContainer := tpchData.Tables.Brands[client.rng.Intn(len(tpchData.Tables.Brands))], tpchData.Tables.Containers[client.rng.Intn(len(tpchData.Tables.Containers))]
	rndBrand, rndContainer = tpchData.Tables.Brands[2], tpchData.Tables.Containers[0]
	var readReply *proto.ApbReadObjectResp
	keyP := crdt.KeyParams{Key: Q17_KEY + rndBrand + rndContainer, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}

	//For local reads, the average inside the map must be read with AvgFullRead
	if isIndexGlobal {
		readP := []crdt.ReadObjectParams{{KeyParams: keyP}}
		readReply = sendReceiveReadObjsProto(client, readP, client.indexServer).GetObjects().GetObjects()[0]
	} else {
		readP := []crdt.ReadObjectParams{{KeyParams: keyP, ReadArgs: crdt.EmbMapPartialArguments{Args: map[string]crdt.ReadArguments{Q17_AVG: crdt.AvgGetFullArguments{}, Q17_TOP: crdt.StateReadArguments{}}}}}
		readReply = mergeIndex(sendReceiveIndexQuery(client, nil, readP), 17)[0]
	}

	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][RRMap]Q17 query: yearly lost revenue if small orders of parts of brand %s and container type %s were no longer taken.\n", rndBrand, rndContainer)
	}
	if isIndexGlobal {
		processQ17ReplyProto(readReply)
		return 2
	} else {
		processQ17LocalReplyProto(readReply)
		return len(client.serverConns) * 2
	}
}
*/

func getQ19Reads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	brandsMap, quantities := getBrandsQuantitiesQ19(client)
	//offset, i := bufI[1], 0
	//3 reads: one per brand
	/*for brand := range brandsMap {
		partialR[offset] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q19_KEY + Q19_CONTAINERS[i], CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]},
			ReadArgs: getQ19QuantityReadArgs(quantities[i], brand)}
		offset++
		i++
	}*/
	//bufI[1] = offset
	readArgs, i := crdt.EmbMapPartialArguments{Args: make(map[string]crdt.ReadArguments, 3)}, 0
	for brand := range brandsMap {
		readArgs.Args[Q19_CONTAINERS[i]] = getQ19QuantityReadArgs(quantities[i], brand)
		i++
	}
	partialR[bufI[1]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q19_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}, ReadArgs: readArgs}
	bufI[1]++
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][RRMap]Q19 query: Discounted revenue of orders delivered by air, in person, of brands %v, quantity %v, with a given size and container.\n", brandsMap, quantities)
	}
	return reads + 33
}

func getBrandsQuantitiesQ19(client QueryClient) (brandsMap map[string]struct{}, quantities []int64) {
	brands, brandMap := tpchData.Tables.Brands, make(map[string]struct{})
	for len(brandMap) < 3 {
		brandMap[brands[client.rng.Intn(len(brands))]] = struct{}{}
	}
	//brands := tpchData.Tables.Brands
	//brandMap := map[string]struct{}{brands[0]: {}, brands[1]: {}, brands[2]: {}}
	//return brandMap, []int64{3, 11, 29}
	return brandMap, []int64{client.rng.Int63n(10) + 1, client.rng.Int63n(11) + 10, client.rng.Int63n(11) + 20}
}

func getQ19QuantityReadArgs(quantity int64, brand string) crdt.EmbMapPartialArguments {
	keys := make(map[string]crdt.ReadArguments, 11)
	for i := 0; i < 11; i++ {
		//keys[i] = brand + strconv.FormatInt(quantity, 10)
		keys[brand+strconv.FormatInt(quantity, 10)] = crdt.StateReadArguments{}
		quantity++
	}
	return crdt.EmbMapPartialArguments{Args: keys}
}

func getQ19LocalDirectReads(client QueryClient, fullR, partialR [][]crdt.ReadObjectParams, bufI [][]int, rngRegions []int, rngRegionsI, reads int) (newReads, rngRegion int) {
	brandsMap, quantities := getBrandsQuantitiesQ19(client)
	//readArgs, keyP, nRegions := make([]crdt.EmbMapPartialArguments, len(brandsMap)), make([]crdt.KeyParams, len(brandsMap)), len(tpchData.Tables.Regions)
	nRegions := len(tpchData.Tables.Regions)
	readArgs, nRegions, j := crdt.EmbMapPartialArguments{Args: make(map[string]crdt.ReadArguments, 3)}, len(tpchData.Tables.Regions), 0
	for brand := range brandsMap {
		readArgs.Args[Q19_CONTAINERS[j]] = getQ19QuantityReadArgs(quantities[j], brand)
		//readArgs[j], keyP[j] = getQ19QuantityReadArgs(quantities[j], brand), crdt.KeyParams{Key: Q19_KEY + Q19_CONTAINERS[j], CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}
		j++
	}
	for i := 0; i < nRegions; i++ {
		/*localP, localBufI := partialR[i], bufI[i][1]
		for j = 0; j < len(readArgs); j++ {
			keyP[j].Bucket = buckets[INDEX_BKT+i]
			localP[localBufI] = crdt.ReadObjectParams{KeyParams: keyP[j], ReadArgs: readArgs[j]}
			localBufI++
		}
		bufI[i][1] = localBufI*/
		partialR[i][bufI[i][1]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q19_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+i]}, ReadArgs: readArgs}
		bufI[i][1]++
	}
	return reads + nRegions*33, 0
}

func getQ19LocalServerReads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	brandsMap, quantities := getBrandsQuantitiesQ19(client)
	//readArgs, keyP, nRegions, offset := make([]crdt.EmbMapPartialArguments, len(brandsMap)), make([]crdt.KeyParams, len(brandsMap)), len(tpchData.Tables.Regions), bufI[1]
	readArgs, nRegions, offset := crdt.EmbMapPartialArguments{Args: make(map[string]crdt.ReadArguments, 3)}, len(tpchData.Tables.Regions), bufI[1]
	j := 0
	for brand := range brandsMap {
		//readArgs[j], keyP[j] = getQ19QuantityReadArgs(quantities[j], Q19_CONTAINERS[j]), crdt.KeyParams{Key: Q19_KEY + brand, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}
		//readArgs[j], keyP[j] = getQ19QuantityReadArgs(quantities[j], brand), crdt.KeyParams{Key: Q19_KEY + Q19_CONTAINERS[j], CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}
		readArgs.Args[Q19_CONTAINERS[j]] = getQ19QuantityReadArgs(quantities[j], brand)
		j++
	}
	for i := 0; i < nRegions; i++ {
		/*for j = 0; j < len(readArgs); j++ {
			keyP[j].Bucket = buckets[INDEX_BKT+i]
			partialR[offset] = crdt.ReadObjectParams{KeyParams: keyP[j], ReadArgs: readArgs[j]}
			offset++
		}*/
		partialR[offset] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q19_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+i]}, ReadArgs: readArgs}
		offset++
	}
	bufI[1] = offset
	return reads + nRegions*33
}

func processQ19Reply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	//processQ19ReplyProto(replies[bufI[1]:bufI[1]+3], client)
	processQ19ReplyProto(replies[bufI[1]], client)
	//bufI[1] += 3
	bufI[1]++
	return reads + 33
}

func processQ19LocalDirectReply(client QueryClient, replies [][]*proto.ApbReadObjectResp, bufI [][]int, rngRegions []int, rngRegionsI int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	bufI1 := localDirectBufIToServerBufI(bufI, 1)
	//relevantReplies := readRepliesToOneSlice(replies, bufI1, 3)
	relevantReplies := readRepliesToOneSlice(replies, bufI1, 1)
	processQ19ReplyProto(mergeQ19IndexReply(relevantReplies)[0], client)
	incrementAllBufILocalDirect(bufI, 1, 1 /*3*/)
	return reads + nRegions*33
}

// Queries that are naturally local (i.e., no merging needed) do not need processLocalServerReply
func processQ19LocalServerReply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	nRegions := len(client.serverConns)
	start, end := bufI[1] /*, bufI[1]+nRegions*3*/, bufI[1]+nRegions
	processQ19ReplyProto(mergeQ19IndexReply(replies[start:end])[0], client)
	//bufI[1] += nRegions * 3
	bufI[1] += nRegions
	return reads + nRegions*33
}

func processQ19ReplyProto( /*protobufs []*proto.ApbReadObjectResp*/ protobufs *proto.ApbReadObjectResp, client QueryClient) {
	revenue := 0.0
	nEntries := 0
	insidePbs := protobufs.GetPartread().GetMap().GetGetvalues().GetValues()
	//fmt.Printf("[ClientQuery]Q19: PBs received: %v\n", protobufs)
	//for _, protobuf := range protobufs {
	for _, insidePb := range insidePbs {
		mapEntries := insidePb.GetValue().GetPartread().GetMap().GetGetvalues().GetValues()
		nEntries += len(mapEntries)
		for _, entry := range mapEntries {
			revenue += entry.GetValue().GetCounterfloat().GetValue()
		}
	}
	if PRINT_QUERY {
		fmt.Println("[ClientQuery]Q19: Discounted revenue of orders delivered by air, in person, of given brands, quantities, with a given size and container: ", revenue)
	}
	*client.nEntries += nEntries
}

func sendQ19(client QueryClient) (nRequests int) {
	brandsMap, quantities := getBrandsQuantitiesQ19(client)
	/*i, readP := 0, make([]crdt.ReadObjectParams, len(brandsMap))
	//3 reads: one per brand
	for brand := range brandsMap {
		readP[i] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q19_KEY + Q19_CONTAINERS[i], CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]},
			ReadArgs: getQ19QuantityReadArgs(quantities[i], brand)}
		i++
	}*/

	readArgs, i := crdt.EmbMapPartialArguments{Args: make(map[string]crdt.ReadArguments, 3)}, 0
	for brand := range brandsMap {
		readArgs.Args[Q19_CONTAINERS[i]] = getQ19QuantityReadArgs(quantities[i], brand)
		i++
	}
	readP := []crdt.ReadObjectParams{{KeyParams: crdt.KeyParams{Key: Q19_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}, ReadArgs: readArgs}}

	protos := mergeIndex(sendReceiveIndexQuery(client, nil, readP), 19)

	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][RRMap]Q19 query: Discounted revenue of orders delivered by air, in person, of brands %v, quantity %v, with a given size and container.\n", brandsMap, quantities)
	}
	processQ19ReplyProto(protos[0], client)
	//processQ19ReplyProto(protos, client)

	if isIndexGlobal {
		return 33
	}
	return 33 * len(tpchData.Tables.Regions)
}

func getQ20Reads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	//Why some return so many results and others none? Shouldn't all return results?
	rndNation, rndColor, rndYear := client.getRngNation(), tpchData.Tables.Colors[client.rng.Intn(len(tpchData.Tables.Colors))], strconv.FormatInt(client.rng.Int63n(5)+1993, 10)
	client.currQ20.nation, client.currQ20.color, client.currQ20.year = tpchData.Tables.Nations[rndNation].N_NAME, rndColor, rndYear
	keyParams := crdt.KeyParams{Key: Q20_KEY + strconv.Itoa(rndNation), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}
	partialR[bufI[1]] = crdt.ReadObjectParams{KeyParams: keyParams, ReadArgs: getQ20ReadArgs(rndColor, rndYear)}
	bufI[1]++
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][RRMap]Q20 query: suppliers of nation %s with excess supply of parts named %s_ for year %s.\n", tpchData.Tables.Nations[rndNation].N_NAME, rndColor, rndYear)
	}
	return reads + 1
}

func getQ20ReadArgs(color string, year string) (read crdt.EmbMapGetValueArguments) {
	getNoComp := &crdt.GetNoCompareArguments{}
	inCompArgs := map[string]crdt.CompareArguments{Q20_NAME: getNoComp, Q20_ADDRESS: getNoComp, Q20_AVAILQTY: getNoComp, year: &crdt.IntCompareArguments{Value: 0, CompType: crdt.H}}
	outerRead := crdt.EmbMapGetValueArguments{Key: color, Args: crdt.EmbMapConditionalReadAllArguments{
		CompareArguments: &crdt.MapCompareArguments{MapArgs: inCompArgs}}}
	return outerRead
}

func getQ20LocalDirectReads(client QueryClient, fullR, partialR [][]crdt.ReadObjectParams, bufI [][]int, rngRegions []int, rngRegionsI, reads int) (newReads, rngRegion int) {
	rndNation, rndColor, rndYear := client.getRngNation(), tpchData.Tables.Colors[client.rng.Intn(len(tpchData.Tables.Colors))], strconv.FormatInt(client.rng.Int63n(5)+1993, 10)
	regionKey := tpchData.Tables.NationkeyToRegionkey(int64(rndNation))
	keyParams := crdt.KeyParams{Key: Q20_KEY + strconv.Itoa(rndNation), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+regionKey]}
	partialR[regionKey][bufI[regionKey][1]] = crdt.ReadObjectParams{KeyParams: keyParams, ReadArgs: getQ20ReadArgs(rndColor, rndYear)}
	bufI[regionKey][1]++
	rngRegions[rngRegionsI] = int(regionKey)
	return reads + 1, int(regionKey)
}

func getQ20LocalServerReads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndNation, rndColor, rndYear := client.getRngNation(), tpchData.Tables.Colors[client.rng.Intn(len(tpchData.Tables.Colors))], strconv.FormatInt(client.rng.Int63n(5)+1993, 10)
	regionKey := tpchData.Tables.NationkeyToRegionkey(int64(rndNation))
	keyParams := crdt.KeyParams{Key: Q20_KEY + strconv.Itoa(rndNation), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+regionKey]}
	partialR[bufI[1]] = crdt.ReadObjectParams{KeyParams: keyParams, ReadArgs: getQ20ReadArgs(rndColor, rndYear)}
	bufI[1]++
	return reads + 1
}

func processQ20Reply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	processQ20ReplyProto(client, replies[bufI[1]])
	bufI[1]++
	return reads + 1
}

func processQ20LocalDirectReply(client QueryClient, replies [][]*proto.ApbReadObjectResp, bufI [][]int, rngRegions []int, rngRegionsI int, reads int) (newReads int) {
	index := rngRegions[rngRegionsI]
	processQ20ReplyProto(client, replies[index][bufI[index][1]])
	bufI[index][1]++
	return reads + 1
}

type Q20PrintInfo struct {
	sName, sAddress string
}

func processQ20ReplyProto(client QueryClient, protobuf *proto.ApbReadObjectResp) {
	//Map[part+sup]->Map[Name,Address,AvailQty,Sum]
	//A map with 4 entries:
	entries := protobuf.GetPartread().GetMap().GetGetvalue().GetValue().GetMap().GetEntries()
	//fmt.Println("[Q20ReplyProto]NEntries received:", len(entries))
	//fmt.Printf("[Q20ReplyProto]Inside GetValue: %+v\n", protobuf.GetPartread().GetMap().GetGetvalue().GetValue())
	//fmt.Printf("Entries: %v\n", entries)
	//fmt.Printf("Outside: %v\n", protobuf.GetPartread().GetMap().GetGetvalue())
	//fmt.Printf("Inside with map: %v\n", protobuf.GetPartread().GetMap().GetGetvalue().GetValue().GetMap())
	//fmt.Printf("Inside with partread: %v\n", protobuf.GetPartread().GetMap().GetGetvalue().GetValue().GetPartread())
	//fmt.Printf("Outside w/o partial read: %v\n", protobuf.GetMap().GetEntries())
	//fmt.Printf("Inside w/o partial read, inside w/o partial: %v\n", protobuf.GetMap().GetEntries()[0].GetValue().GetMap())
	//fmt.Printf("Inside w/o partial read, inside with partial: %v\n", protobuf.GetMap().GetEntries()[0].GetValue().GetPartread().GetMap())
	sortEntries := make([]Q20PrintInfo, len(entries))
	i := 0
	var availQty, sumItem int32

	ignore(sortEntries, i, availQty, sumItem)

	//For fairness, still need to do the filtering and sorting.
	var printInfo Q20PrintInfo
	for _, entry := range entries {
		//An inside map with 4 entries: one for each information
		//fmt.Printf("[Entries]Entry.GetValue(): %v\n", entry.GetValue())
		//fmt.Printf("[Entries]Entry.GetValue().GetMap().GetEntries(): %v\n", entry.GetValue().GetMap().GetEntries())
		//fmt.Printf("[Entries]Entry.GetValue().GetPartread(): %v\n", entry.GetValue().GetPartread())
		partSup := entry.GetValue().GetMap().GetEntries()
		for _, field := range partSup {
			switch string(field.GetKey().GetKey()) {
			case Q20_NAME:
				printInfo.sName = string(field.GetValue().GetReg().GetValue())
			case Q20_ADDRESS:
				printInfo.sAddress = string(field.GetValue().GetReg().GetValue())
			case Q20_AVAILQTY:
				availQty = field.GetValue().GetCounter().GetValue()
			default: //the key will be the year that we queried for
				sumItem = field.GetValue().GetCounter().GetValue()
			}
		}
		if float64(availQty) > 0.5*float64(sumItem) {
			//fmt.Printf("[ClientQuery]Q20 availQty > 0.5*sumItem. Quantity: %d. SumItem: %f. nation %s, color %s, year %s\n",
			//availQty, 0.5*float64(sumItem), client.currQ20.nation, client.currQ20.color, client.currQ20.year)
			sortEntries[i] = printInfo
			i++
		} else {
			//fmt.Printf("[ClientQuery]Q20 availQty is not > 0.5*sumItem. Quantity: %d. SumItem: %f. nation %s, color %s, year %s\n",
			//availQty, 0.5*float64(sumItem), client.currQ20.nation, client.currQ20.color, client.currQ20.year)
		}
	}
	sortEntries = sortEntries[:i]
	sort.Slice(sortEntries, func(i, j int) bool { return sortEntries[i].sName < sortEntries[j].sName })
	//fmt.Printf("[ClientQuery]Q20 query for nation %s, color %s, year %s. Number of entries returned: %d. Number of original entries: %d\n",
	//client.currQ20.nation, client.currQ20.color, client.currQ20.year, len(sortEntries), len(entries))

	if PRINT_QUERY {
		fmt.Printf("[ClientQuery]Q20 query: suppliers of a given nation with excess supply of parts with a given name start for a given year.\n%s\n", sortEntries)
	}

	*client.nEntries += len(entries)
}

func sendQ20(client QueryClient) (nRequests int) {
	rndNation, rndColor, rndYear := client.getRngNation(), tpchData.Tables.Colors[client.rng.Intn(len(tpchData.Tables.Colors))], strconv.FormatInt(client.rng.Int63n(5)+1993, 10)
	client.currQ20.nation, client.currQ20.color, client.currQ20.year = tpchData.Tables.Nations[rndNation].N_NAME, rndColor, rndYear
	//rndNation, rndColor, rndYear = 0, tpchData.Tables.Colors[0], "1995"
	regionKey := tpchData.Tables.NationkeyToRegionkey(int64(rndNation))
	bktI, serverI := getIndexOffset(client, int(regionKey))

	keyParams := crdt.KeyParams{Key: Q20_KEY + strconv.Itoa(rndNation), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bktI]}
	readP := []crdt.ReadObjectParams{{KeyParams: keyParams, ReadArgs: getQ20ReadArgs(rndColor, rndYear)}}
	replyProto := sendReceiveReadProto(client, nil, readP, serverI)

	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][RRMap]Q20 query: suppliers of nation %s with excess supply of parts named %s for year %s.\n", tpchData.Tables.Nations[rndNation].N_NAME, rndColor, rndYear)
	}
	processQ20ReplyProto(client, replyProto.GetObjects().GetObjects()[0])

	return 1
}

/*
func getQ20Reads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndNation, rndColor, rndYear := client.getRngNation(), tpchData.Tables.Colors[client.rng.Intn(len(tpchData.Tables.Colors))], strconv.FormatInt(client.rng.Int63n(5)+1993, 10)
	keyParams := crdt.KeyParams{Key: Q20_KEY + strconv.Itoa(rndNation), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}
	partialR[bufI[1]] = crdt.ReadObjectParams{KeyParams: keyParams, ReadArgs: crdt.GetValueArguments{Key: rndColor + rndYear}}
	bufI[1]++
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][RRMap]Q20 query: suppliers of nation %s with excess supply of parts named %s_ for year %s.\n", tpchData.Tables.Nations[rndNation].N_NAME, rndColor, rndYear)
	}
	return reads + 1
}

func getQ20LocalDirectReads(client QueryClient, fullR, partialR [][]crdt.ReadObjectParams, bufI [][]int, rngRegions []int, rngRegionsI, reads int) (newReads, rngRegion int) {
	rndNation, rndColor, rndYear := client.getRngNation(), tpchData.Tables.Colors[client.rng.Intn(len(tpchData.Tables.Colors))], strconv.FormatInt(client.rng.Int63n(5)+1993, 10)
	regionKey := tpchData.Tables.NationkeyToRegionkey(int64(rndNation))
	keyParams := crdt.KeyParams{Key: Q20_KEY + strconv.Itoa(rndNation), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+regionKey]}
	partialR[regionKey][bufI[regionKey][1]] = crdt.ReadObjectParams{KeyParams: keyParams, ReadArgs: crdt.GetValueArguments{Key: rndColor + rndYear}}
	bufI[regionKey][1]++
	rngRegions[rngRegionsI] = int(regionKey)
	return reads + 1, int(regionKey)
}

func getQ20LocalServerReads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndNation, rndColor, rndYear := client.getRngNation(), tpchData.Tables.Colors[client.rng.Intn(len(tpchData.Tables.Colors))], strconv.FormatInt(client.rng.Int63n(5)+1993, 10)
	regionKey := tpchData.Tables.NationkeyToRegionkey(int64(rndNation))
	keyParams := crdt.KeyParams{Key: Q20_KEY + strconv.Itoa(rndNation), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+regionKey]}
	partialR[bufI[1]] = crdt.ReadObjectParams{KeyParams: keyParams, ReadArgs: crdt.GetValueArguments{Key: rndColor + rndYear}}
	bufI[1]++
	return reads + 1
}

func processQ20Reply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	processQ20ReplyProto(replies[bufI[1]])
	bufI[1]++
	return reads + 1
}

func processQ20LocalDirectReply(client QueryClient, replies [][]*proto.ApbReadObjectResp, bufI [][]int, rngRegions []int, rngRegionsI int, reads int) (newReads int) {
	index := rngRegions[rngRegionsI]
	processQ20ReplyProto(replies[index][bufI[index][1]])
	bufI[index][1]++
	return reads + 1
}

type Q20PrintInfo struct {
	sName, sAddress string
}

func processQ20ReplyProto(protobuf *proto.ApbReadObjectResp) {
	//Map[part+sup]->Map[Name,Address,AvailQty,Sum]
	//A map with 4 entries:
	entries := protobuf.GetPartread().GetMap().GetGetvalue().GetValue().GetMap().GetEntries()
	sortEntries := make([]Q20PrintInfo, len(entries))
	i := 0
	var availQty, sumItem int32

	ignore(sortEntries, i, availQty, sumItem)

	//For fairness, still need to do the filtering and sorting.
	var printInfo Q20PrintInfo
	for _, entry := range entries {
		//An inside map with 4 entries: one for eac information
		partSup := entry.GetValue().GetMap().GetEntries()
		for _, field := range partSup {
			switch string(field.GetKey().GetKey()) {
			case Q20_NAME:
				printInfo.sName = string(field.GetValue().GetReg().GetValue())
			case Q20_ADDRESS:
				printInfo.sAddress = string(field.GetValue().GetReg().GetValue())
			case Q20_AVAILQTY:
				availQty = field.GetValue().GetCounter().GetValue()
			case Q20_SUM:
				sumItem = field.GetValue().GetCounter().GetValue()
			}
		}
		if float64(availQty) > 0.5*float64(sumItem) {
			sortEntries[i] = printInfo
			i++
		}
	}
	sortEntries = sortEntries[:i]
	sort.Slice(sortEntries, func(i, j int) bool { return sortEntries[i].sName < sortEntries[j].sName })

	if PRINT_QUERY {
		fmt.Printf("[ClientQuery]Q20 query: suppliers of a given nation with excess supply of parts with a given name start for a given year.\n%s\n", sortEntries)
	}

}

func sendQ20(client QueryClient) (nRequests int) {
	rndNation, rndColor, rndYear := client.getRngNation(), tpchData.Tables.Colors[client.rng.Intn(len(tpchData.Tables.Colors))], strconv.FormatInt(client.rng.Int63n(5)+1993, 10)
	//rndNation, rndColor, rndYear = 0, tpchData.Tables.Colors[0], "1995"
	regionKey := tpchData.Tables.NationkeyToRegionkey(int64(rndNation))
	bktI, serverI := getIndexOffset(client, int(regionKey))

	keyParams := crdt.KeyParams{Key: Q20_KEY + strconv.Itoa(rndNation), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bktI]}
	readP := []crdt.ReadObjectParams{{KeyParams: keyParams, ReadArgs: crdt.GetValueArguments{Key: rndColor + rndYear}}}
	replyProto := sendReceiveReadProto(client, nil, readP, serverI)

	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][RRMap]Q20 query: suppliers of nation %s with excess supply of parts named %s for year %s.\n", tpchData.Tables.Nations[rndNation].N_NAME, rndColor, rndYear)
	}
	processQ20ReplyProto(replyProto.GetObjects().GetObjects()[0])

	return 1
}
*/

func getQ21Reads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndNation := client.getRngNation()
	fullR[bufI[0]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q21_KEY + strconv.Itoa(rndNation), CrdtType: proto.CRDTType_TOPSUM, Bucket: buckets[INDEX_BKT]}}
	bufI[0]++
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][TopSum]Q21 query: suppliers of nation %s that kept orders waiting (i.e., the only supplier of an order that did not deliver on time)\n", tpchData.Tables.Nations[rndNation].N_NAME)
	}
	return reads + 1
}

func getQ21LocalDirectReads(client QueryClient, fullR, partialR [][]crdt.ReadObjectParams, bufI [][]int, rngRegions []int, rngRegionsI, reads int) (newReads, rngRegion int) {
	rndNation := client.getRngNation()
	regionKey := tpchData.Tables.NationkeyToRegionkey(int64(rndNation))
	fullR[regionKey][bufI[regionKey][0]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q21_KEY + strconv.Itoa(rndNation), CrdtType: proto.CRDTType_TOPSUM, Bucket: buckets[INDEX_BKT+regionKey]}}
	bufI[regionKey][0]++
	rngRegions[rngRegionsI] = int(regionKey)
	return reads + 1, int(regionKey)
}

func getQ21LocalServerReads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	rndNation := client.getRngNation()
	regionKey := tpchData.Tables.NationkeyToRegionkey(int64(rndNation))
	fullR[bufI[0]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q21_KEY + strconv.Itoa(rndNation), CrdtType: proto.CRDTType_TOPSUM, Bucket: buckets[INDEX_BKT+regionKey]}}
	bufI[0]++
	return reads + 1
}

func processQ21Reply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	processQ21ReplyProto(replies[bufI[0]], client)
	bufI[0]++
	return reads + 1
}

func processQ21LocalDirectReply(client QueryClient, replies [][]*proto.ApbReadObjectResp, bufI [][]int, rngRegions []int, rngRegionsI int, reads int) (newReads int) {
	index := rngRegions[rngRegionsI]
	processQ21ReplyProto(replies[index][bufI[index][0]], client)
	bufI[index][0]++
	return reads + 1
}

func processQ21ReplyProto(protobuf *proto.ApbReadObjectResp, client QueryClient) {
	top := protobuf.GetTopk().GetValues()
	if PRINT_QUERY {
		fmt.Println("[ClientQuery]Q21 query: number of orders not delivered on time by the following suppliers of a given country:")
		for _, pair := range top {
			fmt.Printf("%v: %d\n", string(pair.GetData()), pair.GetScore())
		}
	}
	*client.nEntries += len(top)
}

func sendQ21(client QueryClient) (nRequests int) {
	rndNation := client.getRngNation()
	//rndNation = 0
	regionKey := tpchData.Tables.NationkeyToRegionkey(int64(rndNation))
	bktI, serverI := getIndexOffset(client, int(regionKey))

	readP := []crdt.ReadObjectParams{{KeyParams: crdt.KeyParams{Key: Q21_KEY + strconv.Itoa(rndNation), CrdtType: proto.CRDTType_TOPSUM, Bucket: buckets[bktI]}}}
	replyProto := sendReceiveReadObjsProto(client, readP, serverI)

	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][TopSum]Q21 query: suppliers of nation %s that kept orders waiting (i.e., the only supplier of an order that did not deliver on time)\n", tpchData.Tables.Nations[rndNation].N_NAME)
	}
	processQ21ReplyProto(replyProto.GetObjects().GetObjects()[0], client)

	return 1
}

func getQ22Reads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	myRegion, nextRegion, myRegRead, otherRegRead := getQ22ReadsHelper(client)

	partialR[bufI[1]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q22_KEY + strconv.Itoa(myRegion), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}, ReadArgs: myRegRead}
	partialR[bufI[1]+1] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q22_KEY + strconv.Itoa(nextRegion), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}, ReadArgs: otherRegRead}
	bufI[1] += 2

	return reads + 16
}

func getQ22ReadsHelper(client QueryClient) (myRegion, nextRegion int, myRegRead, otherRegRead crdt.EmbMapPartialArguments) {
	myRegion = client.getRngRegion()
	rndNationsP2 := make(map[int8]struct{})
	//rndNationsP1 := tpchData.Tables.NationsByRegion[myRegion] //Each region has exactly 5 nations.
	myRegionNations, rndNationsP1 := tpchData.Tables.NationsByRegion[myRegion], make([]int8, 5) //Each region has exactly 5 nations.
	for i, natId := range myRegionNations {
		rndNationsP1[i] = natId + 10
	}

	nextRegion = (myRegion + 1) % len(tpchData.Tables.NationsByRegion)
	otherRegionNations := tpchData.Tables.NationsByRegion[nextRegion]
	//Request the remaining from the "next" region
	/*for len(rndNationsP2) < 3 {
		rndNationsP2[otherRegionNations[client.rng.Intn(len(otherRegionNations))]+10] = struct{}{}
	}*/
	rndNationsP2[otherRegionNations[0]+10], rndNationsP2[otherRegionNations[1]+10], rndNationsP2[otherRegionNations[2]+10] = struct{}{}, struct{}{}, struct{}{}
	minAvgBal := 100000000.0
	currAvgBal := 0.0
	var pair PairIntFloat
	for _, nat := range rndNationsP1 {
		pair = q22CustNatBalances[nat]
		currAvgBal = pair.first / float64(pair.second)
		//fmt.Printf("Avg ball for nation %d: %f. Sum ball: %f, Number customers: %d\n", nat, currAvgBal, pair.first, pair.second)
		if currAvgBal < minAvgBal {
			minAvgBal = currAvgBal
		}
	}
	for nat := range rndNationsP2 {
		pair = q22CustNatBalances[nat]
		currAvgBal = pair.first / float64(pair.second)
		//fmt.Printf("Avg ball for nation %d: %f. Sum ball: %f, Number customers: %d\n", nat, currAvgBal, pair.first, pair.second)
		if currAvgBal < minAvgBal {
			minAvgBal = currAvgBal
		}
	}
	//fmt.Println("[Q22][ReadsHelper]MinAvgBal to request:", minAvgBal)

	custCompArgs := map[string]crdt.CompareArguments{"C": &crdt.IntCompareArguments{Value: 0, CompType: crdt.EQ}, "A": &crdt.IntCompareArguments{Value: int64(minAvgBal), CompType: crdt.H}}
	custMapArgs, avgArgs := crdt.EmbMapConditionalReadAllArguments{CompareArguments: &crdt.MapCompareArguments{MapArgs: custCompArgs}}, crdt.AvgGetFullArguments{}
	myRegRead, otherRegRead = crdt.EmbMapPartialArguments{Args: make(map[string]crdt.ReadArguments)}, crdt.EmbMapPartialArguments{Args: make(map[string]crdt.ReadArguments)}
	var natIdString string

	for _, natId := range rndNationsP1 {
		natIdString = strconv.FormatInt(int64(natId), 10)
		myRegRead.Args[natIdString], myRegRead.Args[Q22_AVG+natIdString] = custMapArgs, avgArgs
	}
	for natId := range rndNationsP2 {
		natIdString = strconv.FormatInt(int64(natId), 10)
		otherRegRead.Args[natIdString], otherRegRead.Args[Q22_AVG+natIdString] = custMapArgs, avgArgs
	}
	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][RRMap]Q22 query: number of customers of countries with phone code %v, %v that have not placed any order and have above average balance.\n", rndNationsP1, rndNationsP2)
	}
	return
}

func getQ22LocalDirectReads(client QueryClient, fullR, partialR [][]crdt.ReadObjectParams, bufI [][]int, rngRegions []int, rngRegionsI, reads int) (newReads, rngRegion int) {
	myRegion, nextRegion, myRegRead, otherRegRead := getQ22ReadsHelper(client)

	partialR[myRegion][bufI[myRegion][1]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q22_KEY + strconv.Itoa(myRegion), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+myRegion]}, ReadArgs: myRegRead}
	partialR[nextRegion][bufI[nextRegion][1]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q22_KEY + strconv.Itoa(nextRegion), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+nextRegion]}, ReadArgs: otherRegRead}
	bufI[myRegion][1]++
	bufI[nextRegion][1]++
	rngRegions[rngRegionsI] = myRegion

	return reads + 16, myRegion
}

func getQ22LocalServerReads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	myRegion, nextRegion, myRegRead, otherRegRead := getQ22ReadsHelper(client)

	partialR[bufI[1]] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q22_KEY + strconv.Itoa(myRegion), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+myRegion]}, ReadArgs: myRegRead}
	partialR[bufI[1]+1] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q22_KEY + strconv.Itoa(nextRegion), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+nextRegion]}, ReadArgs: otherRegRead}
	bufI[1] += 2
	return reads + 16
}

func processQ22Reply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	processQ22ReplyProto(replies[bufI[1]:bufI[1]+1], client)
	bufI[1] += 2
	return reads + 16
}

func processQ22LocalDirectReply(client QueryClient, replies [][]*proto.ApbReadObjectResp, bufI [][]int, rngRegions []int, rngRegionsI int, reads int) (newReads int) {
	index := rngRegions[rngRegionsI]
	nextIndex := (index + 1) % len(tpchData.Tables.Regions)
	concReplies := []*proto.ApbReadObjectResp{replies[index][bufI[index][1]], replies[nextIndex][bufI[nextIndex][1]]}
	processQ22ReplyProto(concReplies, client)
	bufI[index][1]++
	bufI[nextIndex][1]++
	return reads + 16
}

func processQ22ReplyProto(protos []*proto.ApbReadObjectResp, client QueryClient) {
	//Note: For fairness, processing is still done even when not printing.
	sum, count := int64(0), int64(0)
	var currAvgProto *proto.ApbAvgGetFullReadResp
	natCustMaps, i := make([]*proto.ApbGetMapResp, 8), 0
	natIds := make([]string, 8)
	nEntries := 0
	//fmt.Printf("[Q22]Protos: %+v\n", protos)
	//fmt.Printf("[Q22]")
	for _, regProto := range protos {
		natEntries := regProto.GetPartread().GetMap().GetGetvalues()
		natKeys := natEntries.GetKeys()
		for j, natProtos := range natEntries.GetValues() {
			//fmt.Printf("[Q22]RegProto: %+v\n", natProtos)
			if natProtos.GetCrdttype() == proto.CRDTType_AVG {
				currAvgProto = natProtos.GetValue().GetPartread().GetAvg().GetGetfull()
				sum += currAvgProto.GetSum()
				count += currAvgProto.GetNAdds()
				nEntries++
			} else {
				//fmt.Printf("[Q22]Cust proto: %+v\n", natProtos)
				natIds[i] = string(natKeys[j])
				natCustMaps[i] = natProtos.GetValue().GetMap()
				i++
			}
		}
	}
	//fmt.Printf("[Q22]CustMapProtos: %+v\n", natCustMaps)
	avg, currCustBalance, sumB, nCust := int32(sum/count), int32(0), int32(0), int32(0)
	var currNatEntries, currCustEntries []*proto.ApbMapEntry
	sumBalances, sumCusts := make([]int32, 8), make([]int32, 8)
	i = 0
	for _, custMap := range natCustMaps {
		currNatEntries = custMap.GetEntries() //One entry per customer of the currrent nation
		nEntries += len(currNatEntries)
		for _, custFullEntry := range currNatEntries {
			//fmt.Printf("[Q22]CustEntry: %+v\n", custFullEntry)
			currCustEntries = custFullEntry.GetValue().GetMap().GetEntries()
			//fmt.Printf("[Q22]CurrCustEntries: %+v\n", currCustEntries)
			if string(currCustEntries[0].GetKey().GetKey()) == "A" {
				currCustBalance = currCustEntries[0].GetValue().GetCounter().GetValue()
			} else {
				currCustBalance = currCustEntries[1].GetValue().GetCounter().GetValue()
			}
			if currCustBalance > avg {
				sumB += currCustBalance
				nCust++
			}
		}
		sumBalances[i], sumCusts[i] = sumB, nCust
		sumB, nCust, i = 0, 0, i+1
		/*currNatEntries = custMap.GetEntries()[0].GetValue().GetPartread().GetMap().GetGetvalues()
		currNatKeys, currNatValues := currNatEntries.GetKeys(), currNatEntries.GetValues()
		for i, natValue := range currNatValues {
			//currCustEntries = natValue.GetValue().GetMap().GetEntries() //TODO: Continue fom here
			currCustEntries = natValue.GetValue().GetPartread().GetMap().GetGetvalues()
			fmt.Printf("CurrCustEntries: %+v\n", currCustEntries)

				if string(currCustEntries[0].GetKey().GetKey()) == "A" {
					currCustBalance = currCustEntries[0].GetValue().GetCounter().GetValue()
				} else {
					currCustBalance = currCustEntries[1].GetValue().GetCounter().GetValue()
				}
				if currCustBalance > avg {
					sumB += currCustBalance
					nCust++
				}

			ignore(avg, currCustBalance, currNatKeys, i)
		}
		sumBalances[i], sumCusts[i] = sumB, nCust
		sumB, nCust, i = 0, 0, i+1*/
	}

	if PRINT_QUERY {
		fmt.Println("[ClientQuery]Q22 query: number of customers of countries with a given phone code that have not placed any order and have above average balance.")
		for i, sumB := range sumBalances {
			fmt.Printf("%s: %d, %d\n", natIds[i], sumCusts[i], sumB)
		}
	}
	*client.nEntries += nEntries
}

func sendQ22(client QueryClient) (nRequests int) {
	readParams := make([]crdt.ReadObjectParams, 2)
	myRegion, nextRegion, myRegRead, otherRegRead := getQ22ReadsHelper(client)
	var replyProtos []*proto.ApbReadObjectResp
	if isIndexGlobal {
		readParams[0] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q22_KEY + strconv.Itoa(myRegion), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}, ReadArgs: myRegRead}
		readParams[1] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q22_KEY + strconv.Itoa(nextRegion), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}, ReadArgs: otherRegRead}
		replyProtos = sendReceiveReadProto(client, nil, readParams, client.indexServer).GetObjects().GetObjects()
	} else {
		readParams[0] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q22_KEY + strconv.Itoa(myRegion), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+myRegion]}, ReadArgs: myRegRead}
		readParams[1] = crdt.ReadObjectParams{KeyParams: crdt.KeyParams{Key: Q22_KEY + strconv.Itoa(nextRegion), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+nextRegion]}, ReadArgs: otherRegRead}
		if LOCAL_DIRECT {
			replyProtos = sendReceiveReadProto(client, nil, readParams[:1], client.indexServer).GetObjects().GetObjects()
			replyProtos = append(replyProtos, sendReceiveReadProto(client, nil, readParams[1:], ((client.indexServer + 1) % len(tpchData.Tables.Regions))).GetObjects().GetObjects()[0])
		} else {
			replyProtos = sendReceiveReadProto(client, nil, readParams, client.indexServer).GetObjects().GetObjects()
		}
	}

	if PRINT_QUERY {
		fmt.Println("[ClientQuery][RRMap]Q22 query: number of customers of countries with given phone codes that have not placed any order and have above average balance.")
	}
	processQ22ReplyProto(replyProtos, client)
	return 16
}

/*
func getQ22Reads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	minAvgBal, rndNations := getQ22ReadsGlobalHelper(client)
	custCompArgs := map[string]crdt.CompareArguments{"C": &crdt.IntCompareArguments{Value: 0, CompType: crdt.EQ}, "A": &crdt.IntCompareArguments{Value: int64(minAvgBal), CompType: crdt.H}}
	var keyP crdt.KeyParams
	offset := bufI[1]
	for natId := range rndNations {
		keyP = crdt.KeyParams{Key: Q22_KEY + strconv.FormatInt(natId, 10), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}
		partialR[offset] = crdt.ReadObjectParams{KeyParams: keyP, ReadArgs: crdt.EmbMapConditionalReadAllArguments{CompareArguments: &crdt.MapCompareArguments{MapArgs: custCompArgs}}}
		partialR[offset+1] = crdt.ReadObjectParams{KeyParams: keyP, ReadArgs: crdt.EmbMapPartialArguments{Args: map[string]crdt.ReadArguments{Q22_AVG: crdt.AvgGetFullArguments{}}}}
		offset += 2
	}
	bufI[1] = offset

	if PRINT_QUERY {
		fmt.Printf("[ClientQuery][RRMap]Q22 query: number of customers of countries with phone code %v that have not placed any order and have above average balance.\n", rndNations)
	}
	return reads + 16
}

func getQ22ReadsGlobalHelper(client QueryClient) (minAvgBal float64, rndNations map[int64]struct{}) {
	rndNations, minAvgBal = make(map[int64]struct{}), 100000000.0
	nNations, currAvgBal, rndN := int64(len(tpchData.Tables.Nations)), 0.0, int64(-1)
	var pair PairIntFloat
	for len(rndNations) < 8 {
		rndN = client.rng.Int63n(nNations) + 10
		rndNations[rndN], pair = struct{}{}, q22CustNatBalances[int8(rndN)]
		currAvgBal = pair.first / float64(pair.second)
		if currAvgBal < minAvgBal {
			minAvgBal = currAvgBal
		}
	}
	return
}

func getQ22LocalDirectReads(client QueryClient, fullR, partialR [][]crdt.ReadObjectParams, bufI [][]int, rngRegions []int, rngRegionsI, reads int) (newReads, rngRegion int) {
	minAvgBal, myRegion, nextRegion, rndNationsP1, rndNationsP2 := getQ22LocalDirectReadsHelper(client)

	custCompArgs := map[string]crdt.CompareArguments{"C": &crdt.IntCompareArguments{Value: 0, CompType: crdt.EQ}, "A": &crdt.IntCompareArguments{Value: int64(minAvgBal), CompType: crdt.H}}
	var keyP crdt.KeyParams
	regPartialR, offset := partialR[myRegion], bufI[myRegion][1]
	for _, natId := range rndNationsP1 {
		keyP = crdt.KeyParams{Key: Q22_KEY + strconv.FormatInt(int64(natId), 10), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+myRegion]}
		regPartialR[offset] = crdt.ReadObjectParams{KeyParams: keyP, ReadArgs: crdt.EmbMapConditionalReadAllArguments{CompareArguments: &crdt.MapCompareArguments{MapArgs: custCompArgs}}}
		regPartialR[offset+1] = crdt.ReadObjectParams{KeyParams: keyP, ReadArgs: crdt.EmbMapPartialArguments{Args: map[string]crdt.ReadArguments{Q22_AVG: crdt.AvgGetFullArguments{}}}}
		offset += 2
	}
	bufI[myRegion][1] = offset
	regPartialR, offset = partialR[nextRegion], bufI[nextRegion][1]
	for natId := range rndNationsP2 {
		keyP = crdt.KeyParams{Key: Q22_KEY + strconv.FormatInt(int64(natId), 10), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+nextRegion]}
		regPartialR[offset] = crdt.ReadObjectParams{KeyParams: keyP, ReadArgs: crdt.EmbMapConditionalReadAllArguments{CompareArguments: &crdt.MapCompareArguments{MapArgs: custCompArgs}}}
		regPartialR[offset+1] = crdt.ReadObjectParams{KeyParams: keyP, ReadArgs: crdt.EmbMapPartialArguments{Args: map[string]crdt.ReadArguments{Q22_AVG: crdt.AvgGetFullArguments{}}}}
		offset += 2
	}
	bufI[nextRegion][1] = offset
	rngRegions[rngRegionsI] = myRegion
	return reads + 16, myRegion
}

func getQ22LocalDirectReadsHelper(client QueryClient) (minAvgBal float64, myRegion, nextRegion int, rndNationsP1 []int8, rndNationsP2 map[int8]struct{}) {
	myRegion = client.getRngRegion()
	rndNationsP2 = make(map[int8]struct{})
	rndNationsP1 = tpchData.Tables.NationsByRegion[myRegion] //Each region has exactly 5 nations.

	nextRegion = (myRegion + 1) % len(tpchData.Tables.NationsByRegion)
	otherRegionNations := tpchData.Tables.NationsByRegion[nextRegion]
	//Request the remaining from the "next" region
	for len(rndNationsP2) < 3 {
		rndNationsP2[otherRegionNations[client.rng.Intn(len(otherRegionNations))]+10] = struct{}{}
	}
	minAvgBal = 100000000.0
	currAvgBal := 0.0
	var pair PairIntFloat
	for _, nat := range rndNationsP1 {
		pair = q22CustNatBalances[nat]
		currAvgBal = pair.first / float64(pair.second)
		if currAvgBal < minAvgBal {
			minAvgBal = currAvgBal
		}
	}
	for nat := range rndNationsP2 {
		pair = q22CustNatBalances[nat]
		currAvgBal = pair.first / float64(pair.second)
		if currAvgBal < minAvgBal {
			minAvgBal = currAvgBal
		}
	}
	return
}

func getQ22LocalServerReadsHelper(client QueryClient) (minAvgBal float64, myRegion, nextRegion int8, rndNations map[int8]int8) {
	rndNations, minAvgBal, myRegion = make(map[int8]int8), 100000000.0, int8(client.getRngRegion())
	myRegionNations := tpchData.Tables.NationsByRegion[myRegion] //Each region has exactly 5 nations.
	nextRegion = (myRegion + 1) % int8(len(tpchData.Tables.NationsByRegion))
	otherRegionNations := tpchData.Tables.NationsByRegion[nextRegion]

	//rndNations: Nat -> RegId
	for _, nat := range myRegionNations {
		rndNations[nat+10] = myRegion
	}
	for len(rndNations) < 8 {
		rndNations[otherRegionNations[client.rng.Intn(len(otherRegionNations))]+10] = nextRegion
	}
	currAvgBal := 0.0
	var pair PairIntFloat
	for nat := range rndNations {
		pair = q22CustNatBalances[nat]
		currAvgBal = pair.first / float64(pair.second)
		if currAvgBal < minAvgBal {
			minAvgBal = currAvgBal
		}
	}
	return
}

func getQ22LocalServerReads(client QueryClient, fullR, partialR []crdt.ReadObjectParams, bufI []int, reads int) (newReads int) {
	minAvgBal, _, _, rndNations := getQ22LocalServerReadsHelper(client)

	custCompArgs := map[string]crdt.CompareArguments{"C": &crdt.IntCompareArguments{Value: 0, CompType: crdt.EQ}, "A": &crdt.IntCompareArguments{Value: int64(minAvgBal), CompType: crdt.H}}
	var keyP crdt.KeyParams
	offset := bufI[1]
	for natId, regId := range rndNations {
		keyP = crdt.KeyParams{Key: Q22_KEY + strconv.FormatInt(int64(natId), 10), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+regId]}
		partialR[offset] = crdt.ReadObjectParams{KeyParams: keyP, ReadArgs: crdt.EmbMapConditionalReadAllArguments{CompareArguments: &crdt.MapCompareArguments{MapArgs: custCompArgs}}}
		partialR[offset+1] = crdt.ReadObjectParams{KeyParams: keyP, ReadArgs: crdt.EmbMapPartialArguments{Args: map[string]crdt.ReadArguments{Q22_AVG: crdt.AvgGetFullArguments{}}}}
		offset += 2
	}
	bufI[1] = offset
	return reads + 16
}

func processQ22Reply(client QueryClient, replies []*proto.ApbReadObjectResp, bufI []int, reads int) (newReads int) {
	processQ22ReplyProto(replies[bufI[0] : bufI[0]+16])
	bufI[0] += 16
	return reads + 16
}

func processQ22LocalDirectReply(client QueryClient, replies [][]*proto.ApbReadObjectResp, bufI [][]int, rngRegions []int, rngRegionsI int, reads int) (newReads int) {
	index := rngRegions[rngRegionsI]
	nextIndex := (index + 1) % len(tpchData.Tables.Regions)
	//concReplies := replies[index][bufI[index][0]:bufI[index][0]+10] + replies[nextIndex][bufI[nextIndex][0]:bufI[nextIndex][0]+6]
	concReplies := make([]*proto.ApbReadObjectResp, 16)
	myRegReplies, otherRegReplies := replies[index][bufI[index][0]:bufI[index][0]+10], replies[nextIndex][bufI[nextIndex][0]:bufI[nextIndex][0]+6]
	for i, reply := range myRegReplies {
		concReplies[i] = reply
	}
	for i, reply := range otherRegReplies {
		concReplies[i+10] = reply
	}
	processQ22ReplyProto(concReplies)
	bufI[index][0] += 10
	bufI[nextIndex][1] += 6
	return reads + 16
}

func processQ22ReplyProto(protos []*proto.ApbReadObjectResp) {
	//Pair indexes have maps; odd have averages.
	//Note: For fairness, processing is still done even when not printing.
	sum, count := int64(0), int64(0)
	var currAvgProto *proto.ApbAvgGetFullReadResp
	for i := 1; i < len(protos); i += 2 {
		currAvgProto = protos[i].GetPartread().GetAvg().GetGetfull()
		sum += currAvgProto.GetSum()
		count += currAvgProto.GetNAdds()
	}
	avg := int32(sum / count)
	var currEntries []*proto.ApbMapEntry
	var currEntryEntries []*proto.ApbMapEntry
	sumB, nCust, currCustBalance := int32(0), int32(0), int32(0)
	sumBalances, sumCusts := make([]int32, 8), make([]int32, 8)
	for i := 0; i < len(protos); i += 2 {
		currEntries = protos[i].GetMap().GetEntries()
		for _, entry := range currEntries {
			currEntryEntries = entry.GetValue().GetMap().GetEntries()
			if string(currEntryEntries[0].GetKey().GetKey()) == "A" {
				currCustBalance = currEntryEntries[0].GetValue().GetCounter().GetValue()
			} else {
				currCustBalance = currEntryEntries[1].GetValue().GetCounter().GetValue()
			}
			if currCustBalance > avg {
				sumB += currCustBalance
				nCust++
			}
		}
		sumBalances[i/2], sumCusts[i/2] = sumB, nCust
	}

	if PRINT_QUERY {
		fmt.Println("[ClientQuery]Q22 query: number of customers of countries with a given phone code that have not placed any order and have above average balance.")
		for i, sumB := range sumBalances {
			fmt.Printf("%d: %d\n", sumCusts[i], sumB)
		}
	}
}

func sendQ22(client QueryClient) (nRequests int) {
	readParams := make([]crdt.ReadObjectParams, 16)
	var keyP crdt.KeyParams
	i := 0
	var replyProtos []*proto.ApbReadObjectResp
	//Have to separate between local and global :(
	if isIndexGlobal {
		minAvgBal, rndNations := getQ22ReadsGlobalHelper(client)
		custCompArgs := map[string]crdt.CompareArguments{"C": &crdt.IntCompareArguments{Value: 0, CompType: crdt.EQ}, "A": &crdt.IntCompareArguments{Value: int64(minAvgBal), CompType: crdt.H}}
		for natId := range rndNations {
			keyP = crdt.KeyParams{Key: Q22_KEY + strconv.FormatInt(natId, 10), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT]}
			readParams[i] = crdt.ReadObjectParams{KeyParams: keyP, ReadArgs: crdt.EmbMapConditionalReadAllArguments{CompareArguments: &crdt.MapCompareArguments{MapArgs: custCompArgs}}}
			readParams[i+1] = crdt.ReadObjectParams{KeyParams: keyP, ReadArgs: crdt.EmbMapPartialArguments{Args: map[string]crdt.ReadArguments{Q22_AVG: crdt.AvgGetFullArguments{}}}}
			i += 2
		}
		replyProtos = sendReceiveReadProto(client, nil, readParams, client.indexServer).GetObjects().GetObjects()
	} else if localMode == LOCAL_DIRECT {
		minAvgBal, myRegion, nextRegion, rndNationsP1, rndNationsP2 := getQ22LocalDirectReadsHelper(client)
		custCompArgs := map[string]crdt.CompareArguments{"C": &crdt.IntCompareArguments{Value: 0, CompType: crdt.EQ}, "A": &crdt.IntCompareArguments{Value: int64(minAvgBal), CompType: crdt.H}}
		for _, natId := range rndNationsP1 {
			keyP = crdt.KeyParams{Key: Q22_KEY + strconv.FormatInt(int64(natId), 10), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+myRegion]}
			readParams[i] = crdt.ReadObjectParams{KeyParams: keyP, ReadArgs: crdt.EmbMapConditionalReadAllArguments{CompareArguments: &crdt.MapCompareArguments{MapArgs: custCompArgs}}}
			readParams[i+1] = crdt.ReadObjectParams{KeyParams: keyP, ReadArgs: crdt.EmbMapPartialArguments{Args: map[string]crdt.ReadArguments{Q22_AVG: crdt.AvgGetFullArguments{}}}}
			i += 2
		}
		for natId := range rndNationsP2 {
			keyP = crdt.KeyParams{Key: Q22_KEY + strconv.FormatInt(int64(natId), 10), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+nextRegion]}
			readParams[i] = crdt.ReadObjectParams{KeyParams: keyP, ReadArgs: crdt.EmbMapConditionalReadAllArguments{CompareArguments: &crdt.MapCompareArguments{MapArgs: custCompArgs}}}
			readParams[i+1] = crdt.ReadObjectParams{KeyParams: keyP, ReadArgs: crdt.EmbMapPartialArguments{Args: map[string]crdt.ReadArguments{Q22_AVG: crdt.AvgGetFullArguments{}}}}
			i += 2
		}
		myRegReply := sendReceiveReadProto(client, nil, readParams[:10], client.indexServer).GetObjects().GetObjects()
		otherRegReply := sendReceiveReadProto(client, nil, readParams[10:16], (client.indexServer + 1%len(tpchData.Tables.Regions))).GetObjects().GetObjects()
		replyProtos = make([]*proto.ApbReadObjectResp, 16)
		for j, reply := range myRegReply {
			replyProtos[j] = reply
		}
		for j, reply := range otherRegReply {
			replyProtos[j+10] = reply
		}
	} else {
		minAvgBal, _, _, rndNations := getQ22LocalServerReadsHelper(client)
		custCompArgs := map[string]crdt.CompareArguments{"C": &crdt.IntCompareArguments{Value: 0, CompType: crdt.EQ}, "A": &crdt.IntCompareArguments{Value: int64(minAvgBal), CompType: crdt.H}}
		for natId, regId := range rndNations {
			keyP = crdt.KeyParams{Key: Q22_KEY + strconv.FormatInt(int64(natId), 10), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[INDEX_BKT+regId]}
			readParams[i] = crdt.ReadObjectParams{KeyParams: keyP, ReadArgs: crdt.EmbMapConditionalReadAllArguments{CompareArguments: &crdt.MapCompareArguments{MapArgs: custCompArgs}}}
			readParams[i+1] = crdt.ReadObjectParams{KeyParams: keyP, ReadArgs: crdt.EmbMapPartialArguments{Args: map[string]crdt.ReadArguments{Q22_AVG: crdt.AvgGetFullArguments{}}}}
			i += 2
		}
		replyProtos = sendReceiveReadProto(client, nil, readParams, client.indexServer).GetObjects().GetObjects()
	}

	if PRINT_QUERY {
		fmt.Println("[ClientQuery][RRMap]Q22 query: number of customers of countries with given phone codes that have not placed any order and have above average balance.")
	}
	processQ22ReplyProto(replyProtos)
	return 16
}
*/

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
		readParam := []crdt.ReadObjectParams{getMapReadParams(tableName, idsArgs, client.indexServer)}
		reply := sendReceiveReadProto(client, nil, readParam, client.indexServer)
		states = mergeMapReplies(map[int8]*proto.ApbStaticReadObjectsResp{0: reply})
	}
	return
}

func getMapReadParams(tableIndex int, innerArgs map[string]crdt.ReadArguments, bktI int) (readParams crdt.ReadObjectParams) {
	return crdt.ReadObjectParams{
		KeyParams: crdt.KeyParams{Key: tpch.TableNames[tableIndex], CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bktI]},
		ReadArgs:  crdt.EmbMapPartialArguments{Args: innerArgs},
	}

}

func getReadArgsPerBucket(tableIndex int) (readParams map[int8]crdt.ReadObjectParams, args map[int8]map[string]crdt.ReadArguments) {
	args = make(map[int8]map[string]crdt.ReadArguments)
	var i, regionLen int8 = 0, int8(len(tpchData.Tables.Regions))
	readParams = make(map[int8]crdt.ReadObjectParams)
	for ; i < regionLen; i++ {
		args[i] = make(map[string]crdt.ReadArguments)
		readParams[i] = getMapReadParams(tableIndex, args[i], int(i))
	}
	return
}

func sendReceiveReadObjsProto(client QueryClient, fullReads []crdt.ReadObjectParams, nConn int) (replyProto *proto.ApbStaticReadObjectsResp) {
	antidote.SendProto(antidote.StaticReadObjs, antidote.CreateStaticReadObjs(nil, fullReads), client.serverConns[nConn])
	_, tmpProto, _ := antidote.ReceiveProto(client.serverConns[nConn])
	return tmpProto.(*proto.ApbStaticReadObjectsResp)
}

func sendReceiveReadProto(client QueryClient, fullReads []crdt.ReadObjectParams, partReads []crdt.ReadObjectParams, nConn int) (replyProto *proto.ApbStaticReadObjectsResp) {
	antidote.SendProto(antidote.StaticRead, antidote.CreateStaticRead(nil, fullReads, partReads), client.serverConns[nConn])
	_, tmpProto, _ := antidote.ReceiveProto(client.serverConns[nConn])
	//fmt.Printf("[Queries][SendReceiveReadProto]Proto: %+v\n", tmpProto)
	return tmpProto.(*proto.ApbStaticReadObjectsResp)
}

// TODO: Delete?
func sendReceiveReadProtoNoProcess(client QueryClient, fullReads []crdt.ReadObjectParams, partReads []crdt.ReadObjectParams, nConn int) {
	antidote.SendProto(antidote.StaticRead, antidote.CreateStaticRead(nil, fullReads, partReads), client.serverConns[nConn])
	antidote.ReceiveProtoNoProcess(client.serverConns[nConn])
}

// Note: assumes len(fullReads) == len(partReads), and that each entry matches one connection in client.
func sendReceiveMultipleReadProtos(client QueryClient, fullReads [][]crdt.ReadObjectParams, partReads [][]crdt.ReadObjectParams) (replyProtos []*proto.ApbStaticReadObjectsResp) {
	var partR []crdt.ReadObjectParams
	replyProtos = make([]*proto.ApbStaticReadObjectsResp, len(fullReads))
	for i, fullR := range fullReads {
		partR = partReads[i]
		if len(fullR) > 0 || len(partR) > 0 {
			antidote.SendProto(antidote.StaticRead, antidote.CreateStaticRead(nil, fullR, partR), client.serverConns[i])
		}
	}
	for i, fullR := range fullReads {
		if len(fullR) > 0 || len(partReads[i]) > 0 {
			_, reply, _ := antidote.ReceiveProto(client.serverConns[i])
			replyProtos[i] = reply.(*proto.ApbStaticReadObjectsResp)
		}
	}
	return
}

/*
func sendReceiveMultipleReads(client QueryClient, reads []crdt.ReadObjectParams, nConn int) (replyProto *proto.ApbStaticReadObjectsResp) {
	antidote.SendProto(antidote.StaticRead, antidote.CreateStaticRead(nil, nil, reads), client.serverConns[nConn])
	_, tmpProto, _ := antidote.ReceiveProto(client.serverConns[nConn])
	return tmpProto.(*proto.ApbStaticReadObjectsResp)
}
*/

// Note: assumes only partial reads, for simplicity
func sendReceiveBucketReadProtos(client QueryClient, partReads map[int8]crdt.ReadObjectParams) (replyProtos map[int8]*proto.ApbStaticReadObjectsResp) {
	servers := make(map[int8]struct{})
	fullR := []crdt.ReadObjectParams{}
	for i, partR := range partReads {
		//fmt.Println("Sending part read to", i, partR)
		antidote.SendProto(antidote.StaticRead, antidote.CreateStaticRead(nil, fullR, []crdt.ReadObjectParams{partR}), client.serverConns[i])
		servers[i] = struct{}{}
	}
	return receiveBucketReadReplies(client, servers)
}

func sendReceiveBucketFullReadProtos(client QueryClient, fullReads map[int8][]crdt.ReadObjectParams) (replyProtos map[int8]*proto.ApbStaticReadObjectsResp) {
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
	for i := range reads {
		//fmt.Println("Receiving reply from", i)
		_, replyProto, _ := antidote.ReceiveProto(client.serverConns[i])
		replyProtos[i] = replyProto.(*proto.ApbStaticReadObjectsResp)
		//fmt.Println("Reply received:", replyProtos[i].GetObjects().GetObjects())
	}
	return
}

// Could be more generic as long as it is reasonable to return state instead of crdt.EmbMapEntryState
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

// Same as mergeMapReplies, but assumes that multiple embMap objects may be in readResp. Assumes that all replies have the same number of objects.
func mergeMultipleMapReplies(replies map[int8]*proto.ApbStaticReadObjectsResp, readType proto.READType) (merged []map[string]crdt.EmbMapEntryState) {
	//fmt.Println("Received replies...")
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

func sendReceiveIndexQuery(client QueryClient, fullReads, partReads []crdt.ReadObjectParams) (replies []*proto.ApbStaticReadObjectsResp) {
	if isIndexGlobal {
		if len(partReads) == 0 {
			return []*proto.ApbStaticReadObjectsResp{sendReceiveReadObjsProto(client, fullReads, client.indexServer)}
		} else {
			return []*proto.ApbStaticReadObjectsResp{sendReceiveReadProto(client, fullReads, partReads, client.indexServer)}
		}
	} else if localMode == LOCAL_DIRECT {
		nRegions := len(tpchData.Tables.Regions)
		var currFull, currPart []crdt.ReadObjectParams
		for i := 0; i < nRegions; i++ {
			currFull = make([]crdt.ReadObjectParams, len(fullReads))
			for j, param := range fullReads {
				currFull[j] = crdt.ReadObjectParams{
					KeyParams: crdt.KeyParams{Key: param.Key, CrdtType: param.CrdtType, Bucket: buckets[INDEX_BKT+i]},
				}
			}
			currPart = make([]crdt.ReadObjectParams, len(partReads))
			for j, param := range partReads {
				currPart[j] = crdt.ReadObjectParams{
					KeyParams: crdt.KeyParams{Key: param.Key, CrdtType: param.CrdtType, Bucket: buckets[INDEX_BKT+i]},
					ReadArgs:  param.ReadArgs,
				}
			}
			//fmt.Println("[SendReceiveIndexQuery]Read:", currPart)
			if len(partReads) == 0 {
				antidote.SendProto(antidote.StaticReadObjs, antidote.CreateStaticReadObjs(nil, currFull), client.serverConns[i])
			} else {
				antidote.SendProto(antidote.StaticRead, antidote.CreateStaticRead(nil, currFull, currPart), client.serverConns[i])
			}
		}

		replyProtos := make([]*proto.ApbStaticReadObjectsResp, nRegions)
		var tmpProto pb.Message
		for i := 0; i < nRegions; i++ {
			_, tmpProto, _ = antidote.ReceiveProto(client.serverConns[i])
			replyProtos[i] = tmpProto.(*proto.ApbStaticReadObjectsResp)
		}
		return replyProtos
	} else {
		//Still need to prepare multiple reads, but all for the same server
		nRegions := len(tpchData.Tables.Regions)
		fullReadS := make([]crdt.ReadObjectParams, nRegions*len(fullReads))
		partReadS := make([]crdt.ReadObjectParams, nRegions*len(partReads))
		fullReadI, partReadI := 0, 0

		for _, param := range fullReads {
			for j := 0; j < nRegions; j, fullReadI = j+1, fullReadI+1 {
				fullReadS[fullReadI] = crdt.ReadObjectParams{
					KeyParams: crdt.KeyParams{Key: param.Key, CrdtType: param.CrdtType, Bucket: buckets[INDEX_BKT+j]},
				}
			}
		}
		for _, param := range partReads {
			for j := 0; j < nRegions; j, partReadI = j+1, partReadI+1 {
				partReadS[partReadI] = crdt.ReadObjectParams{
					KeyParams: crdt.KeyParams{Key: param.Key, CrdtType: param.CrdtType, Bucket: buckets[INDEX_BKT+j]},
					ReadArgs:  param.ReadArgs,
				}
			}
		}
		if len(partReads) == 0 {
			antidote.SendProto(antidote.StaticReadObjs, antidote.CreateStaticReadObjs(nil, fullReadS), client.serverConns[client.indexServer])
		} else {
			antidote.SendProto(antidote.StaticRead, antidote.CreateStaticRead(nil, fullReadS, partReadS), client.serverConns[client.indexServer])
		}

		_, tmpProto, _ := antidote.ReceiveProto(client.serverConns[client.indexServer])
		return []*proto.ApbStaticReadObjectsResp{tmpProto.(*proto.ApbStaticReadObjectsResp)}
	}
}

// TODO: Probably change this to use an array instead for the functions
func mergeIndex(protos []*proto.ApbStaticReadObjectsResp, queryN int) []*proto.ApbReadObjectResp {
	if isIndexGlobal {
		//No merging to be done
		return protos[0].Objects.Objects
	}
	switch queryN {
	case 1:
		return mergeQ1IndexReply(getObjectsFromStaticReadResp(protos))
	case 3:
		return mergeQ3IndexReply(getObjectsFromStaticReadResp(protos))
	case 4:
		return mergeQ4IndexReply(getObjectsFromStaticReadResp(protos))
	case 6:
		return mergeQ6IndexReply(getObjectsFromStaticReadResp(protos))
	case 9:
		return mergeQ9IndexReply(getObjectsFromStaticReadResp(protos))
	case 10:
		return mergeQ10IndexReply(getObjectsFromStaticReadResp(protos))
	case 12:
		return mergeQ12IndexReply(getObjectsFromStaticReadResp(protos))
	case 13:
		return mergeQ13IndexReply(getObjectsFromStaticReadResp(protos))
	case 14:
		return mergeQ14IndexReply(getObjectsFromStaticReadResp(protos))
	case 15:
		return mergeQ15IndexReply(getObjectsFromStaticReadResp(protos))
	case 16:
		return mergeQ16IndexReply(getObjectsFromStaticReadResp(protos))
	case 17:
		return mergeQ17IndexReply(getObjectsFromStaticReadResp(protos))
	case 18:
		return mergeQ18IndexReply(getObjectsFromStaticReadResp(protos))
	case 19:
		return mergeQ19IndexReply(getObjectsFromStaticReadResp(protos))
	default:
		return nil
	}
}

//Q5 doesn't need merge as it only needs to ask one region (i.e., one replica)
//Q11 also doesn't need merge as it is single nation.
//Q18 needs merge as it is the top 100 of all customers.

// Q1 is an embedded map with counters and averages. Furthermore, each server replies with two maps. Need to merge all of them together
func mergeQ1IndexReply(protoObjs []*proto.ApbReadObjectResp) (merged []*proto.ApbReadObjectResp) {
	//This function is already prepared to merge any amount of embedded maps
	//fmt.Printf("[Queries][MergeQ1IndexReply]ProtoObjs: %+v\n", protoObjs)
	//fmt.Println("[Queries][MergeQ1IndexReply]Len of protoObjs:", len(protoObjs))

	/*dayOne := []*proto.ApbReadObjectResp{protoObjs[0], protoObjs[2], protoObjs[4], protoObjs[6], protoObjs[8]}
	dayTwo := []*proto.ApbReadObjectResp{protoObjs[1], protoObjs[3], protoObjs[5], protoObjs[7], protoObjs[9]}

	return []*proto.ApbReadObjectResp{mergeEmbMapProtosPartRead(dayOne)[0], mergeEmbMapProtosPartRead(dayTwo)[0]}*/
	return mergeEmbMapProtosPartRead(protoObjs)
}

func mergeQ4IndexReply(protoObjs []*proto.ApbReadObjectResp) (merged []*proto.ApbReadObjectResp) {
	return mergeEmbMapProtosFullRead(protoObjs)
}

func mergeQ6IndexReply(protoObjs []*proto.ApbReadObjectResp) (merged []*proto.ApbReadObjectResp) {
	return mergeEmbMapProtosPartRead(protoObjs)
}

func mergeQ9IndexReply(protoObjs []*proto.ApbReadObjectResp) (merged []*proto.ApbReadObjectResp) {
	/*//Cannot use mergeEmbMapProtosFullRead as we want to group different keys (nations) under one CRDT
	innerProto := make([]*proto.ApbMapEntry, len(tpchData.Tables.Nations))
	var currProtos []*proto.ApbMapEntry
	i := 0
	for _, obj := range protoObjs {
		currProtos = obj.GetMap().GetEntries()
		//fmt.Println("mergeQ9IndexReply, size currProtos:", len(currProtos))
		//fmt.Println()
		for j, currProto := range currProtos {
			innerProto[i+j] = currProto
		}
		i += len(currProtos)
	}
	return []*proto.ApbReadObjectResp{{Map: &proto.ApbGetMapResp{Entries: innerProto}}}*/
	innerProto := make([]*proto.ApbMapEntry, len(tpchData.Tables.Nations))
	var currProtos []*proto.ApbMapEntry
	i := 0
	for _, obj := range protoObjs {
		currProtos = obj.GetPartread().GetMap().GetGetvalue().GetValue().GetMap().GetEntries()
		for j, currProto := range currProtos {
			innerProto[i+j] = currProto
		}
		i += len(currProtos)
	}
	readType, crdtType := proto.READType_GET_VALUE, proto.CRDTType_RRMAP
	return []*proto.ApbReadObjectResp{{Partread: &proto.ApbPartialReadResp{Map: &proto.ApbMapPartialReadResp{
		Getvalue: &proto.ApbMapGetValueResp{Parttype: &readType, Crdttype: &crdtType, Value: &proto.ApbReadObjectResp{Map: &proto.ApbGetMapResp{Entries: innerProto}}}}}}}
}

func mergeQ10IndexReply(protoObjs []*proto.ApbReadObjectResp) (merged []*proto.ApbReadObjectResp) {
	return mergeTopKProtos(protoObjs, 20)
}

func mergeQ12IndexReply(protoObjs []*proto.ApbReadObjectResp) (merged []*proto.ApbReadObjectResp) {
	return mergeEmbMapProtosPartRead(protoObjs)
}

func mergeQ13IndexReply(protoObjs []*proto.ApbReadObjectResp) (merged []*proto.ApbReadObjectResp) {
	//allObjs := []*proto.ApbReadObjectResp{protoObjs[0], protoObjs[2], protoObjs[4], protoObjs[6], protoObjs[8]}
	//wordObjs := []*proto.ApbReadObjectResp{protoObjs[1], protoObjs[3], protoObjs[5], protoObjs[7], protoObjs[9]}
	//fmt.Printf("[ClientQuery][MergeQ13]%+v. Length: %d\n", allObjs, len(allObjs))
	//fmt.Printf("[ClientQuery][MergeQ13]%+v. Length: %d\n", wordObjs, len(wordObjs))
	//return []*proto.ApbReadObjectResp{mergeEmbMapProtosFullRead(allObjs)[0], mergeEmbMapProtosFullRead(wordObjs)[0]}

	//return mergeEmbMapProtosPartRead(protoObjs)

	//Custom merge as we use the read GetValue() and the inside map is not a partial read.
	mergedCounters := make(map[string]int32)
	for _, protoObj := range protoObjs {
		entries := protoObj.GetPartread().GetMap().GetGetvalue().GetValue().GetMap().GetEntries()
		for _, entry := range entries {
			mergedCounters[string(entry.GetKey().GetKey())] += entry.GetValue().GetCounter().GetValue()
		}
	}
	//fmt.Println(mergedCounters)
	crdtType, mergedEntries, i := proto.CRDTType_COUNTER, make([]*proto.ApbMapEntry, len(mergedCounters)), 0
	for key, count := range mergedCounters {
		mergedEntries[i] = &proto.ApbMapEntry{Key: &proto.ApbMapKey{Key: []byte(key), Type: &crdtType}, Value: &proto.ApbReadObjectResp{Counter: &proto.ApbGetCounterResp{Value: pb.Int32(count)}}}
		i++
	}
	mapCrdtType, mapReadType := proto.CRDTType_RRMAP, proto.READType_GET_VALUE
	return []*proto.ApbReadObjectResp{{Partread: &proto.ApbPartialReadResp{Map: &proto.ApbMapPartialReadResp{Getvalue: &proto.ApbMapGetValueResp{Crdttype: &mapCrdtType, Parttype: &mapReadType, Value: &proto.ApbReadObjectResp{Map: &proto.ApbGetMapResp{Entries: mergedEntries}}}}}}}
}

func mergeQ16IndexReply(protoObjs []*proto.ApbReadObjectResp) (merged []*proto.ApbReadObjectResp) {
	return mergeEmbMapProtosPartRead(protoObjs)
}

func mergeQ17IndexReply(protoObjs []*proto.ApbReadObjectResp) (merged []*proto.ApbReadObjectResp) {
	//Custom merge as it is possible for the embedded maps to have different keys in this query
	//This will happen mostly for very low SFs (< 1)
	//This happens as for some combinations of [BRAND][CONTAINER], not all quantities have at least one item.
	/*fmt.Printf("Q17 Pre-merge: %+v\n", protoObjs)
	mergeResult := mergeEmbMapProtosPartRead(protoObjs)
	fmt.Printf("Q17 Merge result: %+v\n", mergeResult)
	*/

	//AVG ("a"), MAP ("m")
	//Then the MAP has subdivisions per quantity.

	avgEntries, mapEntries := make([]*proto.ApbMapGetValueResp, len(protoObjs)), make([]*proto.ApbMapGetValueResp, len(protoObjs))
	for i, protoObj := range protoObjs {
		values := protoObj.GetPartread().GetMap().GetGetvalues().GetValues()
		if values == nil { //This can happen for very low SFs, as some combinations of [BRAND][CONTAINER] may not have any entry.
			return protoObjs
		}
		for _, value := range values {
			if value.GetCrdttype() == proto.CRDTType_AVG {
				avgEntries[i] = value
			} else {
				mapEntries[i] = value
			}
		}
	}

	//Avg entries merging
	sum, avgCount := int64(0), int64(0)
	for _, avgEntry := range avgEntries {
		value := avgEntry.GetValue().GetPartread().GetAvg().GetGetfull()
		sum += *value.Sum
		avgCount += *value.NAdds
	}
	avgType, avgReadType := proto.CRDTType_AVG, proto.READType_GET_FULL_AVG

	//Map entries merging
	mapKeysToValues, floatCounterType, fullReadType := make(map[string][]*proto.ApbMapGetValueResp), proto.CRDTType_COUNTER_FLOAT, proto.READType_FULL
	var stringKey string
	for _, mapEntry := range mapEntries {
		entries := mapEntry.GetValue().GetPartread().GetMap().GetGetvalues()
		keys, values := entries.GetKeys(), entries.GetValues()
		for j, byteKey := range keys {
			stringKey = string(byteKey)
			mapKeysToValues[stringKey] = append(mapKeysToValues[stringKey], values[j])
		}
	}
	mapMergedValues, mapMergedKeys := make([]*proto.ApbMapGetValueResp, len(mapKeysToValues)), make([][]byte, len(mapKeysToValues))
	i := 0
	for keyS, values := range mapKeysToValues {
		mapMergedKeys[i] = []byte(keyS)
		count := 0.0
		for _, value := range values {
			count += value.GetValue().GetCounterfloat().GetValue()
		}
		mapMergedValues[i] = &proto.ApbMapGetValueResp{Value: crdt.CounterFloatState{Value: count}.ToReadResp(), Crdttype: &floatCounterType, Parttype: &fullReadType}
		i++
	}

	//All merging
	mapType, mapReadType := proto.CRDTType_RRMAP, proto.READType_GET_VALUES
	mergedKeys := [][]byte{[]byte(Q17_AVG), []byte(Q17_MAP)}
	mergedValues := []*proto.ApbMapGetValueResp{{Crdttype: &avgType, Parttype: &avgReadType, Value: crdt.AvgFullState{Sum: sum, NAdds: avgCount}.ToReadResp()},
		{Crdttype: &mapType, Parttype: &mapReadType, Value: &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Map: &proto.ApbMapPartialReadResp{
			Getvalues: &proto.ApbMapGetValuesResp{Keys: mapMergedKeys, Values: mapMergedValues}}}}}}

	return []*proto.ApbReadObjectResp{{Partread: &proto.ApbPartialReadResp{
		Map: &proto.ApbMapPartialReadResp{Getvalues: &proto.ApbMapGetValuesResp{Keys: mergedKeys, Values: mergedValues}}}}}
}

func mergeQ19IndexReply(protoObjs []*proto.ApbReadObjectResp) (merged []*proto.ApbReadObjectResp) {
	//3 reads per server.
	//Also custom merge as some protobufs may not have an entry
	/*firstGroup := []*proto.ApbReadObjectResp{protoObjs[0], protoObjs[3], protoObjs[6], protoObjs[9], protoObjs[12]}
	secondGroup := []*proto.ApbReadObjectResp{protoObjs[1], protoObjs[4], protoObjs[7], protoObjs[10], protoObjs[13]}
	thirdGroup := []*proto.ApbReadObjectResp{protoObjs[2], protoObjs[5], protoObjs[8], protoObjs[11], protoObjs[14]}
	merged = make([]*proto.ApbReadObjectResp, 3)
	merged[0], merged[1], merged[2] = mergeQ19IndexReplyHelper(firstGroup), mergeQ19IndexReplyHelper(secondGroup), mergeQ19IndexReplyHelper(thirdGroup)
	return merged
	*/
	var stringKey, contStringKey string
	var mapKeysToValues map[string]float64
	var has bool
	outKeys, outValues := make([][]byte, 3), make([]*proto.ApbMapGetValueResp, 3) //3 containers
	outMapKeysToValues := make(map[string]map[string]float64, 3)                  //3 containers
	for _, protoObj := range protoObjs {
		//Containers -> Brands+quantity -> ...
		contEntries := protoObj.GetPartread().GetMap().GetGetvalues()
		contKeys, contValues := contEntries.GetKeys(), contEntries.GetValues()
		for k, contByteKey := range contKeys {
			contStringKey = string(contByteKey)
			mapKeysToValues, has = outMapKeysToValues[contStringKey]
			if !has {
				mapKeysToValues = make(map[string]float64)
				outMapKeysToValues[contStringKey] = mapKeysToValues
			}
			entries := contValues[k].GetValue().GetPartread().GetMap().GetGetvalues()
			keys, values := entries.GetKeys(), entries.GetValues()
			for j, byteKey := range keys {
				stringKey = string(byteKey)
				mapKeysToValues[stringKey] += values[j].GetValue().GetCounterfloat().GetValue()
			}
		}
	}

	floatCounterType, fullReadType, mapType, getValuesType, i := proto.CRDTType_COUNTER_FLOAT, proto.READType_FULL, proto.CRDTType_RRMAP, proto.READType_GET_VALUES, 0
	for contKey, mapKeysToValues := range outMapKeysToValues {
		mergedKeys, mergedValues := make([][]byte, len(mapKeysToValues)), make([]*proto.ApbMapGetValueResp, len(mapKeysToValues))
		j := 0
		for key, value := range mapKeysToValues {
			mergedKeys[j], mergedValues[j] = []byte(key), &proto.ApbMapGetValueResp{Value: crdt.CounterFloatState{Value: value}.ToReadResp(), Crdttype: &floatCounterType, Parttype: &fullReadType}
			j++
		}
		mapPartResp := &proto.ApbMapPartialReadResp{Getvalues: &proto.ApbMapGetValuesResp{Keys: mergedKeys, Values: mergedValues}}
		outKeys[i], outValues[i] = []byte(contKey), &proto.ApbMapGetValueResp{Value: &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Map: mapPartResp}}, Crdttype: &mapType, Parttype: &getValuesType}
		i++
	}
	return []*proto.ApbReadObjectResp{{Partread: &proto.ApbPartialReadResp{Map: &proto.ApbMapPartialReadResp{Getvalues: &proto.ApbMapGetValuesResp{Keys: outKeys, Values: outValues}}}}}
}

func mergeQ19IndexReplyHelper(protoObjs []*proto.ApbReadObjectResp) (merged *proto.ApbReadObjectResp) {
	//All entries are CounterFloats
	mapKeysToValues := make(map[string]float64)
	var stringKey string
	for _, protoObj := range protoObjs {
		entries := protoObj.GetPartread().GetMap().GetGetvalues()
		keys, values := entries.GetKeys(), entries.GetValues()
		for j, byteKey := range keys {
			stringKey = string(byteKey)
			mapKeysToValues[stringKey] += values[j].GetValue().GetCounterfloat().GetValue()
		}
	}
	mergedKeys, mergedValues := make([][]byte, len(mapKeysToValues)), make([]*proto.ApbMapGetValueResp, len(mapKeysToValues))
	i, floatCounterType, fullReadType := 0, proto.CRDTType_COUNTER_FLOAT, proto.READType_FULL
	for key, value := range mapKeysToValues {
		mergedKeys[i], mergedValues[i] = []byte(key), &proto.ApbMapGetValueResp{Value: crdt.CounterFloatState{Value: value}.ToReadResp(), Crdttype: &floatCounterType, Parttype: &fullReadType}
		i++
	}
	return &proto.ApbReadObjectResp{Partread: &proto.ApbPartialReadResp{Map: &proto.ApbMapPartialReadResp{
		Getvalues: &proto.ApbMapGetValuesResp{Keys: mergedKeys, Values: mergedValues}}}}
}

func mergeQ3IndexReply(protoObjs []*proto.ApbReadObjectResp) (merged []*proto.ApbReadObjectResp) {
	return mergeTopKProtos(protoObjs, 10)
}

// Needs to merge as the info for a date is spread across the CRDTs. A partialRead for AVG is needed in this case.
func mergeQ14IndexReply(protoObjs []*proto.ApbReadObjectResp) (merged []*proto.ApbReadObjectResp) {
	//protoObjs := getObjectsFromStaticReadResp(protos)
	var sum, nAdds int64
	var currProto *proto.ApbAvgGetFullReadResp
	for _, protos := range protoObjs {
		//currProto = protos[0].GetPartread().GetAvg().GetGetfull()
		currProto = protos.GetPartread().GetAvg().GetGetfull()
		sum += currProto.GetSum()
		nAdds += currProto.GetNAdds()
	}
	result := float64(sum) / float64(nAdds)
	return []*proto.ApbReadObjectResp{{Avg: &proto.ApbGetAverageResp{Avg: pb.Float64(result)}}}
}

// Q15 also needs merge.
func mergeQ15IndexReply(protoObjs []*proto.ApbReadObjectResp) (merged []*proto.ApbReadObjectResp) {
	//protoObjs := getObjectsFromStaticReadResp(protos)
	max, amount := int32(-1), 0
	var currPairs []*proto.ApbIntPair
	//Find max and count how many
	for _, topProtos := range protoObjs {
		//currPairs = topProtos[0].GetTopk().GetValues()
		currPairs = topProtos.GetTopk().GetValues()
		if currPairs[0].GetScore() > max {
			max = currPairs[0].GetScore()
			amount = len(currPairs)
		} else if currPairs[0].GetScore() == max {
			amount += len(currPairs)
		}
	}
	values := make([]*proto.ApbIntPair, amount)
	written := 0
	//Add all entries that are max
	for _, topProtos := range protoObjs {
		//currPairs = topProtos[0].GetTopk().GetValues()
		currPairs = topProtos.GetTopk().GetValues()
		if currPairs[0].GetScore() == max {
			for _, pair := range currPairs {
				values[written] = pair
				written++
			}
		}
	}
	return []*proto.ApbReadObjectResp{{Topk: &proto.ApbGetTopkResp{Values: values}}}
}

func mergeQ18IndexReply(protoObjs []*proto.ApbReadObjectResp) (merged []*proto.ApbReadObjectResp) {
	return mergeTopKProtos(protoObjs, 100)
}

// Note: Average CRDT is not supported as it produces an incorrect value to sum and then divide the averages when executing a full read.
func mergeEmbMapProtosFullRead(protoObjs []*proto.ApbReadObjectResp) (merged []*proto.ApbReadObjectResp) {
	keyToObjs := mergeEmbMapProtosInitFullRead(protoObjs)
	innerMapProto := make([]*proto.ApbMapEntry, len(keyToObjs))
	i := 0
	for _, entries := range keyToObjs {
		firstObj := entries[0]
		switch firstObj.GetKey().GetType() {
		case proto.CRDTType_COUNTER:
			value := int32(0)
			for _, entry := range entries {
				value += entry.GetValue().GetCounter().GetValue()
			}
			innerMapProto[i] = &proto.ApbMapEntry{Key: firstObj.GetKey(), Value: crdt.CounterState{Value: value}.ToReadResp()}
		case proto.CRDTType_COUNTER_FLOAT:
			value := 0.0
			for _, entry := range entries {
				value += entry.GetValue().GetCounterfloat().GetValue()
			}
			innerMapProto[i] = &proto.ApbMapEntry{Key: firstObj.GetKey(), Value: crdt.CounterFloatState{Value: value}.ToReadResp()}
		case proto.CRDTType_LWWREG:
			innerMapProto[i] = firstObj
		}
		i++
	}
	return []*proto.ApbReadObjectResp{{Map: &proto.ApbGetMapResp{Entries: innerMapProto}}}
	/*
		innerObjs := make([][]*proto.ApbMapEntry, len(protoObjs))
		for i, obj := range protoObjs {
			innerObjs[i] = obj.GetMap().GetEntries()
		}
		innerMapProto := make([]*proto.ApbMapEntry, len(innerObjs[0]))

		for i, firstObj := range innerObjs[0] {
			switch firstObj.GetKey().GetType() {
			case proto.CRDTType_COUNTER:
				value := int32(0)
				for _, innerMap := range innerObjs {
					value += innerMap[i].GetValue().GetCounter().GetValue()
				}
				innerMapProto[i] = &proto.ApbMapEntry{Key: firstObj.GetKey(), Value: crdt.CounterState{Value: value}.ToReadResp()}
			case proto.CRDTType_LWWREG:
				innerMapProto[i] = firstObj
			}
		}

		return []*proto.ApbReadObjectResp{{Map: &proto.ApbGetMapResp{Entries: innerMapProto}}}
	*/
}

func mergeEmbMapProtosInitFullRead(protoObjs []*proto.ApbReadObjectResp) (keyToObjs map[string][]*proto.ApbMapEntry) {
	keyToObjs = make(map[string][]*proto.ApbMapEntry)
	sReply := protoObjs[0].GetMap().GetEntries()
	for _, entry := range sReply {
		entries := make([]*proto.ApbMapEntry, len(protoObjs))
		entries[0] = entry
		keyToObjs[string(entry.GetKey().GetKey())] = entries
	}
	for j, jEntries := range protoObjs[1:] {
		sReply = jEntries.GetMap().GetEntries()
		for _, entry := range sReply {
			keyToObjs[string(entry.GetKey().GetKey())][j+1] = entry
		}
	}
	return
}

func mergeEmbMapProtosPartRead(protoObjs []*proto.ApbReadObjectResp) (merged []*proto.ApbReadObjectResp) {
	keysToObjs, keys := mergeEmbMapProtosInit(protoObjs)
	innerMapProto := make([]*proto.ApbMapGetValueResp, len(keysToObjs))
	i := 0
	for key, entries := range keysToObjs {
		innerMapProto[i], keys[i] = mergeEmbMapProtosPartReadHelper(key, entries), []byte(key)
		i++
	}
	return []*proto.ApbReadObjectResp{{Partread: &proto.ApbPartialReadResp{
		Map: &proto.ApbMapPartialReadResp{Getvalues: &proto.ApbMapGetValuesResp{Keys: keys, Values: innerMapProto}}}}}
}

func mergeEmbMapProtosPartReadHelper(key string, values []*proto.ApbMapGetValueResp) *proto.ApbMapGetValueResp {
	firstValue := values[0]
	readType, crdtType := firstValue.GetParttype(), firstValue.GetCrdttype()
	switch crdtType {
	case proto.CRDTType_RRMAP:
		//Make another map and then recall mergeEmbMapProtosPartReadHelper
		newInnerObjs := make(map[string][]*proto.ApbMapGetValueResp)
		innerEntries := firstValue.GetValue().GetPartread().GetMap().GetGetvalues()
		innerKeys, innerValues := innerEntries.GetKeys(), innerEntries.GetValues()
		for i, obj := range innerValues {
			objs := make([]*proto.ApbMapGetValueResp, len(values))
			objs[0] = obj
			newInnerObjs[string(innerKeys[i])] = objs
		}

		for j, jObjs := range values[1:] {
			innerEntries = jObjs.GetValue().GetPartread().GetMap().GetGetvalues()
			innerKeys, innerValues = innerEntries.GetKeys(), innerEntries.GetValues()
			//fmt.Printf("[MergeEmbMap]InnerKeys: %+v (len: %d)\n", innerKeys, len(innerKeys))
			//fmt.Printf("[MergeEmbMap]InnerValues: %+v (len: %d)\n", innerValues, len(innerValues))
			for i, obj := range innerValues {
				newInnerObjs[string(innerKeys[i])][j+1] = obj
			}
		}
		innerMapProto := make([]*proto.ApbMapGetValueResp, len(innerKeys))
		i := 0
		for key, entries := range newInnerObjs {
			innerMapProto[i], innerKeys[i] = mergeEmbMapProtosPartReadHelper(key, entries), []byte(key)
			i++
		}
		return &proto.ApbMapGetValueResp{Parttype: &readType, Crdttype: &crdtType, Value: &proto.ApbReadObjectResp{
			Partread: &proto.ApbPartialReadResp{Map: &proto.ApbMapPartialReadResp{Getvalues: &proto.ApbMapGetValuesResp{Keys: innerKeys, Values: innerMapProto}}}}}
	case proto.CRDTType_COUNTER:
		count := int32(0)
		for _, value := range values {
			count += value.GetValue().GetCounter().GetValue()
		}
		return &proto.ApbMapGetValueResp{Value: crdt.CounterState{Value: count}.ToReadResp(), Crdttype: &crdtType, Parttype: &readType}
	case proto.CRDTType_COUNTER_FLOAT:
		count := 0.0
		for _, value := range values {
			count += value.GetValue().GetCounterfloat().GetValue()
		}
		return &proto.ApbMapGetValueResp{Value: crdt.CounterFloatState{Value: count}.ToReadResp(), Crdttype: &crdtType, Parttype: &readType}
	case proto.CRDTType_AVG:
		sum, nAdds := int64(0), int64(0)
		for _, value := range values {
			currAvgStateProto := value.GetValue().GetPartread().GetAvg().GetGetfull()
			sum += currAvgStateProto.GetSum()
			nAdds += currAvgStateProto.GetNAdds()
		}
		return &proto.ApbMapGetValueResp{Value: crdt.AvgFullState{Sum: sum, NAdds: nAdds}.ToReadResp(), Crdttype: &crdtType, Parttype: &readType}
	case proto.CRDTType_LWWREG:
		return firstValue
	case proto.CRDTType_TOPK, proto.CRDTType_TOPK_RMV, proto.CRDTType_TOPSUM:
		merged := make([]*proto.ApbIntPair, 0, len(firstValue.GetValue().GetTopk().GetValues())*len(values))
		for _, value := range values {
			merged = append(merged, value.GetValue().GetTopk().GetValues()...)
		}
		firstTop := firstValue.GetValue().GetTopk()
		firstTop.Values = merged
		return &proto.ApbMapGetValueResp{Value: &proto.ApbReadObjectResp{Topk: firstTop}}
	case proto.CRDTType_ORSET:
		readType := firstValue.GetParttype()
		if readType == proto.READType_N_ELEMS {
			count := int32(0)
			for _, value := range values {
				count += value.GetValue().GetPartread().GetSet().GetNelems().GetCount()
			}
			return &proto.ApbMapGetValueResp{Value: crdt.SetAWNElementsState{Count: int(count)}.ToReadResp(), Crdttype: &crdtType, Parttype: &readType}
		}
	}
	return firstValue
}

// Extracts the objs inside the map of each replica and organizes it in the form of key -> [obj instances (of each replica)]
func mergeEmbMapProtosInit(protoObjs []*proto.ApbReadObjectResp) (keyToObjs map[string][]*proto.ApbMapGetValueResp, keys [][]byte) {
	keyToObjs = make(map[string][]*proto.ApbMapGetValueResp)
	sReply := protoObjs[0].GetPartread().GetMap().GetGetvalues()
	sKeys, sValues := sReply.GetKeys(), sReply.GetValues()
	for i, obj := range sValues {
		objs := make([]*proto.ApbMapGetValueResp, len(protoObjs))
		objs[0] = obj
		keyToObjs[string(sKeys[i])] = objs
	}
	for j, jObjects := range protoObjs[1:] {
		sReply = jObjects.GetPartread().GetMap().GetGetvalues()
		sKeys, sValues = sReply.GetKeys(), sReply.GetValues()
		for i, obj := range sValues {
			keyToObjs[string(sKeys[i])][j+1] = obj
		}
	}
	return keyToObjs, sKeys
}

/*
// Note: Currently it only supports inside the embedded Map the following CRDTs:
// Counters, Averages, LWW (takes the value of the first one), Top (gathers all together), Set (getNelems).
// Pre-condition: the keys and types of the states are the same across all replies.
// Also, it only supports one ApbReadObjectResp per server.
func mergeEmbMapProtosPartRead(protoObjs []*proto.ApbReadObjectResp) (merged []*proto.ApbReadObjectResp) {
	//ProtoObjs: one entry per server
	fmt.Printf("[Queries][MergeEmbMapProtosPartRead]ProtoObjs: %+v\n", protoObjs)
	innerObjs := make([][]*proto.ApbMapGetValueResp, len(protoObjs))
	for i, obj := range protoObjs {
		//InnerObjs[i]: each of the map read by replica i. (may be multiple maps)
		innerObjs[i] = obj.GetPartread().GetMap().GetGetvalues().GetValues()
	}
	firstReply := protoObjs[0].GetPartread().GetMap().GetGetvalues()
	keys, firstValues := firstReply.GetKeys(), firstReply.GetValues()
	innerMapProto := make([]*proto.ApbMapGetValueResp, len(keys))

	fmt.Printf("[Queries][MergeEmbMapProtosPartRead]Entries of the first replica %+v\n:", firstReply)
	fmt.Printf("[Queries][MergeEmbMapProtosPartRead]Keys of the first replica %+v\n:", keys)
	fmt.Println("[Queries][MergeEmbMapProtosPartRead]Number of CRDTs in firstValues:", len(firstValues))
	for i, firstValue := range firstValues {
		fmt.Printf("[Queries][MergeEmbMapProtosPartRead]Index: %d. Key: %v, CrdtType: %v. ReadType: %v. Proto: %+v\n", i, string(keys[i]), firstValue.GetCrdttype(), firstValue.GetParttype(), firstValue)
		//TODO: It's not merging the same entries... :(. Because when converting from map to array it is random...
		//Maybe have to do a temporary map or something
		//Or consider changing the protobufs themselves to be a map
		fmt.Printf("[Queries][MergeEmbMapProtosPartRead]Keys of all servers for index %d: %v, %v, %v, %v, %v\n", i,
			string(protoObjs[0].GetPartread().GetMap().GetGetvalues().GetKeys()[i]), string(protoObjs[1].GetPartread().GetMap().GetGetvalues().GetKeys()[i]),
			string(protoObjs[2].GetPartread().GetMap().GetGetvalues().GetKeys()[i]), string(protoObjs[3].GetPartread().GetMap().GetGetvalues().GetKeys()[i]),
			string(protoObjs[4].GetPartread().GetMap().GetGetvalues().GetKeys()[i]))
		fmt.Println(protoObjs[i].GetPartread().GetMap().GetGetvalues().GetValues()[0])
		innerMapProto[i] = mergeEmbMapProtosPartReadHelper(innerObjs, i)
	}
	fmt.Println()
	for i, key := range keys {
		fmt.Printf("[Queries][MergeEmbMapProtosPartRead]Merge. Key: %v. Value: %+v\n", string(key), innerMapProto[i])
	}
	fmt.Println()
	fmt.Printf("[Queries][MergeEmbMapProtosPartRead]Merged keys: %v, Merged values: %v\n", keys, innerMapProto)
	return []*proto.ApbReadObjectResp{{Partread: &proto.ApbPartialReadResp{
		Map: &proto.ApbMapPartialReadResp{Getvalues: &proto.ApbMapGetValuesResp{Keys: keys, Values: innerMapProto}}}}}
}

// Merges an entry of an embedded map
func mergeEmbMapProtosPartReadHelper(innerObjs [][]*proto.ApbMapGetValueResp, i int) *proto.ApbMapGetValueResp {
	firstValue := innerObjs[0][i]
	readType, crdtType := firstValue.GetParttype(), firstValue.GetCrdttype()
	switch crdtType {
	case proto.CRDTType_RRMAP:
		//innerObjs[i][j]. i: replicas. j: replies of server i.
		newInnerObjs := make([][]*proto.ApbMapGetValueResp, len(innerObjs))
		for j, innerMap := range innerObjs {
			newInnerObjs[j] = innerMap[i].GetValue().GetPartread().GetMap().GetGetvalues().GetValues()
		}
		innerFirstMap := firstValue.GetValue().GetPartread().GetMap().GetGetvalues()
		inKeys := innerFirstMap.GetKeys()
		mergedValues := make([]*proto.ApbMapGetValueResp, len(inKeys))
		for j := range inKeys {
			mergedValues[j] = mergeEmbMapProtosPartReadHelper(newInnerObjs, j)
		}
		return &proto.ApbMapGetValueResp{Parttype: &readType, Crdttype: &crdtType, Value: &proto.ApbReadObjectResp{
			Partread: &proto.ApbPartialReadResp{Map: &proto.ApbMapPartialReadResp{Getvalues: &proto.ApbMapGetValuesResp{Keys: inKeys, Values: mergedValues}}}}}
	case proto.CRDTType_COUNTER:
		value := int32(0)
		for _, innerMap := range innerObjs {
			value += innerMap[i].GetValue().GetCounter().GetValue()
		}
		return &proto.ApbMapGetValueResp{Value: crdt.CounterState{Value: value}.ToReadResp(), Crdttype: &crdtType, Parttype: &readType}
	case proto.CRDTType_COUNTER_FLOAT:
		value := 0.0
		for _, innerMap := range innerObjs {
			value += innerMap[i].GetValue().GetCounterfloat().GetValue()
		}
		return &proto.ApbMapGetValueResp{Value: crdt.CounterFloatState{Value: value}.ToReadResp(), Crdttype: &crdtType, Parttype: &readType}
	case proto.CRDTType_AVG:
		sum, nAdds := int64(0), int64(0)
		for _, innerMap := range innerObjs {
			currAvgStateProto := innerMap[i].GetValue().GetPartread().GetAvg().GetGetfull()
			sum += currAvgStateProto.GetSum()
			nAdds += currAvgStateProto.GetNAdds()
		}
		return &proto.ApbMapGetValueResp{Value: crdt.AvgFullState{Sum: sum, NAdds: nAdds}.ToReadResp(), Crdttype: &crdtType, Parttype: &readType}
	case proto.CRDTType_LWWREG:
		return firstValue
	case proto.CRDTType_TOPK, proto.CRDTType_TOPK_RMV, proto.CRDTType_TOPSUM:
		merged := make([]*proto.ApbIntPair, 0, len(firstValue.GetValue().GetTopk().GetValues())*len(innerObjs))
		for _, innerMap := range innerObjs {
			merged = append(merged, innerMap[i].GetValue().GetTopk().GetValues()...)
		}
		firstTop := firstValue.GetValue().GetTopk()
		firstTop.Values = merged
		return &proto.ApbMapGetValueResp{Value: &proto.ApbReadObjectResp{Topk: firstTop}}
	case proto.CRDTType_ORSET:
		readType := firstValue.GetParttype()
		if readType == proto.READType_N_ELEMS {
			count := int32(0)
			for _, innerMap := range innerObjs {
				count += innerMap[i].GetValue().GetPartread().GetSet().GetNelems().GetCount()
			}
			return &proto.ApbMapGetValueResp{Value: crdt.SetAWNElementsState{Count: int(count)}.ToReadResp(), Crdttype: &crdtType, Parttype: &readType}
		}
	}
	return firstValue
}
*/

// Note: requires for all topK to be ordered.
func mergeTopKProtos(protoObjs []*proto.ApbReadObjectResp, target int) (merged []*proto.ApbReadObjectResp) {
	//protoObjs := getObjectsFromStaticReadResp(protos)
	values := make([]*proto.ApbIntPair, target)
	//currI := make([]int, len(protos))
	currI := make([]int, len(protoObjs))
	end, max, playerId, replicaI, data := true, int32(-1), int32(-1), 0, []byte{}
	var currPair *proto.ApbIntPair
	written := 0
	//Algorithm: go through all protobufs, check first position of each, pick highest. Increase index of the protobuf with highest
	//Keep repeating the algorithm until enough elements (target) are filled.
	//Total iterations: target * len(protoObjs)
	//This works as all topK are ordered.
	for written < target {
		for i, topProtos := range protoObjs { //Goes through all protos
			if len(topProtos.GetTopk().GetValues()) > currI[i] { //Checks if there's still entries left
				//if len(topProtos[0].GetTopk().GetValues()) > currI[i] {
				end = false
				//currPair = topProtos[0].GetTopk().GetValues()[currI[i]]
				currPair = topProtos.GetTopk().GetValues()[currI[i]]
				if currPair.GetScore() > max {
					max, playerId, data, replicaI = currPair.GetScore(), currPair.GetPlayerId(), currPair.Data, i
				}
			}
		}
		if end {
			break
		}
		values[written] = &proto.ApbIntPair{PlayerId: pb.Int32(playerId), Score: pb.Int32(max), Data: data}
		currI[replicaI]++
		written++
		max, end = -1, true
	}
	values = values[:written]
	return []*proto.ApbReadObjectResp{{Topk: &proto.ApbGetTopkResp{Values: values}}}
}

/*
	func getObjectsFromStaticReadResp(protos []*proto.ApbStaticReadObjectsResp) (protoObjs [][]*proto.ApbReadObjectResp) {
		protoObjs = make([][]*proto.ApbReadObjectResp, len(protos))
		for i, proto := range protos {
			protoObjs[i] = proto.GetObjects().GetObjects()
		}
		return
	}
*/
func getObjectsFromStaticReadResp(protos []*proto.ApbStaticReadObjectsResp) (protoObjs []*proto.ApbReadObjectResp) {
	protoObjs = make([]*proto.ApbReadObjectResp, 0, len(protos)) //Usually only 1 read per entry
	for _, proto := range protos {
		protoObjs = append(protoObjs, proto.GetObjects().GetObjects()...)
	}
	return
}

func readRepliesToOneSlice(protos [][]*proto.ApbReadObjectResp, indexes []int, nReadPerServer int) (objs []*proto.ApbReadObjectResp) {
	objs = make([]*proto.ApbReadObjectResp, len(protos)*nReadPerServer)
	for i, serverProtos := range protos {
		objs[i*nReadPerServer] = serverProtos[indexes[i]]
		for j := 1; j < nReadPerServer; j++ { //If the reply has more than one proto, copy those too.
			objs[i*nReadPerServer+j] = serverProtos[indexes[i]+j]
		}
	}
	return objs
}

func getIndexOffset(client QueryClient, region int) (bucketI, serverI int) {
	if !isIndexGlobal {
		//return INDEX_BKT + region
		return INDEX_BKT + region, region
	}
	//return INDEX_BKT
	return INDEX_BKT, client.indexServer
}

func unpackIndexExtraData(data []byte, nEntries int) (parts []string) {
	return strings.SplitN(string(data), "_", nEntries)
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

// If ONLY_LOCAL_DATA_QUERY = true, returns the region of the client's server. Otherwise, returns a random one.
func (client QueryClient) getRngRegion() int {
	if ONLY_LOCAL_DATA_QUERY {
		return client.indexServer
	}
	return client.rng.Intn(len(tpchData.Tables.Regions))
}

func (client QueryClient) getRngNation() int {
	if ONLY_LOCAL_DATA_QUERY {
		nationIDs := tpchData.Tables.GetNationIDsOfRegion(client.indexServer)
		return int(nationIDs[client.rng.Intn(len(nationIDs))])

	}
	return client.rng.Intn(len(tpchData.Tables.Nations))
}

// Fills it with a heuristic balance value that is safe.
// Note that customers' balance is fixed (not updated)
// And if in a query we find out the average is lower than the heuristic value,
// we can just repeat the query with the average and update the heuristic.
// This is only used by query-only clients. Clients with updates use a better heuristic based on real data.
func q22CustNatBalancesFiller() {
	q22CustNatBalances = make(map[int8]PairIntFloat)
	for i := int8(10); i <= 34; i++ { //Using phone numbers identifiers (nationID + 10). There's 25 nations (5 per region)
		q22CustNatBalances[i] = PairIntFloat{first: 3000, second: 1}
	}
}

//OBSOLETE

/*
func sendQueriesNoIndex(conn net.Conn) {
	//Might want to consider supporting partial read of all keys on EmbMaps.
	//The issue is that, theorically, an embedded map can have CRDTs of multiple types in there...
	time.Sleep(3000 * time.Millisecond)
	fmt.Println()
	fmt.Println("Starting to execute Q3...")
	startTime := time.Now().UnixNano()
	sendQ3NoIndex(conn)
	finishTime := time.Now().UnixNano()
	fmt.Println("Finished executing Q3.")
	printExecutionTimes()
	fmt.Println("Q3 no index time:", (finishTime-startTime)/1000000, "ms")
}

func sendQ3NoIndex(conn net.Conn) {
	rndSeg := tpchData.Tables.Segments[rand.Intn(5)]
	rndDay := int8(1 + rand.Int63n(31))
	minDate, maxDate := &Date{YEAR: 1995, MONTH: 03, DAY: rndDay + 1}, &Date{YEAR: 1995, MONTH: 03, DAY: rndDay - 1}

	readParams := make(map[int8][]crdt.ReadObjectParams)
	i, regionLen := int8(0), int8(len(tpchData.Tables.Regions))
	for ; i < regionLen; i++ {
		readParams[i] = []crdt.ReadObjectParams{
			crdt.ReadObjectParams{
				KeyParams: crdt.KeyParams{Key: tableNames[ORDERS], CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[i]},
				ReadArgs:  crdt.StateReadArguments{},
			},
			crdt.ReadObjectParams{
				KeyParams: crdt.KeyParams{Key: tableNames[tpch.LINEITEM], CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[i]},
				ReadArgs:  crdt.StateReadArguments{},
			},
			crdt.ReadObjectParams{
				KeyParams: crdt.KeyParams{Key: tableNames[CUSTOMER], CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[i]},
				ReadArgs:  crdt.StateReadArguments{},
			},
		}
	}
	replies := sendReceiveBucketFullReadProtos(readParams)
	states := mergeMultipleMapReplies(replies, proto.READType_FULL)
	orders, items, customers := states[0], states[1], states[2]

	l_orderId, l_shipdate := headers[tpch.LINEITEM][L_ORDERKEY], headers[tpch.LINEITEM][L_SHIPDATE]
	l_price, l_discount := headers[tpch.LINEITEM][L_EXTENDEDPRICE], headers[tpch.LINEITEM][L_DISCOUNT]
	o_orderdate, o_custkey, o_shippriority := headers[ORDERS][O_ORDERDATE], headers[ORDERS][O_CUSTKEY], headers[ORDERS][O_SHIPPRIOTITY]
	c_mktsegment := headers[CUSTOMER][C_MKTSEGMENT]

	var shipdate, orderDate *Date
	var price, discount float64
	var order, customer crdt.EmbMapEntryState
	var orderID string
	sums := make(map[string]float64)

	for _, item := range items {
		shipdate = createDate(embMapRegisterToString(l_shipdate, item))
		if shipdate.IsHigherOrEqual(minDate) {
			orderID = embMapRegisterToString(l_orderId, item)
			order = orders[orderID]
			orderDate = createDate(embMapRegisterToString(o_orderdate, order))
			if orderDate.isLowerOrEqual(maxDate) {
				customer = customers[embMapRegisterToString(o_custkey, order)]
				if embMapRegisterToString(c_mktsegment, customer) == rndSeg {
					//Segment matches and dates are in range.
					price, _ = strconv.ParseFloat(embMapRegisterToString(l_price, item), 64)
					discount, _ = strconv.ParseFloat(embMapRegisterToString(l_discount, item), 64)
					sums[orderID] += price * (1.0 - discount)
				}
			}
		}
	}
	top10 := make([]string, 10)
	minValue, minIndex := 0.0, 0
	//Finding top 10...
	for id, value := range sums {
		if value > minValue {
			top10[minIndex] = id
			minValue = value
			for i, topId := range top10 {
				if topValue := sums[topId]; topValue < minValue {
					minValue, minIndex = topValue, i
				}
			}
		}
	}
	//Sorting top10
	sort.Slice(top10, func(i, j int) bool { return sums[top10[i]] < sums[top10[j]] })

	//Printing result
	for _, orderID := range top10 {
		order = orders[orderID]
		fmt.Printf("%s | %f | %s | %s\n", orderID, sums[orderID],
			embMapRegisterToString(o_orderdate, order), embMapRegisterToString(o_shippriority, order))
	}

	ignore(rndSeg, rndDay)
}

func sendQ5NoIndex(conn net.Conn) {

}

func sendQ11NoIndex(conn net.Conn) {

}

func sendQ14NoIndex(conn net.Conn) {

}

func sendQ15NoIndex(conn net.Conn) {

}

func sendQ18NoIndex(conn net.Conn) {

}
*/
