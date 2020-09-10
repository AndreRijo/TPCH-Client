package tpch

import (
	"antidote"
	"crdt"
	"encoding/csv"
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"proto"
	"strconv"
	"strings"
	"time"
	"tools"
)

/*
Configs needed:
Key range to use (allows all clients to share single key, all clients share a set of keys,
different client PCs use different keys, etc.)
How to use the key range (SINGLE/SHARED/SPLIT/PER_CLIENT)
Target elements (topk/set/map)
Element range (to use in adds/rems)
Query/Upd rate (likely can use the already existing config)
Add/remove rate (topk/set/map/possibly inc/dec?)

Some stuff not clear yet:
- Need to think well on how to benchmark RRMAP

Do not forget:
- All tests are singleServer.

Used by indexes:
- TopK
- Counter inbedded in RRMAP
- Avg
*/

var (
	BENCH_KEY_TYPE                                string
	BENCH_CRDT                                    proto.CRDTType
	BENCH_N_KEYS, OPS_PER_TXN, N_TXNS_BEFORE_WAIT int
	BENCH_ADD_RATE, BENCH_PART_READ_RATE          float64
	BENCH_DO_PRELOAD                              bool
	QUERY_FUNCS, UPDATE_FUNCS, BENCH_QUERY_RATES  []string

	//Dummy client, just to have access to the function pointers
	dummyClient                                                         BenchClient
	FULL_READ_ARGS                                                      crdt.ReadArguments = nil
	baseSetInfo, baseTopKInfo, baseAvgInfo, baseCounterInfo, baseRRInfo                    = SetInfo{}, TopKInfo{}, AvgInfo{}, CounterInfo{}, RRMapInfo{}
)

const (
	BUCKET    = "benchBucket"
	FULL_READ = "FULL"
)

type BenchStats struct {
	nReads, nUpds int
	timeSpent     int64
}

type BenchClient struct {
	//Can either be a single function (e.g: when doing only adds) or a selecter function (e.g: both adds & rems)
	queryFun   func() (readArgs crdt.ReadArguments)
	updateFun  func() (updArgs crdt.UpdateArguments)
	queryFuns  []func() (readArgs crdt.ReadArguments) //Used for when there's no method implementing the exact set of queries intended.
	rng        *rand.Rand
	conn       net.Conn
	key        string
	keys       []string
	crdtType   proto.CRDTType
	clientID   int
	queryOdds  []float64 //Used only by CRDTs with more than 2 queries
	resultChan chan []BenchStats
	waitFor    *int //Number of txns already sent for which we haven't yet received a reply
	maxWaitFor int  //local copy of N_TXNS_BEFORE_WAIT
	SetInfo
	TopKInfo
	CounterInfo
	RRMapInfo
	AvgInfo
}

type SetInfo struct {
	elems  []crdt.Element
	nElems int
}

type TopKInfo struct {
	maxID, maxScore, topN, topAbove int32
	rndDataSize                     int
}

type CounterInfo struct {
	maxChange, minChange, diff int32
}

type RRMapInfo struct {
	keys          []string
	nKeys         int
	keyType       string
	innerCrdtType proto.CRDTType
	SetInfo
	TopKInfo
	CounterInfo
	AvgInfo
}

type AvgInfo struct {
	maxSum   int64
	maxNAdds int64
}

func startCRDTBench(configs *tools.ConfigLoader) {
	start := time.Now().UnixNano()
	fmt.Println("Reading bench-specific configs...")
	loadBenchConfigs(configs)
	fmt.Println("Bench-specific configs sucessfully read.")
	server := servers[0]

	fmt.Println("Preparing bench clients...")
	clients := make([]BenchClient, TEST_ROUTINES)
	//baseClient := makeClient(server)
	//clients[0] = baseClient
	seed := time.Now().UnixNano()
	for i := 0; i < len(clients); i++ {
		//To ensure data isn't shared.
		//clients[i] = copyClient(baseClient, server, seed, i)
		clients[i] = makeClient(server, seed, i)
	}
	collectQueryStats = make([]bool, TEST_ROUTINES)
	for i := range collectQueryStats {
		collectQueryStats[i] = false
	}
	results := make([][]BenchStats, len(clients))

	printConfigs(clients[0])
	//fmt.Printf("Client0 maxID: %d\n", clients[0].maxID)
	//fmt.Printf("Client0 maxScore: %d\n", clients[0].maxScore)
	//fmt.Printf("Client0 topAbove: %d\n", clients[0].topAbove)
	//fmt.Printf("Client0 topN: %d\n", clients[0].topN)
	//fmt.Printf("Client0 rndDataSize: %d\n", clients[0].rndDataSize)
	//fmt.Println()
	if BENCH_DO_PRELOAD {
		preload(clients[0], getAllKeys())
		fmt.Println("Preload complete.")
	}

	fmt.Printf("Bench clients ready. Waiting to start crdt %s benchmark...\n", clients[0].crdtType)
	toSleep := QUERY_WAIT*1000000 - time.Duration(time.Now().UnixNano()-start)
	if toSleep/1000000 < 10000 {
		time.Sleep(10 * time.Second)
	} else {
		time.Sleep(QUERY_WAIT*1000000 - time.Duration(time.Now().UnixNano()-start))
	}
	fmt.Println("Starting bench clients...")

	if statisticsInterval > 0 {
		go doQueryStatsInterval() //Uses same variables as in clientQueries.go
	}

	startClients(clients)

	fmt.Println("Bench clients started.")
	time.Sleep(time.Duration(TEST_DURATION) * time.Millisecond)
	STOP_QUERIES = true
	fmt.Println("Test time's over.")

	for i, client := range clients {
		results[i] = <-client.resultChan
	}
	fmt.Println()
	fmt.Println("All bench clients have finished.")
	totalReads, totalUpds, avgDuration := 0, 0, 0.0
	for i, result := range results {
		sumR := BenchStats{}
		for _, innerR := range result {
			//fmt.Printf("Client %d, Section %d: %d %d %d %d(ms)\n", i, j, innerR.nReads, innerR.nUpds, innerR.nReads+innerR.nUpds, innerR.timeSpent)
			sumR.nReads, sumR.nUpds, sumR.timeSpent = sumR.nReads+innerR.nReads, sumR.nUpds+innerR.nUpds, sumR.timeSpent+innerR.timeSpent
		}
		nOps := sumR.nReads + sumR.nUpds
		fmt.Printf("%d: Reads: %d, Reads/s: %f, Upds: %d, Upds/s: %f, Ops: %d, Ops/s: %f, Latency(ms): %f\n", i, sumR.nReads, (float64(sumR.nReads)/float64(sumR.timeSpent))*1000,
			sumR.nUpds, (float64(sumR.nUpds)/float64(sumR.timeSpent))*1000, nOps, (float64(nOps)/float64(sumR.timeSpent))*1000, (float64(sumR.timeSpent) / float64(nOps)))
		totalReads, totalUpds, avgDuration = totalReads+sumR.nReads, totalUpds+sumR.nUpds, avgDuration+float64(sumR.timeSpent)
	}
	avgDuration /= float64(len(results))
	totalOps := totalReads + totalUpds
	fmt.Printf("Totals: Reads: %d, Reads/s: %f, Upds: %d, Upds/s: %f, Ops: %d, Ops/s: %f, Latency(ms): %f\n", totalReads, (float64(totalReads)/avgDuration)*1000,
		totalUpds, (float64(totalUpds)/avgDuration)*1000, totalOps, (float64(totalOps)/avgDuration)*1000, (float64(avgDuration) / float64(totalOps/len(results))))

	writeBenchStatsFile(results, clients[0].crdtType)
	os.Exit(0)
}

func printConfigs(bc BenchClient) {
	fmt.Printf("KEY TYPE: %s\nN_KEYS: %d\nADD_RATE: %f\nPART_READ_RATE: %f\n", BENCH_KEY_TYPE, BENCH_N_KEYS, BENCH_ADD_RATE, BENCH_PART_READ_RATE)
	fmt.Printf("QUERY FUNCS: %v\nUPDATE FUNCS: %v\nQUERY_RATES: %v\n", QUERY_FUNCS, UPDATE_FUNCS, BENCH_QUERY_RATES)
	fmt.Printf("SET INFO: {nElems: %d}\n", baseSetInfo.nElems)
	fmt.Printf("TOPK INFO: {maxID: %d, maxScore: %d, topN: %d, topAbove: %d}\n", baseTopKInfo.maxID, baseTopKInfo.maxScore, baseTopKInfo.topN, baseTopKInfo.topAbove)
	fmt.Printf("COUNTER INFO: {minChange: %d, maxChange: %d, diff: %d}\n", baseCounterInfo.minChange, baseCounterInfo.maxChange, baseCounterInfo.diff)
	fmt.Printf("AVG INFO: {maxSum: %d, maxNAdds: %d}\n", baseAvgInfo.maxSum, baseAvgInfo.maxNAdds)
	fmt.Printf("Query fun name: %s, Update fun name: %s\n", tools.GetFunctionName(bc.queryFun), tools.GetFunctionName(bc.updateFun))
	fmt.Print("Query funs names: [")
	for _, fun := range bc.queryFuns {
		fmt.Print(tools.GetFunctionName(fun) + ", ")
	}
	fmt.Println("]")
	fmt.Printf("Query odds: %v\n", bc.queryOdds)
	fmt.Printf("CRDT type: %s %s (clientInfo)\n", BENCH_CRDT, bc.crdtType)
	fmt.Println()
	//TODO: RR info
}

func startClients(clients []BenchClient) {
	if OPS_PER_TXN == 1 {
		if UPD_RATE == 0 {
			fmt.Println("[BenchQuery]")
			for i := 0; i < TEST_ROUTINES; i++ {
				go clients[i].benchQuery()
			}
		} else if UPD_RATE == 1 {
			fmt.Println("[BenchUpdate]")
			for i := 0; i < TEST_ROUTINES; i++ {
				go clients[i].benchUpdate()
			}
		} else {
			fmt.Println("[BenchMix]")
			for i := 0; i < TEST_ROUTINES; i++ {
				//fmt.Println("Starting mix bench for client", i)
				go clients[i].benchMix()
			}
		}
	} else {
		if UPD_RATE == 0 {
			fmt.Println("[BenchQueries]")
			for i := 0; i < TEST_ROUTINES; i++ {
				go clients[i].benchQueries()
			}
		} else if UPD_RATE == 1 {
			fmt.Println("[BenchUpdates]")
			for i := 0; i < TEST_ROUTINES; i++ {
				go clients[i].benchUpdates()
			}
		} else {
			fmt.Println("[BenchMixes]")
			for i := 0; i < TEST_ROUTINES; i++ {
				//fmt.Println("Starting mix bench for client", i)
				go clients[i].benchMixes()
			}
		}
	}
}

/*
BENCH_KEY_TYPE                               string
	BENCH_CRDT                                   proto.CRDTType
	BENCH_N_ELEMS, BENCH_N_KEYS                  int
	BENCH_ADD_RATE, BENCH_PART_READ_RATE         float64
	QUERY_FUNCS, UPDATE_FUNCS, BENCH_QUERY_RATES []string

	type SetInfo struct {
	elems  []crdt.Element
	nElems int
}

type TopKInfo struct {
	maxID, maxScore, topN, topAbove int32
	rndDataSize                     int
}

type CounterInfo struct {
	maxChange, minChange, diff int32
}

type RRMapInfo struct {
	keys          []string
	nKeys         int
	keyType       string
	innerCrdtType proto.CRDTType
	SetInfo
	TopKInfo
	CounterInfo
	AvgInfo
}

type AvgInfo struct {
	maxSum   int64
	maxNAdds int64
}
*/

//TODO: Could still use a copyClient function that avoids having to resort the query and update functions.
func makeClient(server string, seed int64, nClient int) (baseClient BenchClient) {
	conn, err := net.Dial("tcp", server)
	tools.CheckErr("Network connection establishment err", err)

	baseClient = BenchClient{
		rng:        rand.New(rand.NewSource(seed + int64(nClient))),
		conn:       conn,
		crdtType:   BENCH_CRDT,
		clientID:   nClient,
		queryOdds:  processQueryOdds(BENCH_QUERY_RATES),
		resultChan: make(chan []BenchStats),
		maxWaitFor: N_TXNS_BEFORE_WAIT,
		waitFor:    new(int),
	}

	updateBenchClientWithKeys(&baseClient, nClient)
	updateBenchClientWithCrdtInfo(&baseClient)
	baseClient.queryFun, baseClient.queryFuns = selectQueryFuns(BENCH_CRDT, QUERY_FUNCS, BENCH_QUERY_RATES, baseClient)
	if baseClient.queryFun == nil {
		baseClient.queryFun = baseClient.customQuery
	}
	baseClient.updateFun = selectUpdateFuns(BENCH_CRDT, UPDATE_FUNCS, baseClient)

	return
}

/*
func copyClient(baseClient BenchClient, server string, seed int64, nClient int) (newClient BenchClient) {
	conn, _ := net.Dial("tcp", server)
	newClient = BenchClient{
		queryFun:   baseClient.queryFun,
		updateFun:  baseClient.updateFun,
		queryFuns:  baseClient.queryFuns,
		rng:        rand.New(rand.NewSource(seed + int64(nClient))),
		conn:       conn,
		crdtType:   baseClient.crdtType,
		clientID:   nClient,
		resultChan: make(chan []BenchStats),
	}
	copyOdds := make([]float64, len(baseClient.queryOdds))
	for i, odd := range baseClient.queryOdds {
		copyOdds[i] = odd
	}
	newClient.queryOdds = copyOdds
	updateBenchClientWithKeys(&newClient, nClient)
	updateBenchClientWithCrdtInfo(&newClient)

	return
}
*/

func (bc BenchClient) benchQuery() {
	nReads, lastStatReads, lastStatTime, allStats := 0, 0, time.Now().UnixNano()/1000000, make([]BenchStats, 0, TEST_DURATION/int64(statisticsInterval)+1)
	for !STOP_QUERIES {
		/*
			if nReads == 0 && bc.clientID == 0 {
				fmt.Println("Starting reads on client 0. Keys:", bc.keys)
			}
		*/
		if collectQueryStats[bc.clientID] {
			allStats, lastStatReads, lastStatTime = bc.updateQueryStats(allStats, nReads, lastStatReads, lastStatTime)
		}
		bc.sendQuery(bc.queryFun())
		nReads++
		/*
			if nReads == 1 {
				fmt.Printf("Did 1 read on client %d\n", bc.clientID)
			}
			if nReads%20000 == 0 {
				fmt.Printf("Did %d reads on client %d\n", nReads, bc.clientID)
			}
		*/
	}
	allStats, _, _ = bc.updateQueryStats(allStats, nReads, lastStatReads, lastStatTime)
	bc.receiveProtos()
	bc.resultChan <- allStats
}

func (bc BenchClient) benchUpdate() {
	nUpds, lastStatUpds, lastStatTime, allStats := 0, 0, time.Now().UnixNano()/1000000, make([]BenchStats, 0, TEST_DURATION/int64(statisticsInterval)+1)
	for !STOP_QUERIES {
		if collectQueryStats[bc.clientID] {
			allStats, lastStatUpds, lastStatTime = bc.updateUpdateStats(allStats, nUpds, lastStatUpds, lastStatTime)
		}
		bc.sendUpdate(bc.updateFun())
		nUpds++
	}
	allStats, _, _ = bc.updateUpdateStats(allStats, nUpds, lastStatUpds, lastStatTime)
	bc.receiveProtos()
	bc.resultChan <- allStats
}

func (bc BenchClient) benchMix() {
	var rnd float64
	nReads, nUpds, lastStatReads, lastStatUpds, lastStatTime := 0, 0, 0, 0, time.Now().UnixNano()/1000000
	allStats := make([]BenchStats, 0, TEST_DURATION/int64(statisticsInterval)+1)
	for !STOP_QUERIES {
		if collectQueryStats[bc.clientID] {
			allStats, lastStatReads, lastStatUpds, lastStatTime = bc.updateMixStats(allStats, nReads, nUpds, lastStatReads, lastStatUpds, lastStatTime)
		}
		rnd = bc.rng.Float64()
		if rnd < UPD_RATE {
			bc.sendUpdate(bc.updateFun())
			nUpds++
		} else {
			bc.sendQuery(bc.queryFun())
			nReads++
		}
	}
	allStats, _, _, _ = bc.updateMixStats(allStats, nReads, nUpds, lastStatReads, lastStatUpds, lastStatTime)
	bc.receiveProtos()
	bc.resultChan <- allStats
}

func (bc BenchClient) benchQueries() {
	nReads, lastStatReads, lastStatTime, allStats := 0, 0, time.Now().UnixNano()/1000000, make([]BenchStats, 0, TEST_DURATION/int64(statisticsInterval)+1)
	readsPerTxn, readBuf := OPS_PER_TXN, make([]crdt.ReadArguments, OPS_PER_TXN)
	for !STOP_QUERIES {
		if collectQueryStats[bc.clientID] {
			allStats, lastStatReads, lastStatTime = bc.updateQueryStats(allStats, nReads, lastStatReads, lastStatTime)
		}
		for i := 0; i < readsPerTxn; i++ {
			readBuf[i] = bc.queryFun()
		}
		bc.sendQueries(readBuf)
		nReads += readsPerTxn
	}
	allStats, _, _ = bc.updateQueryStats(allStats, nReads, lastStatReads, lastStatTime)
	bc.receiveProtos()
	bc.resultChan <- allStats
}

func (bc BenchClient) benchUpdates() {
	nUpds, lastStatUpds, lastStatTime, allStats := 0, 0, time.Now().UnixNano()/1000000, make([]BenchStats, 0, TEST_DURATION/int64(statisticsInterval)+1)
	updsPerTxn, updBuf := OPS_PER_TXN, make([]crdt.UpdateArguments, OPS_PER_TXN)
	for !STOP_QUERIES {
		if collectQueryStats[bc.clientID] {
			allStats, lastStatUpds, lastStatTime = bc.updateUpdateStats(allStats, nUpds, lastStatUpds, lastStatTime)
		}
		for i := 0; i < updsPerTxn; i++ {
			updBuf[i] = bc.updateFun()
		}
		bc.sendUpdates(updBuf)
		nUpds += updsPerTxn
	}
	allStats, _, _ = bc.updateUpdateStats(allStats, nUpds, lastStatUpds, lastStatTime)
	bc.receiveProtos()
	bc.resultChan <- allStats
}

func (bc BenchClient) benchMixes() {
	var rnd float64
	nReads, nUpds, lastStatReads, lastStatUpds, lastStatTime, currR, currU := 0, 0, 0, 0, time.Now().UnixNano()/1000000, 0, 0
	opsPerTxn, readBuf, writeBuf := OPS_PER_TXN, make([]crdt.ReadArguments, OPS_PER_TXN), make([]crdt.UpdateArguments, OPS_PER_TXN)
	allStats := make([]BenchStats, 0, TEST_DURATION/int64(statisticsInterval)+1)
	for !STOP_QUERIES {
		if collectQueryStats[bc.clientID] {
			allStats, lastStatReads, lastStatUpds, lastStatTime = bc.updateMixStats(allStats, nReads, nUpds, lastStatReads, lastStatUpds, lastStatTime)
		}
		for i := 0; i < opsPerTxn; i++ {
			rnd = bc.rng.Float64()
			if rnd < UPD_RATE {
				writeBuf[currU] = bc.updateFun()
				currU++
			} else {
				readBuf[currR] = bc.queryFun()
				currR++
			}
		}
		if currR > 0 {
			bc.sendQueries(readBuf[:currR])
		}
		if currU > 0 {
			bc.sendUpdates(writeBuf[:currU])
		}
		nReads += currR
		nUpds += currU
		currR, currU = 0, 0
	}
	allStats, _, _, _ = bc.updateMixStats(allStats, nReads, nUpds, lastStatReads, lastStatUpds, lastStatTime)
	bc.receiveProtos()
	bc.resultChan <- allStats
}

func (bc BenchClient) updateQueryStats(stats []BenchStats, nReads, lastStatReads int, lastStatTime int64) (newStats []BenchStats,
	newNReads int, newLastStatTime int64) {
	currStatTime, currBenchStats := time.Now().UnixNano()/1000000, BenchStats{}
	diff := currStatTime - lastStatTime
	if diff < int64(statisticsInterval/100) {
		//Replace
		stats[len(stats)-1].nReads += (nReads - lastStatReads)
		stats[len(stats)-1].timeSpent += diff
		newStats = stats
	} else {
		currBenchStats.nReads, currBenchStats.timeSpent = nReads-lastStatReads, diff
		newStats = append(stats, currBenchStats)
	}
	collectQueryStats[bc.clientID] = false
	return newStats, nReads, currStatTime
}

func (bc BenchClient) updateUpdateStats(stats []BenchStats, nUpds, lastStatUpds int, lastStatTime int64) (newStats []BenchStats,
	newNUpds int, newLastStatTime int64) {
	currStatTime, currBenchStats := time.Now().UnixNano()/1000000, BenchStats{}
	diff := currStatTime - lastStatTime
	if diff < int64(statisticsInterval/100) {
		stats[len(stats)-1].nUpds += (nUpds - lastStatUpds)
		stats[len(stats)-1].timeSpent += diff
		newStats = stats
	} else {
		currBenchStats.nUpds, currBenchStats.timeSpent = nUpds-lastStatUpds, diff
		newStats = append(stats, currBenchStats)
	}
	collectQueryStats[bc.clientID] = false
	return newStats, nUpds, currStatTime
}

func (bc BenchClient) updateMixStats(stats []BenchStats, nReads, nUpds, lastStatReads, lastStatUpds int,
	lastStatTime int64) (newStats []BenchStats, newNReads, newNUpds int, newLastStatTime int64) {
	currStatTime, currBenchStats := time.Now().UnixNano()/1000000, BenchStats{}
	diff := currStatTime - lastStatTime
	if diff < int64(statisticsInterval/100) {
		stats[len(stats)-1].nReads += (nReads - lastStatReads)
		stats[len(stats)-1].nUpds += (nUpds - lastStatUpds)
		stats[len(stats)-1].timeSpent += diff
		newStats = stats
	} else {
		currBenchStats.nReads, currBenchStats.nUpds = nReads-lastStatReads, nUpds-lastStatUpds
		currBenchStats.timeSpent = currStatTime - lastStatTime
		newStats = append(stats, currBenchStats)
	}
	collectQueryStats[bc.clientID] = false
	return newStats, nReads, nUpds, currStatTime
}

///////////queryFun()////////////////

//Generalist query method.
func (bc BenchClient) customQuery() (readArgs crdt.ReadArguments) {
	rnd := bc.rng.Float64()
	for i, odd := range bc.queryOdds {
		if rnd < odd {
			return bc.queryFuns[i]()
		}
	}
	return
}

func (bc BenchClient) fullRead() (readArgs crdt.ReadArguments) {
	//fmt.Printf("Client nElems for fullRead: %d\n", bc.nElems)
	//fmt.Printf("Client elems for fullRead: %v\n", bc.elems)
	return FULL_READ_ARGS
}

func (bc BenchClient) setQuery() (readArgs crdt.ReadArguments) {
	rnd := bc.rng.Float64()
	if rnd < BENCH_PART_READ_RATE {
		return bc.setLookup()
	}
	return bc.fullRead()
}

func (bc BenchClient) setLookup() (readArgs crdt.ReadArguments) {
	elemToLookup := bc.elems[bc.rng.Intn(bc.nElems)]
	return crdt.LookupReadArguments{Elem: elemToLookup}
}

func (bc BenchClient) topKQuery() (readArgs crdt.ReadArguments) {
	rnd := bc.rng.Float64()
	if rnd < bc.queryOdds[0] {
		return bc.fullRead()
	} else if rnd < bc.queryOdds[1] {
		return bc.topKTopN()
	}
	return bc.topKAboveValue()
}

func (bc BenchClient) topKTopN() (readArgs crdt.ReadArguments) {
	return crdt.GetTopNArguments{NumberEntries: bc.topN}
}

func (bc BenchClient) topKAboveValue() (readArgs crdt.ReadArguments) {
	return crdt.GetTopKAboveValueArguments{MinValue: bc.topAbove}
}

////////////updateFun()/////////////////

func (bc BenchClient) setUpdate() (updArgs crdt.UpdateArguments) {
	rnd := bc.rng.Float64()
	if rnd < BENCH_ADD_RATE {
		return bc.setAdd()
	}
	return bc.setRem()
}

func (bc BenchClient) setAdd() (updArgs crdt.UpdateArguments) {
	//fmt.Printf("Client0 nElems for add: %d\n", bc.nElems)
	//fmt.Printf("Client0 elems for add: %v\n", bc.elems)
	return crdt.Add{Element: bc.elems[bc.rng.Intn(bc.nElems)]}
}

func (bc BenchClient) setRem() (updArgs crdt.UpdateArguments) {
	//fmt.Printf("Client0 nElems for rem: %d\n", bc.nElems)
	//fmt.Printf("Client0 elems for rem: %v\n", bc.elems)
	return crdt.Remove{Element: bc.elems[bc.rng.Intn(bc.nElems)]}
}

func (bc BenchClient) topKUpdate() (updArgs crdt.UpdateArguments) {
	rnd := bc.rng.Float64()
	if rnd < BENCH_ADD_RATE {
		return bc.topKAdd()
	}
	return bc.topKRem()
}

func (bc BenchClient) topKAdd() (updArgs crdt.UpdateArguments) {
	data := make([]byte, bc.rndDataSize)
	return crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: bc.rng.Int31n(bc.maxID), Score: bc.rng.Int31n(bc.maxScore), Data: &data}}
}

func (bc BenchClient) topKRem() (updArgs crdt.UpdateArguments) {
	return crdt.TopKRemove{Id: bc.rng.Int31n(bc.maxID)}
}

func (bc BenchClient) counterUpd() (updArgs crdt.UpdateArguments) {
	return crdt.Increment{Change: bc.rng.Int31n(bc.diff) + bc.minChange}
}

func (bc BenchClient) avgUpdate() (updArgs crdt.UpdateArguments) {
	rnd := bc.rng.Float64()
	if rnd < BENCH_ADD_RATE {
		return bc.avgAdd()
	}
	return bc.avgAddMultiple()
}

func (bc BenchClient) avgAdd() (updArgs crdt.UpdateArguments) {
	return crdt.AddValue{Value: bc.rng.Int63n(bc.maxSum) + 1}
}

func (bc BenchClient) avgAddMultiple() (updArgs crdt.UpdateArguments) {
	return crdt.AddMultipleValue{SumValue: bc.rng.Int63n(bc.maxSum) + 1, NAdds: bc.rng.Int63n(bc.maxNAdds) + 1}
}

/////////query/upds order funcs/////////

func setChangeOddsOrder(queryOdds []string, queryFuncs []string) {
	if strings.ToUpper(strings.TrimSpace(queryFuncs[0])) == FULL_READ {
		tmp := queryOdds[0]
		queryOdds[0] = queryOdds[1]
		queryOdds[1] = tmp
	}
}

func topKChangeOddsOrder(queryOdds []string, queryFuncs []string) {
	var fullPos, nPos, vPos int
	for i, funName := range queryFuncs {
		funName = strings.ToUpper(strings.TrimSpace(funName))
		if funName == "TOPV" {
			vPos = i
		} else if funName == "TOPN" {
			nPos = i
		} else {
			fullPos = i
		}
	}
	oddsF, oddsN, oddsV := queryOdds[fullPos], queryOdds[nPos], queryOdds[vPos]
	queryOdds[0], queryOdds[1], queryOdds[2] = oddsF, oddsN, oddsV
}

func topKQueryNameToFun(queryName string) func() crdt.ReadArguments {
	switch strings.ToUpper(strings.TrimSpace(queryName)) {
	case "TOPN":
		return dummyClient.topKTopN
	case "TOPV":
		return dummyClient.topKAboveValue
	}
	return dummyClient.fullRead
}

///////////preloader funcs//////////////

func preload(bc BenchClient, keys []string) {
	var upds []antidote.UpdateObjectParams
	switch bc.crdtType {
	case proto.CRDTType_ORSET:
		upds = preloadSet(keys, bc.SetInfo)
	case proto.CRDTType_TOPK_RMV:
		upds = preloadTopK(keys, bc.TopKInfo)
	case proto.CRDTType_RRMAP:

	//These two don't need preload.
	case proto.CRDTType_COUNTER:
	case proto.CRDTType_AVG:
	}

	if upds != nil {
		antidote.SendProto(antidote.StaticUpdateObjs, antidote.CreateStaticUpdateObjs(nil, upds), bc.conn)
		antidote.ReceiveProto(bc.conn)
	}
}

func preloadSet(keys []string, setInfo SetInfo) (updParams []antidote.UpdateObjectParams) {
	setElems := make([]crdt.Element, len(setInfo.elems)/2)
	//Fill with every position that is not odd
	for i, j := 0, 0; i < len(setInfo.elems) && j < len(setElems); i, j = i+2, j+1 {
		setElems[j] = crdt.Element(setInfo.elems[i])
	}
	var upd crdt.UpdateArguments = crdt.AddAll{Elems: setElems}
	updParams = make([]antidote.UpdateObjectParams, len(keys))
	for i, key := range keys {
		updParams[i] = antidote.UpdateObjectParams{KeyParams: getKeyParams(key, proto.CRDTType_ORSET), UpdateArgs: &upd}
	}
	return
}

func preloadTopK(keys []string, topKInfo TopKInfo) (updParams []antidote.UpdateObjectParams) {
	adds := make([]crdt.UpdateArguments, topKInfo.maxID/2)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	data := make([]byte, topKInfo.rndDataSize)
	//Fill every non-odd ID
	for i, j := int32(0), 0; i < topKInfo.maxID && j < len(adds); i, j = i+2, j+1 {
		adds[j] = crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: i, Score: rng.Int31n(topKInfo.maxScore), Data: &data}}
	}
	updParams = make([]antidote.UpdateObjectParams, len(keys)*len(adds))
	i := 0
	for _, key := range keys {
		keyParam := getKeyParams(key, proto.CRDTType_TOPK_RMV)
		for _, add := range adds {
			updParams[i] = antidote.UpdateObjectParams{KeyParams: keyParam, UpdateArgs: &add}
			i++
		}
	}
	return
}

/////////preparation funcs//////////////

func updateBenchClientWithKeys(baseClient *BenchClient, clientN int) {
	BENCH_KEY_TYPE = strings.ToUpper(strings.TrimSpace(BENCH_KEY_TYPE))
	switch BENCH_KEY_TYPE {
	case "SINGLE":
		baseClient.key = "0"
	case "SHARED":
		baseClient.keys = make([]string, BENCH_N_KEYS)
		for i := 0; i < BENCH_N_KEYS; i++ {
			baseClient.keys[i] = strconv.FormatInt(int64(i), 10)
		}
	case "SPLIT":
		if BENCH_N_KEYS < TEST_ROUTINES {
			if clientN == 0 {
				fmt.Println("Warning - less keys than clients! Each key will be associated to more than one client.")
			}
			baseClient.key = strconv.FormatInt(int64(clientN%BENCH_N_KEYS), 10)
			return
		}
		nKeys := BENCH_N_KEYS / TEST_ROUTINES
		remainder := BENCH_N_KEYS % TEST_ROUTINES
		if clientN < remainder {
			//Distribute the remainder by the first clients
			nKeys++
		}
		baseClient.keys = make([]string, nKeys)
		start := nKeys * clientN
		if remainder >= clientN && remainder != 0 {
			//This client doesn't have remainder but needs to take in consideration the previous clients that do
			start = (nKeys + 1) * remainder
			start += (clientN - remainder) * nKeys
		}
		for i := 0; i < nKeys; i++ {
			baseClient.keys[i] = strconv.FormatInt(int64(start+i), 10)
		}
	case "PER_CLIENT":
		firstKey := clientN * BENCH_N_KEYS
		baseClient.keys = make([]string, BENCH_N_KEYS)
		for i := 0; i < BENCH_N_KEYS; i++ {
			baseClient.keys[i] = strconv.FormatInt(int64(firstKey+i), 10)
		}
	}

}

func updateBenchClientWithCrdtInfo(baseClient *BenchClient) {
	switch baseClient.crdtType {
	case proto.CRDTType_ORSET:
		elems := make([]crdt.Element, baseSetInfo.nElems)
		for i := range elems {
			elems[i] = crdt.Element(strconv.FormatInt(int64(i), 10))
		}
		baseClient.SetInfo = SetInfo{elems: elems, nElems: baseSetInfo.nElems}
	case proto.CRDTType_RRMAP:
		//TODO
	case proto.CRDTType_TOPK_RMV:
		baseClient.TopKInfo = TopKInfo{maxID: baseTopKInfo.maxID, maxScore: baseTopKInfo.maxScore, topN: baseTopKInfo.topN,
			topAbove: baseTopKInfo.topAbove, rndDataSize: baseTopKInfo.rndDataSize}
	case proto.CRDTType_COUNTER:
		baseClient.CounterInfo = CounterInfo{maxChange: baseCounterInfo.maxChange, minChange: baseCounterInfo.minChange, diff: baseCounterInfo.diff}
	case proto.CRDTType_AVG:
		baseClient.AvgInfo = AvgInfo{maxSum: baseAvgInfo.maxSum, maxNAdds: baseAvgInfo.maxNAdds}
	}
}

func selectCrdtType(crdtString string) proto.CRDTType {
	switch strings.ToUpper(crdtString) {
	case "ORSET":
		return proto.CRDTType_ORSET
	case "RRMAP":
		return proto.CRDTType_RRMAP
	case "TOPK":
		return proto.CRDTType_TOPK_RMV
	case "COUNTER":
		return proto.CRDTType_COUNTER
	case "AVG":
		return proto.CRDTType_AVG
	}
	return 0
}

func selectQueryFuns(crdtType proto.CRDTType, funsNames []string, queryOdds []string, client BenchClient) (func() crdt.ReadArguments, []func() crdt.ReadArguments) {
	switch crdtType {
	case proto.CRDTType_ORSET:
		if len(funsNames) > 1 {
			setChangeOddsOrder(queryOdds, funsNames)
			return client.setQuery, nil
		}
		funS := strings.ToUpper(strings.TrimSpace(funsNames[0]))
		if funS == "LOOKUP" {
			return client.setLookup, nil
		}
	case proto.CRDTType_RRMAP:

	case proto.CRDTType_TOPK_RMV:
		if len(funsNames) > 2 {
			topKChangeOddsOrder(queryOdds, funsNames)
			return client.topKQuery, nil
		} else if len(funsNames) == 2 {
			fun0, fun1 := topKQueryNameToFun(funsNames[0]), topKQueryNameToFun(funsNames[1])
			return nil, []func() crdt.ReadArguments{fun0, fun1}
		}
		return topKQueryNameToFun(funsNames[0]), nil

	case proto.CRDTType_COUNTER:
		return client.fullRead, nil
	case proto.CRDTType_AVG:
		return client.fullRead, nil
	}
	return client.fullRead, nil
}

func selectUpdateFuns(crdtType proto.CRDTType, funsNames []string, client BenchClient) func() crdt.UpdateArguments {
	switch crdtType {
	case proto.CRDTType_ORSET:
		if len(funsNames) > 1 {
			return client.setUpdate
		}
		funS := strings.ToUpper(strings.TrimSpace(funsNames[0]))
		if funS == "REM" {
			return client.setRem
		}
		return client.setAdd
	case proto.CRDTType_RRMAP:

	case proto.CRDTType_TOPK_RMV:
		if len(funsNames) > 1 {
			return client.topKUpdate
		}
		funS := strings.ToUpper(strings.TrimSpace(funsNames[0]))
		if funS == "REM" {
			return client.topKRem
		}
		return client.topKAdd
	case proto.CRDTType_COUNTER:
		return client.counterUpd
	case proto.CRDTType_AVG:
		if len(funsNames) > 1 {
			return client.avgUpdate
		}
		funS := strings.ToUpper(strings.TrimSpace(funsNames[0]))
		if funS == "ADD" {
			return client.avgAdd
		}
		return client.avgAddMultiple
	}
	return nil
}

func processQueryOdds(queryOddsStr []string) (queryOdds []float64) {
	queryOdds = make([]float64, len(queryOddsStr))
	curr, total := 0.0, 0.0
	for i, str := range queryOddsStr {
		curr, _ = strconv.ParseFloat(str, 64)
		total += curr
		queryOdds[i] = total
	}
	return
}

///////////helper funcs/////////////////

func getAllKeys() (allKeys []string) {
	switch BENCH_KEY_TYPE {
	case "SHARED":
		allKeys = make([]string, BENCH_N_KEYS)
	case "SINGLE":
		allKeys = make([]string, 1)
	case "SPLIT":
		allKeys = make([]string, BENCH_N_KEYS)
	case "PER_CLIENT":
		allKeys = make([]string, BENCH_N_KEYS*TEST_ROUTINES)
	}
	for i := 0; i < len(allKeys); i++ {
		allKeys[i] = strconv.FormatInt(int64(i), 10)
	}
	return
}

func (bc BenchClient) getKey() string {
	nKeys := len(bc.keys)
	if nKeys > 0 {
		return bc.keys[bc.rng.Intn(nKeys)]
	}
	return bc.key
}

func getKeyParams(key string, crdtType proto.CRDTType) antidote.KeyParams {
	return antidote.KeyParams{Key: key, Bucket: BUCKET, CrdtType: crdtType}
}

func (bc BenchClient) sendQuery(read crdt.ReadArguments) {
	if read == FULL_READ_ARGS {
		toSend := []antidote.ReadObjectParams{antidote.ReadObjectParams{KeyParams: getKeyParams(bc.getKey(), bc.crdtType)}}
		antidote.SendProto(antidote.StaticReadObjs, antidote.CreateStaticReadObjs(nil, toSend), bc.conn)
	} else {
		toSend := []antidote.ReadObjectParams{antidote.ReadObjectParams{KeyParams: getKeyParams(bc.getKey(), bc.crdtType), ReadArgs: read}}
		antidote.SendProto(antidote.StaticRead, antidote.CreateStaticRead(nil, nil, toSend), bc.conn)
	}
	*bc.waitFor++
	if *bc.waitFor >= bc.maxWaitFor {
		bc.receiveProtos()
	}
}

func (bc BenchClient) sendQueries(reads []crdt.ReadArguments) {
	toSend := make([]antidote.ReadObjectParams, len(reads))
	keyParams := getKeyParams(bc.getKey(), bc.crdtType)
	for i, read := range reads {
		if read == FULL_READ_ARGS {
			toSend[i] = antidote.ReadObjectParams{KeyParams: keyParams}
		} else {
			toSend[i] = antidote.ReadObjectParams{KeyParams: keyParams, ReadArgs: read}
		}
	}
	antidote.SendProto(antidote.StaticRead, antidote.CreateStaticRead(nil, nil, toSend), bc.conn)
	*bc.waitFor++
	if *bc.waitFor >= bc.maxWaitFor {
		bc.receiveProtos()
	}
}

func (bc BenchClient) sendUpdate(upd crdt.UpdateArguments) {
	toSend := []antidote.UpdateObjectParams{antidote.UpdateObjectParams{KeyParams: getKeyParams(bc.getKey(), bc.crdtType), UpdateArgs: &upd}}
	antidote.SendProto(antidote.StaticUpdateObjs, antidote.CreateStaticUpdateObjs(nil, toSend), bc.conn)
	*bc.waitFor++
	if *bc.waitFor >= bc.maxWaitFor {
		bc.receiveProtos()
	}
}

func (bc BenchClient) sendUpdates(upds []crdt.UpdateArguments) {
	toSend, keyParams := make([]antidote.UpdateObjectParams, len(upds)), getKeyParams(bc.getKey(), bc.crdtType)
	for i, upd := range upds {
		toSend[i] = antidote.UpdateObjectParams{KeyParams: keyParams, UpdateArgs: &upd}
	}
	antidote.SendProto(antidote.StaticUpdateObjs, antidote.CreateStaticUpdateObjs(nil, toSend), bc.conn)
	*bc.waitFor++
	if *bc.waitFor >= bc.maxWaitFor {
		bc.receiveProtos()
	}
}

func (bc BenchClient) receiveProtos() {
	waitFor := *bc.waitFor
	for i := 0; i < waitFor; i++ {
		antidote.ReceiveProto(bc.conn)
	}
	*bc.waitFor = 0
}

/*
total time | section time | reads | upds | reads/s | upds/s | latency
*/

/////////////////configs///////////////////

func loadBenchConfigs(configs *tools.ConfigLoader) {
	BENCH_N_KEYS, BENCH_KEY_TYPE = configs.GetIntConfig("keys", 1), configs.GetOrDefault("keyType", "SINGLE")
	BENCH_ADD_RATE, BENCH_PART_READ_RATE = configs.GetFloatConfig("addRate", 1), configs.GetFloatConfig("partReadRate", 0)
	BENCH_CRDT, BENCH_DO_PRELOAD = selectCrdtType(configs.GetOrDefault("crdtType", "ORSET")), configs.GetBoolConfig("doPreload", false)
	//QUERY_FUNCS, UPDATE_FUNCS, BENCH_QUERY_RATES
	QUERY_FUNCS = strings.Split(strings.TrimSpace(configs.GetOrDefault("queryFuns", "FULL")), " ")
	UPDATE_FUNCS = strings.Split(strings.TrimSpace(configs.GetOrDefault("updateFuns", "ADD")), " ")
	BENCH_QUERY_RATES = strings.Split(strings.TrimSpace(configs.GetOrDefault("queryRates", "1")), " ")
	OPS_PER_TXN, N_TXNS_BEFORE_WAIT = configs.GetIntConfig("opsPerTxn", 1), configs.GetIntConfig("nTxnsBeforeWait", 1)

	readCrdtConfigs(configs)
}

func readCrdtConfigs(configs *tools.ConfigLoader) {
	fmt.Println("CRDT type in configs:", BENCH_CRDT)
	switch BENCH_CRDT {
	case proto.CRDTType_ORSET:
		baseSetInfo.nElems = configs.GetIntConfig("nSetElems", 1000)
	case proto.CRDTType_RRMAP:
		baseRRInfo.nKeys, baseRRInfo.keyType = configs.GetIntConfig("rrKeys", 1000), configs.GetOrDefault("rrKeyType", "SHARED")
		baseRRInfo.innerCrdtType = selectCrdtType(configs.GetOrDefault("innerCrdtType", "COUNTER"))
		//TODO: Inner map info...
	case proto.CRDTType_TOPK_RMV:
		baseTopKInfo.maxID, baseTopKInfo.maxScore = int32(configs.GetIntConfig("maxID", 1000)), int32(configs.GetIntConfig("maxScore", 10000))
		baseTopKInfo.topN, baseTopKInfo.topAbove = int32(configs.GetIntConfig("topN", 10)), int32(configs.GetIntConfig("topAbove", 9000))
		baseTopKInfo.rndDataSize = configs.GetIntConfig("randomDataSize", 0)
	case proto.CRDTType_COUNTER:
		baseCounterInfo.minChange, baseCounterInfo.maxChange = int32(configs.GetIntConfig("minChange", 0)), int32(configs.GetIntConfig("maxChange", 10))
		baseCounterInfo.diff = baseCounterInfo.maxChange - baseCounterInfo.minChange
	case proto.CRDTType_AVG:
		baseAvgInfo.maxSum, baseAvgInfo.maxNAdds = int64(configs.GetIntConfig("maxSum", 100)), int64(configs.GetIntConfig("maxNAdds", 5))
	}
}

///////////////stats funcs/////////////////

func writeBenchStatsFile(stats [][]BenchStats, crdtType proto.CRDTType) {
	statsPerPart := convertBenchStats(stats)

	file := getStatsFileToWrite(proto.CRDTType_name[int32(crdtType)] + "_" + "benchStats")
	if file == nil {
		return
	}
	defer file.Close()

	//Space for final
	totalData := make([][]string, len(statsPerPart)+1)
	header := []string{"Total time", "Section time", "Reads", "Reads/s", "Upds", "Upds/s", "Ops", "Ops/s", "Average latency (ms)"}

	partReads, partUpds, partOps, partTime := 0, 0, 0, int64(0)
	readS, updsS, opsS, latency := 0.0, 0.0, 0.0, 0.0
	totalReads, totalUpds, totalTime := 0, 0, int64(0)
	for i, partStat := range statsPerPart {
		for _, clientStat := range partStat {
			partReads, partUpds, partTime = partReads+clientStat.nReads, partUpds+clientStat.nUpds, partTime+clientStat.timeSpent
		}
		partOps = partReads + partUpds
		partTime /= int64(TEST_ROUTINES)
		totalReads, totalUpds, totalTime = totalReads+partReads, totalUpds+partUpds, totalTime+partTime
		//Write data, calculate /s & latency
		readS, updsS, opsS = float64(partReads)/float64(partTime)*1000, float64(partUpds)/float64(partTime)*1000, float64(partOps)/float64(partTime)*1000
		latency = float64(partTime*int64(TEST_ROUTINES)) / float64(partReads+partUpds)

		totalData[i] = []string{strconv.FormatInt(totalTime, 10), strconv.FormatInt(partTime, 10), strconv.FormatInt(int64(partReads), 10),
			strconv.FormatFloat(readS, 'f', 10, 64), strconv.FormatInt(int64(partUpds), 10), strconv.FormatFloat(updsS, 'f', 10, 64),
			strconv.FormatInt(int64(partOps), 10), strconv.FormatFloat(opsS, 'f', 10, 64), strconv.FormatFloat(latency, 'f', 10, 64)}
		partReads, partUpds, partTime = 0, 0, 0
	}
	totalOps := totalReads + totalUpds
	readS, updsS, opsS = float64(totalReads)/float64(totalTime)*1000, float64(totalUpds)/float64(totalTime)*1000, float64(totalOps)/float64(totalTime)*1000
	latency = float64(totalTime*int64(TEST_ROUTINES)) / float64(totalReads+totalUpds)
	ignore(header)
	totalData[len(statsPerPart)] = []string{strconv.FormatInt(totalTime, 10), strconv.FormatInt(partTime, 10), strconv.FormatInt(int64(totalReads), 10),
		strconv.FormatFloat(readS, 'f', 10, 64), strconv.FormatInt(int64(totalUpds), 10), strconv.FormatFloat(updsS, 'f', 10, 64),
		strconv.FormatInt(int64(totalOps), 10), strconv.FormatFloat(opsS, 'f', 10, 64), strconv.FormatFloat(latency, 'f', 10, 64)}

	writer := csv.NewWriter(file)
	writer.Comma = ';'
	defer writer.Flush()

	writer.Write(header)
	for _, line := range totalData {
		writer.Write(line)
	}

	fmt.Println("Bench statistics saved successfully.")
}

func convertBenchStats(stats [][]BenchStats) (convStats [][]BenchStats) {
	sizeToUse := int(math.MaxInt32)
	for _, statsSlice := range stats {
		if len(statsSlice) < sizeToUse {
			sizeToUse = len(statsSlice)
		}
	}
	convStats = make([][]BenchStats, sizeToUse)
	var currStatSlice []BenchStats

	for i := range convStats {
		currStatSlice = make([]BenchStats, len(stats))
		for j, stat := range stats {
			currStatSlice[j] = stat[i]
		}
		convStats[i] = currStatSlice
	}

	return
}
