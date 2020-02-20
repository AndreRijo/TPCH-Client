package tpch

import (
	"antidote"
	"crdt"
	"fmt"
	"math/rand"
	"net"
	"proto"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	pb "github.com/golang/protobuf/proto"
)

//In order to allow multiple independent clients executing queries, each client must
//have one instance of this object, which will contain his private variables (namelly,
//its own connections)
type QueryClient struct {
	serverConns []net.Conn
	indexServer int
}

type QueryClientResult struct {
	duration, nReads, nCycles float64
}

const (
	QUERIES_PER_TXN = 6
)

var (
	//Filled automatically by configLoader
	PRINT_QUERY, QUERY_BENCH bool
	TEST_ROUTINES            int
	TEST_DURATION            int64
	QUERY_WAIT               time.Duration
	queryFuncs               []func(QueryClient) int //queries to execute
)

func startQueriesBench() {
	printExecutionTimes()
	fmt.Println("Waiting to start for queries...")
	time.Sleep(QUERY_WAIT * time.Millisecond)
	fmt.Println("Starting queries")

	chans := make([]chan QueryClientResult, TEST_ROUTINES)
	results := make([]QueryClientResult, TEST_ROUTINES)
	serverPerClient := make([]int, TEST_ROUTINES)
	for i := 0; i < TEST_ROUTINES; i++ {
		//serverN := rand.Intn(len(servers))
		serverN := 0
		//fmt.Println("Starting query client", i, "with index server", servers[serverN])
		chans[i] = make(chan QueryClientResult)
		serverPerClient[i] = serverN
		go queryBench(serverN, chans[i])
	}
	fmt.Println("Query clients started...")

	//TODO: Go back on this
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

	//for i, channel := range chans {
	//results[i] = <-channel
	//}
	fmt.Println()
	fmt.Println("All query clients have finished.")
	totalQueries, totalReads, avgDuration := 0.0, 0.0, 0.0
	for i, result := range results {
		fmt.Printf("%d[%d]: QueryTxns: %f, Queries: %f, QueryTxns/s: %f, Query/s: %f, Reads: %f, Reads/s: %f\n", i, serverPerClient[i],
			result.nCycles, result.nCycles*float64(len(queryFuncs)), (result.nCycles/result.duration)*1000,
			(result.nCycles*float64(len(queryFuncs))/result.duration)*1000, result.nReads, (result.nReads/result.duration)*1000)
		totalQueries += result.nCycles
		totalReads += result.nReads
		avgDuration += result.duration
	}
	avgDuration /= float64(len(results))
	fmt.Printf("Totals: QueryTxns: %f, Queries: %f, QueryTxns/s: %f, Query/s: %f, Reads: %f, Reads/s: %f\n", totalQueries, totalQueries*float64(len(queryFuncs)),
		(totalQueries/avgDuration)*1000, (totalQueries*float64(len(queryFuncs))/avgDuration)*1000, totalReads, (totalReads/avgDuration)*1000)
}

func queryBench(defaultServer int, resultChan chan QueryClientResult) {
	conns := make([]net.Conn, len(servers))
	for i := range conns {
		conns[i], _ = net.Dial("tcp", servers[i])
	}
	client := QueryClient{serverConns: conns, indexServer: defaultServer}
	cycles, reads := 0, 0
	startTime := time.Now().UnixNano() / 1000000
	//endTime, lastPrint, printTime := int64(0), startTime, TEST_DURATION/5
	endTime := int64(0)

	for endTime-startTime < TEST_DURATION {
		reads += sendQ3(client)
		reads += sendQ5(client)
		reads += sendQ11(client)
		reads += sendQ14(client)
		reads += sendQ15(client)
		reads += sendQ18(client)

		/*
			for _, query := range queryFuncs {
				reads += query(client)
			}
		*/
		cycles++
		endTime = time.Now().UnixNano() / 1000000
		/*
			if endTime-lastPrint > printTime {
				//fmt.Printf("Query set number %d complete.\n", cycles)
				fmt.Printf("Query sets done so far %d\n", cycles)
				lastPrint = endTime
			}
		*/
	}
	resultChan <- QueryClientResult{duration: float64(endTime - startTime), nCycles: float64(cycles), nReads: float64(reads)}
}

func sendQueries(conn net.Conn) {
	client := QueryClient{serverConns: conns, indexServer: DEFAULT_REPLICA}
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
	fmt.Println("Starting to prepare and send upds")
	//sendDataChangesV2(readUpds())
}

func sendQ3(client QueryClient) (nRequests int) {
	rndSeg := procTables.Segments[rand.Intn(5)]
	rndDay := 1 + rand.Int63n(31)

	readParam := []antidote.ReadObjectParams{antidote.ReadObjectParams{
		KeyParams: antidote.KeyParams{Key: SEGM_DELAY + rndSeg + strconv.FormatInt(rndDay, 10), CrdtType: proto.CRDTType_TOPK_RMV, Bucket: buckets[INDEX_BKT]},
		ReadArgs:  crdt.GetTopNArguments{NumberEntries: 10},
	}}
	//topKProto := sendReceiveReadProto(client, []antidote.ReadObjectParams{}, readParam).GetObjects().GetObjects()[0].GetTopk()
	topKProto := mergeIndex(sendReceiveIndexQuery(client, nil, readParam), 3)[0].GetTopk()
	values := topKProto.GetValues()

	if !INDEX_WITH_FULL_DATA {
		orderIDs := make([]int32, 10)
		written := 0
		for _, pair := range values {
			orderIDs[written] = pair.GetPlayerId()
			written++
		}

		orderIDs = orderIDs[:written]

		orderDate, shipPriority := headers[ORDERS][4], headers[ORDERS][7]
		registerArgs := crdt.StateReadArguments{}
		orderMapArgs := crdt.EmbMapPartialArguments{Args: map[string]crdt.ReadArguments{orderDate: registerArgs, shipPriority: registerArgs}}
		states := getTableState(client, orderMapArgs, ORDERS, orderIDs, procTables.OrderkeyToRegionkey)

		if PRINT_QUERY {
			fmt.Printf("Q3: top 10 orders for segment %s not delivered as of %d:03:1995:\n", rndSeg, rndDay)
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
		fmt.Printf("Q3: top 10 orders for segment %s not delivered as of %d:03:1995:\n", rndSeg, rndDay)
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

func sendQ5(client QueryClient) (nRequests int) {
	rndRegion := rand.Intn(len(procTables.Regions))
	rndRegionN := procTables.Regions[rndRegion].R_NAME
	rndYear := 1993 + rand.Int63n(5)
	bktI, serverI := getIndexOffset(client, rndRegion)

	readParam := []antidote.ReadObjectParams{antidote.ReadObjectParams{KeyParams: antidote.KeyParams{
		Key: NATION_REVENUE + rndRegionN + strconv.FormatInt(rndYear, 10), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bktI]}},
	}

	//replyProto := sendReceiveReadObjsProto(client, readParam, bktI-INDEX_BKT)
	replyProto := sendReceiveReadObjsProto(client, readParam, serverI)
	mapState := crdt.ReadRespProtoToAntidoteState(replyProto.GetObjects().GetObjects()[0], proto.CRDTType_RRMAP, proto.READType_FULL).(crdt.EmbMapEntryState)

	if PRINT_QUERY {
		fmt.Println("Q5: Values for", rndRegion, "in year", rndYear)
		for nation, valueState := range mapState.States {
			fmt.Printf("%s: %d\n", nation, valueState.(crdt.CounterState).Value)
		}
	}

	return 1
}

func sendQ11(client QueryClient) (nRequests int) {
	//Unfortunatelly we can't use the topk query of "only values above min" here as we need to fetch the min from the database first.
	rndNation := procTables.Nations[rand.Intn(len(procTables.Nations))]
	rndNationN, rndNationR := rndNation.N_NAME, rndNation.N_REGIONKEY
	bktI, serverI := getIndexOffset(client, int(rndNationR))

	readParam := []antidote.ReadObjectParams{
		antidote.ReadObjectParams{KeyParams: antidote.KeyParams{Key: IMP_SUPPLY + rndNationN, CrdtType: proto.CRDTType_TOPK_RMV, Bucket: buckets[bktI]}},
		antidote.ReadObjectParams{KeyParams: antidote.KeyParams{Key: SUM_SUPPLY + rndNationN, CrdtType: proto.CRDTType_COUNTER, Bucket: buckets[bktI]}},
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

func sendQ14(client QueryClient) (nRequests int) {
	rndForDate := rand.Int63n(60) //60 months from 1993 to 1997 (5 years)
	year, month := 1993+rndForDate/12, 1+rndForDate%12
	date := strconv.FormatInt(year, 10) + strconv.FormatInt(month, 10)
	var avgProto *proto.ApbGetAverageResp

	//Need to send AvgFullRead instead of state reads in case the index is local. The merge function will return the reply as if it was a single average though.
	if isIndexGlobal {
		readParam := []antidote.ReadObjectParams{antidote.ReadObjectParams{
			KeyParams: antidote.KeyParams{Key: PROMO_PERCENTAGE + date, CrdtType: proto.CRDTType_AVG, Bucket: buckets[INDEX_BKT]},
		}}
		avgProto = sendReceiveReadObjsProto(client, readParam, client.indexServer).GetObjects().GetObjects()[0].GetAvg()
	} else {
		readParam := []antidote.ReadObjectParams{antidote.ReadObjectParams{
			KeyParams: antidote.KeyParams{Key: PROMO_PERCENTAGE + date, CrdtType: proto.CRDTType_AVG, Bucket: buckets[INDEX_BKT]},
			ReadArgs:  crdt.AvgGetFullArguments{},
		}}
		avgProto = mergeIndex(sendReceiveIndexQuery(client, nil, readParam), 14)[0].GetAvg()
	}

	if PRINT_QUERY {
		fmt.Printf("Q14: %d_%d: %f.\n", year, month, avgProto.GetAvg())
	}

	return len(client.serverConns)
}

func sendQ15(client QueryClient) (nRequests int) {
	rndQuarter := 1 + 3*rand.Int63n(4)
	rndYear := 1993 + rand.Int63n(5)
	date := strconv.FormatInt(rndYear, 10) + strconv.FormatInt(rndQuarter, 10)

	readParam := []antidote.ReadObjectParams{antidote.ReadObjectParams{
		KeyParams: antidote.KeyParams{Key: TOP_SUPPLIERS + date, CrdtType: proto.CRDTType_TOPK_RMV, Bucket: buckets[INDEX_BKT]},
		ReadArgs:  crdt.GetTopNArguments{NumberEntries: 1},
	}}
	//topkProto := sendReceiveReadProto(client, []antidote.ReadObjectParams{}, readParam).GetObjects().GetObjects()[0].GetTopk()
	topkProto := mergeIndex(sendReceiveIndexQuery(client, nil, readParam), 15)[0].GetTopk()

	if PRINT_QUERY {
		fmt.Printf("Q15: best supplier(s) for months [%d, %d] of year %d\n", rndQuarter, rndQuarter+2, rndYear)
	}
	values := topkProto.GetValues()
	if len(values) > 1 {
		sort.Slice(values, func(i, j int) bool { return values[i].GetScore() > values[j].GetScore() })
	}
	if PRINT_QUERY {
		for _, pair := range values {
			fmt.Printf("%d: %d\n", pair.GetPlayerId(), pair.GetScore())
		}
	}

	if isIndexGlobal {
		return 1
	}
	return len(client.serverConns)
}

func sendQ18(client QueryClient) (nRequests int) {
	//Might be worth to optimize this to download customers simultaneously with orders
	//However, this requires for the ID in the topK to refer to both keys
	//Also, theoretically this should be a single transaction instead of a static.
	rndQuantity := 312 + rand.Int31n(4)

	//Should ask for a GetTopN if we ever change the topK to not be top 100
	readParam := []antidote.ReadObjectParams{antidote.ReadObjectParams{
		KeyParams: antidote.KeyParams{Key: LARGE_ORDERS + strconv.FormatInt(int64(rndQuantity), 10), CrdtType: proto.CRDTType_TOPK_RMV, Bucket: buckets[INDEX_BKT]},
	}}
	//topkProto := sendReceiveReadObjsProto(readParam).GetObjects().GetObjects()[0].GetTopk()
	topkProto := mergeIndex(sendReceiveIndexQuery(client, readParam, nil), 18)[0].GetTopk()
	values := topkProto.GetValues()
	sort.Slice(values, func(i, j int) bool { return values[i].GetScore() > values[j].GetScore() })

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
				fmt.Printf("Q18: top 100 customers for large quantity orders with quantity above %d: no match found.", rndQuantity)
			}
			return
		}
		orderIDs = orderIDs[:written]

		orderDate, orderTotalPrice, orderCustKey := headers[ORDERS][4], headers[ORDERS][3], headers[ORDERS][1]
		registerArgs := crdt.StateReadArguments{}
		orderMapArgs := crdt.EmbMapPartialArguments{Args: map[string]crdt.ReadArguments{orderDate: registerArgs, orderTotalPrice: registerArgs, orderCustKey: registerArgs}}
		orderStates := getTableState(client, orderMapArgs, ORDERS, orderIDs, procTables.OrderkeyToRegionkey)
		/*
			if isMulti {
				readParams, orderIDsArgs := getReadArgsPerBucket(ORDERS)
				for _, orderID := range orderIDs {
					orderIDsArgs[procTables.OrderkeyToRegionkey(orderID)][strconv.FormatInt(int64(orderID), 10)] = orderMapArgs
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

		cName := headers[CUSTOMER][1]
		orderMapArgs = crdt.EmbMapPartialArguments{Args: map[string]crdt.ReadArguments{cName: registerArgs}}
		custStates := getTableState(client, orderMapArgs, CUSTOMER, customerIDs, procTables.Custkey32ToRegionkey)

		if PRINT_QUERY {
			fmt.Printf("Q18 top 100 customers for large quantity orders with quantity above %d\n", rndQuantity)
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
			fmt.Printf("Q18: top 100 customers for large quantity orders with quantity above %d: no match found.", rndQuantity)
		} else {
			fmt.Printf("Q18 top 100 customers for large quantity orders with quantity above %d\n", rndQuantity)
			var extraData []string
			for _, pair := range values {
				extraData = unpackIndexExtraData(pair.GetData(), Q18_N_EXTRA_DATA)
				fmt.Printf("%s | %s | %d | %s | %s | %d\n",
					extraData[0], extraData[1], pair.GetPlayerId(), extraData[2], extraData[3], pair.GetScore())
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

//nConn will ALWAYS be 0 when indexes aren't partitioned!
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

func mergeIndex(protos []*proto.ApbStaticReadObjectsResp, queryN int) []*proto.ApbReadObjectResp {
	switch queryN {
	case 3:
		return mergeQ3IndexReply(protos)
	case 14:
		return mergeQ14IndexReply(protos)
	case 15:
		return mergeQ15IndexReply(protos)
	case 18:
		return mergeQ18IndexReply(protos)
	default:
		return nil
	}
}

//Q5 doesn't need merge as it only needs to ask one region (i.e., one replica)
//Q11 also doesn't need merge as it is single nation.
//Q18 needs merge as it is the top 100 of all customers.

func mergeQ3IndexReply(protos []*proto.ApbStaticReadObjectsResp) (merged []*proto.ApbReadObjectResp) {
	return mergeTopKProtos(protos, 10)
}

//Needs to merge as the info for a date is spread across the CRDTs. A partialRead for AVG is needed in this case.
func mergeQ14IndexReply(protos []*proto.ApbStaticReadObjectsResp) (merged []*proto.ApbReadObjectResp) {
	protoObjs := getObjectsFromStaticReadResp(protos)
	var sum, nAdds int64
	var currProto *proto.ApbAvgGetFullReadResp
	for _, protos := range protoObjs {
		currProto = protos[0].GetPartread().GetAvg().GetGetfull()
		sum += currProto.GetSum()
		nAdds += currProto.GetNAdds()
	}
	result := float64(sum) / float64(nAdds)
	return []*proto.ApbReadObjectResp{&proto.ApbReadObjectResp{Avg: &proto.ApbGetAverageResp{Avg: pb.Float64(result)}}}
}

//Q15 also needs merge.
func mergeQ15IndexReply(protos []*proto.ApbStaticReadObjectsResp) (merged []*proto.ApbReadObjectResp) {
	protoObjs := getObjectsFromStaticReadResp(protos)
	max, amount := int32(-1), 0
	var currPairs []*proto.ApbIntPair
	//Find max and count how many
	for _, topProtos := range protoObjs {
		currPairs = topProtos[0].GetTopk().GetValues()
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
		currPairs = topProtos[0].GetTopk().GetValues()
		if currPairs[0].GetScore() == max {
			for _, pair := range currPairs {
				values[written] = pair
				written++
			}
		}
	}
	return []*proto.ApbReadObjectResp{&proto.ApbReadObjectResp{Topk: &proto.ApbGetTopkResp{Values: values}}}
}

func mergeQ18IndexReply(protos []*proto.ApbStaticReadObjectsResp) (merged []*proto.ApbReadObjectResp) {
	return mergeTopKProtos(protos, 100)
}

func mergeTopKProtos(protos []*proto.ApbStaticReadObjectsResp, target int) (merged []*proto.ApbReadObjectResp) {
	protoObjs := getObjectsFromStaticReadResp(protos)
	values := make([]*proto.ApbIntPair, 10)
	currI := make([]int, len(protos))
	end, max, playerId, replicaI := true, int32(-1), int32(-1), 0
	var currPair *proto.ApbIntPair
	written := 0
	for written < target {
		for i, topProtos := range protoObjs {
			if len(topProtos[0].GetTopk().GetValues()) > currI[i] {
				end = false
				currPair = topProtos[0].GetTopk().GetValues()[currI[i]]
				if currPair.GetScore() > max {
					max, playerId, replicaI = currPair.GetScore(), currPair.GetPlayerId(), i
				}
			}
		}
		if end {
			break
		}
		values[written] = &proto.ApbIntPair{PlayerId: pb.Int32(playerId), Score: pb.Int32(max)}
		currI[replicaI]++
		written++
		max, end = -1, true
	}
	values = values[:written]
	return []*proto.ApbReadObjectResp{&proto.ApbReadObjectResp{Topk: &proto.ApbGetTopkResp{Values: values}}}
}

func getObjectsFromStaticReadResp(protos []*proto.ApbStaticReadObjectsResp) (protoObjs [][]*proto.ApbReadObjectResp) {
	protoObjs = make([][]*proto.ApbReadObjectResp, len(protos))
	for i, proto := range protos {
		protoObjs[i] = proto.GetObjects().GetObjects()
	}
	return
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
	rndSeg := procTables.Segments[rand.Intn(5)]
	rndDay := int8(1 + rand.Int63n(31))
	minDate, maxDate := &Date{YEAR: 1995, MONTH: 03, DAY: rndDay + 1}, &Date{YEAR: 1995, MONTH: 03, DAY: rndDay - 1}

	readParams := make(map[int8][]antidote.ReadObjectParams)
	i, regionLen := int8(0), int8(len(procTables.Regions))
	for ; i < regionLen; i++ {
		readParams[i] = []antidote.ReadObjectParams{
			antidote.ReadObjectParams{
				KeyParams: antidote.KeyParams{Key: tableNames[ORDERS], CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[i]},
				ReadArgs:  crdt.StateReadArguments{},
			},
			antidote.ReadObjectParams{
				KeyParams: antidote.KeyParams{Key: tableNames[LINEITEM], CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[i]},
				ReadArgs:  crdt.StateReadArguments{},
			},
			antidote.ReadObjectParams{
				KeyParams: antidote.KeyParams{Key: tableNames[CUSTOMER], CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[i]},
				ReadArgs:  crdt.StateReadArguments{},
			},
		}
	}
	replies := sendReceiveBucketFullReadProtos(readParams)
	states := mergeMultipleMapReplies(replies, proto.READType_FULL)
	orders, items, customers := states[0], states[1], states[2]

	l_orderId, l_shipdate := headers[LINEITEM][L_ORDERKEY], headers[LINEITEM][L_SHIPDATE]
	l_price, l_discount := headers[LINEITEM][L_EXTENDEDPRICE], headers[LINEITEM][L_DISCOUNT]
	o_orderdate, o_custkey, o_shippriority := headers[ORDERS][O_ORDERDATE], headers[ORDERS][O_CUSTKEY], headers[ORDERS][O_SHIPPRIOTITY]
	c_mktsegment := headers[CUSTOMER][C_MKTSEGMENT]

	var shipdate, orderDate *Date
	var price, discount float64
	var order, customer crdt.EmbMapEntryState
	var orderID string
	sums := make(map[string]float64)

	for _, item := range items {
		shipdate = createDate(embMapRegisterToString(l_shipdate, item))
		if shipdate.isHigherOrEqual(minDate) {
			orderID = embMapRegisterToString(l_orderId, item)
			order = orders[orderID]
			orderDate = createDate(embMapRegisterToString(o_orderdate, order))
			if orderDate.isSmallerOrEqual(maxDate) {
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
