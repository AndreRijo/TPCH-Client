package client

import (
	"fmt"
	"math/rand"
	"potionDB/crdt/crdt"
	"potionDB/crdt/proto"
	antidote "potionDB/potionDB/components"
	"strconv"
	"strings"
	"time"

	//"tpch_client/src/tpch"
	tpch "tpch_data_processor/tpch"
)

//TODO: Maybe remake clientIndex, clientUpdates and clientQueries to have a file per query?

/*
	Updates count:
	Q3: Max is nOrders * nSegments. nSegments is 5, nOrders is 150000 and 1500000 respectively for 0.1SF and 1SF, thus max is 750000  and 7500000.
	    On 0.1SF, seems to be 159306, and on 1SF 1587920.
	Q5: nNations * 5 years (1993-1997) = 25*5 = 125, always, for all SFs.
	Q11: Max is nNations + nNations * nParts, which, for 0.1SF and 1SF: 25 + 25*20000 = 500025; 25 + 25*200000 = 5000025.
		On 0.1SF, seems to be 75379, and on 1SF 753341.
	Q14: 5 years (1993-1997) * 12 months = 60, always, for all SFs.
	Q15: theorically not predefined. 5 years * 4 months, with each entry containing up to len(suppliers). Max is 20000 for 0.1SF, 200000 for 1SF.
		 On 0.1SF, seems to be 20000, and on 1SF 200000.
	Q18: On 0.1SF it is reporting 1, while on 1SF it is reporting 4.
*/
//Top queries: Q2 (top 1 + tied), Q3 (top 10), Q10 (top 20), Q15 (top 1), Q18 (top 100 but usually the top itself has only around 10 entries), Q21 (top 100)
type PairInt struct {
	first  int32
	second int32
}

type PairIntFloat struct {
	first  float64
	second int64
}

// Idea: allow concurrent goroutines to use different versions of Orders and Lineitems table
type TableInfo struct {
	*tpch.Tables
}

type Q1Data struct {
	sumQuantity, nItems                            int //avg can be calculated from sums and nItems
	sumPrice, sumDiscPrice, sumCharge, sumDiscount float64
}

type Q8YearPair struct {
	sum1995, sum1996 float64
}

type Q13Info struct {
	custToNOrders      map[int32]int8            //cust->nOrders
	wordsToCustNOrders map[string]map[int32]int8 //cust->nOrders with word1word2
	//nOrders            map[int8]int32            //nOrders -> how many customers
	wordsToNOrders map[string]map[int8]int32 //nOrders WITHOUT word1word2 -> how many customers
}

type Q16Info struct {
	suppsPerCategory map[string]map[string]map[string]map[int32]struct{} //size -> brand -> type -> suppkey
	complainSupps    map[int32]bool
}

type Q17Info struct {
	sum, count       map[string]int64            //sum: quantity
	pricePerQuantity map[string]map[int8]float64 //brand+container->quantity->sum(extendedprice)
}

const (
	MIN_MONTH_DAY, MAX_MONTH_DAY                       = int8(1), int8(31)
	Q3_N_EXTRA_DATA, Q18_N_EXTRA_DATA, Q2_N_EXTRA_DATA = 2, 4, 7
	Q10_N_EXTRA_DATA                                   = 7
	MIN_Q6_DISCOUNT, MAX_Q6_DISCOUNT                   = int8(1), int8(10)
	Q16_ALL                                            = "a"
	//Q17_AVG, Q17_TOP                                   = "a", "t"
	Q17_AVG, Q17_MAP                             = "a", "m"
	Q20_NAME, Q20_ADDRESS, Q20_AVAILQTY, Q20_SUM = "N", "A", "Q", "S"
	Q22_AVG                                      = "AVG"
)

const (
	Q1_SUM_QTY, Q1_SUM_BASE_PRICE, Q1_SUM_DISC_PRICE, Q1_SUM_CHARGE = "sq", "sbc", "sdp", "sc"
	Q1_AVG_QTY, Q1_AVG_PRICE, Q1_AVG_DISC, Q1_COUNT_ORDER           = "aq", "ap", "ad", "co"
)

var (
	//"Constants"
	MIN_DATE_Q3, MAX_DATE_Q3 = &tpch.Date{YEAR: 1995, MONTH: 3, DAY: 01}, &tpch.Date{YEAR: 1995, MONTH: 3, DAY: 31}
	MIN_DATE_Q1, MAX_DATE_Q1 = &tpch.Date{YEAR: 1998, MONTH: 8, DAY: 3}, &tpch.Date{YEAR: 1998, MONTH: 10, DAY: 2}
	Q9_YEARS                 = []string{"1992", "1993", "1994", "1995", "1996", "1997", "1998"}
	Q12_PRIORITY             = [2]string{"p", "n"}
	Q13_Word1, Q13_Word2     = [4]string{"special", "pending", "unusual", "express"}, [4]string{"packages", "requests", "accounts", "deposits"}
	Q13_BOTH_WORDS           = [16]string{"specialpackages", "specialrequests", "specialaccounts", "specialdeposits", "pendingpackages", "pendingrequests", "pendingaccounts", "pendingdeposits",
		"unusualpackages", "unusualrequests", "unusualaccounts", "unusualdeposits", "expresspackages", "expressrequests", "expressaccounts", "expressdeposits"}
	//Need to store this in order to avoid on updates having to recalculate everything
	q15Map         map[int16]map[int8]map[int32]*float64
	q15LocalMap    []map[int16]map[int8]map[int32]*float64
	q13Info        Q13Info
	q13RegionInfo  []Q13Info
	Q19_CONTAINERS = [3]string{"S", "M", "L"}
	Q19_QUANTITY   = [41]string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
		"21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "40"}
	q22CustNatBalances map[int8]PairIntFloat

	prepIndexFuncs      []func() ([]crdt.UpdateObjectParams, int)   //Functions that prepare the index's initial data
	prepIndexLocalFuncs []func() ([][]crdt.UpdateObjectParams, int) //Functions  that prepare the local version of index's initial data
	ti                  *TableInfo
)

func (ti TableInfo) setPrepIndexFuncs() {
	i := 0
	for _, queryN := range queryNumbers {
		switch queryN {
		case "1":
			prepIndexFuncs[i] = ti.prepareQ1Index
		case "2": //No updates
			prepIndexFuncs[i] = ti.prepareQ2Index
		case "3":
			prepIndexFuncs[i] = ti.prepareQ3Index
		case "4":
			prepIndexFuncs[i] = ti.prepareQ4Index
		case "5":
			prepIndexFuncs[i] = ti.prepareQ5Index
		case "6":
			prepIndexFuncs[i] = ti.prepareQ6Index
		case "7":
			prepIndexFuncs[i] = ti.prepareQ7Index
		case "8":
			prepIndexFuncs[i] = ti.prepareQ8Index
		case "9":
			prepIndexFuncs[i] = ti.prepareQ9Index
		case "10":
			prepIndexFuncs[i] = ti.prepareQ10Index
		case "11": //No updates
			prepIndexFuncs[i] = ti.prepareQ11Index
		case "12":
			prepIndexFuncs[i] = ti.prepareQ12Index
		case "13":
			prepIndexFuncs[i] = ti.prepareQ13Index
		case "14":
			prepIndexFuncs[i] = ti.prepareQ14Index
		case "15":
			prepIndexFuncs[i] = ti.prepareQ15Index
		case "16": //No updates
			prepIndexFuncs[i] = ti.prepareQ16Index
		case "17":
			prepIndexFuncs[i] = ti.prepareQ17Index
		case "18":
			prepIndexFuncs[i] = ti.prepareQ18Index
		case "19":
			prepIndexFuncs[i] = ti.prepareQ19Index
		case "20":
			prepIndexFuncs[i] = ti.prepareQ20Index
		case "21":
			prepIndexFuncs[i] = ti.prepareQ21Index
		case "22":
			prepIndexFuncs[i] = ti.prepareQ22Index
		}
		i++
	}
}

func (ti TableInfo) setLocalPrepIndexFuncs() {
	i := 0
	for _, queryN := range queryNumbers {
		switch queryN {
		case "1":
			prepIndexLocalFuncs[i] = ti.prepareQ1IndexLocal
		case "2":
			prepIndexLocalFuncs[i] = ti.prepareQ2IndexLocal
		case "3":
			prepIndexLocalFuncs[i] = ti.prepareQ3IndexLocal
		case "4":
			prepIndexLocalFuncs[i] = ti.prepareQ4IndexLocal
		case "5":
			prepIndexLocalFuncs[i] = ti.prepareQ5IndexLocal
		case "6":
			prepIndexLocalFuncs[i] = ti.prepareQ6IndexLocal
		case "7":
			prepIndexLocalFuncs[i] = ti.prepareQ7IndexLocal
		case "8":
			prepIndexLocalFuncs[i] = ti.prepareQ8IndexLocal
		case "9":
			prepIndexLocalFuncs[i] = ti.prepareQ9IndexLocal
		case "10":
			prepIndexLocalFuncs[i] = ti.prepareQ10IndexLocal
		case "11":
			prepIndexLocalFuncs[i] = ti.prepareQ11IndexLocal
		case "12":
			prepIndexLocalFuncs[i] = ti.prepareQ12IndexLocal
		case "13":
			prepIndexLocalFuncs[i] = ti.prepareQ13IndexLocal
		case "14":
			prepIndexLocalFuncs[i] = ti.prepareQ14IndexLocal
		case "15":
			prepIndexLocalFuncs[i] = ti.prepareQ15IndexLocal
		case "16":
			prepIndexLocalFuncs[i] = ti.prepareQ16IndexLocal
		case "17":
			prepIndexLocalFuncs[i] = ti.prepareQ17IndexLocal
		case "18":
			prepIndexLocalFuncs[i] = ti.prepareQ18IndexLocal
		case "19":
			prepIndexLocalFuncs[i] = ti.prepareQ19IndexLocal
		case "20":
			prepIndexLocalFuncs[i] = ti.prepareQ20IndexLocal
		case "21":
			prepIndexLocalFuncs[i] = ti.prepareQ21IndexLocal
		case "22":
			prepIndexLocalFuncs[i] = ti.prepareQ22IndexLocal
		}
		i++
	}
}

func prepareIndexesToSend() {
	ti.Tables, times.prepareIndexProtos, times.queries = tpchData.Tables, make([]int64, len(queryFuncs)), make([]int64, len(queryFuncs))
	ti.setPrepIndexFuncs()
	startTime, nUpds := time.Now().UnixNano()/1000000, make([]int, len(times.prepareIndexProtos))
	var currUpds []crdt.UpdateObjectParams
	fmt.Println("Preparing indexes...")
	for i, prepQuery := range prepIndexFuncs {
		currUpds, nUpds[i] = prepQuery()
		ignore(prepQuery, currUpds)
		fmt.Println("Queuing index Q", queryNumbers[i])
		queueIndex(currUpds)
		fmt.Println("Finished queing index Q", queryNumbers[i])
		times.prepareIndexProtos[i] = time.Now().UnixNano() / 1000000
		fmt.Printf("Index Q%s OK\n", queryNumbers[i])
	}
	endTime := times.prepareIndexProtos[len(times.prepareIndexProtos)-1]

	for i := len(times.prepareIndexProtos) - 1; i > 0; i-- {
		times.prepareIndexProtos[i] -= times.prepareIndexProtos[i-1]
	}
	times.prepareIndexProtos[0] -= startTime
	times.totalIndex = endTime - startTime

	//Signal the end
	if splitIndexLoad {
		for _, channel := range channels.indexChans {
			channel <- QueuedMsg{code: QUEUE_COMPLETE}
		}
	} else {
		channels.indexChans[0] <- QueuedMsg{code: QUEUE_COMPLETE}
	}

	/*
		totalUpds, queryN, updsString := 0, "Q", ""
		for i, nUpdsIndex := range nUpds {
			queryN += queryNumbers[i] + ", "
			updsString += strconv.Itoa(nUpdsIndex) + ", "
			totalUpds += nUpdsIndex
		}
	*/
	totalUpds := 0
	for i, nUpdsIndex := range nUpds {
		fmt.Printf("Q%s: %d\t", queryNumbers[i], nUpdsIndex)
		if i%5 == 4 {
			fmt.Println()
		}
		totalUpds += nUpdsIndex
	}
	fmt.Println()
	dataloadStats.nIndexUpds = totalUpds
	fmt.Println("Total number of index upds:", dataloadStats.nIndexUpds)
	//fmt.Println(queryN[:len(queryN)-2] + ": " + updsString[:len(updsString)-2])

	//time.Sleep(30 * time.Second)
	//Start reading updates data
	if DOES_UPDATES && DOES_QUERIES {
		go startMixBench()
	} else if DOES_UPDATES {
		go startUpdates()
	}
}

/*
func prepareIndexesToSend() {
	ti := TableInfo{Tables: tpchData.Tables}
	startTime := time.Now().UnixNano() / 1000000
	//TODO: Later consider 6, 10, 2, by this order.
	fmt.Println("Preparing indexes...")
	//queueIndex(prepareQ2Index())
	q3Upds, q3N := ti.prepareQ3Index()
	queueIndex(q3Upds)
	times.prepareIndexProtos[0] = time.Now().UnixNano() / 1000000
	fmt.Println("Index Q3 OK")
	q5Upds, _, q5N := ti.prepareQ5Index()
	queueIndex(q5Upds)
	times.prepareIndexProtos[1] = time.Now().UnixNano() / 1000000
	fmt.Println("Index Q5 OK")
	q11Upds, q11N := ti.prepareQ11Index()
	queueIndex(q11Upds)
	times.prepareIndexProtos[2] = time.Now().UnixNano() / 1000000
	fmt.Println("Index Q11 OK")
	q14Upds, q14N := ti.prepareQ14Index()
	queueIndex(q14Upds)
	times.prepareIndexProtos[3] = time.Now().UnixNano() / 1000000
	fmt.Println("Index Q14 OK")
	q15Upds, q15N := ti.prepareQ15Index()
	queueIndex(q15Upds)
	times.prepareIndexProtos[4] = time.Now().UnixNano() / 1000000
	fmt.Println("Index Q15 OK")
	q18Upds, q18N := ti.prepareQ18Index()
	queueIndex(q18Upds)
	endTime := time.Now().UnixNano() / 1000000
	times.prepareIndexProtos[5] = endTime
	fmt.Println("Index Q18 OK")

	ignore(q3Upds, q5Upds, q11Upds, q14Upds, q15Upds)

	for i := len(times.prepareIndexProtos) - 1; i > 0; i-- {
		times.prepareIndexProtos[i] -= times.prepareIndexProtos[i-1]
	}
	times.prepareIndexProtos[0] -= startTime
	times.totalIndex = endTime - startTime

	//Signal the end
	if splitIndexLoad {
		for _, channel := range channels.indexChans {
			channel <- QueuedMsg{code: QUEUE_COMPLETE}
		}
	} else {
		channels.indexChans[0] <- QueuedMsg{code: QUEUE_COMPLETE}
	}

	dataloadStats.nIndexUpds = q3N + q5N + q11N + q14N + q15N + q18N
	fmt.Println("Number of index upds:", dataloadStats.nIndexUpds)
	fmt.Println("Q3, Q5, Q11, Q14, Q15, Q18:", q3N, q5N, q11N, q14N, q15N, q18N)

	//Start reading updates data
	if DOES_UPDATES && DOES_QUERIES {
		go startMixBench()
	} else if DOES_UPDATES {
		go startUpdates()
	}
}
*/

func queueIndex(upds []crdt.UpdateObjectParams) {
	//Sends to a random server if using splitIndexLoad, otherwise defaults to 0
	//channels.indexChans[0] <- QueuedMsg{Message: antidote.CreateUpdateObjs(nil, upds), code: antidote.UpdateObjs}
	channels.indexChans[rand.Intn(len(channels.indexChans))] <- QueuedMsg{Message: antidote.CreateUpdateObjs(nil, upds), code: antidote.UpdateObjs}
}

func prepareIndexesLocalToSend() {
	ti.Tables, times.prepareIndexProtos, times.queries = tpchData.Tables, make([]int64, len(queryFuncs)), make([]int64, len(queryFuncs))
	ti.setLocalPrepIndexFuncs()
	startTime, nUpds := time.Now().UnixNano()/1000000, make([]int, len(times.prepareIndexProtos))
	var currUpds [][]crdt.UpdateObjectParams
	fmt.Println("Preparing (local) indexes...")
	for i, prepQuery := range prepIndexLocalFuncs {
		currUpds, nUpds[i] = prepQuery()
		queueIndexLocal(currUpds)
		ignore(currUpds)
		times.prepareIndexProtos[i] = time.Now().UnixNano() / 1000000
		fmt.Printf("Index Q%s OK\n", queryNumbers[i])
	}
	endTime := times.prepareIndexProtos[len(times.prepareIndexProtos)-1]

	for i := len(times.prepareIndexProtos) - 1; i > 0; i-- {
		times.prepareIndexProtos[i] -= times.prepareIndexProtos[i-1]
	}
	times.prepareIndexProtos[0] -= startTime
	times.totalIndex = endTime - startTime

	//Signal the end
	for _, channel := range channels.indexChans {
		channel <- QueuedMsg{code: QUEUE_COMPLETE}
	}

	totalUpds, queryN, updsString := 0, "Q", ""
	for i, nUpdsIndex := range nUpds {
		queryN += strconv.Itoa(i) + ", "
		updsString += strconv.Itoa(nUpdsIndex) + ", "
		totalUpds += nUpdsIndex
	}
	dataloadStats.nIndexUpds = totalUpds
	fmt.Println("Number of index upds:", dataloadStats.nIndexUpds)
	fmt.Println(queryN[:len(queryN)-2] + ": " + updsString[:len(updsString)-2])

	//Start reading updates data
	if DOES_UPDATES && DOES_QUERIES {
		go startMixBench()
	} else if DOES_UPDATES {
		go startUpdates()
	}
}

/*func prepareIndexesLocalToSend() {
	ti := TableInfo{Tables: tpchData.Tables}
	startTime := time.Now().UnixNano() / 1000000
	//TODO: Later consider 6, 10, 2, by this order.
	fmt.Println("Preparing indexes...")
	//queueIndex(prepareQ2Index())
	q3Upds, q3N := ti.prepareQ3IndexLocal()
	queueIndexLocal(q3Upds)
	times.prepareIndexProtos[0] = time.Now().UnixNano() / 1000000
	fmt.Println("Index Q3 OK")
	q5Upds, q5N := ti.prepareQ5IndexLocal()
	queueIndexLocal(q5Upds)
	times.prepareIndexProtos[1] = time.Now().UnixNano() / 1000000
	fmt.Println("Index Q5 OK")
	q11Upds, q11N := ti.prepareQ11IndexLocal()
	queueIndexLocal(q11Upds)
	times.prepareIndexProtos[2] = time.Now().UnixNano() / 1000000
	fmt.Println("Index Q11 OK")
	q14Upds, q14N := ti.prepareQ14IndexLocal()
	queueIndexLocal(q14Upds)
	times.prepareIndexProtos[3] = time.Now().UnixNano() / 1000000
	fmt.Println("Index Q14 OK")
	q15Upds, q15N := ti.prepareQ15IndexLocal()
	queueIndexLocal(q15Upds)
	times.prepareIndexProtos[4] = time.Now().UnixNano() / 1000000
	fmt.Println("Index Q15 OK")
	q18Upds, q18N := ti.prepareQ18IndexLocal()
	queueIndexLocal(q18Upds)
	endTime := time.Now().UnixNano() / 1000000
	times.prepareIndexProtos[5] = endTime
	fmt.Println("Index Q18 OK")

	for i := len(times.prepareIndexProtos) - 1; i > 0; i-- {
		times.prepareIndexProtos[i] -= times.prepareIndexProtos[i-1]
	}
	times.prepareIndexProtos[0] -= startTime
	times.totalIndex = endTime - startTime

	//Signal the end
	for _, channel := range channels.indexChans {
		channel <- QueuedMsg{code: QUEUE_COMPLETE}
	}

	dataloadStats.nIndexUpds = q3N + q5N + q11N + q14N + q15N + q18N
	fmt.Println("Number of index upds:", dataloadStats.nIndexUpds)
	fmt.Println("Q3, Q5, Q11, Q14, Q15, Q18:", q3N, q5N, q11N, q14N, q15N, q18N)

	//Start reading updates data
	if DOES_UPDATES && DOES_QUERIES {
		go startMixBench()
	} else if DOES_UPDATES {
		go startUpdates()
	}
}*/

func queueIndexLocal(upds [][]crdt.UpdateObjectParams) {
	for i, chanUpds := range upds {
		channels.indexChans[i] <- QueuedMsg{Message: antidote.CreateUpdateObjs(nil, chanUpds), code: antidote.UpdateObjs}
	}
}

func (ti TableInfo) prepareQ3IndexLocal() (upds [][]crdt.UpdateObjectParams, updsDone int) {
	//Segment -> orderDate (day) -> orderKey
	sumMap := make([]map[string]map[int8]map[int32]*float64, len(ti.Tables.Regions))
	nUpds := make([]int, len(sumMap))
	for i := range sumMap {
		sumMap[i] = createQ3Map()
	}

	regionI := int8(0)
	orders := ti.getOrders()
	for orderI, order := range orders {
		if order.O_ORDERDATE.IsLowerOrEqual(MAX_DATE_Q3) {
			regionI = ti.Tables.OrderkeyToRegionkey(order.O_ORDERKEY)
			nUpds[regionI] += ti.q3CalcHelper(sumMap[regionI], order, orderI)
		}
	}

	for _, rUpds := range nUpds {
		updsDone += rUpds
	}

	//Override nUpds if useTopKAll
	if useTopKAll {
		for i, regionSumMap := range sumMap {
			nUpds[i] = 0
			for _, segMap := range regionSumMap {
				nUpds[i] += len(segMap)
			}
		}
	}
	for i, regionSumMap := range sumMap {
		nUpds[i] += len(regionSumMap) * 31 //Inits. Day 1 to 31, inclusive. So 31 inits per segment.
		updsDone += len(regionSumMap) * 31
	}

	upds = make([][]crdt.UpdateObjectParams, len(nUpds))
	for i := range upds {
		upds[i] = ti.makeQ3IndexUpds(sumMap[i], nUpds[i], INDEX_BKT+i)
	}

	return
}

func (ti TableInfo) prepareQ3Index() (upds []crdt.UpdateObjectParams, updsDone int) {
	//sum: l_extendedprice*(1-l_discount)
	//This is for each pair of (l_orderkey, o_orderdate, o_shippriority).
	//The query will still need to collect all whose date < o_orderdate and then filter for only those whose
	//l_shipdate is > date. Or maybe we can have some map for that...

	//This actually needs to be a topK for each pair (o_orderdate, c_mktsegment).
	//In the topK we need to store the sum and orderkey. We'll know the priority when obtaining
	//the object. We don't know the orderdate tho. This is due to each topK storing all o_orderdate < orderDate, but with l_shipdate > o_orderdate.
	//Each entry in the topK will be for an orderkey.
	//Orderdate will need to be retrieved separatelly.

	//segment -> orderDate -> orderkey -> sum
	sumMap := createQ3Map()

	updsDone = 0
	orders := ti.getOrders()
	for orderI, order := range orders {
		//To be in range for the maps, o_orderDate must be <= than the highest date 1995-03-31
		//And l_shipdate must be >= than the smallest date 1995-03-01
		if order.O_ORDERDATE.IsLowerOrEqual(MAX_DATE_Q3) {
			updsDone += ti.q3CalcHelper(sumMap, order, orderI)
		}
	}
	nProtoUpds := updsDone
	//Override nProtoUpds if useTopKAll
	if useTopKAll {
		nProtoUpds = 0
		for _, segMap := range sumMap {
			nProtoUpds += len(segMap)
		}
	}
	nProtoUpds += len(sumMap) * 31 //Inits. Day 1 to 31, inclusive. So 31 inits per segment.
	updsDone += len(sumMap) * 31   //Inits

	return ti.makeQ3IndexUpds(sumMap, nProtoUpds, INDEX_BKT), updsDone
}

// TODO: I might need to review this... shouldn't I only be writting positions between orderdate and shipdate?
// Note: not much efficient, but we don't really know how old can an orderdate be without being shipped. And we also don't have any index for dates
func (ti TableInfo) q3CalcHelper(sumMap map[string]map[int8]map[int32]*float64, order *tpch.Orders, orderI int) (nUpds int) {
	var minDay = MIN_MONTH_DAY
	var currSum *float64
	var has bool
	var j int8

	//fmt.Println("OrderDate:", *order.O_ORDERDATE, ". Compare result:", order.O_ORDERDATE.IsLowerOrEqual(maxDate))
	//Get the customer's market segment
	segMap := sumMap[ti.Tables.Customers[order.O_CUSTKEY].C_MKTSEGMENT]
	orderLineItems := ti.Tables.LineItems[orderI]
	for _, item := range orderLineItems {
		//Check if L_SHIPDATE is higher than minDate and, if it is, check month/year. If month/year > march 1995, then add to all entries. Otherwise, use day to know which entries.
		if item.L_SHIPDATE.IsHigherOrEqual(MIN_DATE_Q3) {
			if item.L_SHIPDATE.MONTH > 3 || item.L_SHIPDATE.YEAR > 1995 {
				//All days
				minDay = 1
			} else {
				minDay = item.L_SHIPDATE.DAY + 1
			}
			//fmt.Println("OrderDate:", *order.O_ORDERDATE, "ShipDate:", *item.L_SHIPDATE)
			//Make a for from minDay to 31 to fill the map
			for j = minDay; j <= MAX_MONTH_DAY; j++ {
				currSum, has = segMap[j][order.O_ORDERKEY]
				if !has {
					currSum = new(float64)
					segMap[j][order.O_ORDERKEY] = currSum
					nUpds++
				}
				*currSum += item.L_EXTENDEDPRICE * (1.0 - item.L_DISCOUNT)
			}
		}
	}
	return
}

// Segment -> orderDate (day) -> orderKey
func createQ3Map() (sumMap map[string]map[int8]map[int32]*float64) {
	sumMap = make(map[string]map[int8]map[int32]*float64)
	var j int8
	for _, seg := range tpchData.Tables.Segments {
		segMap := make(map[int8]map[int32]*float64)
		//Days
		for j = 1; j <= 31; j++ {
			segMap[j] = make(map[int32]*float64)
		}
		sumMap[seg] = segMap
	}
	return
}

func (ti TableInfo) prepareQ5IndexLocal() (upds [][]crdt.UpdateObjectParams, updsDone int) {
	sumMap := ti.createQ5Map()
	orders := ti.getOrders()
	for i, orderItems := range ti.Tables.LineItems {
		ti.q5CalcHelper(sumMap, orderItems, orders[i])
	}
	//Create temporary maps with just one region, in order to receive the upds separatelly
	upds = make([][]crdt.UpdateObjectParams, len(ti.Tables.Regions))
	for i, regMap := range sumMap {
		upds[i] = ti.makeQ5IndexUpds(map[int8]map[int16]map[int8]*float64{i: regMap}, INDEX_BKT+int(i))
	}
	return upds, 5 * len(ti.Tables.Nations)
}

func (ti TableInfo) prepareQ5Index() (upds []crdt.UpdateObjectParams, updsDone int) {
	//2.4.5:
	//sum: l_extendedprice * (1 - l_discount)
	//Group this by the pair (country, year) (i.e., group all the days of a year in the same CRDT).
	//Only lineitems for which the customer and supplier are of the same nation count for this sum.
	//On the query, we ask for a given year and region. However, the results must be shown by nation
	//EmbMap CRDT for each region+date and then inside each there's a CounterCRDT for nation?
	//Region -> Year -> Country -> Sum
	sumMap := ti.createQ5Map()
	orders := ti.getOrders()
	for i, orderItems := range ti.Tables.LineItems {
		ti.q5CalcHelper(sumMap, orderItems, orders[i])
	}

	//Upds done: 5 (years) * nations
	return ti.makeQ5IndexUpds(sumMap, INDEX_BKT), 5 * len(ti.Tables.Nations)
}

func (ti TableInfo) createQ5Map() (sumMap map[int8]map[int16]map[int8]*float64) {
	sumMap = make(map[int8]map[int16]map[int8]*float64)
	//Registering regions and dates
	for _, region := range ti.Tables.Regions {
		regMap := make(map[int16]map[int8]*float64)
		for i := int16(1993); i <= 1997; i++ {
			regMap[i] = make(map[int8]*float64)
		}
		sumMap[region.R_REGIONKEY] = regMap
	}
	//Registering countries
	for _, nation := range ti.Tables.Nations {
		regMap := sumMap[nation.N_REGIONKEY]
		for i := int16(1993); i <= 1997; i++ {
			value := 0.0
			regMap[i][nation.N_NATIONKEY] = &value
		}
	}
	return
}

func (ti TableInfo) q5CalcHelper(sumMap map[int8]map[int16]map[int8]*float64, orderItems []*tpch.LineItem, order *tpch.Orders) {
	var customer *tpch.Customer
	var supplier *tpch.Supplier
	var year int16
	var nationKey, regionKey int8
	var value *float64

	year = order.O_ORDERDATE.YEAR
	if year >= 1993 && year <= 1997 {
		customer = ti.Tables.Customers[order.O_CUSTKEY]
		nationKey = customer.C_NATIONKEY
		regionKey = ti.Tables.Nations[nationKey].N_REGIONKEY
		value = sumMap[regionKey][year][nationKey]
		for _, lineItem := range orderItems {
			//Conditions:
			//Ordered year between 1993 and 1997 (inclusive)
			//Supplier and customer of same nation
			//Calculate: l_extendedprice * (1 - l_discount)
			supplier = ti.Tables.Suppliers[lineItem.L_SUPPKEY]
			if nationKey == supplier.S_NATIONKEY {
				*value += lineItem.L_EXTENDEDPRICE * (1 - lineItem.L_DISCOUNT)
			}
		}
	}
}

func (ti TableInfo) prepareQ11IndexLocal() (upds [][]crdt.UpdateObjectParams, updsDone int) {
	nationMap := make([]map[int8]map[int32]*float64, len(ti.Tables.Regions))
	totalSumMap := make([]map[int8]*float64, len(ti.Tables.Regions))
	updsPerRegion := make([]int, len(ti.Tables.Regions))

	for i := range nationMap {
		nationMap[i] = make(map[int8]map[int32]*float64)
		totalSumMap[i] = make(map[int8]*float64)
	}
	for _, nation := range ti.Tables.Nations {
		nationMap[nation.N_REGIONKEY][nation.N_NATIONKEY] = make(map[int32]*float64)
		totalSumMap[nation.N_REGIONKEY][nation.N_NATIONKEY] = new(float64)
		updsPerRegion[nation.N_REGIONKEY]++ //Each nation has at least 1 upd (totalSum)
		//updsPerRegion[nation.N_REGIONKEY] += 2 //Each nation has at least 2 upds (totalSum + TopK Init)
	}

	var supplier *tpch.Supplier
	var regionK int8
	for _, partSup := range ti.Tables.PartSupps {
		supplier = ti.Tables.Suppliers[partSup.PS_SUPPKEY]
		regionK = ti.Tables.Nations[supplier.S_NATIONKEY].N_REGIONKEY
		updsPerRegion[regionK] += ti.q11CalcHelper(nationMap[regionK], totalSumMap[regionK], partSup, supplier)
	}

	for _, updsR := range updsPerRegion {
		updsDone += updsR
	}

	upds = make([][]crdt.UpdateObjectParams, len(ti.Tables.Regions))
	for i := range upds {
		upds[i] = ti.makeQ11IndexUpds(nationMap[i], totalSumMap[i], updsPerRegion[i], INDEX_BKT+i)
	}

	return
}

func (ti TableInfo) prepareQ11Index() (upds []crdt.UpdateObjectParams, updsDone int) {
	//2.4.11:	sum: ps_supplycost * ps_availqty
	//Group by (part, nation). Nation is the supplier nation.
	//The query itself only wants the groups for a given nation in which the sum represents >= 0.01%/SF of
	//all parts supplied by that nation. But that's defined as a variable...

	//2.4.11 (simple topk of a sum, along with a key (ID))
	//Use a topK for each nation. We need to keep the values for every part, as later on it may reach the top
	//Theorically it could be a topK without removals, as there's no updates on the tables referred
	//If we assume updates are possible, then there could be a problem when two users concurrently update the sum of the same nation
	//, since the value ot a topK is static (i.e., not a counter)
	//Also, the value of the topK should be a decimal instead of an integer.
	//We also need a counterCRDT per nation to store the value of sum(ps_supplycost * ps_availqty) * FRACTION for filtering.

	//nation -> part -> ps_supplycost * ps_availqty
	//This could also be an array as nationIDs are 0 to n-1
	nationMap := make(map[int8]map[int32]*float64)
	totalSumMap := make(map[int8]*float64)

	//Preparing maps for each nation
	for _, nation := range ti.Tables.Nations {
		nationMap[nation.N_NATIONKEY] = make(map[int32]*float64)
		totalSumMap[nation.N_NATIONKEY] = new(float64)
	}

	var supplier *tpch.Supplier
	nUpds := len(ti.Tables.Nations) //Assuming all nations have at least one supplier. Initial value corresponds to the number of nations
	//Calculate totals
	for _, partSup := range ti.Tables.PartSupps {
		supplier = ti.Tables.Suppliers[partSup.PS_SUPPKEY]
		nUpds += ti.q11CalcHelper(nationMap, totalSumMap, partSup, supplier)
	}
	//nUpds += len(ti.Tables.Nations) //Init for each nation TopK

	return ti.makeQ11IndexUpds(nationMap, totalSumMap, nUpds, INDEX_BKT), nUpds
}

func (ti TableInfo) q11CalcHelper(nationMap map[int8]map[int32]*float64, totalSumMap map[int8]*float64, partSup *tpch.PartSupp, supplier *tpch.Supplier) (nUpds int) {
	//Calculate totals
	partMap := nationMap[supplier.S_NATIONKEY]
	currSum, has := partMap[partSup.PS_PARTKEY]
	currTotalSum := totalSumMap[supplier.S_NATIONKEY]
	if !has {
		currSum = new(float64)
		partMap[partSup.PS_PARTKEY] = currSum
		nUpds++
	}
	currValue := (float64(partSup.PS_AVAILQTY) * partSup.PS_SUPPLYCOST)
	*currSum += currValue
	*currTotalSum += currValue
	return
}

func (ti TableInfo) prepareQ14IndexLocal() (upds [][]crdt.UpdateObjectParams, updsDone int) {
	mapPromo, mapTotal := make([]map[string]*float64, len(ti.Tables.Regions)), make([]map[string]*float64, len(ti.Tables.Regions))
	inPromo := ti.Tables.PromoParts

	for i := range mapPromo {
		mapPromo[i], mapTotal[i] = createQ14Maps()
	}

	regionKey := int8(0)
	orders := ti.getOrders()
	//for i, orderItems := range ti.Tables.LineItems[1:] {
	for i, orderItems := range ti.Tables.LineItems {
		//regionKey = ti.Tables.OrderkeyToRegionkey(ti.Tables.Orders[i+1].O_ORDERKEY)
		regionKey = ti.Tables.OrderkeyToRegionkey(orders[i].O_ORDERKEY)
		ti.q14CalcHelper(orderItems, mapPromo[regionKey], mapTotal[regionKey], inPromo)
	}

	upds = make([][]crdt.UpdateObjectParams, len(ti.Tables.Regions))
	for i := range upds {
		upds[i] = ti.makeQ14IndexUpds(mapPromo[i], mapTotal[i], INDEX_BKT+i)
	}
	//NRegions * years (1993-1997) * months (1-12)
	updsDone = len(ti.Tables.Regions) * 5 * 12
	return
}

func (ti TableInfo) prepareQ14Index() (upds []crdt.UpdateObjectParams, updsDone int) {
	//Avg CRDT
	//sum + count (or mix together if possible): l_extendedprice * (1 - l_discount), when p_type starts with "PROMO"
	//Group by month (l_shipdate). Asked date always starts at the 1st day of a given month, between 1993 and 1997.
	//The date interval always covers the whole month.

	//Plan: Use a AddMultipleValue.
	//Keep a map of string -> struct{}{} for the partKeys that we know are of type PROMO%
	//Likelly we should even go through all the parts first to achieve that
	//Then go through lineItem, check with the map, and update the correct sums

	inPromo := ti.Tables.PromoParts
	mapPromo, mapTotal := createQ14Maps()

	//Going through lineitem and updating the totals
	for _, orderItems := range ti.Tables.LineItems {
		//for _, orderItems := range ti.Tables.LineItems[1:] {
		ti.q14CalcHelper(orderItems, mapPromo, mapTotal, inPromo)
	}

	return ti.makeQ14IndexUpds(mapPromo, mapTotal, INDEX_BKT), len(mapPromo)
}

func (ti TableInfo) q14CalcHelper(orderItems []*tpch.LineItem, mapPromo map[string]*float64, mapTotal map[string]*float64, inPromo map[int32]struct{}) {
	var year int16
	revenue := 0.0
	date := ""
	for _, lineItem := range orderItems {
		year = lineItem.L_SHIPDATE.YEAR
		if year >= 1993 && year <= 1997 {
			revenue = lineItem.L_EXTENDEDPRICE * (1.0 - lineItem.L_DISCOUNT)
			date = strconv.FormatInt(int64(year), 10) + strconv.FormatInt(int64(lineItem.L_SHIPDATE.MONTH), 10)
			if _, has := inPromo[lineItem.L_PARTKEY]; has {
				*mapPromo[date] += revenue
			}
			*mapTotal[date] += revenue
		}
	}
}

func createQ14Maps() (mapPromo map[string]*float64, mapTotal map[string]*float64) {
	mapPromo, mapTotal = make(map[string]*float64), make(map[string]*float64)
	var i, j int64
	iString, fullKey := "", ""
	//Preparing the maps that'll hold the results for each month between 1993 and 1997
	for i = 1993; i <= 1997; i++ {
		iString = strconv.FormatInt(i, 10)
		for j = 1; j <= 12; j++ {
			fullKey = iString + strconv.FormatInt(j, 10)
			mapPromo[fullKey], mapTotal[fullKey] = new(float64), new(float64)
		}
	}
	return
}

func (ti TableInfo) prepareQ15IndexLocal() (upds [][]crdt.UpdateObjectParams, updsDone int) {
	yearMap := make([]map[int16]map[int8]map[int32]*float64, len(ti.Tables.Regions))
	nUpds := make([]int, len(ti.Tables.Regions))
	q15LocalMap = yearMap

	for i := range yearMap {
		yearMap[i] = createQ15Map()
	}

	rKey := int8(0)
	orders := ti.getOrders()

	for i, orderItems := range ti.Tables.LineItems {
		//for i, orderItems := range ti.Tables.LineItems[1:] {
		//rKey = ti.Tables.OrderkeyToRegionkey(ti.Tables.Orders[i+1].O_ORDERKEY)
		rKey = ti.Tables.OrderkeyToRegionkey(orders[i].O_ORDERKEY)
		nUpds[rKey] += ti.q15CalcHelper(orderItems, yearMap[rKey])
	}

	for i, nUpdsR := range nUpds {
		updsDone += nUpdsR + 20 //20 (4 quarters * 5 years) TopK inits per region
		nUpds[i] += 20
	}

	upds = make([][]crdt.UpdateObjectParams, len(ti.Tables.Regions))
	if !useTopSum {
		for i := range upds {
			upds[i] = ti.makeQ15IndexUpds(yearMap[i], nUpds[i], INDEX_BKT+i)
		}
	} else {
		for i := range upds {
			upds[i] = ti.makeQ15IndexUpdsTopSum(yearMap[i], nUpds[i], INDEX_BKT+i)
		}
	}

	return
}

func (ti TableInfo) prepareQ15Index() (upds []crdt.UpdateObjectParams, updsDone int) {
	//2.4.15: topk: sum(l_extendedprice * (1-l_discount))
	//Group by month. The sum corresponds to the revenue shipped by a supplier during a given quarter of the year.
	//Date can start between first month of 1993 and 10th month of 1997 (first day always)
	//Have one topK per quarter
	//year -> month -> supplierID -> sum
	yearMap := createQ15Map()
	q15Map = yearMap

	nUpds := 0 //Assuming each quarter has at least one supplier

	for _, orderItems := range ti.Tables.LineItems {
		nUpds += ti.q15CalcHelper(orderItems, yearMap)
	}
	nUpds += 20 //4 quarters, 5 years -> 20 TopK inits

	if !useTopSum {
		fmt.Println("[TPCH_INDEX]Not using topsum to prepare q15 index")
		return ti.makeQ15IndexUpds(yearMap, nUpds, INDEX_BKT), nUpds
	}
	fmt.Println("[TPCH_INDEX]Using topsum to prepare q15 index.")
	return ti.makeQ15IndexUpdsTopSum(yearMap, nUpds, INDEX_BKT), nUpds
}

func (ti TableInfo) q15CalcHelper(orderItems []*tpch.LineItem, yearMap map[int16]map[int8]map[int32]*float64) (nUpds int) {
	var year int16
	var month int8
	var suppMap map[int32]*float64
	var currValue *float64
	var has bool
	for _, item := range orderItems {
		year = item.L_SHIPDATE.YEAR
		if year >= 1993 && year <= 1997 {
			month = ((item.L_SHIPDATE.MONTH-1)/3)*3 + 1
			suppMap = yearMap[year][month]
			currValue, has = suppMap[item.L_SUPPKEY]
			if !has {
				currValue = new(float64)
				suppMap[item.L_SUPPKEY] = currValue
				nUpds++
			}
			*currValue += (item.L_EXTENDEDPRICE * (1.0 - item.L_DISCOUNT))
		}
	}
	return
}

func createQ15Map() (yearMap map[int16]map[int8]map[int32]*float64) {
	yearMap = make(map[int16]map[int8]map[int32]*float64)
	var mMap map[int8]map[int32]*float64

	//Preparing map instances for each quarter between 1993 and 1997
	var year int16 = 1993
	for ; year <= 1997; year++ {
		mMap = make(map[int8]map[int32]*float64)
		mMap[1], mMap[4], mMap[7], mMap[10] = make(map[int32]*float64),
			make(map[int32]*float64), make(map[int32]*float64), make(map[int32]*float64)
		yearMap[year] = mMap
	}
	return
}

func (ti TableInfo) prepareQ18IndexLocal() (upds [][]crdt.UpdateObjectParams, updsDone int) {
	quantityMap := make([]map[int32]map[int32]*PairInt, len(ti.Tables.Regions))
	for i := range quantityMap {
		quantityMap[i] = map[int32]map[int32]*PairInt{312: make(map[int32]*PairInt), 313: make(map[int32]*PairInt),
			314: make(map[int32]*PairInt), 315: make(map[int32]*PairInt)}
	}

	regionKey := int8(0)
	orders := ti.getOrders()
	for i, order := range orders {
		//for i, order := range ti.Tables.Orders[1:] {
		regionKey = ti.Tables.OrderkeyToRegionkey(order.O_ORDERKEY)
		ti.q18CalcHelper(quantityMap[regionKey], order, i)
	}

	upds = make([][]crdt.UpdateObjectParams, len(ti.Tables.Regions))
	for i := range upds {
		upds[i] = ti.makeQ18IndexUpds(quantityMap[i], INDEX_BKT+i)
	}

	if useTopKAll {
		for _, innerMap := range quantityMap {
			updsDone += min(len(innerMap[312]), 1) + min(len(innerMap[313]), 1) + min(len(innerMap[314]), 1) + min(len(innerMap[315]), 1)
		}

	} else {
		for _, innerMap := range quantityMap {
			updsDone += len(innerMap[312]) + len(innerMap[313]) + len(innerMap[314]) + len(innerMap[315])
		}
	}

	return
}

// TODO: This likelly could be reimplemented with only 1 topK and using the query of "above value".
// Albeit that potencially would imply downloading more than 100 customers.
func (ti TableInfo) prepareQ18Index() (upds []crdt.UpdateObjectParams, updsDone int) {
	//2.4.18: topk with 100 elements: o_totalprice, o_orderdate
	//Group by l_quantity. Theorically only need quantities between 312 and 315.
	//Ideally we would store c_custKey + "_" + o_orderKey, and then fetch each one. For now, we're only storing o_orderKey.

	//quantity -> orderKey -> (custKey, quantity)
	quantityMap := make(map[int32]map[int32]*PairInt)
	//Preparing possible quantities
	quantityMap[312] = make(map[int32]*PairInt)
	quantityMap[313] = make(map[int32]*PairInt)
	quantityMap[314] = make(map[int32]*PairInt)
	quantityMap[315] = make(map[int32]*PairInt)

	orders := ti.getOrders()
	//Going through orders and, for each order, checking all its lineitems
	//Doing the other way around is possible but would require a map of order -> quantity.
	//Let's do both and then choose one
	//for i, order := range ti.Tables.Orders[1:] {
	for i, order := range orders {
		ti.q18CalcHelper(quantityMap, order, i)
	}

	/*
		//lineitem -> Order
		lastLineitemId := int8(0)
		var item tpch.LineItem
		orderMap := make(map[int32]*PairInt)
		orderKey := int32(-1)
		var currPair *PairInt
		var holderPair PairInt	//Used to host a new pair that is being created
		var has bool
		//First, acumulate total quantities per order
		for i := 1; i < len(ti.Tables.LineItems); i++ {
			item = ti.Tables[i]
			if item == nil {
				//Skip remaining positions for the order, which will also be nil
				i += ti.Tables.MaxOrderLineitems - lastLineItemId
				continue
			}
			//lineItem not nil, process
			orderKey = item.L_ORDERKEY
			currPair, has = orderMap[orderKey]
			if !has {
				holderPair = PairInt{second: 0}
				currPair = &holderPair
				orderMap[orderKey] = &holderPair
			}
			currPair.quantity += item.L_QUANTITY
		}
		//Now, go through all orders and store in the respective maps
		for orderKey, pair := range orderMap {
			if pair.second >= 312 {
				pair.first = ti.Tables[GetOrderIndex(orderKey)].O_CUSTKEY
				for minQ, orderQuantityMap := range quantityMap {
					if pair.second >= minQ {
						orderQuantityMap[orderKey] = pair
					} else {
						break
					}
				}
			}
		}
	*/
	if useTopKAll {
		updsDone = min(len(quantityMap[312]), 1) + min(len(quantityMap[313]), 1) + min(len(quantityMap[314]), 1) + min(len(quantityMap[315]), 1)
	} else {
		updsDone = len(quantityMap[312]) + len(quantityMap[313]) + len(quantityMap[314]) + len(quantityMap[315])
	}

	return ti.makeQ18IndexUpds(quantityMap, INDEX_BKT), updsDone
}

func (ti TableInfo) q18CalcHelper(quantityMap map[int32]map[int32]*PairInt, order *tpch.Orders, index int) {
	currQuantity := int32(0)
	orderItems := ti.Tables.LineItems[index]
	for _, item := range orderItems {
		currQuantity += int32(item.L_QUANTITY)
	}
	if currQuantity >= 312 {
		currPair := &PairInt{first: order.O_CUSTKEY, second: currQuantity}
		for minQ, orderMap := range quantityMap {
			if currQuantity >= minQ {
				orderMap[order.O_ORDERKEY] = currPair
			} else {
				break
			}
		}
	}
}

func (ti TableInfo) makeQ3IndexUpds(sumMap map[string]map[int8]map[int32]*float64, nUpds int, bucketI int) (upds []crdt.UpdateObjectParams) {
	upds = make([]crdt.UpdateObjectParams, nUpds)
	var keyArgs crdt.KeyParams
	i := 0
	//Inits
	for mktSeg := range sumMap {
		for day := MIN_DATE_Q3.DAY; day <= MAX_DATE_Q3.DAY; day++ { //Need to init all TopKs
			keyArgs = crdt.KeyParams{
				Key:      SEGM_DELAY + mktSeg + strconv.FormatInt(int64(day), 10),
				CrdtType: proto.CRDTType_TOPK_RMV,
				Bucket:   buckets[bucketI],
			}
			upds[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: crdt.TopKInit{TopSize: 10}}
			i++
		}
	}

	if INDEX_WITH_FULL_DATA {
		for mktSeg, segMap := range sumMap {
			for day, dayMap := range segMap {
				//A topK per pair (mktsegment, orderdate)
				keyArgs = crdt.KeyParams{
					Key:      SEGM_DELAY + mktSeg + strconv.FormatInt(int64(day), 10),
					CrdtType: proto.CRDTType_TOPK_RMV,
					Bucket:   buckets[bucketI],
				}
				//TODO: Actually use float
				if !useTopKAll {
					for orderKey, sum := range dayMap {
						upds[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: orderKey, Score: int32(*sum), Data: ti.packQ3IndexExtraDataFromKey(orderKey)}}}
						i++
					}
				} else {
					adds := make([]crdt.TopKScore, len(dayMap))
					j := 0
					for orderKey, sum := range dayMap {
						adds[j] = crdt.TopKScore{Id: orderKey, Score: int32(*sum), Data: ti.packQ3IndexExtraDataFromKey(orderKey)}
						j++
					}
					upds[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: crdt.TopKAddAll{Scores: adds}}
					i++
				}
				fmt.Printf("[CI]Q3 for segment %s day %d: %d\n", mktSeg, day, len(dayMap))
			}
		}
	} else {
		for mktSeg, segMap := range sumMap {
			for day, dayMap := range segMap {
				//A topK per pair (mktsegment, orderdate)
				keyArgs = crdt.KeyParams{
					Key:      SEGM_DELAY + mktSeg + strconv.FormatInt(int64(day), 10),
					CrdtType: proto.CRDTType_TOPK_RMV,
					Bucket:   buckets[bucketI],
				}
				//TODO: Actually use float
				if !useTopKAll {
					for orderKey, sum := range dayMap {
						upds[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: orderKey, Score: int32(*sum)}}}
						i++
					}
				} else {
					adds := make([]crdt.TopKScore, len(dayMap))
					j := 0
					for orderKey, sum := range dayMap {
						adds[j] = crdt.TopKScore{Id: orderKey, Score: int32(*sum)}
						j++
					}
					upds[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: crdt.TopKAddAll{Scores: adds}}
					i++
				}
			}
		}
	}
	//fmt.Println(nUpds)
	return
}

func (ti TableInfo) makeQ5IndexUpds(sumMap map[int8]map[int16]map[int8]*float64, bucketI int) (upds []crdt.UpdateObjectParams) {
	//TODO: Actually have a CRDT to store a float instead of an int
	//Prepare the updates. *5 as there's 5 years and we'll do a embMapCRDT for each (region, year)
	upds = make([]crdt.UpdateObjectParams, len(sumMap)*5)
	i, year := 0, int16(0)
	years := []string{"1993", "1994", "1995", "1996", "1997"}
	regS := ""
	for regK, regionMap := range sumMap {
		regS = NATION_REVENUE + ti.Tables.Regions[regK].R_NAME
		for year = 1993; year <= 1997; year++ {
			yearMap := regionMap[year]
			regDateUpd := crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
			for natK, value := range yearMap {
				regDateUpd.Upds[ti.Tables.Nations[natK].N_NAME] = crdt.Increment{Change: int32(*value)}
			}
			upds[i] = crdt.UpdateObjectParams{
				KeyParams:  crdt.KeyParams{Key: regS + years[year-1993], CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]},
				UpdateArgs: regDateUpd,
			}
			i++
		}
	}
	//fmt.Println(len(sumMap) * 5)
	return
}

func (ti TableInfo) makeQ11IndexUpds(nationMap map[int8]map[int32]*float64, totalSumMap map[int8]*float64, nUpds, bucketI int) (upds []crdt.UpdateObjectParams) {
	nUpds = len(nationMap)
	upds = make([]crdt.UpdateObjectParams, nUpds)
	var keyArgs crdt.KeyParams
	i := 0

	for natKey, partMap := range nationMap {
		//TopK
		adds := make([]crdt.TopKScore, len(partMap))
		j := 0
		for partKey, value := range partMap {
			adds[j] = crdt.TopKScore{Id: partKey, Score: int32(*value)}
			j++
		}
		//Container map + counter
		keyArgs = crdt.KeyParams{Key: Q11_KEY + ti.Tables.Nations[natKey].N_NAME, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}
		mapUpd := crdt.EmbMapUpdateAll{Upds: map[string]crdt.UpdateArguments{
			IMP_SUPPLY: crdt.TopKAddAll{Scores: adds}, SUM_SUPPLY: crdt.Increment{Change: int32(*totalSumMap[natKey] * (0.0001 / scaleFactor))}}}
		upds[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: mapUpd}
		i++
		//fmt.Printf("[CI]Q11 map size for nation %d: %d\n", natKey, len(partMap))
	}

	return
}

/*
func (ti TableInfo) makeQ11IndexUpds(nationMap map[int8]map[int32]*float64, totalSumMap map[int8]*float64, nUpds, bucketI int) (upds []crdt.UpdateObjectParams) {
	//Preparing updates for the topK CRDTs
	if useTopKAll {
		nUpds = len(nationMap) + len(totalSumMap)// + len(nationMap) //Last sum: TopK inits
	}
	upds = make([]crdt.UpdateObjectParams, nUpds)
	//var currUpd crdt.UpdateArguments
	var keyArgs crdt.KeyParams
	i := 0
	//Inits
*/
/*
	for natKey := range nationMap {
		keyArgs = crdt.KeyParams{
			Key:      IMP_SUPPLY + ti.Tables.Nations[natKey].N_NAME,
			CrdtType: proto.CRDTType_TOPK_RMV,
			Bucket:   buckets[bucketI],
		}
		var currUpd crdt.UpdateArguments = crdt.TopKInit{TopSize: 100, ShadowTopSize: 1} //This Top won't receive further updates, so no need for shadow.
		upds[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
		i++
	}
*/ /*

	for natKey, partMap := range nationMap {
		keyArgs = crdt.KeyParams{
			Key:      IMP_SUPPLY + ti.Tables.Nations[natKey].N_NAME,
			CrdtType: proto.CRDTType_TOPK_RMV,
			Bucket:   buckets[bucketI],
		}
		if !useTopKAll {
			for partKey, value := range partMap {
				//TODO: Not use int32
				var currUpd crdt.UpdateArguments = crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: partKey, Score: int32(*value)}}
				upds[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
				i++
			}
		} else {
			adds := make([]crdt.TopKScore, len(partMap))
			j := 0
			for partKey, value := range partMap {
				adds[j] = crdt.TopKScore{Id: partKey, Score: int32(*value)}
				j++
			}
			var currUpd crdt.UpdateArguments = crdt.TopKAddAll{Scores: adds}
			upds[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
			i++
		}
		fmt.Printf("[CI]Q11 map size for nation %d: %d\n", natKey, len(partMap))
	}

	//Preparing updates for the counter CRDTs
	for natKey, value := range totalSumMap {
		keyArgs = crdt.KeyParams{
			Key:      SUM_SUPPLY + ti.Tables.Nations[natKey].N_NAME,
			CrdtType: proto.CRDTType_COUNTER,
			Bucket:   buckets[bucketI],
		}
		//TODO: Not use int32
		var currUpd crdt.UpdateArguments = crdt.Increment{Change: int32(*value * (0.0001 / scaleFactor))}
		upds[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
		i++
	}

	//fmt.Println(nUpds)
	return
}*/

func (ti TableInfo) makeQ14IndexUpds(mapPromo map[string]*float64, mapTotal map[string]*float64, bucketI int) (upds []crdt.UpdateObjectParams) {
	//Create the updates
	upds = make([]crdt.UpdateObjectParams, len(mapPromo), len(mapPromo))
	i := 0
	for key, totalP := range mapTotal {
		promo := *mapPromo[key]
		//fmt.Printf("[ClientIndex]Making Q14 update for key %s, with values %d %d (%f)\n", PROMO_PERCENTAGE+key, int64(100.0*promo), int64(*totalP), (100.0*promo) / *totalP)
		upds[i] = crdt.UpdateObjectParams{
			KeyParams:  crdt.KeyParams{Key: PROMO_PERCENTAGE + key, CrdtType: proto.CRDTType_AVG, Bucket: buckets[bucketI]},
			UpdateArgs: crdt.AddMultipleValue{SumValue: int64(100.0 * promo), NAdds: int64(*totalP)},
		}
		i++
	}
	//fmt.Println(len(mapPromo))
	return
}

func (ti TableInfo) makeQ15IndexUpdsTopSum(yearMap map[int16]map[int8]map[int32]*float64, nUpds int, bucketI int) (upds []crdt.UpdateObjectParams) {
	//It's always 20 updates if using TopKAddAll (5 years * 4 months)
	if useTopKAll {
		nUpds = 40 //one update for each TopK + one init per TopK.
	}
	upds = make([]crdt.UpdateObjectParams, nUpds)
	var keyArgs crdt.KeyParams

	i, month := 0, int8(0)
	for year, monthMap := range yearMap {
		for month = 1; month <= 12; month += 3 {
			keyArgs = crdt.KeyParams{
				Key:      TOP_SUPPLIERS + strconv.FormatInt(int64(year), 10) + strconv.FormatInt(int64(month), 10),
				CrdtType: proto.CRDTType_TOPSUM,
				Bucket:   buckets[bucketI],
			}
			upds[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: crdt.TopKInit{TopSize: 5}}
			i++
			if !useTopKAll {
				for suppKey, value := range monthMap[month] {
					//TODO: Not use int32 for value
					currUpd := crdt.TopSAdd{TopKScore: crdt.TopKScore{Id: suppKey, Score: int32(*value)}}
					upds[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
					i++
				}
			} else {
				adds := make([]crdt.TopKScore, len(monthMap[month]))
				j := 0
				for suppKey, value := range monthMap[month] {
					//TODO: Not use int32 for value
					adds[j] = crdt.TopKScore{Id: suppKey, Score: int32(*value)}
					j++
				}
				upds[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: crdt.TopSAddAll{Scores: adds}}
				i++
			}
			//fmt.Printf("[CI]Q15 size for year %d, month %d: %d\n", year, month, len(monthMap[month]))
		}
	}

	return
}

func (ti TableInfo) makeQ15IndexUpds(yearMap map[int16]map[int8]map[int32]*float64, nUpds int, bucketI int) (upds []crdt.UpdateObjectParams) {
	//Create the updates. Always 20 updates if doing with TopKAddAll (5 years * 4 months)
	if useTopKAll {
		nUpds = 40 //20 normal upds + 20 inits
	}
	upds = make([]crdt.UpdateObjectParams, nUpds)
	var keyArgs crdt.KeyParams

	i, month := 0, int8(0)
	for year, monthMap := range yearMap {
		for month = 1; month <= 12; month += 3 {
			keyArgs = crdt.KeyParams{
				Key:      TOP_SUPPLIERS + strconv.FormatInt(int64(year), 10) + strconv.FormatInt(int64(month), 10),
				CrdtType: proto.CRDTType_TOPK_RMV,
				Bucket:   buckets[bucketI],
			}
			upds[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: crdt.TopKInit{TopSize: 1}}
			i++
			if !useTopKAll {
				for suppKey, value := range monthMap[month] {
					//TODO: Not use int32 for value
					upds[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: suppKey, Score: int32(*value)}}}
					i++
				}
			} else {
				adds := make([]crdt.TopKScore, len(monthMap[month]))
				j := 0
				for suppKey, value := range monthMap[month] {
					//TODO: Not use int32 for value
					adds[j] = crdt.TopKScore{Id: suppKey, Score: int32(*value)}
					j++
				}
				upds[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: crdt.TopKAddAll{Scores: adds}}
				i++
			}
			//fmt.Printf("[CI]Q15 size for year %d, month %d: %d\n", year, month, len(monthMap[month]))
		}
	}
	//fmt.Println(nUpds)
	return
}

// TODO: The Score of the CRDT should be totalprice instead of the sum of amounts
func (ti TableInfo) makeQ18IndexUpds(quantityMap map[int32]map[int32]*PairInt, bucketI int) (upds []crdt.UpdateObjectParams) {
	nUpds := 0
	if useTopKAll {
		nUpds = min(len(quantityMap[312]), 1) + min(len(quantityMap[313]), 1) + min(len(quantityMap[314]), 1) + min(len(quantityMap[315]), 1)
	} else {
		nUpds = len(quantityMap[312]) + len(quantityMap[313]) + len(quantityMap[314]) + len(quantityMap[315])
	}
	//Create the updates
	upds = make([]crdt.UpdateObjectParams, nUpds)
	var keyArgs crdt.KeyParams

	i := 0
	if !INDEX_WITH_FULL_DATA {
		for quantity, orderMap := range quantityMap {
			keyArgs = crdt.KeyParams{
				Key:      LARGE_ORDERS + strconv.FormatInt(int64(quantity), 10),
				CrdtType: proto.CRDTType_TOPK_RMV,
				Bucket:   buckets[bucketI],
			}
			if !useTopKAll {
				for orderKey, pair := range orderMap {
					//TODO: Store the customerKey also
					currUpd := crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: orderKey, Score: pair.second}}
					upds[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: currUpd}
					i++
				}
			} else {
				adds := make([]crdt.TopKScore, len(orderMap))
				j := 0
				for orderKey, pair := range orderMap {
					//fmt.Println("Adding score", orderKey, pair.second)
					adds[j] = crdt.TopKScore{Id: orderKey, Score: pair.second}
					j++
				}
				if j > 0 {
					var currUpd crdt.UpdateArguments = crdt.TopKAddAll{Scores: adds}
					upds[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: currUpd}
					i++
				}
			}
		}
	} else {
		for quantity, orderMap := range quantityMap {
			keyArgs = crdt.KeyParams{
				Key:      LARGE_ORDERS + strconv.FormatInt(int64(quantity), 10),
				CrdtType: proto.CRDTType_TOPK_RMV,
				Bucket:   buckets[bucketI],
			}
			if !useTopKAll {
				for orderKey, pair := range orderMap {
					//TODO: Store the customerKey also
					currUpd := crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: orderKey, Score: pair.second, Data: ti.packQ18IndexExtraDataFromKey(orderKey)}}
					upds[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: currUpd}
					i++
				}
			} else {
				adds := make([]crdt.TopKScore, len(orderMap))
				j := 0
				for orderKey, pair := range orderMap {
					//fmt.Println("Adding score", *pair)
					adds[j] = crdt.TopKScore{Id: orderKey, Score: pair.second, Data: ti.packQ18IndexExtraDataFromKey(orderKey)}
					j++
				}
				if j > 0 {
					upds[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: crdt.TopKAddAll{Scores: adds}}
					i++
				}
			}
		}
	}
	//fmt.Println(nUpds)

	return
}

func (ti TableInfo) packQ3IndexExtraDataFromKey(orderKey int32) (data *[]byte) {
	return packQ3IndexExtraData(ti.Tables.Orders[ti.Tables.GetOrderIndex(orderKey)])
}

func (ti TableInfo) packQ18IndexExtraDataFromKey(orderKey int32) (data *[]byte) {
	order := ti.Tables.Orders[ti.Tables.GetOrderIndex(orderKey)]
	cust := ti.Tables.Customers[order.O_CUSTKEY]
	return packQ18IndexExtraData(order, cust)
}

func packQ3IndexExtraData(order *tpch.Orders) (data *[]byte) {
	date := order.O_ORDERDATE
	var build strings.Builder
	build.WriteString(strconv.FormatInt(int64(date.YEAR), 10))
	build.WriteRune('-')
	build.WriteString(strconv.FormatInt(int64(date.MONTH), 10))
	build.WriteRune('-')
	build.WriteString(strconv.FormatInt(int64(date.DAY), 10))
	build.WriteRune('_')
	build.WriteString(order.O_SHIPPRIORITY)
	buf := []byte(build.String())
	return &buf
}

func packQ18IndexExtraData(order *tpch.Orders, customer *tpch.Customer) (data *[]byte) {
	date := order.O_ORDERDATE
	var build strings.Builder
	build.WriteString(customer.C_NAME)
	build.WriteRune('_')
	build.WriteString(strconv.FormatInt(int64(customer.C_CUSTKEY), 10))
	build.WriteRune('_')
	build.WriteString(strconv.FormatInt(int64(date.YEAR), 10))
	build.WriteRune('-')
	build.WriteString(strconv.FormatInt(int64(date.MONTH), 10))
	build.WriteRune('-')
	build.WriteString(strconv.FormatInt(int64(date.DAY), 10))
	build.WriteRune('_')
	build.WriteString(order.O_TOTALPRICE)
	buf := []byte(build.String())
	return &buf
}

// Same logic as the non-local version. To answer a query, the client needs to aggregate data from all regions.
func (ti TableInfo) prepareQ1IndexLocal() (upds [][]crdt.UpdateObjectParams, updsDone int) {
	q1Map := make([]map[int8]map[string]*Q1Data, len(ti.Tables.Regions))
	for i := range q1Map {
		q1Map[i] = ti.createQ1Map()
	}
	regionI := int8(0)
	orders := ti.getOrders()
	for orderI, order := range orders {
		regionI = ti.Tables.OrderkeyToRegionkey(order.O_ORDERKEY)
		updsDone += q1CalcHelper(q1Map[regionI], order, ti.Tables.LineItems[orderI])
	}
	//Total number of update operations: updsDone * 8
	upds = make([][]crdt.UpdateObjectParams, len(q1Map))
	for i := range upds {
		upds[i] = ti.makeQ1IndexUpds(q1Map[i], INDEX_BKT+i)
	}

	fmt.Printf("[PrepareQ1Local]Upds: %+v, nUpds: %v\n", upds, updsDone)
	return
}

func (ti TableInfo) prepareQ1Index() (upds []crdt.UpdateObjectParams, updsDone int) {
	//An interesting solution:
	//120 has everything that is >= 120 days.
	//The other ones have the acumulation between 60 and 120.
	//This way only need to download 2 entries and yet still very efficient update-wise.
	//(Except for a few entries that will still be slow update-wise).

	//Map[days]->Map[returnflag,linestatus]->(sum,sum,sum,sum,avg,avg,avg,count)
	q1Map := ti.createQ1Map()
	orders := ti.getOrders()
	for orderI, order := range orders {
		updsDone += q1CalcHelper(q1Map, order, ti.Tables.LineItems[orderI])
	}
	//Total number of update operations: updsDone * 8

	return ti.makeQ1IndexUpds(q1Map, INDEX_BKT), updsDone * 8
}

func (ti TableInfo) createQ1Map() (q1Map map[int8]map[string]*Q1Data) {
	//Given the specification of L_RETURNFLAG and L_LINESTATUS, only the following combinations are possible:
	//[RF, AF, NO, NF]
	q1Map = make(map[int8]map[string]*Q1Data)
	for i := 60; i <= 120; i++ {
		q1Map[int8(i)] = map[string]*Q1Data{"RF": {}, "AF": {}, "NO": {}, "NF": {}}
	}
	return
}

func q1CalcHelper(q1Map map[int8]map[string]*Q1Data, order *tpch.Orders, orderItems []*tpch.LineItem) (nUpds int) {
	var currData *Q1Data
	for _, item := range orderItems {
		shipdate := item.L_SHIPDATE
		if shipdate.IsLowerOrEqual(MIN_DATE_Q1) {
			currData = q1Map[120][item.L_RETURNFLAG+item.L_LINESTATUS]
			currData.sumQuantity += int(item.L_QUANTITY)
			currData.sumPrice += item.L_EXTENDEDPRICE
			currData.sumDiscount += item.L_DISCOUNT
			currData.sumDiscPrice += (item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT))
			currData.sumCharge += (item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT) * (1 + item.L_TAX))
			currData.nItems++
			nUpds += 8
			//fmt.Printf("Q1CalcHelper. Key: %v, Data: %v\n", item.L_RETURNFLAG+item.L_LINESTATUS, *currData)
		} else if shipdate.IsLowerOrEqual(MAX_DATE_Q1) {
			startPos := int8(60 + MAX_DATE_Q1.CalculateDiffDate(&shipdate))
			for ; startPos >= 60; startPos-- {
				currData = q1Map[startPos][item.L_RETURNFLAG+item.L_LINESTATUS]
				currData.sumQuantity += int(item.L_QUANTITY)
				currData.sumPrice += item.L_EXTENDEDPRICE
				currData.sumDiscount += item.L_DISCOUNT
				currData.sumDiscPrice += (item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT))
				currData.sumCharge += (item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT) * (1 + item.L_TAX))
				currData.nItems++
				nUpds += 8
			}
		} else {
			continue //Too recent order, skip.
		}
	}
	return
}

func (ti TableInfo) makeQ1IndexUpds(q1Map map[int8]map[string]*Q1Data, bucketI int) (upds []crdt.UpdateObjectParams) {
	//upds = make([]crdt.UpdateObjectParams, 61) //60 to 120 days, inclusive (61 days)
	//pos := makeQ1IndexUpdsHelper(q1Map, upds, 0, bucketI)
	//return upds[:pos]
	upds = make([]crdt.UpdateObjectParams, 1)
	makeQ1IndexUpdsHelper(q1Map, upds, 0, bucketI)
	return upds
}

func makeQ1IndexUpdsHelper(q1Map map[int8]map[string]*Q1Data, buf []crdt.UpdateObjectParams, bufI, bucketI int) (newBufI int) {
	keyArgs := crdt.KeyParams{Key: Q1_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}
	dayMapUpd := crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)} //We don't know the size needed as this function is also used by queryInfos.go
	var outerMapUpd crdt.EmbMapUpdateAll
	for day, dayMap := range q1Map {
		outerMapUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
		for pairKey, entries := range dayMap {
			outerMapUpd.Upds[pairKey] = makeQ1InnerMapUpd(entries)
		}
		dayMapUpd.Upds[strconv.FormatInt(int64(day), 10)] = outerMapUpd
	}
	buf[bufI] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: dayMapUpd}
	return bufI + 1
}

/*func makeQ1IndexUpdsHelper(q1Map map[int8]map[string]*Q1Data, buf []crdt.UpdateObjectParams, bufI, bucketI int) (newBufI int) {
	var keyArgs crdt.KeyParams
	var outerMapUpd crdt.EmbMapUpdateAll
	for day, dayMap := range q1Map {
		keyArgs = crdt.KeyParams{Key: Q1_KEY + strconv.FormatInt(int64(day), 10), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}
		outerMapUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
		for pairKey, entries := range dayMap {
			outerMapUpd.Upds[pairKey] = makeQ1InnerMapUpd(entries)
		}
		buf[bufI] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: outerMapUpd}
		bufI++
	}
	return bufI
}*/

func makeQ1InnerMapUpd(q1Data *Q1Data) (mapUpd crdt.EmbMapUpdateAll) {
	mapUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
	mapUpd.Upds[Q1_SUM_QTY] = crdt.Increment{Change: int32(q1Data.sumQuantity)}
	mapUpd.Upds[Q1_SUM_BASE_PRICE] = crdt.IncrementFloat{Change: q1Data.sumPrice}
	mapUpd.Upds[Q1_SUM_DISC_PRICE] = crdt.IncrementFloat{Change: q1Data.sumDiscPrice}
	mapUpd.Upds[Q1_SUM_CHARGE] = crdt.IncrementFloat{Change: q1Data.sumCharge}
	mapUpd.Upds[Q1_AVG_QTY] = crdt.AddMultipleValue{SumValue: int64(q1Data.sumQuantity), NAdds: int64(q1Data.nItems)}
	mapUpd.Upds[Q1_AVG_PRICE] = crdt.AddMultipleValue{SumValue: int64(q1Data.sumPrice * 100), NAdds: int64(q1Data.nItems)}
	mapUpd.Upds[Q1_AVG_DISC] = crdt.AddMultipleValue{SumValue: int64(q1Data.sumDiscount * 100), NAdds: int64(q1Data.nItems)}
	mapUpd.Upds[Q1_COUNT_ORDER] = crdt.Increment{Change: int32(q1Data.nItems)}
	return mapUpd
}

func (ti TableInfo) getOrders() []*tpch.Orders {
	orders := ti.Tables.Orders
	if orders[0] == nil {
		//Happens only for the initial data
		orders = orders[1:]
	}
	return orders
}

func (ti TableInfo) prepareQ2IndexLocal() (upds [][]crdt.UpdateObjectParams, updsDone int) {
	q2Map := make(map[int8]map[string][]int) //region->type+size->partSupp[i]
	updsPerRegion := make([]int, len(ti.Tables.Regions))
	for _, region := range ti.Tables.Regions {
		q2Map[region.R_REGIONKEY] = make(map[string][]int)
	}
	regionKey := int8(0)
	for i, supplierPart := range ti.PartSupps {
		regionKey = ti.SuppkeyToRegionkey(int64(supplierPart.PS_SUPPKEY))
		updsPerRegion[regionKey] += ti.q2CalcHelper(q2Map, supplierPart, i)
	}
	for _, updsRegion := range updsPerRegion {
		updsDone += updsRegion
	}
	upds = make([][]crdt.UpdateObjectParams, len(ti.Tables.Regions))
	for i, regMap := range q2Map {
		//Make a temporary map with only 1 region entry
		q2MapLocal := map[int8]map[string][]int{i: regMap}
		upds[i] = ti.makeQ2IndexUpds(q2MapLocal, INDEX_BKT+int(i), updsPerRegion[i])
	}
	return
}

func (ti TableInfo) prepareQ2Index() (upds []crdt.UpdateObjectParams, updsDone int) {
	//Map[region]->Map[type+size]->TopK
	//Size: [1-50]
	//Type: only 5 types (Syllable 3 only)
	//Better not prepare beforehand... just create on the spot.
	q2Map := make(map[int8]map[string][]int) //region->type+size->partSupp[i]
	for _, region := range ti.Tables.Regions {
		q2Map[region.R_REGIONKEY] = make(map[string][]int)
	}
	for i, supplierPart := range ti.PartSupps {
		updsDone += ti.q2CalcHelper(q2Map, supplierPart, i)
	}

	return ti.makeQ2IndexUpds(q2Map, INDEX_BKT, updsDone), updsDone
}

func (ti TableInfo) q2CalcHelper(q2Map map[int8]map[string][]int, suppPart *tpch.PartSupp, suppPartI int) (updsDone int) {
	part, supp := ti.Parts[suppPart.PS_PARTKEY], ti.Suppliers[suppPart.PS_SUPPKEY]
	suppRegion := ti.SuppkeyToRegionkey(int64(supp.S_SUPPKEY))
	regionMap := q2Map[suppRegion]
	dataKey := part.P_SIZE + ti.Tables.TypesToShortType[part.P_TYPE]
	partSupps, has := regionMap[dataKey]
	if !has {
		partSupps = make([]int, 0, 10)
	}
	regionMap[dataKey] = append(partSupps, suppPartI)
	return 1
}

// Note: Each top is replicated in its own key, not inside an embedded map.
func (ti TableInfo) makeQ2IndexUpds(q2Map map[int8]map[string][]int, bucketI, nUpds int) (upds []crdt.UpdateObjectParams) {
	if useTopKAll {
		nUpds = 0
		for _, regionMap := range q2Map {
			nUpds += len(regionMap) * 2 //TopKAddAll + TopKInit
		}
	} else {
		for _, regionMap := range q2Map {
			nUpds += len(regionMap) //One TopKInit per CRDT
		}
	}
	upds = make([]crdt.UpdateObjectParams, nUpds)
	var keyArgs crdt.KeyParams
	i := 0
	for regionI, regionMap := range q2Map {
		regionS := strconv.FormatInt(int64(regionI), 10)
		regionKey := Q2_KEY + regionS

		for id, listSupps := range regionMap {
			keyArgs = crdt.KeyParams{Key: regionKey + id, CrdtType: proto.CRDTType_TOPK, Bucket: buckets[bucketI]}
			upds[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: crdt.TopKInit{TopSize: 1}}
			i++
			if useTopKAll {
				adds := make([]crdt.TopKScore, len(listSupps))
				for j, partSuppI := range listSupps {
					adds[j] = ti.makeQ2TopUpd(partSuppI)
				}
				upds[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: crdt.TopKAddAll{Scores: adds}}
				i++
			} else {
				for _, partSuppI := range listSupps {
					upds[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: crdt.TopKAdd{TopKScore: ti.makeQ2TopUpd(partSuppI)}}
					i++
				}
			}
		}
	}
	return
}

func (ti TableInfo) makeQ2TopUpd(partSuppI int) crdt.TopKScore {
	ps := ti.PartSupps[partSuppI]
	return crdt.TopKScore{Id: ps.PS_PARTKEY, Score: int32(ps.PS_SUPPLYCOST * 100), Data: ti.packQ2IndexExtraDataFromPartSupp(ps)}
}

func (ti TableInfo) packQ2IndexExtraDataFromPartSupp(ps *tpch.PartSupp) (data *[]byte) {
	if !INDEX_WITH_FULL_DATA {
		return &[]byte{}
	}
	var build strings.Builder
	supp, part := ti.Suppliers[ps.PS_SUPPKEY], ti.Parts[ps.PS_PARTKEY]
	nation := ti.Nations[supp.S_NATIONKEY]
	/*
		s_acctbal, s_name, n_name, p_mfgr, s_address, s_phone, s_comment
	*/
	build.WriteString(supp.S_ACCTBAL)
	build.WriteRune('_')
	build.WriteString(supp.S_NAME)
	build.WriteRune('_')
	build.WriteString(nation.N_NAME)
	build.WriteRune('_')
	build.WriteString(part.P_MFGR)
	build.WriteRune('_')
	build.WriteString(supp.S_ADDRESS)
	build.WriteRune('_')
	build.WriteString(supp.S_PHONE)
	build.WriteRune('_')
	build.WriteString(supp.S_COMMENT)
	buf := []byte(build.String())
	return &buf
}

func (ti TableInfo) prepareQ4IndexLocal() (upds [][]crdt.UpdateObjectParams, updsDone int) {
	q4RegionMap := make([]map[int16]map[int8]map[string]*int, len(ti.Tables.Regions))
	for i := range q4RegionMap {
		q4RegionMap[i] = ti.createQ4Map()
	}

	regionI, orders := int8(0), ti.getOrders()
	var order *tpch.Orders
	for i, orderItems := range ti.Tables.LineItems {
		order = orders[i]
		regionI = ti.Tables.OrderkeyToRegionkey(order.O_ORDERKEY)
		q4CalcHelper(q4RegionMap[regionI], orderItems, order)
	}

	upds = make([][]crdt.UpdateObjectParams, len(q4RegionMap))
	regNUpds := 0
	for i := range upds {
		upds[i], regNUpds = ti.makeQ4IndexUpds(q4RegionMap[i], INDEX_BKT+i)
		updsDone += regNUpds
	}
	return
}

func (ti TableInfo) prepareQ4Index() (upds []crdt.UpdateObjectParams, updsDone int) {
	//Map[year]->Map[quarter]->Map[orderPriority]->counter
	//Only the year and quarter are parameters. So the query downloads all the orderPriority
	//Thus, implement this with an embedded map CRDT per quarter.
	q4Map := ti.createQ4Map()
	orders := ti.getOrders()

	for i, orderItems := range ti.Tables.LineItems {
		q4CalcHelper(q4Map, orderItems, orders[i])
	}

	return ti.makeQ4IndexUpds(q4Map, INDEX_BKT)
}

func (ti TableInfo) createQ4Map() (q4Map map[int16]map[int8]map[string]*int) {
	q4Map = make(map[int16]map[int8]map[string]*int)
	quarters := []int8{1, 4, 7, 10}
	for year := int16(1993); year <= int16(1997); year++ {
		yearMap := make(map[int8]map[string]*int)
		for _, quarter := range quarters {
			innerMap := make(map[string]*int)
			for _, priority := range ti.Tables.Priorities {
				innerMap[priority] = new(int)
			}
			yearMap[quarter] = innerMap
		}
		q4Map[year] = yearMap
	}
	return
}

func q4CalcHelper(q4Map map[int16]map[int8]map[string]*int, items []*tpch.LineItem, order *tpch.Orders) {
	year := order.O_ORDERDATE.YEAR
	if year >= 1993 && year <= 1997 {
		for _, lineitem := range items {
			//Need to compare orderdates.
			if lineitem.L_COMMITDATE.IsLowerOrEqual(&lineitem.L_RECEIPTDATE) {
				//Do update. No else. When found, no need to keep continuing the cycle either.
				*q4Map[year][tpch.MonthToQuarter(order.O_ORDERDATE.MONTH)][order.O_ORDERPRIORITY]++
				return
			}
		}
	}
	return
}

func (ti TableInfo) makeQ4IndexUpds(q4Map map[int16]map[int8]map[string]*int, bucketI int) (upds []crdt.UpdateObjectParams, nUpds int) {
	upds = make([]crdt.UpdateObjectParams, len(q4Map)*4) //4: 4 quarters
	var keyArgs crdt.KeyParams
	i, nUpds := 0, 0
	for year, yearMap := range q4Map {
		for quarter, quarterMap := range yearMap {
			keyArgs = crdt.KeyParams{
				Key:      Q4_KEY + strconv.FormatInt(int64(year), 10) + strconv.FormatInt(int64(quarter), 10),
				CrdtType: proto.CRDTType_RRMAP,
				Bucket:   buckets[bucketI],
			}
			mapUpd := crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
			for orderPriority, count := range quarterMap {
				mapUpd.Upds[orderPriority] = crdt.Increment{Change: int32(*count)}
				nUpds++
			}
			upds[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: mapUpd}
			i++
		}
	}
	return upds, nUpds
}

func (ti TableInfo) prepareQ6IndexLocal() (upds [][]crdt.UpdateObjectParams, updsDone int) {
	q6RegionMap, orders := make([]map[int16]map[int8]map[int8]*float64, len(ti.Tables.Regions)), ti.getOrders()
	for i := range q6RegionMap {
		q6RegionMap[i] = ti.createQ6Map()
	}
	for i, orderItems := range ti.Tables.LineItems {
		ti.q6CalcHelper(q6RegionMap[ti.Custkey32ToRegionkey(orders[i].O_CUSTKEY)], orderItems)
	}
	upds = make([][]crdt.UpdateObjectParams, len(q6RegionMap))
	regNUpds := 0
	for i := range upds {
		upds[i], regNUpds = ti.makeQ6IndexUpds(q6RegionMap[i], INDEX_BKT+i)
		updsDone += regNUpds
	}
	return
}

func (ti TableInfo) prepareQ6Index() (upds []crdt.UpdateObjectParams, updsDone int) {
	//Map[year,quantity]->Map[discount]->sum
	//Discount is stored multiplied by 100
	//Note: according to the TPC-H specification, discount is [0, 0.10]
	q6Map := ti.createQ6Map()
	for _, orderItems := range ti.Tables.LineItems {
		ti.q6CalcHelper(q6Map, orderItems)
	}
	return ti.makeQ6IndexUpds(q6Map, INDEX_BKT)
}

func (ti TableInfo) createQ6Map() (q6Map map[int16]map[int8]map[int8]*float64) {
	q6Map = make(map[int16]map[int8]map[int8]*float64)
	for year := int16(1993); year <= 1997; year++ {
		yearMap := map[int8]map[int8]*float64{24: make(map[int8]*float64), 25: make(map[int8]*float64)}
		for i := MIN_Q6_DISCOUNT; i <= MAX_Q6_DISCOUNT; i++ {
			yearMap[24][i], yearMap[25][i] = new(float64), new(float64)
		}
		q6Map[year] = yearMap
	}
	return
}

func (ti TableInfo) q6CalcHelper(q6Map map[int16]map[int8]map[int8]*float64, items []*tpch.LineItem) {
	for _, item := range items {
		if item.L_DISCOUNT > 0 && item.L_SHIPDATE.YEAR >= 1993 && item.L_SHIPDATE.YEAR <= 1997 && item.L_QUANTITY < 25 {
			if item.L_QUANTITY < 24 {
				*q6Map[item.L_SHIPDATE.YEAR][24][int8(item.L_DISCOUNT*100)] += (item.L_EXTENDEDPRICE * item.L_DISCOUNT)
			}
			*q6Map[item.L_SHIPDATE.YEAR][25][int8(item.L_DISCOUNT*100)] += (item.L_EXTENDEDPRICE * item.L_DISCOUNT)
		}
	}
}

func (ti TableInfo) makeQ6IndexUpds(q6Map map[int16]map[int8]map[int8]*float64, bucketI int) (upds []crdt.UpdateObjectParams, nUpds int) {
	upds = make([]crdt.UpdateObjectParams, len(q6Map)*2) //*2: Quantities
	strDiscounts := make([]string, MAX_Q6_DISCOUNT-MIN_Q6_DISCOUNT+1)
	var curr float64
	j := 0
	for i := MIN_Q6_DISCOUNT; i <= MAX_Q6_DISCOUNT; i++ {
		strDiscounts[i-1] = strconv.FormatInt(int64(i), 10)
	}
	var keyParams crdt.KeyParams
	var mapUpd crdt.EmbMapUpdateAll
	for year, yearMap := range q6Map {
		for quantity, quantityMap := range yearMap {
			keyParams = crdt.KeyParams{
				Key:      Q6_KEY + strconv.FormatInt(int64(year), 10) + strconv.FormatInt(int64(quantity), 10),
				CrdtType: proto.CRDTType_RRMAP,
				Bucket:   buckets[bucketI],
			}
			mapUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
			for i, disc := range strDiscounts {
				curr = *quantityMap[int8(i)+1]
				if curr > 0 {
					mapUpd.Upds[disc] = crdt.IncrementFloat{Change: curr}
					nUpds++
				}
			}
			upds[j] = crdt.UpdateObjectParams{KeyParams: keyParams, UpdateArgs: mapUpd}
			j++
		}
	}

	return
	//return len(q6Map) * 2 * len(ti.Priorities)
}

func (ti TableInfo) prepareQ7IndexLocal() (upds [][]crdt.UpdateObjectParams, updsDone int) {
	//A bit different frrom the normal Q7.
	//Here we need data duplicated - each server must be able to serve all queries regarding its countries.
	//Thus this means that e.g., UK has to have all data of UK-USA even if USA's id is before UK.
	//(As in Europe we must be able to serve the query of UK locally)
	//This means that a lineitem will generate updates on both countries' entries.
	q7LocalMap, orders := ti.createQ7LocalMap(), ti.getOrders()
	for i, orderItems := range ti.Tables.LineItems {
		ti.q7LocalCalcHelper(q7LocalMap, orders[i], orderItems)
	}
	fmt.Println("[ClientIndex][Q7Local]Q7 local map:")
	for i, regMap := range q7LocalMap {
		fmt.Printf("Region %d: %+v\n", i, regMap)
	}
	fmt.Println()
	upds, regNUpds := make([][]crdt.UpdateObjectParams, len(q7LocalMap)), 0
	for i, q7Map := range q7LocalMap {
		upds[i], regNUpds = ti.makeQ7IndexUpds(q7Map, INDEX_BKT+i)
		updsDone += regNUpds
	}
	return
}

func (ti TableInfo) createQ7LocalMap() (q7Map []map[int8]map[int16]map[int8]*float64) {
	q7Map = make([]map[int8]map[int16]map[int8]*float64, len(ti.Tables.Regions))
	for i := range q7Map {
		q7Map[i] = make(map[int8]map[int16]map[int8]*float64)
	}
	nations := ti.Tables.Nations
	var natMap95, natMap96 map[int8]*float64
	var regKey int8
	//All nations must match with all nations.
	for i, firstNat := range nations {
		regKey = firstNat.N_REGIONKEY
		natMap95, natMap96 = make(map[int8]*float64), make(map[int8]*float64)
		for j := range nations {
			if i != j {
				natMap95[int8(j)], natMap96[int8(j)] = new(float64), new(float64)
			}
		}
		q7Map[regKey][int8(i)] = map[int16]map[int8]*float64{1995: natMap95, 1996: natMap96}
	}
	return
}

func (ti TableInfo) q7LocalCalcHelper(q7Map []map[int8]map[int16]map[int8]*float64, order *tpch.Orders, orderItems []*tpch.LineItem) {
	orderDate := order.O_ORDERDATE
	var custNationKey, suppNationKey, custRegionKey, suppRegionKey int8
	var custMap, suppMap map[int16]map[int8]*float64
	//Shipdate is orderdate + 1~121 days. So if year == 1994 && orderdate.Month < September or orderdate.Year >= 1997, no item will be relevant for this query.
	if (orderDate.YEAR == 1994 && orderDate.MONTH >= 9) || (orderDate.YEAR >= 1995 && orderDate.YEAR <= 1996) {
		custNationKey = ti.OrderToNationkey(order)
		custRegionKey = ti.OrderToRegionkey(order)
		custMap = q7Map[custRegionKey][custNationKey]
		for _, item := range orderItems {
			if item.L_SHIPDATE.YEAR > 1996 || item.L_SHIPDATE.YEAR < 1995 {
				continue //This can happen when orderDate is close to the end of 1996.
			}
			suppNationKey = ti.Tables.SupplierkeyToNationkey(item.L_SUPPKEY)
			if suppNationKey == custNationKey {
				continue
			}
			suppRegionKey = ti.Tables.SuppkeyToRegionkey(int64(item.L_SUPPKEY))
			suppMap = q7Map[suppRegionKey][suppNationKey]
			*custMap[item.L_SHIPDATE.YEAR][suppNationKey] += (item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT))
			*suppMap[item.L_SHIPDATE.YEAR][custNationKey] += (item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT))
		}
	}
}

func (ti TableInfo) prepareQ7Index() (upds []crdt.UpdateObjectParams, updsDone int) {
	//For the CRDTs, do two maps of: (one for year 1995, another for year 1996):
	//Map[nation1]->Map[nation2]->counter
	//Internally, for efficiency on preparing the updates, do:
	//nation1->year->nation2->sum
	q7Map, orders := ti.createQ7Map(), ti.getOrders()
	for i, orderItems := range ti.Tables.LineItems {
		ti.q7CalcHelper(q7Map, orders[i], orderItems)
	}
	return ti.makeQ7IndexUpds(q7Map, INDEX_BKT)
}

func (ti TableInfo) createQ7Map() (q7Map map[int8]map[int16]map[int8]*float64) {
	q7Map = make(map[int8]map[int16]map[int8]*float64)
	//q7Map[1995], q7Map[1996] = make(map[int8]map[int8]*float64), make(map[int8]map[int8]*float64)
	nations := ti.Tables.Nations
	var natMap95, natMap96 map[int8]*float64
	//Idea: for every nation, have information for all the nations after it
	//Nat1: nat2, nat3, ..., natN. Nat10: nat11, nat12, ..., natN. NatN-1: natN.
	for i := range nations[:len(nations)-1] { //last nation will not "match" with any other
		natMap95, natMap96 = make(map[int8]*float64), make(map[int8]*float64)
		for j := range nations[i+1:] {
			natMap95[int8(i+1+j)], natMap96[int8(i+1+j)] = new(float64), new(float64)
		}
		q7Map[int8(i)] = map[int16]map[int8]*float64{1995: natMap95, 1996: natMap96}
	}
	return
}

func (ti TableInfo) q7CalcHelper(q7Map map[int8]map[int16]map[int8]*float64, order *tpch.Orders, orderItems []*tpch.LineItem) {
	orderDate := order.O_ORDERDATE
	var custNationKey, suppNationKey int8
	var custMap, suppMap map[int16]map[int8]*float64
	//Shipdate is orderdate + 1~121 days. So if year == 1994 && orderdate.Month < September or orderdate.Year >= 1997, no item will be relevant for this query.
	if (orderDate.YEAR == 1994 && orderDate.MONTH >= 9) || (orderDate.YEAR >= 1995 && orderDate.YEAR <= 1996) {
		custNationKey = ti.OrderToNationkey(order)
		custMap = q7Map[custNationKey]
		for _, item := range orderItems {
			if item.L_SHIPDATE.YEAR > 1996 || item.L_SHIPDATE.YEAR < 1995 {
				continue //This can happen when orderDate is close to the end of 1996.
			}
			suppNationKey = ti.Tables.SupplierkeyToNationkey(item.L_SUPPKEY)
			if suppNationKey < custNationKey {
				suppMap = q7Map[suppNationKey]
				*suppMap[item.L_SHIPDATE.YEAR][custNationKey] += (item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT))
			} else if suppNationKey > custNationKey {
				*custMap[item.L_SHIPDATE.YEAR][suppNationKey] += (item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT))
			}
			//else: If same nation, do nothing
		}
	}
}

func (ti TableInfo) makeQ7IndexUpds(q7Map map[int8]map[int16]map[int8]*float64, bucketI int) (upds []crdt.UpdateObjectParams, updsDone int) {
	//upds = make([]crdt.UpdateObjectParams, 2) //2 embMaps: 1995, 1996.
	mapUpd95, mapUpd96 := crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}, crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
	nations := ti.Tables.Nations

	var nat1Name, nat2Name string
	var innerUpd95, innerUpd96 crdt.EmbMapUpdateAll
	var nat1Map95, nat1Map96 map[int8]*float64

	for nat1Key, nat1Map := range q7Map {
		nat1Name = nations[nat1Key].N_NAME
		innerUpd95, innerUpd96 = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}, crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
		nat1Map95, nat1Map96 = nat1Map[1995], nat1Map[1996]
		for nat2Key, value := range nat1Map95 {
			nat2Name = nations[nat2Key].N_NAME
			innerUpd95.Upds[nat2Name] = crdt.Increment{Change: int32(*value)}
			innerUpd96.Upds[nat2Name] = crdt.Increment{Change: int32(*nat1Map96[nat2Key])}
			updsDone += 2
		}
		mapUpd95.Upds[nat1Name], mapUpd96.Upds[nat1Name] = innerUpd95, innerUpd96
		fmt.Printf("[ClientIndex][makeQ7Upds]Updates for nation %s (year 1995 only): %+v\n", nat1Name, innerUpd95)
	}
	args := crdt.EmbMapUpdateAll{Upds: map[string]crdt.UpdateArguments{"1995": mapUpd95, "1996": mapUpd96}}
	return []crdt.UpdateObjectParams{{KeyParams: crdt.KeyParams{Key: Q7_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}, UpdateArgs: args}}, updsDone
}

func (ti TableInfo) prepareQ8IndexLocal() (upds [][]crdt.UpdateObjectParams, updsDone int) {
	nationMap, regionMap := ti.createQ8LocalMaps()
	orders := ti.getOrders()
	for i, order := range orders {
		if order.O_ORDERDATE.YEAR == 1995 || order.O_ORDERDATE.YEAR == 1996 {
			ti.q8CalcHelperLocal(nationMap, regionMap, ti.Tables.LineItems[i], order.O_ORDERDATE.YEAR)
		}
	}
	upds = make([][]crdt.UpdateObjectParams, len(regionMap))
	regNUpds := 0
	for i := range upds {
		upds[i], regNUpds = ti.makeQ8IndexUpds(nationMap[i], regionMap[i], INDEX_BKT+i)
		updsDone += regNUpds
	}
	return
}

func (ti TableInfo) createQ8LocalMaps() (nationMap, regionMap []map[string]map[int8]*Q8YearPair) {
	nationMap, regionMap = make([]map[string]map[int8]*Q8YearPair, len(ti.Tables.Regions)), make([]map[string]map[int8]*Q8YearPair, len(ti.Tables.Regions))
	natsPerRegion := ti.Tables.NationsByRegion
	typesMap := ti.Tables.TypesToShortType
	var currNatMap, currRegionMap map[string]map[int8]*Q8YearPair
	var typeNatMap, typeRegMap map[int8]*Q8YearPair

	for i, natsR := range natsPerRegion {
		currNatMap, currRegionMap = make(map[string]map[int8]*Q8YearPair), make(map[string]map[int8]*Q8YearPair)
		for typeS := range typesMap {
			typeNatMap, typeRegMap = make(map[int8]*Q8YearPair), make(map[int8]*Q8YearPair)
			for _, j := range natsR {
				typeNatMap[int8(j)] = &Q8YearPair{sum1995: 0, sum1996: 0}
			}
			typeRegMap[int8(i)] = &Q8YearPair{sum1995: 0, sum1996: 0}
			fmt.Println(typeS)
			currNatMap[typeS], currRegionMap[typeS] = typeNatMap, typeRegMap
		}
		nationMap[i], regionMap[i] = currNatMap, currRegionMap
	}
	fmt.Println()
	return
}

func (ti TableInfo) prepareQ8Index() (upds []crdt.UpdateObjectParams, updsDone int) {
	//1x: Map[type]->Map[nation]->(1995,1996)->sum
	//1x: Map[type]->Map[region]->(1995,1996)->sum
	nationMap, regionMap := ti.createQ8Maps()
	orders := ti.getOrders()
	for i, order := range orders {
		if order.O_ORDERDATE.YEAR == 1995 || order.O_ORDERDATE.YEAR == 1996 {
			ti.q8CalcHelper(nationMap, regionMap, ti.Tables.LineItems[i], order.O_ORDERDATE.YEAR)
		}
	}
	return ti.makeQ8IndexUpds(nationMap, regionMap, INDEX_BKT)
}

func (q8yp *Q8YearPair) addSum(value float64, year int16) {
	if year == 1995 {
		q8yp.sum1995 += value
	} else {
		q8yp.sum1996 += value
	}
}

func (ti TableInfo) createQ8Maps() (nationMap, regionMap map[string]map[int8]*Q8YearPair) {
	nationMap, regionMap = make(map[string]map[int8]*Q8YearPair), make(map[string]map[int8]*Q8YearPair)
	typesMap := ti.Tables.TypesToShortType
	var typeNatMap, typeRegMap map[int8]*Q8YearPair
	nations, regions := ti.Tables.Nations, ti.Tables.Regions
	for typeS := range typesMap {
		typeNatMap, typeRegMap = make(map[int8]*Q8YearPair), make(map[int8]*Q8YearPair)
		for i := range nations {
			typeNatMap[int8(i)] = &Q8YearPair{sum1995: 0, sum1996: 0}
		}
		for i := range regions {
			typeRegMap[int8(i)] = &Q8YearPair{sum1995: 0, sum1996: 0}
		}
		nationMap[typeS], regionMap[typeS] = typeNatMap, typeRegMap
	}
	return
}

func (ti TableInfo) q8CalcHelper(nationMap, regionMap map[string]map[int8]*Q8YearPair, items []*tpch.LineItem, year int16) {
	var natId, regId int8
	var partType string
	var sum float64
	for _, item := range items {
		natId, partType = ti.Tables.SupplierkeyToNationkey(item.L_SUPPKEY), ti.Tables.Parts[item.L_PARTKEY].P_TYPE
		regId, sum = ti.Tables.Nations[natId].N_REGIONKEY, item.L_EXTENDEDPRICE*(1-item.L_DISCOUNT)
		nationMap[partType][natId].addSum(sum, year)
		regionMap[partType][regId].addSum(sum, year)
	}
}

func (ti TableInfo) q8CalcHelperLocal(nationMap, regionMap []map[string]map[int8]*Q8YearPair, items []*tpch.LineItem, year int16) {
	var natId, regId int8
	var partType string
	var sum float64
	for _, item := range items {
		natId, partType = ti.Tables.SupplierkeyToNationkey(item.L_SUPPKEY), ti.Tables.Parts[item.L_PARTKEY].P_TYPE
		regId, sum = ti.Tables.Nations[natId].N_REGIONKEY, item.L_EXTENDEDPRICE*(1-item.L_DISCOUNT)
		nationMap[regId][partType][natId].addSum(sum, year)
		regionMap[regId][partType][regId].addSum(sum, year)
	}
}

func (ti TableInfo) makeQ8IndexUpds(nationMap, regionMap map[string]map[int8]*Q8YearPair, bucketI int) (upds []crdt.UpdateObjectParams, nUpds int) {
	upds = make([]crdt.UpdateObjectParams, len(nationMap))
	_, nUpds = makeQ8IndexUpdsHelper(ti.Tables, nationMap, regionMap, upds, 0, bucketI)
	return
}

// Solution:
// type(key) -> Map[nationID+year|'R'+regionID+year]->sum
// This way, can still spread the view accross partitions but have the 4 needed counters (nation+region+2x year) in the same CRDT.
func makeQ8IndexUpdsHelper(tables *tpch.Tables, nationMap, regionMap map[string]map[int8]*Q8YearPair, buf []crdt.UpdateObjectParams, bufI, bucketI int) (newBufI, nUpds int) {
	var natMap map[int8]*Q8YearPair
	var mapUpd crdt.EmbMapUpdateAll
	keyParams := crdt.KeyParams{CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}
	var natIdS, regIdS string
	for partType, regMap := range regionMap {
		natMap = nationMap[partType]
		keyParams.Key = Q8_KEY + partType
		mapUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
		for regId, pair := range regMap {
			regIdS = "R" + strconv.FormatInt(int64(regId), 10)
			mapUpd.Upds[regIdS+"1995"] = crdt.IncrementFloat{Change: pair.sum1995}
			mapUpd.Upds[regIdS+"1996"] = crdt.IncrementFloat{Change: pair.sum1996}
		}
		for natId, pair := range natMap {
			natIdS = strconv.FormatInt(int64(natId), 10)
			mapUpd.Upds[natIdS+"1995"] = crdt.IncrementFloat{Change: pair.sum1995}
			mapUpd.Upds[natIdS+"1996"] = crdt.IncrementFloat{Change: pair.sum1996}
		}
		//fmt.Printf("[Q8][MakeUpds]Parttype: %s. RegMap: %+v. NatMap: %+v\n", partType, regMap, natMap)
		buf[bufI] = crdt.UpdateObjectParams{KeyParams: keyParams, UpdateArgs: mapUpd}
		bufI++
	}

	//nTypes * (nNations + nRegions) * 2 (years)
	return bufI, len(nationMap) * (len(tables.Nations) + len(tables.Regions)) * 2
}

func (ti TableInfo) prepareQ9IndexLocal() (upds [][]crdt.UpdateObjectParams, updsDone int) {
	q9RegionMap, orders := ti.createQ9LocalMap(), ti.getOrders()
	var regNUpds int
	for i, order := range orders {
		ti.q9LocalCalcHelper(q9RegionMap, ti.Tables.LineItems[i], order.O_ORDERDATE.YEAR)
	}
	upds, regNUpds = make([][]crdt.UpdateObjectParams, len(ti.Tables.Regions)), 0
	for i := range upds {
		fmt.Printf("Size of region %d: %d\n", i, len(q9RegionMap[i][ti.Tables.Colors[0]]))
		upds[i], regNUpds = ti.makeQ9IndexUpds(q9RegionMap[i], INDEX_BKT+i)
		updsDone += regNUpds
	}
	return
}

// For each row in the PART table, four rows in PartSupp table with:
// PS_PARTKEY = P_PARTKEY.
// PS_SUPPKEY = (ps_partkey + (i * (( S/4 ) + (int)(ps_partkey-1 )/S)))) modulo S + 1 where i is the ith supplier within [0 .. 3] and S = SF * 10,000.
// In practice, for SF=1 (10k Suppliers) it goes like: 2, 2502, 5002, 7502, 3, 2503, 5003, 7503, 4, 2504, 5004, 7504, 5, ...
func (ti TableInfo) prepareQ9Index() (upds []crdt.UpdateObjectParams, updsDone int) {
	//Name -> nation -> year. A query downloads all nations and years of a given name (thus, 1 download per query)
	//However, each *lineitem* can generate up to 5 updates. As each part has 5 colors.
	//This could, on worst case scenario, mean that a new order (up to 8 lineitems) could do 40 updates. And another 40 for remove.
	//So an add/remove cycle could lead to 80 updates!!!
	//Better bundle the whole view in one embedded CRDT.
	q9Map, orders := ti.createQ9Map(), ti.getOrders()
	for i, order := range orders {
		ti.q9CalcHelper(q9Map, ti.Tables.LineItems[i], order.O_ORDERDATE.YEAR)
	}
	return ti.makeQ9IndexUpds(q9Map, INDEX_BKT)
}

func (ti TableInfo) createQ9LocalMap() (q9RegionMap []map[string]map[int8]map[int16]*float64) {
	q9RegionMap = make([]map[string]map[int8]map[int16]*float64, len(ti.Tables.Regions))
	natsPerRegion := ti.Tables.NationsByRegion
	var regMap map[string]map[int8]map[int16]*float64
	var colorMap map[int8]map[int16]*float64
	for regId, regNats := range natsPerRegion {
		regMap = make(map[string]map[int8]map[int16]*float64)
		for _, color := range ti.Tables.Colors {
			colorMap = make(map[int8]map[int16]*float64)
			for _, natId := range regNats {
				colorMap[natId] = map[int16]*float64{1992: new(float64), 1993: new(float64), 1994: new(float64),
					1995: new(float64), 1996: new(float64), 1997: new(float64), 1998: new(float64)}
			}
			regMap[color] = colorMap
		}
		q9RegionMap[regId] = regMap
	}
	return
}

func (ti TableInfo) createQ9Map() (q9Map map[string]map[int8]map[int16]*float64) {
	q9Map = make(map[string]map[int8]map[int16]*float64)
	var colorMap map[int8]map[int16]*float64
	nations := ti.Tables.Nations
	for _, color := range ti.Tables.Colors {
		colorMap = make(map[int8]map[int16]*float64)
		for j := range nations {
			colorMap[int8(j)] = map[int16]*float64{1992: new(float64), 1993: new(float64), 1994: new(float64),
				1995: new(float64), 1996: new(float64), 1997: new(float64), 1998: new(float64)}
		}
		q9Map[color] = colorMap
	}
	return
}

func (ti TableInfo) q9LocalCalcHelper(q9RegionMap []map[string]map[int8]map[int16]*float64, items []*tpch.LineItem, year int16) {
	var natId int8
	var colors []string
	var partSupp *tpch.PartSupp
	var value float64
	var regionId int8
	for _, item := range items {
		natId = ti.Tables.SupplierkeyToNationkey(item.L_SUPPKEY)
		colors = strings.Split(ti.Tables.Parts[item.L_PARTKEY].P_NAME, " ")
		partSupp = ti.Tables.GetPartSuppOfLineitem(item.L_PARTKEY, item.L_SUPPKEY)
		value = item.L_EXTENDEDPRICE*(1-item.L_DISCOUNT) - partSupp.PS_SUPPLYCOST*float64(item.L_QUANTITY)
		regionId = ti.Tables.Nations[natId].N_REGIONKEY
		for _, color := range colors {
			*q9RegionMap[regionId][color][natId][year] += value
		}
	}
}

func (ti TableInfo) q9CalcHelper(q9Map map[string]map[int8]map[int16]*float64, items []*tpch.LineItem, year int16) {
	var natId int8
	var colors []string
	var partSupp *tpch.PartSupp
	var value float64
	for _, item := range items {
		natId = ti.Tables.SupplierkeyToNationkey(item.L_SUPPKEY)
		colors = strings.Split(ti.Tables.Parts[item.L_PARTKEY].P_NAME, " ")
		partSupp = ti.Tables.GetPartSuppOfLineitem(item.L_PARTKEY, item.L_SUPPKEY)
		value = item.L_EXTENDEDPRICE*(1-item.L_DISCOUNT) - partSupp.PS_SUPPLYCOST*float64(item.L_QUANTITY)
		for _, color := range colors {
			*q9Map[color][natId][year] += value
		}
	}
}

func (ti TableInfo) makeQ9IndexUpds(q9Map map[string]map[int8]map[int16]*float64, bucketI int) (upds []crdt.UpdateObjectParams, nUpds int) {
	//upds = make([]crdt.UpdateObjectParams, len(ti.Tables.Colors))
	upds = make([]crdt.UpdateObjectParams, 1)
	_, nUpds = makeQ9IndexUpdsHelper(ti.Tables, q9Map, upds, 0, bucketI)
	return
}

func makeQ9IndexUpdsHelper(tables *tpch.Tables, q9Map map[string]map[int8]map[int16]*float64, buf []crdt.UpdateObjectParams, bufI, bucketI int) (newBufI, nUpds int) {
	//var keyParams crdt.KeyParams
	var mapNationUpd, mapYearUpd crdt.EmbMapUpdateAll
	outerMapUpd := crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
	for color, colorMap := range q9Map {
		//keyParams = crdt.KeyParams{Key: Q9_KEY + color, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}
		mapNationUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
		for natId, natMap := range colorMap {
			mapYearUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
			for year, value := range natMap {
				mapYearUpd.Upds[Q9_YEARS[year-1992]] = crdt.IncrementFloat{Change: *value}
				nUpds++
			}
			mapNationUpd.Upds[tables.Nations[natId].N_NAME] = mapYearUpd
		}
		outerMapUpd.Upds[color] = mapNationUpd
		//var updArgs crdt.UpdateArguments = mapNationUpd
		//buf[bufI] = crdt.UpdateObjectParams{KeyParams: keyParams, UpdateArgs: &updArgs}
		//bufI++
	}
	buf[bufI] = crdt.UpdateObjectParams{KeyParams: crdt.KeyParams{Key: Q9_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}, UpdateArgs: outerMapUpd}
	bufI++
	//fmt.Printf("[ClientIndex][Q9][MakeUpds]UpdObjParams: %+v\n", buf[bufI-1])
	//fmt.Printf("[ClientIndex][Q9][MakeUpds]UpdArgs: %+v\n", *buf[bufI-1].UpdateArgs)
	return bufI, nUpds
}

func (ti TableInfo) prepareQ10IndexLocal() (upds [][]crdt.UpdateObjectParams, updsDone int) {
	//reg->quarter->custID->sum
	regQ10Info, orders := make([][]map[int32]*float64, len(ti.Tables.Regions)), ti.getOrders()
	nUpds := make([]int, len(ti.Tables.Regions))
	for i := range regQ10Info {
		regQ10Info[i], nUpds[i] = ti.createQ10Map(), 0
	}
	var regKey int8
	for i, order := range orders {
		if order.O_ORDERDATE.YEAR == 1993 || order.O_ORDERDATE.YEAR == 1994 {
			regKey = ti.Tables.OrderkeyToRegionkey(order.O_ORDERKEY)
			nUpds[regKey] += ti.q10CalcHelper(regQ10Info[regKey], order, ti.Tables.LineItems[i])
		}
	}
	upds = make([][]crdt.UpdateObjectParams, len(ti.Tables.Regions))
	for i, info := range regQ10Info {
		upds[i] = ti.makeQ10IndexUpds(info, INDEX_BKT+i, nUpds[i])
		updsDone += nUpds[i]
	}
	return
}

func (ti TableInfo) prepareQ10Index() (upds []crdt.UpdateObjectParams, updsDone int) {
	//Works by quarters. There's 8 quarters, from 1993 to 1995.
	//[quarter]map[custID]sum
	q10Info, orders := ti.createQ10Map(), ti.getOrders()
	for i, order := range orders {
		if order.O_ORDERDATE.YEAR == 1993 || order.O_ORDERDATE.YEAR == 1994 {
			updsDone += ti.q10CalcHelper(q10Info, order, ti.Tables.LineItems[i])
		}
	}
	return ti.makeQ10IndexUpds(q10Info, INDEX_BKT, updsDone), updsDone
}

func (ti TableInfo) createQ10Map() (q10Info []map[int32]*float64) {
	q10Info = make([]map[int32]*float64, 8) //2 years, 4 quarters
	for i := 0; i < 8; i++ {
		q10Info[i] = make(map[int32]*float64)
	}
	return
}

func (ti TableInfo) q10CalcHelper(q10Info []map[int32]*float64, order *tpch.Orders, lineItems []*tpch.LineItem) (nUpds int) {
	quarter := q10GetQuarterFromDate(&order.O_ORDERDATE)
	for _, item := range lineItems {
		if item.L_RETURNFLAG == "R" {
			clientValue, has := q10Info[quarter][order.O_CUSTKEY]
			if !has {
				clientValue = new(float64)
				q10Info[quarter][order.O_CUSTKEY] = clientValue
				nUpds++
			}
			*clientValue += item.L_EXTENDEDPRICE * (1 - L_DISCOUNT)
		}
	}
	return
}

func q10GetQuarterFromDate(date *tpch.Date) int {
	return int(int16((date.MONTH-1)/3) + (date.YEAR-1993)*4)
}

func (ti TableInfo) makeQ10IndexUpds(q10Info []map[int32]*float64, bucketI, nUpds int) (upds []crdt.UpdateObjectParams) {
	if useTopKAll {
		upds = make([]crdt.UpdateObjectParams, 8*2) //2 years: 8 quarters. *2 to account for TopKInits
	} else {
		upds = make([]crdt.UpdateObjectParams, nUpds+8) //+8: 1 TopKInit per quarter
	}
	var keyParams crdt.KeyParams
	j := 0
	for i, custMap := range q10Info {
		keyParams = crdt.KeyParams{Key: Q10_KEY + strconv.FormatInt(int64(i), 10), CrdtType: proto.CRDTType_TOPSUM, Bucket: buckets[bucketI]}
		upds[i*2] = crdt.UpdateObjectParams{KeyParams: keyParams, UpdateArgs: crdt.TopKInit{TopSize: 20}}
		if useTopKAll {
			currTopUpd := crdt.TopSAddAll{Scores: make([]crdt.TopKScore, len(custMap))}
			j = 0
			for custId, value := range custMap {
				currTopUpd.Scores[j] = crdt.TopKScore{Id: custId, Score: int32(-*value), Data: ti.packQ10IndexExtraDataFromKey(custId)}
				j++
			}
			upds[i*2+1] = crdt.UpdateObjectParams{KeyParams: keyParams, UpdateArgs: currTopUpd}
		} else {
			for custId, value := range custMap {
				upds[j] = crdt.UpdateObjectParams{KeyParams: keyParams, UpdateArgs: crdt.TopSAdd{TopKScore: crdt.TopKScore{Id: custId, Score: int32(*value)}}}
				j++
			}
		}
	}
	return
}

func (ti TableInfo) packQ10IndexExtraDataFromKey(custKey int32) (data *[]byte) {
	if INDEX_WITH_FULL_DATA {
		cust := ti.Tables.Customers[custKey]
		nationName := ti.Tables.Nations[cust.C_NATIONKEY].N_NAME
		return packQ10IndexExtraData(cust, nationName)
	}
	return new([]byte)
}

func packQ10IndexExtraData(cust *tpch.Customer, nationName string) (data *[]byte) {
	var build strings.Builder
	build.WriteString(strconv.FormatInt(int64(cust.C_CUSTKEY), 10))
	build.WriteRune('_')
	build.WriteString(cust.C_NAME)
	build.WriteRune('_')
	build.WriteString(cust.C_ACCTBAL)
	build.WriteRune('_')
	build.WriteString(cust.C_PHONE)
	build.WriteRune('_')
	build.WriteString(nationName)
	build.WriteRune('_')
	build.WriteString(cust.C_ADDRESS)
	build.WriteRune('_')
	build.WriteString(cust.C_COMMENT)
	buf := []byte(build.String())
	return &buf
}

func (ti TableInfo) prepareQ12IndexLocal() (upds [][]crdt.UpdateObjectParams, updsDone int) {
	q12MapReg, orders := make([]map[int16]map[string]map[string]*int32, len(ti.Tables.Regions)), ti.getOrders()
	for i := range q12MapReg {
		q12MapReg[i] = ti.createQ12Map()
	}
	for i, order := range orders {
		if (order.O_ORDERDATE.YEAR >= 1993 || (order.O_ORDERDATE.YEAR == 1992 && order.O_ORDERDATE.MONTH >= 9)) && order.O_ORDERDATE.YEAR <= 1997 {
			ti.q12CalcHelper(q12MapReg[ti.Custkey32ToRegionkey(order.O_CUSTKEY)], order, ti.Tables.LineItems[i])
		}
	}
	upds = make([][]crdt.UpdateObjectParams, len(q12MapReg))
	for i := range upds {
		upds[i] = ti.makeQ12IndexUpds(q12MapReg[i], INDEX_BKT+i)
	}
	return upds, len(q12MapReg) * 5 * len(ti.Tables.Modes) * 2
}

func (ti TableInfo) prepareQ12Index() (upds []crdt.UpdateObjectParams, updsDone int) {
	//Map[year]->Map[shipmode]->Map[priority]->counter
	//Each year is a different CRDT. Shipmodes and priorities are kept inside the same CRDT as two shipmodes and priorities must be downloaded per query
	//4 entries are downloaded per query: 2x priority per shipmode.
	//In CRDTs, shipmode and priority are together to avoid an extra level of redirection.
	//shipmode + priority
	q12Map, orders := ti.createQ12Map(), ti.getOrders()
	for i, order := range orders {
		if (order.O_ORDERDATE.YEAR >= 1993 || (order.O_ORDERDATE.YEAR == 1992 && order.O_ORDERDATE.MONTH >= 9)) && order.O_ORDERDATE.YEAR <= 1997 {
			ti.q12CalcHelper(q12Map, order, ti.Tables.LineItems[i])
		}
	}
	return ti.makeQ12IndexUpds(q12Map, INDEX_BKT), 5 * len(ti.Tables.Modes) * 2 //5: years [1993-1997]. 2: priority/non-priority
}

func (ti TableInfo) createQ12Map() (q12Map map[int16]map[string]map[string]*int32) {
	q12Map = make(map[int16]map[string]map[string]*int32)
	var yearMap map[string]map[string]*int32
	for year := int16(1993); year <= int16(1997); year++ {
		yearMap = make(map[string]map[string]*int32)
		for _, shipMode := range ti.Tables.Modes {
			yearMap[shipMode] = map[string]*int32{Q12_PRIORITY[0]: new(int32), Q12_PRIORITY[1]: new(int32)}
		}
		q12Map[year] = yearMap
	}
	return
}

func (ti TableInfo) q12CalcHelper(q12Map map[int16]map[string]map[string]*int32, order *tpch.Orders, items []*tpch.LineItem) {
	if order.O_ORDERPRIORITY[0] == '1' || order.O_ORDERPRIORITY[1] == '2' { //Priority
		for _, item := range items {
			if item.L_COMMITDATE.IsLower(&item.L_RECEIPTDATE) && item.L_SHIPDATE.IsLower(&item.L_COMMITDATE) && item.L_RECEIPTDATE.YEAR >= 1993 && item.L_RECEIPTDATE.YEAR <= 1997 {
				*q12Map[item.L_RECEIPTDATE.YEAR][item.L_SHIPMODE][Q12_PRIORITY[0]]++
			}
		}
	} else { //Non-priority
		for _, item := range items {
			if item.L_COMMITDATE.IsLower(&item.L_RECEIPTDATE) && item.L_SHIPDATE.IsLower(&item.L_COMMITDATE) && item.L_RECEIPTDATE.YEAR >= 1993 && item.L_RECEIPTDATE.YEAR <= 1997 {
				*q12Map[item.L_RECEIPTDATE.YEAR][item.L_SHIPMODE][Q12_PRIORITY[1]]++
			}
		}
	}
}

func (ti TableInfo) makeQ12IndexUpds(q12Map map[int16]map[string]map[string]*int32, bucketI int) (upds []crdt.UpdateObjectParams) {
	upds = make([]crdt.UpdateObjectParams, 5) //years: [1993-1997]
	makeQ12IndexUpdsHelper(q12Map, upds, 0, bucketI)
	return
}

func makeQ12IndexUpdsHelper(q12Map map[int16]map[string]map[string]*int32, buf []crdt.UpdateObjectParams, bufI, bucketI int) (newBufI int) {
	var keyParams crdt.KeyParams
	var mapUpd crdt.EmbMapUpdateAll
	for year, yearMap := range q12Map {
		keyParams = crdt.KeyParams{Key: Q12_KEY + strconv.FormatInt(int64(year), 10), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}
		mapUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
		for shipPriority, shipMap := range yearMap {
			mapUpd.Upds[shipPriority+Q12_PRIORITY[0]] = crdt.Increment{Change: *shipMap[Q12_PRIORITY[0]]}
			mapUpd.Upds[shipPriority+Q12_PRIORITY[1]] = crdt.Increment{Change: *shipMap[Q12_PRIORITY[1]]}
		}
		buf[bufI] = crdt.UpdateObjectParams{KeyParams: keyParams, UpdateArgs: mapUpd}
		bufI++
	}
	return bufI
}

func (ti TableInfo) prepareQ13IndexLocal() (upds [][]crdt.UpdateObjectParams, updsDone int) {
	q13RegionInfo = make([]Q13Info, len(ti.Tables.Regions))
	orders := ti.getOrders()
	for i := range q13RegionInfo {
		q13RegionInfo[i] = ti.createQ13Map()
	}
	ti.preloadQ13RegionMap()
	for _, order := range orders {
		ti.q13CalcHelper(q13RegionInfo[ti.Custkey32ToRegionkey(order.O_CUSTKEY)], order)
	}
	for _, info := range q13RegionInfo {
		ti.q13FromCustToNOrders(&info)
	}
	upds = make([][]crdt.UpdateObjectParams, len(q13RegionInfo))
	regNUpds := 0
	for i := range upds {
		upds[i], regNUpds = ti.makeQ13IndexUpds(q13RegionInfo[i], INDEX_BKT+i)
		updsDone += regNUpds
		q13Info.wordsToNOrders = nil
	}
	return
}

// Goal: map[Word1+Word2]->nOrders->nCustomers. Each entry respects
// Updating is then "easy": just move customer from one category or nOrders to another
func (ti TableInfo) prepareQ13Index() (upds []crdt.UpdateObjectParams, updsDone int) {
	q13Info = ti.createQ13Map()
	orders := ti.getOrders()
	ti.preloadQ13Map()
	for _, order := range orders {
		ti.q13CalcHelper(q13Info, order)
	}
	ti.q13FromCustToNOrders(&q13Info)
	upds, updsDone = ti.makeQ13IndexUpds(q13Info, INDEX_BKT)
	q13Info.wordsToNOrders = nil
	return
}

func (ti TableInfo) createQ13Map() (info Q13Info) {
	info = Q13Info{wordsToCustNOrders: make(map[string]map[int32]int8), custToNOrders: make(map[int32]int8), wordsToNOrders: make(map[string]map[int8]int32)}
	for _, fullWord := range Q13_BOTH_WORDS {
		info.wordsToCustNOrders[fullWord], info.wordsToNOrders[fullWord] = make(map[int32]int8), make(map[int8]int32)
	}
	return
}

func (ti TableInfo) preloadQ13Map() {
	// Preload with 0 on all customers as many customers (1/3) will not have any order
	for _, fullWord := range Q13_BOTH_WORDS {
		wordEntry := q13Info.wordsToCustNOrders[fullWord]
		for _, cust := range ti.Tables.Customers[1:] {
			wordEntry[cust.C_CUSTKEY] = 0
		}
	}
	for _, cust := range ti.Tables.Customers[1:] {
		q13Info.custToNOrders[cust.C_CUSTKEY] = 0
	}
}

func (ti TableInfo) preloadQ13RegionMap() {
	// Preload with 0 on all customers as many customers (1/3) will not have any order
	for _, fullWord := range Q13_BOTH_WORDS {
		wordEntries := []map[int32]int8{q13RegionInfo[0].wordsToCustNOrders[fullWord], q13RegionInfo[1].wordsToCustNOrders[fullWord],
			q13RegionInfo[2].wordsToCustNOrders[fullWord], q13RegionInfo[3].wordsToCustNOrders[fullWord], q13RegionInfo[4].wordsToCustNOrders[fullWord]}
		for _, cust := range ti.Tables.Customers[1:] {
			wordEntries[ti.Tables.Nations[cust.C_NATIONKEY].N_REGIONKEY][cust.C_CUSTKEY] = 0
		}
	}
	custEntries := []map[int32]int8{q13RegionInfo[0].custToNOrders, q13RegionInfo[1].custToNOrders, q13RegionInfo[2].custToNOrders, q13RegionInfo[3].custToNOrders, q13RegionInfo[4].custToNOrders}
	for _, cust := range ti.Tables.Customers[1:] {
		custEntries[ti.Tables.Nations[cust.C_NATIONKEY].N_REGIONKEY][cust.C_CUSTKEY] = 0
	}
}

func (ti TableInfo) q13CalcHelper(info Q13Info, order *tpch.Orders) {
	info.custToNOrders[order.O_CUSTKEY]++
	words, has := q13FindWordsInComment(order.O_COMMENT)
	if has {
		info.wordsToCustNOrders[words][order.O_CUSTKEY]++
	}
}

func q13FindWordsInComment(comment string) (words string, found bool) {
	for _, word := range Q13_Word1 {
		posW1 := strings.Index(comment, word)
		if posW1 != -1 {
			for _, word2 := range Q13_Word2 {
				posW2 := strings.LastIndex(comment, word2)
				if posW2 > posW1 {
					return word + word2, true
				}
			}
		}
	}
	return "", false
}

func (ti TableInfo) q13FromCustToNOrders(info *Q13Info) {
	for custID, nO := range info.custToNOrders {
		for words, custMap := range info.wordsToCustNOrders {
			info.wordsToNOrders[words][nO-custMap[custID]]++
		}
	}
}

// CRDT: [Word1+Word2] -> [nOrders]->nCustomers
// Keep it all under one CRDT as each update will need to update all words entries (except potencially one)
func (ti TableInfo) makeQ13IndexUpds(info Q13Info, bucketI int) (upds []crdt.UpdateObjectParams, nUpds int) {
	highestNOrders := int8(-1)
	for _, wordToNOrders := range info.wordsToNOrders {
		for nO := range wordToNOrders {
			if nO > highestNOrders {
				highestNOrders = nO
			}
		}
	}

	intToStringNO := make([]string, highestNOrders+1)
	for i := int64(1); i <= int64(highestNOrders); i++ {
		intToStringNO[i] = strconv.FormatInt(i, 10)
	}

	outerMapUpd := crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)} //Words -> Map
	var innerMapUpd crdt.EmbMapUpdateAll                                             //nOrders -> Counter
	for words, nOrdersMap := range info.wordsToNOrders {
		innerMapUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
		for nOrders, nCount := range nOrdersMap {
			innerMapUpd.Upds[intToStringNO[nOrders]] = crdt.Increment{Change: nCount}
			nUpds++
		}
		outerMapUpd.Upds[words] = innerMapUpd
	}
	upds = []crdt.UpdateObjectParams{{KeyParams: crdt.KeyParams{Key: Q13_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}, UpdateArgs: outerMapUpd}}
	return
}

/*
func (ti TableInfo) prepareQ13IndexLocal() (upds [][]crdt.UpdateObjectParams, updsDone int) {
	regionInfo, orders := make([]Q13Info, len(ti.Tables.Regions)), ti.getOrders()
	for i := range regionInfo {
		regionInfo[i] = ti.createQ13Map()
	}
	for _, order := range orders {
		ti.q13CalcHelper(regionInfo[ti.Custkey32ToRegionkey(order.O_CUSTKEY)], order)
	}
	for _, info := range regionInfo {
		ti.q13FromCustToNOrders(&info)
	}
	upds = make([][]crdt.UpdateObjectParams, len(regionInfo))
	regNUpds := 0
	for i := range upds {
		upds[i], regNUpds = ti.makeQ13IndexUpds(regionInfo[i], INDEX_BKT+i)
		updsDone += regNUpds
	}
	return
}

func (ti TableInfo) prepareQ13Index() (upds []crdt.UpdateObjectParams, updsDone int) {
	//o_comment not like ‘%[WORD1]%[WORD2]. Where Word1 has 4 possible values and Word2 has another 4 possible values.
	//Possible solution: have each entry contain all the orders that do not have Word1 followed by Word2.
	//This answers the query directly, but it implies each new order generates either 30 or 32 new updates.
	//Better solution: have a special entry with all orders. Then the "normal entries" contain only the orders that have the two words.
	//Good thing about the solution: all orders only update 1 or 2 CRDTs.
	//Bad thing: now 2 reads instead of 1 are needed.
	//This is a great trade-off, similar read complexity to many other queries.
	q13Info, orders := ti.createQ13Map(), ti.getOrders() //Shared info as later this info will be needed for new/rem orders
	for _, order := range orders {
		ti.q13CalcHelper(q13Info, order)
	}
	ti.q13FromCustToNOrders(&q13Info)
	upds, updsDone = ti.makeQ13IndexUpds(q13Info, INDEX_BKT)
	q13Info.nOrders, q13Info.wordsToNOrders = nil, nil //No longer needed
	return
}

func (ti TableInfo) createQ13Map() (info Q13Info) {
	q13Info := Q13Info{custToNOrders: make(map[int32]int8), wordsToCustNOrders: make(map[string]map[int32]int8),
		nOrders: make(map[int8]int32), wordsToNOrders: make(map[string]map[int8]int32)}
	for _, word1 := range Q13_Word1 {
		for _, word2 := range Q13_Word2 {
			fullWord := word1 + word2
			q13Info.wordsToCustNOrders[fullWord], q13Info.wordsToNOrders[fullWord] = make(map[int32]int8), make(map[int8]int32)
		}
	}
	return q13Info
}

func (ti TableInfo) q13CalcHelper(info Q13Info, order *tpch.Orders) {
	info.custToNOrders[order.O_CUSTKEY]++
	words, has := q13FindWordsInComment(order.O_COMMENT)
	if has {
		info.wordsToCustNOrders[words][order.O_CUSTKEY]++
	}
}

func q13FindWordsInComment(comment string) (words string, found bool) {
	for _, word := range Q13_Word1 {
		posW1 := strings.Index(comment, word)
		if posW1 != -1 {
			for _, word2 := range Q13_Word2 {
				posW2 := strings.LastIndex(comment, word2)
				if posW2 > posW1 {
					return word + word2, true
				}
			}
		}
	}
	return "", false
}

func (ti TableInfo) q13FromCustToNOrders(info *Q13Info) {
	for _, nO := range info.custToNOrders {
		info.nOrders[nO]++
	}
	var currNOrdersMap map[int8]int32
	for words, custMap := range info.wordsToCustNOrders {
		currNOrdersMap = info.wordsToNOrders[words]
		for _, nO := range custMap {
			currNOrdersMap[nO]++
		}
	}
	//Old maps are no longer necessary
	//info.wordsToCustNOrders, info.custToNOrders = nil, nil
}

// CRDT: nOrders -> count of customers with that number of orders
func (ti TableInfo) makeQ13IndexUpds(info Q13Info, bucketI int) (upds []crdt.UpdateObjectParams, nUpds int) {
	highestNOrders := int8(-1)
	for nO := range info.nOrders {
		if nO > highestNOrders {
			highestNOrders = nO
		}
	}
	upds = make([]crdt.UpdateObjectParams, len(info.wordsToNOrders)+1) //+1: the entry for all orders
	intToStringNO := make([]string, highestNOrders+1)
	fmt.Println(info.nOrders)
	info.nOrders[0] = int32(len(ti.Tables.Customers) - len(info.custToNOrders))
	for nO := range info.nOrders {
		intToStringNO[nO] = strconv.FormatInt(int64(nO), 10)
	}

	allOrderMapUpd := crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
	for nO, nCusts := range info.nOrders {
		allOrderMapUpd.Upds[intToStringNO[nO]] = crdt.Increment{Change: nCusts}
		nUpds++
	}

	var allOrderUpd crdt.UpdateArguments = allOrderMapUpd
	upds[0] = crdt.UpdateObjectParams{KeyParams: crdt.KeyParams{Key: Q13_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}, UpdateArgs: &allOrderUpd}
	i := 1
	var currMapUpd crdt.EmbMapUpdateAll
	for words, nOrdersMap := range info.wordsToNOrders {
		currMapUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
		for nOrders, nCount := range nOrdersMap {
			currMapUpd.Upds[intToStringNO[nOrders]] = crdt.Increment{Change: nCount}
			nUpds++
		}
		var args crdt.UpdateArguments = currMapUpd
		upds[i] = crdt.UpdateObjectParams{KeyParams: crdt.KeyParams{Key: Q13_KEY + words, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}, UpdateArgs: &args}
		i++
	}
	return upds, nUpds
}
*/

func (ti TableInfo) prepareQ16IndexLocal() (upds [][]crdt.UpdateObjectParams, updsDone int) {
	regQ16Info := make([]Q16Info, len(ti.Tables.Regions))
	for i := range regQ16Info {
		regQ16Info[i] = ti.createQ16Map()
	}
	var regKey int8
	for _, ps := range ti.Tables.PartSupps {
		regKey = ti.Tables.SuppkeyToRegionkey(int64(ps.PS_SUPPKEY))
		ti.q16CalcHelper(regQ16Info[regKey], ps)
	}
	upds = make([][]crdt.UpdateObjectParams, len(regQ16Info))
	regNUpds := 0
	for i, info := range regQ16Info {
		upds[i], regNUpds = ti.makeQ16IndexUpds(info, INDEX_BKT+i)
		updsDone += regNUpds
	}
	return
}

func (ti TableInfo) prepareQ16Index() (upds []crdt.UpdateObjectParams, updsDone int) {
	//No updates
	//Brand: 5*5. Type: 6*5. Size: 50.
	//We have to present one counter per triple (brand, type, size)
	//The query consists in 8 sizes, all brands except one, all types except one, and exclude suppliers with complains.
	//So, we have to download 24*29*8 = 5568 entries. :(
	//If we only filter on the client, we need to download 25*30*8 = 6000 entries. Still quite a difference.
	//If we create the CRDTs as size->brand->type, we only need to send 2 keys for the server to ignore.
	//So I suppose that is the solution :(

	q16Info := ti.createQ16Map()
	ti.calcSuppComplains(q16Info)
	for _, ps := range ti.Tables.PartSupps {
		ti.q16CalcHelper(q16Info, ps)
	}
	return ti.makeQ16IndexUpds(q16Info, INDEX_BKT)
}

func (ti TableInfo) createQ16Map() (info Q16Info) {
	info.suppsPerCategory = make(map[string]map[string]map[string]map[int32]struct{})
	info.complainSupps = make(map[int32]bool)
	sizes := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25",
		"26", "27", "28", "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "50"}
	var sizeMap map[string]map[string]map[int32]struct{}
	var brandMap map[string]map[int32]struct{}
	for _, size := range sizes {
		sizeMap = make(map[string]map[string]map[int32]struct{})
		for _, brand := range ti.Tables.Brands {
			brandMap = make(map[string]map[int32]struct{})
			for _, typeS := range ti.Tables.MediumType {
				brandMap[typeS] = make(map[int32]struct{})
			}
			sizeMap[brand] = brandMap
		}
		info.suppsPerCategory[size] = sizeMap
	}
	return
}

func (ti TableInfo) calcSuppComplains(q16Info Q16Info) {
	var custI, compI int
	for _, supp := range ti.Tables.Suppliers[1:] {
		custI = strings.Index(supp.S_COMMENT, "Customer")
		if custI > -1 {
			compI = strings.LastIndex(supp.S_COMMENT, "Complaints")
			if compI > custI {
				q16Info.complainSupps[supp.S_SUPPKEY] = true
			}
		}
	}
}

func (ti TableInfo) q16CalcHelper(q16Info Q16Info, ps *tpch.PartSupp) {
	if _, has := q16Info.complainSupps[ps.PS_SUPPKEY]; !has {
		part := ti.Tables.Parts[ps.PS_PARTKEY]
		q16Info.suppsPerCategory[part.P_SIZE][part.P_BRAND][ti.Tables.TypesToMediumType[part.P_TYPE]][ps.PS_SUPPKEY] = struct{}{}
	}
	return
}

func (ti TableInfo) makeQ16IndexUpds(q16Info Q16Info, bucketI int) (upds []crdt.UpdateObjectParams, nUpds int) {
	var allMapUpd, sizeMapUpd, brandMapUpd crdt.EmbMapUpdateAll
	allMapUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
	for size, sizeMap := range q16Info.suppsPerCategory {
		sizeMapUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
		for brand, brandMap := range sizeMap {
			brandMapUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
			for typeValue, typeMap := range brandMap {
				brandMapUpd.Upds[typeValue] = crdt.Increment{Change: int32(len(typeMap))}
				nUpds++
			}
			sizeMapUpd.Upds[brand] = brandMapUpd
		}
		allMapUpd.Upds[size] = sizeMapUpd
	}
	return []crdt.UpdateObjectParams{{KeyParams: crdt.KeyParams{Key: Q16_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}, UpdateArgs: allMapUpd}}, nUpds
}

/*type Q16Info struct {
	sizeSupps         map[string]map[int32]struct{}            //size -> suppkey
	filteredSizeSupps map[string]map[string]map[int32]struct{} //brand+type -> size -> suppkey
	complainSupps     map[int32]bool
}*/

/*
func (ti TableInfo) prepareQ16IndexLocal() (upds [][]crdt.UpdateObjectParams, updsDone int) {
	regQ16Info := make([]Q16Info, len(ti.Tables.Regions))
	for i := range regQ16Info {
		regQ16Info[i] = ti.createQ16Map()
	}
	var regKey int8
	for _, ps := range ti.Tables.PartSupps {
		regKey = ti.Tables.SuppkeyToRegionkey(int64(ps.PS_SUPPKEY))
		ti.q16CalcHelper(regQ16Info[regKey], ps)
	}
	upds = make([][]crdt.UpdateObjectParams, len(regQ16Info))
	regNUpds := 0
	for i, info := range regQ16Info {
		upds[i], regNUpds = ti.makeQ16IndexUpds(info, INDEX_BKT+i)
		updsDone += regNUpds
	}
	return
}

func (ti TableInfo) prepareQ16Index() (upds []crdt.UpdateObjectParams, updsDone int) {
	//No updates
	//Brand: 5*5. Type: 6*5. Size: 50. Logical solution: an entry contains how many suppliers provide a part of size s, not brand b and not type t.
	//If we consider updates, the "logical solution" will "explode" - a new PartSupp for a given size s will have to update 24*29 = 580+116 = 696 entries. Unrealistic.
	//Better solution (same style as Q13):
	//Have an entry of "suppliers for part of size s of ANY brand and ANY type"
	//Then each other entry only contains the "suppliers for part of size s of brand b and type t"
	//Have to do 16 instead of 8 reads that way (2x per size)

	//CRDT organization:
	//Brand+Type -> map[size]counter	(and then 1 CRDT with no brand or type ("all"))

	//In order to account for the count(distinct), we need to store all the supplierIDs, as there may be multiple PartSupps of a given size with the same SupplierID
	//This could be filtered on the client side and thus only use a counter, but updating would never be supported.
	//A more realistic solution is to use Set CRDT and only download the number of elements.
	//This works as the results are PER SIZE.

	q16Info := ti.createQ16Map()
	ti.calcSuppComplains(q16Info)
	for _, ps := range ti.Tables.PartSupps {
		ti.q16CalcHelper(q16Info, ps)
	}
	return ti.makeQ16IndexUpds(q16Info, INDEX_BKT)
}

func (ti TableInfo) createQ16Map() (info Q16Info) {
	info.sizeSupps, info.filteredSizeSupps, info.complainSupps = make(map[string]map[int32]struct{}), make(map[string]map[string]map[int32]struct{}), make(map[int32]bool)
	var innerMap map[string]map[int32]struct{}
	sizes := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25",
		"26", "27", "28", "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "50"}
	for _, brand := range ti.Tables.Brands {
		for _, typeS := range ti.Tables.MediumType {
			innerMap = make(map[string]map[int32]struct{})
			for _, size := range sizes {
				innerMap[size] = make(map[int32]struct{})
			}
			info.filteredSizeSupps[brand+typeS] = innerMap
		}
	}
	for _, size := range sizes {
		info.sizeSupps[size] = make(map[int32]struct{})
	}
	return
}

func (ti TableInfo) calcSuppComplains(q16Info Q16Info) {
	var custI, compI int
	for _, supp := range ti.Tables.Suppliers[1:] {
		custI = strings.Index(supp.S_COMMENT, "Customer")
		if custI > -1 {
			compI = strings.LastIndex(supp.S_COMMENT, "Complaints")
			if compI > custI {
				q16Info.complainSupps[supp.S_SUPPKEY] = true
			}
		}
	}
}

func (ti TableInfo) q16CalcHelper(q16Info Q16Info, ps *tpch.PartSupp) {
	if _, has := q16Info.complainSupps[ps.PS_SUPPKEY]; !has {
		part := ti.Tables.Parts[ps.PS_PARTKEY]
		q16Info.sizeSupps[part.P_SIZE][ps.PS_SUPPKEY] = struct{}{}
		q16Info.filteredSizeSupps[part.P_BRAND+ti.Tables.TypesToMediumType[part.P_TYPE]][part.P_SIZE][ps.PS_SUPPKEY] = struct{}{}
	}
	return
}

func (ti TableInfo) makeQ16IndexUpds(q16Info Q16Info, bucketI int) (upds []crdt.UpdateObjectParams, nUpds int) {
	upds = make([]crdt.UpdateObjectParams, len(tpchData.Tables.Brands)*len(tpchData.Tables.MediumType)+1) //+1 is for the CRDT of "ANY" brand and "ANY" type.
	var keyP crdt.KeyParams
	var innerMapUpd, allMapUpd crdt.EmbMapUpdateAll
	i := 0

	for bt, btSizeMap := range q16Info.filteredSizeSupps {
		innerMapUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
		for size, supps := range btSizeMap {
			innerMapUpd.Upds[size] = crdt.Increment{Change: int32(len(supps))}
			nUpds++
		}
		keyP = crdt.KeyParams{Key: Q16_KEY + bt, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}
		var args crdt.UpdateArguments = innerMapUpd
		upds[i] = crdt.UpdateObjectParams{KeyParams: keyP, UpdateArgs: &args}
		i++
	}
	allMapUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
	for size, suppsMap := range q16Info.sizeSupps {
		allMapUpd.Upds[size] = crdt.Increment{Change: int32(len(suppsMap))}
		nUpds++
	}
	keyP = crdt.KeyParams{Key: Q16_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}
	var newArgs crdt.UpdateArguments = allMapUpd
	upds[i] = crdt.UpdateObjectParams{KeyParams: keyP, UpdateArgs: &newArgs}
	return
}

/*func (ti TableInfo) makeQ16IndexUpds(q16Info Q16Info, bucketI int) (upds []crdt.UpdateObjectParams, nUpds int) {
	upds = make([]crdt.UpdateObjectParams, 50) //one embMapUpd per size
	var keyP crdt.KeyParams
	var innerMapUpd crdt.EmbMapUpdateAll
	i := 0

	for size, suppsMap := range q16Info.sizeSupps {
		innerMapUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
		for bt, btSizeMap := range q16Info.filteredSizeSupps {
			innerMapUpd.Upds[bt] = crdt.Increment{Change: int32(len(btSizeMap[size]))}
			nUpds++
		}
		innerMapUpd.Upds[Q16_ALL] = crdt.Increment{Change: int32(len(suppsMap))}
		keyP = crdt.KeyParams{Key: Q16_KEY + size, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}
		var args crdt.UpdateArguments = innerMapUpd
		upds[i] = crdt.UpdateObjectParams{KeyParams: keyP, UpdateArgs: &args}
		i++
		nUpds++
	}
	return
}*/

/*
func (ti TableInfo) makeQ16IndexUpds(q16Info Q16Info, bucketI int) (upds []crdt.UpdateObjectParams, nUpds int) {
	upds = make([]crdt.UpdateObjectParams, 50) //one embMapUpd per size
	var btSuppMap map[int32]struct{}
	var keyP crdt.KeyParams
	i, j := 0, 0
	for size, suppsMap := range q16Info.sizeSupps {
		innerMapUpd := crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
		for bt, btSizeMap := range q16Info.filteredSizeSupps {
			btSuppMap = btSizeMap[size]
			btUpd := crdt.AddAll{Elems: make([]crdt.Element, len(btSuppMap))}
			j = 0
			for suppKey := range btSuppMap {
				btUpd.Elems[j] = crdt.Element(strconv.FormatInt(int64(suppKey), 10))
				j++
			}
			innerMapUpd.Upds[bt] = btUpd
			nUpds++
		}
		sizeUpd := crdt.AddAll{Elems: make([]crdt.Element, len(suppsMap))}
		j = 0
		for suppKey := range suppsMap {
			sizeUpd.Elems[j] = crdt.Element(strconv.FormatInt(int64(suppKey), 10))
			j++
		}
		innerMapUpd.Upds[Q16_ALL] = sizeUpd
		keyP = crdt.KeyParams{Key: Q16_KEY + size, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}
		var args crdt.UpdateArguments = innerMapUpd
		upds[i] = crdt.UpdateObjectParams{KeyParams: keyP, UpdateArgs: &args}
		i++
		nUpds++
	}
	return
}*/

func (ti TableInfo) prepareQ17IndexLocal() (upds [][]crdt.UpdateObjectParams, updsDone int) {
	regQ17Info := make([]Q17Info, len(ti.Tables.Regions))
	for i := range regQ17Info {
		regQ17Info[i] = ti.createQ17Map()
	}
	var regKey int8
	for _, items := range ti.Tables.LineItems {
		regKey = ti.Tables.OrderkeyToRegionkey(items[0].L_ORDERKEY)
		ti.q17CalcHelper(regQ17Info[regKey], items)
	}
	upds = make([][]crdt.UpdateObjectParams, len(regQ17Info))
	for i, info := range regQ17Info {
		updsDone += len(info.pricePerQuantity)
		upds[i] = ti.makeQ17IndexUpds(info, INDEX_BKT+i)
	}
	return
}

func (ti TableInfo) prepareQ17Index() (upds []crdt.UpdateObjectParams, updsDone int) {
	//Two entries per pair of brand, container:
	//Map[brand+container]->avg
	//Map[brand+container]->Map[quantity]->sum(extendedprice)
	//Group both of these entries inside the same embedded Map CRDT.
	//So, for each brand+container, a Map(avg, Map))
	//The client downloads the avg and the quantity map, and sums up only the relevant quantities.
	//This solution is OK as, at most, each map has 50 entries (and on average, only ~5-6 are relevant).
	//Downloading 10 entries is a good heuristic (can download the remaining entries if the average is somehow very high.)
	q17Info := ti.createQ17Map()
	for _, items := range ti.Tables.LineItems {
		ti.q17CalcHelper(q17Info, items)
	}
	updsDone += len(q17Info.sum) //Averages
	for _, quantityMap := range q17Info.pricePerQuantity {
		updsDone += len(quantityMap) //Map
	}
	return ti.makeQ17IndexUpds(q17Info, INDEX_BKT), updsDone
}

func (ti TableInfo) createQ17Map() (info Q17Info) {
	info.sum, info.count, info.pricePerQuantity = make(map[string]int64), make(map[string]int64), make(map[string]map[int8]float64)
	brands, containers := ti.Tables.Brands, ti.Tables.Containers
	var bc string
	for _, brand := range brands {
		for _, container := range containers {
			bc = brand + container
			info.pricePerQuantity[bc] = make(map[int8]float64)
		}
	}
	return
}

func (ti TableInfo) q17CalcHelper(info Q17Info, items []*tpch.LineItem) (nUpds int) {
	var bc string
	var part *tpch.Part
	for _, item := range items {
		part = ti.Tables.Parts[item.L_PARTKEY]
		bc = part.P_BRAND + part.P_CONTAINER
		info.sum[bc] += int64(item.L_QUANTITY)
		info.count[bc]++
		info.pricePerQuantity[bc][item.L_QUANTITY] += item.L_EXTENDEDPRICE
	}
	return
}

func (ti TableInfo) makeQ17IndexUpds(q17Info Q17Info, bucketI int) (upds []crdt.UpdateObjectParams) {
	upds = make([]crdt.UpdateObjectParams, len(q17Info.sum))
	makeQ17IndexUpdsHelper(q17Info, upds, 0, bucketI)
	return upds
}

func makeQ17IndexUpdsHelper(q17Info Q17Info, buf []crdt.UpdateObjectParams, bufI, bucketI int) (newBufI int) {
	var keyParams crdt.KeyParams
	var count int64
	var quantityMap map[int8]float64
	var avgUpd crdt.AddMultipleValue
	var quantityUpd crdt.EmbMapUpdateAll
	quantitiesStrings := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23",
		"24", "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "50"}
	for bc, sum := range q17Info.sum {
		quantityMap, count = q17Info.pricePerQuantity[bc], q17Info.count[bc]
		keyParams = crdt.KeyParams{Key: Q17_KEY + bc, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}
		avgUpd = crdt.AddMultipleValue{SumValue: sum, NAdds: count}
		quantityUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
		for quantity, sum := range quantityMap {
			quantityUpd.Upds[quantitiesStrings[quantity]] = crdt.IncrementFloat{Change: sum}
		}
		buf[bufI] = crdt.UpdateObjectParams{KeyParams: keyParams, UpdateArgs: crdt.EmbMapUpdateAll{Upds: map[string]crdt.UpdateArguments{Q17_AVG: avgUpd, Q17_MAP: quantityUpd}}}
		bufI++
	}
	return bufI
}

/*
type PriceQuantityPair struct {
	price    float64
	quantity int8
}

type Q17Info struct {
	sum, count map[string]int64                       //sum: quantity.
	topInfo    map[string]map[int32]PriceQuantityPair //brand+container -> ORDERKEY+LINENUMBER*N_ORDERS
}

func (ti TableInfo) prepareQ17IndexLocal() (upds [][]crdt.UpdateObjectParams, updsDone int) {
	regQ17Info := make([]Q17Info, len(ti.Tables.Regions))
	nUpds := make([]int, len(regQ17Info))
	for i := range regQ17Info {
		regQ17Info[i], nUpds[i] = ti.createQ17Map(), 0
	}
	var regKey int8
	for _, items := range ti.Tables.LineItems {
		regKey = ti.Tables.OrderkeyToRegionkey(items[0].L_ORDERKEY)
		nUpds[regKey] += ti.q17CalcHelper(regQ17Info[regKey], items)
	}
	upds = make([][]crdt.UpdateObjectParams, len(regQ17Info))
	for i, info := range regQ17Info {
		upds[i] = ti.makeQ17IndexUpds(info, INDEX_BKT+i, nUpds[i])
		updsDone += nUpds[i]
	}
	return
}

func (ti TableInfo) prepareQ17Index() (upds []crdt.UpdateObjectParams, updsDone int) {
	//Note: L_EXTENDEDPRICE can be calculated from L_QUANTITY and P_RETAILPRICE
	//So in theory, we could have a map of P_PARTID -> P_RETAILPRICE or assume the client knows the price
	//And avoid having extendedprice in the TopK.
	//For query simplicity, this implementation leaves it as optional data in the Top.
	//Map[brand+container]->avg
	//Map[brand+container]->MinTopK(Id, quantity, extendedprice)
	//The client then has to sum all the extendedprice of quantity that match the filter.
	//To achieve the MinTopK, just put the quantity as negative.
	q17Info := ti.createQ17Map()
	for _, items := range ti.Tables.LineItems {
		updsDone += ti.q17CalcHelper(q17Info, items)
	}

	brand, container := tpchData.Tables.Brands[2], tpchData.Tables.Containers[0]
	sum, count, top := q17Info.sum[brand+container], q17Info.count[brand+container], q17Info.topInfo[brand+container]
	avg := float64(sum) / float64(count)
	compAvg := int8(avg * 0.2)
	total := 0.0
	for _, entry := range top {
		if entry.quantity <= compAvg {
			total += entry.price
		}
	}
	fmt.Printf("Q17CalcHelper. Brand %s, container %s has %d entries, avg %0.01f (comp to: %d) and yearly average loss %0.01f\n",
		brand, container, len(top), avg, compAvg, total/7.0)
	//Q17CalcHelper. Brand Brand#13, container SM CASE has avg 26.5 (comp to: 5) and yearly average loss 5397.3
	//Yearly average: 75214.77
	updsDone += (len(ti.Tables.Brands) * len(ti.Tables.Containers)) //Avgs
	return ti.makeQ17IndexUpds(q17Info, INDEX_BKT, updsDone), updsDone
}

func (ti TableInfo) createQ17Map() (info Q17Info) {
	info.sum, info.count, info.topInfo = make(map[string]int64), make(map[string]int64), make(map[string]map[int32]PriceQuantityPair)
	brands, containers := ti.Tables.Brands, ti.Tables.Containers
	var bc string
	for _, brand := range brands {
		for _, container := range containers {
			bc = brand + container
			info.topInfo[bc] = make(map[int32]PriceQuantityPair)
		}
	}
	return
}

func (ti TableInfo) q17CalcHelper(info Q17Info, items []*tpch.LineItem) (nUpds int) {
	var bc string
	var part *tpch.Part
	for _, item := range items {
		part = ti.Tables.Parts[item.L_PARTKEY]
		bc = part.P_BRAND + part.P_CONTAINER
		info.sum[bc] += int64(item.L_QUANTITY)
		info.count[bc]++
		info.topInfo[bc][item.L_ORDERKEY+(int32(TableEntries[tpch.ORDERS])*int32(item.L_LINENUMBER))] = PriceQuantityPair{price: item.L_EXTENDEDPRICE, quantity: item.L_QUANTITY}
		nUpds++
	}
	return
}

// Can use TopK instead of TopSum as each entry is a lineitem.
// Thus, no incs/decs are needed, just add and rem
func (ti TableInfo) makeQ17IndexUpds(q17Info Q17Info, bucketI, nUpds int) (upds []crdt.UpdateObjectParams) {
	if useTopKAll {
		upds = make([]crdt.UpdateObjectParams, len(q17Info.sum))
	} else {
		upds = make([]crdt.UpdateObjectParams, nUpds)
	}
	makeQ17IndexUpdsHelper(q17Info, upds, 0, bucketI)
	return
}

func makeQ17IndexUpdsHelper(q17Info Q17Info, buf []crdt.UpdateObjectParams, bufI, bucketI int) (newBufI int) {
	fmt.Println(len(q17Info.topInfo), len(q17Info.sum), len(q17Info.count))
	var keyParams crdt.KeyParams
	var topKAllUpd crdt.TopKAddAll
	var avgUpd crdt.AddMultipleValue
	//var sum, count int64
	var count int64
	var info map[int32]PriceQuantityPair
	var data []byte
	j := 0
	for bc, sum := range q17Info.sum {
		//for bc, info := range q17Info.topInfo {
		info, count = q17Info.topInfo[bc], q17Info.count[bc]
		//sum, count = q17Info.sum[bc], q17Info.count[bc]
		keyParams = crdt.KeyParams{Key: Q17_KEY + bc, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}
		avgUpd = crdt.AddMultipleValue{SumValue: sum, NAdds: count}
		if useTopKAll {
			topKAllUpd = crdt.TopKAddAll{Scores: make([]crdt.TopKScore, len(info))}
			j = 0
			for id, pair := range info {
				data = []byte(strconv.FormatFloat(pair.price, 'f', -1, 64))
				topKAllUpd.Scores[j] = crdt.TopKScore{Id: id, Score: -int32(pair.quantity), Data: &data}
				j++
			}
			var currUpd crdt.UpdateArguments = crdt.EmbMapUpdateAll{Upds: map[string]crdt.UpdateArguments{Q17_AVG: avgUpd, Q17_TOP: topKAllUpd}}
			buf[bufI] = crdt.UpdateObjectParams{KeyParams: keyParams, UpdateArgs: &currUpd}
			bufI++
		} else {
			var currUpd crdt.UpdateArguments = crdt.EmbMapUpdate{Key: Q17_AVG, Upd: avgUpd}
			buf[bufI] = crdt.UpdateObjectParams{KeyParams: keyParams, UpdateArgs: &currUpd}
			bufI++
			for id, pair := range info {
				data = []byte(strconv.FormatFloat(pair.price, 'f', -1, 64))
				var currUpd crdt.UpdateArguments = crdt.EmbMapUpdate{Key: Q17_TOP, Upd: crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: id, Score: -int32(pair.quantity), Data: &data}}}
				buf[bufI] = crdt.UpdateObjectParams{KeyParams: keyParams, UpdateArgs: &currUpd}
				bufI++
			}
		}
	}
	return bufI
}
*/

func (ti TableInfo) prepareQ19IndexLocal() (upds [][]crdt.UpdateObjectParams, updsDone int) {
	regQ19Info := make([]map[string]map[string]map[int8]float64, len(ti.Tables.Regions))
	for i := range regQ19Info {
		regQ19Info[i] = ti.createQ19Map()
	}
	var regKey int8
	for _, items := range ti.Tables.LineItems {
		regKey = ti.Tables.OrderkeyToRegionkey(items[0].L_ORDERKEY)
		ti.q19CalcHelper(regQ19Info[regKey], items)
	}
	upds = make([][]crdt.UpdateObjectParams, len(regQ19Info))
	nRegionUpds := 0
	for i, info := range regQ19Info {
		upds[i], nRegionUpds = ti.makeQ19IndexUpds(info, INDEX_BKT+i)
		updsDone += nRegionUpds
	}
	return
}

func (ti TableInfo) prepareQ19Index() (upds []crdt.UpdateObjectParams, updsDone int) {
	//Container(SM/MED/LG) -> Brand -> Quantity
	//Have to do one download per quantity
	q19Info := ti.createQ19Map()
	for _, items := range ti.Tables.LineItems {
		ti.q19CalcHelper(q19Info, items)
	}
	return ti.makeQ19IndexUpds(q19Info, INDEX_BKT)
}

func (ti TableInfo) createQ19Map() (q19Info map[string]map[string]map[int8]float64) {
	q19Info = map[string]map[string]map[int8]float64{"S": make(map[string]map[int8]float64), "M": make(map[string]map[int8]float64), "L": make(map[string]map[int8]float64)}
	for _, b := range ti.Tables.Brands {
		q19Info["S"][b], q19Info["M"][b], q19Info["L"][b] = make(map[int8]float64), make(map[int8]float64), make(map[int8]float64)
	}
	return
}

func (ti TableInfo) q19CalcHelper(q19Info map[string]map[string]map[int8]float64, items []*tpch.LineItem) {
	var itemEligible bool
	var containerType, brand string
	for _, item := range items {
		itemEligible, containerType, brand = isLineItemEligibleForQ19(ti.Tables, item)
		if itemEligible {
			q19Info[containerType][brand][item.L_QUANTITY] += (item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT))
		}
	}
}

func isLineItemEligibleForQ19(tables *tpch.Tables, item *tpch.LineItem) (eligible bool, containerS, brand string) {
	if !strings.HasSuffix(item.L_SHIPMODE, "AIR") || item.L_SHIPINSTRUCT[0] != 'D' || item.L_QUANTITY > 40 {
		return false, "", ""
	}
	part := tables.Parts[item.L_PARTKEY]
	containerType := part.P_CONTAINER[0]
	containerS, brand = string(containerType), part.P_BRAND
	if containerType == 'S' {
		subContainer := part.P_CONTAINER[3:]
		if subContainer[0] == 'P' || subContainer == "CASE" || strings.HasPrefix(subContainer, "BO") {
			if len(part.P_SIZE) == 1 || part.P_SIZE[0] <= '5' {
				if item.L_QUANTITY >= 1 && item.L_QUANTITY <= 20 {
					return true, containerS, brand
				}
			}
		}
	} else if containerType == 'M' {
		subContainer := part.P_CONTAINER[4:]
		if subContainer[0] == 'B' || subContainer[0] == 'P' {
			if len(part.P_SIZE) == 1 || (len(part.P_SIZE) == 2 && part.P_SIZE[1] == '0') { //I.e., P_SIZE >= 1 && P_SIZE <= 10
				if item.L_QUANTITY >= 10 && item.L_QUANTITY <= 30 {
					return true, containerS, brand
				}
			}
		}
	} else if containerType == 'L' {
		subContainer := part.P_CONTAINER[3:]
		if subContainer[0] == 'P' || subContainer == "CASE" || strings.HasPrefix(subContainer, "BO") {
			if len(part.P_SIZE) == 1 || (len(part.P_SIZE) == 2 && part.P_SIZE[1] >= '0' && part.P_SIZE[1] <= '5') {
				if item.L_QUANTITY >= 20 {
					return true, containerS, brand
				}
			}
		}
	}
	return false, "", ""
}

func (ti TableInfo) makeQ19IndexUpds(q19Info map[string]map[string]map[int8]float64, bucketI int) (upds []crdt.UpdateObjectParams, nUpds int) {
	//upds = make([]crdt.UpdateObjectParams, 3) //3 container types
	upds = make([]crdt.UpdateObjectParams, 1)
	_, nUpds = makeQ19IndexUpdsHelper(q19Info, upds, 0, bucketI)
	return
}

func makeQ19IndexUpdsHelper(q19Info map[string]map[string]map[int8]float64, buf []crdt.UpdateObjectParams, bufI, bucketI int) (newBufI, nUpds int) {
	outerUpd := crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments, 3)} //3 container types
	for container, containerMap := range q19Info {
		mapUpd := crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
		for brand, quantityMap := range containerMap {
			for quantity, value := range quantityMap {
				mapUpd.Upds[brand+Q19_QUANTITY[quantity]] = crdt.IncrementFloat{Change: value}
				nUpds++
			}
		}
		//buf[bufI] = crdt.UpdateObjectParams{KeyParams: crdt.KeyParams{Key: Q19_KEY + container, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}, UpdateArgs: mapUpd}
		//bufI++
		outerUpd.Upds[container] = mapUpd
	}
	buf[bufI] = crdt.UpdateObjectParams{KeyParams: crdt.KeyParams{Key: Q19_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}, UpdateArgs: outerUpd}
	return bufI + 1, nUpds
}

/*
func (ti TableInfo) prepareQ20IndexLocal() (upds [][]crdt.UpdateObjectParams, updsDone int) {
	q20Map := ti.createQ20Map()
	for _, items := range ti.Tables.LineItems {
		ti.q20CalcHelper(q20Map, items)
	}
	//Local query (nation). Organize it by regions however so that we can re-use makeQ20IndexUpds
	//[region]->[nation]->Map[color]->Map[year]->Map[partkey+supplierkey]->(sum, availqty)
	q20RegMap := make([]map[int8]map[string]map[int16]map[PairInt]int32, len(ti.Tables.Regions))
	for regId, nations := range ti.Tables.NationsByRegion {
		natMap := make(map[int8]map[string]map[int16]map[PairInt]int32, len(nations))
		for _, natId := range nations {
			natMap[natId] = q20Map[natId]
		}
		q20RegMap[regId] = natMap
	}

	upds = make([][]crdt.UpdateObjectParams, len(q20RegMap))
	regNUpds := 0
	for i, natMap := range q20RegMap {
		upds[i], regNUpds = ti.makeQ20IndexUpds(natMap, INDEX_BKT+i)
		updsDone += regNUpds
	}
	return
}
*/

func (ti TableInfo) prepareQ20IndexLocal() (upds [][]crdt.UpdateObjectParams, updsDone int) {
	q20Map := ti.createQ20Map()
	for _, items := range ti.Tables.LineItems {
		ti.q20CalcHelper(q20Map, items)
	}
	//Local query (nation). Organize it by regions however so that we can re-use makeQ20IndexUpds
	//[region]->[nation]->Map[color]->Map[partkey+supplierkey]->Map[year]->sum
	q20RegMap := make([]map[int8]map[string]map[PairInt]map[int16]int32, len(ti.Tables.Regions))
	for regId, nations := range ti.Tables.NationsByRegion {
		natMap := make(map[int8]map[string]map[PairInt]map[int16]int32, len(nations))
		for _, natId := range nations {
			natMap[natId] = q20Map[natId]
		}
		q20RegMap[regId] = natMap
	}

	upds = make([][]crdt.UpdateObjectParams, len(q20RegMap))
	regNUpds := 0
	for i, natMap := range q20RegMap {
		upds[i], regNUpds = ti.makeQ20IndexUpds(natMap, INDEX_BKT+i)
		updsDone += regNUpds
	}
	return
}

func (ti TableInfo) prepareQ20Index() (upds []crdt.UpdateObjectParams, updsDone int) {
	//Map[nation]->Map[color]->Map[year]->Map[partkey+supplierkey]->(sum, availqty)
	//One CRDT per nation. Color and year are together as one key (color+year)

	//Given we have supplierName, supplierAddress and availQty, which are the same independently of the years,
	//Maybe we should put them separately.
	//Map[nation]->Map[color]->Map[partkey+supplierkey]->(supplierName, supplierAddress, availQty, map[year]->sum)
	//Reading is still a "single read", but needs special arguments: need to ask to read supplierName, supplierAddress, availQty and then one entry of the map.

	q20Map := ti.createQ20Map()
	for _, items := range ti.Tables.LineItems {
		ti.q20CalcHelper(q20Map, items)
	}
	return ti.makeQ20IndexUpds(q20Map, INDEX_BKT)
}

func (ti TableInfo) createQ20Map() (q20Map map[int8]map[string]map[PairInt]map[int16]int32) {
	//nation->color->(partkey, supplierkey)->year->sum
	q20Map = make(map[int8]map[string]map[PairInt]map[int16]int32, len(ti.Tables.Nations))
	var nationMap map[string]map[PairInt]map[int16]int32
	for nationID := range ti.Tables.Nations {
		nationMap = make(map[string]map[PairInt]map[int16]int32, len(ti.Tables.Colors))
		for _, color := range ti.Tables.Colors {
			//nationMap[color] = map[PairInt]map[int16]int32{1993: make(map[PairInt]int32), 1994: make(map[PairInt]int32), 1995: make(map[PairInt]int32), 1996: make(map[PairInt]int32), 1997: make(map[PairInt]int32)}
			nationMap[color] = make(map[PairInt]map[int16]int32) //TODO: Size?
		}
		q20Map[int8(nationID)] = nationMap
	}
	//Go through PartSupps to create the PairInts
	var nationKey int8
	var part *tpch.Part
	var spaceIndex int
	var color string
	for _, ps := range ti.Tables.PartSupps {
		nationKey, part = ti.SupplierkeyToNationkey(ps.PS_SUPPKEY), ti.Tables.Parts[ps.PS_PARTKEY]
		spaceIndex = strings.Index(part.P_NAME, " ") //We only want the first color
		color = part.P_NAME[:spaceIndex]
		q20Map[nationKey][color][PairInt{first: ps.PS_PARTKEY, second: ps.PS_SUPPKEY}] = make(map[int16]int32)
	}
	return
}

func (ti TableInfo) q20CalcHelper(q20Map map[int8]map[string]map[PairInt]map[int16]int32, items []*tpch.LineItem) {
	var part *tpch.Part
	var spaceIndex int
	var nationKey int8
	var color string
	for _, item := range items {
		if item.L_SHIPDATE.YEAR >= 1993 && item.L_SHIPDATE.YEAR <= 1997 {
			part = ti.Tables.Parts[item.L_PARTKEY]
			spaceIndex, nationKey = strings.Index(part.P_NAME, " "), ti.Tables.SupplierkeyToNationkey(item.L_SUPPKEY)
			color = part.P_NAME[:spaceIndex]
			q20Map[nationKey][color][PairInt{first: item.L_PARTKEY, second: item.L_SUPPKEY}][item.L_SHIPDATE.YEAR] += int32(item.L_QUANTITY)
		}
	}
}

func (ti TableInfo) makeQ20IndexUpds(q20Map map[int8]map[string]map[PairInt]map[int16]int32, bucketI int) (upds []crdt.UpdateObjectParams, nUpds int) {
	upds = make([]crdt.UpdateObjectParams, len(q20Map)) //1 crdt per Nation
	var natMapUpd, colorMapUpd, pairMapUpd crdt.EmbMapUpdateAll
	var sup *tpch.Supplier
	var partsup *tpch.PartSupp
	i := 0
	//Map[nation]->Map[color]->Map[partkey+supplierkey]->(supplierName, supplierAddress, availQty, map[year]->sum)
	for natId, natMap := range q20Map {
		natMapUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
		fmt.Printf("[Q20][MakeIndexUpds]Entries for nation %d: %d\n", natId, len(natMap))
		for color, colorMap := range natMap {
			colorMapUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
			fmt.Printf("[Q20][MakeIndexUpds]Entries for color %s: %d\n", color, len(colorMap))
			for pairInfo, yearMap := range colorMap {
				sup, partsup = ti.Tables.Suppliers[pairInfo.second], ti.Tables.GetPartSuppOfLineitem(pairInfo.first, pairInfo.second)
				pairMapUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
				pairMapUpd.Upds[Q20_NAME], pairMapUpd.Upds[Q20_ADDRESS] = crdt.SetValue{NewValue: sup.S_NAME}, crdt.SetValue{NewValue: sup.S_ADDRESS}
				pairMapUpd.Upds[Q20_AVAILQTY] = crdt.Increment{Change: partsup.PS_AVAILQTY}
				for year, value := range yearMap {
					pairMapUpd.Upds[strconv.FormatInt(int64(year), 10)] = crdt.Increment{Change: value}
				}
				nUpds++
				fmt.Printf("[Q20][MakeIndexUpds]Entries for pairInfo %+v: %d. This should be okay though as we have %d updates for this entry.\n",
					pairInfo, len(yearMap), len(pairMapUpd.Upds))
				colorMapUpd.Upds[strconv.FormatInt(int64(pairInfo.first), 10)+"_"+strconv.FormatInt(int64(pairInfo.second), 10)] = pairMapUpd
			}
			//With low SFs it may happen there's no entries for the color
			if len(colorMapUpd.Upds) > 0 {
				natMapUpd.Upds[color] = colorMapUpd
			}
		}
		upds[i] = crdt.UpdateObjectParams{KeyParams: crdt.KeyParams{Key: Q20_KEY + strconv.FormatInt(int64(natId), 10), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}, UpdateArgs: natMapUpd}
		i++
	}
	return
}

/*
func (ti TableInfo) createQ20Map() (q20Map map[int8]map[string]map[int16]map[PairInt]int32) {
	q20Map = make(map[int8]map[string]map[int16]map[PairInt]int32, len(ti.Tables.Nations))
	var nationMap map[string]map[int16]map[PairInt]int32
	for nationID := range ti.Tables.Nations {
		nationMap = make(map[string]map[int16]map[PairInt]int32)
		for _, color := range ti.Tables.Colors {
			nationMap[color] = map[int16]map[PairInt]int32{1993: make(map[PairInt]int32), 1994: make(map[PairInt]int32), 1995: make(map[PairInt]int32), 1996: make(map[PairInt]int32), 1997: make(map[PairInt]int32)}
		}
		q20Map[int8(nationID)] = nationMap
	}
	return
}

func (ti TableInfo) q20CalcHelper(q20Map map[int8]map[string]map[int16]map[PairInt]int32, items []*tpch.LineItem) {
	var part *tpch.Part
	var spaceIndex int
	var nationKey int8
	var color string
	for _, item := range items {
		if item.L_SHIPDATE.YEAR >= 1993 && item.L_SHIPDATE.YEAR <= 1997 {
			part = ti.Tables.Parts[item.L_PARTKEY]
			spaceIndex, nationKey = strings.Index(part.P_NAME, " "), ti.Tables.SupplierkeyToNationkey(item.L_SUPPKEY)
			color = part.P_NAME[:spaceIndex]
			q20Map[nationKey][color][item.L_SHIPDATE.YEAR][PairInt{first: item.L_PARTKEY, second: item.L_SUPPKEY}] += int32(item.L_QUANTITY)
		}
	}
}

func (ti TableInfo) makeQ20IndexUpds(q20Map map[int8]map[string]map[int16]map[PairInt]int32, bucketI int) (upds []crdt.UpdateObjectParams, nUpds int) {
	upds = make([]crdt.UpdateObjectParams, len(q20Map)) //1 crdt per Nation
	var natMapUpd, yearMapUpd crdt.EmbMapUpdateAll
	var sup *tpch.Supplier
	var partsup *tpch.PartSupp
	i := 0
	for natId, natMap := range q20Map {
		natMapUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
		for color, colorMap := range natMap {
			for year, yearMap := range colorMap {
				yearMapUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
				fmt.Printf("[Q20][IndexUpds]Size of yearMap: %d (for color %s, year %d)\n", len(yearMap), color, year)
				for partSuppPair, count := range yearMap {
					sup, partsup = ti.Tables.Suppliers[partSuppPair.second], ti.Tables.GetPartSuppOfLineitem(partSuppPair.first, partSuppPair.second)
					yearMapUpd.Upds[strconv.FormatInt(int64(partSuppPair.first), 10)+"_"+strconv.FormatInt(int64(partSuppPair.second), 10)] =
						crdt.EmbMapUpdateAll{Upds: map[string]crdt.UpdateArguments{Q20_NAME: crdt.SetValue{NewValue: sup.S_NAME}, Q20_ADDRESS: crdt.SetValue{NewValue: sup.S_ADDRESS},
							Q20_AVAILQTY: crdt.Increment{Change: partsup.PS_AVAILQTY}, Q20_SUM: crdt.Increment{Change: count}}}
					nUpds++
				}
				natMapUpd.Upds[color+strconv.FormatInt(int64(year), 10)] = yearMapUpd
			}
		}
		var args crdt.UpdateArguments = natMapUpd
		upds[i] = crdt.UpdateObjectParams{KeyParams: crdt.KeyParams{Key: Q20_KEY + strconv.FormatInt(int64(natId), 10), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}, UpdateArgs: &args}
		i++
	}
	return
}*/

func (ti TableInfo) prepareQ21IndexLocal() (upds [][]crdt.UpdateObjectParams, updsDone int) {
	//Local query (nation). Organize it by regions however so that we can re-use makeQ21IndexUpds
	q21Map, orders := ti.createQ21Map(), ti.getOrders()
	for i, order := range orders {
		if order.O_ORDERSTATUS == "F" {
			ti.q21CalcHelper(q21Map, ti.Tables.LineItems[i])
		}
	}
	q21RegMap := make([]map[int8]map[int32]int32, len(ti.Tables.Regions))
	for regId, nations := range ti.Tables.NationsByRegion {
		natMap := make(map[int8]map[int32]int32, len(nations))
		for _, natId := range nations {
			natMap[natId] = q21Map[natId]
		}
		q21RegMap[regId] = natMap
	}

	upds = make([][]crdt.UpdateObjectParams, len(q21RegMap))
	regNUpds := 0
	for i, natMap := range q21RegMap {
		upds[i], regNUpds = ti.makeQ21IndexUpds(natMap, INDEX_BKT+i)
		updsDone += regNUpds
	}
	return
}

func (ti TableInfo) prepareQ21Index() (upds []crdt.UpdateObjectParams, updsDone int) {
	//Nation -> SupplierID -> count
	//CRDTs: one TopSum (supplierID, count, suppliername) per nation
	q21Map, orders := ti.createQ21Map(), ti.getOrders()
	for i, order := range orders {
		if order.O_ORDERSTATUS == "F" {
			ti.q21CalcHelper(q21Map, ti.Tables.LineItems[i])
		}
	}
	return ti.makeQ21IndexUpds(q21Map, INDEX_BKT)
}

func (ti TableInfo) createQ21Map() (q21Map map[int8]map[int32]int32) {
	q21Map = make(map[int8]map[int32]int32)
	for natId := range ti.Tables.Nations {
		q21Map[int8(natId)] = make(map[int32]int32)
	}
	return
}

func (ti TableInfo) q21CalcHelper(q21Map map[int8]map[int32]int32, items []*tpch.LineItem) {
	lateSuppKey := int32(-1)
	for _, item := range items {
		if item.L_RECEIPTDATE.IsHigher(&item.L_COMMITDATE) {
			if lateSuppKey != -1 && lateSuppKey != item.L_SUPPKEY {
				return //More than one supplier late, thus no update.
			}
			lateSuppKey = item.L_SUPPKEY
		}
	}
	if lateSuppKey == -1 {
		return //No supplier late, thus no update.
	}
	for _, item := range items { //Check if there exists a different supplierKey
		if item.L_SUPPKEY != lateSuppKey { //There is at least one other supplier, so can update
			q21Map[ti.Tables.SupplierkeyToNationkey(lateSuppKey)][lateSuppKey]++
			//fmt.Printf("[Q21]Late supplier! SuppId %d, NationId %d\n", lateSuppKey, ti.Tables.SupplierkeyToNationkey(lateSuppKey))
			return
		}
	}
	//If it reaches here, it means the supplier is late but he's the only supplier of the order - so no update.
}

func (ti TableInfo) makeQ21IndexUpds(q21Map map[int8]map[int32]int32, bucketI int) (upds []crdt.UpdateObjectParams, nUpds int) {
	upds = make([]crdt.UpdateObjectParams, len(q21Map)) //1 CRDT per nation
	j := 0
	for natId, natMap := range q21Map {
		topSumUpd, i := crdt.TopSAddAll{Scores: make([]crdt.TopKScore, len(natMap))}, 0
		nUpds += len(natMap)
		for suppKey, count := range natMap {
			data := []byte(ti.Tables.Suppliers[suppKey].S_NAME)
			topSumUpd.Scores[i] = crdt.TopKScore{Id: suppKey, Score: count, Data: &data}
			i++
		}
		upds[j] = crdt.UpdateObjectParams{KeyParams: crdt.KeyParams{Key: Q21_KEY + strconv.FormatInt(int64(natId), 10), CrdtType: proto.CRDTType_TOPSUM, Bucket: buckets[bucketI]}, UpdateArgs: topSumUpd}
		j++
	}
	return
}

func (ti TableInfo) prepareQ22IndexLocal() (upds [][]crdt.UpdateObjectParams, updsDone int) {
	regQ22Map, orders := ti.createLocalQ22Map(), ti.getOrders()
	q22CustNatBalances = make(map[int8]PairIntFloat)
	var natKey int8
	//To force the customers who have no orders to still show up on q22Map
	for _, cust := range ti.Customers[1:] {
		regQ22Map[ti.Tables.Nations[cust.C_NATIONKEY].N_REGIONKEY][cust.C_NATIONKEY+10][cust.C_CUSTKEY] = 0
	}
	for _, order := range orders {
		natKey = ti.Tables.OrderToNationkey(order)
		regQ22Map[ti.Tables.Nations[natKey].N_REGIONKEY][natKey+10][order.O_CUSTKEY]++
	}
	ti.q22CalcHelper() //Fill up q22CustNatBalances
	upds = make([][]crdt.UpdateObjectParams, len(ti.Tables.Regions))
	for i, q22Map := range regQ22Map {
		//fmt.Printf("[Q22]IndexLocal. Region: %d\n", i)
		upds[i] = ti.makeQ22LocalIndexUpds(q22Map, i, INDEX_BKT+i)
	}

	return upds, len(ti.Tables.Customers) + len(ti.Tables.Nations)
}

func (ti TableInfo) prepareQ22Index() (upds []crdt.UpdateObjectParams, updsDone int) {
	//The first 2 numbers of the phone number are the customer's country ID + 10.
	//So no need to check the phone - just check the nationID and add 10.
	//Map[country]->Map[custID]->(orderCount, acctbal)
	//Map[country]->Avg(acctbal)
	//The average and the map of customers will be stored together in one CRDT for efficiency
	q22Map, orders := ti.createQ22Map(), ti.getOrders()
	//To force the customers who have no orders to still show up on q22Map
	for _, cust := range ti.Customers[1:] {
		q22Map[cust.C_NATIONKEY+10][cust.C_CUSTKEY] = 0
	}
	for _, order := range orders {
		q22Map[ti.Tables.OrderToNationkey(order)+10][order.O_CUSTKEY]++
	}
	ti.q22CalcHelper() //Fill up q22CustNatBalances
	//Number of updates is len(ti.Tables.Customers) + len(ti.Tables.Nations) (last one is for the averages)
	return ti.makeQ22IndexUpds(q22Map, INDEX_BKT), len(ti.Tables.Customers) + len(ti.Tables.Nations)
}

func (ti TableInfo) q22CalcHelper() {
	q22CustNatBalances = make(map[int8]PairIntFloat)
	var balance float64
	var pair PairIntFloat
	for _, cust := range ti.Tables.Customers[1:] {
		balance, _ = strconv.ParseFloat(cust.C_ACCTBAL, 64)
		if balance > 0 {
			pair = q22CustNatBalances[cust.C_NATIONKEY+10]
			pair.first += balance
			pair.second++
			q22CustNatBalances[cust.C_NATIONKEY+10] = pair
		}
	}
}

func (ti TableInfo) createQ22Map() (q22Map map[int8]map[int32]int32) {
	q22Map = make(map[int8]map[int32]int32)
	for natID := range ti.Tables.Nations {
		q22Map[int8(natID+10)] = make(map[int32]int32)
	}
	return
}

func (ti TableInfo) createLocalQ22Map() (regQ22Map []map[int8]map[int32]int32) {
	//regionID->nationID->customerID->orderCount
	regQ22Map = make([]map[int8]map[int32]int32, len(ti.Tables.Regions))
	for regID, nats := range ti.NationsByRegion {
		currQ22Map := make(map[int8]map[int32]int32)
		for _, natID := range nats {
			currQ22Map[natID+10] = make(map[int32]int32)
		}
		regQ22Map[regID] = currQ22Map
	}
	return
}

func (ti TableInfo) makeQ22LocalIndexUpds(q22Map map[int8]map[int32]int32, regionID, bucketI int) (upds []crdt.UpdateObjectParams) {
	regMapUpds := crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}

	var natMapUpd crdt.EmbMapUpdateAll
	var pair PairIntFloat
	var acctbal float64
	var natIdString string
	//We already know all natIds are for this region
	for natId, custMap := range q22Map {
		natMapUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
		pair, natIdString = q22CustNatBalances[natId], strconv.FormatInt(int64(natId), 10)
		for custId, nOrders := range custMap {
			acctbal, _ = strconv.ParseFloat(ti.Tables.Customers[custId].C_ACCTBAL, 64)
			natMapUpd.Upds[strconv.FormatInt(int64(custId), 10)] = crdt.EmbMapUpdateAll{Upds: map[string]crdt.UpdateArguments{"C": crdt.Increment{Change: nOrders}, "A": crdt.Increment{Change: int32(acctbal)}}}
		}
		regMapUpds.Upds[natIdString], regMapUpds.Upds[Q22_AVG+natIdString] = natMapUpd, crdt.AddMultipleValue{SumValue: int64(pair.first), NAdds: pair.second}
	}
	return []crdt.UpdateObjectParams{{KeyParams: crdt.KeyParams{Key: Q22_KEY + strconv.Itoa(regionID), Bucket: buckets[bucketI], CrdtType: proto.CRDTType_RRMAP}, UpdateArgs: regMapUpds}}
}

func (ti TableInfo) makeQ22IndexUpds(q22Map map[int8]map[int32]int32, bucketI int) (upds []crdt.UpdateObjectParams) {
	upds = make([]crdt.UpdateObjectParams, len(ti.Tables.Regions)) //1 CRDT per region
	regMapUpds := make([]crdt.EmbMapUpdateAll, len(ti.Tables.Regions))
	for i := range regMapUpds {
		regMapUpds[i] = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
	}
	var pair PairIntFloat
	var acctbal float64
	var natMapUpd, currRegUpds crdt.EmbMapUpdateAll
	var natIdString string
	for natId, custMap := range q22Map {
		fmt.Printf("[Q22]MakeQ22IndexUpds. NatId: %d. Number of customers: %d\n", natId, len(custMap))
		natMapUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
		pair = q22CustNatBalances[natId]
		for custId, nOrders := range custMap {
			acctbal, _ = strconv.ParseFloat(ti.Tables.Customers[custId].C_ACCTBAL, 64)
			natMapUpd.Upds[strconv.FormatInt(int64(custId), 10)] = crdt.EmbMapUpdateAll{Upds: map[string]crdt.UpdateArguments{"C": crdt.Increment{Change: nOrders}, "A": crdt.Increment{Change: int32(acctbal)}}}
		}
		currRegUpds, natIdString = regMapUpds[tpchData.Tables.Nations[natId-10].N_REGIONKEY], strconv.FormatInt(int64(natId), 10)
		currRegUpds.Upds[natIdString], currRegUpds.Upds[Q22_AVG+natIdString] = natMapUpd, crdt.AddMultipleValue{SumValue: int64(pair.first), NAdds: pair.second}
	}
	for i, regMap := range regMapUpds {
		fmt.Printf("[Q22]MakeQ22IndexUpds. Region %d. NUpds: %d\n", i, len(regMap.Upds))
		upds[i] = crdt.UpdateObjectParams{KeyParams: crdt.KeyParams{Key: Q22_KEY + strconv.Itoa(i), Bucket: buckets[bucketI], CrdtType: proto.CRDTType_RRMAP}, UpdateArgs: regMap}
	}
	return upds
}

/*
func (ti TableInfo) makeQ22IndexUpds(q22Map map[int8]map[int32]int32, bucketI int) (upds []crdt.UpdateObjectParams) {
	upds = make([]crdt.UpdateObjectParams, len(q22Map)) //1 CRDT per nation
	var pair PairIntFloat
	var acctbal float64
	for natId, custMap := range q22Map {
		natMapUpd := crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
		pair = q22CustNatBalances[natId]
		natMapUpd.Upds[Q22_AVG] = crdt.AddMultipleValue{SumValue: int64(pair.first), NAdds: pair.second}
		//Probably need to move this inside another map
		for custId, nOrders := range custMap {
			acctbal, _ = strconv.ParseFloat(ti.Tables.Customers[custId].C_ACCTBAL, 64)
			natMapUpd.Upds[strconv.FormatInt(int64(custId), 10)] = crdt.EmbMapUpdateAll{Upds: map[string]crdt.UpdateArguments{"C": crdt.Increment{Change: nOrders}, "A": crdt.Increment{Change: int32(acctbal)}}}
		}
		var args crdt.UpdateArguments = natMapUpd
		upds[natId-10] = crdt.UpdateObjectParams{KeyParams: crdt.KeyParams{Key: Q22_KEY + strconv.FormatInt(int64(natId), 10), Bucket: buckets[bucketI], CrdtType: proto.CRDTType_RRMAP}, UpdateArgs: &args}
	}
	return upds
}
*/
