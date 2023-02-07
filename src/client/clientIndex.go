package client

import (
	"fmt"
	"math/rand"
	"potionDB/src/antidote"
	"potionDB/src/crdt"
	"potionDB/src/proto"
	"strconv"
	"strings"
	"time"

	//"tpch_client/src/tpch"
	"tpch_data/tpch"
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
type PairInt struct {
	first  int32
	second int32
}

//Idea: allow concurrent goroutines to use different versions of Orders and Lineitems table
type TableInfo struct {
	*tpch.Tables
}

const (
	MIN_MONTH_DAY, MAX_MONTH_DAY      = int8(1), int8(31)
	Q3_N_EXTRA_DATA, Q18_N_EXTRA_DATA = 2, 4
)

var (
	//"Constants"
	MIN_DATE_Q3, MAX_DATE_Q3 = &tpch.Date{YEAR: 1995, MONTH: 03, DAY: 01}, &tpch.Date{YEAR: 1995, MONTH: 03, DAY: 31}
	//Need to store this in order to avoid on updates having to recalculate everything
	q15Map      map[int16]map[int8]map[int32]*float64
	q15LocalMap []map[int16]map[int8]map[int32]*float64
)

func prepareIndexesToSend() {
	ti := TableInfo{Tables: procTables}
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
	if DOES_UPDATES {
		go startUpdates()
	}
}

func queueIndex(upds []antidote.UpdateObjectParams) {
	//Sends to a random server if using splitIndexLoad, otherwise defaults to 0
	//channels.indexChans[0] <- QueuedMsg{Message: antidote.CreateUpdateObjs(nil, upds), code: antidote.UpdateObjs}
	channels.indexChans[rand.Intn(len(channels.indexChans))] <- QueuedMsg{Message: antidote.CreateUpdateObjs(nil, upds), code: antidote.UpdateObjs}
}

func prepareIndexesLocalToSend() {
	ti := TableInfo{Tables: procTables}
	startTime := time.Now().UnixNano() / 1000000
	//TODO: Later consider 6, 10, 2, by this order.
	fmt.Println("Preparing indexes...")
	//queueIndex(prepareQ2Index())
	q3Upds, q3N := ti.prepareQ3IndexLocal()
	queueIndexLocal(q3Upds)
	times.prepareIndexProtos[0] = time.Now().UnixNano() / 1000000
	fmt.Println("Index Q3 OK")
	_, q5Upds, q5N := ti.prepareQ5Index()
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
	if DOES_UPDATES {
		go startUpdates()
	}
}

func queueIndexLocal(upds [][]antidote.UpdateObjectParams) {
	for i, chanUpds := range upds {
		channels.indexChans[i] <- QueuedMsg{Message: antidote.CreateUpdateObjs(nil, chanUpds), code: antidote.UpdateObjs}
	}
}

/*
func prepareQ2Index() (upds []antidote.UpdateObjectParams) {
	//topk: max(s_acctbal).
	//Each TopK contains the suppliers that supply a part of a given size and type at min cost.
	//TopK should be applied for each pair of (pair.size, pair.type, region)
	//Each entry in a TopK is the supplierID and the supplycost.
	//Each TopK internally should be ordered from min to max. We can achieve this using negative values.

	//For now extra data (s_acctbal, n_name, p_mfgr, s_address, etc.) needs to be fetched separatelly.
	//We'll

	return
}
*/

func (ti TableInfo) prepareQ3IndexLocal() (upds [][]antidote.UpdateObjectParams, updsDone int) {
	//Segment -> orderDate (day) -> orderKey
	sumMap := make([]map[string]map[int8]map[int32]*float64, len(ti.Tables.Regions))
	nUpds := make([]int, len(sumMap))
	for i := range sumMap {
		sumMap[i] = createQ3Map()
	}

	regionI := int8(0)
	orders := ti.Tables.Orders
	if orders[0] == nil {
		//Happens only for the initial data
		orders = orders[1:]
	}
	for orderI, order := range orders {
		if order.O_ORDERDATE.IsSmallerOrEqual(MAX_DATE_Q3) {
			regionI = ti.Tables.OrderkeyToRegionkey(order.O_ORDERKEY)
			nUpds[regionI] += ti.q3CalcHelper(sumMap[regionI], order, orderI)
		}
	}

	for _, rUpds := range nUpds {
		updsDone += rUpds
	}

	//TODO: Don't I need to override nProtoUpds also here?
	//Override nUpds if useTopKAll
	if useTopKAll {
		for i, regionSumMap := range sumMap {
			nUpds[i] = 0
			for _, segMap := range regionSumMap {
				nUpds[i] += len(segMap)
			}
		}
	}

	upds = make([][]antidote.UpdateObjectParams, len(nUpds))
	for i := range upds {
		upds[i] = ti.makeQ3IndexUpds(sumMap[i], nUpds[i], INDEX_BKT+i)
	}

	return
}

func (ti TableInfo) prepareQ3Index() (upds []antidote.UpdateObjectParams, updsDone int) {
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
	orders := ti.Tables.Orders
	if orders[0] == nil {
		//Happens only for the initial data
		orders = orders[1:]
	}
	for orderI, order := range orders {
		//To be in range for the maps, o_orderDate must be <= than the highest date 1995-03-31
		//And l_shipdate must be >= than the smallest date 1995-03-01
		if order.O_ORDERDATE.IsSmallerOrEqual(MAX_DATE_Q3) {
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

	return ti.makeQ3IndexUpds(sumMap, nProtoUpds, INDEX_BKT), updsDone
}

//TODO: I might need to review this... shouldn't I only be writting positions between orderdate and shipdate?
//Note: not much efficient, but we don't really know how old can an orderdate be without being shipped. And we also don't have any index for dates
func (ti TableInfo) q3CalcHelper(sumMap map[string]map[int8]map[int32]*float64, order *tpch.Orders, orderI int) (nUpds int) {
	var minDay = MIN_MONTH_DAY
	var currSum *float64
	var has bool
	var j int8

	//fmt.Println("OrderDate:", *order.O_ORDERDATE, ". Compare result:", order.O_ORDERDATE.IsSmallerOrEqual(maxDate))
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

//Segment -> orderDate (day) -> orderKey
func createQ3Map() (sumMap map[string]map[int8]map[int32]*float64) {
	sumMap = make(map[string]map[int8]map[int32]*float64)
	var j int8
	for _, seg := range procTables.Segments {
		segMap := make(map[int8]map[int32]*float64)
		//Days
		for j = 1; j <= 31; j++ {
			segMap[j] = make(map[int32]*float64)
		}
		sumMap[seg] = segMap
	}
	return
}

func (ti TableInfo) prepareQ5Index() (upds []antidote.UpdateObjectParams, multiUpds [][]antidote.UpdateObjectParams, updsDone int) {
	//2.4.5:
	//sum: l_extendedprice * (1 - l_discount)
	//Group this by the pair (country, year) (i.e., group all the days of a year in the same CRDT).
	//Only lineitems for which the costumer and supplier are of the same nation count for this sum.
	//On the query, we ask for a given year and region. However, the results must be shown by nation
	//EmbMap CRDT for each region+date and then inside each there's a CounterCRDT for nation?

	var i int16
	//Region -> Year -> Country -> Sum
	sumMap := make(map[int8]map[int16]map[int8]*float64)
	//Registering regions and dates
	for _, region := range ti.Tables.Regions {
		regMap := make(map[int16]map[int8]*float64)
		for i = 1993; i <= 1997; i++ {
			regMap[i] = make(map[int8]*float64)
		}
		sumMap[region.R_REGIONKEY] = regMap
	}
	//Registering countries
	for _, nation := range ti.Tables.Nations {
		regMap := sumMap[nation.N_REGIONKEY]
		for i = 1993; i <= 1997; i++ {
			value := 0.0
			regMap[i][nation.N_NATIONKEY] = &value
		}
	}

	//Actually collecting the data
	orders := ti.Tables.Orders
	if orders[0] == nil {
		//Happens only for the initial data
		orders = orders[1:]
	}
	var order *tpch.Orders
	var customer *tpch.Customer
	var supplier *tpch.Supplier
	var year int16
	var nationKey, regionKey int8
	var value *float64

	//for i, orderItems := range ti.Tables.LineItems[1:] {
	for i, orderItems := range ti.Tables.LineItems {
		order = orders[i]
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

	//Upds done: 5 (years) * nations
	if isIndexGlobal {
		return ti.makeQ5IndexUpds(sumMap, INDEX_BKT), nil, 5 * len(ti.Tables.Nations)
	}
	//Create temporary maps with just one region, in order to receive the upds separatelly
	multiUpds = make([][]antidote.UpdateObjectParams, len(ti.Tables.Regions))
	for i, regMap := range sumMap {
		multiUpds[i] = ti.makeQ5IndexUpds(map[int8]map[int16]map[int8]*float64{i: regMap}, INDEX_BKT+int(i))
	}
	return nil, multiUpds, 5 * len(ti.Tables.Nations)
}

func (ti TableInfo) prepareQ11IndexLocal() (upds [][]antidote.UpdateObjectParams, updsDone int) {
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

	upds = make([][]antidote.UpdateObjectParams, len(ti.Tables.Regions))
	for i := range upds {
		upds[i] = ti.makeQ11IndexUpds(nationMap[i], totalSumMap[i], updsPerRegion[i], INDEX_BKT+i)
	}

	return
}

func (ti TableInfo) prepareQ11Index() (upds []antidote.UpdateObjectParams, updsDone int) {
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

func (ti TableInfo) prepareQ14IndexLocal() (upds [][]antidote.UpdateObjectParams, updsDone int) {
	mapPromo, mapTotal := make([]map[string]*float64, len(ti.Tables.Regions)), make([]map[string]*float64, len(ti.Tables.Regions))
	inPromo := ti.Tables.PromoParts

	for i := range mapPromo {
		mapPromo[i], mapTotal[i] = createQ14Maps()
	}

	regionKey := int8(0)
	orders := ti.Tables.Orders
	if orders[0] == nil {
		//Happens only for the initial data
		orders = orders[1:]
	}
	//for i, orderItems := range ti.Tables.LineItems[1:] {
	for i, orderItems := range ti.Tables.LineItems {
		//regionKey = ti.Tables.OrderkeyToRegionkey(ti.Tables.Orders[i+1].O_ORDERKEY)
		regionKey = ti.Tables.OrderkeyToRegionkey(orders[i].O_ORDERKEY)
		ti.q14CalcHelper(orderItems, mapPromo[regionKey], mapTotal[regionKey], inPromo)
	}

	upds = make([][]antidote.UpdateObjectParams, len(ti.Tables.Regions))
	for i := range upds {
		upds[i] = ti.makeQ14IndexUpds(mapPromo[i], mapTotal[i], INDEX_BKT+i)
	}
	//NRegions * years (1993-1997) * months (1-12)
	updsDone = len(ti.Tables.Regions) * 5 * 12
	return
}

func (ti TableInfo) prepareQ14Index() (upds []antidote.UpdateObjectParams, updsDone int) {
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

func (ti TableInfo) prepareQ15IndexLocal() (upds [][]antidote.UpdateObjectParams, updsDone int) {
	yearMap := make([]map[int16]map[int8]map[int32]*float64, len(ti.Tables.Regions))
	nUpds := make([]int, len(ti.Tables.Regions))
	q15LocalMap = yearMap

	for i := range yearMap {
		yearMap[i] = createQ15Map()
	}

	rKey := int8(0)
	orders := ti.Tables.Orders
	if orders[0] == nil {
		//Happens only for the initial data
		orders = orders[1:]
	}

	for i, orderItems := range ti.Tables.LineItems {
		//for i, orderItems := range ti.Tables.LineItems[1:] {
		//rKey = ti.Tables.OrderkeyToRegionkey(ti.Tables.Orders[i+1].O_ORDERKEY)
		rKey = ti.Tables.OrderkeyToRegionkey(orders[i].O_ORDERKEY)
		nUpds[rKey] += ti.q15CalcHelper(orderItems, yearMap[rKey])
	}

	for _, nUpdsR := range nUpds {
		updsDone += nUpdsR
	}

	upds = make([][]antidote.UpdateObjectParams, len(ti.Tables.Regions))
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

//TODO: Can I really assume that quarters are Jan-Mar, Apr-Jun, Jul-Sep, Oct-Dec?
func (ti TableInfo) prepareQ15Index() (upds []antidote.UpdateObjectParams, updsDone int) {
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

func (ti TableInfo) prepareQ18IndexLocal() (upds [][]antidote.UpdateObjectParams, updsDone int) {
	quantityMap := make([]map[int32]map[int32]*PairInt, len(ti.Tables.Regions))
	for i := range quantityMap {
		quantityMap[i] = map[int32]map[int32]*PairInt{312: make(map[int32]*PairInt), 313: make(map[int32]*PairInt),
			314: make(map[int32]*PairInt), 315: make(map[int32]*PairInt)}
	}

	regionKey := int8(0)
	orders := ti.Tables.Orders
	if orders[0] == nil {
		//Happens only for the initial data
		orders = orders[1:]
	}
	for i, order := range orders {
		//for i, order := range ti.Tables.Orders[1:] {
		regionKey = ti.Tables.OrderkeyToRegionkey(order.O_ORDERKEY)
		ti.q18CalcHelper(quantityMap[regionKey], order, i)
	}

	upds = make([][]antidote.UpdateObjectParams, len(ti.Tables.Regions))
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

//TODO: This likelly could be reimplemented with only 1 topK and using the query of "above value".
//Albeit that potencially would imply downloading more than 100 customers.
func (ti TableInfo) prepareQ18Index() (upds []antidote.UpdateObjectParams, updsDone int) {
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

	orders := ti.Tables.Orders
	if orders[0] == nil {
		//Happens only for the initial data
		orders = orders[1:]
	}
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

func (ti TableInfo) makeQ3IndexUpds(sumMap map[string]map[int8]map[int32]*float64, nUpds int, bucketI int) (upds []antidote.UpdateObjectParams) {
	upds = make([]antidote.UpdateObjectParams, nUpds)
	var keyArgs antidote.KeyParams
	i := 0
	if INDEX_WITH_FULL_DATA {
		for mktSeg, segMap := range sumMap {
			for day, dayMap := range segMap {
				//A topK per pair (mktsegment, orderdate)
				keyArgs = antidote.KeyParams{
					Key:      SEGM_DELAY + mktSeg + strconv.FormatInt(int64(day), 10),
					CrdtType: proto.CRDTType_TOPK_RMV,
					Bucket:   buckets[bucketI],
				}
				//TODO: Actually use float
				if !useTopKAll {
					for orderKey, sum := range dayMap {
						var currUpd crdt.UpdateArguments = crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: orderKey, Score: int32(*sum),
							Data: ti.packQ3IndexExtraDataFromKey(orderKey)}}
						upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
						i++
					}
				} else {
					adds := make([]crdt.TopKScore, len(dayMap))
					j := 0
					for orderKey, sum := range dayMap {
						adds[j] = crdt.TopKScore{Id: orderKey, Score: int32(*sum), Data: ti.packQ3IndexExtraDataFromKey(orderKey)}
						j++
					}
					var currUpd crdt.UpdateArguments = crdt.TopKAddAll{Scores: adds}
					upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
					i++
				}
				//fmt.Printf("[CI]Q3 for segment %s day %d: %d\n", mktSeg, day, len(dayMap))
			}
		}
	} else {
		for mktSeg, segMap := range sumMap {
			for day, dayMap := range segMap {
				//A topK per pair (mktsegment, orderdate)
				keyArgs = antidote.KeyParams{
					Key:      SEGM_DELAY + mktSeg + strconv.FormatInt(int64(day), 10),
					CrdtType: proto.CRDTType_TOPK_RMV,
					Bucket:   buckets[bucketI],
				}
				//TODO: Actually use float
				if !useTopKAll {
					for orderKey, sum := range dayMap {
						var currUpd crdt.UpdateArguments = crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: orderKey, Score: int32(*sum)}}
						upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
						i++
					}
				} else {
					adds := make([]crdt.TopKScore, len(dayMap))
					j := 0
					for orderKey, sum := range dayMap {
						adds[j] = crdt.TopKScore{Id: orderKey, Score: int32(*sum)}
						j++
					}
					var currUpd crdt.UpdateArguments = crdt.TopKAddAll{Scores: adds}
					upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
					i++
				}
			}
		}
	}
	//fmt.Println(nUpds)
	return
}

func (ti TableInfo) makeQ5IndexUpds(sumMap map[int8]map[int16]map[int8]*float64, bucketI int) (upds []antidote.UpdateObjectParams) {
	//TODO: Actually have a CRDT to store a float instead of an int
	//Prepare the updates. *5 as there's 5 years and we'll do a embMapCRDT for each (region, year)
	upds = make([]antidote.UpdateObjectParams, len(sumMap)*5)
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
			var args crdt.UpdateArguments = regDateUpd
			upds[i] = antidote.UpdateObjectParams{
				KeyParams:  antidote.KeyParams{Key: regS + years[year-1993], CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]},
				UpdateArgs: &args,
			}
			i++
		}
	}
	//fmt.Println(len(sumMap) * 5)
	return
}

func (ti TableInfo) makeQ11IndexUpds(nationMap map[int8]map[int32]*float64, totalSumMap map[int8]*float64, nUpds, bucketI int) (upds []antidote.UpdateObjectParams) {
	//Preparing updates for the topK CRDTs
	if useTopKAll {
		nUpds = len(nationMap) + len(totalSumMap)
	}
	upds = make([]antidote.UpdateObjectParams, nUpds)
	//var currUpd crdt.UpdateArguments
	var keyArgs antidote.KeyParams
	i := 0

	for natKey, partMap := range nationMap {
		keyArgs = antidote.KeyParams{
			Key:      IMP_SUPPLY + ti.Tables.Nations[natKey].N_NAME,
			CrdtType: proto.CRDTType_TOPK_RMV,
			Bucket:   buckets[bucketI],
		}
		if !useTopKAll {
			for partKey, value := range partMap {
				//TODO: Not use int32
				var currUpd crdt.UpdateArguments = crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: partKey, Score: int32(*value)}}
				upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
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
			upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
			i++
		}
		fmt.Printf("[CI]Q11 map size for nation %d: %d\n", natKey, len(partMap))
	}

	//Preparing updates for the counter CRDTs
	for natKey, value := range totalSumMap {
		keyArgs = antidote.KeyParams{
			Key:      SUM_SUPPLY + ti.Tables.Nations[natKey].N_NAME,
			CrdtType: proto.CRDTType_COUNTER,
			Bucket:   buckets[bucketI],
		}
		//TODO: Not use int32
		var currUpd crdt.UpdateArguments = crdt.Increment{Change: int32(*value * (0.0001 / scaleFactor))}
		upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
		i++
	}

	//fmt.Println(nUpds)
	return
}

func (ti TableInfo) makeQ14IndexUpds(mapPromo map[string]*float64, mapTotal map[string]*float64, bucketI int) (upds []antidote.UpdateObjectParams) {
	//Create the updates
	upds = make([]antidote.UpdateObjectParams, len(mapPromo), len(mapPromo))
	i := 0
	for key, totalP := range mapTotal {
		promo := *mapPromo[key]
		var currUpd crdt.UpdateArguments = crdt.AddMultipleValue{
			SumValue: int64(100.0 * promo),
			NAdds:    int64(*totalP),
		}
		//fmt.Printf("[ClientIndex]Making Q14 update for key %s, with values %d %d (%f)\n", PROMO_PERCENTAGE+key, int64(100.0*promo), int64(*totalP), (100.0*promo) / *totalP)
		upds[i] = antidote.UpdateObjectParams{
			KeyParams:  antidote.KeyParams{Key: PROMO_PERCENTAGE + key, CrdtType: proto.CRDTType_AVG, Bucket: buckets[bucketI]},
			UpdateArgs: &currUpd,
		}
		i++
	}
	//fmt.Println(len(mapPromo))
	return
}

func (ti TableInfo) makeQ15IndexUpdsTopSum(yearMap map[int16]map[int8]map[int32]*float64, nUpds int, bucketI int) (upds []antidote.UpdateObjectParams) {
	//It's always 20 updates if using TopKAddAll (5 years * 4 months)
	if useTopKAll {
		nUpds = 20
	}
	upds = make([]antidote.UpdateObjectParams, nUpds)
	var keyArgs antidote.KeyParams

	i, month := 0, int8(0)
	for year, monthMap := range yearMap {
		for month = 1; month <= 12; month += 3 {
			keyArgs = antidote.KeyParams{
				Key:      TOP_SUPPLIERS + strconv.FormatInt(int64(year), 10) + strconv.FormatInt(int64(month), 10),
				CrdtType: proto.CRDTType_TOPSUM,
				Bucket:   buckets[bucketI],
			}
			if !useTopKAll {
				for suppKey, value := range monthMap[month] {
					//TODO: Not use int32 for value
					var currUpd crdt.UpdateArguments = crdt.TopSAdd{TopKScore: crdt.TopKScore{
						Id:    suppKey,
						Score: int32(*value),
					}}
					upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
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
				var currUpd crdt.UpdateArguments = crdt.TopSAddAll{Scores: adds}
				upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
				i++
			}
			fmt.Printf("[CI]Q15 size for year %d, month %d: %d\n", year, month, len(monthMap[month]))
		}
	}

	return
}

func (ti TableInfo) makeQ15IndexUpds(yearMap map[int16]map[int8]map[int32]*float64, nUpds int, bucketI int) (upds []antidote.UpdateObjectParams) {
	//Create the updates. Always 20 updates if doing with TopKAddAll (5 years * 4 months)
	if useTopKAll {
		nUpds = 20
	}
	upds = make([]antidote.UpdateObjectParams, nUpds)
	var keyArgs antidote.KeyParams

	i, month := 0, int8(0)
	for year, monthMap := range yearMap {
		for month = 1; month <= 12; month += 3 {
			keyArgs = antidote.KeyParams{
				Key:      TOP_SUPPLIERS + strconv.FormatInt(int64(year), 10) + strconv.FormatInt(int64(month), 10),
				CrdtType: proto.CRDTType_TOPK_RMV,
				Bucket:   buckets[bucketI],
			}
			if !useTopKAll {
				for suppKey, value := range monthMap[month] {
					//TODO: Not use int32 for value
					var currUpd crdt.UpdateArguments = crdt.TopKAdd{TopKScore: crdt.TopKScore{
						Id:    suppKey,
						Score: int32(*value),
					}}
					upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
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
				var currUpd crdt.UpdateArguments = crdt.TopKAddAll{Scores: adds}
				upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
				i++
			}
			fmt.Printf("[CI]Q15 size for year %d, month %d: %d\n", year, month, len(monthMap[month]))
		}
	}
	//fmt.Println(nUpds)
	return
}

//TODO: The Score of the CRDT should be totalprice instead of the sum of amounts
func (ti TableInfo) makeQ18IndexUpds(quantityMap map[int32]map[int32]*PairInt, bucketI int) (upds []antidote.UpdateObjectParams) {
	nUpds := 0
	if useTopKAll {
		nUpds = min(len(quantityMap[312]), 1) + min(len(quantityMap[313]), 1) + min(len(quantityMap[314]), 1) + min(len(quantityMap[315]), 1)
	} else {
		nUpds = len(quantityMap[312]) + len(quantityMap[313]) + len(quantityMap[314]) + len(quantityMap[315])
	}
	//Create the updates
	upds = make([]antidote.UpdateObjectParams, nUpds)
	var keyArgs antidote.KeyParams

	i := 0
	if !INDEX_WITH_FULL_DATA {
		for quantity, orderMap := range quantityMap {
			keyArgs = antidote.KeyParams{
				Key:      LARGE_ORDERS + strconv.FormatInt(int64(quantity), 10),
				CrdtType: proto.CRDTType_TOPK_RMV,
				Bucket:   buckets[bucketI],
			}
			if !useTopKAll {
				for orderKey, pair := range orderMap {
					//TODO: Store the customerKey also
					var currUpd crdt.UpdateArguments = crdt.TopKAdd{TopKScore: crdt.TopKScore{
						Id:    orderKey,
						Score: pair.second,
					}}
					upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
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
					upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
					i++
				}
			}
		}
	} else {
		for quantity, orderMap := range quantityMap {
			keyArgs = antidote.KeyParams{
				Key:      LARGE_ORDERS + strconv.FormatInt(int64(quantity), 10),
				CrdtType: proto.CRDTType_TOPK_RMV,
				Bucket:   buckets[bucketI],
			}
			if !useTopKAll {
				for orderKey, pair := range orderMap {
					//TODO: Store the customerKey also
					var currUpd crdt.UpdateArguments = crdt.TopKAdd{TopKScore: crdt.TopKScore{
						Id:    orderKey,
						Score: pair.second,
						Data:  ti.packQ18IndexExtraDataFromKey(orderKey),
					}}
					upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
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
					var currUpd crdt.UpdateArguments = crdt.TopKAddAll{Scores: adds}
					upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
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
