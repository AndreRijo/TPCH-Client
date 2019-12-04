package tpch

import (
	"antidote"
	"crdt"
	"fmt"
	"proto"
	"strconv"
	"strings"
	"time"
)

type PairInt struct {
	first  int32
	second int32
}

var (
	//"Constants"
	MIN_DATE_Q3, MAX_DATE_Q3          = &Date{YEAR: 1995, MONTH: 03, DAY: 01}, &Date{YEAR: 1995, MONTH: 03, DAY: 31}
	MIN_MONTH_DAY, MAX_MONTH_DAY      = int8(1), int8(31)
	Q3_N_EXTRA_DATA, Q18_N_EXTRA_DATA = 2, 4

	//Need to store this in order to avoid on updates having to recalculate everything
	q15Map      map[int16]map[int8]map[int32]*float64
	q15LocalMap []map[int16]map[int8]map[int32]*float64
)

func prepareIndexesToSend() {
	startTime := time.Now().UnixNano() / 1000000
	//TODO: Later consider 6, 10, 2, by this order.
	fmt.Println("Preparing indexes...")
	//queueIndex(prepareQ2Index())
	queueIndex(prepareQ3Index())
	times.prepareIndexProtos[0] = time.Now().UnixNano() / 1000000
	fmt.Println("Index Q3 OK")
	q5Upds, _ := prepareQ5Index()
	queueIndex(q5Upds)
	times.prepareIndexProtos[1] = time.Now().UnixNano() / 1000000
	fmt.Println("Index Q5 OK")
	queueIndex(prepareQ11Index())
	times.prepareIndexProtos[2] = time.Now().UnixNano() / 1000000
	fmt.Println("Index Q11 OK")
	queueIndex(prepareQ14Index())
	times.prepareIndexProtos[3] = time.Now().UnixNano() / 1000000
	fmt.Println("Index Q14 OK")
	queueIndex(prepareQ15Index())
	times.prepareIndexProtos[4] = time.Now().UnixNano() / 1000000
	fmt.Println("Index Q15 OK")
	queueIndex(prepareQ18Index())
	endTime := time.Now().UnixNano() / 1000000
	times.prepareIndexProtos[5] = endTime
	fmt.Println("Index Q18 OK")

	for i := len(times.prepareIndexProtos) - 1; i > 0; i-- {
		times.prepareIndexProtos[i] -= times.prepareIndexProtos[i-1]
	}
	times.prepareIndexProtos[0] -= startTime
	times.totalIndex = endTime - startTime

	//Signal the end
	channels.indexChans[0] <- QueuedMsg{code: QUEUE_COMPLETE}
}

func queueIndex(upds []antidote.UpdateObjectParams) {
	channels.indexChans[0] <- QueuedMsg{Message: antidote.CreateUpdateObjs(nil, upds), code: antidote.UpdateObjs}
}

func prepareIndexesLocalToSend() {
	startTime := time.Now().UnixNano() / 1000000
	//TODO: Later consider 6, 10, 2, by this order.
	fmt.Println("Preparing indexes...")
	//queueIndex(prepareQ2Index())
	queueIndexLocal(prepareQ3IndexLocal())
	times.prepareIndexProtos[0] = time.Now().UnixNano() / 1000000
	fmt.Println("Index Q3 OK")
	_, q5Upds := prepareQ5Index()
	queueIndexLocal(q5Upds)
	times.prepareIndexProtos[1] = time.Now().UnixNano() / 1000000
	fmt.Println("Index Q5 OK")
	queueIndexLocal(prepareQ11IndexLocal())
	times.prepareIndexProtos[2] = time.Now().UnixNano() / 1000000
	fmt.Println("Index Q11 OK")
	queueIndexLocal(prepareQ14IndexLocal())
	times.prepareIndexProtos[3] = time.Now().UnixNano() / 1000000
	fmt.Println("Index Q14 OK")
	queueIndexLocal(prepareQ15IndexLocal())
	times.prepareIndexProtos[4] = time.Now().UnixNano() / 1000000
	fmt.Println("Index Q15 OK")
	queueIndexLocal(prepareQ18IndexLocal())
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

func prepareQ3IndexLocal() (upds [][]antidote.UpdateObjectParams) {
	sumMap := make([]map[string]map[int8]map[int32]*float64, len(procTables.Regions))
	nUpds := make([]int, len(sumMap))

	var j int8
	for i := range sumMap {
		serverMap := make(map[string]map[int8]map[int32]*float64)
		for _, seg := range procTables.Segments {
			segMap := make(map[int8]map[int32]*float64)
			for j = 1; j <= 31; j++ {
				segMap[j] = make(map[int32]*float64)
			}
			serverMap[seg] = segMap
		}
		sumMap[i] = serverMap
	}

	regionI := int8(0)
	for orderI, order := range procTables.Orders[1:] {
		regionI = procTables.OrderkeyToRegionkey(order.O_ORDERKEY)
		nUpds[regionI] += q3CalcHelper(sumMap[regionI], order, orderI)
	}

	upds = make([][]antidote.UpdateObjectParams, len(nUpds))
	for i := range upds {
		upds[i] = makeQ3IndexUpds(sumMap[i], nUpds[i], INDEX_BKT+i)
	}

	return
}

func prepareQ3Index() (upds []antidote.UpdateObjectParams) {
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
	sumMap := make(map[string]map[int8]map[int32]*float64)

	var j int8
	//Preparing maps
	for _, seg := range procTables.Segments {
		segMap := make(map[int8]map[int32]*float64)
		for j = 1; j <= 31; j++ {
			segMap[j] = make(map[int32]*float64)
		}
		sumMap[seg] = segMap
	}

	nUpds := 0
	for orderI, order := range procTables.Orders[1:] {
		nUpds += q3CalcHelper(sumMap, order, orderI)
	}

	return makeQ3IndexUpds(sumMap, nUpds, INDEX_BKT)
}

//Note: not much efficient, but we don't really know how old can an orderdate be without being shipped. And we also don't have any index for dates
func q3CalcHelper(sumMap map[string]map[int8]map[int32]*float64, order *Orders, orderI int) (nUpds int) {
	var minDay = MIN_MONTH_DAY
	var currSum *float64
	var has bool
	var j int8
	//To be in range for the maps, o_orderDate must be <= than the highest date 1995-03-31
	//And l_shipdate must be >= than the smallest date 1995-03-01
	if order.O_ORDERDATE.isSmallerOrEqual(MAX_DATE_Q3) {
		//fmt.Println("OrderDate:", *order.O_ORDERDATE, ". Compare result:", order.O_ORDERDATE.isSmallerOrEqual(maxDate))
		//Get the customer's market segment
		segMap := sumMap[procTables.Customers[order.O_CUSTKEY].C_MKTSEGMENT]
		orderLineItems := procTables.LineItems[orderI]
		for _, item := range orderLineItems {
			//Check if L_SHIPDATE is higher than minDate and, if it is, check month/year. If month/year > march 1995, then add to all entries. Otherwise, use day to know which entries.
			if item.L_SHIPDATE.isHigherOrEqual(MIN_DATE_Q3) {
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
	}
	return
}

func prepareQ5Index() (upds []antidote.UpdateObjectParams, multiUpds [][]antidote.UpdateObjectParams) {
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
	for _, region := range procTables.Regions {
		regMap := make(map[int16]map[int8]*float64)
		for i = 1993; i <= 1997; i++ {
			regMap[i] = make(map[int8]*float64)
		}
		sumMap[region.R_REGIONKEY] = regMap
	}
	//Registering countries
	for _, nation := range procTables.Nations {
		regMap := sumMap[nation.N_REGIONKEY]
		for i = 1993; i <= 1997; i++ {
			value := 0.0
			regMap[i][nation.N_NATIONKEY] = &value
		}
	}

	//Actually collecting the data
	orders := procTables.Orders[1:]
	var order *Orders
	var customer *Customer
	var supplier *Supplier
	var year int16
	var nationKey, regionKey int8
	var value *float64

	for i, orderItems := range procTables.LineItems[1:] {
		order = orders[i]
		year = order.O_ORDERDATE.YEAR
		if year >= 1993 && year <= 1997 {
			customer = procTables.Customers[order.O_CUSTKEY]
			nationKey = customer.C_NATIONKEY
			regionKey = procTables.Nations[nationKey].N_REGIONKEY
			value = sumMap[regionKey][year][nationKey]
			for _, lineItem := range orderItems {
				//Conditions:
				//Ordered year between 1993 and 1997 (inclusive)
				//Supplier and customer of same nation
				//Calculate: l_extendedprice * (1 - l_discount)
				supplier = procTables.Suppliers[lineItem.L_SUPPKEY]
				if nationKey == supplier.S_NATIONKEY {
					*value += lineItem.L_EXTENDEDPRICE * (1 - lineItem.L_DISCOUNT)
				}
			}
		}
	}

	if isIndexGlobal {
		return makeQ5IndexUpds(sumMap, INDEX_BKT), nil
	}
	//Create temporary maps with just one region, in order to receive the upds separatelly
	multiUpds = make([][]antidote.UpdateObjectParams, len(procTables.Regions))
	for i, regMap := range sumMap {
		multiUpds[i] = makeQ5IndexUpds(map[int8]map[int16]map[int8]*float64{i: regMap}, INDEX_BKT+int(i))
	}
	return nil, multiUpds
}

func prepareQ11IndexLocal() (upds [][]antidote.UpdateObjectParams) {
	nationMap := make([]map[int8]map[int32]*float64, len(procTables.Regions))
	totalSumMap := make([]map[int8]*float64, len(procTables.Regions))
	updsPerRegion := make([]int, len(procTables.Regions))

	for i := range nationMap {
		nationMap[i] = make(map[int8]map[int32]*float64)
		totalSumMap[i] = make(map[int8]*float64)
	}
	for _, nation := range procTables.Nations {
		nationMap[nation.N_REGIONKEY][nation.N_NATIONKEY] = make(map[int32]*float64)
		totalSumMap[nation.N_REGIONKEY][nation.N_NATIONKEY] = new(float64)
		updsPerRegion[nation.N_REGIONKEY]++ //Each nation has at least 1 upd (totalSum)
	}

	var supplier *Supplier
	var regionK int8
	for _, partSup := range procTables.PartSupps {
		supplier = procTables.Suppliers[partSup.PS_SUPPKEY]
		regionK = procTables.Nations[supplier.S_NATIONKEY].N_REGIONKEY
		updsPerRegion[regionK] += q11CalcHelper(nationMap[regionK], totalSumMap[regionK], partSup, supplier)
	}

	upds = make([][]antidote.UpdateObjectParams, len(procTables.Regions))
	for i := range upds {
		upds[i] = makeQ11IndexUpds(nationMap[i], totalSumMap[i], updsPerRegion[i], INDEX_BKT+i)
	}

	return
}

func prepareQ11Index() (upds []antidote.UpdateObjectParams) {
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
	for _, nation := range procTables.Nations {
		nationMap[nation.N_NATIONKEY] = make(map[int32]*float64)
		totalSumMap[nation.N_NATIONKEY] = new(float64)
	}

	var supplier *Supplier
	nUpds := len(procTables.Nations) //Assuming all nations have at least one supplier. Initial value corresponds to the number of nations
	//Calculate totals
	for _, partSup := range procTables.PartSupps {
		supplier = procTables.Suppliers[partSup.PS_SUPPKEY]
		nUpds += q11CalcHelper(nationMap, totalSumMap, partSup, supplier)
	}

	return makeQ11IndexUpds(nationMap, totalSumMap, nUpds, INDEX_BKT)
}

func q11CalcHelper(nationMap map[int8]map[int32]*float64, totalSumMap map[int8]*float64, partSup *PartSupp, supplier *Supplier) (nUpds int) {
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

func prepareQ14IndexLocal() (upds [][]antidote.UpdateObjectParams) {
	mapPromo, mapTotal := make([]map[string]*float64, len(procTables.Regions)), make([]map[string]*float64, len(procTables.Regions))
	inPromo := procTables.PromoParts

	var currMapP, currMapT map[string]*float64
	iString, fullKey := "", ""
	var j, k int64
	for i := range mapPromo {
		currMapP, currMapT = make(map[string]*float64), make(map[string]*float64)
		for j = 1993; j <= 1997; j++ {
			iString = strconv.FormatInt(j, 10)
			for k = 1; k <= 12; k++ {
				fullKey = iString + strconv.FormatInt(k, 10)
				currMapP[fullKey], currMapT[fullKey] = new(float64), new(float64)
			}
		}
		mapPromo[i], mapTotal[i] = currMapP, currMapT
	}

	regionKey := int8(0)
	for i, orderItems := range procTables.LineItems[1:] {
		regionKey = procTables.OrderkeyToRegionkey(procTables.Orders[i+1].O_ORDERKEY)
		q14CalcHelper(orderItems, mapPromo[regionKey], mapTotal[regionKey], inPromo)
	}

	upds = make([][]antidote.UpdateObjectParams, len(procTables.Regions))
	for i := range upds {
		upds[i] = makeQ14IndexUpds(mapPromo[i], mapTotal[i], INDEX_BKT+i)
	}
	return
}

func prepareQ14Index() (upds []antidote.UpdateObjectParams) {
	//Avg CRDT
	//sum + count (or mix together if possible): l_extendedprice * (1 - l_discount), when p_type starts with "PROMO"
	//Group by month (l_shipdate). Asked date always starts at the 1st day of a given month, between 1993 and 1997.
	//The date interval always covers the whole month.

	//Plan: Use a AddMultipleValue.
	//Keep a map of string -> struct{}{} for the partKeys that we know are of type PROMO%
	//Likelly we should even go through all the parts first to achieve that
	//Then go through lineItem, check with the map, and update the correct sums

	mapPromo, mapTotal := make(map[string]*float64), make(map[string]*float64)
	inPromo := procTables.PromoParts

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

	//Going through lineitem and updating the totals
	for _, orderItems := range procTables.LineItems[1:] {
		q14CalcHelper(orderItems, mapPromo, mapTotal, inPromo)
	}

	return makeQ14IndexUpds(mapPromo, mapTotal, INDEX_BKT)
}

func q14CalcHelper(orderItems []*LineItem, mapPromo map[string]*float64, mapTotal map[string]*float64, inPromo map[int32]struct{}) {
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

func prepareQ15IndexLocal() (upds [][]antidote.UpdateObjectParams) {
	yearMap := make([]map[int16]map[int8]map[int32]*float64, len(procTables.Regions))
	nUpds := make([]int, len(procTables.Regions))
	q15LocalMap = yearMap

	var year int16 = 1993
	var regionMap map[int16]map[int8]map[int32]*float64
	var mMap map[int8]map[int32]*float64
	for i := range yearMap {
		regionMap = make(map[int16]map[int8]map[int32]*float64)
		for year = 1993; year <= 1997; year++ {
			mMap = make(map[int8]map[int32]*float64)
			mMap[1], mMap[4], mMap[7], mMap[10] = make(map[int32]*float64),
				make(map[int32]*float64), make(map[int32]*float64), make(map[int32]*float64)
			regionMap[year] = mMap
		}
		yearMap[i] = regionMap
	}

	rKey := int8(0)
	for i, orderItems := range procTables.LineItems[1:] {
		rKey = procTables.OrderkeyToRegionkey(procTables.Orders[i+1].O_ORDERKEY)
		nUpds[rKey] += q15CalcHelper(orderItems, yearMap[rKey])
	}

	upds = make([][]antidote.UpdateObjectParams, len(procTables.Regions))
	for i := range upds {
		upds[i] = makeQ15IndexUpds(yearMap[i], nUpds[i], INDEX_BKT+i)
	}

	return
}

//TODO: Can I really assume that quarters are Jan-Mar, Apr-Jun, Jul-Sep, Oct-Dec?
func prepareQ15Index() (upds []antidote.UpdateObjectParams) {
	//2.4.15: topk: sum(l_extendedprice * (1-l_discount))
	//Group by month. The sum corresponds to the revenue shipped by a supplier during a given quarter of the year.
	//Date can start between first month of 1993 and 10th month of 1997 (first day always)
	//Have one topK per quarter
	//year -> month -> supplierID -> sum
	yearMap := make(map[int16]map[int8]map[int32]*float64)
	q15Map = yearMap
	var mMap map[int8]map[int32]*float64

	//Preparing map instances for each quarter between 1993 and 1997
	var year int16 = 1993
	for ; year <= 1997; year++ {
		mMap = make(map[int8]map[int32]*float64)
		mMap[1], mMap[4], mMap[7], mMap[10] = make(map[int32]*float64),
			make(map[int32]*float64), make(map[int32]*float64), make(map[int32]*float64)
		yearMap[year] = mMap
	}

	nUpds := 0 //Assuming each quarter has at least one supplier

	for _, orderItems := range procTables.LineItems[1:] {
		nUpds += q15CalcHelper(orderItems, yearMap)
	}

	return makeQ15IndexUpds(yearMap, nUpds, INDEX_BKT)
}

func q15CalcHelper(orderItems []*LineItem, yearMap map[int16]map[int8]map[int32]*float64) (nUpds int) {
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

func prepareQ18IndexLocal() (upds [][]antidote.UpdateObjectParams) {
	quantityMap := make([]map[int32]map[int32]*PairInt, len(procTables.Regions))
	for i := range quantityMap {
		quantityMap[i] = map[int32]map[int32]*PairInt{312: make(map[int32]*PairInt), 313: make(map[int32]*PairInt),
			314: make(map[int32]*PairInt), 315: make(map[int32]*PairInt)}
	}

	regionKey := int8(0)
	for i, order := range procTables.Orders[1:] {
		regionKey = procTables.OrderkeyToRegionkey(order.O_ORDERKEY)
		q18CalcHelper(quantityMap[regionKey], order, i)
	}

	upds = make([][]antidote.UpdateObjectParams, len(procTables.Regions))
	for i := range upds {
		upds[i] = makeQ18IndexUpds(quantityMap[i], INDEX_BKT+i)
	}
	return
}

//TODO: This likelly could be reimplemented with only 1 topK and using the query of "above value".
//Albeit that potencially would imply downloading more than 100 customers.
func prepareQ18Index() (upds []antidote.UpdateObjectParams) {
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

	//Going through orders and, for each order, checking all its lineitems
	//Doing the other way around is possible but would require a map of order -> quantity.
	//Let's do both and then choose one
	for i, order := range procTables.Orders[1:] {
		q18CalcHelper(quantityMap, order, i)
	}

	/*
		//lineitem -> Order
		lastLineitemId := int8(0)
		var item *LineItem
		orderMap := make(map[int32]*PairInt)
		orderKey := int32(-1)
		var currPair *PairInt
		var holderPair PairInt	//Used to host a new pair that is being created
		var has bool
		//First, acumulate total quantities per order
		for i := 1; i < len(procTables.LineItems); i++ {
			item = procTables[i]
			if item == nil {
				//Skip remaining positions for the order, which will also be nil
				i += procTables.MaxOrderLineitems - lastLineItemId
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
				pair.first = procTables[GetOrderIndex(orderKey)].O_CUSTKEY
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

	return makeQ18IndexUpds(quantityMap, INDEX_BKT)
}

func q18CalcHelper(quantityMap map[int32]map[int32]*PairInt, order *Orders, index int) {
	currQuantity := int32(0)
	orderItems := procTables.LineItems[index]
	for _, item := range orderItems {
		currQuantity += int32(item.L_QUANTITY)
	}
	//fmt.Printf("Order ID %d has total quantity %d.\n", orderKey, currQuantity)
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

func makeQ3IndexUpds(sumMap map[string]map[int8]map[int32]*float64, nUpds int, bucketI int) (upds []antidote.UpdateObjectParams) {
	//TODO: TopKAddAll for optimization purposes?
	upds = make([]antidote.UpdateObjectParams, nUpds)
	var keyArgs antidote.KeyParams
	i := 0
	if !INDEX_WITH_FULL_DATA {
		for mktSeg, segMap := range sumMap {
			for day, dayMap := range segMap {
				//A topK per pair (mktsegment, orderdate)
				keyArgs = antidote.KeyParams{
					Key:      SEGM_DELAY + mktSeg + strconv.FormatInt(int64(day), 10),
					CrdtType: proto.CRDTType_TOPK_RMV,
					Bucket:   buckets[bucketI],
				}
				//TODO: Actually use float
				for orderKey, sum := range dayMap {
					var currUpd crdt.UpdateArguments = crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: orderKey, Score: int32(*sum),
						Data: packQ3IndexExtraData(orderKey)}}
					upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
					i++
				}
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
				for orderKey, sum := range dayMap {
					var currUpd crdt.UpdateArguments = crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: orderKey, Score: int32(*sum)}}
					upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
					i++
				}
			}
		}
	}

	fmt.Println(nUpds)
	return
}

func makeQ5IndexUpds(sumMap map[int8]map[int16]map[int8]*float64, bucketI int) (upds []antidote.UpdateObjectParams) {
	//TODO: Actually have a CRDT to store a float instead of an int
	//Prepare the updates. *5 as there's 5 years and we'll do a embMapCRDT for each (region, year)
	upds = make([]antidote.UpdateObjectParams, len(sumMap)*5)
	i, year := 0, int16(0)
	years := []string{"1993", "1994", "1995", "1996", "1997"}
	regS := ""
	for regK, regionMap := range sumMap {
		regS = NATION_REVENUE + procTables.Regions[regK].R_NAME
		for year = 1993; year <= 1997; year++ {
			yearMap := regionMap[year]
			regDateUpd := crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
			for natK, value := range yearMap {
				regDateUpd.Upds[procTables.Nations[natK].N_NAME] = crdt.Increment{Change: int32(*value)}
			}
			var args crdt.UpdateArguments = regDateUpd
			upds[i] = antidote.UpdateObjectParams{
				KeyParams:  antidote.KeyParams{Key: regS + years[year-1993], CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]},
				UpdateArgs: &args,
			}
			i++
		}
	}
	fmt.Println(len(sumMap) * 5)
	return
}

func makeQ11IndexUpds(nationMap map[int8]map[int32]*float64, totalSumMap map[int8]*float64, nUpds, bucketI int) (upds []antidote.UpdateObjectParams) {
	//Preparing updates for the topK CRDTs
	//TODO: TopKAddAll for optimization purposes
	upds = make([]antidote.UpdateObjectParams, nUpds)
	//var currUpd crdt.UpdateArguments
	var keyArgs antidote.KeyParams
	i := 0

	for natKey, partMap := range nationMap {
		keyArgs = antidote.KeyParams{
			Key:      IMP_SUPPLY + procTables.Nations[natKey].N_NAME,
			CrdtType: proto.CRDTType_TOPK_RMV,
			Bucket:   buckets[bucketI],
		}
		for partKey, value := range partMap {
			//TODO: Not use int32
			var currUpd crdt.UpdateArguments = crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: partKey, Score: int32(*value)}}
			upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
			i++
		}
	}

	//Preparing updates for the counter CRDTs
	for natKey, value := range totalSumMap {
		keyArgs = antidote.KeyParams{
			Key:      SUM_SUPPLY + procTables.Nations[natKey].N_NAME,
			CrdtType: proto.CRDTType_COUNTER,
			Bucket:   buckets[bucketI],
		}
		//TODO: Not use int32
		var currUpd crdt.UpdateArguments = crdt.Increment{Change: int32(*value * (0.0001 / scaleFactor))}
		upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
		i++
	}

	fmt.Println(nUpds)
	return
}

func makeQ14IndexUpds(mapPromo map[string]*float64, mapTotal map[string]*float64, bucketI int) (upds []antidote.UpdateObjectParams) {
	//Create the updates
	upds = make([]antidote.UpdateObjectParams, len(mapPromo), len(mapPromo))
	i := 0
	for key, totalP := range mapTotal {
		promo := *mapPromo[key]
		var currUpd crdt.UpdateArguments = crdt.AddMultipleValue{
			SumValue: int64(100.0 * promo),
			NAdds:    int64(*totalP),
		}
		upds[i] = antidote.UpdateObjectParams{
			KeyParams:  antidote.KeyParams{Key: PROMO_PERCENTAGE + key, CrdtType: proto.CRDTType_AVG, Bucket: buckets[bucketI]},
			UpdateArgs: &currUpd,
		}
		i++
	}
	fmt.Println(len(mapPromo))
	return
}

func makeQ15IndexUpds(yearMap map[int16]map[int8]map[int32]*float64, nUpds int, bucketI int) (upds []antidote.UpdateObjectParams) {
	//Create the updates
	upds = make([]antidote.UpdateObjectParams, nUpds)
	var keyArgs antidote.KeyParams

	//TODO: TopK with multipleAdd
	i, month := 0, int8(0)
	for year, monthMap := range yearMap {
		for month = 1; month <= 12; month += 3 {
			keyArgs = antidote.KeyParams{
				Key:      TOP_SUPPLIERS + strconv.FormatInt(int64(year), 10) + strconv.FormatInt(int64(month), 10),
				CrdtType: proto.CRDTType_TOPK_RMV,
				Bucket:   buckets[bucketI],
			}
			for suppKey, value := range monthMap[month] {
				//TODO: Not use int32 for value
				var currUpd crdt.UpdateArguments = crdt.TopKAdd{TopKScore: crdt.TopKScore{
					Id:    suppKey,
					Score: int32(*value),
				}}
				upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
				i++
			}
		}
	}
	fmt.Println(nUpds)
	return
}

func makeQ18IndexUpds(quantityMap map[int32]map[int32]*PairInt, bucketI int) (upds []antidote.UpdateObjectParams) {
	//Create the updates
	upds = make([]antidote.UpdateObjectParams, len(quantityMap[312])+len(quantityMap[313])+
		len(quantityMap[314])+len(quantityMap[315]))
	var keyArgs antidote.KeyParams

	//TODO: TopK with multipleAdd
	i := 0
	if !INDEX_WITH_FULL_DATA {
		for quantity, orderMap := range quantityMap {
			keyArgs = antidote.KeyParams{
				Key:      LARGE_ORDERS + strconv.FormatInt(int64(quantity), 10),
				CrdtType: proto.CRDTType_TOPK_RMV,
				Bucket:   buckets[bucketI],
			}
			for orderKey, pair := range orderMap {
				//TODO: Store the customerKey also
				var currUpd crdt.UpdateArguments = crdt.TopKAdd{TopKScore: crdt.TopKScore{
					Id:    orderKey,
					Score: pair.second,
				}}
				upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
				i++
			}
		}
	} else {
		for quantity, orderMap := range quantityMap {
			keyArgs = antidote.KeyParams{
				Key:      LARGE_ORDERS + strconv.FormatInt(int64(quantity), 10),
				CrdtType: proto.CRDTType_TOPK_RMV,
				Bucket:   buckets[bucketI],
			}
			for orderKey, pair := range orderMap {
				//TODO: Store the customerKey also
				var currUpd crdt.UpdateArguments = crdt.TopKAdd{TopKScore: crdt.TopKScore{
					Id:    orderKey,
					Score: pair.second,
					Data:  packQ18IndexExtraData(orderKey),
				}}
				upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
				i++
			}
		}
	}
	fmt.Println(len(quantityMap[312]) + len(quantityMap[313]) + len(quantityMap[314]) + len(quantityMap[315]))

	return
}

func packQ3IndexExtraData(orderKey int32) (data *[]byte) {
	order := procTables.Orders[GetOrderIndex(orderKey)]
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

func packQ18IndexExtraData(orderKey int32) (data *[]byte) {
	order := procTables.Orders[GetOrderIndex(orderKey)]
	customer := procTables.Customers[order.O_CUSTKEY]
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
