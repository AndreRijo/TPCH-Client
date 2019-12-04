package tpch

import (
	"antidote"
	"crdt"
	"fmt"
	"proto"
	"strconv"
	"strings"
)

func readUpds() (ordersUpds [][]string, lineItemUpds [][]string, deleteKeys []string) {
	updPartsRead := [][]int8{read[ORDERS], read[LINEITEM]}
	return ReadUpdates(updCompleteFilename[:], updEntries[:], updParts[:], updPartsRead)
}

func sendDataChangesV2(ordersUpds [][]string, lineItemUpds [][]string, deleteKeys []string) {
	//Maybe I can do like this:
	//1st - deletes
	//2nd - updates
	//3rd - indexes
	//Works as long as it is in the same txn or in the same staticUpdateObjs
	//I actually can't do everything in the same transaction as it involves different servers...
	//But I should still guarantee that, for the same server, it's atomic, albeit index results will still be screwed.
	sendDeletes(deleteKeys)
	sendUpdates(ordersUpds, lineItemUpds)
	sendIndexUpdates(deleteKeys, ordersUpds, lineItemUpds)
	fmt.Println("Updates sent")
}

func sendDeletes(deleteKeys []string) {
	orders, items := getDeleteClientTables(deleteKeys)
	orders, items = orders[1:], items[1:] //Hide initial entry which is always empty
	nDeleteItems := getNumberDeleteItems(items)
	ordersPerServer, itemsPerServer, orderIndex, itemsIndex := makePartionedDeleteStructs(deleteKeys, nDeleteItems)

	var order *Orders
	var orderItems []*LineItem
	var custRegion, suppRegion int8
	var itemS string

	for i, orderS := range deleteKeys {
		order, orderItems = orders[i], items[i]
		custRegion = procTables.CustkeyToRegionkey(int64(order.O_CUSTKEY))
		ordersPerServer[custRegion][orderIndex[custRegion]] = orderS
		orderIndex[custRegion]++

		for _, item := range orderItems {
			itemS = orderS + strconv.FormatInt(int64(item.L_PARTKEY), 10) +
				strconv.FormatInt(int64(item.L_SUPPKEY), 10) + strconv.FormatInt(int64(item.L_LINENUMBER), 10)
			itemsPerServer[custRegion][itemsIndex[custRegion]] = itemS
			itemsIndex[custRegion]++
			suppRegion = procTables.SuppkeyToRegionkey(int64(item.L_SUPPKEY))
			if custRegion != suppRegion {
				itemsPerServer[suppRegion][itemsIndex[suppRegion]] = itemS
				itemsIndex[suppRegion]++
			}
		}
	}

	var orderParams, itemParams antidote.UpdateObjectParams
	for i, orderKeys := range ordersPerServer {
		orderParams = getDeleteParams(tableNames[ORDERS], buckets[i], orderKeys, orderIndex[i])
		itemParams = getDeleteParams(tableNames[LINEITEM], buckets[i], itemsPerServer[i], itemsIndex[i])
		channels.dataChans[i] <- QueuedMsg{
			code:    antidote.StaticUpdateObjs,
			Message: antidote.CreateStaticUpdateObjs(nil, []antidote.UpdateObjectParams{orderParams, itemParams}),
		}
	}

}

func getDeleteClientTables(deleteKeys []string) (orders []*Orders, items [][]*LineItem) {
	orders, items = procTables.LastAddedOrders, procTables.LastAddedLineItems
	if orders == nil {
		//First delete run
		orders, items = procTables.Orders[:len(deleteKeys)+1], procTables.LineItems[:len(deleteKeys)+1]
	}
	return
}

func getDeleteParams(tableName string, bkt string, keys []string, nKeys int) antidote.UpdateObjectParams {
	var mapRemove crdt.UpdateArguments = crdt.MapRemoveAll{Keys: keys[:nKeys]}
	return antidote.UpdateObjectParams{
		KeyParams:  antidote.KeyParams{Key: tableName, CrdtType: proto.CRDTType_RRMAP, Bucket: bkt},
		UpdateArgs: &mapRemove,
	}
}

func makePartionedDeleteStructs(deleteKeys []string, nDeleteItems int) (ordersPerServer [][]string, itemsPerServer [][]string, orderIndex []int, itemsIndex []int) {
	//Objects are partitioned, so removing keys is no longer simply passing the slice :(
	//We don't know a priori how many orders are for each server... nor lineitems.
	ordersPerServer, itemsPerServer = make([][]string, len(channels.dataChans)), make([][]string, len(channels.dataChans))
	orderIndex, itemsIndex = make([]int, len(channels.dataChans)), make([]int, len(channels.dataChans))
	for i := range channels.dataChans {
		//ordersPerServer[i] = make([]string, int(float64(len(deleteKeys))*0.25))
		//itemsPerServer[i] = make([]string, int(float64(nDeleteItems)*0.5))
		ordersPerServer[i] = make([]string, int(float64(len(deleteKeys))*0.9))
		itemsPerServer[i] = make([]string, int(float64(nDeleteItems)*0.9))
		orderIndex[i], itemsIndex[i] = 0, 0
	}
	return
}

func getNumberDeleteItems(items [][]*LineItem) (nItems int) {
	for _, orderItems := range items {
		nItems += len(orderItems)
	}
	return
}

func sendUpdates(ordersUpds [][]string, lineItemUpds [][]string) {
	//Different strategy from the one in sendUpdateData. We'll prepare all updates while the initial data is still being sent as updates are around 0.1% of lineitems.
	//Also, at first we'll only do updates, indexes will be done afterwards and considering simultaneously added and removed keys.
	//Worst case if we need to save memory, we can do the index calculus first.
	ordersPerServer, itemsPerServer := make([]map[string]crdt.UpdateArguments, len(procTables.Regions)), make([]map[string]crdt.UpdateArguments, len(procTables.Regions))
	for i := 0; i < len(procTables.Regions); i++ {
		ordersPerServer[i] = make(map[string]crdt.UpdateArguments)
		itemsPerServer[i] = make(map[string]crdt.UpdateArguments)
	}
	var key string
	var upd *crdt.EmbMapUpdateAll
	orderFunc, itemFunc := regionFuncs[ORDERS], multiRegionFunc[LINEITEM]
	var itemRegions []int8

	for _, order := range ordersUpds {
		key, upd = getEntryUpd(headers[ORDERS], keys[ORDERS], order, read[ORDERS])
		ordersPerServer[orderFunc(order)][key] = *upd
	}
	for _, item := range lineItemUpds {
		key, upd = getEntryUpd(headers[LINEITEM], keys[LINEITEM], item, read[LINEITEM])
		itemRegions = itemFunc(item)
		for _, region := range itemRegions {
			itemsPerServer[region][key] = upd
		}
	}
	for i := 0; i < len(procTables.Regions); i++ {
		queueDataProto(ordersPerServer[i], tableNames[ORDERS], buckets[i], channels.dataChans[i])
		queueDataProto(itemsPerServer[i], tableNames[LINEITEM], buckets[i], channels.dataChans[i])
	}

}

func sendIndexUpdates(deleteKeys []string, ordersUpds [][]string, lineItemUpds [][]string) {
	indexUpds := make([][]antidote.UpdateObjectParams, 7)
	remOrders, remItems := getDeleteClientTables(deleteKeys)
	procTables.UpdateOrderLineitems(ordersUpds, lineItemUpds)

	newOrders, newItems := procTables.LastAddedOrders, procTables.LastAddedLineItems
	indexUpds[0], indexUpds[1] = getQ3UpdsV2(remOrders, remItems, newOrders, newItems)
	indexUpds[2] = getQ5UpdsV2(remOrders, remItems, newOrders, newItems)
	//Query 11 doesn't need updates (Nation/Supply only, which are never updated.)
	//q11Upds := getQ11Upds(order, lineItems)
	indexUpds[3] = getQ14UpdsV2(remOrders, remItems, newOrders, newItems)
	indexUpds[4] = getQ15UpdsV2(remOrders, remItems, newOrders, newItems)
	indexUpds[5], indexUpds[6] = getQ18UpdsV2(remOrders, remItems, newOrders, newItems)

	for _, upds := range indexUpds {
		//Might want to send this to a different channel? And group?
		channels.indexChans[0] <- QueuedMsg{code: antidote.StaticUpdateObjs,
			Message: antidote.CreateStaticUpdateObjs(nil, upds),
		}
	}
}

func getQ3UpdsV2(remOrders []*Orders, remItems [][]*LineItem, newOrders []*Orders, newItems [][]*LineItem) (rems []antidote.UpdateObjectParams,
	upds []antidote.UpdateObjectParams) {
	//remItems can be ignored. As for remOrders, we'll need to find their segment and orderdate to know where to remove.
	//newOrders and newItems should be equal to clientIndex.
	var j int8
	//segment -> orderDate -> orderkey
	remMap := make(map[string]map[int8]map[int32]struct{})
	for _, seg := range procTables.Segments {
		segMap := make(map[int8]map[int32]struct{})
		for j = 1; j <= 31; j++ {
			segMap[j] = make(map[int32]struct{})
		}
		remMap[seg] = segMap
	}
	minDate, maxDate := &Date{YEAR: 1995, MONTH: 03, DAY: 01}, &Date{YEAR: 1995, MONTH: 03, DAY: 31}
	var minDay int8 = 0
	const maxDay int8 = 31
	var segMap map[int8]map[int32]struct{}
	var orderLineItems []*LineItem

	for orderI, order := range remOrders[1:] {
		if order.O_ORDERDATE.isSmallerOrEqual(maxDate) {
			segMap = remMap[procTables.Customers[order.O_CUSTKEY].C_MKTSEGMENT]
			orderLineItems = procTables.LineItems[orderI]
			for _, item := range orderLineItems {
				//Check if L_SHIPDATE is higher than minDate and, if it is, check month/year. If month/year > march 1995, then add to all entries. Otherwise, use day to know which entries.
				if item.L_SHIPDATE.isHigherOrEqual(minDate) {
					if item.L_SHIPDATE.MONTH > 3 || item.L_SHIPDATE.YEAR > 1995 {
						//All days
						minDay = 1
					} else {
						minDay = item.L_SHIPDATE.DAY + 1
					}
					//Make a for from minDay to 31 to fill the map
					for j = minDay; j <= maxDay; j++ {
						segMap[j][order.O_ORDERKEY] = struct{}{}
					}
				}
			}
		}
	}
	nUpds := 0
	for _, segMap := range remMap {
		for _, dayMap := range segMap {
			nUpds += len(dayMap)
		}
	}

	rems = makeQ3IndexRemoves(remMap, nUpds)
	existingOrders, existingItems := procTables.Orders, procTables.LineItems
	procTables.Orders, procTables.LineItems = newOrders, newItems
	upds = prepareQ3Index()
	procTables.Orders, procTables.LineItems = existingOrders, existingItems
	return
}

func getQ5UpdsV2(remOrders []*Orders, remItems [][]*LineItem, newOrders []*Orders, newItems [][]*LineItem) (upds []antidote.UpdateObjectParams) {
	//Removes can be translated into Decrements, while additions can be translated into Increments.
	//Instead, we'll just do both together and use increments for all.
	//Remember that a negative increment is the same as a decrement

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

	var order *Orders
	var customer *Customer
	var supplier *Supplier
	var year int16
	var nationKey, regionKey int8
	var value *float64
	//Same processing for removed and new, just with opposite signals
	procOrders, procItems, multiplier := newOrders[1:], newItems[1:], 1.0
	for j := 0; j < 2; j++ {
		fmt.Println(j)
		for i, orderItems := range procItems {
			order = procOrders[i]
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
						*value += multiplier * lineItem.L_EXTENDEDPRICE * (1 - lineItem.L_DISCOUNT)
					}
				}
			}
		}
		//Now, deletes
		procOrders, procItems, multiplier = remOrders[1:], remItems[1:], -1.0
	}

	return makeQ5IndexUpds(sumMap, INDEX_BKT)
}

func getQ14UpdsV2(remOrders []*Orders, remItems [][]*LineItem, newOrders []*Orders, newItems [][]*LineItem) (upds []antidote.UpdateObjectParams) {
	//We can do the same trick as in Q5. Removes are AddMultipleValue with negatives, while news are with positives.
	//Should be ok to mix both.

	mapPromo, mapTotal := make(map[string]*float64), make(map[string]*float64)
	inPromo := procTables.PromoParts

	var i, j int64
	iString, fullKey := "", ""
	//Preparing the maps that'll hold the results for each month between 1993 and 1997
	for i = 1993; i <= 1997; i++ {
		iString = strconv.FormatInt(i, 10)
		for j = 1; j <= 12; j++ {
			fullKey = iString + strconv.FormatInt(j, 10)
			promo, total := 0.0, 0.0
			mapPromo[fullKey], mapTotal[fullKey] = &promo, &total
		}
	}

	//Going through lineitem and updating the totals
	var year int16
	revenue := 0.0
	date := ""

	procItems, multiplier := newItems[1:], 1.0
	//Same processing for removed and new, just with inverted signals
	for i := 0; i < 2; i++ {
		for _, orderItems := range procItems {
			for _, lineItem := range orderItems {
				year = lineItem.L_SHIPDATE.YEAR
				if year >= 1993 && year <= 1997 {
					revenue = multiplier * lineItem.L_EXTENDEDPRICE * (1.0 - lineItem.L_DISCOUNT)
					date = strconv.FormatInt(int64(year), 10) + strconv.FormatInt(int64(lineItem.L_SHIPDATE.MONTH), 10)
					if _, has := inPromo[lineItem.L_PARTKEY]; has {
						*mapPromo[date] += revenue
					}
					*mapTotal[date] += revenue
				}
			}
		}
		procItems, multiplier = remItems[1:], -1.0
	}

	return makeQ14IndexUpds(mapPromo, mapTotal, INDEX_BKT)
}

func getQ15UpdsV2(remOrders []*Orders, remItems [][]*LineItem, newOrders []*Orders, newItems [][]*LineItem) (upds []antidote.UpdateObjectParams) {
	//Need to update totals in q15Map and mark which entries were updated
	//Also uses the same idea as in other queries than removes are the same as news but with inverted signals
	updEntries := make(map[int16]map[int8]map[int32]struct{})
	var year int16 = 1993
	for ; year <= 1997; year++ {
		mMap := make(map[int8]map[int32]struct{})
		mMap[1], mMap[4], mMap[7], mMap[10] = make(map[int32]struct{}), make(map[int32]struct{}), make(map[int32]struct{}), make(map[int32]struct{})
		updEntries[year] = mMap
	}

	var month int8
	var suppMap map[int32]*float64
	var currValue *float64
	var has bool

	for _, orderItems := range remItems[1:] {
		for _, item := range orderItems {
			year = item.L_SHIPDATE.YEAR
			if year >= 1993 && year <= 1997 {
				month = ((item.L_SHIPDATE.MONTH-1)/3)*3 + 1
				*q15Map[year][month][item.L_SUPPKEY] -= (item.L_EXTENDEDPRICE * (1.0 - item.L_DISCOUNT))
				updEntries[year][month][item.L_SUPPKEY] = struct{}{}
			}
		}
	}

	for _, orderItems := range newItems[1:] {
		for _, item := range orderItems {
			year = item.L_SHIPDATE.YEAR
			if year >= 1993 && year <= 1997 {
				month = ((item.L_SHIPDATE.MONTH-1)/3)*3 + 1
				suppMap = q15Map[year][month]
				currValue, has = suppMap[item.L_SUPPKEY]
				if !has {
					currValue = new(float64)
					suppMap[item.L_SUPPKEY] = currValue
				}
				*currValue += (item.L_EXTENDEDPRICE * (1.0 - item.L_DISCOUNT))
				updEntries[year][month][item.L_SUPPKEY] = struct{}{}
			}
		}
	}

	nUpds := 0
	for _, monthMap := range updEntries {
		for _, suppMap := range monthMap {
			nUpds += len(suppMap)
		}
	}

	return makeQ15IndexUpdsDeletes(q15Map, updEntries, nUpds)
}

func getQ18UpdsV2(remOrders []*Orders, remItems [][]*LineItem, newOrders []*Orders,
	newItems [][]*LineItem) (rems []antidote.UpdateObjectParams, upds []antidote.UpdateObjectParams) {
	//Remove orders, and then call prepareQ18Index with the new orders/items
	//Quantity -> orderID
	toRemove := make([]map[int32]struct{}, 4, 4)
	for i, _ := range toRemove {
		toRemove[i] = make(map[int32]struct{})
	}

	orderKey, currQuantity, j := int32(-1), int32(0), int32(0)
	var orderItems []*LineItem
	nRems := 0
	for i, order := range remOrders[1:] {
		currQuantity = 0
		orderKey = order.O_ORDERKEY
		orderItems = remItems[i+1]
		for _, item := range orderItems {
			currQuantity += int32(item.L_QUANTITY)
		}
		if currQuantity >= 312 {
			currQuantity -= 311
			for j = 0; j < currQuantity && j < 4; j++ {
				toRemove[j][orderKey] = struct{}{}
				nRems++
			}
		}
	}

	rems = makeQ18IndexRemoves(toRemove, nRems)
	existingOrders, existingItems := procTables.Orders, procTables.LineItems
	procTables.Orders, procTables.LineItems = newOrders, newItems
	upds = prepareQ18Index()
	procTables.Orders, procTables.LineItems = existingOrders, existingItems
	return
}

func makeQ3IndexRemoves(remMap map[string]map[int8]map[int32]struct{}, nUpds int) (rems []antidote.UpdateObjectParams) {
	//TODO: TopKRemoveAll for optimization purposes?
	rems = make([]antidote.UpdateObjectParams, nUpds)
	var keyArgs antidote.KeyParams
	i := 0
	for mktSeg, segMap := range remMap {
		for day, dayMap := range segMap {
			//A topK per pair (mktsegment, orderdate)
			keyArgs = antidote.KeyParams{
				Key:      SEGM_DELAY + mktSeg + strconv.FormatInt(int64(day), 10),
				CrdtType: proto.CRDTType_TOPK_RMV,
				Bucket:   buckets[INDEX_BKT],
			}
			for orderKey := range dayMap {
				var currUpd crdt.UpdateArguments = crdt.TopKRemove{Id: orderKey}
				rems[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
				fmt.Println(rems[i])
				i++
			}
		}
	}
	return
}

func makeQ15IndexUpdsDeletes(yearMap map[int16]map[int8]map[int32]*float64, updEntries map[int16]map[int8]map[int32]struct{},
	nUpds int) (upds []antidote.UpdateObjectParams) {

	upds = make([]antidote.UpdateObjectParams, nUpds)
	var monthMap map[int8]map[int32]*float64
	var suppMap map[int32]*float64
	var keyArgs antidote.KeyParams
	i := 0
	var value *float64
	for year, mUpd := range updEntries {
		monthMap = yearMap[year]
		for month, sUpd := range mUpd {
			suppMap = monthMap[month]
			if len(sUpd) > 0 {
				keyArgs = antidote.KeyParams{
					Key:      TOP_SUPPLIERS + strconv.FormatInt(int64(year), 10) + strconv.FormatInt(int64(month), 10),
					CrdtType: proto.CRDTType_TOPK_RMV,
					Bucket:   buckets[INDEX_BKT],
				}
				for suppKey, _ := range sUpd {
					value = suppMap[suppKey]
					var currUpd crdt.UpdateArguments
					if *value == 0.0 {
						currUpd = crdt.TopKRemove{Id: suppKey}
					} else {
						currUpd = crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: suppKey, Score: int32(*value)}}
					}
					upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
					i++
				}
			}
		}
	}
	return
}

func makeQ18IndexRemoves(toRemove []map[int32]struct{}, nRems int) (rems []antidote.UpdateObjectParams) {
	rems = make([]antidote.UpdateObjectParams, nRems)
	var keyArgs antidote.KeyParams

	//TODO: TopK with multipleRemove
	i := 0
	for baseQ, orderMap := range toRemove {
		keyArgs = antidote.KeyParams{
			Key:      LARGE_ORDERS + strconv.FormatInt(int64(312+baseQ), 10),
			CrdtType: proto.CRDTType_TOPK_RMV,
			Bucket:   buckets[INDEX_BKT],
		}
		for orderKey := range orderMap {
			var currUpd crdt.UpdateArguments = crdt.TopKRemove{Id: orderKey}
			rems[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
			i++
		}
	}
	return
}

/*****OBSOLETE*****/

func sendUpdateData(ordersUpds [][]string, lineItemUpds [][]string, deleteKeys []string) {
	/*
		Idea: re-use the connections (possibly with different channels). We'll prepare some updates while the initial data is still being sent, but not all.
		Each update must generate updates for its own table and its indexes. However, let's try to avoid having to use the client tables
		In short:
		Go through each lineItem until we get a full order
		For each order
			- Call a method per query to generate updates for the respective index
			- Group those updates
			- Send those updates
		We can optionally consider grouping multiple orders and calling each query method per order. However, we must ensure everything happens in the same txn.
	*/

	updsPerServer := make([]map[string]crdt.UpdateArguments, len(procTables.Regions))
	startLine, endLine := 0, 0
	for _, order := range ordersUpds {
		orderID := order[0]
		//Out of bounds on lineItemUpds is an issue
		for ; lineItemUpds[endLine][0] == orderID; endLine++ {
		}
		getUpdWithIndex(order, lineItemUpds[startLine:endLine])
		startLine = endLine
	}
	ignore(updsPerServer)
}

func getUpdWithIndex(order []string, lineItems [][]string) {
	_, orderUpd := getEntryUpd(headers[ORDERS], keys[ORDERS], order, read[ORDERS])
	lineUpds := make([]*crdt.EmbMapUpdateAll, len(lineItems))
	for i, item := range lineItems {
		_, lineUpds[i] = getEntryUpd(headers[LINEITEM], keys[LINEITEM], order, read[LINEITEM])
		ignore(item)
	}
	//orderObj, lineItemsObjs := procTables.UpdateOrderLineitems(order, lineItems)
	//indexUpds := getIndexUpds(orderObj, lineItemsObjs)
	ignore(orderUpd)
}

func getIndexUpds(order *Orders, lineItems []*LineItem) {
	q3Upds := getQ3Upds(order, lineItems)
	q5Upds := getQ5Upds(order, lineItems)
	//Query 11 doesn't need updates (Nation/Supply only, which are never updated.)
	//q11Upds := getQ11Upds(order, lineItems)
	q14Upds := getQ14Upds(order, lineItems)
	q15Upds := getQ15Upds(order, lineItems)
	q18Upds := getQ18Upds(order, lineItems)
	ignore(q3Upds, q5Upds, q14Upds, q15Upds, q18Upds)
}

func getQ3Upds(order *Orders, lineItems []*LineItem) (upds []antidote.UpdateObjectParams) {
	/*
		Q3 - TopK per pair (o_orderdate, c_mktsegment). Each topK entry: (orderKey, sum)
				- Sum = l_extendedprice * (1 - l_discount)
				Need to update all entries for each date < O_ORDERDATE and whose C_MKTSEGMENT matches the
				order's customer C_MKTSEGMENT.
	*/
	const maxDay int8 = 31
	sums := make([]*float64, maxDay)
	for i := range sums {
		sums[i] = new(float64)
	}
	nUpds := 0
	mktSeg := procTables.Customers[order.O_CUSTKEY].C_MKTSEGMENT

	date := order.O_ORDERDATE
	minDate, maxDate := &Date{YEAR: 1995, MONTH: 03, DAY: 01}, &Date{YEAR: 1995, MONTH: 03, DAY: 31}
	var minDay int8
	if date.isSmallerOrEqual(maxDate) {
		for _, item := range lineItems {
			if item.L_SHIPDATE.isHigherOrEqual(minDate) {
				if item.L_SHIPDATE.MONTH > 3 || item.L_SHIPDATE.YEAR > 1995 {
					//All days
					minDay = 1
				} else {
					minDay = item.L_SHIPDATE.DAY + 1
				}
				for j := minDay; j <= maxDay; j++ {
					if *sums[j] == 0 {
						nUpds++
					}
					*sums[j] += item.L_EXTENDEDPRICE * (1.0 - item.L_DISCOUNT)
				}
			}
		}

		upds = make([]antidote.UpdateObjectParams, nUpds)
		var keyArgs antidote.KeyParams
		i := 0
		for day, sum := range sums {
			if *sum > 0 {
				keyArgs = antidote.KeyParams{
					Key:      SEGM_DELAY + mktSeg + strconv.FormatInt(int64(day+1), 10),
					CrdtType: proto.CRDTType_TOPK_RMV,
					Bucket:   buckets[INDEX_BKT],
				}
				var currUpd crdt.UpdateArguments = crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: order.O_ORDERKEY, Score: int32(*sum)}}
				upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
				i++
			}
		}

		return
	}
	//No update in the index needed
	return []antidote.UpdateObjectParams{}
}
func getQ5Upds(order *Orders, lineItems []*LineItem) (upds []antidote.UpdateObjectParams) {
	/*
		Q5 - sum(l_extendedprice * (1 - l_discount)) for each pair (country, pair)
		The indexes are implemented as a EmbMap of region+date, with one counter entry per nation
		Note that all updates will go for the same year, region and nation! (as the supplier's nation must match the customer's)
		So we only need one value and, thus, one increment, if any.
	*/
	year := order.O_ORDERDATE.YEAR
	value := 0.0
	if year >= 1993 && year <= 1997 {
		customer := procTables.Customers[order.O_CUSTKEY]
		nation := procTables.Nations[customer.C_NATIONKEY]
		var supplier *Supplier
		for _, item := range lineItems {
			supplier = procTables.Suppliers[item.L_SUPPKEY]
			if customer.C_NATIONKEY == supplier.S_NATIONKEY {
				value += item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT)
			}
		}
		if value > 0.0 {
			var mapUpd crdt.UpdateArguments = crdt.EmbMapUpdateAll{
				Upds: map[string]crdt.UpdateArguments{nation.N_NAME: crdt.Increment{Change: int32(value)}},
			}
			return []antidote.UpdateObjectParams{
				antidote.UpdateObjectParams{
					KeyParams: antidote.KeyParams{
						Key:      NATION_REVENUE + procTables.Regions[nation.N_NATIONKEY].R_NAME + strconv.FormatInt(int64(year), 10),
						CrdtType: proto.CRDTType_RRMAP,
						Bucket:   buckets[INDEX_BKT],
					},
					UpdateArgs: &mapUpd,
				},
			}
		}
	}
	//No update in the index needed
	return []antidote.UpdateObjectParams{}
}
func getQ14Upds(order *Orders, lineItems []*LineItem) (upds []antidote.UpdateObjectParams) {
	/*
		Q14 - 100*sum(l_extendedprice * (1-l_discount)), when p_type = PROMO%, then divide by
		sum(l_extendedprice * (1-l_discount)) of all parts.
		So if the piece is in promo, just add the same value in both SumValue and NAdds. If it isn't, add 0
		in SumValue and the value in NAdds.
		Do this for each piece. Group it by month (l_shipdate)
	*/

	promoValues, totalValues := make(map[string]float64, len(lineItems)), make(map[string]float64, len(lineItems))
	currValue := 0.0
	const promo = "PROMO"
	var key string

	for _, item := range lineItems {
		if item.L_SHIPDATE.YEAR >= 1993 && item.L_SHIPDATE.YEAR <= 1997 {
			key = strconv.FormatInt(int64(item.L_SHIPDATE.YEAR), 10) + strconv.FormatInt(int64(item.L_SHIPDATE.MONTH), 10)
			currValue = item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT)
			if strings.HasPrefix(procTables.Parts[item.L_PARTKEY].P_TYPE, promo) {
				promoValues[key] += currValue
			}
			totalValues[key] += currValue
		}

	}

	upds = make([]antidote.UpdateObjectParams, len(totalValues))
	i := 0
	for key, total := range totalValues {
		promo := promoValues[key]
		var currUpd crdt.UpdateArguments = crdt.AddMultipleValue{
			SumValue: int64(100.0 * promo),
			NAdds:    int64(total),
		}
		upds[i] = antidote.UpdateObjectParams{
			KeyParams:  antidote.KeyParams{Key: PROMO_PERCENTAGE + key, CrdtType: proto.CRDTType_AVG, Bucket: buckets[INDEX_BKT]},
			UpdateArgs: &currUpd,
		}
		i++
	}

	return
}

func getQ15Upds(order *Orders, lineItems []*LineItem) (upds []antidote.UpdateObjectParams) {
	/*
		Q15: topk(sum(l_extendedprice * (1-l_discount))). Each entry corresponds to one supplier.
		One topk by quarters, between 1st month of 1993 and 10th month of 1997.
		To determine quarter, use l_shipdate
	*/
	/*
		yearMap := make(map[int16]map[int8]map[int32]float64)
		var monthMap map[int8]map[int32]float64
		var suppMap map[int32]float64
	*/
	//Year (0-4 for 1993-1997), Quarter (0-3), supplierID -> sum
	yearMap := make([][]map[int32]float64, 4)
	for i := range yearMap {
		yearMap[i] = make([]map[int32]float64, 4)
	}
	var suppMap map[int32]float64
	year, month, possibleUpds := int16(0), int8(0), 0
	var year64, month64 int64

	for _, item := range lineItems {
		year = item.L_SHIPDATE.YEAR - 1993
		//year >= 1993 && year <= 1997
		if year >= 0 && year <= 3 {
			month = ((item.L_SHIPDATE.MONTH - 1) / 3)
			suppMap = yearMap[year][month]
			if suppMap == nil {
				suppMap = make(map[int32]float64)
				yearMap[year][month] = suppMap
			}
			suppMap[item.L_SUPPKEY] += (item.L_EXTENDEDPRICE * (1.0 - item.L_DISCOUNT))
			possibleUpds++
		}
	}

	upds = make([]antidote.UpdateObjectParams, possibleUpds)
	var keyArgs antidote.KeyParams
	i := 0
	for j, monthMap := range yearMap {
		year64 = int64(j) + 1993
		for k, suppMap := range monthMap {
			month64 = int64(k)*3 + 1
			keyArgs = antidote.KeyParams{
				Key:      TOP_SUPPLIERS + strconv.FormatInt(year64, 10) + strconv.FormatInt(month64, 10),
				CrdtType: proto.CRDTType_TOPK_RMV,
				Bucket:   buckets[INDEX_BKT],
			}
			for suppKey, value := range suppMap {
				var currUpd crdt.UpdateArguments = crdt.TopKAdd{TopKScore: crdt.TopKScore{
					Id:    suppKey,
					Score: int32(value),
				}}
				upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
				i++
			}
		}
	}

	//If multiple lineitems are from the same supplier, then not all positions in upds will be filled
	return upds[:i]
}

func getQ18Upds(order *Orders, lineItems []*LineItem) (upds []antidote.UpdateObjectParams) {
	quantity := 0
	for _, item := range lineItems {
		quantity += int(item.L_QUANTITY)
	}

	var keyArgs antidote.KeyParams
	if quantity >= 312 {
		nUpds := min(315, quantity) - 311 //311 instead of 312 to give space for 312-315.
		upds = make([]antidote.UpdateObjectParams, nUpds)
		for i := 0; i < nUpds; i++ {
			keyArgs = antidote.KeyParams{
				Key:      LARGE_ORDERS + strconv.FormatInt(int64(quantity), 10),
				CrdtType: proto.CRDTType_TOPK_RMV,
				Bucket:   buckets[INDEX_BKT],
			}
			var currUpd crdt.UpdateArguments = crdt.TopKAdd{TopKScore: crdt.TopKScore{
				Id:    order.O_ORDERKEY,
				Score: int32(quantity),
			}}
			upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
			i++
		}
	}

	//if quantity is < 312 then there's no updates
	return []antidote.UpdateObjectParams{}
}

/*
func prepareDeletes(deleteKeys []string) {
	tableKeys := []string{tableNames[3], tableNames[1]}
	deleteProto := getDeletes(tableKeys, deleteKeys)
	updsQueue <- QueuedMsg{Message: antidote.CreateStaticUpdateObjs(nil, deleteProto), code: antidote.StaticUpdateObjs}
}

func getDeletes(tableKeys []string, deleteKeys []string) (objDeletes []antidote.UpdateObjectParams) {
	objDeletes = make([]antidote.UpdateObjectParams, len(tableKeys))
	i := 0
	for _, tableKey := range tableKeys {
		objDeletes[i] = *getTableDelete(tableKey, deleteKeys)
		i++
	}
	return objDeletes
}

func getTableDelete(tableKey string, deleteKeys []string) (delete *antidote.UpdateObjectParams) {
	var mapRemove crdt.UpdateArguments = crdt.MapRemoveAll{Keys: deleteKeys}
	return &antidote.UpdateObjectParams{
		KeyParams:  antidote.KeyParams{Key: tableKey, CrdtType: proto.CRDTType_RRMAP, Bucket: "bkt"},
		UpdateArgs: &mapRemove,
	}
}
*/