package tpch

import (
	"encoding/csv"
	"fmt"
	"math"
	"potionDB/src/antidote"
	"potionDB/src/crdt"
	"potionDB/src/proto"
	"strconv"
	"time"
)

type TableInfo struct {
	*Tables
}

var (
	//Filled by configLoader
	N_UPDATE_FILES                 int //Used by this update client only. clientQueryUpd.go use START_UPD_FILE and FINISH_UPD_FILE.
	UPDATES_GOROUTINES             int
	UPDATE_INDEX, UPDATE_BASE_DATA bool //If indexes/base data (order/items) should be updated or not. The latter is only supported by mix clients for now.
	UPDATE_SPECIFIC_INDEX_ONLY     bool //Uses list of queries to know which indexes to update

	updsNames = [...]string{"orders", "lineitem", "delete"}
	//Orders and delete follow SF, except for SF = 0.01. It's filled automatically in tpchClient.go
	updEntries          []int
	updParts            = [...]int{9, 16}
	collectUpdStats     = false //Becomes true when enough time has passed to collect statistics again
	updStats            = make([]UpdatesStats, 0, 100)
	currUpdStats        = UpdatesStats{}
	nFinishedUpdComms   = int64(0)
	collectUpdStatsComm = []bool{false, false, false, false, false}
	updStatsComm        = make([][]UpdatesStats, 5)
	indexesToUpd        []int //Each position corresponds to the number of a query

	updsFinishChan = make(chan bool, UPDATES_GOROUTINES)
)

//TODO: removes/updates for individual objects (i.e., option in which each customer has its own CRDT)?

//Pre-condition: routines >= N_UPDATE_FILES. Also, distribution isn't much fair if routines is close to N_UPDATE_FILES (ideally, should be at most 1/4)
func splitUpdatesPerRoutine(routines int, ordersUpds [][]string, lineItemUpds [][]string, deleteKeys []string, lineItemSizes []int) (filesPerRoutine int,
	tableInfos []TableInfo, routineOrders, routineItems [][][]string, routineDelete [][]string, routineLineSizes [][]int) {
	orderStart, lineStart, orderFinish, lineFinish := 0, 0, -1, 0
	//Calculating split per routine. Last goroutine will take the leftovers
	filesPerRoutine = N_UPDATE_FILES / routines
	routineOrders, routineItems, routineDelete, routineLineSizes = make([][][]string, routines), make([][][]string, routines),
		make([][]string, routines), make([][]int, routines)
	tableInfos, currTableInfo := make([]TableInfo, routines), TableInfo{}
	j, previousJ := 0, 0
	for i := 0; i < routines-1; i++ {
		for ; j < (i+1)*filesPerRoutine; j++ {
			lineFinish += lineItemSizes[j]
		}
		orderFinish += (j - previousJ) * (updEntries[0] + 1)
		//orderFinish += ordersPerRoutine
		routineOrders[i], routineItems[i], routineDelete[i], routineLineSizes[i] = ordersUpds[orderStart:orderFinish], lineItemUpds[lineStart:lineFinish],
			deleteKeys[orderStart:orderFinish], lineItemSizes[previousJ:j]

		currTableInfo = TableInfo{Tables: procTables.GetShallowCopy()}
		//Need to update last deleted index...
		currTableInfo.LastDeletedPos, currTableInfo.orderIndexFun = orderStart, currTableInfo.getUpdateOrderIndex
		tableInfos[i] = currTableInfo

		orderStart, lineStart, previousJ = orderFinish, lineFinish, j
	}
	//Leftovers go to the last goroutine. C'est la vie.
	currTableInfo = TableInfo{Tables: procTables.GetShallowCopy()}
	currTableInfo.LastDeletedPos, currTableInfo.orderIndexFun = orderStart, currTableInfo.getUpdateOrderIndex
	routineOrders[routines-1], routineItems[routines-1] = ordersUpds[orderStart:], lineItemUpds[lineStart:]
	routineDelete[routines-1], routineLineSizes[routines-1] = deleteKeys[orderStart:], lineItemSizes[previousJ:]
	tableInfos[routines-1] = currTableInfo

	//Fix lastDeletePos of first routine (orders start at index 1)
	tableInfos[0].LastDeletedPos = 1

	return
}

func startUpdates() {
	fmt.Println("Reading updates...")
	readStart := time.Now().UnixNano()
	ordersUpds, lineItemUpds, deleteKeys, lineItemSizes := readUpds()
	readFinish := time.Now().UnixNano()
	fmt.Println("Finished reading updates. Time taken for read:", (readFinish-readStart)/1000000, "ms")

	connectToServers()

	//Start server communication for update sending
	for i := range channels.dataChans {
		go handleUpdatesComm(i)
	}

	if N_UPDATE_FILES < UPDATES_GOROUTINES {
		UPDATES_GOROUTINES = N_UPDATE_FILES
	}

	filesPerRoutine, tableInfos, routineOrders, routineItems, routineDelete, routineLineSizes :=
		splitUpdatesPerRoutine(UPDATES_GOROUTINES, ordersUpds, lineItemUpds, deleteKeys, lineItemSizes)

	//Sleep until query clients start. We need to offset the time spent on sending base data/indexes, plus reading time
	//QUERY_WAIT is in ms.
	fmt.Println("Waiting to start updates...")
	time.Sleep(QUERY_WAIT*1000000 - time.Duration(time.Now().UnixNano()-times.startTime))
	fmt.Println("Starting updates...")
	if statisticsInterval > 0 {
		go doUpdStatsInterval()
	}
	updateStart := time.Now().UnixNano()
	fmt.Println("Number of update files:", N_UPDATE_FILES)

	for i := 0; i < UPDATES_GOROUTINES; i++ {
		nFiles := filesPerRoutine
		if i == UPDATES_GOROUTINES-1 {
			nFiles = N_UPDATE_FILES - (filesPerRoutine * (UPDATES_GOROUTINES))
		}
		go func(nFiles int, ti TableInfo, orders [][]string, items [][]string, deletes []string, itemSizes []int) {
			orderS, lineS, orderF, lineF := 0, 0, -1, 0
			if orders[0][0] > "1" {
				orderF = 0
			}
			for j := 0; j < nFiles; j++ {
				if j%(N_UPDATE_FILES/40) == 0 {
					fmt.Println("Update", j)
				}
				orderF, lineF = orderF+updEntries[0]+1, lineF+itemSizes[j]
				ti.sendDataChangesV2(orders[orderS:orderF], items[lineS:lineF], deletes[orderS:orderF])
				orderS, lineS = orderF, lineF
				//break
			}
			updsFinishChan <- true
			fmt.Println("Finished goroutine")
		}(nFiles, tableInfos[i], routineOrders[i], routineItems[i], routineDelete[i], routineLineSizes[i])
	}

	for nFinished := 0; nFinished < UPDATES_GOROUTINES; nFinished++ {
		<-updsFinishChan
	}

	for i := 0; i < len(channels.dataChans); i++ {
		//channels.dataChans[i] <- QueuedMsg{code: QUEUE_COMPLETE}
		channels.updateChans[i] <- QueuedMsgWithStat{QueuedMsg: QueuedMsg{code: QUEUE_COMPLETE}}
	}
	updateFinish := time.Now().UnixNano()
	fmt.Println("Finished updates. Time taken for updating:", (updateFinish-updateStart)/1000000)
}

func readUpds() (ordersUpds [][]string, lineItemUpds [][]string, deleteKeys []string, lineItemSizes []int) {
	updPartsRead := [][]int8{read[ORDERS], read[LINEITEM]}
	return ReadUpdates(updCompleteFilename[:], updEntries[:], updParts[:], updPartsRead, N_UPDATE_FILES)
}

func (ti TableInfo) sendDataChangesV2(ordersUpds [][]string, lineItemUpds [][]string, deleteKeys []string) {
	//Maybe I can do like this:
	//1st - deletes
	//2nd - updates
	//3rd - indexes
	//Works as long as it is in the same txn or in the same staticUpdateObjs
	//I actually can't do everything in the same transaction as it involves different servers...
	//But I should still guarantee that, for the same server, it's atomic, albeit index results will still be screwed.
	ti.sendDeletes(deleteKeys)
	//deletedOrders, deletedItems := getDeleteClientTables(deleteKeys)
	ti.sendUpdates(ordersUpds, lineItemUpds)
	//ignore(deletedOrders, deletedItems)

	return
}

func (ti TableInfo) sendDeletes(deleteKeys []string) (orders []*Orders, items [][]*LineItem) {
	//updsDone := 0
	//start := time.Now().UnixNano()
	orders, items = ti.getDeleteClientTables(deleteKeys)
	//orders, items = orders[1:], items[1:] //Hide initial entry which is always empty
	nDeleteItems := getNumberDeleteItems(items)
	ordersPerServer, itemsPerServer, orderIndex, itemsIndex := makePartionedDeleteStructs(deleteKeys, nDeleteItems)

	var order *Orders
	var orderItems []*LineItem
	var custRegion, suppRegion int8
	var itemS string

	//fmt.Println("[DELETE]Last order, item:", *orders[len(orders)-1], *items[len(orders)-1][len(items[len(orders)-1])-1])

	for i, orderS := range deleteKeys {
		order, orderItems = orders[i], items[i]
		custRegion = ti.Tables.Custkey32ToRegionkey(order.O_CUSTKEY)
		ordersPerServer[custRegion][orderIndex[custRegion]] = orderS
		orderIndex[custRegion]++

		for _, item := range orderItems {
			//TODO: is itemS correct? Considering on getEntryUpd/getEntryKey they add "_" (and, possibly, tableName)
			itemS = orderS + strconv.FormatInt(int64(item.L_PARTKEY), 10) +
				strconv.FormatInt(int64(item.L_SUPPKEY), 10) + strconv.FormatInt(int64(item.L_LINENUMBER), 10)
			itemsPerServer[custRegion][itemsIndex[custRegion]] = itemS
			itemsIndex[custRegion]++
			suppRegion = ti.Tables.SuppkeyToRegionkey(int64(item.L_SUPPKEY))
			if custRegion != suppRegion {
				itemsPerServer[suppRegion][itemsIndex[suppRegion]] = itemS
				itemsIndex[suppRegion]++
			}
		}
	}

	var orderParams, itemParams antidote.UpdateObjectParams
	if !CRDT_PER_OBJ {
		for i, orderKeys := range ordersPerServer {
			orderParams = getDeleteParams(tableNames[ORDERS], buckets[i], orderKeys, orderIndex[i])
			itemParams = getDeleteParams(tableNames[LINEITEM], buckets[i], itemsPerServer[i], itemsIndex[i])
			/*
				channels.dataChans[i] <- QueuedMsg{
					code:    antidote.StaticUpdateObjs,
					Message: antidote.CreateStaticUpdateObjs(nil, []antidote.UpdateObjectParams{orderParams, itemParams}),
				}
				updsDone += orderIndex[i] + itemsIndex[i]
			*/
			queueStatMsg(i, []antidote.UpdateObjectParams{orderParams, itemParams}, orderIndex[i]+itemsIndex[i], REMOVE_TYPE)
		}
	} else {
		for i, orderKeys := range ordersPerServer {
			//upds := make([]antidote.UpdateObjectParams, len(orderKeys)+len(itemsPerServer[i]))
			upds := make([]antidote.UpdateObjectParams, orderIndex[i]+itemsIndex[i])
			written := getPerObjDeleteParams(tableNames[ORDERS], buckets[i], orderKeys, orderIndex[i], upds, 0)
			getPerObjDeleteParams(tableNames[LINEITEM], buckets[i], itemsPerServer[i], itemsIndex[i], upds, written)
			/*
				channels.dataChans[i] <- QueuedMsg{
					code:    antidote.StaticUpdateObjs,
					Message: antidote.CreateStaticUpdateObjs(nil, upds),
				}
				updsDone += orderIndex[i] + itemsIndex[i]
			*/
			queueStatMsg(i, upds, orderIndex[i]+itemsIndex[i], REMOVE_TYPE)
		}
	}
	/*
		finish := time.Now().UnixNano()
		currUpdStats.removeDataTimeSpent += finish - start
		currUpdStats.removeDataUpds += updsDone
	*/
	return
}

func (ti TableInfo) getDeleteClientTables(deleteKeys []string) (orders []*Orders, items [][]*LineItem) {
	startPos, endPos := ti.Tables.LastDeletedPos, ti.Tables.LastDeletedPos+len(deleteKeys)
	//Not needed as now LastDeletedPos starts at 1.
	/*
		if startPos == 0 {
			startPos, endPos = 1, endPos+1 //first entry is empty
			//startPos = 1
		}
	*/
	ti.Tables.LastDeletedPos = endPos
	//lineitems and orders are offset by 1 unit
	orders, items = ti.Tables.Orders[startPos:endPos], ti.Tables.LineItems[startPos-1:endPos-1]
	return
}

func getDeleteParams(tableName string, bkt string, keys []string, nKeys int) antidote.UpdateObjectParams {
	var mapRemove crdt.UpdateArguments = crdt.MapRemoveAll{Keys: keys[:nKeys]}
	return antidote.UpdateObjectParams{
		KeyParams:  antidote.KeyParams{Key: tableName, CrdtType: proto.CRDTType_RRMAP, Bucket: bkt},
		UpdateArgs: &mapRemove,
	}
}

//TODO: DeleteAll?
func getPerObjDeleteParams(tableName string, bkt string, keys []string, nKeys int,
	buffer []antidote.UpdateObjectParams, bufI int) (newBufI int) {
	var delete crdt.UpdateArguments = crdt.ResetOp{}
	for _, key := range keys {
		buffer[bufI] = antidote.UpdateObjectParams{
			KeyParams:  antidote.KeyParams{Key: tableName + key, CrdtType: proto.CRDTType_RRMAP, Bucket: bkt},
			UpdateArgs: &delete,
		}
		bufI++
	}
	return bufI
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

func (ti TableInfo) sendUpdates(ordersUpds [][]string, lineItemUpds [][]string) {
	//start := time.Now().UnixNano()
	//Different strategy from the one in sendUpdateData. We'll prepare all updates while the initial data is still being sent as updates are around 0.1% of lineitems.
	//Also, at first we'll only do updates, indexes will be done afterwards and considering simultaneously added and removed keys.
	//Worst case if we need to save memory, we can do the index calculus first.
	ordersPerServer, itemsPerServer := make([]map[string]crdt.UpdateArguments, len(ti.Tables.Regions)), make([]map[string]crdt.UpdateArguments, len(ti.Tables.Regions))
	for i := 0; i < len(ti.Tables.Regions); i++ {
		ordersPerServer[i] = make(map[string]crdt.UpdateArguments)
		itemsPerServer[i] = make(map[string]crdt.UpdateArguments)
	}
	var key string
	var upd *crdt.MapAddAll
	orderFunc, itemFunc := regionFuncs[ORDERS], multiRegionFunc[LINEITEM]
	var itemRegions []int8

	for _, order := range ordersUpds {
		key, upd = getEntryORMapUpd(headers[ORDERS], keys[ORDERS], order, read[ORDERS])
		key = getEntryKey(tableNames[ORDERS], key)
		ordersPerServer[orderFunc(order)][key] = *upd
	}
	for _, item := range lineItemUpds {
		key, upd = getEntryORMapUpd(headers[LINEITEM], keys[LINEITEM], item, read[LINEITEM])
		key = getEntryKey(tableNames[LINEITEM], key)
		itemRegions = itemFunc(item)
		for _, region := range itemRegions {
			itemsPerServer[region][key] = *upd
		}
	}

	var currUpdParams []antidote.UpdateObjectParams
	for i := 0; i < len(ti.Tables.Regions); i++ {
		//This could be in a single msg
		currUpdParams = make([]antidote.UpdateObjectParams,
			getUpdParamsSize([]map[string]crdt.UpdateArguments{ordersPerServer[i], itemsPerServer[i]}))
		written := getDataUpdateParamsWithBuf(ordersPerServer[i], tableNames[ORDERS], buckets[i], currUpdParams, 0)
		getDataUpdateParamsWithBuf(itemsPerServer[i], tableNames[LINEITEM], buckets[i], currUpdParams, written)
		queueStatMsg(i, currUpdParams, len(ordersPerServer[i])+len(itemsPerServer[i]), NEW_TYPE)
		/*
			//orders
			currUpdParams = getDataUpdateParams(ordersPerServer[i], tableNames[ORDERS], buckets[i])
			channels.updateChans[i] <- QueuedMsgWithStat{QueuedMsg: QueuedMsg{
				code:    antidote.StaticUpdateObjs,
				Message: antidote.CreateStaticUpdateObjs(nil, currUpdParams)},
				nData:    len(ordersPerServer[i]),
				dataType: NEW_TYPE,
			}
			//lineitems
			currUpdParams = getDataUpdateParams(itemsPerServer[i], tableNames[LINEITEM], buckets[i])
			channels.updateChans[i] <- QueuedMsgWithStat{QueuedMsg: QueuedMsg{
				code:    antidote.StaticUpdateObjs,
				Message: antidote.CreateStaticUpdateObjs(nil, currUpdParams)},
				nData:    len(itemsPerServer[i]),
				dataType: NEW_TYPE,
			}
		*/

		/*
			queueDataProto(ordersPerServer[i], tableNames[ORDERS], buckets[i], channels.dataChans[i])
			queueDataProto(itemsPerServer[i], tableNames[LINEITEM], buckets[i], channels.dataChans[i])
		*/

	}

	/*
		finish := time.Now().UnixNano()
		currUpdStats.newDataTimeSpent += finish - start
		//TODO: This isn't correct for lineItemUpds, as some lineItems go to 2 regions!!!
		currUpdStats.newDataUpds += len(ordersUpds) + len(lineItemUpds)
	*/
}

func getUpdParamsSize(upds []map[string]crdt.UpdateArguments) (size int) {
	if CRDT_PER_OBJ {
		for _, updMap := range upds {
			size += len(updMap)
		}
		return size
	} else {
		return len(upds)
	}
}

func getDataUpdateParamsWithBuf(currMap map[string]crdt.UpdateArguments, name string, bucket string,
	buf []antidote.UpdateObjectParams, bufI int) (written int) {
	if CRDT_PER_OBJ {
		for key, upd := range currMap {
			buf[bufI] = antidote.UpdateObjectParams{
				KeyParams:  antidote.KeyParams{Key: key, CrdtType: proto.CRDTType_RRMAP, Bucket: bucket},
				UpdateArgs: &upd,
			}
			bufI++
		}
	} else {
		var currUpd crdt.UpdateArguments = crdt.EmbMapUpdateAll{Upds: currMap}
		buf[bufI] = antidote.UpdateObjectParams{
			KeyParams:  antidote.KeyParams{Key: name, CrdtType: proto.CRDTType_RRMAP, Bucket: bucket},
			UpdateArgs: &currUpd}
		bufI++
	}
	return bufI
}

func queueStatMsg(chanIndex int, upds []antidote.UpdateObjectParams, nUpds int, dataType int) {
	channels.updateChans[chanIndex] <- QueuedMsgWithStat{
		QueuedMsg: QueuedMsg{code: antidote.StaticUpdateObjs, Message: antidote.CreateStaticUpdateObjs(nil, upds)},
		nData:     nUpds,
		dataType:  dataType,
	}
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
	_, orderUpd := getEntryORMapUpd(headers[ORDERS], keys[ORDERS], order, read[ORDERS])
	lineUpds := make([]*crdt.MapAddAll, len(lineItems))
	for i, item := range lineItems {
		_, lineUpds[i] = getEntryORMapUpd(headers[LINEITEM], keys[LINEITEM], order, read[LINEITEM])
		ignore(item)
	}
	//orderObj, lineItemsObjs := ti.Tables.UpdateOrderLineitems(order, lineItems)
	//indexUpds := getIndexUpds(orderObj, lineItemsObjs)
	ignore(orderUpd)
}

func doUpdStatsInterval() {
	for {
		time.Sleep(statisticsInterval * time.Millisecond)
		collectUpdStats = true
		collectUpdStatsComm = []bool{true, true, true, true, true}
	}
}

func mergeCommUpdateStats() {
	sizeToUse := int(math.MaxInt32)
	for _, updStat := range updStatsComm {
		if len(updStat) < sizeToUse {
			sizeToUse = len(updStat)
		}
	}
	updStats = make([]UpdatesStats, sizeToUse)
	var currMerge UpdatesStats
	var currStat UpdatesStats
	nClients := int64(len(updStatsComm))

	for i := range updStats {
		currMerge = UpdatesStats{}
		for _, commStats := range updStatsComm {
			currStat = commStats[i]
			currMerge.newDataTimeSpent += currStat.newDataTimeSpent
			currMerge.newDataUpds += currStat.newDataUpds
			currMerge.removeDataTimeSpent += currStat.removeDataTimeSpent
			currMerge.removeDataUpds += currStat.removeDataUpds
			currMerge.indexTimeSpent += currStat.indexTimeSpent
			currMerge.indexUpds += currStat.indexUpds
		}
		currMerge.newDataTimeSpent /= nClients
		currMerge.removeDataTimeSpent /= nClients
		currMerge.indexTimeSpent /= nClients
		updStats[i] = currMerge
	}

}

func writeUpdsStatsFile() {
	data := make([][]string, len(updStats)+2)
	data[0] = []string{"Total time", "Section time", "nAllUpds data", "nAllUpds/s", "nUpds data", "nUpds time (ms)", "nUpds/s", "nUpds/totalS",
		"nRems data", "nRems time (ms)", "nRems/s", "nRems/totalS", "nIndex data", "nIndex time (ms)", "nIndex/s", "nIndex/totalS"}
	newUpdsPerSecond, removesPerSecond, indexPerSecond, updsPerSecond := 0.0, 0.0, 0.0, 0.0
	newUpdsPerTotalSecond, removesPerTotalSecond, indexPerTotalSecond := 0.0, 0.0, 0.0
	totalNewUpds, totalRemoveUpds, totalIndexUpds := 0, 0, 0
	totalNewTime, totalRemoveTime, totalIndexTime := int64(0), int64(0), int64(0)
	timeSpent, totalTimeSpent, totalUpds := int64(0), int64(0), 0

	for i, updStat := range updStats {
		timeSpent = updStat.indexTimeSpent + updStat.newDataTimeSpent + updStat.removeDataTimeSpent
		newUpdsPerSecond = (float64(updStat.newDataUpds) / float64(updStat.newDataTimeSpent)) * 1000
		removesPerSecond = (float64(updStat.removeDataUpds) / float64(updStat.removeDataTimeSpent)) * 1000
		indexPerSecond = (float64(updStat.indexUpds) / float64(updStat.indexTimeSpent)) * 1000
		newUpdsPerTotalSecond = (float64(updStat.newDataUpds) / float64(timeSpent)) * 1000
		removesPerTotalSecond = (float64(updStat.removeDataUpds) / float64(timeSpent)) * 1000
		indexPerTotalSecond = (float64(updStat.indexUpds) / float64(timeSpent)) * 1000
		updsPerSecond = newUpdsPerTotalSecond + removesPerTotalSecond + indexPerTotalSecond

		totalTimeSpent += timeSpent
		totalNewUpds += updStat.newDataUpds
		totalRemoveUpds += updStat.removeDataUpds
		totalIndexUpds += updStat.indexUpds
		totalNewTime += updStat.newDataTimeSpent
		totalRemoveTime += updStat.removeDataTimeSpent
		totalIndexTime += updStat.indexTimeSpent
		totalUpds += updStat.newDataUpds + updStat.removeDataUpds + updStat.indexUpds

		data[i+1] = []string{strconv.FormatInt(totalTimeSpent, 10), strconv.FormatInt(timeSpent, 10),
			strconv.FormatInt(int64(updStat.newDataUpds+updStat.removeDataUpds+updStat.indexUpds), 10), strconv.FormatFloat(updsPerSecond, 'f', 10, 64),
			strconv.FormatInt(int64(updStat.newDataUpds), 10), strconv.FormatInt(updStat.newDataTimeSpent, 10),
			strconv.FormatFloat(newUpdsPerSecond, 'f', 10, 64), strconv.FormatFloat(newUpdsPerTotalSecond, 'f', 10, 64),
			strconv.FormatInt(int64(updStat.removeDataUpds), 10), strconv.FormatInt(updStat.removeDataTimeSpent, 10),
			strconv.FormatFloat(removesPerSecond, 'f', 10, 64), strconv.FormatFloat(removesPerTotalSecond, 'f', 10, 64),
			strconv.FormatInt(int64(updStat.indexUpds), 10), strconv.FormatInt(updStat.indexTimeSpent, 10),
			strconv.FormatFloat(indexPerSecond, 'f', 10, 64), strconv.FormatFloat(indexPerTotalSecond, 'f', 10, 64)}
	}

	totalUpdsPerSecond := (float64(totalNewUpds+totalRemoveUpds+totalIndexUpds) / float64(totalTimeSpent)) * 1000
	totalNewUpdsPerSecond := (float64(totalNewUpds) / float64(totalNewTime)) * 1000
	totalNewUpdsPerTotalSecond := (float64(totalNewUpds) / float64(totalTimeSpent)) * 1000
	totalRemovesPerSecond := (float64(totalRemoveUpds) / float64(totalRemoveTime)) * 1000
	totalRemovesPerTotalSecond := (float64(totalRemoveUpds) / float64(totalTimeSpent)) * 1000
	totalIndexPerSecond := (float64(totalIndexUpds) / float64(totalIndexTime)) * 1000
	totalIndexPerTotalSecond := (float64(totalIndexUpds) / float64(totalTimeSpent)) * 1000

	data[len(data)-1] = []string{strconv.FormatInt(totalTimeSpent, 10), strconv.FormatInt(totalTimeSpent, 10),
		strconv.FormatInt(int64(totalUpds), 10), strconv.FormatFloat(totalUpdsPerSecond, 'f', 10, 64),
		strconv.FormatInt(int64(totalNewUpds), 10), strconv.FormatInt(totalNewTime, 10),
		strconv.FormatFloat(totalNewUpdsPerSecond, 'f', 10, 64), strconv.FormatFloat(totalNewUpdsPerTotalSecond, 'f', 10, 64),
		strconv.FormatInt(int64(totalRemoveUpds), 10), strconv.FormatInt(totalRemoveTime, 10),
		strconv.FormatFloat(totalRemovesPerSecond, 'f', 10, 64), strconv.FormatFloat(totalRemovesPerTotalSecond, 'f', 10, 64),
		strconv.FormatInt(int64(totalIndexUpds), 10), strconv.FormatInt(totalIndexTime, 10),
		strconv.FormatFloat(totalIndexPerSecond, 'f', 10, 64), strconv.FormatFloat(totalIndexPerTotalSecond, 'f', 10, 64)}

	file := getStatsFileToWrite("updateStats")
	if file == nil {
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	writer.Comma = ';'
	defer writer.Flush()

	for _, line := range data {
		writer.Write(line)
	}

	fmt.Println("Update statistics saved successfully.")
}

/*
//One every x seconds.
type UpdatesStats struct {
	newDataUpds         int
	removeDataUpds      int
	indexUpds           int
	newDataTimeSpent    int64
	removeDataTimeSpent int64
	indexTimeSpent      int64
}
*/

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
