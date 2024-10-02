package client

import (
	"encoding/csv"
	"fmt"
	"math"
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

var (
	//Filled by configLoader
	N_UPDATE_FILES                 int //Used by this update client only. clientQueryUpd.go use START_UPD_FILE and FINISH_UPD_FILE.
	UPDATES_GOROUTINES             int
	UPDATE_INDEX, UPDATE_BASE_DATA bool //If indexes/base data (order/items) should be updated or not. The latter is only supported by mix clients for now.
	UPDATE_SPECIFIC_INDEX_ONLY     bool //Uses list of queries to know which indexes to update

	//UpdsNames = [...]string{"orders", "lineitem", "delete"}
	//Orders and delete follow SF, except for SF = 0.01. It's filled automatically in tpchClient.go
	//updEntries          []int
	collectUpdStats     = false //Becomes true when enough time has passed to collect statistics again
	updStats            = make([]UpdatesStats, 0, 100)
	currUpdStats        = UpdatesStats{}
	nFinishedUpdComms   = int64(0)
	collectUpdStatsComm = []bool{false, false, false, false, false}
	updStatsComm        = make([][]UpdatesStats, 5)
	indexesToUpd        []int //Each position corresponds to the number of a query

	updsFinishChan = make(chan bool, UPDATES_GOROUTINES)
)

func splitUpdatesPerRoutineAndRegionHelper(ordersUpds [][]string, lineItemUpds [][]string, deleteKeys []string, itemSizesPerOrder []int) (
	tableInfos []TableInfo, routineOrders, routineItems [][][]string, routineDelete [][]string, routineLineSizes [][]int, regionForClient []int) {
	if N_UPD_CLIENTS == 0 {
		return splitUpdatesPerRoutineAndRegion(TEST_ROUTINES, ordersUpds, lineItemUpds, deleteKeys, itemSizesPerOrder)
	}
	tableInfos, routineOrders, routineItems, routineDelete, routineLineSizes, regionForClient = splitUpdatesPerRoutineAndRegion(N_UPD_CLIENTS, ordersUpds, lineItemUpds, deleteKeys, itemSizesPerOrder)
	newTableInfos := make([]TableInfo, TEST_ROUTINES)
	for i, ti := range tableInfos {
		newTableInfos[i] = ti
	}
	for i := len(tableInfos); i < len(newTableInfos); i++ {
		newTableInfos[i] = TableInfo{Tables: tpchData.Tables.GetShallowCopy()}
		newTableInfos[i].SetOrderIndexFunToUpdates()
	}
	tableInfos = newTableInfos
	return
}

// Check if updating lineitems really needs to send to two regions - done, it does
// Also, check some TODOs - some suggest possible errors. And check if anything is wrong with the counting - First part is done and fixed. Didn't check count.
// Splits per nOrders and region. Note that lineItemSizes []int is per order here
// If there's more regions than clients, each client will have multiple regions, but sorted by region
// E.g: 2 clients, 5 regions: first client will have R1, R2, first half of R5; second client will have R3, R4, second half of R5
// Note: no need to set lastDeletedPos: that's only used by legacy code on clientUpdates.go
// regionForClient: first region of each client
func splitUpdatesPerRoutineAndRegion(routines int, ordersUpds [][]string, lineItemUpds [][]string, deleteKeys []string, lineItemSizes []int) (
	tableInfos []TableInfo, routineOrders, routineItems [][][]string, routineDelete [][]string, routineLineSizes [][]int, regionForClient []int) {

	fmt.Printf("[CU]Sizes of initial data: orders %d, items %d, deletes %d, sizes %d\n", len(ordersUpds), len(lineItemUpds), len(deleteKeys), len(lineItemSizes))
	//fmt.Println(deleteKeys)

	nRegions := 5
	//First, group by regions
	ordersPerRegion, itemsPerRegion, deletesPerRegion, lineSizesPerRegion := make([][][]string, nRegions), make([][][]string, nRegions),
		make([][]string, nRegions), make([][]int, nRegions)
	for i := 0; i < nRegions; i++ {
		ordersPerRegion[i], itemsPerRegion[i] = make([][]string, 0, len(ordersUpds)/(nRegions-1)), make([][]string, 0, len(lineItemUpds)/(nRegions-1))
		deletesPerRegion[i], lineSizesPerRegion[i] = make([]string, 0, len(deleteKeys)/(nRegions-1)), make([]int, 0, len(lineItemSizes)/(nRegions-1))
	}

	currStartItem := 0
	//Tables has the base data processed... can go consult there where this order should belong to
	for i, order := range ordersUpds {
		custKey, _ := strconv.ParseInt(order[O_CUSTKEY], 10, 64)
		region := tpchData.Tables.CustkeyToRegionkey(custKey)
		nItems := lineItemSizes[i]
		ordersPerRegion[region] = append(ordersPerRegion[region], order)
		itemsPerRegion[region] = append(itemsPerRegion[region], lineItemUpds[currStartItem:currStartItem+nItems]...)
		deletesPerRegion[region] = append(deletesPerRegion[region], deleteKeys[i])
		lineSizesPerRegion[region] = append(lineSizesPerRegion[region], nItems)
		currStartItem += nItems
	}

	/*
		for i := 0; i < nRegions; i++ {
			fmt.Printf("[CU]Sizes of region %d: orders %d; items %d; deletes %d; sizes %d\n", i,
				len(ordersPerRegion[i]), len(itemsPerRegion[i]), len(deletesPerRegion[i]), len(lineSizesPerRegion[i]))
		}
	*/

	//Preparations for routine splitting
	ordersPerRoutine, remaining := len(ordersUpds)/routines, len(ordersUpds)%routines
	routineOrders, routineItems, routineDelete, routineLineSizes, regionForClient = make([][][]string, routines), make([][][]string, routines),
		make([][]string, routines), make([][]int, routines), make([]int, routines)
	tableInfos, currTableInfo := make([]TableInfo, routines), TableInfo{}
	currRegion := 0
	if NON_RANDOM_SERVERS {
		//Make clients start in different regions. Will help for tests with very few clients
		currRegion, _ = strconv.Atoi(id)
		currRegion = currRegion % nRegions
	}
	currOrders, currItems, currDeletes, currSizes := ordersPerRegion[currRegion], itemsPerRegion[currRegion], deletesPerRegion[currRegion], lineSizesPerRegion[currRegion]
	orderStart, lineStart, orderFinish, lineFinish, j, leftForRoutine := 0, 0, 0, 0, 0, 0
	//leftForRoutine: used to store how many orders from the next region the client needs

	//Splitting by routines
	for i := 0; i < routines; i++ {
		regionForClient[i] = currRegion
		orderFinish += ordersPerRoutine
		//fmt.Printf("[CU]OrderFinish %d, ordersPerRoutine %d\n", orderFinish, ordersPerRoutine)
		if i < remaining {
			//Extra order
			//fmt.Println("[CU]Adding remaining")
			orderFinish++
		}
		if orderFinish > len(currOrders) {
			//fmt.Printf("[CU]Too many orders. OrderStart %d, OrderFinish %d, region orders %d\n", orderStart, orderFinish, len(currOrders))
			leftForRoutine = orderFinish - len(currOrders)
			orderFinish = len(currOrders)
			//fmt.Printf("[CU]Set new leftForRoutine at %d, orderFinish %d (region orders: %d)\n", leftForRoutine, orderFinish, len(currOrders))
			if leftForRoutine > ordersPerRoutine && routines > 2 {
				//Majority region is the next one
				regionForClient[i] = (currRegion + 1) % nRegions
			}
		}
		for j = orderStart; j < orderFinish; j++ {
			lineFinish += currSizes[j]
		}
		//fmt.Printf("[CU]Finished adding lineitemsizes. LineStart %d, lineFinish %d\n", lineStart, lineFinish)
		routineOrders[i], routineItems[i], routineDelete[i], routineLineSizes[i] = make([][]string, orderFinish-orderStart),
			make([][]string, lineFinish-lineStart), make([]string, orderFinish-orderStart), make([]int, orderFinish-orderStart)
		copy(routineOrders[i], currOrders[orderStart:orderFinish])
		copy(routineItems[i], currItems[lineStart:lineFinish])
		copy(routineDelete[i], currDeletes[orderStart:orderFinish])
		copy(routineLineSizes[i], currSizes[orderStart:orderFinish])
		/*fmt.Printf("[CU]Routine sizes after copy: orders %d, items %d, deletes %d, sizes %d\n",
			len(routineOrders[i]), len(routineItems[i]), len(routineDelete[i]), len(routineLineSizes[i]))
		fmt.Printf("[CU]Actual entries copied: orders %d, items %d, deletes %d, sizes %d\n", nOrderCopy, nItemCopy, nDeleteCopy, nSizesCopy)
		fmt.Printf("[CU]Last order/item: %v (%v)\n", routineOrders[i][len(routineOrders[i])-1], routineItems[i][len(routineItems[i])-1])*/
		//routineOrders[i], routineItems[i], routineDelete[i], routineLineSizes[i] = currOrders[orderStart:orderFinish], currItems[lineStart:lineFinish],
		//	currDeletes[orderStart:orderFinish], currSizes[orderStart:orderFinish]
		orderStart, lineStart = orderFinish, lineFinish

		if orderFinish == len(currOrders) {
			//fmt.Println("[CU]Changing to next region")
			//Next region
			currRegion = (currRegion + 1) % nRegions
			currOrders, currItems, currDeletes, currSizes = ordersPerRegion[currRegion], itemsPerRegion[currRegion], deletesPerRegion[currRegion], lineSizesPerRegion[currRegion]
			orderStart, orderFinish, lineStart, lineFinish, j = 0, 0, 0, 0, 0
		}

		/*
			fmt.Println("[CU]First region orders+items")
			currLine := 0
			for i, order := range routineOrders[0] {
				nItems := routineLineSizes[0][i]
				var buf strings.Builder
				buf.WriteString(fmt.Sprintf("[%d] %v (", nItems, order[O_ORDERKEY]))
				for j := 0; j < nItems; j++ {
					buf.WriteString(fmt.Sprintf("%v, ", routineItems[0][currLine][L_ORDERKEY]))
					currLine++
				}
				buf.WriteString(")")
				fmt.Println(buf.String())
			}
		*/

		//for tries := 0; leftForRoutine > 0 && tries < 10; tries++ {
		//With 1 or 2 clients, this may happen more than once (or with more than 5 regions)
		for leftForRoutine > 0 {
			//fmt.Println("[CU]Still have remaining orders:", leftForRoutine)
			orderFinish += leftForRoutine
			if orderFinish > len(currOrders) {
				leftForRoutine = orderFinish - len(currOrders)
				orderFinish = len(currOrders)
			} else {
				leftForRoutine = 0
			}
			for j = orderStart; j < orderFinish; j++ {
				lineFinish += currSizes[j]
			}
			routineOrders[i], routineItems[i], routineDelete[i], routineLineSizes[i] = append(routineOrders[i], currOrders[orderStart:orderFinish]...),
				append(routineItems[i], currItems[lineStart:lineFinish]...), append(routineDelete[i], currDeletes[orderStart:orderFinish]...),
				append(routineLineSizes[i], currSizes[orderStart:orderFinish]...)

			orderStart, lineStart = orderFinish, lineFinish

			if orderFinish == len(currOrders) {
				//Next region
				currRegion = (currRegion + 1) % nRegions
				currOrders, currItems, currDeletes, currSizes = ordersPerRegion[currRegion], itemsPerRegion[currRegion], deletesPerRegion[currRegion], lineSizesPerRegion[currRegion]
				orderStart, orderFinish, lineStart, lineFinish, j = 0, 0, 0, 0, 0
			}

			/*
				if tries == 9 {
					fmt.Println("[CU]Giving up for excessive tries.")
				}
			*/
		}

		currTableInfo = TableInfo{Tables: tpchData.Tables.GetShallowCopy()}
		currTableInfo.SetOrderIndexFunToUpdates()
		tableInfos[i] = currTableInfo
	}

	//Now that there's multiple routines doing updates, the q15Map/q15LocalMap must be filled for ALL possible combinations.
	if UPDATE_INDEX {
		if isIndexGlobal {
			completeFillQ15Map(q15Map)
		} else {
			for _, q15SubMap := range q15LocalMap {
				completeFillQ15Map(q15SubMap)
			}
		}
	}

	/*
		for i := 0; i < routines; i++ {
			regionsFound := make([]bool, 5)
			for _, order := range routineOrders[i] {
				custKey, _ := strconv.ParseInt(order[O_CUSTKEY], 10, 64)
				region := tpchData.Tables.CustkeyToRegionkey(custKey)
				regionsFound[region] = true
			}
			nRegionsFound := 0
			fmt.Printf("[CU]Client %d has the following regions:", i)
			for j, found := range regionsFound {
				if found {
					fmt.Printf("%d, ", j)
					nRegionsFound++
				}
			}
			fmt.Println()
			fmt.Printf("[CU]Client %d has %d different regions for its orders.\n", i, nRegionsFound)
			fmt.Printf("[CU]Client %d has the following sizes: orders %d lineitems %d deletes %d sizes %d\n",
				i, len(routineOrders[i]), len(routineItems[i]), len(routineDelete[i]), len(routineLineSizes[i]))
		}
	*/

	return

	//Let's do two cases:
	//Case a) less than 5
	//Case b) more than 5

	/*
		if N_ROUTINES < nRegions {
			//In this case, each goroutine gets a region, and then there's leftovers
			//So this means up to two regions per client
		} else {
			//Here I could:
			//a) attribute most of a region to each server, then leftovers to the remaining client (bad, one client gets 5 connections)
			//b) distribute in such a way each client gets mostly one region and a bit of another (two per client)
			//So both b) and N_ROUTINES < nRegions are the same...
			//Basically, much simpler solution:
			//see all the regions as a very big array. Split that array by N_ROUTINES. Attribute slices
			//with this, each client gets at most two regions, some even one. And best, they will only contact one at a time instead of "hot swapping"

		}
	*/
}

// Instead of splitting per file, splits per nOrders. Works as long as routines < len(ordersUpds)
// Note: here lineItemSizes is per order, not per file.
func newSplitUpdatesPerRoutine(routines int, ordersUpds [][]string, lineItemUpds [][]string, deleteKeys []string, lineItemSizes []int) (
	tableInfos []TableInfo, routineOrders, routineItems [][][]string, routineDelete [][]string, routineLineSizes [][]int) {
	//First clients have an extra order until "remaining"
	ordersPerRoutine, remaining := len(ordersUpds)/routines, len(ordersUpds)%routines
	routineOrders, routineItems, routineDelete, routineLineSizes = make([][][]string, routines), make([][][]string, routines),
		make([][]string, routines), make([][]int, routines)
	tableInfos, currTableInfo := make([]TableInfo, routines), TableInfo{}
	orderStart, lineStart, orderFinish, lineFinish, j := 0, 0, 0, 0, 0

	for i := 0; i < routines; i++ {
		orderFinish += ordersPerRoutine
		if i < remaining {
			//Extra order
			orderFinish++
		}
		for j = orderStart; j < orderFinish; j++ {
			lineFinish += lineItemSizes[j]
		}
		routineOrders[i], routineItems[i], routineDelete[i], routineLineSizes[i] = ordersUpds[orderStart:orderFinish], lineItemUpds[lineStart:lineFinish],
			deleteKeys[orderStart:orderFinish], lineItemSizes[orderStart:orderFinish]

		currTableInfo = TableInfo{Tables: tpchData.Tables.GetShallowCopy()}
		//Need to update last deleted index...
		currTableInfo.LastDeletedPos = orderStart
		currTableInfo.SetOrderIndexFunToUpdates()
		tableInfos[i] = currTableInfo
		orderStart, lineStart = orderFinish, lineFinish
	}

	//Fix lastDeletePos of first routine (orders start at index 1)
	tableInfos[0].LastDeletedPos = 1

	//Now that there's multiple routines doing updates, the q15Map/q15LocalMap must be filled for ALL possible combinations.
	if UPDATE_INDEX {
		if isIndexGlobal {
			completeFillQ15Map(q15Map)
		} else {
			for _, q15SubMap := range q15LocalMap {
				completeFillQ15Map(q15SubMap)
			}
		}
	}

	return
}

//TODO: removes/updates for individual objects (i.e., option in which each customer has its own CRDT)?

// Pre-condition: routines >= N_UPDATE_FILES. Also, distribution isn't much fair if routines is close to N_UPDATE_FILES (ideally, should be at most 1/4)
// Note: lineItemSizes is indexed per file, not order.
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
		orderFinish += (j - previousJ) * (tpch.UpdEntries[0] + 1)
		//orderFinish += ordersPerRoutine
		routineOrders[i], routineItems[i], routineDelete[i], routineLineSizes[i] = ordersUpds[orderStart:orderFinish], lineItemUpds[lineStart:lineFinish],
			deleteKeys[orderStart:orderFinish], lineItemSizes[previousJ:j]

		currTableInfo = TableInfo{Tables: tpchData.Tables.GetShallowCopy()}
		//Need to update last deleted index...
		currTableInfo.LastDeletedPos = orderStart
		currTableInfo.SetOrderIndexFunToUpdates()
		tableInfos[i] = currTableInfo

		orderStart, lineStart, previousJ = orderFinish, lineFinish, j
	}
	//Leftovers go to the last goroutine. C'est la vie.
	currTableInfo = TableInfo{Tables: tpchData.Tables.GetShallowCopy()}
	currTableInfo.LastDeletedPos = orderStart
	currTableInfo.SetOrderIndexFunToUpdates()
	routineOrders[routines-1], routineItems[routines-1] = ordersUpds[orderStart:], lineItemUpds[lineStart:]
	routineDelete[routines-1], routineLineSizes[routines-1] = deleteKeys[orderStart:], lineItemSizes[previousJ:]
	tableInfos[routines-1] = currTableInfo

	//Fix lastDeletePos of first routine (orders start at index 1)
	tableInfos[0].LastDeletedPos = 1

	//Now that there's multiple routines doing updates, the q15Map/q15LocalMap must be filled for ALL possible combinations.
	if UPDATE_INDEX {
		if isIndexGlobal {
			completeFillQ15Map(q15Map)
		} else {
			for _, q15SubMap := range q15LocalMap {
				completeFillQ15Map(q15SubMap)
			}
		}
	}

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
				orderF, lineF = orderF+tpch.UpdEntries[0]+1, lineF+itemSizes[j]
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

func completeFillQ15Map(q15MapToUse map[int16]map[int8]map[int32]*float64) {
	var mMap map[int8]map[int32]*float64
	var suppMap map[int32]*float64
	has := false
	for year := int16(1993); year <= 1997; year++ {
		mMap = q15MapToUse[year]
		for month := int8(1); month < 12; month += 3 {
			suppMap = mMap[month]
			for suppID := int32(1); suppID < int32(len(tpchData.Tables.Suppliers)); suppID++ {
				_, has = suppMap[suppID]
				if !has {
					suppMap[suppID] = new(float64)
				}
			}
		}
	}
}

func readUpds() (ordersUpds [][]string, lineItemUpds [][]string, deleteKeys []string, lineItemSizes []int) {
	updPartsRead := [][]int8{tpchData.ToRead[tpch.ORDERS], tpchData.ToRead[tpch.LINEITEM]}
	ordersUpds, lineItemUpds, deleteKeys, lineItemSizes, N_UPDATE_FILES = tpch.ReadUpdates(updCompleteFilename[:], tpch.UpdEntries[:], tpch.UpdParts[:], updPartsRead, N_UPDATE_FILES)
	return
}

func (ti TableInfo) sendDataChangesV2(ordersUpds [][]string, lineItemUpds [][]string, deleteKeys []string) {
	//Maybe I can do like this:
	//1st - deletes
	//2nd - updates
	//3rd - indexes
	//Works as long as it is in the same txn or in the same staticUpdateObjs
	//I actually can't do everything in the same transaction as it involves different servers...
	//But I should still guarantee that, for the same server, it's atomic, albeit index results will still be screwed.
	var deletedOrders []*tpch.Orders
	var deletedItems [][]*tpch.LineItem
	deletedOrders, deletedItems = ti.sendDeletes(deleteKeys)
	//deletedOrders, deletedItems := getDeleteClientTables(deleteKeys)
	ti.sendUpdates(ordersUpds, lineItemUpds)
	if UPDATE_INDEX {
		ti.sendIndexUpdates(deleteKeys, ordersUpds, lineItemUpds, deletedOrders, deletedItems)
	}
	//ignore(deletedOrders, deletedItems)

	return
}

func (ti TableInfo) sendDeletes(deleteKeys []string) (orders []*tpch.Orders, items [][]*tpch.LineItem) {
	//updsDone := 0
	//start := time.Now().UnixNano()
	orders, items = ti.getDeleteClientTables(deleteKeys)
	//orders, items = orders[1:], items[1:] //Hide initial entry which is always empty
	nDeleteItems := getNumberDeleteItems(items)
	ordersPerServer, itemsPerServer, orderIndex, itemsIndex := makePartionedDeleteStructs(deleteKeys, nDeleteItems)

	var order *tpch.Orders
	var orderItems []*tpch.LineItem
	var custRegion, suppRegion int8
	var itemS string

	//fmt.Println("[DELETE]Last order, item:", tpch.Orders[len(orders)-1], *items[len(orders)-1][len(items[len(orders)-1])-1])

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

	var orderParams, itemParams crdt.UpdateObjectParams
	if !CRDT_PER_OBJ {
		for i, orderKeys := range ordersPerServer {
			orderParams = getDeleteParams(tpch.TableNames[tpch.ORDERS], buckets[i], orderKeys, orderIndex[i])
			itemParams = getDeleteParams(tpch.TableNames[tpch.LINEITEM], buckets[i], itemsPerServer[i], itemsIndex[i])
			/*
				channels.dataChans[i] <- QueuedMsg{
					code:    antidote.StaticUpdateObjs,
					Message: antidote.CreateStaticUpdateObjs(nil, []crdt.UpdateObjectParams{orderParams, itemParams}),
				}
				updsDone += orderIndex[i] + itemsIndex[i]
			*/
			queueStatMsg(i, []crdt.UpdateObjectParams{orderParams, itemParams}, orderIndex[i]+itemsIndex[i], REMOVE_TYPE)
		}
	} else {
		for i, orderKeys := range ordersPerServer {
			//upds := make([]crdt.UpdateObjectParams, len(orderKeys)+len(itemsPerServer[i]))
			upds := make([]crdt.UpdateObjectParams, orderIndex[i]+itemsIndex[i])
			written := getPerObjDeleteParams(tpch.TableNames[tpch.ORDERS], buckets[i], orderKeys, orderIndex[i], upds, 0)
			getPerObjDeleteParams(tpch.TableNames[tpch.LINEITEM], buckets[i], itemsPerServer[i], itemsIndex[i], upds, written)
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

func (ti TableInfo) getDeleteClientTables(deleteKeys []string) (orders []*tpch.Orders, items [][]*tpch.LineItem) {
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

func getDeleteParams(tableName string, bkt string, keys []string, nKeys int) crdt.UpdateObjectParams {
	return crdt.UpdateObjectParams{KeyParams: crdt.MakeKeyParams(tableName, proto.CRDTType_RRMAP, bkt), UpdateArgs: crdt.MapRemoveAll{Keys: keys[:nKeys]}}
}

// TODO: DeleteAll?
func getPerObjDeleteParams(tableName string, bkt string, keys []string, nKeys int,
	buffer []crdt.UpdateObjectParams, bufI int) (newBufI int) {
	for _, key := range keys {
		buffer[bufI] = crdt.UpdateObjectParams{KeyParams: crdt.MakeKeyParams(tableName+key, proto.CRDTType_RRMAP, bkt), UpdateArgs: crdt.ResetOp{}}
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

func getNumberDeleteItems(items [][]*tpch.LineItem) (nItems int) {
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
	var upd crdt.EmbMapUpdateAll
	orderFunc, itemFunc := regionFuncs[tpch.ORDERS], multiRegionFunc[tpch.LINEITEM]
	var itemRegions []int8

	for _, order := range ordersUpds {
		key, upd = tpch.GetInnerMapEntry(tpchData.Headers[tpch.ORDERS], tpchData.Keys[tpch.ORDERS], order, tpchData.ToRead[tpch.ORDERS])
		key = getEntryKey(tpch.TableNames[tpch.ORDERS], key)
		ordersPerServer[orderFunc(order)][key] = upd
	}
	for _, item := range lineItemUpds {
		key, upd = tpch.GetInnerMapEntry(tpchData.Headers[tpch.LINEITEM], tpchData.Keys[tpch.LINEITEM], item, tpchData.ToRead[tpch.LINEITEM])
		key = getEntryKey(tpch.TableNames[tpch.LINEITEM], key)
		itemRegions = itemFunc(item)
		for _, region := range itemRegions {
			itemsPerServer[region][key] = upd
		}
	}

	var currUpdParams []crdt.UpdateObjectParams
	for i := 0; i < len(ti.Tables.Regions); i++ {
		//This could be in a single msg
		currUpdParams = make([]crdt.UpdateObjectParams,
			getUpdParamsSize([]map[string]crdt.UpdateArguments{ordersPerServer[i], itemsPerServer[i]}))
		written := getDataUpdateParamsWithBuf(ordersPerServer[i], tpch.TableNames[tpch.ORDERS], buckets[i], currUpdParams, 0)
		getDataUpdateParamsWithBuf(itemsPerServer[i], tpch.TableNames[tpch.LINEITEM], buckets[i], currUpdParams, written)
		queueStatMsg(i, currUpdParams, len(ordersPerServer[i])+len(itemsPerServer[i]), NEW_TYPE)
		/*
			//orders
			currUpdParams = getDataUpdateParams(ordersPerServer[i], TableNames[tpch.ORDERS], buckets[i])
			channels.updateChans[i] <- QueuedMsgWithStat{QueuedMsg: QueuedMsg{
				code:    antidote.StaticUpdateObjs,
				Message: antidote.CreateStaticUpdateObjs(nil, currUpdParams)},
				nData:    len(ordersPerServer[i]),
				dataType: NEW_TYPE,
			}
			//lineitems
			currUpdParams = getDataUpdateParams(itemsPerServer[i], TableNames[tpch.LINEITEM], buckets[i])
			channels.updateChans[i] <- QueuedMsgWithStat{QueuedMsg: QueuedMsg{
				code:    antidote.StaticUpdateObjs,
				Message: antidote.CreateStaticUpdateObjs(nil, currUpdParams)},
				nData:    len(itemsPerServer[i]),
				dataType: NEW_TYPE,
			}
		*/

		/*
			queueDataProto(ordersPerServer[i], TableNames[tpch.ORDERS], buckets[i], channels.dataChans[i])
			queueDataProto(itemsPerServer[i], TableNames[tpch.LINEITEM], buckets[i], channels.dataChans[i])
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
	buf []crdt.UpdateObjectParams, bufI int) (written int) {
	if CRDT_PER_OBJ {
		for key, upd := range currMap {
			buf[bufI] = crdt.UpdateObjectParams{KeyParams: crdt.MakeKeyParams(key, proto.CRDTType_RRMAP, bucket), UpdateArgs: upd}
			bufI++
		}
	} else {
		buf[bufI] = crdt.UpdateObjectParams{KeyParams: crdt.MakeKeyParams(name, proto.CRDTType_RRMAP, bucket), UpdateArgs: crdt.EmbMapUpdateAll{Upds: currMap}}
		bufI++
	}
	return bufI
}

func (ti TableInfo) sendIndexUpdates(deleteKeys []string, ordersUpds [][]string, lineItemUpds [][]string,
	remOrders []*tpch.Orders, remItems [][]*tpch.LineItem) {
	if UPDATE_SPECIFIC_INDEX_ONLY {
		if isIndexGlobal {
			ti.sendPartGlobalIndexUpdates(deleteKeys, ordersUpds, lineItemUpds, remOrders, remItems)
		} else {
			ti.sendPartLocalIndexUpdates(deleteKeys, ordersUpds, lineItemUpds, remOrders, remItems)
		}
	} else {
		if isIndexGlobal {
			ti.sendGlobalIndexUpdates(deleteKeys, ordersUpds, lineItemUpds, remOrders, remItems)
		} else {
			ti.sendLocalIndexUpdates(deleteKeys, ordersUpds, lineItemUpds, remOrders, remItems)
		}
	}
}

func (ti TableInfo) sendPartGlobalIndexUpdates(deleteKeys []string, ordersUpds [][]string, lineItemUpds [][]string,
	remOrders []*tpch.Orders, remItems [][]*tpch.LineItem) {

	indexUpds, indexNUpds := make([][]crdt.UpdateObjectParams, 7), make([]int, 7)
	ti.Tables.UpdateOrderLineitems(ordersUpds, lineItemUpds)
	newOrders, newItems := ti.Tables.LastAddedOrders, ti.Tables.LastAddedLineItems
	i, j := 0, 0
	//Query 11 doesn't need updates (Nation/Supply only, which are never updated.)

	if indexesToUpd[i] == 3 {
		indexUpds[0], indexUpds[1], indexNUpds[0], indexNUpds[1] = ti.getQ3UpdsV2(remOrders, remItems, newOrders, newItems)
		i++
		j = 2
	}
	if indexesToUpd[i] == 5 {
		indexUpds[j], _, indexNUpds[j] = ti.getQ5UpdsV2(remOrders, remItems, newOrders, newItems)
		i++
		j++
	}
	if indexesToUpd[i] == 11 {
		i++
	}
	if indexesToUpd[i] == 14 {
		indexUpds[j], indexNUpds[j] = ti.getQ14UpdsV2(remOrders, remItems, newOrders, newItems)
		i++
		j++
	}
	if indexesToUpd[i] == 15 {
		if !useTopSum {
			indexUpds[j], indexNUpds[j] = ti.getQ15UpdsV2(remOrders, remItems, newOrders, newItems)
		} else {
			indexUpds[j], indexNUpds[j] = ti.getQ15TopSumUpdsV2(remOrders, remItems, newOrders, newItems)
		}
		i++
		j++
	}
	if indexesToUpd[i] == 18 {
		indexUpds[j], indexUpds[j+1], indexNUpds[j], indexNUpds[j+1] = ti.getQ18UpdsV2(remOrders, remItems, newOrders, newItems)
		i++
		j += 2
	}

	for k := 0; k < j; j++ {
		queueStatMsg(rand.Intn(len(channels.indexChans)), indexUpds[k], indexNUpds[k], INDEX_TYPE)
	}
}

func (ti TableInfo) sendPartLocalIndexUpdates(deleteKeys []string, ordersUpds [][]string, lineItemUpds [][]string,
	remOrders []*tpch.Orders, remItems [][]*tpch.LineItem) {

	indexUpds, indexNUpds := make([][][]crdt.UpdateObjectParams, 7), make([]int, 7)
	ti.Tables.UpdateOrderLineitems(ordersUpds, lineItemUpds)
	newOrders, newItems := ti.Tables.LastAddedOrders, ti.Tables.LastAddedLineItems
	i, j := 0, 0
	//Query 11 doesn't need updates (Nation/Supply only, which are never updated.)

	if indexesToUpd[i] == 3 {
		indexUpds[0], indexUpds[1], indexNUpds[0], indexNUpds[1] = ti.getQ3UpdsLocalV2(remOrders, remItems, newOrders, newItems)
		i++
		j = 2
	}
	if indexesToUpd[i] == 5 {
		_, indexUpds[j], indexNUpds[j] = ti.getQ5UpdsV2(remOrders, remItems, newOrders, newItems)
		i++
		j++
	}
	if indexesToUpd[i] == 11 {
		i++
	}
	if indexesToUpd[i] == 14 {
		indexUpds[j], indexNUpds[j] = ti.getQ14UpdsLocalV2(remOrders, remItems, newOrders, newItems)
		i++
		j++
	}
	if indexesToUpd[i] == 15 {
		if useTopSum {
			indexUpds[j], indexNUpds[j] = ti.getQ15TopSumUpdsLocalV2(remOrders, remItems, newOrders, newItems)
		} else {
			indexUpds[j], indexNUpds[j] = ti.getQ15UpdsLocalV2(remOrders, remItems, newOrders, newItems)
		}
		i++
		j++
	}
	if indexesToUpd[i] == 18 {
		indexUpds[j], indexUpds[j+1], indexNUpds[j], indexNUpds[j+1] = ti.getQ18UpdsLocalV2(remOrders, remItems, newOrders, newItems)
		i++
		j += 2
	}

	for k := 0; k < j; j++ {
		for i, chanUpds := range indexUpds[k] {
			if len(chanUpds) > 0 {
				queueStatMsg(i, chanUpds, indexNUpds[k], INDEX_TYPE)
			}
		}
	}
}

func (ti TableInfo) sendGlobalIndexUpdates(deleteKeys []string, ordersUpds [][]string, lineItemUpds [][]string,
	remOrders []*tpch.Orders, remItems [][]*tpch.LineItem) {

	//start := time.Now().UnixNano()
	indexUpds := make([][]crdt.UpdateObjectParams, 7)
	indexNUpds := make([]int, 7)
	ti.Tables.UpdateOrderLineitems(ordersUpds, lineItemUpds)

	newOrders, newItems := ti.Tables.LastAddedOrders, ti.Tables.LastAddedLineItems
	//Query 11 doesn't need updates (Nation/Supply only, which are never updated.)

	indexUpds[6], indexNUpds[6] = ti.getQ14UpdsV2(remOrders, remItems, newOrders, newItems)
	if !useTopSum {
		indexUpds[0], indexNUpds[0] = ti.getQ15UpdsV2(remOrders, remItems, newOrders, newItems)
	} else {
		indexUpds[0], indexNUpds[0] = ti.getQ15TopSumUpdsV2(remOrders, remItems, newOrders, newItems)
	}
	indexUpds[1], _, indexNUpds[1] = ti.getQ5UpdsV2(remOrders, remItems, newOrders, newItems)
	indexUpds[2], indexUpds[3], indexNUpds[2], indexNUpds[3] = ti.getQ18UpdsV2(remOrders, remItems, newOrders, newItems)
	//Works but it's very slow! (added ~50s overhead instead of like 2-4s as each other did)
	indexUpds[4], indexUpds[5], indexNUpds[4], indexNUpds[5] = ti.getQ3UpdsV2(remOrders, remItems, newOrders, newItems)

	//for _, upds := range indexUpds {
	for i, upds := range indexUpds {
		//TODO: Smarter sending?
		/*
			channels.dataChans[rand.Intn(len(channels.indexChans))] <- QueuedMsg{code: antidote.StaticUpdateObjs,
				//channels.dataChans[0] <- QueuedMsg{code: antidote.StaticUpdateObjs,
				Message: antidote.CreateStaticUpdateObjs(nil, upds),
			}
		*/
		/*
			channels.updateChans[rand.Intn(len(channels.indexChans))] <- QueuedMsgWithStat{
				QueuedMsg: QueuedMsg{code: antidote.StaticUpdateObjs, Message: antidote.CreateStaticUpdateObjs(nil, upds)},
				nData:     indexNUpds[i],
				dataType:  INDEX_TYPE,
			}
		*/
		//TODO: This rand should be specific per goroutine
		//IndexChans is on purpose, to take in consideration the index split and SINGLE_INDEX_SERVER configs.
		queueStatMsg(rand.Intn(len(channels.indexChans)), upds, indexNUpds[i], INDEX_TYPE)
	}

	/*
		totalUpds := indexNUpds[0] + indexNUpds[1] + indexNUpds[2] + indexNUpds[3] + indexNUpds[4] + indexNUpds[5] + indexNUpds[6]
		finish := time.Now().UnixNano()
		currUpdStats.indexTimeSpent += finish - start
		currUpdStats.indexUpds += totalUpds
	*/
}

func (ti TableInfo) sendLocalIndexUpdates(deleteKeys []string, ordersUpds [][]string, lineItemUpds [][]string,
	remOrders []*tpch.Orders, remItems [][]*tpch.LineItem) {

	//start := time.Now().UnixNano()
	indexUpds := make([][][]crdt.UpdateObjectParams, 7)
	indexNUpds := make([]int, 7) //Less 2 as Q18 and Q3 have their deletes & upds grouped together
	ti.Tables.UpdateOrderLineitems(ordersUpds, lineItemUpds)

	newOrders, newItems := ti.Tables.LastAddedOrders, ti.Tables.LastAddedLineItems
	//Query 11 doesn't need updates (Nation/Supply only, which are never updated.)

	indexUpds[6], indexNUpds[6] = ti.getQ14UpdsLocalV2(remOrders, remItems, newOrders, newItems)
	if !useTopSum {
		indexUpds[0], indexNUpds[0] = ti.getQ15UpdsLocalV2(remOrders, remItems, newOrders, newItems)
	} else {
		indexUpds[0], indexNUpds[0] = ti.getQ15TopSumUpdsLocalV2(remOrders, remItems, newOrders, newItems)
	}
	_, indexUpds[1], indexNUpds[1] = ti.getQ5UpdsV2(remOrders, remItems, newOrders, newItems)
	indexUpds[2], indexUpds[3], indexNUpds[2], indexNUpds[3] = ti.getQ18UpdsLocalV2(remOrders, remItems, newOrders, newItems)
	indexUpds[4], indexUpds[5], indexNUpds[4], indexNUpds[5] = ti.getQ3UpdsLocalV2(remOrders, remItems, newOrders, newItems)

	for j, upds := range indexUpds {
		for i, chanUpds := range upds {
			if len(chanUpds) > 0 {
				/*
					channels.dataChans[i] <- QueuedMsg{code: antidote.StaticUpdateObjs,
						Message: antidote.CreateStaticUpdateObjs(nil, chanUpds),
					}
				*/
				/*
					channels.updateChans[i] <- QueuedMsgWithStat{
						QueuedMsg: QueuedMsg{code: antidote.StaticUpdateObjs, Message: antidote.CreateStaticUpdateObjs(nil, chanUpds)},
						nData:     indexNUpds[j],
						dataType:  INDEX_TYPE,
					}
				*/
				queueStatMsg(i, chanUpds, indexNUpds[j], INDEX_TYPE)
			}
		}
	}

	/*
		totalUpds := indexNUpds[0] + indexNUpds[1] + indexNUpds[2] + indexNUpds[3] + indexNUpds[4] + indexNUpds[5] + indexNUpds[6]
		finish := time.Now().UnixNano()
		currUpdStats.indexTimeSpent += finish - start
		currUpdStats.indexUpds += totalUpds
	*/
	//return indexNUpds[0] + indexNUpds[1] + indexNUpds[2] + indexNUpds[3] + indexNUpds[4] + indexNUpds[5] + indexNUpds[6]
}

func (ti TableInfo) getQ3UpdsLocalV2(remOrders []*tpch.Orders, remItems [][]*tpch.LineItem, newOrders []*tpch.Orders, newItems [][]*tpch.LineItem) (rems [][]crdt.UpdateObjectParams,
	upds [][]crdt.UpdateObjectParams, nRems int, nAdds int) {
	//Segment -> orderDate (day) -> orderKey
	remMap := make([]map[string]map[int8]map[int32]struct{}, len(ti.Tables.Regions))
	nUpds := make([]int, len(remMap))
	for i := range remMap {
		remMap[i] = createQ3DeleteMap()
	}

	for orderI, order := range remOrders {
		if order.O_ORDERDATE.IsLowerOrEqual(MAX_DATE_Q3) {
			ti.q3UpdsCalcHelper(remMap[ti.Tables.OrderkeyToRegionkey(order.O_ORDERKEY)], order, orderI)
		}
	}

	updsDonePart := 0
	for i := range nUpds {
		updsDonePart, nUpds[i] = getQ3NumberUpds(remMap[i])
		nRems += updsDonePart
	}
	rems = make([][]crdt.UpdateObjectParams, len(nUpds))
	for i := range rems {
		rems[i] = makeQ3IndexRemoves(remMap[i], nUpds[i], INDEX_BKT+i)
	}
	existingOrders, existingItems := ti.Tables.Orders, ti.Tables.LineItems
	ti.Tables.Orders, ti.Tables.LineItems = newOrders, newItems
	upds, nAdds = ti.prepareQ3IndexLocal()
	ti.Tables.Orders, ti.Tables.LineItems = existingOrders, existingItems
	return rems, upds, nRems, nAdds
}

func (ti TableInfo) getQ3UpdsV2(remOrders []*tpch.Orders, remItems [][]*tpch.LineItem, newOrders []*tpch.Orders, newItems [][]*tpch.LineItem) (rems []crdt.UpdateObjectParams,
	upds []crdt.UpdateObjectParams, nAdds int, nRems int) {
	//remItems can be ignored. As for remOrders, we'll need to find their segment and orderdate to know where to remove.
	//newOrders and newItems should be equal to clientIndex.
	//segment -> orderDate -> orderkey
	remMap := createQ3DeleteMap()

	for orderI, order := range remOrders {
		if order.O_ORDERDATE.IsLowerOrEqual(MAX_DATE_Q3) {
			ti.q3UpdsCalcHelper(remMap, order, orderI)
		}
	}

	nRems, nProtoUpds := getQ3NumberUpds(remMap)
	rems = makeQ3IndexRemoves(remMap, nProtoUpds, INDEX_BKT)
	existingOrders, existingItems := ti.Tables.Orders, ti.Tables.LineItems
	ti.Tables.Orders, ti.Tables.LineItems = newOrders, newItems
	upds, nAdds = ti.prepareQ3Index()
	ti.Tables.Orders, ti.Tables.LineItems = existingOrders, existingItems
	return rems, upds, nAdds, nRems
}

func (ti TableInfo) q3UpdsCalcHelper(remMap map[string]map[int8]map[int32]struct{}, order *tpch.Orders, orderI int) {
	minDay, j := MIN_MONTH_DAY, int8(0)
	segMap := remMap[ti.Tables.Customers[order.O_CUSTKEY].C_MKTSEGMENT]
	orderLineItems := ti.Tables.LineItems[orderI]

	for _, item := range orderLineItems {
		//Check if L_SHIPDATE is higher than minDate and, if it is, check month/year. If month/year > march 1995, then add to all entries. Otherwise, use day to know which entries.
		//Note: items in the same order may be shipped at different dates (day, month or even year)
		if item.L_SHIPDATE.IsHigherOrEqual(MIN_DATE_Q3) {
			if item.L_SHIPDATE.MONTH > 3 || item.L_SHIPDATE.YEAR > 1995 {
				//All days
				minDay = 1
			} else {
				minDay = item.L_SHIPDATE.DAY + 1
			}
			//Make a for from minDay to 31 to fill the map
			for j = minDay; j <= MAX_MONTH_DAY; j++ {
				segMap[j][order.O_ORDERKEY] = struct{}{}
			}
		}
	}
}

func getQ3NumberUpds(remMap map[string]map[int8]map[int32]struct{}) (nUpds, nProtoUpds int) {
	nUpds, nProtoUpds = 0, 0
	if !useTopKAll {
		for _, segMap := range remMap {
			for _, dayMap := range segMap {
				nUpds += len(dayMap)
			}
		}
		nProtoUpds = nUpds
	} else {
		for _, segMap := range remMap {
			for _, dayMap := range segMap {
				nUpds += len(dayMap)
			}
			nProtoUpds += len(segMap)
		}
	}
	return
}

// Segment -> orderDate (day) -> orderKey
func createQ3DeleteMap() (sumMap map[string]map[int8]map[int32]struct{}) {
	sumMap = make(map[string]map[int8]map[int32]struct{})
	var j int8
	for _, seg := range tpchData.Tables.Segments {
		segMap := make(map[int8]map[int32]struct{})
		//Days
		for j = 1; j <= 31; j++ {
			segMap[j] = make(map[int32]struct{})
		}
		sumMap[seg] = segMap
	}
	return
}

func (ti TableInfo) getQ5UpdsV2(remOrders []*tpch.Orders, remItems [][]*tpch.LineItem, newOrders []*tpch.Orders, newItems [][]*tpch.LineItem) (upds []crdt.UpdateObjectParams,
	multiUpds [][]crdt.UpdateObjectParams, updsDone int) {
	//Removes can be translated into Decrements, while additions can be translated into Increments.
	//Instead, we'll just do both together and use increments for all.
	//Remember that a negative increment is the same as a decrement

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

	var order *tpch.Orders
	var customer *tpch.Customer
	var supplier *tpch.Supplier
	var year int16
	var nationKey, regionKey int8
	var value *float64
	//Same processing for removed and new, just with opposite signals
	procOrders, procItems, multiplier := newOrders, newItems, 1.0
	for j := 0; j < 2; j++ {
		for i, orderItems := range procItems {
			order = procOrders[i]
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
						*value += multiplier * lineItem.L_EXTENDEDPRICE * (1 - lineItem.L_DISCOUNT)
					}
				}
			}
		}
		//Now, deletes
		procOrders, procItems, multiplier = remOrders, remItems, -1.0
	}

	for _, yearMap := range sumMap {
		for _, natMap := range yearMap {
			updsDone += len(natMap)
		}
	}

	if isIndexGlobal {
		return ti.makeQ5IndexUpds(sumMap, INDEX_BKT), nil, updsDone
	}
	//Create temporary maps with just one region, in order to receive the upds separatelly
	multiUpds = make([][]crdt.UpdateObjectParams, len(ti.Tables.Regions))
	for i, regMap := range sumMap {
		multiUpds[i] = ti.makeQ5IndexUpds(map[int8]map[int16]map[int8]*float64{i: regMap}, INDEX_BKT+int(i))
	}
	return nil, multiUpds, updsDone
}

func (ti TableInfo) getQ14UpdsLocalV2(remOrders []*tpch.Orders, remItems [][]*tpch.LineItem, newOrders []*tpch.Orders, newItems [][]*tpch.LineItem) (upds [][]crdt.UpdateObjectParams, updsDone int) {
	mapPromo, mapTotal := make([]map[string]*float64, len(ti.Tables.Regions)), make([]map[string]*float64, len(ti.Tables.Regions))
	inPromo := ti.Tables.PromoParts

	for i := range mapPromo {
		mapPromo[i], mapTotal[i] = createQ14Maps()
	}

	procItems, procOrders, multiplier, regionKey := newItems, newOrders, 1.0, int8(0)
	for i := 0; i < 2; i++ {
		for j, orderItems := range procItems {
			regionKey = ti.Tables.OrderkeyToRegionkey(procOrders[j].O_ORDERKEY)
			q14UpdsCalcHelper(multiplier, orderItems, mapPromo[regionKey], mapTotal[regionKey], inPromo)
		}
		procItems, procOrders, multiplier = remItems, remOrders, -1.0
	}

	upds = make([][]crdt.UpdateObjectParams, len(ti.Tables.Regions))
	for i := range upds {
		upds[i] = ti.makeQ14IndexUpds(mapPromo[i], mapTotal[i], INDEX_BKT+i)
	}
	//nRegions * years (1993-1997) * months (1-12)
	updsDone = len(ti.Tables.Regions) * 5 * 12
	return
}

func (ti TableInfo) getQ14UpdsV2(remOrders []*tpch.Orders, remItems [][]*tpch.LineItem, newOrders []*tpch.Orders, newItems [][]*tpch.LineItem) (upds []crdt.UpdateObjectParams, updsDone int) {
	//We can do the same trick as in Q5. Removes are AddMultipleValue with negatives, while news are with positives.
	//Should be ok to mix both.

	inPromo := ti.Tables.PromoParts
	mapPromo, mapTotal := createQ14Maps()

	//Going through lineitem and updating the totals
	procItems, multiplier := newItems, 1.0
	//Same processing for removed and new, just with inverted signals
	for i := 0; i < 2; i++ {
		for _, orderItems := range procItems {
			q14UpdsCalcHelper(multiplier, orderItems, mapPromo, mapTotal, inPromo)
		}
		procItems, multiplier = remItems, -1.0
	}

	//years*months
	return ti.makeQ14IndexUpds(mapPromo, mapTotal, INDEX_BKT), 5 * 12
}

func q14UpdsCalcHelper(multiplier float64, orderItems []*tpch.LineItem, mapPromo map[string]*float64, mapTotal map[string]*float64, inPromo map[int32]struct{}) {
	var year int16
	revenue := 0.0
	date := ""
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

func (ti TableInfo) getQ15TopSumUpdsLocalV2(remOrders []*tpch.Orders, remItems [][]*tpch.LineItem, newOrders []*tpch.Orders, newItems [][]*tpch.LineItem) (upds [][]crdt.UpdateObjectParams, updsDone int) {
	diffEntries := make([]map[int16]map[int8]map[int32]float64, len(ti.Tables.Regions))
	nUpds := make([]int, len(ti.Tables.Regions))
	rKey := int8(0)

	for i := range diffEntries {
		diffEntries[i] = createQ15TopSumEntriesMap()
	}

	for i, orderItems := range remItems {
		rKey = ti.Tables.OrderkeyToRegionkey(remOrders[i].O_ORDERKEY)
		q15TopSumUpdsRemCalcHelper(orderItems, q15LocalMap[rKey], diffEntries[rKey])
	}
	for i, orderItems := range newItems {
		rKey = ti.Tables.OrderkeyToRegionkey(newOrders[i].O_ORDERKEY)
		q15TopSumUpdsNewCalcHelper(orderItems, q15LocalMap[rKey], diffEntries[rKey])
	}

	for i := range nUpds {
		nUpds[i] = getQ15TopSumNumberUpds(diffEntries[i])
	}
	upds = make([][]crdt.UpdateObjectParams, len(ti.Tables.Regions))
	updsDoneRegion := 0
	for i := range upds {
		upds[i], updsDoneRegion = makeQ15TopSumIndexUpdsDeletes(q15LocalMap[i], diffEntries[i], nUpds[i], INDEX_BKT+i)
		updsDone += updsDoneRegion
	}

	return
}

func (ti TableInfo) getQ15UpdsLocalV2(remOrders []*tpch.Orders, remItems [][]*tpch.LineItem, newOrders []*tpch.Orders, newItems [][]*tpch.LineItem) (upds [][]crdt.UpdateObjectParams, updsDone int) {
	updEntries := make([]map[int16]map[int8]map[int32]struct{}, len(ti.Tables.Regions))
	nUpds := make([]int, len(ti.Tables.Regions))
	rKey := int8(0)

	for i := range updEntries {
		updEntries[i] = createQ15EntriesMap()
	}

	for i, orderItems := range remItems {
		rKey = ti.Tables.OrderkeyToRegionkey(remOrders[i].O_ORDERKEY)
		q15UpdsRemCalcHelper(orderItems, q15LocalMap[rKey], updEntries[rKey])
	}
	for i, orderItems := range newItems {
		rKey = ti.Tables.OrderkeyToRegionkey(newOrders[i].O_ORDERKEY)
		q15UpdsNewCalcHelper(orderItems, q15LocalMap[rKey], updEntries[rKey])
	}

	for i := range nUpds {
		nUpds[i] = getQ15NumberUpds(updEntries[i])
	}
	upds = make([][]crdt.UpdateObjectParams, len(ti.Tables.Regions))
	updsDoneRegion := 0
	for i := range upds {
		upds[i], updsDoneRegion = makeQ15IndexUpdsDeletes(q15LocalMap[i], updEntries[i], nUpds[i], INDEX_BKT+i)
		updsDone += updsDoneRegion
	}

	return
}

func (ti TableInfo) getQ15TopSumUpdsV2(remOrders []*tpch.Orders, remItems [][]*tpch.LineItem, newOrders []*tpch.Orders, newItems [][]*tpch.LineItem) (upds []crdt.UpdateObjectParams, updsDone int) {
	//Need to update totals in q15Map and mark which entries were updated
	//Also uses the same idea as in other queries than removes are the same as news but with inverted signals
	diffEntries := createQ15TopSumEntriesMap()

	for _, orderItems := range remItems {
		q15TopSumUpdsRemCalcHelper(orderItems, q15Map, diffEntries)
	}
	for _, orderItems := range newItems {
		q15TopSumUpdsNewCalcHelper(orderItems, q15Map, diffEntries)
	}

	return makeQ15TopSumIndexUpdsDeletes(q15Map, diffEntries, getQ15TopSumNumberUpds(diffEntries), INDEX_BKT)
}

func q15TopSumUpdsRemCalcHelper(orderItems []*tpch.LineItem, yearMap map[int16]map[int8]map[int32]*float64,
	diffEntries map[int16]map[int8]map[int32]float64) {
	year, month := int16(0), int8(0)
	value := 0.0
	for _, item := range orderItems {
		year = item.L_SHIPDATE.YEAR
		if year >= 1993 && year <= 1997 {
			month, value = ((item.L_SHIPDATE.MONTH-1)/3)*3+1, (item.L_EXTENDEDPRICE * (1.0 - item.L_DISCOUNT))
			*yearMap[year][month][item.L_SUPPKEY] -= value
			diffEntries[year][month][item.L_SUPPKEY] -= value
		}
	}
}

func q15TopSumUpdsNewCalcHelper(orderItems []*tpch.LineItem, yearMap map[int16]map[int8]map[int32]*float64,
	diffEntries map[int16]map[int8]map[int32]float64) {
	year, month := int16(0), int8(0)
	value := 0.0
	for _, item := range orderItems {
		year = item.L_SHIPDATE.YEAR
		if year >= 1993 && year <= 1997 {
			month, value = ((item.L_SHIPDATE.MONTH-1)/3)*3+1, (item.L_EXTENDEDPRICE * (1.0 - item.L_DISCOUNT))
			*yearMap[year][month][item.L_SUPPKEY] += value
			diffEntries[year][month][item.L_SUPPKEY] += value
		}
	}
}

func getQ15TopSumNumberUpds(diffEntries map[int16]map[int8]map[int32]float64) (nUpds int) {
	nUpds = 0
	//TODO: TopSumAll?
	for _, monthMap := range diffEntries {
		for _, suppMap := range monthMap {
			nUpds += len(suppMap)
		}
	}

	return
}

// For top-sum, we need to register the diff, so that we can inc/dec the correct amount. Map is per-client.
func createQ15TopSumEntriesMap() (diffEntries map[int16]map[int8]map[int32]float64) {
	diffEntries = make(map[int16]map[int8]map[int32]float64)
	var mMap map[int8]map[int32]float64
	var year int16 = 1993
	for ; year <= 1997; year++ {
		mMap = make(map[int8]map[int32]float64)
		mMap[1], mMap[4], mMap[7], mMap[10] = make(map[int32]float64), make(map[int32]float64), make(map[int32]float64), make(map[int32]float64)
		diffEntries[year] = mMap
	}
	return
}

func (ti TableInfo) getQ15UpdsV2(remOrders []*tpch.Orders, remItems [][]*tpch.LineItem, newOrders []*tpch.Orders, newItems [][]*tpch.LineItem) (upds []crdt.UpdateObjectParams, updsDone int) {
	//Need to update totals in q15Map and mark which entries were updated
	//Also uses the same idea as in other queries than removes are the same as news but with inverted signals
	updEntries := createQ15EntriesMap()

	for _, orderItems := range remItems {
		q15UpdsRemCalcHelper(orderItems, q15Map, updEntries)
	}
	for _, orderItems := range newItems {
		q15UpdsNewCalcHelper(orderItems, q15Map, updEntries)
	}

	return makeQ15IndexUpdsDeletes(q15Map, updEntries, getQ15NumberUpds(updEntries), INDEX_BKT)
}

func q15UpdsRemCalcHelper(orderItems []*tpch.LineItem, yearMap map[int16]map[int8]map[int32]*float64,
	updEntries map[int16]map[int8]map[int32]struct{}) {
	year, month := int16(0), int8(0)
	for _, item := range orderItems {
		year = item.L_SHIPDATE.YEAR
		if year >= 1993 && year <= 1997 {
			month = ((item.L_SHIPDATE.MONTH-1)/3)*3 + 1
			*yearMap[year][month][item.L_SUPPKEY] -= (item.L_EXTENDEDPRICE * (1.0 - item.L_DISCOUNT))
			updEntries[year][month][item.L_SUPPKEY] = struct{}{}
		}
	}
}

func q15UpdsNewCalcHelper(orderItems []*tpch.LineItem, yearMap map[int16]map[int8]map[int32]*float64,
	updEntries map[int16]map[int8]map[int32]struct{}) {
	year, month := int16(0), int8(0)
	for _, item := range orderItems {
		year = item.L_SHIPDATE.YEAR
		if year >= 1993 && year <= 1997 {
			month = ((item.L_SHIPDATE.MONTH-1)/3)*3 + 1
			*yearMap[year][month][item.L_SUPPKEY] += (item.L_EXTENDEDPRICE * (1.0 - item.L_DISCOUNT))
			updEntries[year][month][item.L_SUPPKEY] = struct{}{}
		}
	}
}

func getQ15NumberUpds(updEntries map[int16]map[int8]map[int32]struct{}) (nUpds int) {
	nUpds = 0
	if !useTopKAll {
		for _, monthMap := range updEntries {
			for _, suppMap := range monthMap {
				nUpds += len(suppMap)
			}
		}
	} else {
		for _, monthMap := range updEntries {
			nUpds += 2 * len(monthMap)
		}
	}
	return
}

func createQ15EntriesMap() (updEntries map[int16]map[int8]map[int32]struct{}) {
	updEntries = make(map[int16]map[int8]map[int32]struct{})
	var mMap map[int8]map[int32]struct{}
	var year int16 = 1993
	for ; year <= 1997; year++ {
		mMap = make(map[int8]map[int32]struct{})
		mMap[1], mMap[4], mMap[7], mMap[10] = make(map[int32]struct{}), make(map[int32]struct{}), make(map[int32]struct{}), make(map[int32]struct{})
		updEntries[year] = mMap
	}
	return
}
func createQ18DeleteMap() (toRemove []map[int32]struct{}) {
	toRemove = make([]map[int32]struct{}, 4)
	for i := range toRemove {
		toRemove[i] = make(map[int32]struct{})
	}
	return
}

func (ti TableInfo) getQ18UpdsLocalV2(remOrders []*tpch.Orders, remItems [][]*tpch.LineItem, newOrders []*tpch.Orders,
	newItems [][]*tpch.LineItem) (rems [][]crdt.UpdateObjectParams, upds [][]crdt.UpdateObjectParams, nAddsI int, nRemsI int) {

	toRemove := make([][]map[int32]struct{}, len(ti.Tables.Regions))
	nRems := make([]int, len(ti.Tables.Regions))
	rKey, orderKey := int8(0), int32(0)
	for i := range toRemove {
		toRemove[i] = createQ18DeleteMap()
	}

	nRemsOrder := 0
	for i, order := range remOrders {
		orderKey = order.O_ORDERKEY
		rKey = ti.Tables.OrderkeyToRegionkey(orderKey)
		nRemsOrder = q18UpdsCalcHelper(toRemove[rKey], remItems[i], orderKey)
		nRems[rKey] += nRemsOrder
		nRemsI += nRemsOrder
	}

	if useTopKAll {
		for i := range nRems {
			nRems[i] = 0
			for _, orderMap := range toRemove[i] {
				nRems[i] += len(orderMap)
			}
		}
	}

	rems = make([][]crdt.UpdateObjectParams, len(nRems))
	for i := range rems {
		rems[i] = makeQ18IndexRemoves(toRemove[i], nRems[i], INDEX_BKT+i)
	}
	existingOrders, existingItems := ti.Tables.Orders, ti.Tables.LineItems
	ti.Tables.Orders, ti.Tables.LineItems = newOrders, newItems
	upds, nAddsI = ti.prepareQ18IndexLocal()
	ti.Tables.Orders, ti.Tables.LineItems = existingOrders, existingItems
	return rems, upds, nAddsI, nRemsI
}

func (ti TableInfo) getQ18UpdsV2(remOrders []*tpch.Orders, remItems [][]*tpch.LineItem, newOrders []*tpch.Orders,
	newItems [][]*tpch.LineItem) (rems []crdt.UpdateObjectParams, upds []crdt.UpdateObjectParams, nAdds int, nRems int) {
	//Remove orders, and then call prepareQ18Index with the new orders/items
	//Quantity -> orderID
	toRemove := createQ18DeleteMap()

	for i, order := range remOrders {
		nRems += q18UpdsCalcHelper(toRemove, remItems[i], order.O_ORDERKEY)
	}

	if useTopKAll {
		nRems = 0
		for _, orderMap := range toRemove {
			nRems += len(orderMap)
		}
	}

	rems = makeQ18IndexRemoves(toRemove, nRems, INDEX_BKT)
	existingOrders, existingItems := ti.Tables.Orders, ti.Tables.LineItems
	ti.Tables.Orders, ti.Tables.LineItems = newOrders, newItems
	upds, nAdds = ti.prepareQ18Index()
	ti.Tables.Orders, ti.Tables.LineItems = existingOrders, existingItems
	return rems, upds, nAdds, nRems
}

func q18UpdsCalcHelper(toRemove []map[int32]struct{}, orderItems []*tpch.LineItem, orderKey int32) (nRems int) {
	currQuantity, nRems, j := int32(0), 0, int32(0)
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
	return
}

func makeQ3IndexRemoves(remMap map[string]map[int8]map[int32]struct{}, nUpds int, bucketI int) (rems []crdt.UpdateObjectParams) {
	rems = make([]crdt.UpdateObjectParams, nUpds)
	var keyArgs crdt.KeyParams
	i := 0
	for mktSeg, segMap := range remMap {
		for day, dayMap := range segMap {
			//A topK per pair (mktsegment, orderdate)
			keyArgs = crdt.MakeKeyParams(SEGM_DELAY+mktSeg+strconv.FormatInt(int64(day), 10), proto.CRDTType_TOPK_RMV, buckets[bucketI])
			if !useTopKAll {
				for orderKey := range dayMap {
					rems[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: crdt.TopKRemove{Id: orderKey}}
					i++
				}
			} else {
				dayRems := make([]int32, len(dayMap))
				j := 0
				for orderKey := range dayMap {
					dayRems[j] = orderKey
					j++
				}
				rems[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: crdt.TopKRemoveAll{Ids: dayRems}}
				i++
			}

		}
	}
	return
}

func makeQ15TopSumIndexUpdsDeletes(yearMap map[int16]map[int8]map[int32]*float64, diffEntries map[int16]map[int8]map[int32]float64,
	nUpds int, bucketI int) (upds []crdt.UpdateObjectParams, updsDone int) {
	upds = make([]crdt.UpdateObjectParams, nUpds)
	index, done := makeQ15TopSumIndexUpdsDeletesHelper(yearMap, diffEntries, bucketI, upds, 0)
	return upds[:index], done
}

func makeQ15TopSumIndexUpdsDeletesHelper(yearMap map[int16]map[int8]map[int32]*float64, diffEntries map[int16]map[int8]map[int32]float64,
	bucketI int, upds []crdt.UpdateObjectParams, bufI int) (newBufI int, updsDone int) {

	oldBufI := bufI
	var monthMap map[int8]map[int32]float64
	var suppMap map[int32]float64
	var keyArgs crdt.KeyParams
	var value float64
	for year, mUpd := range diffEntries {
		monthMap = diffEntries[year]
		for month, sUpd := range mUpd {
			if len(sUpd) > 0 {
				suppMap = monthMap[month]
				keyArgs = crdt.MakeKeyParams(TOP_SUPPLIERS+strconv.FormatInt(int64(year), 10)+strconv.FormatInt(int64(month), 10), proto.CRDTType_TOPSUM, buckets[bucketI])
				if !useTopKAll {
					for suppKey := range sUpd {
						value = suppMap[suppKey]
						var currUpd crdt.UpdateArguments
						if value > 0.0 {
							currUpd = crdt.TopSAdd{TopKScore: crdt.TopKScore{Id: suppKey, Score: int32(value)}}
						} else if value < 0.0 {
							currUpd = crdt.TopSSub{TopKScore: crdt.TopKScore{Id: suppKey, Score: int32(value)}}
						}
						upds[bufI] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: currUpd}
						bufI++
					}
					updsDone = bufI - oldBufI
				} else {
					incs, decs := make([]crdt.TopKScore, len(suppMap)), make([]crdt.TopKScore, len(suppMap))
					j, k := 0, 0
					for suppKey := range sUpd {
						value = suppMap[suppKey]
						if value > 0.0 {
							incs[j], j = crdt.TopKScore{Id: suppKey, Score: int32(value)}, j+1
						} else if value < 0.0 {
							decs[k], k = crdt.TopKScore{Id: suppKey, Score: int32(value)}, k+1
						}
					}
					if j > 0 {
						incs = incs[:j]
						upds[bufI] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: crdt.TopSAddAll{Scores: incs}}
						bufI++
					}
					if k > 0 {
						decs = decs[:k]
						upds[bufI] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: crdt.TopSSubAll{Scores: decs}}
						bufI++
					}
					updsDone += j + k
				}
			}
		}
	}
	return bufI, updsDone
}

func makeQ15IndexUpdsDeletes(yearMap map[int16]map[int8]map[int32]*float64, updEntries map[int16]map[int8]map[int32]struct{},
	nUpds int, bucketI int) (upds []crdt.UpdateObjectParams, updsDone int) {
	upds = make([]crdt.UpdateObjectParams, nUpds)
	index, done := makeQ15IndexUpdsDeletesHelper(yearMap, updEntries, bucketI, upds, 0)
	return upds[:index], done
}

func makeQ15IndexUpdsDeletesHelper(yearMap map[int16]map[int8]map[int32]*float64, updEntries map[int16]map[int8]map[int32]struct{},
	bucketI int, upds []crdt.UpdateObjectParams, bufI int) (newBufI int, updsDone int) {

	//Note: I don't think this actually is correct - if for a given supplier the value decreases, the topKAdd won't have any effect - as a higher value is there.

	oldBufI := bufI
	var monthMap map[int8]map[int32]*float64
	var suppMap map[int32]*float64
	var keyArgs crdt.KeyParams
	var value *float64
	for year, mUpd := range updEntries {
		monthMap = yearMap[year]
		for month, sUpd := range mUpd {
			if len(sUpd) > 0 {
				suppMap = monthMap[month]
				keyArgs = crdt.MakeKeyParams(TOP_SUPPLIERS+strconv.FormatInt(int64(year), 10)+strconv.FormatInt(int64(month), 10), proto.CRDTType_TOPK_RMV, buckets[bucketI])
				if !useTopKAll {
					for suppKey := range sUpd {
						value = suppMap[suppKey]
						var currUpd crdt.UpdateArguments
						if *value == 0.0 {
							currUpd = crdt.TopKRemove{Id: suppKey}
						} else {
							currUpd = crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: suppKey, Score: int32(*value)}}
						}
						upds[bufI] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: currUpd}
						bufI++
					}
					updsDone = bufI - oldBufI
				} else {
					adds, rems := make([]crdt.TopKScore, len(suppMap)), make([]int32, len(suppMap))
					j, k := 0, 0
					for suppKey := range sUpd {
						value = suppMap[suppKey]
						if *value == 0.0 {
							rems[k] = suppKey
							k++
						} else {
							adds[j] = crdt.TopKScore{Id: suppKey, Score: int32(*value)}
							j++
						}
					}
					if j > 0 {
						adds = adds[:j]
						upds[bufI] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: crdt.TopKAddAll{Scores: adds}}
						bufI++
					}
					if k > 0 {
						rems = rems[:k]
						upds[bufI] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: crdt.TopKRemoveAll{Ids: rems}}
						bufI++
					}
					updsDone += j + k
				}
			}
		}
	}
	return bufI, updsDone
}

func makeQ18IndexRemoves(toRemove []map[int32]struct{}, nRems int, bucketI int) (rems []crdt.UpdateObjectParams) {
	rems = make([]crdt.UpdateObjectParams, nRems)
	var keyArgs crdt.KeyParams

	i := 0
	for baseQ, orderMap := range toRemove {
		keyArgs = crdt.MakeKeyParams(LARGE_ORDERS+strconv.FormatInt(int64(312+baseQ), 10), proto.CRDTType_TOPK_RMV, buckets[bucketI])
		if !useTopKAll {
			for orderKey := range orderMap {
				rems[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: crdt.TopKRemove{Id: orderKey}}
				i++
			}
		} else if len(orderMap) > 0 {
			ids := make([]int32, len(orderMap))
			j := 0
			for orderKey := range orderMap {
				ids[j] = orderKey
				j++
			}
			rems[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: crdt.TopKRemoveAll{Ids: ids}}
			i++
		}
	}
	return
}

func queueStatMsg(chanIndex int, upds []crdt.UpdateObjectParams, nUpds int, dataType int) {
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

	updsPerServer := make([]map[string]crdt.UpdateArguments, len(tpchData.Tables.Regions))
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
	_, orderUpd := tpch.GetInnerMapEntry(tpchData.Headers[tpch.ORDERS], tpchData.Keys[tpch.ORDERS], order, tpchData.ToRead[tpch.ORDERS])
	lineUpds := make([]crdt.EmbMapUpdateAll, len(lineItems))
	for i, item := range lineItems {
		_, lineUpds[i] = tpch.GetInnerMapEntry(tpchData.Headers[tpch.LINEITEM], tpchData.Keys[tpch.LINEITEM], order, tpchData.ToRead[tpch.LINEITEM])
		ignore(item)
	}
	//orderObj, lineItemsObjs := ti.Tables.UpdateOrderLineitems(order, lineItems)
	//indexUpds := getIndexUpds(orderObj, lineItemsObjs)
	ignore(orderUpd)
}

func getIndexUpds(order *tpch.Orders, lineItems []*tpch.LineItem) {
	q3Upds := getQ3Upds(order, lineItems)
	q5Upds := getQ5Upds(order, lineItems)
	//Query 11 doesn't need updates (Nation/Supply only, which are never updated.)
	//q11Upds := getQ11Upds(order, lineItems)
	q14Upds := getQ14Upds(order, lineItems)
	q15Upds := getQ15Upds(order, lineItems)
	q18Upds := getQ18Upds(order, lineItems)
	ignore(q3Upds, q5Upds, q14Upds, q15Upds, q18Upds)
}

func getQ3Upds(order *tpch.Orders, lineItems []*tpch.LineItem) (upds []crdt.UpdateObjectParams) {
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
	mktSeg := tpchData.Tables.Customers[order.O_CUSTKEY].C_MKTSEGMENT

	date := order.O_ORDERDATE
	minDate, maxDate := &tpch.Date{YEAR: 1995, MONTH: 03, DAY: 01}, &tpch.Date{YEAR: 1995, MONTH: 03, DAY: 31}
	var minDay int8
	if date.IsLowerOrEqual(maxDate) {
		for _, item := range lineItems {
			if item.L_SHIPDATE.IsHigherOrEqual(minDate) {
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

		upds = make([]crdt.UpdateObjectParams, nUpds)
		var keyArgs crdt.KeyParams
		i := 0
		for day, sum := range sums {
			if *sum > 0 {
				keyArgs = crdt.MakeKeyParams(SEGM_DELAY+mktSeg+strconv.FormatInt(int64(day+1), 10), proto.CRDTType_TOPK_RMV, buckets[INDEX_BKT])
				upds[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: order.O_ORDERKEY, Score: int32(*sum)}}}
				i++
			}
		}

		return
	}
	//No update in the index needed
	return []crdt.UpdateObjectParams{}
}
func getQ5Upds(order *tpch.Orders, lineItems []*tpch.LineItem) (upds []crdt.UpdateObjectParams) {
	/*
		Q5 - sum(l_extendedprice * (1 - l_discount)) for each pair (country, pair)
		The indexes are implemented as a EmbMap of region+date, with one counter entry per nation
		Note that all updates will go for the same year, region and nation! (as the supplier's nation must match the customer's)
		So we only need one value and, thus, one increment, if any.
	*/
	year := order.O_ORDERDATE.YEAR
	value := 0.0
	if year >= 1993 && year <= 1997 {
		customer := tpchData.Tables.Customers[order.O_CUSTKEY]
		nation := tpchData.Tables.Nations[customer.C_NATIONKEY]
		var supplier *tpch.Supplier
		for _, item := range lineItems {
			supplier = tpchData.Tables.Suppliers[item.L_SUPPKEY]
			if customer.C_NATIONKEY == supplier.S_NATIONKEY {
				value += item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT)
			}
		}
		if value > 0.0 {
			return []crdt.UpdateObjectParams{{
				KeyParams:  crdt.MakeKeyParams(NATION_REVENUE+tpchData.Tables.Regions[nation.N_NATIONKEY].R_NAME+strconv.FormatInt(int64(year), 10), proto.CRDTType_RRMAP, buckets[INDEX_BKT]),
				UpdateArgs: crdt.EmbMapUpdateAll{Upds: map[string]crdt.UpdateArguments{nation.N_NAME: crdt.Increment{Change: int32(value)}}},
			}}
		}
	}
	//No update in the index needed
	return []crdt.UpdateObjectParams{}
}
func getQ14Upds(order *tpch.Orders, lineItems []*tpch.LineItem) (upds []crdt.UpdateObjectParams) {
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
			if strings.HasPrefix(tpchData.Tables.Parts[item.L_PARTKEY].P_TYPE, promo) {
				promoValues[key] += currValue
			}
			totalValues[key] += currValue
		}

	}

	upds = make([]crdt.UpdateObjectParams, len(totalValues))
	i := 0
	for key, total := range totalValues {
		promo := promoValues[key]
		currUpd := crdt.AddMultipleValue{SumValue: int64(100.0 * promo), NAdds: int64(total)}
		upds[i] = crdt.UpdateObjectParams{KeyParams: crdt.MakeKeyParams(PROMO_PERCENTAGE+key, proto.CRDTType_AVG, buckets[INDEX_BKT]), UpdateArgs: &currUpd}
		i++
	}

	return
}

func getQ15Upds(order *tpch.Orders, lineItems []*tpch.LineItem) (upds []crdt.UpdateObjectParams) {
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

	upds = make([]crdt.UpdateObjectParams, possibleUpds)
	var keyArgs crdt.KeyParams
	i := 0
	for j, monthMap := range yearMap {
		year64 = int64(j) + 1993
		for k, suppMap := range monthMap {
			month64 = int64(k)*3 + 1
			keyArgs = crdt.MakeKeyParams(TOP_SUPPLIERS+strconv.FormatInt(year64, 10)+strconv.FormatInt(month64, 10), proto.CRDTType_TOPK_RMV, buckets[INDEX_BKT])
			for suppKey, value := range suppMap {
				upds[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: suppKey, Score: int32(value)}}}
				i++
			}
		}
	}

	//If multiple lineitems are from the same supplier, then not all positions in upds will be filled
	return upds[:i]
}

func getQ18Upds(order *tpch.Orders, lineItems []*tpch.LineItem) (upds []crdt.UpdateObjectParams) {
	quantity := 0
	for _, item := range lineItems {
		quantity += int(item.L_QUANTITY)
	}

	var keyArgs crdt.KeyParams
	if quantity >= 312 {
		nUpds := min(315, quantity) - 311 //311 instead of 312 to give space for 312-315.
		upds = make([]crdt.UpdateObjectParams, nUpds)
		for i := 0; i < nUpds; i++ {
			keyArgs = crdt.MakeKeyParams(LARGE_ORDERS+strconv.FormatInt(int64(quantity), 10), proto.CRDTType_TOPK_RMV, buckets[INDEX_BKT])
			upds[i] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: order.O_ORDERKEY, Score: int32(quantity)}}}
			i++
		}
	}

	//if quantity is < 312 then there's no updates
	return []crdt.UpdateObjectParams{}
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
	tableKeys := []string{TableNames[3], TableNames[1]}
	deleteProto := getDeletes(tableKeys, deleteKeys)
	updsQueue <- QueuedMsg{Message: antidote.CreateStaticUpdateObjs(nil, deleteProto), code: antidote.StaticUpdateObjs}
}

func getDeletes(tableKeys []string, deleteKeys []string) (objDeletes []crdt.UpdateObjectParams) {
	objDeletes = make([]crdt.UpdateObjectParams, len(tableKeys))
	i := 0
	for _, tableKey := range tableKeys {
		objDeletes[i] = *getTableDelete(tableKey, deleteKeys)
		i++
	}
	return objDeletes
}

func getTableDelete(tableKey string, deleteKeys []string) (delete *crdt.UpdateObjectParams) {
	var mapRemove crdt.UpdateArguments = crdt.MapRemoveAll{Keys: deleteKeys}
	return &crdt.UpdateObjectParams{
		KeyParams:  antidote.KeyParams{Key: tableKey, CrdtType: proto.CRDTType_RRMAP, Bucket: "bkt"},
		UpdateArgs: &mapRemove,
	}
}
*/
