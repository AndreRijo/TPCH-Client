package client

import (
	"fmt"
	"math"
	"potionDB/src/antidote"
	"potionDB/src/crdt"
	"potionDB/src/proto"
	"runtime"
	"strings"
	"time"

	//"tpch_client/src/tpch"
	"tpch_data/tpch"
)

//Deals with loading the initial data (along with headers) from the files, creating the initial client tables
//and generating the protobufs for the data

var (
	//Filled by prepareConfigs()
	MAX_LINEITEM_GOROUTINES         int  //max number of goroutines to be used to send lineItems. LineItems protos take around 90% of the dataload time.
	LOAD_BASE_DATA, LOAD_INDEX_DATA bool //If the dataload client should load to the server the base and index data, respectively

	//minimum amount of lineitems to be processed by each goroutine. The number of created goroutines is always <= MAX_LINEITEM_GOROUTINES
	LINEITEM_FACTOR        = 300000
	prepareProtoFinishChan chan bool
)

func handleHeaders() {
	startTime := time.Now().UnixNano() / 1000000
	headers, keys, read = tpch.ReadHeaders(headerLoc, len(TableNames))
	finishTime := time.Now().UnixNano() / 1000000
	times.header = finishTime - startTime
}

func handleTables() {
	startTime := time.Now().UnixNano() / 1000000
	//Force these to be read first
	readProcessSendTable(tpch.REGION)
	readProcessSendTable(tpch.NATION)
	readProcessSendTable(tpch.SUPPLIER)
	readProcessSendTable(tpch.CUSTOMER)
	readProcessSendTable(tpch.ORDERS)
	//Order is irrelevant now
	readProcessSendTable(tpch.LINEITEM)
	readProcessSendTable(tpch.PARTSUPP)
	readProcessSendTable(tpch.PART)
	finishTime := time.Now().UnixNano() / 1000000
	times.read = finishTime - startTime
}

func readProcessSendTable(i int) {
	fmt.Println("Reading", TableNames[i], i)
	nEntries := TableEntries[i]
	if TableUsesSF[i] {
		nEntries = int(float64(nEntries) * scaleFactor)
	}
	tables[i] = tpch.ReadTable(tableFolder+TableNames[i]+TableExtension, TableParts[i], nEntries, read[i])
	//Read complete, can now start processing and sending it
	channels.procTableChan <- i
}

func readOrderUpdates() {
	updOrders := tpch.ReadOrderUpdates(UpdsNames[0], updEntries[0], UpdParts[0], read[0], N_UPDATE_FILES)
	procTables.FillOrdersToRegion(updOrders)
	//procTables.FillOrdersToRegion(nil)
}

func handleTableProcessing() {
	left := len(TableNames)
	for ; left > 0; left-- {
		i := <-channels.procTableChan
		fmt.Println("Creating", TableNames[i], i)
		switch i {
		case tpch.CUSTOMER:
			procTables.CreateCustomers(tables)
		case tpch.LINEITEM:
			procTables.CreateLineitems(tables)
		case tpch.NATION:
			procTables.CreateNations(tables)
		case tpch.ORDERS:
			procTables.CreateOrders(tables)
		case tpch.PART:
			procTables.CreateParts(tables)
		case tpch.REGION:
			procTables.CreateRegions(tables)
		case tpch.PARTSUPP:
			procTables.CreatePartsupps(tables)
		case tpch.SUPPLIER:
			procTables.CreateSuppliers(tables)
		}
		if DOES_DATA_LOAD && LOAD_BASE_DATA {
			channels.prepSendChan <- i
		}
	}
	procTables.NationsByRegion = tpch.CreateNationsByRegionTable(procTables.Nations, procTables.Regions)
	times.clientTables = (time.Now().UnixNano() - times.startTime) / 1000000
	fmt.Println("Finished creating all tables.")
	//If we're only doing queries, we can clean the unprocessed tables right now, as no further processing will be done
	if !DOES_DATA_LOAD && !DOES_UPDATES {
		tables = nil
	} else if DOES_DATA_LOAD && LOAD_INDEX_DATA {
		//Start preparing the indexes
		if isIndexGlobal {
			prepareIndexesToSend()
		} else {
			prepareIndexesLocalToSend()
		}
	} else if DOES_UPDATES {
		//For the case of DOES_DATA_LOAD && DOES_UPDATES, updates will start after indexes are prepared.
		//Also, preparing Q15 index is necessary for the updates to work.
		if isIndexGlobal {
			TableInfo{Tables: procTables}.prepareQ15Index()
		} else {
			TableInfo{Tables: procTables}.prepareQ15IndexLocal()
		}
		if DOES_QUERIES {
			go startMixBench()
		} else {
			go startUpdates()
		}
	}
	if DOES_QUERIES && withUpdates && !DOES_UPDATES {
		//Read only order updates in order to know the new orderIDs added by the update client.
		go readOrderUpdates()
	}
}

func handlePrepareSend() {
	nFinished := 0
	prepareProtoFinishChan = make(chan bool, len(TableNames))
	left := len(TableNames)
	for ; left > 0; left-- {
		i := <-channels.prepSendChan
		fmt.Println("Preparing protos", TableNames[i], i)
		if i == tpch.PART {
			go prepareSendAny(i, PART_BKT)
		} else if !isMulti {
			go prepareSendAny(i, 0)
		} else if i == tpch.LINEITEM {
			go prepareSendMultiplePartitioned(i)
		} else {
			go prepareSendPartitioned(i)
		}
		//runtime.GC()
	}
	//fmt.Println("Waiting for:", nFinished)
	for ; nFinished < len(TableNames); nFinished++ {
		<-prepareProtoFinishChan
		fmt.Println("Received prepareFinish, nFinished will now be:", nFinished+1)
	}
	runtime.GC()
	times.prepareDataProtos = (time.Now().UnixNano() - times.startTime) / 1000000
	updsDone := len(procTables.Customers) + len(procTables.Nations) + len(procTables.Orders) + len(procTables.Parts) +
		len(procTables.PartSupps) + len(procTables.Regions) + len(procTables.Suppliers) + times.nLineItemsSent
	if isMulti {
		updsDone += times.nLineItemsSent
	} else {
		updsDone += len(procTables.LineItems)
	}
	fmt.Println("Finished preparing protos for all tables. Number of upds generated:", updsDone)
	dataloadStats.nDataUpds = updsDone
	for _, channel := range channels.dataChans {
		channel <- QueuedMsg{code: QUEUE_COMPLETE}
	}
}

func getDataUpdateParams(currMap map[string]crdt.UpdateArguments, name string, bucket string) (updParams []antidote.UpdateObjectParams) {
	if CRDT_PER_OBJ {
		updParams = make([]antidote.UpdateObjectParams, len(currMap))
		i := 0
		for key, upd := range currMap {
			updParams[i] = antidote.UpdateObjectParams{
				KeyParams:  antidote.KeyParams{Key: key, CrdtType: proto.CRDTType_RRMAP, Bucket: bucket},
				UpdateArgs: &upd,
			}
			i++
		}
	} else {
		var currUpd crdt.UpdateArguments = crdt.EmbMapUpdateAll{Upds: currMap}
		updParams = []antidote.UpdateObjectParams{antidote.UpdateObjectParams{
			KeyParams:  antidote.KeyParams{Key: name, CrdtType: proto.CRDTType_RRMAP, Bucket: bucket},
			UpdateArgs: &currUpd}}
	}
	return
}

func queueDataProto(currMap map[string]crdt.UpdateArguments, name string, bucket string, dataChan chan QueuedMsg) {
	updParams := getDataUpdateParams(currMap, name, bucket)
	dataChan <- QueuedMsg{code: antidote.StaticUpdateObjs, Message: antidote.CreateStaticUpdateObjs(nil, updParams)}
}

//Updates a table entry's key, depending on whenever each table entry should be in a separate CRDT or not.
func getEntryKey(tableName string, entryKey string) (key string) {
	if CRDT_PER_OBJ {
		return tableName + entryKey
	}
	return entryKey
}

//Inner most updates: the object/entry itself (upd to an RWEmbMap, whose entries are LWWRegisters)
func getEntryUpd(headers []string, primKeys []int, table []string, read []int8) (objKey string, entriesUpd *crdt.EmbMapUpdateAll) {
	entries := make(map[string]crdt.UpdateArguments)
	for _, tableI := range read {
		entries[headers[tableI]] = crdt.SetValue{NewValue: table[tableI]}
	}

	var buf strings.Builder
	for _, keyIndex := range primKeys {
		buf.WriteString(table[keyIndex])
		//TODO: Remove, just for easier debug
		buf.WriteRune('_')
	}
	//TODO: Also remove the slicing after removing the "_"
	return buf.String()[:buf.Len()-1], &crdt.EmbMapUpdateAll{Upds: entries}
}

//Inner most updates: the object/entry itself (upd to an RWEmbMap, whose entries are LWWRegisters)
/*
func getEntryORMapUpd(headers []string, primKeys []int, table []string, read []int8) (objKey string, entriesUpd *crdt.MapAddAll) {
	entries := make(map[string]crdt.Element)
	for _, tableI := range read {
		entries[headers[tableI]] = crdt.Element(table[tableI])
	}

	var buf strings.Builder
	for _, keyIndex := range primKeys {
		buf.WriteString(table[keyIndex])
		//TODO: Remove, just for easier debug
		buf.WriteRune('_')
	}
	//TODO: Also remove the slicing after removing the "_"
	return buf.String()[:buf.Len()-1], &crdt.MapAddAll{Values: entries}
}
*/

func prepareSendPartitioned(tableIndex int) {
	//TODO: Maybe have a method that does this initialization and returns a struct with all the fields?
	regionFunc := regionFuncs[tableIndex]

	updsPerServer := make([]map[string]crdt.UpdateArguments, len(procTables.Regions))
	for i := range updsPerServer {
		updsPerServer[i] = make(map[string]crdt.UpdateArguments, maxUpdSize)
	}
	table, header, primKeys, read, name := tables[tableIndex], headers[tableIndex], keys[tableIndex], read[tableIndex], TableNames[tableIndex]

	var key string
	var upd *crdt.EmbMapUpdateAll
	var region int8
	var currMap map[string]crdt.UpdateArguments
	for _, obj := range table {
		key, upd = getEntryUpd(header, primKeys, obj, read)
		key = getEntryKey(name, key)
		region = regionFunc(obj)
		currMap = updsPerServer[region]
		currMap[key] = *upd
		if len(currMap) == maxUpdSize {
			queueDataProto(currMap, name, buckets[region], channels.dataChans[region])
			updsPerServer[region] = make(map[string]crdt.UpdateArguments)
		}
	}

	for i, leftUpds := range updsPerServer {
		if len(leftUpds) > 0 {
			queueDataProto(leftUpds, name, buckets[i], channels.dataChans[i])
		}
	}
	//Clean table
	tables[tableIndex] = nil
	prepareProtoFinishChan <- true
}

func prepareSendMultiplePartitioned(tableIndex int) {
	regionFunc := multiRegionFunc[tableIndex]

	/*
		updsPerServer := make([]map[string]crdt.UpdateArguments, len(procTables.Regions))
		for i := range updsPerServer {
			updsPerServer[i] = make(map[string]crdt.UpdateArguments)
		}
	*/
	table, header, primKeys, read, name := tables[tableIndex], headers[tableIndex], keys[tableIndex], read[tableIndex], TableNames[tableIndex]

	//Splitting workload between goroutines
	targetNRoutines := int(math.Min(float64(MAX_LINEITEM_GOROUTINES), float64(len(table)/LINEITEM_FACTOR)+1))
	if targetNRoutines == MAX_LINEITEM_GOROUTINES {
		LINEITEM_FACTOR = len(table) / MAX_LINEITEM_GOROUTINES
	}
	startPos, endPos := 0, LINEITEM_FACTOR
	subTables := make([][][]string, targetNRoutines)
	for i := 0; i < targetNRoutines-1; i++ {
		subTables[i] = table[startPos:endPos]
		startPos = endPos
		endPos += LINEITEM_FACTOR
	}
	fmt.Println(len(table))
	fmt.Println("NGoroutines:", targetNRoutines)
	fmt.Println(MAX_LINEITEM_GOROUTINES, len(table)/LINEITEM_FACTOR+1)
	subTables[targetNRoutines-1] = table[startPos:]
	doneChan := make(chan bool, targetNRoutines)
	for _, subTable := range subTables {
		go func(itemTable [][]string) {
			fmt.Println(len(itemTable))
			updsPerServer := make([]map[string]crdt.UpdateArguments, len(procTables.Regions))
			for i := range updsPerServer {
				updsPerServer[i] = make(map[string]crdt.UpdateArguments)
			}
			var key string
			var upd *crdt.EmbMapUpdateAll
			var regions []int8
			var currMap map[string]crdt.UpdateArguments
			count := 0
			printTarget := (len(itemTable) / 2) / maxUpdSize
			for _, obj := range itemTable {
				key, upd = getEntryUpd(header, primKeys, obj, read)
				key = getEntryKey(name, key)
				regions = regionFunc(obj)
				for _, reg := range regions {
					currMap = updsPerServer[reg]
					currMap[key] = *upd
					if len(currMap) == maxUpdSize {
						count++
						if count%printTarget == 0 {
							fmt.Println("Queuing lineitem proto", count, count*maxUpdSize)
						}
						queueDataProto(currMap, name, buckets[reg], channels.dataChans[reg])
						updsPerServer[reg] = make(map[string]crdt.UpdateArguments)
					}
				}
				times.nLineItemsSent += len(regions)
			}

			for i, leftUpds := range updsPerServer {
				if len(leftUpds) > 0 {
					queueDataProto(leftUpds, name, buckets[i], channels.dataChans[i])
				}
			}
			doneChan <- true
		}(subTable)
	}

	for i := 0; i < targetNRoutines; i++ {
		<-doneChan
	}
	/*
		var key string
		var upd *crdt.EmbMapUpdateAll
		var regions []int8
		var currMap map[string]crdt.UpdateArguments
		count := 0
		printTarget := (len(table) / 10) / maxUpdSize
		for _, obj := range table {
			key, upd = getEntryUpd(header, primKeys, obj, read)
			key = getEntryKey(name, key)
			regions = regionFunc(obj)
			for _, reg := range regions {
				currMap = updsPerServer[reg]
				currMap[key] = *upd
				if len(currMap) == maxUpdSize {
					count++
					if count%printTarget == 0 {
						fmt.Println("Queuing lineitem proto", count, count*maxUpdSize)
					}
					queueDataProto(currMap, name, buckets[reg], channels.dataChans[reg])
					updsPerServer[reg] = make(map[string]crdt.UpdateArguments)
				}
			}
			times.nLineItemsSent += len(regions)
		}

		for i, leftUpds := range updsPerServer {
			if len(leftUpds) > 0 {
				queueDataProto(leftUpds, name, buckets[i], channels.dataChans[i])
			}
		}
	*/
	//Clean table
	tables[tableIndex] = nil
	prepareProtoFinishChan <- true
}

func prepareSendAny(tableIndex int, bucketIndex int) {
	upds := make(map[string]crdt.UpdateArguments)
	table, header, primKeys, read, name := tables[tableIndex], headers[tableIndex], keys[tableIndex], read[tableIndex], TableNames[tableIndex]

	var key string
	var upd *crdt.EmbMapUpdateAll
	for _, obj := range table {
		key, upd = getEntryUpd(header, primKeys, obj, read)
		key = getEntryKey(name, key)
		upds[key] = *upd
		if len(upds) == maxUpdSize {
			queueDataProto(upds, name, buckets[bucketIndex], channels.dataChans[0])
			upds = make(map[string]crdt.UpdateArguments)
		}
	}

	if len(upds) > 0 {
		queueDataProto(upds, name, buckets[bucketIndex], channels.dataChans[0])
	}
	//Clean table
	tables[tableIndex] = nil
	prepareProtoFinishChan <- true
}
