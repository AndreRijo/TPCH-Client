package client

import (
	"fmt"
	"math"
	"potionDB/crdt/crdt"
	"potionDB/crdt/proto"
	antidote "potionDB/potionDB/components"
	"runtime"
	"strings"
	"time"

	//"tpch_client/src/tpch"
	tpch "tpch_data_processor/tpch"
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

func loadData() {
	tpchData = &tpch.TpchData{TpchConfigs: tpch.TpchConfigs{Sf: scaleFactor, DataLoc: commonFolder, IsSingleServer: SINGLE_INDEX_SERVER}, Tables: &tpch.Tables{}}
	tpchData.Initialize()
	//go createAndSendDataProtos() //Prepare goroutine that will create and send protobufs
	tableTimes := tpchData.PrepareBaseData()
	times.startTime, times.header, times.read, times.clientTables = tableTimes.StartTime, tableTimes.Header, tableTimes.Read, tableTimes.ClientTables
	if !DOES_DATA_LOAD {
		for i := 0; i < len(tpch.TableNames); i++ {
			<-tpchData.ProcChan
		}
		fmt.Println("[ClientDataload]Finished reading and processing base data.")
		startClientsFromDL()
		//Consume procChan here to wait until all tables are created
		//Afterwards, call method to start query/update/queryUpdate client
		//createAndSendDataProtos() should also do the same
	}
}

func startClientsFromDL() {
	times.clientTables = (time.Now().UnixNano() - times.startTime) / 1000000
	//fmt.Println("Finished creating all tables.")
	//If we're only doing queries, we can clean the unprocessed tables right now, as no further processing will be done
	if !DOES_DATA_LOAD && !DOES_UPDATES {
		tpchData.RawTables, tpchData.ToRead, tpchData.Keys, tpchData.Headers = nil, nil, nil, nil
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
			TableInfo{Tables: tpchData.Tables}.prepareQ15Index()
		} else {
			TableInfo{Tables: tpchData.Tables}.prepareQ15IndexLocal()
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

func createAndSendDataProtos() {
	nFinished := 0
	prepareProtoFinishChan = make(chan bool, len(tpch.TableNames))
	left := len(tpch.TableNames)
	fmt.Println("[ClientDataLoad]Waiting to start preparing protos.")
	for ; left > 0; left-- {
		fmt.Printf("[ClientDataLoad]Info about chan: %+v (capacity: %d)\n", tpchData.ProcChan, cap(tpchData.ProcChan))
		i := <-tpchData.ProcChan
		fmt.Println("[ClientDataLoad]Preparing protos", tpch.TableNames[i], i)
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
	go startClientsFromDL()
	//fmt.Println("Waiting for:", nFinished)
	for ; nFinished < len(tpch.TableNames); nFinished++ {
		<-prepareProtoFinishChan
		fmt.Println("Received prepareFinish, nFinished will now be:", nFinished+1)
	}
	runtime.GC()
	times.prepareDataProtos = (time.Now().UnixNano() - times.startTime) / 1000000
	updsDone := len(tpchData.Tables.Customers) + len(tpchData.Tables.Nations) + len(tpchData.Tables.Orders) + len(tpchData.Tables.Parts) +
		len(tpchData.Tables.PartSupps) + len(tpchData.Tables.Regions) + len(tpchData.Tables.Suppliers) + times.nLineItemsSent
	if isMulti {
		updsDone += times.nLineItemsSent
	} else {
		updsDone += len(tpchData.Tables.LineItems)
	}
	fmt.Println("Finished preparing protos for all tables. Number of upds generated:", updsDone)
	dataloadStats.nDataUpds = updsDone
	for _, channel := range channels.dataChans {
		channel <- QueuedMsg{code: QUEUE_COMPLETE}
	}
}

/*
func handleHeaders() {
	startTime := time.Now().UnixNano() / 1000000
	tpchData.Headers, tpchData.Keys, tpchData.ToRead = tpch.ReadHeaders(headerLoc, len(tpch.TableNames))
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
	fmt.Println("Reading", tpch.TableNames[i], i)
	nEntries := tpch.TableEntries[i]
	if tpch.TableUsesSF[i] {
		nEntries = int(float64(nEntries) * scaleFactor)
	}
	tpchData.RawTables[i] = tpch.ReadTable(tableFolder+tpch.TableNames[i]+TableExtension, tpch.TableParts[i], nEntries, tpchData.ToRead[i])
	//Read complete, can now start processing and sending it
	channels.procTableChan <- i
}*/

func readOrderUpdates() {
	updOrders := tpch.ReadOrderUpdates(tpch.UpdsNames[0], tpch.UpdEntries[0], tpch.UpdParts[0], tpchData.ToRead[0], N_UPDATE_FILES)
	tpchData.Tables.FillOrdersToRegion(updOrders)
	//tpchData.Tables.FillOrdersToRegion(nil)
}

/*
func handleTableProcessing() {
	left := len(tpch.TableNames)
	for ; left > 0; left-- {
		i := <-channels.procTableChan
		fmt.Println("Creating", tpch.TableNames[i], i)
		switch i {
		case tpch.CUSTOMER:
			tpchData.Tables.CreateCustomers(tpchData.RawTables)
		case tpch.LINEITEM:
			tpchData.Tables.CreateLineitems(tpchData.RawTables)
		case tpch.NATION:
			tpchData.Tables.CreateNations(tpchData.RawTables)
		case tpch.ORDERS:
			tpchData.Tables.CreateOrders(tpchData.RawTables)
		case tpch.PART:
			tpchData.Tables.CreateParts(tpchData.RawTables)
		case tpch.REGION:
			tpchData.Tables.CreateRegions(tpchData.RawTables)
		case tpch.PARTSUPP:
			tpchData.Tables.CreatePartsupps(tpchData.RawTables)
		case tpch.SUPPLIER:
			tpchData.Tables.CreateSuppliers(tpchData.RawTables)
		}
		if DOES_DATA_LOAD && LOAD_BASE_DATA {
			channels.prepSendChan <- i
		}
	}
	tpchData.Tables.NationsByRegion = tpch.CreateNationsByRegionTable(tpchData.Tables.Nations, tpchData.Tables.Regions)
	tpchData.Tables.SortedNationsName, tpchData.Tables.SortedNationsNameByRegion = tpch.CreateSortedNations(tpchData.Tables.Nations)
	times.clientTables = (time.Now().UnixNano() - times.startTime) / 1000000
	fmt.Println("Finished creating all tables.")
	//If we're only doing queries, we can clean the unprocessed tables right now, as no further processing will be done
	if !DOES_DATA_LOAD && !DOES_UPDATES {
		tpchData.RawTables, tpchData.ToRead, tpchData.Keys, tpchData.Headers = nil, nil, nil, nil
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
			TableInfo{Tables: tpchData.Tables}.prepareQ15Index()
		} else {
			TableInfo{Tables: tpchData.Tables}.prepareQ15IndexLocal()
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
	prepareProtoFinishChan = make(chan bool, len(tpch.TableNames))
	left := len(tpch.TableNames)
	for ; left > 0; left-- {
		i := <-channels.prepSendChan
		fmt.Println("Preparing protos", tpch.TableNames[i], i)
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
	for ; nFinished < len(tpch.TableNames); nFinished++ {
		<-prepareProtoFinishChan
		fmt.Println("Received prepareFinish, nFinished will now be:", nFinished+1)
	}
	runtime.GC()
	times.prepareDataProtos = (time.Now().UnixNano() - times.startTime) / 1000000
	updsDone := len(tpchData.Tables.Customers) + len(tpchData.Tables.Nations) + len(tpchData.Tables.Orders) + len(tpchData.Tables.Parts) +
		len(tpchData.Tables.PartSupps) + len(tpchData.Tables.Regions) + len(tpchData.Tables.Suppliers) + times.nLineItemsSent
	if isMulti {
		updsDone += times.nLineItemsSent
	} else {
		updsDone += len(tpchData.Tables.LineItems)
	}
	fmt.Println("Finished preparing protos for all tables. Number of upds generated:", updsDone)
	dataloadStats.nDataUpds = updsDone
	for _, channel := range channels.dataChans {
		channel <- QueuedMsg{code: QUEUE_COMPLETE}
	}
}
*/

func getDataUpdateParams(currMap map[string]crdt.UpdateArguments, name string, bucket string) (updParams []crdt.UpdateObjectParams) {
	if CRDT_PER_OBJ {
		updParams = make([]crdt.UpdateObjectParams, len(currMap))
		i := 0
		for key, upd := range currMap {
			updParams[i] = crdt.UpdateObjectParams{
				KeyParams:  crdt.KeyParams{Key: key, CrdtType: proto.CRDTType_RRMAP, Bucket: bucket},
				UpdateArgs: upd,
			}
			i++
		}
	} else {
		var currUpd crdt.UpdateArguments = crdt.EmbMapUpdateAll{Upds: currMap}
		updParams = []crdt.UpdateObjectParams{{
			KeyParams:  crdt.KeyParams{Key: name, CrdtType: proto.CRDTType_RRMAP, Bucket: bucket},
			UpdateArgs: currUpd}}
	}
	return
}

func queueDataProto(currMap map[string]crdt.UpdateArguments, name string, bucket string, dataChan chan QueuedMsg) {
	updParams := getDataUpdateParams(currMap, name, bucket)
	dataChan <- QueuedMsg{code: antidote.StaticUpdateObjs, Message: antidote.CreateStaticUpdateObjs(nil, updParams)}
}

// Updates a table entry's key, depending on whenever each table entry should be in a separate CRDT or not.
func getEntryKey(tableName string, entryKey string) (key string) {
	if CRDT_PER_OBJ {
		return tableName + entryKey
	}
	return entryKey
}

// Inner most updates: the object/entry itself (upd to an RWEmbMap, whose entries are LWWRegisters)
/*func getEntryUpd(headers []string, primKeys []int, table []string, read []int8) (objKey string, entriesUpd *crdt.EmbMapUpdateAll) {
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
}*/

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

	updsPerServer := make([]map[string]crdt.UpdateArguments, len(tpchData.Tables.Regions))
	for i := range updsPerServer {
		updsPerServer[i] = make(map[string]crdt.UpdateArguments, maxUpdSize)
	}
	table, header, primKeys, read, name := tpchData.RawTables[tableIndex], tpchData.Headers[tableIndex], tpchData.Keys[tableIndex], tpchData.ToRead[tableIndex], tpch.TableNames[tableIndex]

	var key string
	var upd crdt.EmbMapUpdateAll
	var region int8
	var currMap map[string]crdt.UpdateArguments
	for _, obj := range table {
		key, upd = GetInnerMapEntry(header, primKeys, obj, read)
		key = getEntryKey(name, key)
		region = regionFunc(obj)
		currMap = updsPerServer[region]
		currMap[key] = upd
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
	tpchData.RawTables[tableIndex] = nil
	prepareProtoFinishChan <- true
}

func prepareSendMultiplePartitioned(tableIndex int) {
	regionFunc := multiRegionFunc[tableIndex]

	/*
		updsPerServer := make([]map[string]crdt.UpdateArguments, len(tpchData.Tables.Regions))
		for i := range updsPerServer {
			updsPerServer[i] = make(map[string]crdt.UpdateArguments)
		}
	*/
	table, header, primKeys, read, name := tpchData.RawTables[tableIndex], tpchData.Headers[tableIndex], tpchData.Keys[tableIndex], tpchData.ToRead[tableIndex], tpch.TableNames[tableIndex]

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
			updsPerServer := make([]map[string]crdt.UpdateArguments, len(tpchData.Tables.Regions))
			for i := range updsPerServer {
				updsPerServer[i] = make(map[string]crdt.UpdateArguments)
			}
			var key string
			var upd crdt.EmbMapUpdateAll
			var regions []int8
			var currMap map[string]crdt.UpdateArguments
			count := 0
			printTarget := (len(itemTable) / 2) / maxUpdSize
			for _, obj := range itemTable {
				key, upd = GetInnerMapEntry(header, primKeys, obj, read)
				key = getEntryKey(name, key)
				regions = regionFunc(obj)
				for _, reg := range regions {
					currMap = updsPerServer[reg]
					currMap[key] = upd
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
	tpchData.RawTables[tableIndex] = nil
	prepareProtoFinishChan <- true
}

func prepareSendAny(tableIndex int, bucketIndex int) {
	upds := make(map[string]crdt.UpdateArguments)
	table, header, primKeys, read, name := tpchData.RawTables[tableIndex], tpchData.Headers[tableIndex], tpchData.Keys[tableIndex], tpchData.ToRead[tableIndex], tpch.TableNames[tableIndex]

	var key string
	var upd crdt.EmbMapUpdateAll
	for _, obj := range table {
		key, upd = GetInnerMapEntry(header, primKeys, obj, read)
		key = getEntryKey(name, key)
		upds[key] = upd
		if len(upds) == maxUpdSize {
			queueDataProto(upds, name, buckets[bucketIndex], channels.dataChans[0])
			upds = make(map[string]crdt.UpdateArguments)
		}
	}

	if len(upds) > 0 {
		queueDataProto(upds, name, buckets[bucketIndex], channels.dataChans[0])
	}
	//Clean table
	tpchData.RawTables[tableIndex] = nil
	prepareProtoFinishChan <- true
}

// Inner most updates: the object/entry itself (upd to an RWEmbMap, whose entries are LWWRegisters)
func GetInnerMapEntry(headers []string, primKeys []int, object []string, toRead []int8) (objKey string, upd crdt.EmbMapUpdateAll) {
	entries := make(map[string]crdt.UpdateArguments)
	for _, tableI := range toRead {
		entries[headers[tableI]] = crdt.SetValue{NewValue: object[tableI]}
	}

	var buf strings.Builder
	for _, keyIndex := range primKeys {
		buf.WriteString(object[keyIndex])
		//TODO: Remove, just for easier debug
		buf.WriteRune('_')
	}
	//TODO: Also remove the slicing after removing the "_"
	return buf.String()[:buf.Len()-1], crdt.EmbMapUpdateAll{Upds: entries}
}

func GetInnerMapEntryArray(headers []string, primKeys []int, object []string, toRead []int8) (objKey string, upd crdt.EmbMapUpdateAllArray) {
	entries := make([]crdt.EmbMapUpdate, len(toRead))
	for i, tableI := range toRead {
		entries[i] = crdt.EmbMapUpdate{Key: headers[tableI], Upd: crdt.SetValue{NewValue: object[tableI]}}
	}

	var buf strings.Builder
	for _, keyIndex := range primKeys {
		buf.WriteString(object[keyIndex])
		//TODO: Remove, just for easier debug
		buf.WriteRune('_')
	}
	//TODO: Also remove the slicing after removing the "_"
	return buf.String()[:buf.Len()-1], crdt.EmbMapUpdateAllArray{Upds: entries}
}
