package tpch

import (
	"antidote"
	"crdt"
	"fmt"
	"proto"
	"runtime"
	"strings"
	"time"
)

//Deals with loading the initial data (along with headers) from the files, creating the initial client tables
//and generating the protobufs for the data

func handleHeaders() {
	startTime := time.Now().UnixNano() / 1000000
	headers, keys, read = ReadHeaders(headerLoc, len(tableNames))
	finishTime := time.Now().UnixNano() / 1000000
	times.header = finishTime - startTime
}

func handleTables() {
	startTime := time.Now().UnixNano() / 1000000
	//Force these to be read first
	readProcessSendTable(REGION)
	readProcessSendTable(NATION)
	readProcessSendTable(SUPPLIER)
	readProcessSendTable(CUSTOMER)
	//Order is irrelevant now
	readProcessSendTable(ORDERS)
	readProcessSendTable(LINEITEM)
	readProcessSendTable(PARTSUPP)
	readProcessSendTable(PART)
	finishTime := time.Now().UnixNano() / 1000000
	times.read = finishTime - startTime
}

func readProcessSendTable(i int) {
	fmt.Println("Reading", tableNames[i], i)
	nEntries := tableEntries[i]
	if tableUsesSF[i] {
		nEntries = int(float64(nEntries) * scaleFactor)
	}
	tables[i] = ReadTable(tableFolder+tableNames[i]+tableExtension, tableParts[i], nEntries, read[i])
	//Read complete, can now start processing and sending it
	channels.procTableChan <- i
}

func handleTableProcessing() {
	left := len(tableNames)
	for ; left > 0; left-- {
		i := <-channels.procTableChan
		fmt.Println("Creating", tableNames[i], i)
		switch i {
		case CUSTOMER:
			procTables.CreateCustomers(tables)
		case LINEITEM:
			procTables.CreateLineitems(tables)
		case NATION:
			procTables.CreateNations(tables)
		case ORDERS:
			procTables.CreateOrders(tables)
		case PART:
			procTables.CreateParts(tables)
		case REGION:
			procTables.CreateRegions(tables)
		case PARTSUPP:
			procTables.CreatePartsupps(tables)
		case SUPPLIER:
			procTables.CreateSuppliers(tables)
		}
		channels.prepSendChan <- i
	}
	times.clientTables = (time.Now().UnixNano() - times.startTime) / 1000000
	fmt.Println("Finished creating all tables")
	//Now we can start preparing the indexes
	if isIndexGlobal {
		prepareIndexesToSend()
	} else {
		prepareIndexesLocalToSend()
	}

}

func handlePrepareSend() {
	left := len(tableNames)
	for ; left > 0; left-- {
		i := <-channels.prepSendChan
		fmt.Println("Preparing protos", tableNames[i], i)
		if i == PART {
			prepareSendAny(i, PART_BKT)
		} else if !isMulti {
			prepareSendAny(i, 0)
		} else if i == LINEITEM {
			prepareSendMultiplePartitioned(i)
		} else {
			prepareSendPartitioned(i)
		}
		runtime.GC()
	}
	times.prepareDataProtos = (time.Now().UnixNano() - times.startTime) / 1000000
	fmt.Println("Finished preparing protos for all tables")
	for _, channel := range channels.dataChans {
		channel <- QueuedMsg{code: QUEUE_COMPLETE}
	}
}

func prepareSendPartitioned(tableIndex int) {
	regionFunc := regionFuncs[tableIndex]

	updsPerServer := make([]map[string]crdt.UpdateArguments, len(procTables.Regions))
	for i := range updsPerServer {
		updsPerServer[i] = make(map[string]crdt.UpdateArguments)
	}
	table, header, primKeys, read, name := tables[tableIndex], headers[tableIndex], keys[tableIndex], read[tableIndex], tableNames[tableIndex]

	var key string
	var upd *crdt.EmbMapUpdateAll
	var region int8
	var currMap map[string]crdt.UpdateArguments
	for _, obj := range table {
		key, upd = getEntryUpd(header, primKeys, obj, read)
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
}

func prepareSendMultiplePartitioned(tableIndex int) {
	regionFunc := multiRegionFunc[tableIndex]

	updsPerServer := make([]map[string]crdt.UpdateArguments, len(procTables.Regions))
	for i := range updsPerServer {
		updsPerServer[i] = make(map[string]crdt.UpdateArguments)
	}
	table, header, primKeys, read, name := tables[tableIndex], headers[tableIndex], keys[tableIndex], read[tableIndex], tableNames[tableIndex]

	var key string
	var upd *crdt.EmbMapUpdateAll
	var regions []int8
	var currMap map[string]crdt.UpdateArguments
	count := 0
	printTarget := (len(table) / 10) / maxUpdSize
	for _, obj := range table {
		key, upd = getEntryUpd(header, primKeys, obj, read)
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
	}

	for i, leftUpds := range updsPerServer {
		if len(leftUpds) > 0 {
			queueDataProto(leftUpds, name, buckets[i], channels.dataChans[i])
		}
	}
	//Clean table
	tables[tableIndex] = nil
}

func prepareSendAny(tableIndex int, bucketIndex int) {
	upds := make(map[string]crdt.UpdateArguments)
	table, header, primKeys, read, name := tables[tableIndex], headers[tableIndex], keys[tableIndex], read[tableIndex], tableNames[tableIndex]

	var key string
	var upd *crdt.EmbMapUpdateAll
	for _, obj := range table {
		key, upd = getEntryUpd(header, primKeys, obj, read)
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
}

func queueDataProto(currMap map[string]crdt.UpdateArguments, name string, bucket string, dataChan chan QueuedMsg) {
	var currUpd crdt.UpdateArguments = crdt.EmbMapUpdateAll{Upds: currMap}
	dataChan <- QueuedMsg{
		code: antidote.StaticUpdateObjs,
		Message: antidote.CreateStaticUpdateObjs(nil, []antidote.UpdateObjectParams{antidote.UpdateObjectParams{
			KeyParams:  antidote.KeyParams{Key: name, CrdtType: proto.CRDTType_RRMAP, Bucket: bucket},
			UpdateArgs: &currUpd}})}
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
