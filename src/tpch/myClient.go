package tpch

import (
	"fmt"
	"math/rand"
	"net"
	"potionDB/src/antidote"
	"potionDB/src/crdt"
	"potionDB/src/proto"
	"strconv"
	"time"
)

func updateMyQueryStats(stats []QueryStats, updStats []UpdateStats, nReads, lastStatReads, nQueries, lastStatQueries int, lastStatTime int64, qTime, updTime int64, nTxnsQ, nTxnsU int) (newStats []QueryStats, newUStats []UpdateStats,
	newNReads, newNQueries int, newLastStatTime int64) {

	currStatTime, currQueryStats := time.Now().UnixNano()/1000000, QueryStats{}

	diffT, diffR, diffQ := currStatTime-lastStatTime, nReads-lastStatReads, nQueries-lastStatQueries
	if diffT < int64(statisticsInterval/100) && len(stats) > 0 {
		//Replace
		lastStatI := len(stats) - 1
		stats[lastStatI].nReads += diffR
		stats[lastStatI].nQueries += diffQ
		stats[lastStatI].timeSpent += diffT
		stats[lastStatI].latency += (qTime / 1000000)
		stats[lastStatI].nTxns += nTxnsQ
		updStats[lastStatI].latency += (updTime / 1000000)
		updStats[lastStatI].nTxns += nTxnsU
	} else {
		currQueryStats.nReads, currQueryStats.nQueries, currQueryStats.timeSpent, currQueryStats.latency, currQueryStats.nTxns = diffR, diffQ, diffT, qTime/1000000, nTxnsQ
		stats = append(stats, currQueryStats)
		var updSt = UpdateStats{}
		updSt.latency = updTime / 1000000
		updSt.nTxns = nTxnsU
		updStats = append(updStats, updSt)
	}
	return stats, updStats, nReads, nQueries, currStatTime
}

func myNewTest() {
	readParams := make([]antidote.ReadObjectParams, 2)
	updParams := make([]antidote.UpdateObjectParams, 2)
	for start := time.Now(); time.Since(start) < time.Minute*2; {
		rand.Seed(time.Now().UnixNano())
		orderRand1 := rand.Intn(len(procTables.Orders)-2) + 1
		orkey1 := procTables.Orders[orderRand1+1].O_ORDERKEY
		regKey1 := procTables.orderkeyToRegionkey(orkey1)

		var orkey2 int32 = -1
		var regKey2 int8 = -1
		for {
			orderRand2 := rand.Intn(len(procTables.Orders)-2) + 1
			orkey2 = procTables.Orders[orderRand2+1].O_ORDERKEY
			regKey2 = procTables.orderkeyToRegionkey(orkey2)
			if regKey2 == regKey1 {
				break
			}
		}
		kParams1 := antidote.CreateKeyParams(tableNames[ORDERS]+strconv.Itoa(int(orkey1)), proto.CRDTType_RRMAP, buckets[regKey1])
		kParams2 := antidote.CreateKeyParams(tableNames[ORDERS]+strconv.Itoa(int(orkey2)), proto.CRDTType_RRMAP, buckets[regKey2])

		readParams = []antidote.ReadObjectParams{{
			KeyParams: kParams1,
			ReadArgs:  crdt.StateReadArguments{},
		}, {
			KeyParams: kParams2,
			ReadArgs:  crdt.StateReadArguments{},
		}}

		antidote.SendProto(antidote.StaticRead, antidote.CreateStaticRead(nil, readParams, nil), conns[regKey1])
		protoTypeRead, protobufRead, _ := antidote.ReceiveProto(conns[regKey1])

		fmt.Println("FIRST READ:", protoTypeRead, protobufRead)

		var mapUpd crdt.UpdateArguments = crdt.EmbMapUpdate{Key: "-c", Upd: crdt.SetValue{NewValue: "a"}}
		updParams = []antidote.UpdateObjectParams{{
			KeyParams: antidote.KeyParams{Key: tableNames[ORDERS] + strconv.Itoa(int(orkey1)), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[regKey1]}, UpdateArgs: &mapUpd},
			{
				KeyParams: antidote.KeyParams{Key: tableNames[ORDERS] + strconv.Itoa(int(orkey2)), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[regKey2]}, UpdateArgs: &mapUpd},
		}
		antidote.SendProto(antidote.StaticUpdateObjs, antidote.CreateStaticUpdateObjs(nil, updParams), conns[regKey1])
		protoTypeUp, protobufUp, _ := antidote.ReceiveProto(conns[regKey1])

		fmt.Println("FIRST UPDATE:", protoTypeUp, protobufUp)

		antidote.SendProto(antidote.StaticRead, antidote.CreateStaticRead(nil, readParams, nil), conns[regKey1])
		protoTypeRead, protobufRead, _ = antidote.ReceiveProto(conns[regKey1])

		fmt.Println("SECOND READ:", protoTypeRead, protobufRead)

		mapUpd = crdt.EmbMapUpdate{Key: "-c", Upd: crdt.SetValue{NewValue: "z"}}
		updParams = []antidote.UpdateObjectParams{{
			KeyParams: antidote.KeyParams{Key: tableNames[ORDERS] + strconv.Itoa(int(orkey1)), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[regKey1]}, UpdateArgs: &mapUpd},
			{
				KeyParams: antidote.KeyParams{Key: tableNames[ORDERS] + strconv.Itoa(int(orkey2)), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[regKey2]}, UpdateArgs: &mapUpd},
		}
		antidote.SendProto(antidote.StaticUpdateObjs, antidote.CreateStaticUpdateObjs(nil, updParams), conns[regKey1])
		protoTypeUp, protobufUp, _ = antidote.ReceiveProto(conns[regKey1])

		fmt.Println("SECOND UPDATE:", protoTypeUp, protobufUp)

		antidote.SendProto(antidote.StaticRead, antidote.CreateStaticRead(nil, readParams, nil), conns[regKey1])
		protoTypeRead, protobufRead, _ = antidote.ReceiveProto(conns[regKey1])

		fmt.Println("THIRD READ:", protoTypeRead, protobufRead)
	}
}

func myTestGlobal() {
	time.Sleep(200 * time.Second)
	localRatio := [11]int{100, 90, 80, 70, 60, 50, 40, 30, 20, 10, 0}
	for _, ratio := range localRatio {
		myTestGlobalAux(ratio)
	}
}

func myTestGlobalAux(ratio int) {
	conns := make([]net.Conn, len(servers))
	for i := range conns {
		conns[i], _ = net.Dial("tcp", servers[i])
	}

	queryStats := make([]QueryStats, 0, TEST_DURATION/int64(statisticsInterval)+1)
	updStats := make([]UpdateStats, 0, TEST_DURATION/int64(statisticsInterval)+1)
	lastStatQueries, lastStatReads := 0, 0
	queries, reads := 0, 0
	startTime := time.Now().UnixNano() / 1000000
	lastStatTime := startTime
	startTxnTime, currUpdSpentTime, currQuerySpentTime := int64(0), int64(0), int64(0)
	readTxns, updTxns := 0, 0

	successQueries := 0
	localReads := 0
	rmtReads := 0
	localUpds := 0
	rmtUpds := 0
	updsDone := 0

	nTxns := 0

	fmt.Println("Starting the Sofia test...")
	for start := time.Now(); time.Since(start) < time.Minute*2; {
		rand.Seed(time.Now().UnixNano())
		isLocal := rand.Intn(100) <= ratio
		if isLocal {
			conns, queryStats, updStats, lastStatQueries, lastStatReads, queries, reads, lastStatTime, startTxnTime, currUpdSpentTime, currQuerySpentTime, readTxns, updTxns, successQueries, localReads, rmtReads, localUpds, rmtUpds, updsDone = bothLocal(conns, queryStats, updStats, lastStatQueries, lastStatReads, queries, reads, lastStatTime, startTxnTime, currUpdSpentTime, currQuerySpentTime, readTxns, updTxns, successQueries, localReads, rmtReads, localUpds, rmtUpds, updsDone)
			nTxns++
		} else {
			numLocal := rand.Intn(3)
			if numLocal == 0 {
				conns, queryStats, updStats, lastStatQueries, lastStatReads, queries, reads, lastStatTime, startTxnTime, currUpdSpentTime, currQuerySpentTime, readTxns, updTxns, successQueries, localReads, rmtReads, localUpds, rmtUpds, updsDone = bothRemote(conns, queryStats, updStats, lastStatQueries, lastStatReads, queries, reads, lastStatTime, startTxnTime, currUpdSpentTime, currQuerySpentTime, readTxns, updTxns, successQueries, localReads, rmtReads, localUpds, rmtUpds, updsDone)
				nTxns++
			} else if numLocal == 1 {
				conns, queryStats, updStats, lastStatQueries, lastStatReads, queries, reads, lastStatTime, startTxnTime, currUpdSpentTime, currQuerySpentTime, readTxns, updTxns, successQueries, localReads, rmtReads, localUpds, rmtUpds, updsDone = order1Local2Remote(conns, queryStats, updStats, lastStatQueries, lastStatReads, queries, reads, lastStatTime, startTxnTime, currUpdSpentTime, currQuerySpentTime, readTxns, updTxns, successQueries, localReads, rmtReads, localUpds, rmtUpds, updsDone)
				nTxns++
			} else {
				conns, queryStats, updStats, lastStatQueries, lastStatReads, queries, reads, lastStatTime, startTxnTime, currUpdSpentTime, currQuerySpentTime, readTxns, updTxns, successQueries, localReads, rmtReads, localUpds, rmtUpds, updsDone = order1Remote2Local(conns, queryStats, updStats, lastStatQueries, lastStatReads, queries, reads, lastStatTime, startTxnTime, currUpdSpentTime, currQuerySpentTime, readTxns, updTxns, successQueries, localReads, rmtReads, localUpds, rmtUpds, updsDone)
				nTxns++
			}
		}
	}
	endTime := time.Now().UnixNano() / 1000000
	var client = MixClientResult{QueryClientResult: QueryClientResult{duration: float64(endTime - startTime), nQueries: float64(successQueries),
		nReads: float64(reads), intermediateResults: queryStats}, updsDone: updsDone, updStats: updStats}

	results := make([]MixClientResult, 1)
	results[0] = client

	fmt.Println("********TESTE DA SOFIA********")
	fmt.Println("Results for a ratio of", ratio, "local operations to", 100-ratio, "remote")

	fmt.Println("Total duration:", endTime-startTime, "ms")
	fmt.Println("Average latency:", (endTime-startTime)/int64(nTxns*5), "ms")
	fmt.Println("Number of ops:", 5*nTxns)

	for _, result := range results {
		fmt.Printf("Totals: Queries: %f, Query/s: %f, Reads: %f, Reads/s: %f, Upds: %d, Upds/s: %f\n",
			result.nQueries, (result.nQueries/result.duration)*1000, result.nReads, (result.nReads/result.duration)*1000,
			result.updsDone, (float64(result.updsDone)/result.duration)*1000)
	}

	fmt.Println("Total number of queries executed successfully:", queries)
	fmt.Println("Total number of local read executed successfully:", localReads)
	fmt.Println("Total number of remote reads executed successfully:", rmtReads)
	fmt.Println("Total number of updates executed successfully:", updsDone)
	fmt.Println("Total number of local updates executed successfully:", localUpds)
	fmt.Println("Total number of remote updates executed successfully:", rmtUpds)
	fmt.Println("Finished the test!")

}

func bothLocal(conns []net.Conn, queryStats []QueryStats, updStats []UpdateStats, lastStatQueries int, lastStatReads int, queries int, reads int, lastStatTime int64, startTxnTime int64, currUpdSpentTime int64, currQuerySpentTime int64, readTxns int, updTxns int, successQueries int, localReads int, rmtReads int, localUpds int, rmtUpds int, updsDone int) (newConns []net.Conn, newQueryStats []QueryStats, newUpdStats []UpdateStats, newLastStatQueries int, newLastStatReads int, newQueries int, newReads int, newLastStatTime int64, newStartTxnTime int64, newCurrUpdSpentTime int64, newCurrQuerySpentTime int64, newReadTxns int, newUpdTxns int, newSuccessQueries int, newLocalReads int, newRmtReads int, newLocalUpds int, newRmtUpds int, newUpdsDone int) {
	rand.Seed(time.Now().UnixNano())

	readParams := make([]antidote.ReadObjectParams, 2)
	updParams := make([]antidote.UpdateObjectParams, 2)
	orderRand1 := rand.Intn(len(procTables.Orders)-2) + 1
	orkey1 := procTables.Orders[orderRand1+1].O_ORDERKEY
	regKey1 := procTables.orderkeyToRegionkey(orkey1)

	var orkey2 int32 = -1
	var regKey2 int8 = -1
	for {
		orderRand2 := rand.Intn(len(procTables.Orders)-2) + 1
		orkey2 = procTables.Orders[orderRand2+1].O_ORDERKEY
		regKey2 = procTables.orderkeyToRegionkey(orkey2)
		if regKey2 == regKey1 {
			break
		}
	}

	startTxn := antidote.CreateStartTransaction(nil)
	antidote.SendProto(antidote.StartTrans, startTxn, conns[regKey1])
	_, txnReplyProto, _ := antidote.ReceiveProto(conns[regKey1])
	txnId := txnReplyProto.(*proto.ApbStartTransactionResp).GetTransactionDescriptor()
	startTxnTime = recordStartLatency()

	kParams1 := antidote.CreateKeyParams(tableNames[ORDERS]+strconv.Itoa(int(orkey1)), proto.CRDTType_RRMAP, buckets[regKey1])
	kParams2 := antidote.CreateKeyParams(tableNames[ORDERS]+strconv.Itoa(int(orkey2)), proto.CRDTType_RRMAP, buckets[regKey2])

	readParams = []antidote.ReadObjectParams{{
		KeyParams: kParams1,
		ReadArgs:  crdt.StateReadArguments{},
	}, {
		KeyParams: kParams2,
		ReadArgs:  crdt.StateReadArguments{},
	}}
	antidote.SendProto(antidote.ReadObjs, antidote.CreateReadObjs(txnId, readParams), conns[regKey1])
	protoTypeRead, _, _ := antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("FIRST READ:", protoTypeRead, protobufRead)
	if protoTypeRead == antidote.ReadObjsReply {
		successQueries++
		localReads += 2
		queries++
		reads++
		currQuerySpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStats(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime, readTxns, updTxns)
		currUpdSpentTime, currQuerySpentTime, readTxns, updTxns = 0, 0, 0, 0
	}

	var mapUpd crdt.UpdateArguments = crdt.EmbMapUpdate{Key: "-c", Upd: crdt.SetValue{NewValue: "a"}}
	updParams = []antidote.UpdateObjectParams{{
		KeyParams: antidote.KeyParams{Key: tableNames[ORDERS] + strconv.Itoa(int(orkey1)), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[regKey1]}, UpdateArgs: &mapUpd},
		{
			KeyParams: antidote.KeyParams{Key: tableNames[ORDERS] + strconv.Itoa(int(orkey2)), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[regKey2]}, UpdateArgs: &mapUpd},
	}
	antidote.SendProto(antidote.UpdateObjs, antidote.CreateUpdateObjs(txnId, updParams), conns[regKey1])
	protoTypeUp, protobufUp, _ := antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("FIRST UPDATE:", protoTypeUp, protobufUp)
	if protoTypeUp == antidote.OpReply && *protobufUp.(*proto.ApbOperationResp).Success {
		localUpds += 2
		updsDone++
		queries++
		currUpdSpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStats(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime, readTxns, updTxns)
		currUpdSpentTime, currQuerySpentTime, readTxns, updTxns = 0, 0, 0, 0
	}

	antidote.SendProto(antidote.ReadObjs, antidote.CreateReadObjs(txnId, readParams), conns[regKey1])
	protoTypeRead, _, _ = antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("SECOND READ:", protoTypeRead, protobufRead)
	if protoTypeRead == antidote.ReadObjsReply {
		successQueries++
		localReads += 2
		queries++
		reads++
		currQuerySpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStats(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime, readTxns, updTxns)
		currUpdSpentTime, currQuerySpentTime, readTxns, updTxns = 0, 0, 0, 0
	}

	mapUpd = crdt.EmbMapUpdate{Key: "-c", Upd: crdt.SetValue{NewValue: "z"}}
	updParams = []antidote.UpdateObjectParams{{
		KeyParams: antidote.KeyParams{Key: tableNames[ORDERS] + strconv.Itoa(int(orkey1)), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[regKey1]}, UpdateArgs: &mapUpd},
		{
			KeyParams: antidote.KeyParams{Key: tableNames[ORDERS] + strconv.Itoa(int(orkey2)), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[regKey2]}, UpdateArgs: &mapUpd},
	}
	antidote.SendProto(antidote.UpdateObjs, antidote.CreateUpdateObjs(txnId, updParams), conns[regKey1])
	protoTypeUp, protobufUp, _ = antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("SECOND UPDATE:", protoTypeUp, protobufUp)
	if protoTypeUp == antidote.OpReply && *protobufUp.(*proto.ApbOperationResp).Success {
		localUpds += 2
		updsDone++
		queries++
		updTxns++
		currUpdSpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStats(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime, readTxns, updTxns)
		currUpdSpentTime, currQuerySpentTime, readTxns, updTxns = 0, 0, 0, 0
	}

	antidote.SendProto(antidote.ReadObjs, antidote.CreateReadObjs(txnId, readParams), conns[regKey1])
	protoTypeRead, _, _ = antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("THIRD READ:", protoTypeRead, protobufRead)
	if protoTypeRead == antidote.ReadObjsReply {
		successQueries++
		localReads += 2
		queries++
		reads++
		readTxns++
		currQuerySpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStats(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime, readTxns, updTxns)
		currUpdSpentTime, currQuerySpentTime, readTxns, updTxns = 0, 0, 0, 0
	}

	antidote.SendProto(antidote.CommitTrans, antidote.CreateCommitTransaction(txnId), conns[regKey1])
	antidote.ReceiveProto(conns[regKey1])

	return conns, queryStats, updStats, lastStatQueries, lastStatReads, queries, reads, lastStatTime, startTxnTime, currUpdSpentTime, currQuerySpentTime, readTxns, updTxns, successQueries, localReads, rmtReads, localUpds, rmtUpds, updsDone
}

func order1Local2Remote(conns []net.Conn, queryStats []QueryStats, updStats []UpdateStats, lastStatQueries int, lastStatReads int, queries int, reads int, lastStatTime int64, startTxnTime int64, currUpdSpentTime int64, currQuerySpentTime int64, readTxns int, updTxns int, successQueries int, localReads int, rmtReads int, localUpds int, rmtUpds int, updsDone int) (newConns []net.Conn, newQueryStats []QueryStats, newUpdStats []UpdateStats, newLastStatQueries int, newLastStatReads int, newQueries int, newReads int, newLastStatTime int64, newStartTxnTime int64, newCurrUpdSpentTime int64, newCurrQuerySpentTime int64, newReadTxns int, newUpdTxns int, newSuccessQueries int, newLocalReads int, newRmtReads int, newLocalUpds int, newRmtUpds int, newUpdsDone int) {
	rand.Seed(time.Now().UnixNano())
	readParams := make([]antidote.ReadObjectParams, 2)
	updParams := make([]antidote.UpdateObjectParams, 2)

	orderRand1 := rand.Intn(len(procTables.Orders)-2) + 1
	orkey1 := procTables.Orders[orderRand1+1].O_ORDERKEY
	regKey1 := procTables.orderkeyToRegionkey(orkey1)

	var orkey2 int32 = -1
	var regKey2 int8 = -1
	for {
		orderRand2 := rand.Intn(len(procTables.Orders)-2) + 1
		orkey2 = procTables.Orders[orderRand2+1].O_ORDERKEY
		regKey2 = procTables.orderkeyToRegionkey(orkey2)
		if regKey2 != regKey1 {
			break
		}
	}

	startTxn := antidote.CreateStartTransaction(nil)
	antidote.SendProto(antidote.StartTrans, startTxn, conns[regKey1])
	_, txnReplyProto, _ := antidote.ReceiveProto(conns[regKey1])
	txnId := txnReplyProto.(*proto.ApbStartTransactionResp).GetTransactionDescriptor()
	startTxnTime = recordStartLatency()

	kParams1 := antidote.CreateKeyParams(tableNames[ORDERS]+strconv.Itoa(int(orkey1)), proto.CRDTType_RRMAP, buckets[regKey1])
	kParams2 := antidote.CreateKeyParams(tableNames[ORDERS]+strconv.Itoa(int(orkey2)), proto.CRDTType_RRMAP, buckets[regKey2])

	readParams = []antidote.ReadObjectParams{{
		KeyParams: kParams1,
		ReadArgs:  crdt.StateReadArguments{},
	}, {
		KeyParams: kParams2,
		ReadArgs:  crdt.StateReadArguments{},
	}}
	antidote.SendProto(antidote.ReadObjs, antidote.CreateReadObjs(txnId, readParams), conns[regKey1])
	protoTypeRead, _, _ := antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("FIRST READ:", protoTypeRead, protobufRead)
	if protoTypeRead == antidote.ReadObjsReply {
		successQueries++
		localReads++
		rmtReads++
		queries++
		reads++
		currQuerySpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStats(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime, readTxns, updTxns)
		currUpdSpentTime, currQuerySpentTime, readTxns, updTxns = 0, 0, 0, 0
	}

	var mapUpd crdt.UpdateArguments = crdt.EmbMapUpdate{Key: "-c", Upd: crdt.SetValue{NewValue: "a"}}
	updParams = []antidote.UpdateObjectParams{{
		KeyParams: antidote.KeyParams{Key: tableNames[ORDERS] + strconv.Itoa(int(orkey1)), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[regKey1]}, UpdateArgs: &mapUpd},
		{
			KeyParams: antidote.KeyParams{Key: tableNames[ORDERS] + strconv.Itoa(int(orkey2)), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[regKey2]}, UpdateArgs: &mapUpd},
	}
	antidote.SendProto(antidote.UpdateObjs, antidote.CreateUpdateObjs(txnId, updParams), conns[regKey1])
	protoTypeUp, protobufUp, _ := antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("FIRST UPDATE:", protoTypeUp, protobufUp)
	if protoTypeUp == antidote.OpReply && *protobufUp.(*proto.ApbOperationResp).Success {
		localUpds++
		rmtUpds++
		updsDone++
		queries++
		currUpdSpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStats(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime, readTxns, updTxns)
		currUpdSpentTime, currQuerySpentTime, readTxns, updTxns = 0, 0, 0, 0
	}

	antidote.SendProto(antidote.ReadObjs, antidote.CreateReadObjs(txnId, readParams), conns[regKey1])
	protoTypeRead, _, _ = antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("SECOND READ:", protoTypeRead, protobufRead)
	if protoTypeRead == antidote.ReadObjsReply {
		successQueries++
		localReads++
		rmtReads++
		queries++
		reads++
		currQuerySpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStats(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime, readTxns, updTxns)
		currUpdSpentTime, currQuerySpentTime, readTxns, updTxns = 0, 0, 0, 0
	}

	mapUpd = crdt.EmbMapUpdate{Key: "-c", Upd: crdt.SetValue{NewValue: "z"}}
	updParams = []antidote.UpdateObjectParams{{
		KeyParams: antidote.KeyParams{Key: tableNames[ORDERS] + strconv.Itoa(int(orkey1)), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[regKey1]}, UpdateArgs: &mapUpd},
		{
			KeyParams: antidote.KeyParams{Key: tableNames[ORDERS] + strconv.Itoa(int(orkey2)), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[regKey2]}, UpdateArgs: &mapUpd},
	}
	antidote.SendProto(antidote.UpdateObjs, antidote.CreateUpdateObjs(txnId, updParams), conns[regKey1])
	protoTypeUp, protobufUp, _ = antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("SECOND UPDATE:", protoTypeUp, protobufUp)
	if protoTypeUp == antidote.OpReply && *protobufUp.(*proto.ApbOperationResp).Success {
		localUpds++
		rmtUpds++
		updsDone++
		queries++
		updTxns++
		currUpdSpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStats(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime, readTxns, updTxns)
		currUpdSpentTime, currQuerySpentTime, readTxns, updTxns = 0, 0, 0, 0
	}

	antidote.SendProto(antidote.ReadObjs, antidote.CreateReadObjs(txnId, readParams), conns[regKey1])
	protoTypeRead, _, _ = antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("THIRD READ:", protoTypeRead, protobufRead)
	if protoTypeRead == antidote.ReadObjsReply {
		successQueries++
		localReads++
		rmtReads++
		queries++
		reads++
		readTxns++
		currQuerySpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStats(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime, readTxns, updTxns)
		currUpdSpentTime, currQuerySpentTime, readTxns, updTxns = 0, 0, 0, 0
	}

	antidote.SendProto(antidote.CommitTrans, antidote.CreateCommitTransaction(txnId), conns[regKey1])
	antidote.ReceiveProto(conns[regKey1])

	return conns, queryStats, updStats, lastStatQueries, lastStatReads, queries, reads, lastStatTime, startTxnTime, currUpdSpentTime, currQuerySpentTime, readTxns, updTxns, successQueries, localReads, rmtReads, localUpds, rmtUpds, updsDone
}

func order1Remote2Local(conns []net.Conn, queryStats []QueryStats, updStats []UpdateStats, lastStatQueries int, lastStatReads int, queries int, reads int, lastStatTime int64, startTxnTime int64, currUpdSpentTime int64, currQuerySpentTime int64, readTxns int, updTxns int, successQueries int, localReads int, rmtReads int, localUpds int, rmtUpds int, updsDone int) (newConns []net.Conn, newQueryStats []QueryStats, newUpdStats []UpdateStats, newLastStatQueries int, newLastStatReads int, newQueries int, newReads int, newLastStatTime int64, newStartTxnTime int64, newCurrUpdSpentTime int64, newCurrQuerySpentTime int64, newReadTxns int, newUpdTxns int, newSuccessQueries int, newLocalReads int, newRmtReads int, newLocalUpds int, newRmtUpds int, newUpdsDone int) {
	rand.Seed(time.Now().UnixNano())
	readParams := make([]antidote.ReadObjectParams, 2)
	updParams := make([]antidote.UpdateObjectParams, 2)

	orderRand2 := rand.Intn(len(procTables.Orders)-2) + 1
	orkey2 := procTables.Orders[orderRand2+1].O_ORDERKEY
	regKey2 := procTables.orderkeyToRegionkey(orkey2)

	var orkey1 int32 = -1
	var regKey1 int8 = -1
	for {
		orderRand1 := rand.Intn(len(procTables.Orders)-2) + 1
		orkey1 = procTables.Orders[orderRand1+1].O_ORDERKEY
		regKey1 = procTables.orderkeyToRegionkey(orkey1)
		if regKey2 != regKey1 {
			break
		}
	}

	startTxn := antidote.CreateStartTransaction(nil)
	antidote.SendProto(antidote.StartTrans, startTxn, conns[regKey1])
	_, txnReplyProto, _ := antidote.ReceiveProto(conns[regKey1])
	txnId := txnReplyProto.(*proto.ApbStartTransactionResp).GetTransactionDescriptor()
	startTxnTime = recordStartLatency()

	kParams1 := antidote.CreateKeyParams(tableNames[ORDERS]+strconv.Itoa(int(orkey1)), proto.CRDTType_RRMAP, buckets[regKey1])
	kParams2 := antidote.CreateKeyParams(tableNames[ORDERS]+strconv.Itoa(int(orkey2)), proto.CRDTType_RRMAP, buckets[regKey2])

	readParams = []antidote.ReadObjectParams{{
		KeyParams: kParams1,
		ReadArgs:  crdt.StateReadArguments{},
	}, {
		KeyParams: kParams2,
		ReadArgs:  crdt.StateReadArguments{},
	}}
	antidote.SendProto(antidote.ReadObjs, antidote.CreateReadObjs(txnId, readParams), conns[regKey1])
	protoTypeRead, _, _ := antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("FIRST READ:", protoTypeRead, protobufRead)
	if protoTypeRead == antidote.ReadObjsReply {
		successQueries++
		localReads++
		rmtReads++
		queries++
		reads++
		currQuerySpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStats(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime, readTxns, updTxns)
		currUpdSpentTime, currQuerySpentTime, readTxns, updTxns = 0, 0, 0, 0
	}

	var mapUpd crdt.UpdateArguments = crdt.EmbMapUpdate{Key: "-c", Upd: crdt.SetValue{NewValue: "a"}}
	updParams = []antidote.UpdateObjectParams{{
		KeyParams: antidote.KeyParams{Key: tableNames[ORDERS] + strconv.Itoa(int(orkey1)), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[regKey1]}, UpdateArgs: &mapUpd},
		{
			KeyParams: antidote.KeyParams{Key: tableNames[ORDERS] + strconv.Itoa(int(orkey2)), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[regKey2]}, UpdateArgs: &mapUpd},
	}
	antidote.SendProto(antidote.UpdateObjs, antidote.CreateUpdateObjs(txnId, updParams), conns[regKey1])
	protoTypeUp, protobufUp, _ := antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("FIRST UPDATE:", protoTypeUp, protobufUp)
	if protoTypeUp == antidote.OpReply && *protobufUp.(*proto.ApbOperationResp).Success {
		localUpds++
		rmtUpds++
		updsDone++
		queries++
		currUpdSpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStats(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime, readTxns, updTxns)
		currUpdSpentTime, currQuerySpentTime, readTxns, updTxns = 0, 0, 0, 0
	}

	antidote.SendProto(antidote.ReadObjs, antidote.CreateReadObjs(txnId, readParams), conns[regKey1])
	protoTypeRead, _, _ = antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("SECOND READ:", protoTypeRead, protobufRead)
	if protoTypeRead == antidote.ReadObjsReply {
		successQueries++
		localReads++
		rmtReads++
		queries++
		reads++
		currQuerySpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStats(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime, readTxns, updTxns)
		currUpdSpentTime, currQuerySpentTime, readTxns, updTxns = 0, 0, 0, 0
	}

	mapUpd = crdt.EmbMapUpdate{Key: "-c", Upd: crdt.SetValue{NewValue: "z"}}
	updParams = []antidote.UpdateObjectParams{{
		KeyParams: antidote.KeyParams{Key: tableNames[ORDERS] + strconv.Itoa(int(orkey1)), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[regKey1]}, UpdateArgs: &mapUpd},
		{
			KeyParams: antidote.KeyParams{Key: tableNames[ORDERS] + strconv.Itoa(int(orkey2)), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[regKey2]}, UpdateArgs: &mapUpd},
	}
	antidote.SendProto(antidote.UpdateObjs, antidote.CreateUpdateObjs(txnId, updParams), conns[regKey1])
	protoTypeUp, protobufUp, _ = antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("SECOND UPDATE:", protoTypeUp, protobufUp)
	if protoTypeUp == antidote.OpReply && *protobufUp.(*proto.ApbOperationResp).Success {
		localUpds++
		rmtUpds++
		updsDone++
		queries++
		updTxns++
		currUpdSpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStats(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime, readTxns, updTxns)
		currUpdSpentTime, currQuerySpentTime, readTxns, updTxns = 0, 0, 0, 0
	}

	antidote.SendProto(antidote.ReadObjs, antidote.CreateReadObjs(txnId, readParams), conns[regKey1])
	protoTypeRead, _, _ = antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("THIRD READ:", protoTypeRead, protobufRead)
	if protoTypeRead == antidote.ReadObjsReply {
		successQueries++
		localReads++
		rmtReads++
		queries++
		reads++
		readTxns++
		currQuerySpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStats(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime, readTxns, updTxns)
		currUpdSpentTime, currQuerySpentTime, readTxns, updTxns = 0, 0, 0, 0
	}

	antidote.SendProto(antidote.CommitTrans, antidote.CreateCommitTransaction(txnId), conns[regKey1])
	antidote.ReceiveProto(conns[regKey1])

	return conns, queryStats, updStats, lastStatQueries, lastStatReads, queries, reads, lastStatTime, startTxnTime, currUpdSpentTime, currQuerySpentTime, readTxns, updTxns, successQueries, localReads, rmtReads, localUpds, rmtUpds, updsDone
}

func bothRemote(conns []net.Conn, queryStats []QueryStats, updStats []UpdateStats, lastStatQueries int, lastStatReads int, queries int, reads int, lastStatTime int64, startTxnTime int64, currUpdSpentTime int64, currQuerySpentTime int64, readTxns int, updTxns int, successQueries int, localReads int, rmtReads int, localUpds int, rmtUpds int, updsDone int) (newConns []net.Conn, newQueryStats []QueryStats, newUpdStats []UpdateStats, newLastStatQueries int, newLastStatReads int, newQueries int, newReads int, newLastStatTime int64, newStartTxnTime int64, newCurrUpdSpentTime int64, newCurrQuerySpentTime int64, newReadTxns int, newUpdTxns int, newSuccessQueries int, newLocalReads int, newRmtReads int, newLocalUpds int, newRmtUpds int, newUpdsDone int) {
	rand.Seed(time.Now().UnixNano())
	readParams := make([]antidote.ReadObjectParams, 2)
	updParams := make([]antidote.UpdateObjectParams, 2)

	orderRand1 := rand.Intn(len(procTables.Orders)-2) + 1
	orkey1 := procTables.Orders[orderRand1+1].O_ORDERKEY
	regKey1 := procTables.orderkeyToRegionkey(orkey1)
	regKey := (regKey1 + 1) % int8(len(procTables.Regions))

	var orkey2 int32 = -1
	var regKey2 int8 = -1
	for {
		orderRand2 := rand.Intn(len(procTables.Orders)-2) + 1
		orkey2 = procTables.Orders[orderRand2+1].O_ORDERKEY
		regKey2 = procTables.orderkeyToRegionkey(orkey2)
		if regKey2 != regKey {
			break
		}
	}

	startTxn := antidote.CreateStartTransaction(nil)
	antidote.SendProto(antidote.StartTrans, startTxn, conns[regKey1])
	_, txnReplyProto, _ := antidote.ReceiveProto(conns[regKey1])
	txnId := txnReplyProto.(*proto.ApbStartTransactionResp).GetTransactionDescriptor()
	startTxnTime = recordStartLatency()

	kParams1 := antidote.CreateKeyParams(tableNames[ORDERS]+strconv.Itoa(int(orkey1)), proto.CRDTType_RRMAP, buckets[regKey1])
	kParams2 := antidote.CreateKeyParams(tableNames[ORDERS]+strconv.Itoa(int(orkey2)), proto.CRDTType_RRMAP, buckets[regKey2])

	readParams = []antidote.ReadObjectParams{{
		KeyParams: kParams1,
		ReadArgs:  crdt.StateReadArguments{},
	}, {
		KeyParams: kParams2,
		ReadArgs:  crdt.StateReadArguments{},
	}}
	antidote.SendProto(antidote.ReadObjs, antidote.CreateReadObjs(txnId, readParams), conns[regKey1])
	protoTypeRead, _, _ := antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("FIRST READ:", protoTypeRead, protobufRead)
	if protoTypeRead == antidote.ReadObjsReply {
		successQueries++
		queries++
		reads++
		rmtReads += 2
		currQuerySpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStats(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime, readTxns, updTxns)
		currUpdSpentTime, currQuerySpentTime, readTxns, updTxns = 0, 0, 0, 0
	}

	var mapUpd crdt.UpdateArguments = crdt.EmbMapUpdate{Key: "-c", Upd: crdt.SetValue{NewValue: "a"}}
	updParams = []antidote.UpdateObjectParams{{
		KeyParams: antidote.KeyParams{Key: tableNames[ORDERS] + strconv.Itoa(int(orkey1)), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[regKey1]}, UpdateArgs: &mapUpd},
		{
			KeyParams: antidote.KeyParams{Key: tableNames[ORDERS] + strconv.Itoa(int(orkey2)), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[regKey2]}, UpdateArgs: &mapUpd},
	}
	antidote.SendProto(antidote.UpdateObjs, antidote.CreateUpdateObjs(txnId, updParams), conns[regKey1])
	protoTypeUp, protobufUp, _ := antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("FIRST UPDATE:", protoTypeUp, protobufUp)
	if protoTypeUp == antidote.OpReply && *protobufUp.(*proto.ApbOperationResp).Success {
		updsDone++
		queries++
		rmtUpds += 2
		currUpdSpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStats(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime, readTxns, updTxns)
		currUpdSpentTime, currQuerySpentTime, readTxns, updTxns = 0, 0, 0, 0
	}

	antidote.SendProto(antidote.ReadObjs, antidote.CreateReadObjs(txnId, readParams), conns[regKey1])
	protoTypeRead, _, _ = antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("SECOND READ:", protoTypeRead, protobufRead)
	if protoTypeRead == antidote.ReadObjsReply {
		successQueries++
		queries++
		reads++
		rmtReads += 2
		currQuerySpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStats(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime, readTxns, updTxns)
		currUpdSpentTime, currQuerySpentTime, readTxns, updTxns = 0, 0, 0, 0
	}

	mapUpd = crdt.EmbMapUpdate{Key: "-c", Upd: crdt.SetValue{NewValue: "z"}}
	updParams = []antidote.UpdateObjectParams{{
		KeyParams: antidote.KeyParams{Key: tableNames[ORDERS] + strconv.Itoa(int(orkey1)), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[regKey1]}, UpdateArgs: &mapUpd},
		{
			KeyParams: antidote.KeyParams{Key: tableNames[ORDERS] + strconv.Itoa(int(orkey2)), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[regKey2]}, UpdateArgs: &mapUpd},
	}
	antidote.SendProto(antidote.UpdateObjs, antidote.CreateUpdateObjs(txnId, updParams), conns[regKey1])
	protoTypeUp, protobufUp, _ = antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("SECOND UPDATE:", protoTypeUp, protobufUp)
	if protoTypeUp == antidote.OpReply && *protobufUp.(*proto.ApbOperationResp).Success {
		updsDone++
		queries++
		updTxns++
		rmtUpds += 2
		currUpdSpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStats(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime, readTxns, updTxns)
		currUpdSpentTime, currQuerySpentTime, readTxns, updTxns = 0, 0, 0, 0
	}

	antidote.SendProto(antidote.ReadObjs, antidote.CreateReadObjs(txnId, readParams), conns[regKey1])
	protoTypeRead, _, _ = antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("THIRD READ:", protoTypeRead, protobufRead)
	if protoTypeRead == antidote.ReadObjsReply {
		successQueries++
		queries++
		reads++
		readTxns++
		rmtReads += 2
		currQuerySpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStats(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime, readTxns, updTxns)
		currUpdSpentTime, currQuerySpentTime, readTxns, updTxns = 0, 0, 0, 0
	}

	antidote.SendProto(antidote.CommitTrans, antidote.CreateCommitTransaction(txnId), conns[regKey1])
	antidote.ReceiveProto(conns[regKey1])

	return conns, queryStats, updStats, lastStatQueries, lastStatReads, queries, reads, lastStatTime, startTxnTime, currUpdSpentTime, currQuerySpentTime, readTxns, updTxns, successQueries, localReads, rmtReads, localUpds, rmtUpds, updsDone

}
