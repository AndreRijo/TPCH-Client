package tpch

import (
	"encoding/csv"
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"potionDB/src/antidote"
	"potionDB/src/crdt"
	"potionDB/src/proto"
	"strconv"
	"time"
)

const (
	NR_THREADS = 20
	DURATION   = 2
)

type MyClientResult struct {
	QueryClientResult
	updStats   []UpdateStats
	ratio      int
	nTxns      int
	localReads int
	rmtReads   int
	localUpds  int
	rmtUpds    int
}

func startMyTest() {
	time.Sleep(20 * time.Second)
	chans := make([][]chan MyClientResult, NR_THREADS)
	results := make([][]MyClientResult, NR_THREADS)
	localRatio := [11]int{100, 90, 80, 70, 60, 50, 40, 30, 20, 10, 0}

	for i := range chans {
		chans[i] = make([]chan MyClientResult, len(localRatio))
		results[i] = make([]MyClientResult, len(localRatio))
		for j := range chans[i] {
			chans[i][j] = make(chan MyClientResult)
		}
	}
	for i := 0; i < NR_THREADS; i++ {
		//go interFunc(chans[i])
		go func(i int) {
			resultsByClient := make([]MyClientResult, len(localRatio))
			for ratioIdx, ratio := range localRatio {
				fmt.Println("RATIO:", ratio)
				fmt.Println("I INSIDE GO FUNC:", i)
				resultsByClient = myTestGlobalAux2(ratioIdx, ratio, resultsByClient)
			}
			//fmt.Println("LEN RESULTS BY CLIENT:", len(resultsByClient))
			for j, client := range resultsByClient {
				chans[i][j] <- client
			}
		}(i)
	}

	go func() {
		for i, channel := range chans {
			for j := range channel {
				results[i][j] = <-channel[j]
			}
		}
		//Notify update channels that there's no further updates
		/*
			for _, channel := range channels.updateChans {
				channel <- QueuedMsgWithStat{QueuedMsg: QueuedMsg{code: QUEUE_COMPLETE}}
			}
		*/
		//fmt.Printf("Time (ms) from test end until all clients replied: %d (ms)\n", (time.Now().UnixNano()-stopTime)/1000000)
		fmt.Println("All query/upd clients have finished.")
		//totalQueries, totalReads, avgDuration, totalUpds, nFuncs := 0.0, 0.0, 0.0, 0.0, float64(len(queryFuncs))
		for i, resSlice := range results {
			for _, result := range resSlice {

				fmt.Println("********TESTE DA SOFIA********")
				fmt.Printf("Thread %d for a ratio of %d local operations to a %d remote\n", i, result.ratio, 100-result.ratio)
				latency := result.duration / float64(result.nTxns*5)
				fmt.Println("Average latency:", latency, "ms")
				fmt.Println("Number of ops:", 5*result.nTxns)

				fmt.Println("Total number of local read executed successfully:", result.localReads)
				fmt.Println("Total number of remote reads executed successfully:", result.rmtReads)
				fmt.Println("Total number of local updates executed successfully:", result.localUpds)
				fmt.Println("Total number of remote updates executed successfully:", result.rmtUpds)

			}
			writeMyStatsFile(resSlice)
		}
		os.Exit(0)
	}()

}

func myTestGlobalAux2(ratioIdx int, ratio int, resultsByClient []MyClientResult) []MyClientResult {
	conns := make([]net.Conn, len(servers))
	for i := range conns {
		fmt.Println("Connecting to ", servers[i])
		conns[i], _ = net.Dial("tcp", servers[i])
	}
	startTime := time.Now().UnixNano() / 1000000
	lastStatTime := startTime
	queryStats := make([]QueryStats, 0, DURATION/int64(statisticsInterval)+1)
	updStats := make([]UpdateStats, 0, DURATION/int64(statisticsInterval)+1)
	lastStatQueries, lastStatReads := 0, 0
	queries, reads := 0, 0
	startTxnTime, currUpdSpentTime, currQuerySpentTime := int64(0), int64(0), int64(0)
	localReads, rmtReads, localUpds, rmtUpds, nTxns := 0, 0, 0, 0, 0

	fmt.Println("Starting the Sofia test...")
	//for start := time.Now(); time.Since(start) < time.Minute; {
	for start := time.Now(); time.Since(start) < time.Minute*DURATION; {
		rand.Seed(time.Now().UnixNano())
		isLocal := rand.Intn(100) <= ratio
		if isLocal {
			conns, queryStats, updStats, lastStatQueries, lastStatReads, queries, reads, lastStatTime, startTxnTime, currUpdSpentTime, currQuerySpentTime, localReads, rmtReads, localUpds, rmtUpds = bothLocal2(conns, queryStats, updStats, lastStatQueries, lastStatReads, queries, reads, lastStatTime, startTxnTime, currUpdSpentTime, currQuerySpentTime, localReads, rmtReads, localUpds, rmtUpds)
			nTxns++
		} else {
			numLocal := rand.Intn(3)
			if numLocal == 0 {
				conns, queryStats, updStats, lastStatQueries, lastStatReads, queries, reads, lastStatTime, startTxnTime, currUpdSpentTime, currQuerySpentTime, localReads, rmtReads, localUpds, rmtUpds = bothRemote2(conns, queryStats, updStats, lastStatQueries, lastStatReads, queries, reads, lastStatTime, startTxnTime, currUpdSpentTime, currQuerySpentTime, localReads, rmtReads, localUpds, rmtUpds)
				nTxns++
			} else if numLocal == 1 {
				conns, queryStats, updStats, lastStatQueries, lastStatReads, queries, reads, lastStatTime, startTxnTime, currUpdSpentTime, currQuerySpentTime, localReads, rmtReads, localUpds, rmtUpds = order1Local2Remote2(conns, queryStats, updStats, lastStatQueries, lastStatReads, queries, reads, lastStatTime, startTxnTime, currUpdSpentTime, currQuerySpentTime, localReads, rmtReads, localUpds, rmtUpds)
				nTxns++
			} else {
				conns, queryStats, updStats, lastStatQueries, lastStatReads, queries, reads, lastStatTime, startTxnTime, currUpdSpentTime, currQuerySpentTime, localReads, rmtReads, localUpds, rmtUpds = order1Remote2Local2(conns, queryStats, updStats, lastStatQueries, lastStatReads, queries, reads, lastStatTime, startTxnTime, currUpdSpentTime, currQuerySpentTime, localReads, rmtReads, localUpds, rmtUpds)
				nTxns++
			}
		}
	}
	endTime := time.Now().UnixNano() / 1000000

	resultsByClient[ratioIdx] = MyClientResult{
		QueryClientResult: QueryClientResult{duration: float64(endTime - startTime), nQueries: float64(queries),
			nReads: float64(reads), intermediateResults: queryStats},
		updStats:   updStats,
		ratio:      ratio,
		nTxns:      nTxns,
		localReads: localReads,
		rmtReads:   rmtReads,
		localUpds:  localUpds,
		rmtUpds:    rmtUpds,
	}

	return resultsByClient
}

func order1Remote2Local2(conns []net.Conn, queryStats []QueryStats, updStats []UpdateStats, lastStatQueries int, lastStatReads int, queries int, reads int, lastStatTime int64, startTxnTime int64, currUpdSpentTime int64, currQuerySpentTime int64, localReads int, rmtReads int, localUpds int, rmtUpds int) ([]net.Conn, []QueryStats, []UpdateStats, int, int, int, int, int64, int64, int64, int64, int, int, int, int) {
	//fmt.Println("1remote2local")
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
		localReads++
		rmtReads++
		queries++
		reads++
		currQuerySpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStatsNew(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime)
		currUpdSpentTime, currQuerySpentTime = 0, 0
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
		queries++
		currUpdSpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStatsNew(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime)
		currUpdSpentTime, currQuerySpentTime = 0, 0
	}

	antidote.SendProto(antidote.ReadObjs, antidote.CreateReadObjs(txnId, readParams), conns[regKey1])
	protoTypeRead, _, _ = antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("SECOND READ:", protoTypeRead, protobufRead)
	if protoTypeRead == antidote.ReadObjsReply {
		localReads++
		rmtReads++
		queries++
		reads++
		currQuerySpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStatsNew(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime)
		currUpdSpentTime, currQuerySpentTime = 0, 0
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
		queries++
		currUpdSpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStatsNew(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime)
		currUpdSpentTime, currQuerySpentTime = 0, 0
	}

	antidote.SendProto(antidote.ReadObjs, antidote.CreateReadObjs(txnId, readParams), conns[regKey1])
	protoTypeRead, _, _ = antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("THIRD READ:", protoTypeRead, protobufRead)
	if protoTypeRead == antidote.ReadObjsReply {
		localReads++
		rmtReads++
		queries++
		reads++
		currQuerySpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStatsNew(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime)
		currUpdSpentTime, currQuerySpentTime = 0, 0
	}

	antidote.SendProto(antidote.CommitTrans, antidote.CreateCommitTransaction(txnId), conns[regKey1])
	antidote.ReceiveProto(conns[regKey1])

	return conns, queryStats, updStats, lastStatQueries, lastStatReads, queries, reads, lastStatTime, startTxnTime, currUpdSpentTime, currQuerySpentTime, localReads, rmtReads, localUpds, rmtUpds
}

func order1Local2Remote2(conns []net.Conn, queryStats []QueryStats, updStats []UpdateStats, lastStatQueries int, lastStatReads int, queries int, reads int, lastStatTime int64, startTxnTime int64, currUpdSpentTime int64, currQuerySpentTime int64, localReads int, rmtReads int, localUpds int, rmtUpds int) ([]net.Conn, []QueryStats, []UpdateStats, int, int, int, int, int64, int64, int64, int64, int, int, int, int) {
	//fmt.Println("1local2remote")
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
	//fmt.Println("Start txn ok")
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
	//fmt.Println("First read ok")
	//fmt.Println("FIRST READ:", protoTypeRead, protobufRead)
	if protoTypeRead == antidote.ReadObjsReply {
		localReads++
		rmtReads++
		queries++
		reads++
		currQuerySpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStatsNew(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime)
		currUpdSpentTime, currQuerySpentTime = 0, 0
	}

	var mapUpd crdt.UpdateArguments = crdt.EmbMapUpdate{Key: "-c", Upd: crdt.SetValue{NewValue: "a"}}
	updParams = []antidote.UpdateObjectParams{{
		KeyParams: antidote.KeyParams{Key: tableNames[ORDERS] + strconv.Itoa(int(orkey1)), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[regKey1]}, UpdateArgs: &mapUpd},
		{
			KeyParams: antidote.KeyParams{Key: tableNames[ORDERS] + strconv.Itoa(int(orkey2)), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[regKey2]}, UpdateArgs: &mapUpd},
	}
	antidote.SendProto(antidote.UpdateObjs, antidote.CreateUpdateObjs(txnId, updParams), conns[regKey1])
	protoTypeUp, protobufUp, _ := antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("First upd ok")
	//fmt.Println("FIRST UPDATE:", protoTypeUp, protobufUp)
	if protoTypeUp == antidote.OpReply && *protobufUp.(*proto.ApbOperationResp).Success {
		localUpds++
		rmtUpds++
		queries++
		currUpdSpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStatsNew(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime)
		currUpdSpentTime, currQuerySpentTime = 0, 0
	}

	antidote.SendProto(antidote.ReadObjs, antidote.CreateReadObjs(txnId, readParams), conns[regKey1])
	protoTypeRead, _, _ = antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("Second read ok")
	//fmt.Println("SECOND READ:", protoTypeRead, protobufRead)
	if protoTypeRead == antidote.ReadObjsReply {
		localReads++
		rmtReads++
		queries++
		reads++
		currQuerySpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStatsNew(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime)
		currUpdSpentTime, currQuerySpentTime = 0, 0
	}

	mapUpd = crdt.EmbMapUpdate{Key: "-c", Upd: crdt.SetValue{NewValue: "z"}}
	updParams = []antidote.UpdateObjectParams{{
		KeyParams: antidote.KeyParams{Key: tableNames[ORDERS] + strconv.Itoa(int(orkey1)), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[regKey1]}, UpdateArgs: &mapUpd},
		{
			KeyParams: antidote.KeyParams{Key: tableNames[ORDERS] + strconv.Itoa(int(orkey2)), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[regKey2]}, UpdateArgs: &mapUpd},
	}
	antidote.SendProto(antidote.UpdateObjs, antidote.CreateUpdateObjs(txnId, updParams), conns[regKey1])
	protoTypeUp, protobufUp, _ = antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("Second upd ok")
	//fmt.Println("SECOND UPDATE:", protoTypeUp, protobufUp)
	if protoTypeUp == antidote.OpReply && *protobufUp.(*proto.ApbOperationResp).Success {
		localUpds++
		rmtUpds++
		queries++
		currUpdSpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStatsNew(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime)
		currUpdSpentTime, currQuerySpentTime = 0, 0
	}

	antidote.SendProto(antidote.ReadObjs, antidote.CreateReadObjs(txnId, readParams), conns[regKey1])
	protoTypeRead, _, _ = antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("Third read ok")
	//fmt.Println("THIRD READ:", protoTypeRead, protobufRead)
	if protoTypeRead == antidote.ReadObjsReply {
		localReads++
		rmtReads++
		queries++
		reads++
		currQuerySpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStatsNew(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime)
		currUpdSpentTime, currQuerySpentTime = 0, 0
	}

	antidote.SendProto(antidote.CommitTrans, antidote.CreateCommitTransaction(txnId), conns[regKey1])
	antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("Commit ok")

	return conns, queryStats, updStats, lastStatQueries, lastStatReads, queries, reads, lastStatTime, startTxnTime, currUpdSpentTime, currQuerySpentTime, localReads, rmtReads, localUpds, rmtUpds

}

func bothRemote2(conns []net.Conn, queryStats []QueryStats, updStats []UpdateStats, lastStatQueries int, lastStatReads int, queries int, reads int, lastStatTime int64, startTxnTime int64, currUpdSpentTime int64, currQuerySpentTime int64, localReads int, rmtReads int, localUpds int, rmtUpds int) ([]net.Conn, []QueryStats, []UpdateStats, int, int, int, int, int64, int64, int64, int64, int, int, int, int) {
	//fmt.Println("2remote")
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
		queries++
		reads++
		rmtReads += 2
		currQuerySpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStatsNew(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime)
		currUpdSpentTime, currQuerySpentTime = 0, 0
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
		queries++
		rmtUpds += 2
		currUpdSpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStatsNew(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime)
		currUpdSpentTime, currQuerySpentTime = 0, 0
	}

	antidote.SendProto(antidote.ReadObjs, antidote.CreateReadObjs(txnId, readParams), conns[regKey1])
	protoTypeRead, _, _ = antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("SECOND READ:", protoTypeRead, protobufRead)
	if protoTypeRead == antidote.ReadObjsReply {
		queries++
		reads++
		rmtReads += 2
		currQuerySpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStatsNew(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime)
		currUpdSpentTime, currQuerySpentTime = 0, 0
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
		queries++
		rmtUpds += 2
		currUpdSpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStatsNew(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime)
		currUpdSpentTime, currQuerySpentTime = 0, 0
	}

	antidote.SendProto(antidote.ReadObjs, antidote.CreateReadObjs(txnId, readParams), conns[regKey1])
	protoTypeRead, _, _ = antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("THIRD READ:", protoTypeRead, protobufRead)
	if protoTypeRead == antidote.ReadObjsReply {
		queries++
		reads++
		rmtReads += 2
		currQuerySpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStatsNew(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime)
		currUpdSpentTime, currQuerySpentTime = 0, 0
	}

	antidote.SendProto(antidote.CommitTrans, antidote.CreateCommitTransaction(txnId), conns[regKey1])
	antidote.ReceiveProto(conns[regKey1])

	return conns, queryStats, updStats, lastStatQueries, lastStatReads, queries, reads, lastStatTime, startTxnTime, currUpdSpentTime, currQuerySpentTime, localReads, rmtReads, localUpds, rmtUpds

}

func bothLocal2(conns []net.Conn, queryStats []QueryStats, updStats []UpdateStats, lastStatQueries int, lastStatReads int, queries int, reads int, lastStatTime int64, startTxnTime int64, currUpdSpentTime int64, currQuerySpentTime int64, localReads int, rmtReads int, localUpds int, rmtUpds int) ([]net.Conn, []QueryStats, []UpdateStats, int, int, int, int, int64, int64, int64, int64, int, int, int, int) {
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
		localReads += 2
		queries++
		reads++
		currQuerySpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStatsNew(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime)
		currUpdSpentTime, currQuerySpentTime = 0, 0
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
		queries++
		currUpdSpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStatsNew(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime)
		currUpdSpentTime, currQuerySpentTime = 0, 0
	}

	antidote.SendProto(antidote.ReadObjs, antidote.CreateReadObjs(txnId, readParams), conns[regKey1])
	protoTypeRead, _, _ = antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("SECOND READ:", protoTypeRead, protobufRead)
	if protoTypeRead == antidote.ReadObjsReply {
		localReads += 2
		queries++
		reads++
		currQuerySpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStatsNew(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime)
		currUpdSpentTime, currQuerySpentTime = 0, 0
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
		queries++
		currUpdSpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStatsNew(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime)
		currUpdSpentTime, currQuerySpentTime = 0, 0
	}

	antidote.SendProto(antidote.ReadObjs, antidote.CreateReadObjs(txnId, readParams), conns[regKey1])
	protoTypeRead, _, _ = antidote.ReceiveProto(conns[regKey1])
	//fmt.Println("THIRD READ:", protoTypeRead, protobufRead)
	if protoTypeRead == antidote.ReadObjsReply {
		localReads += 2
		queries++
		reads++
		currQuerySpentTime += recordFinishLatency(startTxnTime)
		queryStats, updStats, lastStatReads, lastStatQueries, lastStatTime = updateMyQueryStatsNew(queryStats, updStats, reads, lastStatReads, queries, lastStatQueries, lastStatTime, currQuerySpentTime, currUpdSpentTime)
		currUpdSpentTime, currQuerySpentTime = 0, 0
	}

	antidote.SendProto(antidote.CommitTrans, antidote.CreateCommitTransaction(txnId), conns[regKey1])
	antidote.ReceiveProto(conns[regKey1])

	return conns, queryStats, updStats, lastStatQueries, lastStatReads, queries, reads, lastStatTime, startTxnTime, currUpdSpentTime, currQuerySpentTime, localReads, rmtReads, localUpds, rmtUpds

}

func updateMyQueryStatsNew(stats []QueryStats, updStats []UpdateStats, nReads, lastStatReads, nQueries, lastStatQueries int, lastStatTime int64, qTime, updTime int64) (newStats []QueryStats, newUStats []UpdateStats,
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
		updStats[lastStatI].latency += (updTime / 1000000)
	} else {
		currQueryStats.nReads, currQueryStats.nQueries, currQueryStats.timeSpent, currQueryStats.latency = diffR, diffQ, diffT, qTime/1000000
		stats = append(stats, currQueryStats)
		var updSt = UpdateStats{}
		updSt.latency = updTime / 1000000
		updStats = append(updStats, updSt)
	}
	return stats, updStats, nReads, nQueries, currStatTime
}

func convertMyStats(stats []MyClientResult) (qStats [][]QueryStats, uStats [][]UpdateStats, localUpds []int, localReads []int, rmtUpds []int, rmtReads []int, nTxns []int, duration []float64) {
	sizeToUse := int(math.MaxInt32)
	for _, mixStats := range stats {
		if len(mixStats.intermediateResults) < sizeToUse {
			sizeToUse = len(mixStats.intermediateResults)
		}
	}
	qStats, uStats = make([][]QueryStats, sizeToUse), make([][]UpdateStats, sizeToUse)
	var currQSlice []QueryStats
	var currUSlice []UpdateStats
	localUpds, localReads, rmtUpds, rmtReads, nTxns = make([]int, 1), make([]int, 1), make([]int, 1), make([]int, 1), make([]int, 1)
	duration = make([]float64, 1)

	for i := range qStats {
		currQSlice, currUSlice = make([]QueryStats, len(stats)), make([]UpdateStats, len(stats))
		for j, stat := range stats {
			currQSlice[j], currUSlice[j] = stat.intermediateResults[i], stat.updStats[i]
		}
		qStats[i], uStats[i] = currQSlice, currUSlice
	}
	for _, stat := range stats {
		localUpds = append(localUpds, stat.localUpds)
		localReads = append(localReads, stat.localReads)
		rmtUpds = append(rmtUpds, stat.rmtUpds)
		rmtReads = append(rmtReads, stat.rmtReads)
		nTxns = append(nTxns, stat.nTxns)
		duration = append(duration, stat.duration)
	}

	return
}

func writeMyStatsFile(stats []MyClientResult) {

	_, _, localUpds, localReads, rmtUpds, rmtReads, nTxns, duration := convertMyStats(stats)

	totalData := make([][]string, len(localUpds)) //space for final data as well

	header := []string{"Total time", "Section time", "Local reads", "Remote reads", "Local updates", "Remote updates",
		"Ops", "OpsVer", "Ops/s", "Txns", "Txn/s", "Average latency(ms)"}

	totalTime := float64(0)

	for i := range localUpds {
		totalTime += duration[i]
		ops := nTxns[i] * 5
		opsPerSec := float64(ops) / duration[i]
		txnPerSec := float64(nTxns[i]) / duration[i]
		latency := duration[i] / float64(ops)
		totalData[i] = []string{strconv.FormatFloat(totalTime, 'f', 10, 64), strconv.FormatFloat(duration[i], 'f', 10, 64),
			strconv.FormatInt(int64(localReads[i]), 10), strconv.FormatInt(int64(rmtReads[i]), 10), strconv.FormatInt(int64(localUpds[i]), 10),
			strconv.FormatInt(int64(rmtUpds[i]), 10), strconv.FormatInt(int64(ops), 10), strconv.FormatInt(int64(localReads[i]+rmtReads[i]+localUpds[i]+rmtUpds[i])/2, 10),
			strconv.FormatFloat(opsPerSec, 'f', 10, 64), strconv.FormatInt(int64(nTxns[i]), 10), strconv.FormatFloat(txnPerSec, 'f', 10, 64),
			strconv.FormatFloat(latency, 'f', 10, 64)}

	}

	file := getStatsFileToWrite("mixStats")
	if file == nil {
		return
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	writer.Comma = ';'
	defer writer.Flush()

	writer.Write(header)
	for _, line := range totalData {
		writer.Write(line)
	}

	fmt.Println("Mix statistics saved successfully to " + file.Name())
}
