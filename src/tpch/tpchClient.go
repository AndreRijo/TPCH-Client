package tpch

import (
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"syscall"
	"time"
	"tools"
)

/*
	Server types:
	A) Multi-server, global views (default PotionDB)
	B) Multi-server, local views
	C) Multi-server + "know-all-server", with all index CRDTs being in the "know-all-server"
	that also replicates all the data
	D) (opptional) Single-server

	We can easily have PotionDB simulate these 3 types just by adapting the tpchClient.
	Preferably we can even use the same data load, communication and queries code...
	We just need to adapt variables and some helper communication methods *may* do different
	actions depending on the server type (e.g: in queries, when sending indexes, for A) and C)
	it sends a single request, for B) it splits the request).

	One detail about C) - we just need to make the central server replicate every bucket.
	There's actually no change required in the client - just need to set the server configurations
	correctly.

	Preferably we'd also support D) single-server structure, for debugging/baseline purposes.

	But what exactly do we need to change?

	***Base data uploading*** (DONE, UNTESTED. Did both 2) and 3))
		- No changes to support A), B), C) - data is always partitioned in buckets. For D), there's
		at least 3 options: 1) keep region buckets and open 1 connection to the server per region bucket;
		2) use a different set of regionFuncs that always return the same bucket, which might be more tricky
		as the association bucket - server is made by the index; 3) when starting to prepare the protobufs,
		use prepareSendAny().

	***Indexes***
		- To support B), each method that creates the updates would need to know how to split the data,
		as there is no generic way to do so. (MOSTLY DONE, UNTESTED [Missing support for index updates.])
		- A), C) and D) are already supported, albeit C) requires server configuration - the "know-all-server"
		is the only one that should be set to replicate the INDEX bucket

	***Queries***
		- Have a generic method that given a key, crdtType and readArgs, splits the index read to multiple servers
		when running B) but sends to single server when running the others. Queries will need to know if the index data
		is split or not (e.g.: top 10 of a single server is not the same as top 10 of each server.) (DONE, UNTESTED.
		Each query knows if the index needs to be split or not, but it's a generic method that does so if it's needed. As for
		merge, a generic method calls the appropriate merge.)
		- There's no intuitive easy way to support D) properly when fetching non-index data. Possible solutions
		include: 1) pretending that there's multiple servers; 2) "if-else" in each query to create the reads
		metadata differently depending if it's single or multi-server; 3) generic method that groups the read
		requests in one. (DONE, UNTESTED. Used a method that given a list of IDs, requests the necessary reads.)
		- A) and C) are already supported.

	NOTE: On C), the global server will have lineitems repeated in different buckets. Is this desirable?
*/

//Note: Variables are shared between tpchClient and each of the client*.go files

type ExecutionTimes struct {
	startTime          int64
	header             int64
	read               int64
	clientTables       int64
	prepareDataProtos  int64
	sendDataProtos     []int64
	prepareIndexProtos []int64
	sendIndexProtos    int64
	totalData          int64
	totalIndex         int64
	queries            []int64
	totalQueries       int64
	finishTime         int64
}

const (
	cpuProfileLoc = "../../profiles/cpu.prof"
	memProfileLoc = "../../profiles/mem.prof"

	//tableFolder = "/Users/a.rijo/Documents/University_6th_year/potionDB docs/2.18.0_rc2/tables/0.01SF/"
	//headerLoc = "/Users/a.rijo/Documents/University_6th_year/potionDB docs/tpc_h/tpch_headers_min.txt"
	//updFolder   = "/Users/a.rijo/Documents/University_6th_year/potionDB docs/2.18.0_rc2/upds/0.01SF/"

	//commonFolder                                  = "/Users/a.rijo/Documents/University_6th_year/potionDB docs/"
	tableFormat, updFormat, header                = "2.18.0_rc2/tables/%sSF/", "2.18.0_rc2/upds/%sSF/", "tpc_h/tpch_headers_min.txt"
	tableExtension, updExtension, deleteExtension = ".tbl", ".tbl.u1", ".1"

	PROMO_PERCENTAGE, IMP_SUPPLY, SUM_SUPPLY, NATION_REVENUE, TOP_SUPPLIERS, LARGE_ORDERS, SEGM_DELAY = "q14pp", "q11iss", "q11sum", "q5nr", "q15ts", "q18lo", "q3sd"

	C_NATIONKEY, L_SUPPKEY, L_ORDERKEY, N_REGIONKEY, O_CUSTKEY, PS_SUPPKEY, S_NATIONKEY, R_REGIONKEY = 3, 2, 0, 2, 1, 1, 3, 0
	PART_BKT, INDEX_BKT                                                                              = 5, 6
	O_ORDERDATE, C_MKTSEGMENT, L_SHIPDATE, L_EXTENDEDPRICE, L_DISCOUNT, O_SHIPPRIOTITY               = 4, 6, 10, 5, 6, 5
)

var (
	//Filled dinamically by prepareConfigs()
	tableFolder, updFolder, commonFolder, headerLoc string
	scaleFactor                                     float64
	isIndexGlobal, isMulti, memDebug, profiling     bool
	maxUpdSize                                      int //Max number of entries for a single upd msg. To avoid sending the whole table in one request...
	INDEX_WITH_FULL_DATA, CRDT_PER_OBJ              bool

	//Constants...
	tableNames = [...]string{"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"}
	//The lineItem table number of entries must be changed manually, as it is not a direct relation with SF.
	//6001215
	//2999668
	//1800093
	//600572
	//60175
	tableEntries = [...]int{150000, 60175, 25, 1500000, 200000, 800000, 5, 10000}
	tableParts   = [...]int{8, 16, 4, 9, 9, 5, 3, 7}
	tableUsesSF  = [...]bool{true, false, false, true, true, true, false, true}
	updsNames    = [...]string{"orders", "lineitem", "delete"}
	//Orders and delete follow SF, except for SF = 0.01. It's easier to just put it manually
	//updEntries = [...]int{10, 37, 10}
	updEntries []int
	//updEntries = [...]int{150, 592, 150}
	//updEntries   = [...]int{1500, 5822, 1500}
	updParts            = [...]int{9, 16}
	updCompleteFilename = [...]string{updFolder + updsNames[0] + updExtension, updFolder + updsNames[1] + updExtension,
		updFolder + updsNames[2] + deleteExtension}

	//Just for debugging
	nProtosSent = 0

	//Table data
	headers    [][]string
	tables     [][][]string
	keys       [][]int
	read       [][]int8 //Positions in tables that actually have values
	procTables *Tables

	//Note: Part isn't partitioned and lineItem uses multiRegionFunc
	//regionFuncs = [...]func([]string) int8{custToRegion, nil, nationToRegion, ordersToRegion,
	//nil, partSuppToRegion, regionToRegion, supplierToRegion}
	//multiRegionFunc = [...]func([]string) []int8{nil, lineitemToRegion, nil, nil, nil, nil, nil, nil}

	regionFuncs     [8]func([]string) int8
	multiRegionFunc [8]func([]string) []int8

	conns   []net.Conn
	buckets []string

	times = ExecutionTimes{
		sendDataProtos:     make([]int64, len(servers)),
		prepareIndexProtos: make([]int64, 6),
		queries:            make([]int64, 6),
	}
)

func loadConfigs() {
	loadConfigsFile()
	prepareConfigs()
}

func loadConfigsFile() {
	configFolder := flag.String("config", "none", "sub-folder in configs folder that contains the configuration files to be used.")
	flag.Parse()
	fmt.Println("Using configFolder:", *configFolder)
	if *configFolder == "none" {
		isMulti, isIndexGlobal, memDebug, profiling, scaleFactor, maxUpdSize = true, true, true, true, 0.1, 2000
		commonFolder = "/Users/a.rijo/Documents/University_6th_year/potionDB docs/"
		MAX_BUFF_PROTOS, QUERY_WAIT, FORCE_PROTO_CLEAN, TEST_ROUTINES, TEST_DURATION = 200, 5000, 10000, 10, 20000
		PRINT_QUERY, QUERY_BENCH = true, false
		INDEX_WITH_FULL_DATA, CRDT_PER_OBJ = true, false
	} else {
		configs := &tools.ConfigLoader{}
		configs.LoadConfigs(*configFolder)
		isMulti, isIndexGlobal = configs.GetBoolConfig("multiServer", true), configs.GetBoolConfig("globalIndex", true)
		memDebug, profiling = configs.GetBoolConfig("memDebug", true), configs.GetBoolConfig("profiling", false)
		scaleFactor, _ = strconv.ParseFloat(configs.GetConfig("scale"), 64)
		maxUpdSize64, _ := strconv.ParseInt(configs.GetOrDefault("updsPerProto", "100"), 10, 64)
		maxUpdSize = int(maxUpdSize64)
		commonFolder = configs.GetConfig("folder")
		servers = strings.Split(configs.GetConfig("servers"), " ")
		MAX_BUFF_PROTOS, QUERY_WAIT, FORCE_PROTO_CLEAN, TEST_ROUTINES, TEST_DURATION = configs.GetIntConfig("maxBuffProtos", 100),
			time.Duration(configs.GetIntConfig("queryWait", 5000)), configs.GetIntConfig("forceMemClean", 10000),
			configs.GetIntConfig("queryClients", 10), int64(configs.GetIntConfig("queryDuration", 20000))
		PRINT_QUERY, QUERY_BENCH = configs.GetBoolConfig("queryPrint", true), configs.GetBoolConfig("queryBench", false)
		INDEX_WITH_FULL_DATA, CRDT_PER_OBJ = configs.GetBoolConfig("indexFullData", true), configs.GetBoolConfig("crdtPerObj", false)
		//Query wait is in nanoseconds!!!
		fmt.Println(isMulti)
		fmt.Println(isIndexGlobal)
		fmt.Println(memDebug)
		fmt.Println(profiling)
		fmt.Println(scaleFactor)
		fmt.Println(maxUpdSize)
		fmt.Println(commonFolder)
		fmt.Println(servers)
		fmt.Println(MAX_BUFF_PROTOS)
		fmt.Println(QUERY_WAIT)
		fmt.Println(FORCE_PROTO_CLEAN)
		fmt.Println(TEST_ROUTINES)
		fmt.Println(TEST_DURATION)
		fmt.Println(PRINT_QUERY)
		fmt.Println(QUERY_BENCH)
		fmt.Println(INDEX_WITH_FULL_DATA)
		fmt.Println(CRDT_PER_OBJ)
	}
}

func prepareConfigs() {
	scaleFactorS := strconv.FormatFloat(scaleFactor, 'f', -1, 64)
	tableFolder, updFolder = commonFolder+fmt.Sprintf(tableFormat, scaleFactorS), commonFolder+fmt.Sprintf(updFormat, scaleFactorS)
	headerLoc = commonFolder + header
	if isMulti {
		//servers = []string{"127.0.0.1:8087", "127.0.0.1:8088", "127.0.0.1:8089", "127.0.0.1:8090", "127.0.0.1:8091"}
		times.sendDataProtos = make([]int64, len(servers))
		if isIndexGlobal {
			buckets = []string{"R1", "R2", "R3", "R4", "R5", "PART", "INDEX"}
		} else {
			buckets = []string{"R1", "R2", "R3", "R4", "R5", "PART", "I1", "I2", "I3", "I4", "I5"}
		}
		//Note: Part isn't partitioned and lineItem uses multiRegionFunc
		regionFuncs = [8]func([]string) int8{custToRegion, nil, nationToRegion, ordersToRegion, nil, partSuppToRegion, regionToRegion, supplierToRegion}
		multiRegionFunc = [8]func([]string) []int8{nil, lineitemToRegion, nil, nil, nil, nil, nil, nil}
		channels.dataChans = []chan QueuedMsg{make(chan QueuedMsg, MAX_BUFF_PROTOS), make(chan QueuedMsg, MAX_BUFF_PROTOS),
			make(chan QueuedMsg, MAX_BUFF_PROTOS), make(chan QueuedMsg, MAX_BUFF_PROTOS), make(chan QueuedMsg, MAX_BUFF_PROTOS)}
	} else {
		//servers = []string{"127.0.0.1:8087"}
		servers = []string{servers[0]}
		buckets = []string{"R", "", "", "", "", "PART", "INDEX"}
		times.sendDataProtos = make([]int64, 1)
		regionFuncs = [8]func([]string) int8{singleRegion, singleRegion, singleRegion, singleRegion, singleRegion, singleRegion, singleRegion, singleRegion}
		channels.dataChans = []chan QueuedMsg{make(chan QueuedMsg, MAX_BUFF_PROTOS)}
	}

	if isIndexGlobal {
		channels.indexChans = []chan QueuedMsg{make(chan QueuedMsg, MAX_BUFF_PROTOS)}
	} else {
		channels.indexChans = []chan QueuedMsg{make(chan QueuedMsg, MAX_BUFF_PROTOS), make(chan QueuedMsg, MAX_BUFF_PROTOS),
			make(chan QueuedMsg, MAX_BUFF_PROTOS), make(chan QueuedMsg, MAX_BUFF_PROTOS), make(chan QueuedMsg, MAX_BUFF_PROTOS)}
	}
	conns = make([]net.Conn, len(servers))

	switch scaleFactor {
	case 0.01:
		tableEntries[LINEITEM] = 60175
		updEntries = []int{10, 37, 10}
	case 0.1:
		tableEntries[LINEITEM] = 600572
		updEntries = []int{150, 592, 150}
	case 0.2:
		tableEntries[LINEITEM] = 1800093
		updEntries = []int{300, 1164, 300} //NOTE: FAKE VALUES!
	case 0.3:
		tableEntries[LINEITEM] = 2999668
		updEntries = []int{450, 1747, 450} //NOTE: FAKE VALUES!
	case 1:
		tableEntries[LINEITEM] = 6001215
		updEntries = []int{1500, 5822, 1500}
	}

}

func StartClient() {
	loadConfigs()
	go debugMemory()
	go func() {
		i := int64(10)
		for {
			time.Sleep(10000 * time.Millisecond)
			file, _ := os.Create(memProfileLoc + strconv.FormatInt(i, 10))
			defer file.Close()
			pprof.WriteHeapProfile(file)
			i += 10
		}
	}()

	startTime := time.Now().UnixNano()
	rand.Seed(startTime)
	times.startTime = startTime
	handleHeaders()

	for i := range channels.dataChans {
		go handleServerComm(i)
	}
	go handleTableProcessing()
	go handlePrepareSend()

	tables = make([][][]string, len(tableNames))
	procTables = &Tables{}
	procTables.InitConstants()
	handleTables()

	//tables = nil
	//debug.FreeOSMemory()
	//prepareIndexesToSend()

	//collectDataStatistics()
	select {}
}

func printExecutionTimes() {
	fmt.Println()
	fmt.Println("*****TEST DURATION*****")
	fmt.Println("Total duration:", (times.finishTime-times.startTime)/1000000, "ms")
	fmt.Println("Header read:", times.header, "ms")
	fmt.Println("Data read:", times.read, "ms")
	fmt.Println("Client tables:", times.clientTables, "ms")
	fmt.Println("Preparation of data protobufs:", times.prepareDataProtos, "ms")
	fmt.Println("Preparation of index protobufs:", times.prepareIndexProtos, "Total:", times.totalIndex, "ms")
	fmt.Println("Sending of data protobufs:", times.sendDataProtos, "Data + Index:", times.totalData, "ms")
	fmt.Println("Sending of index protobufs:", times.sendIndexProtos, "ms")
	fmt.Println("Execution of queries:", times.queries, "Total:", times.totalQueries, "ms")
}

func collectDataStatistics() {
	nationsToRegion := make(map[int8]int8)
	for _, nation := range procTables.Nations {
		nationsToRegion[nation.N_NATIONKEY] = nation.N_REGIONKEY
	}

	custKeyPerNation, orderKeyPerNation, lineItemPerNation := make(map[int8]*int32), make(map[int8]*int32), make(map[int8]*int)
	for nation := range nationsToRegion {
		/*
			custKeyPerNation[nation] = make([]int32, int(float64(tableEntries[CUSTOMER])*scaleFactor*0.1))
			orderKeyPerNation[nation] = make([]int32, int(float64(tableEntries[ORDERS])*scaleFactor*0.1))
			lineItemPerNation[nation] = make([]int32, int(float64(tableEntries[LINEITEM])*0.1))
		*/
		var cust, order int32 = 0, 0
		var line int = 0
		custKeyPerNation[nation], orderKeyPerNation[nation], lineItemPerNation[nation] = &cust, &order, &line
	}

	for _, customer := range procTables.Customers[1:] {
		//custKeyPerNation[customer.C_NATIONKEY] = append(custKeyPerNation[customer.C_NATIONKEY], customer.C_CUSTKEY)
		*custKeyPerNation[customer.C_NATIONKEY]++
	}
	orders, lineItems := 0, 0
	for _, order := range procTables.Orders[1:] {
		nationKey := procTables.Customers[order.O_CUSTKEY].C_NATIONKEY
		//orderKeyPerNation[nationKey] = append(orderKeyPerNation[nationKey], order.O_ORDERKEY)
		*orderKeyPerNation[nationKey]++
		orders++
	}
	/*
		for _, lineItem := range procTables.LineItems {
			if lineItem != nil {
				nationKey := procTables.Customers[procTables.Orders[GetOrderIndex(lineItem.L_ORDERKEY)].O_CUSTKEY].C_NATIONKEY
				//lineItemPerNation[nationKey] = append(lineItemPerNation[nationKey], lineItem.L_ORDERKEY*8+int32(lineItem.L_LINENUMBER))
				*lineItemPerNation[nationKey]++
				lineItems++
			}
		}
		for _, lineItem := range procTables.LineItems {
			if lineItem != nil {
				if lineItem.L_LINENUMBER != 1 {
					fmt.Println(lineItem.L_LINENUMBER)
				}
			}
		}
	*/
	for i, orderItems := range procTables.LineItems[1:] {
		nationKey := procTables.Customers[procTables.Orders[i].O_CUSTKEY].C_NATIONKEY
		*lineItemPerNation[nationKey] += len(orderItems)
		lineItems += len(orderItems)
		//Might be a good idea to also collect statistics on the supplier side...
	}

	custPerRegion, orderPerRegion, linePerRegion := make(map[int8]*int32), make(map[int8]*int32), make(map[int8]*int32)
	for _, region := range procTables.Regions {
		var cust, order, line int32 = 0, 0, 0
		custPerRegion[region.R_REGIONKEY], orderPerRegion[region.R_REGIONKEY], linePerRegion[region.R_REGIONKEY] = &cust, &order, &line
	}
	for nation, region := range nationsToRegion {
		*custPerRegion[region] += *custKeyPerNation[nation]
		*orderPerRegion[region] += *orderKeyPerNation[nation]
		*linePerRegion[region] += int32(*lineItemPerNation[nation])
	}

	fmt.Println("PER REGION STATISTICS")
	for _, region := range procTables.Regions {
		fmt.Printf("[%s] - CUST: %d, ORDER: %d, LINE: %d\n", region.R_NAME, *custPerRegion[region.R_REGIONKEY],
			*orderPerRegion[region.R_REGIONKEY], *linePerRegion[region.R_REGIONKEY])
	}
	fmt.Printf("Number of orders, lineitems: %d, %d", orders, lineItems)
}

func min(first int, second int) int {
	if first < second {
		return first
	}
	return second
}

func ignore(any ...interface{}) {

}

func startProfiling() {
	file, err := os.Create(cpuProfileLoc)
	tools.CheckErr("Failed to create CPU profile file: ", err)
	pprof.StartCPUProfile(file)
	fmt.Println("Started CPU profiling")
	fmt.Println("Started mem profiling")
}

func stopProfiling() {
	sigs := make(chan os.Signal, 10)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		fmt.Println("Saving profiles...")
		pprof.StopCPUProfile()
		file, err := os.Create(memProfileLoc)
		defer file.Close()
		tools.CheckErr("Failed to create Memory profile file: ", err)
		pprof.WriteHeapProfile(file)
		fmt.Println("Profiles saved, closing...")
		os.Exit(0)
	}()
}

func debugMemory() {
	memStats := runtime.MemStats{}
	var maxAlloc uint64 = 0
	//Go routine that pools memStats.Alloc frequently and stores the highest observed value
	go func() {
		for {
			currAlloc := memStats.Alloc
			if currAlloc > maxAlloc {
				maxAlloc = currAlloc
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()

	const MB = 1048576
	count := 0
	for {
		runtime.ReadMemStats(&memStats)
		fmt.Printf("Total mem stolen from OS: %d MB\n", memStats.Sys/MB)
		fmt.Printf("Max alloced: %d MB\n", maxAlloc/MB)
		fmt.Printf("Currently alloced: %d MB\n", memStats.Alloc/MB)
		fmt.Printf("Mem that could be returned to OS: %d MB\n", (memStats.HeapIdle-memStats.HeapReleased)/MB)
		fmt.Printf("Number of objs still malloced: %d\n", memStats.HeapObjects)
		fmt.Printf("Largest heap size: %d MB\n", memStats.HeapSys/MB)
		fmt.Printf("Stack size stolen from OS: %d MB\n", memStats.StackSys/MB)
		fmt.Printf("Stack size in use: %d MB\n", memStats.StackInuse/MB)
		fmt.Printf("Number of goroutines: %d\n", runtime.NumGoroutine())
		fmt.Printf("Number of GC cycles: %d\n", memStats.NumGC)
		fmt.Println()
		count++
		/*
			if count%30 == 0 {
				fmt.Println("Calling GC")
				runtime.GC()
			}
		*/
		time.Sleep(5000 * time.Millisecond)
	}
}

/**********BUCKET CHOOSER HELPERS**********/

//Funcs for deciding bucket for partial replication

func custToRegion(obj []string) int8 {
	nationKey, _ := strconv.ParseInt(obj[C_NATIONKEY], 10, 8)
	return procTables.NationkeyToRegionkey(nationKey)
}

func lineitemToRegion(obj []string) []int8 {
	suppKey, _ := strconv.ParseInt(obj[L_SUPPKEY], 10, 32)
	orderKey, _ := strconv.ParseInt(obj[L_ORDERKEY], 10, 32)
	r1, r2 := procTables.SuppkeyToRegionkey(suppKey), procTables.OrderkeyToRegionkey(int32(orderKey))
	if r1 == r2 {
		return []int8{r1}
	}
	return []int8{r1, r2}
}

func nationToRegion(obj []string) int8 {
	nationKey, _ := strconv.ParseInt(obj[C_NATIONKEY], 10, 8)
	return procTables.NationkeyToRegionkey(nationKey)
}

func ordersToRegion(obj []string) int8 {
	custKey, _ := strconv.ParseInt(obj[O_CUSTKEY], 10, 32)
	return procTables.CustkeyToRegionkey(custKey)
}

func partSuppToRegion(obj []string) int8 {
	suppKey, _ := strconv.ParseInt(obj[PS_SUPPKEY], 10, 32)
	return procTables.SuppkeyToRegionkey(suppKey)
}

func regionToRegion(obj []string) int8 {
	regionKey, _ := strconv.ParseInt(obj[R_REGIONKEY], 10, 8)
	return int8(regionKey)
}

func supplierToRegion(obj []string) int8 {
	nationKey, _ := strconv.ParseInt(obj[S_NATIONKEY], 10, 8)
	return procTables.NationkeyToRegionkey(nationKey)
}

func singleRegion(obj []string) int8 {
	return 0
}

/*
	c: customer, l: lineItem, n: nation, o: orders, p: part, ps: partsupp, r: region, s: supplier
*/
/*
	List of indexes:
	2.4.1: 	sum: l_quantity, l_extendedprice, l_extendedprice * (1 - l_discount), l_extendedprice * (1-l_discount)*(1+l_tax))
			avg: l_quantity, l_extendedprice, l_discount
			All this based on the days (l_shipdate). Possible idea: 1 instance per day and then sum/avg days requested by query?
	2.4.2:	topk: min: ps_supplycost
			TopK should be applied for each pair of (pair.size, pair.type, region)
			Each entry in a TopK is the supplierID and the supplycost.
			Each TopK internally should be ordered from min to max. We can achieve this using negative values.
			We should also keep some extra data with the TopK (supplier, region, etc.)
	2.4.3:	sum: l_extendedprice*(1-l_discount)
			This is for each pair of (l_orderkey, o_orderdate, o_shippriority).
			The query will still need to collect all whose date < o_orderdate and then filter for only those whose
			l_shipdate is > date. Or maybe we can have some map for that...
	2.4.4:	sum/count: "number of orders in which at least one lineitem was received later than the commited date"
			Group this by the pair (o_orderpriority, month) (i.e., group all the days of a month in the same CRDT)
	2.4.5:
			sum: l_extendedprice * (1 - l_discount)
			Group this by the pair (country, year) (i.e., group all the days of a year in the same CRDT).
			Only lineitems for which the costumer and supplier are of the same nation count for this sum.
	2.4.6:	sum: l_extendedprice * l_discount
			Group this by the pair (year, amount, DISCOUNT), where discount would need a precision rate of 0.01.
			This likelly needs to be better thought of.
	2.4.7:	sum: l_extendedprice * (1 - l_discount)
			Group this by the pair (nation1, nation2, year) (i.e., group all the days of a year in the same CRDT).
			This can be between any two nations (but nation1 != nation2), where nation1 is the supplier nation and
			nation2 the customer nation.
			Theorically only years 1995 and 1996 are required.
	2.4.8:	two sums:
			1)	sum: l_extendedprice * (1 - l_discount)
			Group this by (year, product)
			2) sum: l_extendedprice * (1 - l_discount)
			Group this by (nation, year, product). Only consider products that are supplied by that nation
			Note: 1) may be usable for some other query? Check out.
	2.4.9:	sum: l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity
			Group this by (nation, year, product). Only consider products that are supplied by that nation.
			Note that this is the same grouping as 2.4.8's 2), but also considers ps_supplycost * l_quantity.
			IMPORTANT NOTE: in the query, the products listed are ones that contains in their name a certain substring.
			That'll have to be dealt with efficiently. The list of possible substrings is known.
	2.4.10: topk: top 20 of: sum: l_extendedprice * (1 - l_discount)
			Group this by (day, costumer). Need to do the sum for returned products by that costumer that were ordered
			in that day.
			NOTE: Reconsider this grouping, as the query asks for a period of 3 months, but starting on a random day
			between February 1993 and January 1995.
			Likelly needs to be better thought of.
	2.4.11:	sum: ps_supplycost * ps_availqty
			Group by (part, nation). Nation is the supplier nation.
			The query itself only wants the groups for a given nation in which the sum represents >= 0.01%/SF of
			all parts supplied by that nation. But that's defined as a variable...
	2.4.12:	count: number of lineitems for which l_receiptdate > l_commitdate
			Group by priority. If we consider the specifity of the query, two groups are enough
			(URGENT + PRIORITY, OTHERS)
	2.4.13:	count: number of customers with a given number of orders
			Group by PRIORITY. The query itself filters by two comment words picked at random from a limited amount
			of possible values. This MUST be taken in consideration when creating the indexes.
	2.4.14:	sum + count (or mix together if possible): l_extendedprice * (1 - l_discount), when p_type starts with "PROMO"
			Group by month (l_shipdate). Asked date always starts at the 1st day of a given month, between 1993 and 1997.
			The date interval always covers the whole month.
	2.4.15:	topk: sum(l_extendedprice * (1-l_discount))
			Group by month. The sum corresponds to the revenue shipped by a supplier during a given quarter of the year.
			Date can start between first month of 1993 and 10th month of 1997 (first day always)
	2.4.16:	count: number of suppliers (check text below)
			Query: counts suppliers for parts of a given size (receive 8 sizes, 50 different are possible) that aren't of
			a given brand and whose type doesn't start with a given substring (substring selected from the list of strings defined
			for Types in 4.2.2.13). Since the substring is always 2 out of 3 words, it might be feasible to group by word as well.
			Group by SIZE at least.
			This one should be better thought of. Might be wise to also group by brand. Also need to consider the TYPE filter.
			Note: When making the count, ignore all entries that have in the comment "Customer Complaints"
	2.4.17: sum(l_extendedprice) / 7
			Group by (BRAND, CONTAINER). I think I also need to group by part...?
			Filter to only consider orders for which que ordered quantity is less than 20% or the average ordered quantity
			for that item. Likelly need an avg index for that.
	2.4.18: topk with 100 elements: o_totalprice, o_orderdate
			Group by l_quantity. Theorically only need quantities between 312 and 315.
			Stores orders whose quantity is above a given value.
	2.4.19:	sum(l_extendedprice * (1 - l_discount))
			Think quite well on how to do this one. A lot of filters are used.
	2.4.20:	No idea which topk to "directly" answer this query.
			However, sum(l_quantity) grouped by (YEAR, SUPPLIER, PART) would help a lot for sure.
			May have to consider how to handle the COLOR filter, or on a better index.
	2.4.21:	topk(count(number of supplied orders that were delivered (receiptdate) after commitdate only due to this supplier))
			This count only considers multi-supplier orders with current status of F in which only one supplier delivered late.
			Group count by (NATION, SUPPLIER)
			Group topk by NATION.
	2.4.22:	count: (number of customers without orders for 7 years and above average positive account balance)
			sum: (c_acctbal)
			avg: (c_acctbal). Aditional filter: c_acctbal > 0.0
			Considers customers who haven't placed orders for 7 years (i.e., entire DB? Query doesn't filter years) but that
			have greater than average "positive" account balance.
			The three indexes have the same filtering (apart from the aditional one on avg) and grouping rules.
			Group by (substring(1, 2, c_phone))
*/
/*
	Map CRDT:
	Different types being concurrently assigned to the same key - assume this doesn't happen.
	Remove/Update: likelly go with remove-wins, which seems to be the easier.
*/
/*
	To select: 2.4.3 (topk with sum), 2.4.5 (sum, grouped by (country, year)),
	2.4.11 (simple topk of a sum, along with a key (ID)), 2.4.14 (sum + count, no topk, no other data to show),
	2.4.15, 2.4.18 (topk, but with multiple data to show)
	2.4.2 seems to be more complicated than it is worth it.
	Could be interesting if we found a good way to group it: 2.4.6, 2.4.10
*/
