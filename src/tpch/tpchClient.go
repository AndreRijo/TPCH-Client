package tpch

import (
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"potionDB/src/antidote"
	"potionDB/src/proto"
	"potionDB/src/tools"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

//TODO: Support on the benchmark thing coupling multiple queries in one txn? That may lead to higher (or lower) queries/s.
/*
	A possible solution:
	- Prepare index readArgs.
		- If the query requires subsequent gets, return whatever information is necessary for that. Likelly also needs to know how many read replies it's expected
		for each query to have.
	- Send those args as a single transaction (or a static read?). Store the information generated alongside the args. Wait for the reply
	- For each txn reply... (need to consult the stored information to decide that)
		- If there's any subsequent get to be done, do it. Likelly needs to do similar steps to above (might be an issue if there's a 3 level query)
		- Otherwise, just process the result
		- It might be wise to divide this in categories. E.g., Q3, Q5, Q11, Q11A (part 2 of Q11), etc.
		- Likelly do this in a different file.
*/

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
	nLineItemsSent     int
}

//Only one
type DataloadStats struct {
	nDataUpds      int
	nIndexUpds     int
	dataTimeSpent  int64
	indexTimeSpent int64
	nSendersDone   int
	sync.Mutex     //Used for dataTimeSpent, indexTimeSpent and nServersDone, as there's one goroutine per server.
}

//One every x seconds.
type UpdatesStats struct {
	newDataUpds         int
	removeDataUpds      int
	indexUpds           int
	newDataTimeSpent    int64 //This (and the 2 below) are used for update-only client.
	removeDataTimeSpent int64
	indexTimeSpent      int64
}

//One every x seconds.
type QueryStats struct {
	nQueries  int
	nReads    int
	timeSpent int64
	latency   int64 //Used when LATENCY_MODE is PER_BATCH
	nTxns     int   //Note: only mix clients use nTxns currently
}

const (
	cpuProfileLoc = "../../profiles/cpu.prof"
	memProfileLoc = "../../profiles/mem.prof"

	tableFolder = "/Users/joao/Documents/tese/codeAndDataStuff/tables/0.01SF/"
	headerLoc   = "/Users/joao/Documents/tese/codeAndDataStuff/headers/tpch_headers_min.txt"
	updFolder   = "" // TODOX

	//commonFolder                                  = "/Users/a.rijo/Documents/University_6th_year/potionDB docs/"
	tableFormat, updFormat, header                = "2.18.0_rc2/tables/%sSF/", "2.18.0_rc2/upds/%sSF/", "tpc_h/tpch_headers_min.txt"
	tableExtension, updExtension, deleteExtension = ".tbl", ".tbl.u", "."

	PROMO_PERCENTAGE, IMP_SUPPLY, SUM_SUPPLY, NATION_REVENUE, TOP_SUPPLIERS, LARGE_ORDERS, SEGM_DELAY = "q14pp", "q11iss", "q11sum", "q5nr", "q15ts", "q18lo", "q3sd"

	C_NATIONKEY, L_SUPPKEY, L_ORDERKEY, N_REGIONKEY, O_CUSTKEY, PS_SUPPKEY, S_NATIONKEY, R_REGIONKEY = 3, 2, 0, 2, 1, 1, 3, 0
	PART_BKT, INDEX_BKT                                                                              = 5, 6
	O_ORDERDATE, C_MKTSEGMENT, L_SHIPDATE, L_EXTENDEDPRICE, L_DISCOUNT, O_SHIPPRIOTITY, O_ORDERKEY   = 4, 6, 10, 5, 6, 5, 0

	AVG_OP, AVG_BATCH, PER_BATCH = 0, 1, 2 //LATENCY_MODE. Note that only the latter supports recording latencies for queries separated from update latencies.
)

var (
	//Filled dinamically by prepareConfigs()
	commonFolder string
	//tableFolder, updFolder, commonFolder, headerLoc string
	scaleFactor float64
	//TODO: Finish useTopSum.
	isIndexGlobal, isMulti, memDebug, profiling, splitIndexLoad, useTopKAll, useTopSum bool
	maxUpdSize                                                                         int //Max number of entries for a single upd msg. To avoid sending the whole table in one request...
	INDEX_WITH_FULL_DATA, CRDT_PER_OBJ, SINGLE_INDEX_SERVER                            bool
	DOES_DATA_LOAD, DOES_QUERIES, DOES_UPDATES, CRDT_BENCH                             bool
	withUpdates                                                                        bool //Also loads the necessary update data in order to still be able to do the queries with updates.
	updCompleteFilename                                                                [3]string
	statisticsInterval                                                                 time.Duration //Milliseconds. A negative number means that no statistics need to be collected.
	statsSaveLocation                                                                  string
	LATENCY_MODE                                                                       int //OP_AVG, BATCH_AVG, PER_BATCH

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

	regionFuncs         [8]func([]string) int8
	multiRegionFunc     [8]func([]string) []int8
	specialLineItemFunc func(order []string, item []string) []int8 //Uses customer key instead of order key

	conns   []net.Conn
	buckets []string

	times = ExecutionTimes{
		sendDataProtos:     make([]int64, len(servers)),
		prepareIndexProtos: make([]int64, 6),
		queries:            make([]int64, 6),
	}

	dataloadStats                                                                                                    = DataloadStats{}
	configFolder, indexGlobalString, testRoutinesString, statsSaveLocationString, singleIndexString, updRateString   *string
	idString, updateIndexString, splitUpdatesString, splitUpdatesNoWaitString, updateBaseString, notifyAddressString *string
	nReadsTxn, updateSpecificIndex, batchModeS, latencyModeS, useTopSumString, serversString                         *string
	//Bench
	nKeysString, keysTypeString, addRateString, partReadRateString, queryRatesString, opsPerTxnString, nTxnsBeforeWaitString, nElemsString,
	maxIDString, maxScoreString, topNString, topAboveString, rndDataSizeString, minChangeString, maxChangeString, maxSumString, maxNAddsString *string

	id     string     //id for statistics files
	idLock sync.Mutex //Statistics files may be written concurrently, thus this lock protects the ID from being changed concurrently.
)

func LoadConfigs() {
	configs := loadFlags()
	loadConfigsFile(configs)
	prepareConfigs()
}

func loadFlags() (configs *tools.ConfigLoader) {
	reset := flag.String("reset", "none", "set this flag to true for resetting the server status. The program will exit afterwards.")
	configFolder = flag.String("config", "none", "sub-folder in configs folder that contains the configuration files to be used.")
	indexGlobalString = flag.String("global_index", "none", "if indexes are global (i.e., data from all servers) or local (only data in the server)")
	testRoutinesString = flag.String("query_clients", "none", "number of query client processes (ignored if this isn't a query client)")
	statsSaveLocationString = flag.String("test_name", "none", "name of the test/path to write statistics to.")
	singleIndexString = flag.String("single_index_server", "none", "if only the first server has the index, or all have the index. Ignored if global_index is false.")
	updRateString = flag.String("upd_rate", "none", "rate of updates for mix clients. Ignored if this is a query only client or update only client")
	idString = flag.String("id", "none", "id to use for statistics file. Should be set when multiple client instances are running on the same disk.")
	updateIndexString = flag.String("updateIndex", "none", "if indexes should be updated or not. Dataload client ignores this.")
	updateBaseString = flag.String("updateBase", "none", "if base data should be updated or not. Dataload client ignores this.")
	splitUpdatesString = flag.String("split_updates", "none", "if each update should be sent in its own transaction or not. Only mix clients support this.")
	splitUpdatesNoWaitString = flag.String("split_updates_no_wait", "none", "if true, all updates related to an order are sent before waiting for a reply.")
	notifyAddressString = flag.String("notify_address", "none", "ip:port to notify when test is complete. 'none' (or don't provide this flag) if notification isn't desired.")
	nReadsTxn = flag.String("n_reads_txn", "none", "number of reads (not queries) per transaction. Works for both mix and query clients.")
	updateSpecificIndex = flag.String("update_specific_index", "none", "if only the indexes correspondent to the queries in the config file should be updated.")
	batchModeS = flag.String("batch_mode", "none", "how queries are grouped in a transaction - CYCLE, SINGLE.")
	latencyModeS = flag.String("latency_mode", "none", "how is latency measured - AVG_OP, AVG_BATCH, PER_BATCH.")
	useTopSumString = flag.String("use_top_sum", "none", "for supported queries (Q15), if true TopSum will be used instead of TopK.")
	serversString = flag.String("servers", "none", "list of servers to connect to.")
	//fmt.Println("On flag: ", *serversString)

	registerBenchFlags()

	flag.Parse()
	fmt.Println("On flag after parse: ", *serversString)
	fmt.Println("ConfigFolder after parse:", *configFolder)

	configs = &tools.ConfigLoader{}
	if isFlagValid(configFolder) {
		fmt.Println("Loading valid configs")
		configs.LoadConfigs(*configFolder)
	} else {
		fmt.Println("Loading empty configs")
		configs.InitEmptyConfig()
	}
	if isFlagValid(reset) {
		resetServers(configs)
		os.Exit(0)
	}

	if isFlagValid(indexGlobalString) {
		configs.ReplaceConfig("globalIndex", *indexGlobalString)
	}
	if isFlagValid(testRoutinesString) {
		configs.ReplaceConfig("queryClients", *testRoutinesString)
	}
	if isFlagValid(statsSaveLocationString) {
		configs.ReplaceConfig("statsLocation", *statsSaveLocationString)
	}
	if isFlagValid(singleIndexString) {
		configs.ReplaceConfig("singleIndexServer", *singleIndexString)
	}
	if isFlagValid(updRateString) {
		configs.ReplaceConfig("updRate", *updRateString)
	}
	if isFlagValid(idString) {
		configs.ReplaceConfig("id", *idString)
	}
	if isFlagValid(updateIndexString) {
		configs.ReplaceConfig("updateIndex", *updateIndexString)
	}
	if isFlagValid(updateBaseString) {
		configs.ReplaceConfig("updateBase", *updateIndexString)
	}
	if isFlagValid(splitUpdatesString) {
		configs.ReplaceConfig("splitUpdates", *splitUpdatesString)
	}
	if isFlagValid(splitUpdatesNoWaitString) {
		configs.ReplaceConfig("splitUpdatesNoWait", *splitUpdatesNoWaitString)
	}
	if isFlagValid(notifyAddressString) {
		configs.ReplaceConfig("notifyAddress", *notifyAddressString)
	}
	if isFlagValid(nReadsTxn) {
		configs.ReplaceConfig("nReadsTxn", *nReadsTxn)
	}
	if isFlagValid(updateSpecificIndex) {
		configs.ReplaceConfig("updateSpecificIndex", *updateSpecificIndex)
	}
	if isFlagValid(batchModeS) {
		configs.ReplaceConfig("batchMode", *batchModeS)
	}
	if isFlagValid(latencyModeS) {
		configs.ReplaceConfig("latencyMode", *latencyModeS)
	}
	if isFlagValid(useTopSumString) {
		configs.ReplaceConfig("useTopSum", *useTopSumString)
	}
	if isFlagValid(serversString) {
		fmt.Println("ServersString is valid")
		processServersCommandLine(configs)
		fmt.Println("On configs: ", configs.GetConfig("servers"))
	} else {
		fmt.Println("ServersString is not valid as its value is:", *serversString)
	}
	loadBenchFlags(configs)
	return
}

func printFlags(f *flag.Flag) {
	fmt.Println(*f)
}

func loadConfigsFile(configs *tools.ConfigLoader) {

	fmt.Println("Using configFolder:", *configFolder)
	//These configs may have been defined via flags
	isIndexGlobal = configs.GetBoolConfig("globalIndex", true)
	TEST_ROUTINES = configs.GetIntConfig("queryClients", 10)
	statsSaveLocation = configs.GetOrDefault("statsLocation", "unknownStats/")
	SINGLE_INDEX_SERVER = configs.GetBoolConfig("singleIndexServer", false)
	UPD_RATE = configs.GetFloatConfig("updRate", 0.1)
	id = configs.GetOrDefault("id", "0")

	//All remaining are non-flag configs
	if *configFolder == "none" {
		fmt.Println("Non-defined configFolder, using defaults")
		isMulti, splitIndexLoad, memDebug, profiling, scaleFactor, maxUpdSize, useTopKAll, useTopSum = true, true, false, false, 0.1, 2000, false, false
		commonFolder = "/Users/joao/Documents/tese/codeAndDataStuff/"
		MAX_BUFF_PROTOS, QUERY_WAIT, FORCE_PROTO_CLEAN, TEST_DURATION = 200, 5000, 10000, 20000

		PRINT_QUERY, QUERY_BENCH, CRDT_BENCH = true, false, false
		INDEX_WITH_FULL_DATA, CRDT_PER_OBJ, DOES_DATA_LOAD, DOES_QUERIES, DOES_UPDATES = true, false, true, true, false
		withUpdates, N_UPDATE_FILES, START_UPD_FILE, FINISH_UPD_FILE = false, 1000, 1, 1000
		//queryFuncs = []func(QueryClient) int{sendQ3, sendQ5, sendQ11, sendQ14, sendQ15, sendQ18}
		statisticsInterval = -1
		MAX_LINEITEM_GOROUTINES, UPDATES_GOROUTINES, READS_PER_TXN = 16, 16, 1
		UPDATE_INDEX, UPDATE_SPECIFIC_INDEX_ONLY, BATCH_MODE = true, false, CYCLE
	} else {
		fmt.Println("Defined config folder.")
		isMulti, splitIndexLoad, useTopKAll = configs.GetBoolConfig("multiServer", true),
			configs.GetBoolConfig("splitIndexLoad", true), configs.GetBoolConfig("useTopKAll", false)
		useTopSum = configs.GetBoolConfig("useTopSum", false) //Ignored in this version
		memDebug, profiling = configs.GetBoolConfig("memDebug", true), configs.GetBoolConfig("profiling", false)
		scaleFactor, _ = strconv.ParseFloat(configs.GetConfig("scale"), 64)
		maxUpdSize64, _ := strconv.ParseInt(configs.GetOrDefault("updsPerProto", "100"), 10, 64)
		maxUpdSize = int(maxUpdSize64)
		commonFolder = configs.GetConfig("folder")
		servers = strings.Split(configs.GetConfig("servers"), " ")
		MAX_BUFF_PROTOS, QUERY_WAIT, FORCE_PROTO_CLEAN, TEST_DURATION = configs.GetIntConfig("maxBuffProtos", 100),
			time.Duration(configs.GetIntConfig("queryWait", 5000)), configs.GetIntConfig("forceMemClean", 10000),
			int64(configs.GetIntConfig("queryDuration", 20000))
		PRINT_QUERY, QUERY_BENCH, CRDT_BENCH = configs.GetBoolConfig("queryPrint", true), configs.GetBoolConfig("queryBench", false), configs.GetBoolConfig("crdtBench", false)
		INDEX_WITH_FULL_DATA, CRDT_PER_OBJ, DOES_DATA_LOAD, DOES_QUERIES, DOES_UPDATES = configs.GetBoolConfig("indexFullData", true), configs.GetBoolConfig("crdtPerObj", false),
			configs.GetBoolConfig("doDataLoad", true), configs.GetBoolConfig("doQueries", true), configs.GetBoolConfig("doUpdates", false)
		withUpdates, N_UPDATE_FILES = configs.GetBoolConfig("withUpdates", false), configs.GetIntConfig("nUpdateFiles", 1000)
		statisticsInterval = time.Duration(configs.GetIntConfig("statisticsInterval", 5000))
		MAX_LINEITEM_GOROUTINES, UPDATES_GOROUTINES = configs.GetIntConfig("maxLineitemGoroutines", 16), configs.GetIntConfig("updGoroutines", 16)
		START_UPD_FILE, FINISH_UPD_FILE = configs.GetIntConfig("startUpdFile", 1), configs.GetIntConfig("finishUpdFile", 1000)
		UPDATE_INDEX, SPLIT_UPDATES, SPLIT_UPDATES_NO_WAIT, UPDATE_BASE_DATA, NOTIFY_ADDRESS = configs.GetBoolConfig("updateIndex", true), configs.GetBoolConfig("splitUpdates", false),
			configs.GetBoolConfig("splitUpdatesNoWait", true), configs.GetBoolConfig("updateBase", true), configs.GetOrDefault("notifyAddress", "")
		READS_PER_TXN, UPDATE_SPECIFIC_INDEX_ONLY = configs.GetIntConfig("nReadsTxn", 1), configs.GetBoolConfig("updateSpecificIndex", false)
		BATCH_MODE, LATENCY_MODE = batchModeStringToInt(configs.GetOrDefault("batchMode", "CYCLE")), latencyModeStringToInt(configs.GetOrDefault("latencyMode", "AVG_OP"))

		//queryNumbers := strings.Split(configs.GetOrDefault("queries", "3, 5, 11, 14, 15, 18"), " ")
		//setQueryList(queryNumbers)

		if useTopSum {
			fmt.Println("[TPCH_CLIENT]Using top-sum!")
		}
		if CRDT_BENCH {
			loadBenchConfigs(configs)
		}
		fmt.Println("Servers loaded from configs: ", servers)
	}
}

func prepareConfigs() {
	fmt.Println("Servers before prepare configs:", servers)
	//scaleFactorS := strconv.FormatFloat(scaleFactor, 'f', -1, 64)
	//tableFolder, updFolder = commonFolder+fmt.Sprintf(tableFormat, scaleFactorS), commonFolder+fmt.Sprintf(updFormat, scaleFactorS)
	updCompleteFilename = [3]string{updFolder + updsNames[0] + updExtension, updFolder + updsNames[1] + updExtension,
		updFolder + updsNames[2] + deleteExtension}
	//headerLoc = commonFolder + header
	if isMulti {
		//servers = []string{"127.0.0.1:8087", "127.0.0.1:8088", "127.0.0.1:8089", "127.0.0.1:8090", "127.0.0.1:8091"}
		times.sendDataProtos = make([]int64, len(servers))
		if !isIndexGlobal {
			buckets = []string{"R1", "R2", "R3", "R4", "R5", "PART"}
		} else if !splitIndexLoad {
			buckets = []string{"R1", "R2", "R3", "R4", "R5", "PART"}
		} else {
			buckets = []string{"R1", "R2", "R3", "R4", "R5", "PART"}
		}
		//Note: Part isn't partitioned and lineItem uses multiRegionFunc
		regionFuncs = [8]func([]string) int8{custToRegion, nil, nationToRegion, ordersToRegion, nil, partSuppToRegion, regionToRegion, supplierToRegion}
		multiRegionFunc = [8]func([]string) []int8{nil, lineitemToRegion, nil, nil, nil, nil, nil, nil}
		specialLineItemFunc = specialLineitemToRegion
		channels.dataChans = []chan QueuedMsg{make(chan QueuedMsg, MAX_BUFF_PROTOS), make(chan QueuedMsg, MAX_BUFF_PROTOS),
			make(chan QueuedMsg, MAX_BUFF_PROTOS), make(chan QueuedMsg, MAX_BUFF_PROTOS), make(chan QueuedMsg, MAX_BUFF_PROTOS)}
		channels.updateChans = []chan QueuedMsgWithStat{make(chan QueuedMsgWithStat, MAX_BUFF_PROTOS), make(chan QueuedMsgWithStat, MAX_BUFF_PROTOS),
			make(chan QueuedMsgWithStat, MAX_BUFF_PROTOS), make(chan QueuedMsgWithStat, MAX_BUFF_PROTOS), make(chan QueuedMsgWithStat, MAX_BUFF_PROTOS)}
	} else {
		//servers = []string{"127.0.0.1:8087"}
		servers = []string{servers[0]}
		buckets = []string{"R", "", "", "", "", "PART", "INDEX"}
		times.sendDataProtos = make([]int64, 1)
		regionFuncs = [8]func([]string) int8{singleRegion, singleRegion, singleRegion, singleRegion, singleRegion, singleRegion, singleRegion, singleRegion}
		specialLineItemFunc = singleLineitemToRegion
		channels.dataChans = []chan QueuedMsg{make(chan QueuedMsg, MAX_BUFF_PROTOS)}
		channels.updateChans = []chan QueuedMsgWithStat{make(chan QueuedMsgWithStat, MAX_BUFF_PROTOS)}
	}

	if !isIndexGlobal || (splitIndexLoad && !SINGLE_INDEX_SERVER) {
		channels.indexChans = []chan QueuedMsg{make(chan QueuedMsg, MAX_BUFF_PROTOS), make(chan QueuedMsg, MAX_BUFF_PROTOS),
			make(chan QueuedMsg, MAX_BUFF_PROTOS), make(chan QueuedMsg, MAX_BUFF_PROTOS), make(chan QueuedMsg, MAX_BUFF_PROTOS)}
	} else {
		channels.indexChans = []chan QueuedMsg{make(chan QueuedMsg, MAX_BUFF_PROTOS)}
	}
	conns = make([]net.Conn, len(servers))

	//Note: All lineitems apart from 0.1SF need to be updated
	switch scaleFactor {
	case 0.01:
		tableEntries[LINEITEM] = 60175
		//updEntries = []int{10, 37, 10}
		updEntries = []int{15, 41, 16}
	case 0.1:
		tableEntries[LINEITEM] = 600572
		//updEntries = []int{150, 592, 150}
		//updEntries = []int{150, 601, 150}
		updEntries = []int{151, 601, 150}
	case 0.2:
		tableEntries[LINEITEM] = 1800093
		updEntries = []int{300, 1164, 300} //NOTE: FAKE VALUES!
	case 0.3:
		tableEntries[LINEITEM] = 2999668
		updEntries = []int{450, 1747, 450} //NOTE: FAKE VALUES!
	case 1:
		tableEntries[LINEITEM] = 6001215
		//updEntries = []int{1500, 5822, 1500}
		updEntries = []int{1500, 6001, 1500}
	}

	fmt.Println("Servers after prepareConfigs:", servers)
}

func StartClient() {
	LoadConfigs()
	if CRDT_BENCH {
		startCRDTBench()
	}
	if memDebug {
		go debugMemory()
	}
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

	go handleTableProcessing() // inicializa a var procTables com o que está em tables
	if DOES_DATA_LOAD {
		//Start tcp connection to each server for data loading. Query clients create and manage their own connections.
		//Update clients start their connections after all data is loaded.
		go connectToServers() // envia mensagens do dataLoad para as réplicas
	}
	if DOES_DATA_LOAD {
		//Prepare to send initial data protos
		go handlePrepareSend() // crIa os pedidos que são enviados para o connectToServers()
	}

	tables = make([][][]string, len(tableNames))
	procTables = &Tables{}
	procTables.InitConstants()
	handleTables() // Lê tabelas e envia mensagem para o handleTableProcessing() à medida que as tabelas estao lidas

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

func specialLineitemToRegion(order []string, item []string) []int8 {
	suppKey, _ := strconv.ParseInt(item[L_SUPPKEY], 10, 32)
	custKey, _ := strconv.ParseInt(order[O_CUSTKEY], 10, 32)
	r1, r2 := procTables.SuppkeyToRegionkey(suppKey), procTables.CustkeyToRegionkey(custKey)
	if r1 == r2 {
		return []int8{r1}
	}
	return []int8{r1, r2}
}

func singleLineitemToRegion(order []string, item []string) []int8 {
	return []int8{0}
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
func setQueryList(queryStrings []string) {
	queryFuncs, getReadsFuncs = make([]func(QueryClient) int, len(queryStrings)),
		make([]func(QueryClient, []antidote.ReadObjectParams, []antidote.ReadObjectParams, []int, int) int, len(queryStrings))
	processReadReplies = make([]func(QueryClient, []*proto.ApbReadObjectResp, []int, int) int, len(queryStrings))
	indexesToUpd = make([]int, len(queryStrings))
	i := 0
	for _, queryN := range queryStrings {
		switch queryN {
		case "3":
			queryFuncs[i], getReadsFuncs[i], processReadReplies[i], indexesToUpd[i] = sendQ3, getQ3Reads, processQ3Reply, 3
		case "5":
			queryFuncs[i], getReadsFuncs[i], processReadReplies[i], indexesToUpd[i] = sendQ5, getQ5Reads, processQ5Reply, 5
		case "11":
			//Q11 doens't have updates
			queryFuncs[i], getReadsFuncs[i], processReadReplies[i] = sendQ11, getQ11Reads, processQ11Reply
		case "14":
			queryFuncs[i], getReadsFuncs[i], processReadReplies[i], indexesToUpd[i] = sendQ14, getQ14Reads, processQ14Reply, 14
		case "15":
			queryFuncs[i], getReadsFuncs[i], processReadReplies[i], indexesToUpd[i] = sendQ15, getQ15Reads, processQ15Reply, 15
		case "18":
			queryFuncs[i], getReadsFuncs[i], processReadReplies[i], indexesToUpd[i] = sendQ18, getQ18Reads, processQ18Reply, 18
		}
		i++
	}
	sort.Ints(indexesToUpd)
	return
}
*/

func latencyModeStringToInt(typeS string) int {
	switch strings.ToUpper(typeS) {
	case "AVG_OP":
		return AVG_OP
	case "AVG_BATCH":
		return AVG_BATCH
	case "PER_BATCH":
		return PER_BATCH
	default:
		fmt.Println("[ERROR]Unknown batch mode type. Exitting")
		os.Exit(0)
	}
	return AVG_OP
}

func resetServers(configs *tools.ConfigLoader) {
	start := time.Now().UnixNano()
	servers = strings.Split(configs.GetConfig("servers"), " ")
	conns := make([]net.Conn, len(servers))
	for i, server := range servers {
		conn, err := net.Dial("tcp", server)
		tools.CheckErr("Network connection establishment err", err)
		conns[i] = conn
	}
	for i, conn := range conns {
		fmt.Printf("Requested server %s to reset.\n", servers[i])
		antidote.SendProto(antidote.ResetServer, &proto.ApbResetServer{}, conn)
	}
	for i, conn := range conns {
		antidote.ReceiveProto(conn)
		fmt.Printf("Server %s has finished resetting itself.\n", servers[i])
	}
	end := time.Now().UnixNano()
	fmt.Println("Time taken for the reset process: ", (end-start)/1000000, "ms")
}

//Bench
func registerBenchFlags() {
	nKeysString = flag.String("b_nkeys", "none", "number of keys for bench clients.")
	keysTypeString = flag.String("b_keys_type", "none", "type of key splitting for bench clients.")
	addRateString = flag.String("b_add_rate", "none", "add rate for bench clients.")
	partReadRateString = flag.String("b_part_read_rate", "none", "partial read rate for bench clients.")
	queryRatesString = flag.String("b_query_rates", "none", "query distribution for bench clients. Use form e.g. [0,0.5,1]")
	opsPerTxnString = flag.String("b_ops_per_txn", "none", "number of operations to do in a single transaction.")
	nTxnsBeforeWaitString = flag.String("b_txns_before_wait", "none", "number of txns to be sent before waiting for replies")
	nElemsString = flag.String("b_nelems", "none", "number of elements for bench clients' set")
	maxScoreString = flag.String("b_max_score", "none", "max score value for bench clients' topk")
	maxIDString = flag.String("b_max_id", "none", "max id value for bench clients' topk")
	topAboveString = flag.String("b_top_above", "none", "max value for bench clients' topGetAbove (topk)")
	topNString = flag.String("b_top_n", "none", "max value for bench clients' topGetN (topk)")
	rndDataSizeString = flag.String("b_rnd_size", "none", "size for bench clients' topk extra data")
	minChangeString = flag.String("b_min_change", "none", "min value for bench clients' increments (counter)")
	maxChangeString = flag.String("b_max_change", "none", "max value for bench clients' increments (counter)")
	maxSumString = flag.String("b_max_sum", "none", "max sum value for bench clients' average")
	maxNAddsString = flag.String("b_max_nAdds", "none", "max nAdds value for bench clients' average")
}

func loadBenchFlags(configs *tools.ConfigLoader) {
	if isFlagValid(nKeysString) {
		configs.ReplaceConfig("keys", *nKeysString)
	}
	if isFlagValid(keysTypeString) {
		configs.ReplaceConfig("keyType", *keysTypeString)
	}
	if isFlagValid(addRateString) {
		configs.ReplaceConfig("addRate", *addRateString)
	}
	if isFlagValid(partReadRateString) {
		configs.ReplaceConfig("partReadRate", *partReadRateString)
	}
	if isFlagValid(queryRatesString) {
		configs.ReplaceConfig("queryRates", *queryRatesString)
	}
	if isFlagValid(opsPerTxnString) {
		configs.ReplaceConfig("opsPerTxn", *opsPerTxnString)
	}
	if isFlagValid(nTxnsBeforeWaitString) {
		configs.ReplaceConfig("nTxnsBeforeWait", *nTxnsBeforeWaitString)
	}
	if isFlagValid(nElemsString) {
		configs.ReplaceConfig("nSetElems", *nElemsString)
	}
	if isFlagValid(maxScoreString) {
		configs.ReplaceConfig("maxScore", *maxScoreString)
	}
	if isFlagValid(maxIDString) {
		configs.ReplaceConfig("maxID", *maxIDString)
	}
	if isFlagValid(topAboveString) {
		configs.ReplaceConfig("topAbove", *topAboveString)
	}
	if isFlagValid(topNString) {
		configs.ReplaceConfig("topN", *topNString)
	}
	if isFlagValid(rndDataSizeString) {
		configs.ReplaceConfig("randomDataSize", *rndDataSizeString)
	}
	if isFlagValid(minChangeString) {
		configs.ReplaceConfig("minChange", *minChangeString)
	}
	if isFlagValid(maxChangeString) {
		configs.ReplaceConfig("maxChange", *maxChangeString)
	}
	if isFlagValid(maxSumString) {
		configs.ReplaceConfig("maxSum", *maxSumString)
	}
	if isFlagValid(maxNAddsString) {
		configs.ReplaceConfig("maxNAdds", *maxNAddsString)
	}
}

func isFlagValid(value *string) bool {
	return *value != "none" && *value != "" && *value != " "
}

func processServersCommandLine(configs *tools.ConfigLoader) {
	srv := *serversString
	if srv[0] == '[' {
		srv = strings.Replace(srv[1:len(srv)-1], ",", " ", -1)
	}
	configs.ReplaceConfig("servers", srv)
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
