package main

import (
	"antidote"
	"crdt"
	"fmt"
	rand "math/rand"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"
	"tools"
	"tpchTools"

	"github.com/golang/protobuf/proto"
)

//TODO: There's a bug with potionDB when applying static transactions. Eventually, one transaction doesn't complete and thus the system freezes.
//This bug is even easier to notice with high update length (e.g: with 10k, it usually happens at the 2-4th txn)
//This happens even with only partition. It is related to the logger-replicator communication - the logger blocks while processing the replicator request

//Clause 5.3.7.7: one pair of refresh functions per query stream.
//I.e., if there's only 1 query client, then each refresh function is only executed once.
//Scheduling of the refresh functions is left to the tester.
//Theorically, according to 5.3.7.10, the update data could be stored in the DB before the benchmark begins
//TODO: Actually build a "table" in the client which allows the client to quickly access each field with the key and access the correct type
//Potencially I could still use slices for efficiency and use constants to index, but it would be advisable to still have the concrete types
//Maybe I could use an array of structs (each struct matching one type of table) or the struct itself be an array

//TODO: Possibly support TopK returning results sorted?

type QueuedMsg struct {
	proto.Message
	code byte
}

const (
	tableFolder = "/Users/a.rijo/Documents/University_6th_year/potionDB docs/2.18.0_rc2/tables/0.1SF/"
	headerLoc   = "/Users/a.rijo/Documents/University_6th_year/potionDB docs/tpc_h/tpch_headers.txt"
	updFolder   = "/Users/a.rijo/Documents/University_6th_year/potionDB docs/2.18.0_rc2/upds/0.1SF/"

	scaleFactor                                   = 0.1
	tableExtension, updExtension, deleteExtension = ".tbl", ".tbl.u1", ".1"
	maxUpdSize                                    = 2000 //Max number of entries for a single upd msg. To avoid sending the whole table in one request...

	//TODO: Think about buckets
	bucket          = "bkt"
	headerOrders    = 3
	headerLineItems = 1

	//Keys for indexes
	PROMO_PERCENTAGE = "q14pp"
	IMP_SUPPLY       = "q11iss"
	SUM_SUPPLY       = "q11sum"
	NATION_REVENUE   = "q5nr"
	TOP_SUPPLIERS    = "q15ts"
	LARGE_ORDERS     = "q18lo"
	SEGM_DELAY       = "q3sd"

	QUEUE_COMPLETE = byte(255)
)

var (
	//Constants...
	tableNames = [...]string{"customer", "lineItem", "nation", "orders", "part", "partsupp", "region", "supplier"}
	//The lineItem table number of entries must be changed manually, as it is not a direct relation with SF.
	//6001215
	//600572
	//60175
	tableEntries = [...]int{150000, 600572, 25, 1500000, 200000, 800000, 5, 10000}
	tableParts   = [...]int{8, 16, 4, 9, 9, 5, 3, 7}
	tableUsesSF  = [...]bool{true, false, false, true, true, true, false, true}
	updsNames    = [...]string{"orders", "lineitem", "delete"}
	//Orders and delete follow SF, except for SF = 0.01. It's easier to just put it manually
	updEntries = [...]int{10, 37, 10}
	//updEntries   = [...]int{150, 592, 150}
	//updEntries   = [...]int{1500, 5822, 1500}
	updParts            = [...]int{9, 16}
	updCompleteFilename = [...]string{updFolder + updsNames[0] + updExtension, updFolder + updsNames[1] + updExtension,
		updFolder + updsNames[2] + deleteExtension}

	servers = []string{"127.0.0.1:8087"}

	//Just for debugging
	nProtosSent = 0

	//Table data
	headers    [][]string
	tables     [][][]string
	keys       [][]int
	procTables *Tables

	//Queues to send data
	dataQueue  = make(chan QueuedMsg, 100)
	indexQueue = make(chan QueuedMsg, 100)
	//queriesQueue  = make(chan QueuedMsg, 100)
	updsQueue = make(chan QueuedMsg, 100)
)

//TODO: Start sending tables while still reading them
func main() {
	rand.Seed(time.Now().UnixNano())
	go handleServerComm()
	fmt.Println("Reading headers")
	headers, keys = tpchTools.ReadHeaders(headerLoc, len(tableNames))
	tables = readTables()
	prepareTablesToSend()
	procTables = CreateClientTables(tables)
	prepareIndexesToSend()
	//TODO: Queries and updates
	select {}

}

func handleServerComm() {
	conn, err := net.Dial("tcp", servers[0])
	tools.CheckErr("Network connection establishment err", err)

	fmt.Println("Starting to send data protos...")
	complete := false
	for !complete {
		msg := <-dataQueue
		if msg.code == QUEUE_COMPLETE {
			complete = true
		} else {
			antidote.SendProto(msg.code, msg.Message, conn)
			antidote.ReceiveProto(conn)
		}
	}
	fmt.Println("All data protos have been sent.")

	fmt.Println("Starting to send index protos...")
	//Start a txn for the indexes
	startTxn := antidote.CreateStartTransaction(nil)
	antidote.SendProto(antidote.StartTrans, startTxn, conn)
	_, txnReplyProto, _ := antidote.ReceiveProto(conn)
	txnId := txnReplyProto.(*antidote.ApbStartTransactionResp).GetTransactionDescriptor()

	complete = false
	for !complete {
		msg := <-indexQueue
		if msg.code == QUEUE_COMPLETE {
			complete = true
		} else {
			msg.Message.(*antidote.ApbUpdateObjects).TransactionDescriptor = txnId
			antidote.SendProto(msg.code, msg.Message, conn)
			antidote.ReceiveProto(conn)
		}
	}

	//Commit for indexes
	commitTxn := antidote.CreateCommitTransaction(txnId)
	antidote.SendProto(antidote.CommitTrans, commitTxn, conn)
	antidote.ReceiveProto(conn)
	fmt.Println("All index protos have been sent.")

	sendQueries(conn)
}

func readTables() (tables [][][]string) {
	//tableName -> [entry] -> [fields of each entry]
	tables = make([][][]string, len(tableNames), len(tableNames))
	for i := 0; i < len(tableNames); i++ {
		fmt.Println("Reading", tableNames[i])
		nEntries := tableEntries[i]
		if tableUsesSF[i] {
			nEntries = int(float64(nEntries) * scaleFactor)
		}
		tables[i] = tpchTools.ReadTable(tableFolder+tableNames[i]+tableExtension, tableParts[i], nEntries)
	}
	return
}

func prepareTablesToSend() {
	startTime := time.Now().UnixNano() / 1000000
	for i := 0; i < len(tables); i++ {
		fmt.Println("Preparing table", tableNames[i])
		prepareTable(tableNames[i], headers[i], keys[i], tables[i], dataQueue)
		fmt.Println("Prepared table", tableNames[i])
	}
	//Send msg signalling the end
	dataQueue <- QueuedMsg{code: QUEUE_COMPLETE}
	finishTime := time.Now().UnixNano() / 1000000
	fmt.Println("Time taken for preparing tables to send:", finishTime-startTime, "ms")
}

/*
func sendTableData(conn net.Conn, primKeys [][]int) {

		fmt.Println("Table len", len(tables))
		for i := 0; i < len(primKeys); i++ {
			fmt.Println("Preparing to send", tableNames[i])
			sendTable(conn, tableNames[i], headers[i], primKeys[i], tables[i])
		}
		fmt.Println("Sent", nProtosSent, "protos.")

	prepareAndSendIndexes(conn, primKeys)
}
*/

//This is both used for sending the initial data and updates, at least for now
func prepareTable(name string, headers []string, primKeys []int, table [][]string, queue chan QueuedMsg) {
	for nEntriesLeft := len(table); nEntriesLeft > 0; nEntriesLeft -= maxUpdSize {
		nUpds := min(maxUpdSize, nEntriesLeft)
		//fmt.Println("Preparing", nUpds, "updates")
		updates := []antidote.UpdateObjectParams{*getTableUpd(name, headers, primKeys, table[nEntriesLeft-nUpds:nEntriesLeft])}
		dataQueue <- QueuedMsg{Message: antidote.CreateStaticUpdateObjs(updates), code: antidote.StaticUpdateObjs}
	}
}

/*
func sendTable(conn net.Conn, name string, headers []string, primKeys []int, table [][]string) {
	fmt.Println("Table", name, "len", len(table))
	for nEntriesLeft := len(table); nEntriesLeft > 0; nEntriesLeft -= maxUpdSize {
		nUpds := min(maxUpdSize, nEntriesLeft)
		fmt.Println("Preparing to send", nUpds, "updates")
		updates := []antidote.UpdateObjectParams{*getTableUpd(name, headers, primKeys, table[nEntriesLeft-nUpds:nEntriesLeft])}
		fmt.Println("Sending proto")
		antidote.SendProto(antidote.StaticUpdateObjs, antidote.CreateStaticUpdateObjs(updates), conn)
		fmt.Println("Receiving proto")
		antidote.ReceiveProto(conn)
		fmt.Println("Reply proto received")
		nProtosSent++
	}
}
*/

//Updates to multiple keys of a table (e.g., multiple keys of the costumer table)
//Each table is also a RWEmbMap whose values are RWEmbMaps.
func getTableUpd(name string, headers []string, primKeys []int, table [][]string) *antidote.UpdateObjectParams {
	objsUpds := make(map[string]crdt.UpdateArguments)
	for _, obj := range table {
		//fmt.Println("Preparing table upd for an obj")
		key, upd := getEntryUpd(headers, primKeys, obj)
		//key, upd := getEntryORMapUpd(headers, primKeys, obj)
		//fmt.Println("Update prepared for key", key)
		/*
			if existingUpd, has := objsUpds[key]; has {
				fmt.Println("ERROR - there's already an existing update for this key!")
				fmt.Println("Key", key)
				fmt.Println("Update", existingUpd)
				fmt.Println("New upd", *upd)
			}
		*/
		objsUpds[key] = *upd
	}
	//fmt.Println("Len of key updates:", len(objsUpds))
	var keyUpd crdt.UpdateArguments = crdt.EmbMapUpdateAll{Upds: objsUpds}
	return &antidote.UpdateObjectParams{
		KeyParams:  antidote.KeyParams{Key: name, CrdtType: antidote.CRDTType_RRMAP, Bucket: "bkt"},
		UpdateArgs: &keyUpd,
	}
}

//Inner most updates: the object/entry itself (upd to an RWEmbMap, whose entries are LWWRegisters)
func getEntryUpd(headers []string, primKeys []int, table []string) (objKey string, entriesUpd *crdt.EmbMapUpdateAll) {
	//fmt.Println("Preparing entry upd for an obj")
	//fmt.Println("Table entry len", len(table))
	//fmt.Println("Table entry", table)
	entries := make(map[string]crdt.UpdateArguments)
	for i, fieldName := range headers {
		entries[fieldName] = crdt.SetValue{NewValue: table[i]}
	}
	//fmt.Println("Fields prepared, building key")
	var buf strings.Builder
	for _, keyIndex := range primKeys {
		buf.WriteString(table[keyIndex])
		//TODO: Remove, just for easier debug
		buf.WriteRune('_')
	}
	//fmt.Println("Key built, returning")
	//TODO: Also remove the slicing after removing the "_"
	return buf.String()[:buf.Len()-1], &crdt.EmbMapUpdateAll{Upds: entries}
}

//Inner most updates: the object/entry itself (upd to an RWEmbMap, whose entries are LWWRegisters)
func getEntryORMapUpd(headers []string, primKeys []int, table []string) (objKey string, entriesUpd *crdt.MapAddAll) {
	//fmt.Println("Preparing entry upd for an obj")
	//fmt.Println("Table entry len", len(table))
	//fmt.Println("Table entry", table)
	entries := make(map[string]crdt.Element)
	for i, fieldName := range headers {
		entries[fieldName] = crdt.Element(table[i])
	}
	//fmt.Println("Fields prepared, building key")
	var buf strings.Builder
	for _, keyIndex := range primKeys {
		buf.WriteString(table[keyIndex])
		//TODO: Remove, just for easier debug
		buf.WriteRune('_')
	}
	//fmt.Println("Key built, returning")
	//TODO: Also remove the slicing after removing the "_"
	return buf.String()[:buf.Len()-1], &crdt.MapAddAll{Values: entries}
}

func prepareIndexesToSend() {
	finishTimes := make([]int64, 7)
	finishTimes[0] = time.Now().UnixNano() / 1000000
	queries := []int{-1, 3, 5, 11, 14, 15, 18}
	//TODO: Later consider 6, 10, 2, by this order.
	fmt.Println("Preparing indexes...")
	//queueIndex(prepareQ2Index())
	queueIndex(prepareQ3Index())
	finishTimes[1] = time.Now().UnixNano() / 1000000
	fmt.Println("Index Q3 OK")
	queueIndex(prepareQ5Index())
	finishTimes[2] = time.Now().UnixNano() / 1000000
	fmt.Println("Index Q5 OK")
	queueIndex(prepareQ11Index())
	finishTimes[3] = time.Now().UnixNano() / 1000000
	fmt.Println("Index Q11 OK")
	queueIndex(prepareQ14Index())
	finishTimes[4] = time.Now().UnixNano() / 1000000
	fmt.Println("Index Q14 OK")
	queueIndex(prepareQ15Index())
	finishTimes[5] = time.Now().UnixNano() / 1000000
	fmt.Println("Index Q15 OK")
	queueIndex(prepareQ18Index())
	finishTimes[6] = time.Now().UnixNano() / 1000000
	fmt.Println("Index Q18 OK")

	//Signal the end
	indexQueue <- QueuedMsg{code: QUEUE_COMPLETE}

	fmt.Println("All indexes prepared.")
	fmt.Println("Total time taken preparing indexes:", finishTimes[6]-finishTimes[0], "ms")
	for i := 1; i < len(finishTimes); i++ {
		fmt.Printf("Time taken for Q%d: %d ms\n", queries[i], finishTimes[i]-finishTimes[i-1])
	}
}

//TODO: Delete, but keep the comments
func prepareAndSendIndexes(conn net.Conn, primKeys [][]int) {
	fmt.Println("Sending indexes...")
	/*
		It would help to have the headers also indexed, in order to avoid having to search for each column.
		I likelly also need to have the tables indexed by primKey?
	*/
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
	q14Upds := prepareQ14Index()

	startTxn := antidote.CreateStartTransaction(nil)
	antidote.SendProto(antidote.StartTrans, startTxn, conn)
	_, txnReplyProto, _ := antidote.ReceiveProto(conn)
	txnId := txnReplyProto.(*antidote.ApbStartTransactionResp).GetTransactionDescriptor()

	//Send all indexes
	updsProto := antidote.CreateUpdateObjsFromArray(txnId, q14Upds)
	antidote.SendProto(antidote.UpdateObjs, updsProto, conn)
	antidote.ReceiveProto(conn)

	commitTxn := antidote.CreateCommitTransaction(txnId)
	antidote.SendProto(antidote.CommitTrans, commitTxn, conn)
	antidote.ReceiveProto(conn)
}

func queueIndex(upds []antidote.UpdateObjectParams) {
	indexQueue <- QueuedMsg{Message: antidote.CreateUpdateObjsFromArray(nil, upds), code: antidote.UpdateObjs}
}

/*
func prepareQ2Index() (upds []antidote.UpdateObjectParams) {
	//topk: max(s_acctbal).
	//Each TopK contains the suppliers that supply a part of a given size and type at min cost.
	//TopK should be applied for each pair of (pair.size, pair.type, region)
	//Each entry in a TopK is the supplierID and the supplycost.
	//Each TopK internally should be ordered from min to max. We can achieve this using negative values.

	//For now extra data (s_acctbal, n_name, p_mfgr, s_address, etc.) needs to be fetched separatelly.
	//We'll

	return
}
*/

func prepareQ3Index() (upds []antidote.UpdateObjectParams) {
	//sum: l_extendedprice*(1-l_discount)
	//This is for each pair of (l_orderkey, o_orderdate, o_shippriority).
	//The query will still need to collect all whose date < o_orderdate and then filter for only those whose
	//l_shipdate is > date. Or maybe we can have some map for that...

	//This actually needs to be a topK for each pair (o_orderdate, c_mktsegment).
	//In the topK we need to store the sum and orderkey. We'll know the priority when obtaining
	//the object. We don't know the orderdate tho. This is due to each topK storing all o_orderdate < orderDate, but with l_shipdate > o_orderdate.
	//Each entry in the topK will be for an orderkey.
	//Orderdate will need to be retrieved separatelly.

	/*
		type IntPair struct {
			first  int32
			second int32
		}*/
	//segment -> orderDate -> orderkey -> sum
	sumMap := make(map[string]map[int8]map[int32]*float64)
	//TODO: Store this somewhere. This comes from segments
	mktSegments := []string{"AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"}

	var i int32
	var j int8
	//Preparing maps
	for _, seg := range mktSegments {
		segMap := make(map[int8]map[int32]*float64)
		for j = 1; j <= 31; j++ {
			segMap[j] = make(map[int32]*float64)
		}
		sumMap[seg] = segMap
	}

	//Note: not much efficient, but we don't really know how old can an orderdate be without being shipped. And we also don't have any index for dates
	minDate, maxDate := &Date{YEAR: 1995, MONTH: 03, DAY: 01}, &Date{YEAR: 1995, MONTH: 03, DAY: 31}
	var item *LineItem
	var minDay int8 = 0
	const maxDay int8 = 31
	var segMap map[int8]map[int32]*float64
	var currSum *float64
	var has bool
	nUpds := 0

	for _, order := range procTables.Orders[1:] {
		//To be in range for the maps, o_orderDate must be <= than the highest date 1995-03-31
		//And l_shipdate must be >= than the smallest date 1995-03-01
		if order.O_ORDERDATE.isSmallerOrEqual(maxDate) {
			//fmt.Println("OrderDate:", *order.O_ORDERDATE, ". Compare result:", order.O_ORDERDATE.isSmallerOrEqual(maxDate))
			//Get the customer's market segment
			segMap = sumMap[procTables.Customers[order.O_CUSTKEY].C_MKTSEGMENT]
			for i = 1; i <= procTables.MaxOrderLineitems; i++ {
				item = procTables.LineItems[GetLineitemIndex(int8(i), order.O_ORDERKEY, procTables.MaxOrderLineitems)]
				if item == nil {
					//Not all orders have the max number of lineitems
					break
				}
				//Check if L_SHIPDATE is higher than minDate and, if it is, check month/year. If month/year > march 1995, then add to all entries. Otherwise, use day to know which entries.
				if item.L_SHIPDATE.isHigher(minDate) || minDate == item.L_SHIPDATE {
					if item.L_SHIPDATE.MONTH > 3 || item.L_SHIPDATE.YEAR > 1995 {
						//All days
						minDay = 1
					} else {
						minDay = item.L_SHIPDATE.DAY + 1
					}
					//fmt.Println("OrderDate:", *order.O_ORDERDATE, "ShipDate:", *item.L_SHIPDATE)
					//Make a for from minDay to 31 to fill the map
					for j = minDay; j <= maxDay; j++ {
						currSum, has = segMap[j][order.O_ORDERKEY]
						if !has {
							currSum = new(float64)
							segMap[j][order.O_ORDERKEY] = currSum
							nUpds++
						}
						*currSum += item.L_EXTENDEDPRICE * (1.0 - item.L_DISCOUNT)
					}
				}
			}
		}
	}

	//TODO: TopKAddAll for optimization purposes?
	upds = make([]antidote.UpdateObjectParams, nUpds)
	var keyArgs antidote.KeyParams
	i = 0
	for mktSeg, segMap := range sumMap {
		for day, dayMap := range segMap {
			//A topK per pair (mktsegment, orderdate)
			keyArgs = antidote.KeyParams{
				Key:      SEGM_DELAY + mktSeg + strconv.FormatInt(int64(day), 10),
				CrdtType: antidote.CRDTType_TOPK_RMV,
				Bucket:   "bkt",
			}
			//TODO: Actually use float
			for orderKey, sum := range dayMap {
				var currUpd crdt.UpdateArguments = crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: orderKey, Score: int32(*sum)}}
				upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
				i++
			}
		}
	}
	fmt.Println(nUpds)

	return
}

func prepareQ5Index() (upds []antidote.UpdateObjectParams) {
	//2.4.5:
	//sum: l_extendedprice * (1 - l_discount)
	//Group this by the pair (country, year) (i.e., group all the days of a year in the same CRDT).
	//Only lineitems for which the costumer and supplier are of the same nation count for this sum.
	//On the query, we ask for a given year and region. However, the results must be shown by nation
	//EmbMap CRDT for each region+date and then inside each there's a CounterCRDT for nation?

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

	//Actually collecting the data
	var order *Orders
	var customer *Customer
	var supplier *Supplier
	var year int16
	var nationKey, regionKey int8
	var value *float64
	var lineItem *LineItem
	var j, lastID int32 = 0, 0

	for j = 1; j < int32(len(procTables.LineItems)); j++ {
		lineItem = procTables.LineItems[j]
		if lineItem == nil {
			//Need to skip positions. Don't forget that j is already incremented at the end of the cycle
			j += procTables.MaxOrderLineitems - lastID - 1
			lastID = 0
		} else {
			//Conditions:
			//Ordered year between 1993 and 1997 (inclusive)
			//Supplier and customer of same nation
			//Calculate: l_extendedprice * (1 - l_discount)
			order = procTables.Orders[GetOrderIndex(lineItem.L_ORDERKEY)]
			year = order.O_ORDERDATE.YEAR
			if year >= 1993 && year <= 1997 {
				customer = procTables.Customers[order.O_CUSTKEY]
				supplier = procTables.Suppliers[lineItem.L_SUPPKEY]
				nationKey = customer.C_NATIONKEY
				if nationKey == supplier.S_NATIONKEY {
					regionKey = procTables.Nations[nationKey].N_REGIONKEY
					value = sumMap[regionKey][year][nationKey]
					*value += lineItem.L_EXTENDEDPRICE * (1 - lineItem.L_DISCOUNT)
				}
			}
			lastID = int32(lineItem.L_LINENUMBER)
		}
	}

	//TODO (later): Way to get only part of an EmbMapCRDT.
	//TODO: Actually have a CRDT to store a float instead of an int
	//Prepare the updates. *5 as there's 5 years and we'll do a embMapCRDT for each (region, year)
	upds = make([]antidote.UpdateObjectParams, len(sumMap)*5)
	i = 0
	years := []string{"1993", "1994", "1995", "1996", "1997"}
	regS := ""
	for regK, regionMap := range sumMap {
		regS = NATION_REVENUE + procTables.Regions[regK].R_NAME
		for year = 1993; year <= 1997; year++ {
			yearMap := regionMap[year]
			regDateUpd := crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
			for natK, value := range yearMap {
				regDateUpd.Upds[procTables.Nations[natK].N_NAME] = crdt.Increment{Change: int32(*value)}
			}
			var args crdt.UpdateArguments = regDateUpd
			upds[i] = antidote.UpdateObjectParams{
				KeyParams:  antidote.KeyParams{Key: regS + years[year-1993], CrdtType: antidote.CRDTType_RRMAP, Bucket: "bkt"},
				UpdateArgs: &args,
			}
			i++
		}
	}
	fmt.Println(len(sumMap) * 5)

	return
}

func prepareQ11Index() (upds []antidote.UpdateObjectParams) {
	//2.4.11:	sum: ps_supplycost * ps_availqty
	//Group by (part, nation). Nation is the supplier nation.
	//The query itself only wants the groups for a given nation in which the sum represents >= 0.01%/SF of
	//all parts supplied by that nation. But that's defined as a variable...

	//2.4.11 (simple topk of a sum, along with a key (ID))
	//Use a topK for each nation. We need to keep the values for every part, as later on it may reach the top
	//Theorically it could be a topK without removals, as there's no updates on the tables referred
	//If we assume updates are possible, then there could be a problem when two users concurrently update the sum of the same nation
	//, since the value ot a topK is static (i.e., not a counter)
	//Also, the value of the topK should be a decimal instead of an integer.
	//We also need a counterCRDT per nation to store the value of sum(ps_supplycost * ps_availqty) * FRACTION for filtering.

	//nation -> part -> ps_supplycost * ps_availqty
	//This could also be an array as nationIDs are 0 to n-1
	nationMap := make(map[int8]map[int32]*float64)
	totalSumMap := make(map[int8]*float64)

	//Preparing maps for each nation
	for _, nation := range procTables.Nations {
		nationMap[nation.N_NATIONKEY] = make(map[int32]*float64)
		totalSumMap[nation.N_NATIONKEY] = new(float64)
	}

	var partMap map[int32]*float64
	var currSum, currTotalSum *float64
	var currValue float64
	var supplier *Supplier
	var has bool
	nUpds := len(procTables.Nations) //Assuming all nations have at least one supplier. Initial value corresponds to the number of nations
	//Calculate totals
	for _, partSup := range procTables.PartSupps {
		supplier = procTables.Suppliers[partSup.PS_SUPPKEY]
		partMap = nationMap[supplier.S_NATIONKEY]
		currSum, has = partMap[partSup.PS_PARTKEY]
		currTotalSum = totalSumMap[supplier.S_NATIONKEY]
		if !has {
			currSum = new(float64)
			partMap[partSup.PS_PARTKEY] = currSum
			nUpds++
		}
		currValue = (float64(partSup.PS_AVAILQTY) * partSup.PS_SUPPLYCOST)
		*currSum += currValue
		*currTotalSum += currValue
	}

	//Preparing updates for the topK CRDTs
	//TODO: TopKAddAll for optimization purposes
	upds = make([]antidote.UpdateObjectParams, nUpds, nUpds)
	//var currUpd crdt.UpdateArguments
	var keyArgs antidote.KeyParams
	i := 0

	for natKey, partMap := range nationMap {
		keyArgs = antidote.KeyParams{
			Key:      IMP_SUPPLY + procTables.Nations[natKey].N_NAME,
			CrdtType: antidote.CRDTType_TOPK_RMV,
			Bucket:   "bkt",
		}
		for partKey, value := range partMap {
			//TODO: Not use int32
			var currUpd crdt.UpdateArguments = crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: partKey, Score: int32(*value)}}
			upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
			i++
		}
	}

	//Preparing updates for the counter CRDTs
	for natKey, value := range totalSumMap {
		keyArgs = antidote.KeyParams{
			Key:      SUM_SUPPLY + procTables.Nations[natKey].N_NAME,
			CrdtType: antidote.CRDTType_COUNTER,
			Bucket:   "bkt",
		}
		//TODO: Not use int32
		var currUpd crdt.UpdateArguments = crdt.Increment{Change: int32(*value * (0.0001 / scaleFactor))}
		upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
		i++
	}

	fmt.Println(nUpds)
	return
}

func prepareQ14Index() (upds []antidote.UpdateObjectParams) {
	//Avg CRDT
	//sum + count (or mix together if possible): l_extendedprice * (1 - l_discount), when p_type starts with "PROMO"
	//Group by month (l_shipdate). Asked date always starts at the 1st day of a given month, between 1993 and 1997.
	//The date interval always covers the whole month.

	//Plan: Use a AddMultipleValue.
	//Keep a map of string -> struct{}{} for the partKeys that we know are of type PROMO%
	//Likelly we should even go through all the parts first to achieve that
	//Then go through lineItem, check with the map, and update the correct sums

	mapPromo, mapTotal := make(map[string]*float64), make(map[string]*float64)
	inPromo := make(map[int32]struct{})
	const promo = "PROMO"

	//Checking which parts are in promo
	for _, part := range procTables.Parts[1:] {
		if strings.HasPrefix(part.P_TYPE, promo) {
			inPromo[part.P_PARTKEY] = struct{}{}
		}
	}

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
	var lineItem *LineItem
	lastID := 0
	for k := 1; k < len(procTables.LineItems); k++ {
		lineItem = procTables.LineItems[k]
		if lineItem == nil {
			//Need to skip positions. Don't forget that k is already incremented at the end of the cycle
			k += int(procTables.MaxOrderLineitems) - lastID - 1
			lastID = 0
		} else {
			year = lineItem.L_SHIPDATE.YEAR
			if year >= 1993 && year <= 1997 {
				revenue = lineItem.L_EXTENDEDPRICE * (1.0 - lineItem.L_DISCOUNT)
				date = strconv.FormatInt(int64(year), 10) + strconv.FormatInt(int64(lineItem.L_SHIPDATE.MONTH), 10)
				if _, has := inPromo[lineItem.L_PARTKEY]; has {
					*mapPromo[date] += revenue
				}
				*mapTotal[date] += revenue
			}
			lastID = int(lineItem.L_LINENUMBER)
		}
	}

	//Create the updates
	upds = make([]antidote.UpdateObjectParams, len(mapPromo), len(mapPromo))
	i = 0
	for key, totalP := range mapTotal {
		promo := *mapPromo[key]
		var currUpd crdt.UpdateArguments = crdt.AddMultipleValue{
			SumValue: int64(100.0 * promo),
			NAdds:    int64(*totalP),
		}
		upds[i] = antidote.UpdateObjectParams{
			KeyParams:  antidote.KeyParams{Key: PROMO_PERCENTAGE + key, CrdtType: antidote.CRDTType_AVG, Bucket: "bkt"},
			UpdateArgs: &currUpd,
		}
		i++
	}
	fmt.Println(len(mapPromo))

	return
}

//TODO: Can I really assume that quarters are Jan-Mar, Apr-Jun, Jul-Sep, Oct-Dec?
func prepareQ15Index() (upds []antidote.UpdateObjectParams) {
	//2.4.15: topk: sum(l_extendedprice * (1-l_discount))
	//Group by month. The sum corresponds to the revenue shipped by a supplier during a given quarter of the year.
	//Date can start between first month of 1993 and 10th month of 1997 (first day always)
	//Have one topK per quarter
	//month -> supplierID -> sum
	yearMap := make(map[int16]map[int8]map[int32]*float64)
	var monthMap map[int8]map[int32]*float64

	//Preparing map instances for each quarter between 1993 and 1997
	var year int16 = 1993
	for ; year <= 1997; year++ {
		monthMap = make(map[int8]map[int32]*float64)
		monthMap[1] = make(map[int32]*float64)
		monthMap[4] = make(map[int32]*float64)
		monthMap[7] = make(map[int32]*float64)
		monthMap[10] = make(map[int32]*float64)
		yearMap[year] = monthMap
	}

	var month int8
	var suppMap map[int32]*float64
	var currValue *float64
	var has bool
	nUpds := 0 //Assuming each quarter has at least one supplier
	var item *LineItem
	lastID := 0

	//Go through lineitems to calculate totals
	for i := 1; i < len(procTables.LineItems); i++ {
		item = procTables.LineItems[i]
		if item == nil {
			//Need to skip positions. Don't forget that i is already incremented at the end of the cycle
			i += int(procTables.MaxOrderLineitems) - lastID - 1
			lastID = 0
		} else {
			year = item.L_SHIPDATE.YEAR
			if year >= 1993 && year <= 1997 {
				month = ((item.L_SHIPDATE.MONTH-1)/3)*3 + 1
				suppMap = yearMap[year][month]
				currValue, has = suppMap[item.L_SUPPKEY]
				if !has {
					currValue = new(float64)
					suppMap[item.L_SUPPKEY] = currValue
					nUpds++
				}
				*currValue += (item.L_EXTENDEDPRICE * (1.0 - item.L_DISCOUNT))
			}
			lastID = int(item.L_LINENUMBER)
		}
	}

	//Create the updates
	upds = make([]antidote.UpdateObjectParams, nUpds)
	var keyArgs antidote.KeyParams

	//TODO: TopK with multipleAdd
	i := 0
	for year, monthMap := range yearMap {
		for month = 1; month <= 12; month += 3 {
			keyArgs = antidote.KeyParams{
				Key:      TOP_SUPPLIERS + strconv.FormatInt(int64(year), 10) + strconv.FormatInt(int64(month), 10),
				CrdtType: antidote.CRDTType_TOPK_RMV,
				Bucket:   "bkt",
			}
			for suppKey, value := range monthMap[month] {
				//TODO: Not use int32 for value
				var currUpd crdt.UpdateArguments = crdt.TopKAdd{TopKScore: crdt.TopKScore{
					Id:    int32(suppKey),
					Score: int32(*value),
				}}
				upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
				i++
			}
		}
	}
	fmt.Println(nUpds)

	return
}

func prepareQ18Index() (upds []antidote.UpdateObjectParams) {
	//2.4.18: topk with 100 elements: o_totalprice, o_orderdate
	//Group by l_quantity. Theorically only need quantities between 312 and 315.
	//Ideally we would store c_custKey + "_" + o_orderKey, and then fetch each one. For now, we're only storing o_orderKey.
	//tmp struct to hold the results
	type PairInt struct {
		first  int32
		second int32
	}
	//quantity -> orderKey -> (custKey, quantity)
	quantityMap := make(map[int32]map[int32]*PairInt)
	//Preparing possible quantities
	quantityMap[312] = make(map[int32]*PairInt)
	quantityMap[313] = make(map[int32]*PairInt)
	quantityMap[314] = make(map[int32]*PairInt)
	quantityMap[315] = make(map[int32]*PairInt)

	//Going through orders and, for each order, checking all its lineitems
	//Doing the other way around is possible but would require a map of order -> quantity.
	//Let's do both and then choose one

	//Order -> lineitem
	//item := procTables.LineItems[1]
	var item *LineItem
	orderKey, currItemIndex, currQuantity := int32(-1), int32(0), int32(0)
	var currPair *PairInt
	var orderKeyOffset int32
	for _, order := range procTables.Orders[1:] {
		currQuantity = 0
		orderKey = order.O_ORDERKEY
		orderKeyOffset = GetLineitemIndex(1, orderKey, procTables.MaxOrderLineitems)
		item = procTables.LineItems[orderKeyOffset]
		for currItemIndex = 1; item != nil && item.L_ORDERKEY == order.O_ORDERKEY; currItemIndex++ {
			//fmt.Println(item)
			currQuantity += item.L_QUANTITY
			item = procTables.LineItems[orderKeyOffset+currItemIndex]
			//item = procTables.LineItems[GetLineitemIndex(int8(currItemIndex+1), orderKey, procTables.MaxOrderLineitems)]
		}
		//fmt.Printf("Order ID %d has total quantity %d.\n", orderKey, currQuantity)
		if currQuantity >= 312 {
			currPair = &PairInt{first: order.O_CUSTKEY, second: currQuantity}
			for minQ, orderMap := range quantityMap {
				if currQuantity >= minQ {
					orderMap[orderKey] = currPair
				} else {
					break
				}
			}
		}
	}

	/*
		//lineitem -> Order
		lastLineitemId := int8(0)
		var item *LineItem
		orderMap := make(map[int32]*PairInt)
		orderKey := int32(-1)
		var currPair *PairInt
		var holderPair PairInt	//Used to host a new pair that is being created
		var has bool
		//First, acumulate total quantities per order
		for i := 1; i < len(procTables.LineItems); i++ {
			item = procTables[i]
			if item == nil {
				//Skip remaining positions for the order, which will also be nil
				i += procTables.MaxOrderLineitems - lastLineItemId
				continue
			}
			//lineItem not nil, process
			orderKey = item.L_ORDERKEY
			currPair, has = orderMap[orderKey]
			if !has {
				holderPair = PairInt{second: 0}
				currPair = &holderPair
				orderMap[orderKey] = &holderPair
			}
			currPair.quantity += item.L_QUANTITY
		}
		//Now, go through all orders and store in the respective maps
		for orderKey, pair := range orderMap {
			if pair.second >= 312 {
				pair.first = procTables[GetOrderIndex(orderKey)].O_CUSTKEY
				for minQ, orderQuantityMap := range quantityMap {
					if pair.second >= minQ {
						orderQuantityMap[orderKey] = pair
					} else {
						break
					}
				}
			}
		}
	*/

	//Create the updates
	upds = make([]antidote.UpdateObjectParams, len(quantityMap[312])+len(quantityMap[313])+
		len(quantityMap[314])+len(quantityMap[315]))
	var keyArgs antidote.KeyParams

	//TODO: TopK with multipleAdd
	i := 0
	for quantity, orderMap := range quantityMap {
		keyArgs = antidote.KeyParams{
			Key:      LARGE_ORDERS + strconv.FormatInt(int64(quantity), 10),
			CrdtType: antidote.CRDTType_TOPK_RMV,
			Bucket:   "bkt",
		}
		for orderKey, pair := range orderMap {
			//TODO: Store the customerKey also
			var currUpd crdt.UpdateArguments = crdt.TopKAdd{TopKScore: crdt.TopKScore{
				Id:    orderKey,
				Score: pair.second,
			}}
			upds[i] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
			i++
		}
	}
	fmt.Println(len(quantityMap[312]) + len(quantityMap[313]) + len(quantityMap[314]) + len(quantityMap[315]))

	return
}

/*
func sendTableCreation(headers [][]string, keys []string) {
	updates := make([]antidote.UpdateObjectParams, 1)
}

func sendTableEntries(headers [][]string, keys []string, table [][][]string) {

}
*/

func readUpds() (ordersUpds [][]string, lineItemUpds [][]string, deleteKeys []string) {
	return tpchTools.ReadUpdates(updCompleteFilename[:], updEntries[:], updParts[:])
}

//TODO: This will need to be updated as we add "index" CRDTs
//TODO: Get the "right moment" to send these updates. This may require signaling from the queries goroutine
//TODO: This also needs to update the internal db! Or, at least, the index CRDTs
func sendUpdateData(ordersUpds [][]string, lineItemUpds [][]string, deleteKeys []string) {
	//Separate connection
	//conn, _ := net.Dial("tcp", servers[0])
	//Key is always at 0
	primKeys := []int{0}
	//TODO: Both updates MUST be grouped in a single transaction according to TPC-H
	prepareTable(tableNames[headerOrders], headers[headerOrders], primKeys, ordersUpds, updsQueue)
	prepareTable(tableNames[headerLineItems], headers[headerLineItems], primKeys, lineItemUpds, updsQueue)
	//sendTable(conn, tableNames[headerOrders], headers[headerOrders], primKeys, ordersUpds)
	//sendTable(conn, tableNames[headerLineItems], headers[headerLineItems], primKeys, lineItemUpds)
	prepareDeletes(deleteKeys)
}

func prepareDeletes(deleteKeys []string) {
	tableKeys := []string{tableNames[3], tableNames[1]}
	deleteProto := getDeletes(tableKeys, deleteKeys)
	updsQueue <- QueuedMsg{Message: antidote.CreateStaticUpdateObjs(deleteProto), code: antidote.StaticUpdateObjs}
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
		KeyParams:  antidote.KeyParams{Key: tableKey, CrdtType: antidote.CRDTType_RRMAP, Bucket: "bkt"},
		UpdateArgs: &mapRemove,
	}
}

func sendQueries(conn net.Conn) {
	finishTimes := make([]int64, 7)
	finishTimes[0] = time.Now().UnixNano() / 1000000
	queries := []int{-1, 3, 5, 11, 14, 15, 18}
	fmt.Println()
	sendQ3(conn)
	finishTimes[1] = time.Now().UnixNano() / 1000000
	fmt.Println()
	sendQ5(conn)
	finishTimes[2] = time.Now().UnixNano() / 1000000
	fmt.Println()
	sendQ11(conn)
	finishTimes[3] = time.Now().UnixNano() / 1000000
	fmt.Println()
	sendQ14(conn)
	finishTimes[4] = time.Now().UnixNano() / 1000000
	fmt.Println()
	sendQ15(conn)
	finishTimes[5] = time.Now().UnixNano() / 1000000
	fmt.Println()
	sendQ18(conn)
	finishTimes[6] = time.Now().UnixNano() / 1000000
	fmt.Println()

	fmt.Println("All queries have been executed")
	fmt.Println("Total time for the queries:", finishTimes[6]-finishTimes[0], "ms")
	for i := 1; i < len(finishTimes); i++ {
		fmt.Printf("Time taken for Q%d: %d ms\n", queries[i], finishTimes[i]-finishTimes[i-1])
	}
}

func sendQ3(conn net.Conn) {
	//TODO
	//This should likelly be somewhere else
	segments := []string{"AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"}
	rndSeg := segments[rand.Intn(5)]
	rndDay := 1 + rand.Int63n(31)
	readParam := []antidote.ReadObjectParams{antidote.ReadObjectParams{
		KeyParams: antidote.KeyParams{Key: SEGM_DELAY + rndSeg + strconv.FormatInt(rndDay, 10), CrdtType: antidote.CRDTType_TOPK_RMV, Bucket: "bkt"},
	}}

	readProto := antidote.CreateStaticReadObjs(nil, readParam)
	antidote.SendProto(antidote.StaticReadObjs, readProto, conn)
	_, replyProto, _ := antidote.ReceiveProto(conn)
	//topK := antidote.ConvertProtoObjectToAntidoteState(
	//replyProto.(*antidote.ApbStaticReadObjectsResp).GetObjects().GetObjects()[0], antidote.CRDTType_TOPK_RMV).(crdt.TopKValueState)
	topKProto := replyProto.(*antidote.ApbStaticReadObjectsResp).GetObjects().GetObjects()[0].GetTopk()
	values := topKProto.GetValues()
	sort.Slice(values, func(i, j int) bool { return values[i].GetScore() > values[j].GetScore() })

	orderIDs := make([]int32, 10)
	written := 0
	for _, pair := range values {
		orderIDs[written] = pair.GetPlayerId()
		written++
		if written == 10 {
			break
		}
	}

	orderIDs = orderIDs[:written]
	//TODO: Only read part of the map
	readParam[0] = antidote.ReadObjectParams{KeyParams: antidote.KeyParams{Key: tableNames[3], CrdtType: antidote.CRDTType_RRMAP, Bucket: "bkt"}}
	readProto = antidote.CreateStaticReadObjs(nil, readParam)
	antidote.SendProto(antidote.StaticReadObjs, readProto, conn)
	_, replyProto, _ = antidote.ReceiveProto(conn)
	ordersMapState := antidote.ConvertProtoObjectToAntidoteState(
		replyProto.(*antidote.ApbStaticReadObjectsResp).GetObjects().GetObjects()[0], antidote.CRDTType_RRMAP).(crdt.EmbMapEntryState)

	fmt.Printf("Q3: top 10 orders for segment %s not delivered as of %d:03:1995:\n", rndSeg, rndDay)
	written = 0
	var orderID int32
	var order map[string]crdt.State
	for _, pair := range values {
		orderID = pair.GetPlayerId()
		order = ordersMapState.States[strconv.FormatInt(int64(orderID), 10)].(crdt.EmbMapEntryState).States
		fmt.Printf("%d | %d | %s | %s\n", orderID, pair.GetScore(),
			order["O_ORDERDATE"].(crdt.RegisterState).Value.(string), order["O_SHIPPRIORITY"].(crdt.RegisterState).Value.(string))
		written++
		if written == 10 {
			break
		}
	}

}

func sendQ5(conn net.Conn) {
	//TODO: Read only part of the EmbMapCRDT
	rndRegion := procTables.Regions[rand.Intn(len(procTables.Regions))].R_NAME
	rndYear := 1993 + rand.Int63n(5)
	readParam := []antidote.ReadObjectParams{antidote.ReadObjectParams{
		KeyParams: antidote.KeyParams{
			Key: NATION_REVENUE + rndRegion + strconv.FormatInt(rndYear, 10), CrdtType: antidote.CRDTType_RRMAP, Bucket: "bkt"},
	},
	}

	readProto := antidote.CreateStaticReadObjs(nil, readParam)
	antidote.SendProto(antidote.StaticReadObjs, readProto, conn)
	_, replyProto, _ := antidote.ReceiveProto(conn)
	mapState := antidote.ConvertProtoObjectToAntidoteState(
		replyProto.(*antidote.ApbStaticReadObjectsResp).GetObjects().GetObjects()[0], antidote.CRDTType_RRMAP).(crdt.EmbMapEntryState)

	//year -> nation
	//yearMap := mapState.States[strconv.FormatInt(rndYear, 10)].(crdt.EmbMapEntryState)
	fmt.Println("Q5: Values for", rndRegion, "in year", rndYear)
	for nation, valueState := range mapState.States {
		fmt.Printf("%s: %d\n", nation, valueState.(crdt.CounterState).Value)
	}

}

func sendQ11(conn net.Conn) {
	rndNation := procTables.Nations[rand.Intn(len(procTables.Nations))].N_NAME
	readParam := []antidote.ReadObjectParams{
		antidote.ReadObjectParams{KeyParams: antidote.KeyParams{Key: IMP_SUPPLY + rndNation, CrdtType: antidote.CRDTType_TOPK_RMV, Bucket: "bkt"}},
		antidote.ReadObjectParams{KeyParams: antidote.KeyParams{Key: SUM_SUPPLY + rndNation, CrdtType: antidote.CRDTType_COUNTER, Bucket: "bkt"}},
	}

	readProto := antidote.CreateStaticReadObjs(nil, readParam)
	antidote.SendProto(antidote.StaticReadObjs, readProto, conn)
	_, replyProto, _ := antidote.ReceiveProto(conn)
	objsProto := replyProto.(*antidote.ApbStaticReadObjectsResp).GetObjects().GetObjects()
	topkProto, counterProto := objsProto[0].GetTopk(), objsProto[1].GetCounter()

	minValue := counterProto.GetValue()
	fmt.Println("Q11: Values for", rndNation)
	values := topkProto.GetValues()
	sort.Slice(values, func(i, j int) bool { return values[i].GetScore() > values[j].GetScore() })
	for _, pair := range values {
		if pair.GetScore() < minValue {
			break
		}
		fmt.Printf("%d: %d.\n", pair.GetPlayerId(), pair.GetScore())
	}
}

func sendQ14(conn net.Conn) {
	/*
		2.4.14:	sum + count (or mix together if possible): l_extendedprice * (1 - l_discount), when p_type starts with "PROMO"
				Group by month (l_shipdate). Asked date always starts at the 1st day of a given month, between 1993 and 1997.
				The date interval always covers the whole month.
	*/
	rndForDate := rand.Int63n(60) //60 months from 1993 to 1997 (5 years)
	var year int64 = 1993 + rndForDate/12
	var month int64 = 1 + rndForDate%12
	var date string
	date = strconv.FormatInt(year, 10) + strconv.FormatInt(month, 10)

	readParam := []antidote.ReadObjectParams{antidote.ReadObjectParams{
		KeyParams: antidote.KeyParams{Key: PROMO_PERCENTAGE + date, CrdtType: antidote.CRDTType_AVG, Bucket: "bkt"},
	}}
	readProto := antidote.CreateStaticReadObjs(nil, readParam)
	antidote.SendProto(antidote.StaticReadObjs, readProto, conn)
	_, replyProto, _ := antidote.ReceiveProto(conn)
	avgProto := replyProto.(*antidote.ApbStaticReadObjectsResp)
	fmt.Printf("Q14: %d_%d: %f.\n", year, month, avgProto.GetObjects().GetObjects()[0].GetAvg().GetAvg())
}

func sendQ15(conn net.Conn) {
	rndQuarter := 1 + 3*rand.Int63n(4)
	rndYear := 1993 + rand.Int63n(5)
	date := strconv.FormatInt(rndYear, 10) + strconv.FormatInt(rndQuarter, 10)

	readParam := []antidote.ReadObjectParams{antidote.ReadObjectParams{
		KeyParams: antidote.KeyParams{Key: TOP_SUPPLIERS + date, CrdtType: antidote.CRDTType_TOPK_RMV, Bucket: "bkt"},
	}}
	readProto := antidote.CreateStaticReadObjs(nil, readParam)
	antidote.SendProto(antidote.StaticReadObjs, readProto, conn)
	_, replyProto, _ := antidote.ReceiveProto(conn)
	topkProto := replyProto.(*antidote.ApbStaticReadObjectsResp).GetObjects().GetObjects()[0].GetTopk()

	fmt.Printf("Q15: best supplier(s) for months [%d, %d] of year %d\n", rndQuarter, rndQuarter+2, rndYear)
	values := topkProto.GetValues()
	sort.Slice(values, func(i, j int) bool { return values[i].GetScore() > values[j].GetScore() })
	value := topkProto.GetValues()[0].GetScore()
	for _, pair := range values {
		if pair.GetScore() < value {
			break
		}
		fmt.Printf("%d: %d\n", pair.GetPlayerId(), pair.GetScore())
	}
}

func sendQ18(conn net.Conn) {
	//TODO: Optimize this in two regards: a) not download whole map, b) download customers simultaneously with orders
	//The latter requires for the ID in the topK to refer to both keys
	//Also, theorically this should be a single transaction instead of a static.
	rndQuantity := 312 + rand.Int31n(4)

	readParam := []antidote.ReadObjectParams{antidote.ReadObjectParams{
		KeyParams: antidote.KeyParams{Key: LARGE_ORDERS + strconv.FormatInt(int64(rndQuantity), 10), CrdtType: antidote.CRDTType_TOPK_RMV, Bucket: "bkt"},
	}}
	readProto := antidote.CreateStaticReadObjs(nil, readParam)
	antidote.SendProto(antidote.StaticReadObjs, readProto, conn)
	_, replyProto, _ := antidote.ReceiveProto(conn)
	topkProto := replyProto.(*antidote.ApbStaticReadObjectsResp).GetObjects().GetObjects()[0].GetTopk()
	values := topkProto.GetValues()
	sort.Slice(values, func(i, j int) bool { return values[i].GetScore() > values[j].GetScore() })

	//Get orders
	orderIDs := make([]int32, 100, 100)
	written := 0
	for _, pair := range values {
		if pair.GetScore() < rndQuantity {
			break
		}
		orderIDs[written] = pair.GetPlayerId()
		written++
		if written == 100 {
			break
		}
	}
	if written == 0 {
		fmt.Printf("Q18: top 100 customers for large quantity orders with quantity above %d: no match found.", rndQuantity)
		return
	}
	orderIDs = orderIDs[:written]
	//TODO: Only read part of the map
	readParam[0] = antidote.ReadObjectParams{KeyParams: antidote.KeyParams{Key: tableNames[3], CrdtType: antidote.CRDTType_RRMAP, Bucket: "bkt"}}
	readProto = antidote.CreateStaticReadObjs(nil, readParam)
	antidote.SendProto(antidote.StaticReadObjs, readProto, conn)
	_, replyProto, _ = antidote.ReceiveProto(conn)
	ordersMapState := antidote.ConvertProtoObjectToAntidoteState(
		replyProto.(*antidote.ApbStaticReadObjectsResp).GetObjects().GetObjects()[0], antidote.CRDTType_RRMAP).(crdt.EmbMapEntryState)

	customerIDs := make([]int32, 100, 100)
	mapCustomerIDs := make(map[int32]struct{})
	var currCustomerID int64
	var int32CustomerID int32
	written = 0
	for _, orderID := range orderIDs {
		currCustomerID, _ = strconv.ParseInt(ordersMapState.States[strconv.FormatInt(int64(orderID), 10)].(crdt.EmbMapEntryState).States["O_CUSTKEY"].(crdt.RegisterState).Value.(string), 10, 32)
		int32CustomerID = int32(currCustomerID)
		_, has := mapCustomerIDs[int32CustomerID]
		if !has {
			mapCustomerIDs[int32CustomerID] = struct{}{}
			customerIDs[written] = int32CustomerID
			written++
		}
	}

	customerIDs = customerIDs[:written]

	readParam[0] = antidote.ReadObjectParams{KeyParams: antidote.KeyParams{Key: tableNames[3], CrdtType: antidote.CRDTType_RRMAP, Bucket: "bkt"}}
	readProto = antidote.CreateStaticReadObjs(nil, readParam)
	antidote.SendProto(antidote.StaticReadObjs, readProto, conn)
	_, replyProto, _ = antidote.ReceiveProto(conn)
	custMapState := antidote.ConvertProtoObjectToAntidoteState(
		replyProto.(*antidote.ApbStaticReadObjectsResp).GetObjects().GetObjects()[0], antidote.CRDTType_RRMAP).(crdt.EmbMapEntryState)

	fmt.Printf("Q18 top 100 customers for large quantity orders with quantity above %d\n", rndQuantity)
	nPrinted := 0
	var order, customer map[string]crdt.State
	for _, pair := range values {
		if pair.GetScore() < rndQuantity {
			break
		}
		order = ordersMapState.States[strconv.FormatInt(int64(pair.GetPlayerId()), 10)].(crdt.EmbMapEntryState).States
		currCustomerID, _ = strconv.ParseInt(order["O_CUSTKEY"].(crdt.RegisterState).Value.(string), 10, 32)
		customer = custMapState.States[strconv.FormatInt(currCustomerID, 10)].(crdt.EmbMapEntryState).States
		fmt.Printf("%s | %d | %d | %s | %s | %d\n",
			customer["C_NAME"], currCustomerID, pair.GetPlayerId(), order["O_ORDERDATE"], order["O_TOTALPRICE"], pair.GetScore())
		nPrinted++
	}

	/*
		fmt.Printf("Q18 top 100 customers for large quantity orders with quantity above %d\n", rndQuantity)

		nPrinted := 0
		for _, pair := range values {
			if pair.GetScore() < rndQuantity {
				break
			}
			fmt.Printf("%d: %d\n", pair.GetPlayerId(), pair.GetScore())
			nPrinted++
			if nPrinted == 100 {
				break
			}
		}
		if nPrinted == 0 {
			fmt.Println("No orders satisfy the criteria")
		}
	*/
}

func min(first int, second int) int {
	if first < second {
		return first
	}
	return second
}

func ignore(any ...interface{}) {

}
