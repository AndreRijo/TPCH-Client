package main

import (
	"fmt"
	"strconv"
	"time"
)

//*****TABLES*****//

type Customer struct {
	C_CUSTKEY    int32
	C_NAME       string
	C_ADDRESS    string
	C_NATIONKEY  int8
	C_PHONE      string
	C_ACCTBAL    string
	C_MKTSEGMENT string
	C_COMMENT    string
}

type LineItem struct {
	L_ORDERKEY      int32
	L_PARTKEY       int32
	L_SUPPKEY       int32
	L_LINENUMBER    int8
	L_QUANTITY      int32
	L_EXTENDEDPRICE float64
	L_DISCOUNT      float64
	L_TAX           string
	L_RETURNFLAG    string
	L_LINESTATUS    string
	L_SHIPDATE      *Date
	L_COMMITDATE    string
	L_RECEIPTDATE   string
	L_SHIPINSTRUCT  string
	L_SHIPMODE      string
	L_COMMENT       string
}

type Nation struct {
	N_NATIONKEY int8
	N_NAME      string
	N_REGIONKEY int8
	N_COMMENT   string
}

type Orders struct {
	O_ORDERKEY      int32
	O_CUSTKEY       int32
	O_ORDERSTATUS   string
	O_TOTALPRICE    string
	O_ORDERDATE     *Date
	O_ORDERPRIORITY string
	O_CLERK         string
	O_SHIPPRIORITY  string
	O_COMMENT       string
}

type Part struct {
	P_PARTKEY     int32
	P_NAME        string
	P_MFGR        string
	P_BRAND       string
	P_TYPE        string
	P_SIZE        string
	P_CONTAINER   string
	P_RETAILPRICE string
	P_COMMENT     string
}

type Region struct {
	R_REGIONKEY int8
	R_NAME      string
	R_COMMENT   string
}

type PartSupp struct {
	PS_PARTKEY    int32
	PS_SUPPKEY    int32
	PS_AVAILQTY   int32
	PS_SUPPLYCOST float64
	PS_COMMENT    string
}

type Supplier struct {
	S_SUPPKEY   int32
	S_NAME      string
	S_ADDRESS   string
	S_NATIONKEY int8
	S_PHONE     string
	S_ACCTBAL   string
	S_COMMENT   string
}

//For Customers, Suppliers and Parts the first entry is empty in order for the ID to match the position in the array
//Orders and LineItems use special position. Always use GetOrder/LineItemIndex() methods to access them
//PartSupps has no easy indexing for now. Nations/Regions use 0-n and so do their IDs.
type Tables struct {
	Customers         []*Customer
	LineItems         []*LineItem
	Nations           []*Nation
	Orders            []*Orders
	Parts             []*Part
	PartSupps         []*PartSupp
	Regions           []*Region
	Suppliers         []*Supplier
	MaxOrderLineitems int32
}

//*****Auxiliary data types*****//

type Date struct {
	YEAR  int16
	MONTH int8
	DAY   int8
}

//returns true if the caller is higher than the argument
func (date *Date) isHigher(otherDate *Date) bool {
	if date.YEAR > otherDate.YEAR {
		return true
	}
	if date.YEAR < otherDate.YEAR {
		return false
	}
	//equal years
	if date.MONTH > otherDate.MONTH {
		return true
	}
	if date.MONTH < otherDate.MONTH {
		return false
	}
	//equal year and month
	if date.DAY > otherDate.DAY {
		return true
	}
	return false
}

func (date *Date) isSmallerOrEqual(otherDate *Date) bool {
	if date.YEAR < otherDate.YEAR {
		return true
	}
	if date.YEAR > otherDate.YEAR {
		return false
	}
	//equal years
	if date.MONTH < otherDate.MONTH {
		return true
	}
	if date.MONTH > otherDate.MONTH {
		return false
	}
	//equal year and month
	if date.DAY < otherDate.DAY {
		return false
	}
	if date.DAY > otherDate.DAY {
		return true
	}
	return true
}

func CreateClientTables(rawData [][][]string) (tables *Tables) {
	startTime := time.Now().UnixNano() / 1000000
	lineItems, maxOrderLineitems := createLineitemTable(rawData[1], len(rawData[3]))
	tables = &Tables{
		Customers:         createCustomerTable(rawData[0]),
		LineItems:         lineItems,
		Nations:           createNationTable(rawData[2]),
		Orders:            createOrdersTable(rawData[3]),
		Parts:             createPartTable(rawData[4]),
		PartSupps:         createPartsuppTable(rawData[5]),
		Regions:           createRegionTable(rawData[6]),
		Suppliers:         createSupplierTable(rawData[7]),
		MaxOrderLineitems: maxOrderLineitems,
	}
	endTime := time.Now().UnixNano() / 1000000
	fmt.Println("Time taken to process tables:", endTime-startTime, "ms")
	return
}

func createCustomerTable(cTable [][]string) (customers []*Customer) {
	//Customers IDs are 1 -> n, so we reserve an extra empty space at start
	fmt.Println("Creating customer table")
	customers = make([]*Customer, len(cTable)+1)
	var nationKey int64
	for i, entry := range cTable {
		nationKey, _ = strconv.ParseInt(entry[3], 10, 8)
		customers[i+1] = &Customer{
			C_CUSTKEY:    int32(i + 1),
			C_NAME:       entry[1],
			C_ADDRESS:    entry[2],
			C_NATIONKEY:  int8(nationKey),
			C_PHONE:      entry[4],
			C_ACCTBAL:    entry[5],
			C_MKTSEGMENT: entry[6],
			C_COMMENT:    entry[7],
		}
	}
	return
}

func createLineitemTable(liTable [][]string, nOrders int) (lineItems []*LineItem, maxLineItem int32) {
	fmt.Println("Creating lineItem table")
	//First discover what is the max number of items an order can have
	max := liTable[0][3]
	for _, entry := range liTable {
		if entry[3] > max {
			max = entry[3]
		}
	}
	max64, _ := strconv.ParseInt(max, 10, 32)
	maxLineItem = int32(max64)
	//TODO: If this max is the same accross SFs, then we can just hardcode it

	//TODO: Remove +100
	nEntries := int32(nOrders)*maxLineItem + maxLineItem + 1 + 100 //Leave one extra empty entry at the end for easier access via order
	lineItems = make([]*LineItem, nEntries, nEntries)
	var partKey, orderKey, suppKey, lineNumber, quantity int64
	var convLineNumber int8
	var convOrderKey int32
	var extendedPrice, discount float64

	for _, entry := range liTable {
		orderKey, _ = strconv.ParseInt(entry[0], 10, 32)
		partKey, _ = strconv.ParseInt(entry[1], 10, 32)
		suppKey, _ = strconv.ParseInt(entry[2], 10, 32)
		lineNumber, _ = strconv.ParseInt(entry[3], 10, 8)
		quantity, _ = strconv.ParseInt(entry[4], 10, 32)
		convLineNumber, convOrderKey = int8(lineNumber), int32(orderKey)
		extendedPrice, _ = strconv.ParseFloat(entry[5], 32)
		discount, _ = strconv.ParseFloat(entry[6], 32)

		index := GetLineitemIndex(convLineNumber, convOrderKey, maxLineItem)
		lineItems[index] = &LineItem{
			L_ORDERKEY:      convOrderKey,
			L_PARTKEY:       int32(partKey),
			L_SUPPKEY:       int32(suppKey),
			L_LINENUMBER:    convLineNumber,
			L_QUANTITY:      int32(quantity),
			L_EXTENDEDPRICE: extendedPrice,
			L_DISCOUNT:      discount,
			L_TAX:           entry[7],
			L_RETURNFLAG:    entry[8],
			L_LINESTATUS:    entry[9],
			L_SHIPDATE:      createDate(entry[10]),
			L_COMMITDATE:    entry[11],
			L_RECEIPTDATE:   entry[12],
			L_SHIPINSTRUCT:  entry[13],
			L_SHIPMODE:      entry[14],
			L_COMMENT:       entry[15],
		}
	}
	return
}

func createNationTable(nTable [][]string) (nations []*Nation) {
	fmt.Println("Creating nation table")
	nations = make([]*Nation, len(nTable))
	var nationKey, regionKey int64
	for i, entry := range nTable {
		nationKey, _ = strconv.ParseInt(entry[0], 10, 8)
		regionKey, _ = strconv.ParseInt(entry[2], 10, 8)
		nations[i] = &Nation{
			N_NATIONKEY: int8(nationKey),
			N_NAME:      entry[1],
			N_REGIONKEY: int8(regionKey),
			N_COMMENT:   entry[3],
		}
	}
	return
}

func createOrdersTable(oTable [][]string) (orders []*Orders) {
	fmt.Println("Creating orders table")
	orders = make([]*Orders, len(oTable)+1)
	var orderKey, customerKey int64
	for i, entry := range oTable {
		orderKey, _ = strconv.ParseInt(entry[0], 10, 32)
		customerKey, _ = strconv.ParseInt(entry[1], 10, 32)
		orders[i+1] = &Orders{
			O_ORDERKEY:      int32(orderKey),
			O_CUSTKEY:       int32(customerKey),
			O_ORDERSTATUS:   entry[2],
			O_TOTALPRICE:    entry[3],
			O_ORDERDATE:     createDate(entry[4]),
			O_ORDERPRIORITY: entry[5],
			O_CLERK:         entry[6],
			O_SHIPPRIORITY:  entry[7],
			O_COMMENT:       entry[8],
		}
	}
	return
}

func createPartTable(pTable [][]string) (parts []*Part) {
	fmt.Println("Creating parts table")
	parts = make([]*Part, len(pTable)+1)
	var partKey int64
	for i, entry := range pTable {
		partKey, _ = strconv.ParseInt(entry[0], 10, 32)
		parts[i+1] = &Part{
			P_PARTKEY:     int32(partKey),
			P_NAME:        entry[1],
			P_MFGR:        entry[2],
			P_BRAND:       entry[3],
			P_TYPE:        entry[4],
			P_SIZE:        entry[5],
			P_CONTAINER:   entry[6],
			P_RETAILPRICE: entry[7],
			P_COMMENT:     entry[8],
		}
	}
	return
}

func createPartsuppTable(psTable [][]string) (partSupps []*PartSupp) {
	fmt.Println("Creating partsupp table")
	partSupps = make([]*PartSupp, len(psTable))
	var partKey, suppKey, availQty int64
	var supplyCost float64
	for i, entry := range psTable {
		partKey, _ = strconv.ParseInt(entry[0], 10, 32)
		suppKey, _ = strconv.ParseInt(entry[1], 10, 32)
		availQty, _ = strconv.ParseInt(entry[2], 10, 32)
		supplyCost, _ = strconv.ParseFloat(entry[3], 64)
		partSupps[i] = &PartSupp{
			PS_PARTKEY:    int32(partKey),
			PS_SUPPKEY:    int32(suppKey),
			PS_AVAILQTY:   int32(availQty),
			PS_SUPPLYCOST: supplyCost,
			PS_COMMENT:    entry[4],
		}
	}
	return
}

func createRegionTable(rTable [][]string) (regions []*Region) {
	fmt.Println("Creating region table")
	regions = make([]*Region, len(rTable))
	var regionKey int64
	for i, entry := range rTable {
		regionKey, _ = strconv.ParseInt(entry[0], 10, 8)
		regions[i] = &Region{
			R_REGIONKEY: int8(regionKey),
			R_NAME:      entry[1],
			R_COMMENT:   entry[2],
		}
	}
	return
}

func createSupplierTable(sTable [][]string) (suppliers []*Supplier) {
	fmt.Println("Creating supplier table")
	suppliers = make([]*Supplier, len(sTable)+1)
	var suppKey, nationKey int64
	for i, entry := range sTable {
		suppKey, _ = strconv.ParseInt(entry[0], 10, 32)
		nationKey, _ = strconv.ParseInt(entry[3], 0, 8)
		suppliers[i+1] = &Supplier{
			S_SUPPKEY:   int32(suppKey),
			S_NAME:      entry[1],
			S_ADDRESS:   entry[2],
			S_NATIONKEY: int8(nationKey),
			S_PHONE:     entry[4],
			S_ACCTBAL:   entry[5],
			S_COMMENT:   entry[6],
		}
	}
	return
}

func GetOrderIndex(orderKey int32) (indexKey int32) {
	//1 -> 7: 1 -> 7
	//9 -> 15: 1 -> 7
	//32 -> 39: 8 -> 15
	//40 -> 47: 8 -> 15
	//64 -> 71: 16 -> 23
	//72 -> 79: 16 -> 23
	return orderKey%8 + 8*(orderKey/32)
}

func GetLineitemIndex(lineitemKey int8, orderKey int32, maxLineitem int32) (indexKey int32) {
	//Same idea as in getOrderIndex but... we need to find a way to manage with multiple keys
	//Note that delete deletes a whole order so all of the lineitems of that order get deleted.
	//And new lineitems are for new orders.
	//Seems like each order may have up to 7 lineitems
	//1: 1-7
	//2: 8-14
	//3: 15-21
	//... 4: 22, 5: 29, 6: 36, 7: 43-49, 8:50-56,
	//9: 1-7
	//10: 8-14
	//..
	//32: 57-63
	//64: 113-119
	//65: 120-126
	//Offset of lineItem in an order + order offset before it loops (1-8 and 9-15 use same slots e.g)
	//+ space after loop
	//1-8 are special cases
	lineKey := int32(lineitemKey)
	if orderKey <= 8 {
		return lineKey + maxLineitem*(orderKey-1)
	}
	//return (lineKey % (maxLineitem + 1)) + ((maxLineitem * (orderKey - 1)) % (maxLineitem * 8)) + maxLineitem*8*(orderKey/32)
	return (lineKey % (maxLineitem + 1)) + ((maxLineitem * (orderKey)) % (maxLineitem * 8)) + maxLineitem*8*(orderKey/32)
}

func createDate(stringDate string) (date *Date) {
	yearS, monthS, dayS := stringDate[0:4], stringDate[5:7], stringDate[8:10]
	year64, _ := strconv.ParseInt(yearS, 10, 16)
	month64, _ := strconv.ParseInt(monthS, 10, 8)
	day64, _ := strconv.ParseInt(dayS, 10, 8)
	return &Date{
		YEAR:  int16(year64),
		MONTH: int8(month64),
		DAY:   int8(day64),
	}
}
