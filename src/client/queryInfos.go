package client

import (
	"potionDB/crdt/crdt"
	"potionDB/crdt/proto"
	"strconv"
	"strings"
	tpch "tpch_data_processor/tpch"

	//pb "github.com/golang/protobuf/proto"
	pb "google.golang.org/protobuf/proto"
)

//Q2, Q11, Q16 have no updates
//Q3, Q4, Q5, Q10, Q17, Q18, Q21, Q22 do not need local version

/*
Number of CRDTs updated per query:
Q1: 1-60 (about ~95/% of the time, only 1 upd.)
Q3: 0-62 (add/remove can update up to 31 days, each day is 1 CRDT)
Q4: 0-2 (most of the time 2)
Q5: 0-2
Q6: 0-4 (up to 2 upds per add/rem. Only updates if item is with discount.)
Q7: 0-1 (local (nation) orders do not lead to any update. There is only 1 CRDT and thus, 1 update.)
Q8: 0-16 (only does updates for lineitems ordered during 1995 or 1996. May be up to 1 update per lineitem (if all parts are different))
Q9: 0-16 (up to 1 update per lineitem. Can expect on average 8 updates (as an average order has 4 lineitems).)
Q10: 0-2 (will be 0 if add and remove's orderdates are not 1993 or 1994.)
Q12: 0-4 (each new order/rem order may have up to 2 years. Each year is a CRDT.)
Q13: 0-3 (most often 1)
Q14: 0-8 (at most 2 years per add/rem, and 2 average per add/rem)
Q14_Local: same as Q14
Q15, Q15_top_sum: 0-4 (at most 2 pairs year+quarter per add/rem)
Q17: 0-16 (on average, 8. Up to 1 update per lineitem, if all parts are different)
Q18: 0-8 (one customer per add/rem, but may have to update up to 4 quantities. The majority of the times it will give 0 updates.)
Q19: 0-16 (on average, 8. Up to 1 update per lineitem, if all parts are different)
Q20: 0-16 (on average, 8. anUp to 1 update per lneitem, if all supplierNations are different)
Q21: 0-2 (1 per order. May often be 0 or 1 updates as not all orders only have 1 supplier late (or even any late at all))
Q22: 2 (1 per order, as each order has 1 customer.)
*/

//Contains all the methods related to each query's structure (SingleQ3, etc.)
//Methods to build only part of a query structure (i.e.., add-only or rem-only) are in queryInfosHelper.go

type SingleQueryInfo interface {
	buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo
	countUpds() (nUpds, nUpdsStats, nBlockUpds int)
	makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int)
	makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int)
}

//*****SingleQ3, Q3*****

type SingleQ3 struct {
	remMap      map[int8]struct{}
	updMap      map[int8]*float64
	updStartPos int8
	nUpds       int
	newSeg      string
	oldSeg      string
}

func (q3 SingleQ3) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	//Single segment, but multiple days (days are cumulative, and *possibly?* items may have different days)
	nUpds := 0
	if newOrder.O_ORDERDATE.IsLowerOrEqual(MAX_DATE_Q3) {
		//Need to do add
		q3.newsCalcHelper(newOrder, newItems)
		nUpds += int(MAX_MONTH_DAY - q3.updStartPos + 1)
	}
	//Single segment, but multiple days (as days are cumulative).
	if remOrder.O_ORDERDATE.IsLowerOrEqual(MAX_DATE_Q3) {
		//Need to do rem
		q3.remsCalcHelper(remOrder, remItems)
		nUpds += len(q3.remMap)
	}
	q3.nUpds, q3.newSeg, q3.oldSeg = nUpds, tables.Customers[newOrder.O_CUSTKEY].C_MKTSEGMENT, tables.Customers[remOrder.O_CUSTKEY].C_MKTSEGMENT

	return q3
}

func (q3 *SingleQ3) newsCalcHelper(order *tpch.Orders, items []*tpch.LineItem) {
	q3.updMap = make(map[int8]*float64)
	i, j, minDay := int8(1), int8(1), int8(1)
	for ; i <= 31; i++ {
		q3.updMap[i] = new(float64)
	}
	q3.updStartPos = MAX_MONTH_DAY + 1 //+1 so that the sum on getSingleQ3Upds ends as 0 if no upd is to be done.
	for _, item := range items {
		if item.L_SHIPDATE.IsHigherOrEqual(MIN_DATE_Q3) {
			if item.L_SHIPDATE.MONTH > 3 || item.L_SHIPDATE.YEAR > 1995 {
				//All days
				minDay = 1
			} else {
				minDay = item.L_SHIPDATE.DAY + 1
			}
			//Make a for from minDay to 31 to fill the map
			for j = minDay; j <= MAX_MONTH_DAY; j++ {
				*q3.updMap[j] += item.L_EXTENDEDPRICE * (1.0 - item.L_DISCOUNT)
			}
			if minDay < q3.updStartPos {
				q3.updStartPos = minDay
			}
		}
	}
}

func (q3 *SingleQ3) remsCalcHelper(order *tpch.Orders, items []*tpch.LineItem) {
	q3.remMap = make(map[int8]struct{})
	minDay, j := MIN_MONTH_DAY, int8(0)
	for _, item := range items {
		if item.L_SHIPDATE.IsHigherOrEqual(MIN_DATE_Q3) {
			if item.L_SHIPDATE.MONTH > 3 || item.L_SHIPDATE.YEAR > 1995 {
				//All days
				minDay = 1
			} else {
				minDay = item.L_SHIPDATE.DAY + 1
			}
			//Make a for from minDay to 31 to fill the map
			for j = minDay; j <= MAX_MONTH_DAY; j++ {
				q3.remMap[j] = struct{}{}
			}
		}
	}
}

func (q3 SingleQ3) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	//fmt.Printf("[Q3]nUpds: %d, nUpdsStats: %d, nBlockUpds: %d\n", q3.totalUpds, q3.totalUpds, getNBlockUpds(q3.totalUpds))
	return q3.nUpds, q3.nUpds, getNBlockUpds(q3.nUpds)
}

func (q3 SingleQ3) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {

	var keyArgs crdt.KeyParams
	//Adds. Due to the date restriction, there may be no adds.
	if q3.updStartPos != 0 && q3.updStartPos <= MAX_MONTH_DAY {
		updSegKey := SEGM_DELAY + q3.newSeg
		for day := q3.updStartPos; day <= MAX_MONTH_DAY; day++ {
			keyArgs = crdt.KeyParams{
				Key:      updSegKey + strconv.FormatInt(int64(day), 10),
				CrdtType: proto.CRDTType_TOPK_RMV,
				Bucket:   buckets[bucketI],
			}
			var currUpd crdt.UpdateArguments
			if INDEX_WITH_FULL_DATA {
				currUpd = crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: newOrder.O_ORDERKEY, Score: int32(*q3.updMap[day]), Data: packQ3IndexExtraData(newOrder)}}
			} else {
				currUpd = crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: newOrder.O_ORDERKEY, Score: int32(*q3.updMap[day])}}
			}
			buf[bufI] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: currUpd}
			bufI++
		}
	}
	return bufI
}

func (q3 SingleQ3) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {

	var keyArgs crdt.KeyParams
	//Rems
	if len(q3.remMap) > 0 {
		remSegKey := SEGM_DELAY + q3.oldSeg
		for day := range q3.remMap {
			keyArgs = crdt.KeyParams{
				Key:      remSegKey + strconv.FormatInt(int64(day), 10),
				CrdtType: proto.CRDTType_TOPK_RMV,
				Bucket:   buckets[bucketI],
			}
			//useTopKAll not relevant as it is only one order
			buf[bufI] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: crdt.TopKRemove{Id: remOrder.O_ORDERKEY}}
			bufI++
		}
	}
	return bufI
}

//*****SingleQ5, Q5*****

type SingleQ5 struct {
	remValue, updValue *float64
}

func (q5 SingleQ5) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	q5.updValue = q5.updsCalcHelper(tables, newOrder, newItems)
	q5.remValue = q5.updsCalcHelper(tables, remOrder, remItems)
	return q5
}

func (q5 SingleQ5) updsCalcHelper(tables *tpch.Tables, order *tpch.Orders, items []*tpch.LineItem) *float64 {
	value := 0.0
	orderNationKey := tables.Customers[order.O_CUSTKEY].C_NATIONKEY
	var suppNationKey int8
	for _, lineItem := range items {
		suppNationKey = tables.Suppliers[lineItem.L_SUPPKEY].S_NATIONKEY
		if suppNationKey == orderNationKey {
			value += lineItem.L_EXTENDEDPRICE * (1 - lineItem.L_DISCOUNT)
		}
	}
	return &value
}
func (q5 SingleQ5) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	if *q5.updValue != 0 {
		nUpds++
	}
	if *q5.remValue != 0 {
		nUpds++
	}
	//fmt.Printf("[Q5]nUpds: %d, nUpdsStats: %d, nBlockUpds: %d\n", nUpds, nUpds, getNBlockUpds(nUpds))
	return nUpds, nUpds, getNBlockUpds(nUpds)
}

func (q5 SingleQ5) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q5.makeUpdsArgsHelper(*q5.updValue, 1.0, tables, newOrder, buf, bufI, bucketI)
}

func (q5 SingleQ5) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q5.makeUpdsArgsHelper(*q5.remValue, -1.0, tables, remOrder, buf, bufI, bucketI)
}

func (q5 SingleQ5) makeUpdsArgsHelper(value float64, signal float64, tables *tpch.Tables, order *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	if value != 0 {
		remYear, remNation := order.O_ORDERDATE.YEAR, tables.Nations[tables.Customers[order.O_CUSTKEY].C_NATIONKEY]
		buf[bufI] = crdt.UpdateObjectParams{
			KeyParams: crdt.KeyParams{Key: NATION_REVENUE + tables.Regions[remNation.N_REGIONKEY].R_NAME + strconv.FormatInt(int64(remYear), 10),
				CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]},
			UpdateArgs: crdt.EmbMapUpdate{Key: remNation.N_NAME, Upd: crdt.Increment{Change: int32(signal * value)}},
		}
		return bufI + 1
	}
	return bufI
}

//*****SingleQ14, Q14*****

type SingleQ14 struct {
	mapPromo, mapTotal map[string]*float64
	nAdds, nRems       int
}

func (q14 SingleQ14) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	//May have multiple updates, as not all items in an order may have been shipped in the same date
	q14.mapPromo, q14.mapTotal = make(map[string]*float64), make(map[string]*float64)

	q14.updsCalcHelper(1, tables, newOrder, newItems)
	q14.updsCalcHelper(-1, tables, remOrder, remItems)

	return q14
}

func (q14 SingleQ14) updsCalcHelper(multiplier float64, tables *tpch.Tables, order *tpch.Orders, items []*tpch.LineItem) {
	var year int16
	revenue := 0.0
	date := ""
	for _, lineItem := range items {
		year = lineItem.L_SHIPDATE.YEAR
		if year >= 1993 && year <= 1997 {
			revenue = multiplier * lineItem.L_EXTENDEDPRICE * (1.0 - lineItem.L_DISCOUNT)
			date = strconv.FormatInt(int64(year), 10) + strconv.FormatInt(int64(lineItem.L_SHIPDATE.MONTH), 10)
			totalValue, has := q14.mapTotal[date]
			if !has {
				totalValue = new(float64)
				q14.mapTotal[date] = totalValue
				q14.mapPromo[date] = new(float64)
			}
			if _, has := tables.PromoParts[lineItem.L_PARTKEY]; has {
				*q14.mapPromo[date] += revenue
			}
			*q14.mapTotal[date] += revenue
		}
	}
}

func (q14 SingleQ14) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	size := len(q14.mapTotal)
	//fmt.Printf("[Q14]nUpds: %d, nUpdsStats: %d, nBlockUpds: %d\n", size, size, getNBlockUpds(size))
	return size, size, getNBlockUpds(size)
}

func (q14 SingleQ14) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	for key, totalP := range q14.mapTotal {
		promo := *q14.mapPromo[key]
		currUpd := crdt.AddMultipleValue{SumValue: int64(100.0 * promo), NAdds: int64(*totalP)}
		buf[bufI] = crdt.UpdateObjectParams{
			KeyParams:  crdt.KeyParams{Key: PROMO_PERCENTAGE + key, CrdtType: proto.CRDTType_AVG, Bucket: buckets[bucketI]},
			UpdateArgs: currUpd,
		}
		bufI++
	}

	return bufI
}
func (q14 SingleQ14) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	//Empty, does nothing.
	return bufI
}

//*****SingleLocalQ14, Q14*****

type SingleLocalQ14 struct {
	newQ14, remQ14 SingleQ14
}

/*
type SingleLocalQ14 struct {
	mapPromoRem, mapTotalRem, mapPromoUpd, mapTotalUpd map[string]*float64
}*/

func (q14 SingleLocalQ14) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	q14.newQ14.mapPromo, q14.newQ14.mapTotal, q14.remQ14.mapPromo, q14.remQ14.mapTotal = make(map[string]*float64),
		make(map[string]*float64), make(map[string]*float64), make(map[string]*float64)

	q14.remQ14.updsCalcHelper(1, tables, newOrder, newItems)
	q14.newQ14.updsCalcHelper(-1, tables, remOrder, remItems)

	return q14
}

func (q14 SingleLocalQ14) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	nUpdsN, nUpdsStatsN, nBlockUpdsN := q14.newQ14.countUpds()
	nUpdsR, nUpdsStatsR, nBlockUpdsR := q14.remQ14.countUpds()
	return nUpdsN + nUpdsR, nUpdsStatsN + nUpdsStatsR, getNBlockUpds(nBlockUpdsN + nBlockUpdsR)
}

func (q14 SingleLocalQ14) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q14.newQ14.makeNewUpdArgs(tables, newOrder, buf, bufI, bucketI)
}
func (q14 SingleLocalQ14) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	//SingleQ14.makeRemUpdArgs does nothing, so we use the makeNewUpdArgs instead. Same logic for add/rem.
	return q14.remQ14.makeNewUpdArgs(tables, remOrder, buf, bufI, bucketI)
}

//*****SingleQ15, Q15*****

type SingleQ15 struct {
	updEntries map[int16]map[int8]map[int32]struct{}
}

func (q15 SingleQ15) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	//Different items may have different suppliers... thus, multiple updates.
	//Also shipdates are potencially different.

	//These methods are also used in other files.
	updEntries := createQ15EntriesMap()
	q15UpdsNewCalcHelper(newItems, q15Map, updEntries)
	q15UpdsRemCalcHelper(remItems, q15Map, updEntries)

	return SingleQ15{updEntries: updEntries}
}

func (q15 SingleQ15) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	if !useTopKAll {
		for _, monthMap := range q15.updEntries {
			for _, suppMap := range monthMap {
				nUpds += len(suppMap)
			}
		}
		//fmt.Printf("[Q15]nUpds: %d, nUpdsStats: %d, nBlockUpds: %d\n", nUpds, nUpds, getNBlockUpds(nUpds))
		return nUpds, nUpds, getNBlockUpds(nUpds)
	} else {
		var suppMapValues map[int32]*float64
		foundA, foundR := 0, 0
		for year, monthMap := range q15.updEntries {
			for month, suppMap := range monthMap {
				if len(suppMap) == 1 {
					nUpds++
					nUpdsStats++
				} else {
					suppMapValues = q15Map[year][month]
					for _, value := range suppMapValues {
						if *value > 0 {
							foundA++
						} else {
							foundR++
						}
					}
					nUpdsStats += foundA + foundR
					if foundA > 0 {
						nUpds++
						foundA = 0
					}
					if foundR > 0 {
						nUpds++
						foundR = 0
					}
				}
			}
		}
	}
	//fmt.Printf("[Q15]nUpds: %d, nUpdsStats: %d, nBlockUpds: %d\n", nUpds, nUpdsStats, getNBlockUpds(nUpds))
	return nUpds, nUpdsStats, getNBlockUpds(nUpds)
}

func (q15 SingleQ15) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	newBufI, _ = makeQ15IndexUpdsDeletesHelper(q15Map, q15.updEntries, bucketI, buf, bufI)
	return
}
func (q15 SingleQ15) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	//Does nothing
	return bufI
}

//*****SingleLocalQ15, Q15*****

type SingleLocalQ15 struct {
	newQ15, remQ15 SingleQ15
}

/*type SingleLocalQ15 struct {
	remEntries, updEntries map[int16]map[int8]map[int32]struct{}
}*/

func (q15 SingleLocalQ15) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	updKey, remKey := tables.OrderToRegionkey(newOrder), tables.OrderToRegionkey(remOrder)
	updEntries, remEntries := createQ15EntriesMap(), createQ15EntriesMap()
	q15UpdsNewCalcHelper(newItems, q15LocalMap[updKey], updEntries)
	q15UpdsRemCalcHelper(remItems, q15LocalMap[remKey], remEntries)

	return SingleLocalQ15{newQ15: SingleQ15{updEntries: updEntries}, remQ15: SingleQ15{updEntries: remEntries}}
}

func (q15 SingleLocalQ15) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	nUpdsN, nUpdsStatsN, nBlockUpdsN := q15.newQ15.countUpds()
	nUpdsR, nUpdsStatsR, nBlockUpdsR := q15.remQ15.countUpds()
	return nUpdsN + nUpdsR, nUpdsStatsN + nUpdsStatsR, getNBlockUpds(nBlockUpdsN + nBlockUpdsR)
}

func (q15 SingleLocalQ15) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	regKey := tables.OrderToRegionkey(newOrder)
	newBufI, _ = makeQ15IndexUpdsDeletesHelper(q15LocalMap[regKey], q15.newQ15.updEntries, bucketI, buf, bufI)
	return newBufI
}
func (q15 SingleLocalQ15) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	regKey := tables.OrderToRegionkey(remOrder)
	newBufI, _ = makeQ15IndexUpdsDeletesHelper(q15LocalMap[regKey], q15.remQ15.updEntries, bucketI, buf, bufI)
	return newBufI
}

//*****SingleQ15TopSum, Q15TopSum*****

type SingleQ15TopSum struct {
	diffEntries map[int16]map[int8]map[int32]float64
}

func (q15 SingleQ15TopSum) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	diffEntries := createQ15TopSumEntriesMap()
	q15TopSumUpdsNewCalcHelper(newItems, q15Map, diffEntries)
	q15TopSumUpdsRemCalcHelper(remItems, q15Map, diffEntries)

	return SingleQ15TopSum{diffEntries: diffEntries}
}

func (q15 SingleQ15TopSum) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	if !useTopKAll {
		for _, monthMap := range q15.diffEntries {
			for _, suppMap := range monthMap {
				nUpds += len(suppMap)
			}
		}
		//fmt.Printf("[Q15]nUpds: %d, nUpdsStats: %d, nBlockUpds: %d\n", nUpds, nUpds, getNBlockUpds(nUpds))
		return nUpds, nUpds, getNBlockUpds(nUpds)
	} else {
		var suppMapValues map[int32]*float64
		foundInc, foundDec := 0, 0
		for year, monthMap := range q15.diffEntries {
			for month, suppMap := range monthMap {
				if len(suppMap) == 1 {
					nUpds++
					nUpdsStats++
				} else {
					suppMapValues = q15Map[year][month]
					for _, value := range suppMapValues {
						if *value > 0 {
							foundInc++
						} else {
							foundDec++
						}
					}
					nUpdsStats += foundInc + foundDec
					if foundInc > 0 {
						nUpds++
						foundInc = 0
					}
					if foundDec > 0 {
						nUpds++
						foundDec = 0
					}
				}
			}
		}
	}
	//fmt.Printf("[Q15]nUpds: %d, nUpdsStats: %d, nBlockUpds: %d\n", nUpds, nUpdsStats, getNBlockUpds(nUpds))
	return nUpds, nUpdsStats, getNBlockUpds(nUpds)
}

func (q15 SingleQ15TopSum) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	newBufI, _ = makeQ15TopSumIndexUpdsDeletesHelper(q15Map, q15.diffEntries, bucketI, buf, bufI)
	return
}
func (q15 SingleQ15TopSum) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	//Does nothing
	return bufI
}

//*****SingleLocalQ15TopSum, Q15TopSum*****

type SingleLocalQ15TopSum struct {
	newQ15, remQ15 SingleQ15TopSum
}

/*type SingleLocalQ15TopSum struct {
	updEntries, remEntries map[int16]map[int8]map[int32]float64
}*/

func (q15 SingleLocalQ15TopSum) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {

	updKey, remKey := tables.OrderToRegionkey(newOrder), tables.OrderToRegionkey(remOrder)
	updEntries, remEntries := createQ15TopSumEntriesMap(), createQ15TopSumEntriesMap()
	q15TopSumUpdsNewCalcHelper(newItems, q15LocalMap[updKey], updEntries)
	q15TopSumUpdsRemCalcHelper(remItems, q15LocalMap[remKey], remEntries)

	return SingleLocalQ15TopSum{newQ15: SingleQ15TopSum{diffEntries: updEntries}, remQ15: SingleQ15TopSum{diffEntries: remEntries}}
}

func (q15 SingleLocalQ15TopSum) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	nUpdsN, nUpdsStatsN, nBlockUpdsN := q15.newQ15.countUpds()
	nUpdsR, nUpdsStatsR, nBlockUpdsR := q15.remQ15.countUpds()
	return nUpdsN + nUpdsR, nUpdsStatsN + nUpdsStatsR, nBlockUpdsN + nBlockUpdsR
}

func (q15 SingleLocalQ15TopSum) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	regKey := tables.OrderToRegionkey(newOrder)
	newBufI, _ = makeQ15TopSumIndexUpdsDeletesHelper(q15LocalMap[regKey], q15.newQ15.diffEntries, bucketI, buf, bufI)
	return newBufI
}
func (q15 SingleLocalQ15TopSum) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	regKey := tables.OrderToRegionkey(remOrder)
	newBufI, _ = makeQ15TopSumIndexUpdsDeletesHelper(q15LocalMap[regKey], q15.remQ15.diffEntries, bucketI, buf, bufI)
	return newBufI
}

//*****SingleQ18*****

type SingleQ18 struct {
	remQuantity, updQuantity int64
}

func (q18 SingleQ18) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	//Single order -> single customer. However, it may have to potencially update the 4 quantity indexes
	for _, item := range remItems {
		q18.remQuantity += int64(item.L_QUANTITY)
	}
	for _, item := range newItems {
		q18.updQuantity += int64(item.L_QUANTITY)
	}
	return q18
}

func (q18 SingleQ18) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	nUpds += q18.countUpdsHelper(q18.updQuantity)
	nUpds += q18.countUpdsHelper(q18.remQuantity)
	//fmt.Printf("[Q18]nUpds: %d, nUpdsStats: %d, nBlockUpds: %d\n", nUpds, nUpds, getNBlockUpds(nUpds))
	return nUpds, nUpds, getNBlockUpds(nUpds)
}

func (q18 SingleQ18) countUpdsHelper(value int64) (nUpds int) {
	if value >= 312 {
		nUpds = int(value) - 312 + 1
		if nUpds > 4 {
			return 4
		}
		return
	}
	return 0
}

func (q18 SingleQ18) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	updQ := q18.updQuantity
	var keyArgs crdt.KeyParams

	if updQ >= 312 {
		//fmt.Println("[Q18]Upd quantity is actually >= 312", updQ, newOrder.O_ORDERKEY)
		if updQ > 315 {
			updQ = 315
		}
		for i := int64(312); i <= updQ; i++ {
			keyArgs = crdt.KeyParams{
				Key:      LARGE_ORDERS + strconv.FormatInt(i, 10),
				CrdtType: proto.CRDTType_TOPK_RMV,
				Bucket:   buckets[bucketI],
			}
			//useTopKAll is irrelevant since it is only 1 order
			var currUpd crdt.UpdateArguments
			if !INDEX_WITH_FULL_DATA {
				currUpd = crdt.TopKAdd{TopKScore: crdt.TopKScore{Id: newOrder.O_ORDERKEY, Score: int32(q18.updQuantity)}}
			} else {
				currUpd = crdt.TopKAdd{TopKScore: crdt.TopKScore{
					Id:    newOrder.O_ORDERKEY,
					Score: int32(q18.updQuantity),
					Data:  packQ18IndexExtraData(newOrder, tables.Customers[newOrder.O_CUSTKEY])}}
			}
			buf[bufI] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: currUpd}
			bufI++
		}
	}
	return bufI
}
func (q18 SingleQ18) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	remQ := q18.remQuantity
	var keyArgs crdt.KeyParams

	if remQ >= 312 {
		if remQ > 315 {
			remQ = 315
		}
		for i := int64(312); i <= remQ; i++ {
			keyArgs = crdt.KeyParams{
				Key:      LARGE_ORDERS + strconv.FormatInt(i, 10),
				CrdtType: proto.CRDTType_TOPK_RMV,
				Bucket:   buckets[bucketI],
			}
			//useTopKAll is irrelevant since it is only 1 order
			buf[bufI] = crdt.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: crdt.TopKRemove{Id: remOrder.O_ORDERKEY}}
			bufI++
		}
	}
	return bufI
}

//*****SingleQ1, Q1**+**

type SingleQ1 struct {
	q1Map map[int8]map[string]*Q1Data
	done  *bool
}

func (q1 SingleQ1) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	q1.q1Map, q1.done = make(map[int8]map[string]*Q1Data), new(bool)
	//TODO: Problem is that q1Map is not initialized for the days.
	//Can either initialize it manually first or do our own calcHelper. Probably own calcHelper
	//And can use signal for add/rem.
	q1.q1CalcHelper(newOrder, newItems, true)
	q1.q1CalcHelper(remOrder, remItems, false)
	return q1
}

func (q1 SingleQ1) q1CalcHelper(order *tpch.Orders, items []*tpch.LineItem, isAdd bool) {
	for _, item := range items {
		shipdate := item.L_SHIPDATE
		if shipdate.IsLowerOrEqual(MIN_DATE_Q1) {
			q1.fillQ1Data(120, item, isAdd)
		} else if shipdate.IsLowerOrEqual(MAX_DATE_Q1) {
			startPos := int8(60 + MAX_DATE_Q1.CalculateDiffDate(&shipdate))
			for ; startPos >= 60; startPos-- {
				q1.fillQ1Data(startPos, item, isAdd)
			}
		} else {
			continue //Too recent order, skip.
		}
	}
}

func (q1 SingleQ1) fillQ1Data(day int8, item *tpch.LineItem, isAdd bool) {
	currDayEntry, has := q1.q1Map[day]
	var currData *Q1Data
	if !has {
		currDayEntry = make(map[string]*Q1Data)
		currData = &Q1Data{}
		currDayEntry[item.L_RETURNFLAG+item.L_LINESTATUS] = currData
		q1.q1Map[day] = currDayEntry
	} else {
		currData, has = currDayEntry[item.L_RETURNFLAG+item.L_LINESTATUS]
		if !has {
			currData = &Q1Data{}
			currDayEntry[item.L_RETURNFLAG+item.L_LINESTATUS] = currData
		}
	}
	if isAdd {
		currData.sumQuantity += int(item.L_QUANTITY)
		currData.sumPrice += item.L_EXTENDEDPRICE
		currData.sumDiscount += item.L_DISCOUNT
		currData.sumDiscPrice += (item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT))
		currData.sumCharge += (item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT) * (1 + item.L_TAX))
		currData.nItems++
	} else {
		currData.sumQuantity -= int(item.L_QUANTITY)
		currData.sumPrice -= item.L_EXTENDEDPRICE
		currData.sumDiscount -= item.L_DISCOUNT
		currData.sumDiscPrice -= (item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT))
		currData.sumCharge -= (item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT) * (1 + item.L_TAX))
		currData.nItems--
	}
}

/*
func (q1 SingleQ1) remCalcHelper(remOrder *tpch.Orders, remItems []*tpch.LineItem) {
	var currData *Q1Data
	for _, item := range remItems {
		shipdate := item.L_SHIPDATE
		if shipdate.IsLowerOrEqual(MIN_DATE_Q1) {
			currData = q1.q1Map[120][item.L_RETURNFLAG+item.L_LINESTATUS]
			currData.sumQuantity -= int(item.L_QUANTITY)
			currData.sumPrice -= item.L_EXTENDEDPRICE
			currData.sumDiscount -= item.L_DISCOUNT
			currData.sumDiscPrice -= (item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT))
			currData.sumCharge -= (item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT) * (1 + item.L_TAX))
			currData.nItems--
		} else if shipdate.IsLowerOrEqual(MAX_DATE_Q1) {
			startPos := int8(60 + MAX_DATE_Q1.CalculateDiffDate(&shipdate))
			for ; startPos >= 60; startPos-- {
				currData = q1.q1Map[startPos][item.L_RETURNFLAG+item.L_LINESTATUS]
				currData.sumQuantity -= int(item.L_QUANTITY)
				currData.sumPrice -= item.L_EXTENDEDPRICE
				currData.sumDiscount -= item.L_DISCOUNT
				currData.sumDiscPrice -= (item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT))
				currData.sumCharge -= (item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT) * (1 + item.L_TAX))
				currData.nItems--
			}
		} else {
			continue //Too recent order, skip.
		}
	}
}*/

func (q1 SingleQ1) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return len(q1.q1Map), len(q1.q1Map), getNBlockUpds(len(q1.q1Map))
}

func (q1 SingleQ1) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	if !*q1.done {
		*q1.done = true
		return makeQ1IndexUpdsHelper(q1.q1Map, buf, bufI, bucketI)
	}
	return bufI
}

func (q1 SingleQ1) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q1.makeNewUpdArgs(tables, remOrder, buf, bufI, bucketI)
}

//*****SingleLocalQ1, Q1**+**

type SingleLocalQ1 struct {
	q1New, q1Rem SingleQ1
}

func (q1 SingleLocalQ1) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	q1.q1New, q1.q1Rem = SingleQ1{q1Map: make(map[int8]map[string]*Q1Data), done: new(bool)}, SingleQ1{q1Map: make(map[int8]map[string]*Q1Data), done: new(bool)}
	q1.q1New.q1CalcHelper(newOrder, newItems, true)
	q1.q1Rem.q1CalcHelper(remOrder, remItems, false)
	return q1
}

func (q1 SingleLocalQ1) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	nUpdsN, nUpdsStatsN, nBlockUpdsN := q1.q1New.countUpds()
	nUpdsR, nUpdsStatsR, nBlockUpdsR := q1.q1Rem.countUpds()
	return nUpdsN + nUpdsR, nUpdsStatsN + nUpdsStatsR, nBlockUpdsN + nBlockUpdsR
}

func (q1 SingleLocalQ1) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q1.q1New.makeNewUpdArgs(tables, newOrder, buf, bufI, bucketI)
}

func (q1 SingleLocalQ1) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q1.q1Rem.makeRemUpdArgs(tables, remOrder, buf, bufI, bucketI)
}

func getNBlockUpds(nUpds int) int {
	if nUpds > 0 {
		return 1
	}
	return 0
}

//*****SingleQ4, Q4**+**
//Does not need local version as SingleQ4 already does new and rem updates seperately.

type SingleQ4 struct {
	doesInc, doesDec bool
}

func (q4 SingleQ4) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	return SingleQ4{doesInc: q4.calcHelper(newOrder, newItems), doesDec: q4.calcHelper(remOrder, remItems)}
}

func (q4 SingleQ4) calcHelper(order *tpch.Orders, items []*tpch.LineItem) bool {
	year := order.O_ORDERDATE.YEAR
	if year < 1993 || year > 1997 {
		return false
	}
	for _, lineitem := range items {
		if lineitem.L_COMMITDATE.IsLowerOrEqual(&lineitem.L_RECEIPTDATE) {
			return true
		}
	}
	return false
}

func (q4 SingleQ4) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	if q4.doesInc {
		nUpds++
	}
	if q4.doesDec {
		nUpds++
	}
	return nUpds, nUpds, getNBlockUpds(nUpds)
}

func (q4 SingleQ4) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	if q4.doesInc {
		return q4.makeUpdsArgsHelper(tables, newOrder, buf, bufI, bucketI, 1)
	}
	return bufI
}

func (q4 SingleQ4) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	if q4.doesDec {
		return q4.makeUpdsArgsHelper(tables, remOrder, buf, bufI, bucketI, -1)
	}
	return bufI
}

func (q4 SingleQ4) makeUpdsArgsHelper(tables *tpch.Tables, order *tpch.Orders, buf []crdt.UpdateObjectParams, bufI, bucketI, signal int) (newBufI int) {
	date := order.O_ORDERDATE
	year, quarter := date.YEAR, tpch.MonthToQuarter(date.MONTH)
	embMapUpd := crdt.EmbMapUpdate{Key: order.O_ORDERPRIORITY, Upd: crdt.Increment{Change: int32(signal)}}
	key := Q4_KEY + strconv.FormatInt(int64(year), 10) + strconv.FormatInt(int64(quarter), 10)
	buf[bufI] = crdt.UpdateObjectParams{
		KeyParams:  crdt.KeyParams{Key: key, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]},
		UpdateArgs: embMapUpd,
	}
	return bufI + 1
}

//*****SingleQ6, Q6**+**

// May be different years, quantity, everything... :(
type SingleQ6 struct {
	diffMap map[int16]map[int8]map[int8]*float64
	nUpds   int
	done    *bool
}

func (q6 SingleQ6) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	q6.diffMap, q6.done = make(map[int16]map[int8]map[int8]*float64), new(bool)
	for year := int16(1993); year <= 1997; year++ {
		q6.diffMap[year] = map[int8]map[int8]*float64{24: make(map[int8]*float64), 25: make(map[int8]*float64)}
	}
	q6.nUpds += q6.calcHelper(newItems, 1)
	q6.nUpds += q6.calcHelper(remItems, -1)
	return q6
}

func (q6 SingleQ6) calcHelper(items []*tpch.LineItem, signal float64) (nUpds int) {
	for _, item := range items {
		if item.L_DISCOUNT > 0 && item.L_SHIPDATE.YEAR >= 1993 && item.L_SHIPDATE.YEAR <= 1997 && item.L_QUANTITY <= 25 {
			if item.L_QUANTITY <= 24 {
				value, has := q6.diffMap[item.L_SHIPDATE.YEAR][24][int8(item.L_DISCOUNT*100)]
				if !has {
					nUpds++
					value = pb.Float64(0)
					q6.diffMap[item.L_SHIPDATE.YEAR][24][int8(item.L_DISCOUNT*100)] = value
				}
				*value += item.L_EXTENDEDPRICE * L_DISCOUNT * signal
			}
			value, has := q6.diffMap[item.L_SHIPDATE.YEAR][25][int8(item.L_DISCOUNT*100)]
			if !has {
				nUpds++
				value = pb.Float64(0)
				q6.diffMap[item.L_SHIPDATE.YEAR][25][int8(item.L_DISCOUNT*100)] = value
			}
			*value += item.L_EXTENDEDPRICE * L_DISCOUNT * signal
		}
	}
	return
}

func (q6 SingleQ6) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q6.nUpds, q6.nUpds, nBlockUpds
}

func (q6 SingleQ6) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	if !*q6.done {
		var keyParams crdt.KeyParams
		for year, yearMap := range q6.diffMap {
			for quantity, quantityMap := range yearMap {
				if len(quantityMap) > 0 {
					keyParams = crdt.KeyParams{
						Key:      Q6_KEY + strconv.FormatInt(int64(year), 10) + strconv.FormatInt(int64(quantity), 10),
						CrdtType: proto.CRDTType_RRMAP,
						Bucket:   buckets[bucketI],
					}
					mapUpd := crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
					for disc, value := range quantityMap {
						if *value != 0 {
							mapUpd.Upds[strconv.FormatInt(int64(disc), 10)] = crdt.IncrementFloat{Change: *value}
						}
					}
					buf[bufI] = crdt.UpdateObjectParams{KeyParams: keyParams, UpdateArgs: mapUpd}
					bufI++
				}
			}
		}
		*q6.done = true
	}
	return bufI
}

func (q6 SingleQ6) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q6.makeNewUpdArgs(tables, remOrder, buf, bufI, bucketI)
}

//*****SingleLocalQ6, Q6**+**

// Add and Rem may be for different regions
type SingleLocalQ6 struct {
	newQ6, remQ6 SingleQ6
}

func (q6 SingleLocalQ6) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	q6.newQ6 = SingleQ6{diffMap: make(map[int16]map[int8]map[int8]*float64), nUpds: 0, done: new(bool)}
	q6.remQ6 = SingleQ6{diffMap: make(map[int16]map[int8]map[int8]*float64), nUpds: 0, done: new(bool)}
	for year := int16(1993); year <= 1997; year++ {
		q6.newQ6.diffMap[year] = map[int8]map[int8]*float64{24: make(map[int8]*float64), 25: make(map[int8]*float64)}
		q6.remQ6.diffMap[year] = map[int8]map[int8]*float64{24: make(map[int8]*float64), 25: make(map[int8]*float64)}
	}
	q6.newQ6.nUpds += q6.newQ6.calcHelper(newItems, 1)
	q6.remQ6.nUpds += q6.remQ6.calcHelper(remItems, -1)
	return q6
}

func (q6 SingleLocalQ6) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	nUpdsN, nUpdsStatsN, nBlockUpdsN := q6.newQ6.countUpds()
	nUpdsR, nUpdsStatsR, nBlockUpdsR := q6.remQ6.countUpds()
	return nUpdsN + nUpdsR, nUpdsStatsN + nUpdsStatsR, getNBlockUpds(nBlockUpdsN + nBlockUpdsR)
}

func (q6 SingleLocalQ6) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q6.newQ6.makeNewUpdArgs(tables, newOrder, buf, bufI, bucketI)
}
func (q6 SingleLocalQ6) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q6.remQ6.makeRemUpdArgs(tables, remOrder, buf, bufI, bucketI)
}

//*****SingleQ7, Q7**+**

type SingleQ7 struct {
	//For each other, could be two years, multiple supply countries but only one customer country
	q7Map map[int8]map[int16]map[int8]*float64
	nUpds int
	done  *bool
}

func (q7 SingleQ7) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	q7.q7Map, q7.nUpds, q7.done = make(map[int8]map[int16]map[int8]*float64), 0, new(bool)
	q7.calcHelper(tables, newOrder, newItems, 1)
	q7.calcHelper(tables, remOrder, remItems, -1)
	return q7
}

func (q7 SingleQ7) calcHelper(tables *tpch.Tables, order *tpch.Orders, items []*tpch.LineItem, signal float64) {
	orderDate := order.O_ORDERDATE
	var suppNationKey int8
	atLeastOne := false
	if (orderDate.YEAR == 1994 && orderDate.MONTH >= 9) || (orderDate.YEAR >= 1995 && orderDate.YEAR <= 1996) {
		custNatKey := tables.OrderToNationkey(order)
		custMap, has := q7.q7Map[custNatKey] //May already have if both orders have the same customer country
		if !has {
			custMap = map[int16]map[int8]*float64{1995: make(map[int8]*float64), 1996: make(map[int8]*float64)}
			q7.q7Map[custNatKey] = custMap
		}
		for _, item := range items {
			if item.L_SHIPDATE.YEAR > 1996 || item.L_SHIPDATE.YEAR < 1995 {
				continue //This can happen when orderDate is close to the end of 1996.
			}
			atLeastOne = true
			suppNationKey = tables.SupplierkeyToNationkey(item.L_SUPPKEY)
			if suppNationKey != custNatKey {
				//fmt.Printf("[Q7][CalcHelper]Year: %d\n", item.L_SHIPDATE.YEAR)
				value, has := custMap[item.L_SHIPDATE.YEAR][suppNationKey]
				if !has {
					value = new(float64)
					custMap[item.L_SHIPDATE.YEAR][suppNationKey] = value
					q7.nUpds++
				}
				*value += (item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT) * signal)
			}
		}
		if !atLeastOne { //Delete map entry as it is empty
			delete(q7.q7Map, custNatKey)
		}
	}
	return
}

func (q7 SingleQ7) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return nUpds, nUpds, getNBlockUpds(nUpds)
}

// TODO: CRDT organization: year -> custNation -> suppNation
func (q7 SingleQ7) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	if !*q7.done && q7.nUpds > 0 {
		*q7.done = true
		nations := tables.Nations
		mapUpd95, mapUpd96 := crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}, crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
		var nat1Name string
		for nat1Key, nat1Map := range q7.q7Map {
			nat1Name = nations[nat1Key].N_NAME
			for year, yearMap := range nat1Map {
				if len(yearMap) == 0 {
					continue //This year has no updates
				}
				innerUpd := crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
				for nat2Key, value := range yearMap {
					innerUpd.Upds[nations[nat2Key].N_NAME] = crdt.Increment{Change: int32(*value)}
				}
				if year == 1995 {
					mapUpd95.Upds[nat1Name] = innerUpd
				} else {
					mapUpd96.Upds[nat1Name] = innerUpd
				}
			}
		}
		args := crdt.EmbMapUpdateAll{Upds: map[string]crdt.UpdateArguments{"1995": mapUpd95, "1996": mapUpd96}}
		buf[bufI] = crdt.UpdateObjectParams{KeyParams: crdt.KeyParams{Key: Q7_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}, UpdateArgs: args}
		bufI++
	}
	return bufI
}

func (q7 SingleQ7) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q7.makeNewUpdArgs(tables, remOrder, buf, bufI, bucketI)
}

//*****SingleLocalQ7, Q7**+**

type SingleLocalQ7 struct {
	newQ7, remQ7 SingleQ7
}

func (q7 SingleLocalQ7) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	q7.newQ7, q7.remQ7 = SingleQ7{q7Map: make(map[int8]map[int16]map[int8]*float64), nUpds: 0, done: new(bool)}, SingleQ7{q7Map: make(map[int8]map[int16]map[int8]*float64), nUpds: 0, done: new(bool)}
	q7.newQ7.calcHelper(tables, newOrder, newItems, 1)
	q7.remQ7.calcHelper(tables, remOrder, remItems, -1)
	return q7
}

func (q7 SingleLocalQ7) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	nUpdsN, nUpdsStatsN, nBlockUpdsN := q7.newQ7.countUpds()
	nUpdsR, nUpdsStatsR, nBlockUpdsR := q7.remQ7.countUpds()
	return nUpdsN + nUpdsR, nUpdsStatsN + nUpdsStatsR, getNBlockUpds(nBlockUpdsN + nBlockUpdsR)
}

func (q7 SingleLocalQ7) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q7.newQ7.makeNewUpdArgs(tables, newOrder, buf, bufI, bucketI)
}

func (q7 SingleLocalQ7) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q7.remQ7.makeRemUpdArgs(tables, remOrder, buf, bufI, bucketI)
}

// *****SingleQ8, Q8*****
// An order may be supplied by suppliers of multiple nations and regions :(
// And may have multiple types of parts too.
type SingleQ8 struct {
	nationMap, regionMap map[string]map[int8]*Q8YearPair
	nUpds                int
	done                 *bool
}

func (q8 SingleQ8) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	//fmt.Printf("[Q8][BuildUpd]NewOrderYear: %d (nItems: %d). RemOrderYear: %d (nItems: %d)\n", newOrder.O_ORDERDATE.YEAR, len(newItems), remOrder.O_ORDERDATE.YEAR, len(remItems))
	q8.nationMap, q8.regionMap, q8.nUpds, q8.done = make(map[string]map[int8]*Q8YearPair), make(map[string]map[int8]*Q8YearPair), 0, new(bool)
	q8.nUpds += q8.calcHelper(tables, newOrder, newItems, 1)
	q8.nUpds += q8.calcHelper(tables, remOrder, remItems, -1)
	return q8
}

func (q8 SingleQ8) calcHelper(tables *tpch.Tables, order *tpch.Orders, items []*tpch.LineItem, signal float64) (nUpds int) {
	var natId, regId int8
	var partType string
	var sum float64
	var has bool
	var partNMap, partRMap map[int8]*Q8YearPair
	year := order.O_ORDERDATE.YEAR
	if year == 1995 || year == 1996 {
		for _, item := range items {
			partType = tables.Parts[item.L_PARTKEY].P_TYPE
			partNMap, has = q8.nationMap[partType]
			partRMap = q8.regionMap[partType]
			if !has {
				partNMap, partRMap = make(map[int8]*Q8YearPair), make(map[int8]*Q8YearPair)
				partNMap[natId], partRMap[regId] = &Q8YearPair{sum1995: 0, sum1996: 0}, &Q8YearPair{sum1995: 0, sum1996: 0}
				q8.nationMap[partType], q8.regionMap[partType] = partNMap, partRMap
			}
			nationPair, has := partNMap[natId]
			if !has {
				nationPair = &Q8YearPair{sum1995: 0, sum1996: 0}
				partNMap[natId] = nationPair
				nUpds++
			}
			regionPair, has := partRMap[regId]
			if !has {
				regionPair = &Q8YearPair{sum1995: 0, sum1996: 0}
				partRMap[regId] = regionPair
				nUpds++
			}
			sum = item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT)
			nationPair.addSum(sum, year)
			regionPair.addSum(sum, year)
		}
	}
	return
}

func (q8 SingleQ8) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q8.nUpds, q8.nUpds, getNBlockUpds(q8.nUpds)
}

func (q8 SingleQ8) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	if !*q8.done {
		bufI, _ = makeQ8IndexUpdsHelper(tables, q8.nationMap, q8.regionMap, buf, bufI, bucketI)
		*q8.done = true
	}
	return bufI
}

func (q8 SingleQ8) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q8.makeNewUpdArgs(tables, remOrder, buf, bufI, bucketI)
}

// *****SingleLocalQ8, Q8*****
// One SingleQ8 per region
type SingleLocalQ8 struct {
	q8PerReg          []SingleQ8
	nUpds, nBlockUpds int
	done              *bool
}

func (q8 SingleLocalQ8) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	q8.q8PerReg, q8.nUpds, q8.nBlockUpds, q8.done = make([]SingleQ8, len(tables.Regions)), 0, 0, new(bool)
	newItemsReg, remItemsReg := tables.GetOrderItemsPerSupplier(newItems), tables.GetOrderItemsPerSupplier(remItems)
	for i, newItems := range newItemsReg {
		remItems := remItemsReg[i]
		if len(newItems) > 0 || len(remItems) > 0 {
			q8.q8PerReg[i] = SingleQ8{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems).(SingleQ8) //It's okay to call buildUpdInfo even if one of the items slice is nil
			q8.nBlockUpds++
			regNUpds, _, _ := q8.q8PerReg[i].countRemUpds()
			q8.nUpds += regNUpds
		}
	}
	return q8
}

func (q8 SingleLocalQ8) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q8.nUpds, q8.nUpds, q8.nBlockUpds
}

func (q8 SingleLocalQ8) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	if !*q8.done {
		for i, q8Reg := range q8.q8PerReg {
			if q8Reg.nationMap != nil { //If nil, then this region has no updates.
				bufI = q8Reg.makeNewUpdArgs(tables, newOrder, buf, bufI, INDEX_BKT+i)
			}
		}
		*q8.done = true
	}
	return bufI
}

func (q8 SingleLocalQ8) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q8.makeNewUpdArgs(tables, remOrder, buf, bufI, bucketI)
}

// *****SingleQ9, Q9**+**
// One new/rem order can be multiple colors, nations and regions... but single year per order.
// Since there is many colors per order however, better do it together.
type SingleQ9 struct {
	q9Map map[string]map[int8]map[int16]*float64
	done  *bool
	nUpds int
}

func (q9 SingleQ9) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	q9.q9Map, q9.done = make(map[string]map[int8]map[int16]*float64), new(bool)
	q9.nUpds += q9.calcHelper(tables, newOrder, newItems, 1)
	q9.nUpds += q9.calcHelper(tables, remOrder, remItems, -1)
	return q9
}

func (q9 SingleQ9) calcHelper(tables *tpch.Tables, order *tpch.Orders, items []*tpch.LineItem, signal float64) (nUpds int) {
	var natId int8
	var colors []string
	var partSupp *tpch.PartSupp
	var value float64
	var colorMap map[int8]map[int16]*float64
	var nationMap map[int16]*float64
	var has bool
	var sum *float64
	year := order.O_ORDERDATE.YEAR

	for _, item := range items {
		natId = tables.SupplierkeyToNationkey(item.L_SUPPKEY)
		colors = strings.Split(tables.Parts[item.L_PARTKEY].P_NAME, " ")
		partSupp = tables.GetPartSuppOfLineitem(item.L_PARTKEY, item.L_SUPPKEY)
		value = item.L_EXTENDEDPRICE*(1-item.L_DISCOUNT) - partSupp.PS_SUPPLYCOST*float64(item.L_QUANTITY)*signal
		for _, color := range colors {
			colorMap, has = q9.q9Map[color]
			if !has {
				colorMap = make(map[int8]map[int16]*float64)
				q9.q9Map[color] = colorMap
				nationMap = map[int16]*float64{year: new(float64)}
				*nationMap[year] += value
				colorMap[natId] = nationMap
				nUpds++
				continue
			}
			nationMap, has = colorMap[natId]
			if !has {
				nationMap = map[int16]*float64{year: new(float64)}
				*nationMap[year] += value
				colorMap[natId] = nationMap
				nUpds++
				continue
			}
			sum, has = nationMap[year]
			if !has {
				sum = new(float64)
				nationMap[year] = sum
				nUpds++
			}
			*sum += value
		}
	}
	return
}

func (q9 SingleQ9) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q9.nUpds, q9.nUpds, getNBlockUpds(q9.nUpds)
}

func (q9 SingleQ9) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	if !*q9.done {
		*q9.done = true
		bufI, _ = makeQ9IndexUpdsHelper(tables, q9.q9Map, buf, bufI, bucketI)
	}
	return bufI
}

func (q9 SingleQ9) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q9.makeNewUpdArgs(tables, remOrder, buf, bufI, bucketI)
}

// *****SingleLocalQ9, Q9*****
// One order may lead to multiple regions.
type SingleLocalQ9 struct {
	q9PerReg          []SingleQ9
	nUpds, nBlockUpds int
	done              *bool
}

func (q9 SingleLocalQ9) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	q9.q9PerReg, q9.nUpds, q9.nBlockUpds, q9.done = make([]SingleQ9, len(tables.Regions)), 0, 0, new(bool)
	newItemsReg, remItemsReg := tables.GetOrderItemsPerSupplier(newItems), tables.GetOrderItemsPerSupplier(remItems)
	for i, newItems := range newItemsReg {
		remItems := remItemsReg[i]
		if len(newItems) > 0 || len(remItems) > 0 {
			q9.q9PerReg[i] = SingleQ9{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems).(SingleQ9) //It's okay to call buildUpdInfo even if one of the items slice is nil
			q9.nBlockUpds++
			regNUpds, _, _ := q9.q9PerReg[i].countUpds()
			q9.nUpds += regNUpds
		}
	}
	return q9
}

func (q9 SingleLocalQ9) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q9.nUpds, q9.nUpds, q9.nBlockUpds
}

func (q9 SingleLocalQ9) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	if !*q9.done {
		for i, q9Reg := range q9.q9PerReg {
			if q9Reg.q9Map != nil { //If it's nil, then it means that region has no updates
				bufI = q9Reg.makeNewUpdArgs(tables, newOrder, buf, bufI, INDEX_BKT+i)
			}
		}
		*q9.done = true
	}
	return bufI
}

func (q9 SingleLocalQ9) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q9.makeNewUpdArgs(tables, remOrder, buf, bufI, bucketI)
}

// *****SingleQ10, Q10**+**
// One customer on add, one on remove. So an add or remove are fully local. Only 1 quarter per customer too
// Thus, no local version is needed.
// (In theory, both customers can be for the same quarter... 12.5% chance.
// However it is just better to still issue 2 TopKAdd in that situation instead of 87.5% of the time 2 TopKAddAll and 12.5% 1 TopKAddAll)
type SingleQ10 struct {
	addValue, remValue float64
}

func (q10 SingleQ10) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	q10.addValue, q10.remValue = q10.calcHelper(tables, newOrder, newItems, 1), q10.calcHelper(tables, remOrder, remItems, -1)
	return q10
}

func (q10 SingleQ10) calcHelper(tables *tpch.Tables, order *tpch.Orders, items []*tpch.LineItem, signal float64) (value float64) {
	if order.O_ORDERDATE.YEAR == 1993 || order.O_ORDERDATE.YEAR == 1994 {
		for _, item := range items {
			if item.L_RETURNFLAG == "R" {
				value += item.L_EXTENDEDPRICE * (1 - L_DISCOUNT) * signal
			}
		}
	}
	return
}

func (q10 SingleQ10) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	if q10.addValue != 0 {
		nUpds++
	}
	if q10.remValue != 0 {
		nUpds++
	}
	return nUpds, nUpds, getNBlockUpds(nUpds)
}

func (q10 SingleQ10) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q10.makeUpdArgsHelper(tables, newOrder, buf, bufI, bucketI, q10.addValue)
}

func (q10 SingleQ10) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q10.makeUpdArgsHelper(tables, remOrder, buf, bufI, bucketI, q10.remValue)
}

func (q10 SingleQ10) makeUpdArgsHelper(tables *tpch.Tables, order *tpch.Orders, buf []crdt.UpdateObjectParams, bufI, bucketI int, value float64) (newBufI int) {
	if value == 0 {
		return bufI
	}
	var topUpd crdt.UpdateArguments
	if INDEX_WITH_FULL_DATA {
		cust := tables.Customers[order.O_CUSTKEY]
		topUpd = crdt.TopSAdd{TopKScore: crdt.TopKScore{Id: order.O_CUSTKEY, Score: int32(q10.remValue), Data: packQ10IndexExtraData(cust, tables.Nations[cust.C_NATIONKEY].N_NAME)}}
	} else {
		topUpd = crdt.TopSAdd{TopKScore: crdt.TopKScore{Id: order.O_CUSTKEY, Score: int32(q10.remValue)}}
	}
	buf[bufI] = crdt.UpdateObjectParams{KeyParams: crdt.KeyParams{Key: Q10_KEY + strconv.FormatInt(int64(O_CUSTKEY), 10), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}, UpdateArgs: topUpd}
	return bufI + 1
}

// *****SingleQ12, Q12**+**
// Even a single order may have 2 years. Each order only has 1 priority but can have multiple shipmodes.
// Better still make the map
type SingleQ12 struct {
	q12Map map[int16]map[string]map[string]*int32
	done   *bool
	nUpds  int
}

func (q12 SingleQ12) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	q12.q12Map, q12.nUpds, q12.done = make(map[int16]map[string]map[string]*int32), 0, new(bool)
	q12.nUpds += q12.calcHelper(tables, newOrder, newItems, 1)
	q12.nUpds += q12.calcHelper(tables, remOrder, remItems, -1)
	return q12
}

func (q12 SingleQ12) calcHelper(tables *tpch.Tables, order *tpch.Orders, items []*tpch.LineItem, signal int32) (nUpds int) {
	if (order.O_ORDERDATE.YEAR >= 1993 || (order.O_ORDERDATE.YEAR == 1992 && order.O_ORDERDATE.MONTH >= 9)) && order.O_ORDERDATE.YEAR <= 1997 {
		var yearMap map[string]map[string]*int32
		var shipModeMap map[string]*int32
		var count *int32
		var priority string
		var has bool
		if order.O_ORDERPRIORITY[0] == '1' || order.O_ORDERPRIORITY[1] == '2' { //Priority
			priority = Q12_PRIORITY[0]
		} else { //Non-priority
			priority = Q12_PRIORITY[1]
		}
		for _, item := range items {
			yearMap, has = q12.q12Map[item.L_RECEIPTDATE.YEAR]
			if !has {
				yearMap = make(map[string]map[string]*int32)
				q12.q12Map[item.L_RECEIPTDATE.YEAR] = yearMap
			}
			shipModeMap, has = yearMap[item.L_SHIPMODE]
			if !has {
				count = new(int32)
				shipModeMap = map[string]*int32{priority: count}
				yearMap[item.L_SHIPMODE] = shipModeMap
				nUpds++
			} else {
				count, has = shipModeMap[priority]
				if !has { //Doesn't have yet this priority
					count = new(int32)
					shipModeMap[priority] = count
				} else {
					count = shipModeMap[priority]
				}
			}
			*count += signal
		}
	}
	return
}

func (q12 SingleQ12) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q12.nUpds, q12.nUpds, getNBlockUpds(q12.nUpds)
}

func (q12 SingleQ12) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	if !*q12.done {
		*q12.done = true
		var keyParams crdt.KeyParams
		var mapUpd crdt.EmbMapUpdateAll
		for year, yearMap := range q12.q12Map {
			keyParams = crdt.KeyParams{Key: Q12_KEY + strconv.FormatInt(int64(year), 10), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}
			mapUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
			for shipPriority, shipMap := range yearMap {
				for q12Prio, value := range shipMap {
					mapUpd.Upds[shipPriority+q12Prio] = crdt.Increment{Change: *value}
				}
			}
			buf[bufI] = crdt.UpdateObjectParams{KeyParams: keyParams, UpdateArgs: mapUpd}
			bufI++
		}
	}
	return bufI
}

func (q12 SingleQ12) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q12.makeNewUpdArgs(tables, remOrder, buf, bufI, bucketI)
}

//*****SingleLocalQ12, Q12*****

type SingleLocalQ12 struct {
	addQ12, remQ12 SingleQ12
	nUpds          int
}

func (q12 SingleLocalQ12) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	q12.addQ12, q12.remQ12 = SingleQ12{q12Map: make(map[int16]map[string]map[string]*int32), nUpds: 0, done: new(bool)}, SingleQ12{q12Map: make(map[int16]map[string]map[string]*int32), nUpds: 0, done: new(bool)}
	q12.nUpds += q12.addQ12.calcHelper(tables, newOrder, newItems, 1)
	q12.nUpds += q12.remQ12.calcHelper(tables, remOrder, remItems, -1)
	return q12
}

func (q12 SingleLocalQ12) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q12.nUpds, q12.nUpds, getNBlockUpds(q12.nUpds)
}

func (q12 SingleLocalQ12) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q12.addQ12.makeNewUpdArgs(tables, newOrder, buf, bufI, bucketI)
}

func (q12 SingleLocalQ12) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q12.remQ12.makeRemUpdArgs(tables, remOrder, buf, bufI, bucketI)
}

//*****SingleQ13, Q13*****

type SingleQ13 struct {
	newCustWordsOldOrders, remCustWordsOldOrders map[string]int8 //Stores the old value of nOrders of the customer
	done                                         *bool
}

func (q13 SingleQ13) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	q13.newCustWordsOldOrders = q13.calcHelper(tables, newOrder, newItems, 1)
	q13.remCustWordsOldOrders = q13.calcHelper(tables, remOrder, remItems, -1)
	q13.done = new(bool)
	return q13
}

func (q13 SingleQ13) calcHelper(tables *tpch.Tables, order *tpch.Orders, items []*tpch.LineItem, signal int8) (newCustWordsOldOrders map[string]int8) {
	custId := order.O_CUSTKEY
	newCustWordsOldOrders = make(map[string]int8)
	words, has := q13FindWordsInComment(order.O_COMMENT)
	if has {
		for currWords, custMap := range q13Info.wordsToCustNOrders {
			if words != currWords {
				newCustWordsOldOrders[currWords] = custMap[custId]
				custMap[custId] += signal
			}
		}
	} else { //Most common case. More efficient because no need to check for the word in every step
		for currWords, custMap := range q13Info.wordsToCustNOrders {
			newCustWordsOldOrders[currWords] = custMap[custId]
			custMap[custId] += signal
		}
	}
	return
}

func (q13 SingleQ13) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return len(q13.newCustWordsOldOrders) + len(q13.remCustWordsOldOrders), len(q13.newCustWordsOldOrders) + len(q13.remCustWordsOldOrders),
		getNBlockUpds(len(q13.newCustWordsOldOrders) + len(q13.remCustWordsOldOrders))
}

// Change: +1 for a new order, -1 for a removal.
func (q13 SingleQ13) partialUpdsHelper(tables *tpch.Tables, buf []crdt.UpdateObjectParams, bufI, bucketI int, change int8, wordsOrders map[string]int8) (newBufI int) {
	outerMapUpd := crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)} //Words -> Map
	var innerMapUpd crdt.EmbMapUpdateAll                                             //nOrders -> Counter
	for word, oldValue := range wordsOrders {
		oldKey, newKey := strconv.FormatInt(int64(oldValue), 10), strconv.FormatInt(int64(oldValue+change), 10)
		innerMapUpd = crdt.EmbMapUpdateAll{Upds: map[string]crdt.UpdateArguments{oldKey: crdt.Increment{Change: -1}, newKey: crdt.Increment{Change: 1}}}
		outerMapUpd.Upds[word] = innerMapUpd
	}
	buf[bufI] = crdt.UpdateObjectParams{KeyParams: crdt.KeyParams{Key: Q13_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}, UpdateArgs: outerMapUpd}
	bufI++
	return bufI
}

func (q13 SingleQ13) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI, bucketI int) (newBufI int) {
	if !*q13.done { //There's only 1 CRDT so it's better to do all updates at once
		*q13.done = true
		if q13.newCustWordsOldOrders == nil {
			return q13.partialUpdsHelper(tables, buf, bufI, bucketI, -1, q13.remCustWordsOldOrders)
		}
		if q13.remCustWordsOldOrders == nil {
			return q13.partialUpdsHelper(tables, buf, bufI, bucketI, 1, q13.newCustWordsOldOrders)
		}
		outerMapUpd := crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)} //Words -> Map
		var innerMapUpd crdt.EmbMapUpdateAll                                             //nOrders -> Counter
		//Go through all combinations of words. At most each map is missing one word entry
		for _, word := range Q13_BOTH_WORDS { //word: Word1 + Word2
			newValue, hasNew := q13.newCustWordsOldOrders[word]
			remValue, hasRem := q13.remCustWordsOldOrders[word]
			if !hasNew && !hasRem {
				continue //Very rare case, both the new and removed order have this "word" in their comment.
			}
			innerMapUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
			if !hasRem { //No optimization possible
				innerMapUpd.Upds[strconv.FormatInt(int64(newValue), 10)] = crdt.Increment{Change: -1}
				innerMapUpd.Upds[strconv.FormatInt(int64(newValue+1), 10)] = crdt.Increment{Change: 1}
			} else if !hasNew { //No optimization possible
				innerMapUpd.Upds[strconv.FormatInt(int64(remValue), 10)] = crdt.Increment{Change: -1}
				innerMapUpd.Upds[strconv.FormatInt(int64(remValue-1), 10)] = crdt.Increment{Change: 1}
			} else { ///hasRem && hasNew are true
				if newValue == remValue { //Can merge together one of the updates
					innerMapUpd.Upds[strconv.FormatInt(int64(newValue), 10)] = crdt.Increment{Change: -2}
					innerMapUpd.Upds[strconv.FormatInt(int64(newValue+1), 10)] = crdt.Increment{Change: 1}
					innerMapUpd.Upds[strconv.FormatInt(int64(newValue-1), 10)] = crdt.Increment{Change: 1}
				} else if newValue == remValue-1 { //No update is needed. It's like if the customers "changed places"

				} else if newValue == remValue-2 { //Can merge together the update of newValue+1, remValue-1
					innerMapUpd.Upds[strconv.FormatInt(int64(newValue), 10)] = crdt.Increment{Change: -1}
					innerMapUpd.Upds[strconv.FormatInt(int64(remValue), 10)] = crdt.Increment{Change: -1}
					innerMapUpd.Upds[strconv.FormatInt(int64(newValue+1), 10)] = crdt.Increment{Change: 2} //Same as remValue-1
				} else { //No optimizatiion possible
					innerMapUpd.Upds[strconv.FormatInt(int64(newValue), 10)] = crdt.Increment{Change: -1}
					innerMapUpd.Upds[strconv.FormatInt(int64(newValue+1), 10)] = crdt.Increment{Change: 1}
					innerMapUpd.Upds[strconv.FormatInt(int64(remValue), 10)] = crdt.Increment{Change: -1}
					innerMapUpd.Upds[strconv.FormatInt(int64(remValue-1), 10)] = crdt.Increment{Change: 1}
				}
			}
			outerMapUpd.Upds[word] = innerMapUpd
		}
		buf[bufI] = crdt.UpdateObjectParams{KeyParams: crdt.KeyParams{Key: Q13_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}, UpdateArgs: outerMapUpd}
		bufI++
	}
	return bufI
}

func (q13 SingleQ13) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q13.makeNewUpdArgs(tables, remOrder, buf, bufI, bucketI)
}

//*****SingleLocalQ13, Q13*****

type SingleLocalQ13 struct {
	SingleQ13
	sameNation bool //Different processing depending if both new and rem order belong to customers of same nation or not.
}

func (q13 SingleLocalQ13) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	//Think well. If the same customer region, we can just use SingleQ13.
	//Otherwise, using two SingleQ13 would be an option but quite inneficient when calculating updates
	//Due to all the checks on exists. Would be advisable to make a custom solution for local.
	q13.newCustWordsOldOrders = q13.SingleQ13.calcHelper(tables, newOrder, newItems, 1)
	q13.remCustWordsOldOrders = q13.SingleQ13.calcHelper(tables, remOrder, remItems, -1)
	if tables.OrderToRegionkey(newOrder) == tables.OrderToRegionkey(remOrder) {
		q13.sameNation = true
	} else {
		q13.sameNation = false
	}
	q13.done = new(bool)
	return q13
}

func (q13 SingleLocalQ13) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q13.SingleQ13.countUpds()
}

func (q13 SingleLocalQ13) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	if !*q13.done {
		if q13.sameNation {
			bufI = q13.SingleQ13.makeNewUpdArgs(tables, newOrder, buf, bufI, bucketI)
		} else { //No optimizations or special cases.
			bufI = q13.partialUpdsHelper(tables, buf, bufI, bucketI, 1, q13.newCustWordsOldOrders)
		}
	}
	return bufI
}

func (q13 SingleLocalQ13) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	if !*q13.done {
		if q13.sameNation {
			bufI = q13.SingleQ13.makeRemUpdArgs(tables, remOrder, buf, bufI, bucketI)
		} else { //No optimizations or special cases.
			bufI = q13.partialUpdsHelper(tables, buf, bufI, bucketI, -1, q13.remCustWordsOldOrders)
		}
	}
	return bufI
}

//*****SingleQ13, Q13*****
/*
type SingleQ13 struct {
	newCustOldOrders, remCustOldOrders           int8
	newWords, remWords                           string //If nil, it means it does not have any of the words
	newCustWordsOldOrders, remCustWordsOldOrders int8
	done                                         bool
}

func (q13 SingleQ13) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	q13.newCustOldOrders, q13.newCustWordsOldOrders, q13.newWords = q13.calcHelper(tables, newOrder, newItems)
	q13.remCustOldOrders, q13.remCustWordsOldOrders, q13.remWords = q13.calcHelper(tables, remOrder, remItems)
	q13.done = false
	return q13
}

func (q13 SingleQ13) calcHelper(tables *tpch.Tables, order *tpch.Orders, items []*tpch.LineItem) (oldNOrders, oldWordsNOrders int8, words string) {
	custId := order.O_CUSTKEY
	oldNOrders = q13Info.custToNOrders[custId]
	q13Info.custToNOrders[custId]++
	var has bool
	words, has = q13FindWordsInComment(order.O_COMMENT)
	if has {
		oldWordsNOrders = q13Info.wordsToCustNOrders[words][custId]
		q13Info.wordsToCustNOrders[words][custId]++
		return
	}
	return oldNOrders, -1, ""
}

func (q13 SingleQ13) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	nUpds = 2 //newCustId, remCustId
	if q13.newWords != "" {
		nUpds++
	}
	if q13.remWords != "" {
		nUpds++
	}
	return nUpds, nUpds, getNBlockUpds(nUpds)
}

func (q13 SingleQ13) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	if !q13.done {
		q13.done = true
		allOrderMapUpd := crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
		if q13.newCustOldOrders == q13.remCustOldOrders {
			allOrderMapUpd.Upds[strconv.FormatInt(int64(q13.newCustOldOrders), 10)] = crdt.Increment{Change: -2}
			allOrderMapUpd.Upds[strconv.FormatInt(int64(q13.newCustOldOrders+1), 10)] = crdt.Increment{Change: 1}
			allOrderMapUpd.Upds[strconv.FormatInt(int64(q13.newCustOldOrders-1), 10)] = crdt.Increment{Change: 1}
		} else if q13.newCustOldOrders == q13.remCustOldOrders-1 {
			//Nothing
		} else {
			allOrderMapUpd.Upds[strconv.FormatInt(int64(q13.newCustOldOrders), 10)] = crdt.Increment{Change: -1}
			allOrderMapUpd.Upds[strconv.FormatInt(int64(q13.newCustOldOrders+1), 10)] = crdt.Increment{Change: 1}
			allOrderMapUpd.Upds[strconv.FormatInt(int64(q13.remCustOldOrders), 10)] = crdt.Increment{Change: -1}
			allOrderMapUpd.Upds[strconv.FormatInt(int64(q13.remCustOldOrders-1), 10)] = crdt.Increment{Change: 1}
		}
		if len(allOrderMapUpd.Upds) > 0 {
			var allUpd crdt.UpdateArguments = allOrderMapUpd
			buf[bufI] = crdt.UpdateObjectParams{KeyParams: crdt.KeyParams{Key: Q13_KEY, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}, UpdateArgs: &allUpd}
			bufI++
		}
		//No need to optimize for putting both newWords and remWords in same update:
		//It's already uncommon for an order to match any word, and then they'd both have to match the same word (1/16 chance)
		//Furthermore, if they have different amounts, the optimization is not possible either.
		if q13.newCustWordsOldOrders > -1 {
			var upd crdt.UpdateArguments = crdt.EmbMapUpdateAll{Upds: map[string]crdt.UpdateArguments{strconv.FormatInt(int64(q13.newCustWordsOldOrders), 10): crdt.Increment{Change: -1}, strconv.FormatInt(int64(q13.newCustWordsOldOrders+1), 10): crdt.Increment{Change: 1}}}
			buf[bufI] = crdt.UpdateObjectParams{KeyParams: crdt.KeyParams{Key: Q13_KEY + q13.newWords, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}, UpdateArgs: &upd}
			bufI++
		}
		if q13.remCustWordsOldOrders > -1 {
			var upd crdt.UpdateArguments = crdt.EmbMapUpdateAll{Upds: map[string]crdt.UpdateArguments{strconv.FormatInt(int64(q13.remCustWordsOldOrders), 10): crdt.Increment{Change: -1}, strconv.FormatInt(int64(q13.remCustWordsOldOrders-1), 10): crdt.Increment{Change: 1}}}
			buf[bufI] = crdt.UpdateObjectParams{KeyParams: crdt.KeyParams{Key: Q13_KEY + q13.remWords, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}, UpdateArgs: &upd}
			bufI++
		}
	}
	return bufI
}

func (q13 SingleQ13) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q13.makeNewUpdArgs(tables, remOrder, buf, bufI, bucketI)
}

// *****SingleLocalQ13, Q13*****
type SingleLocalQ13 struct {
	newQ13, remQ13 SingleQ13
}

func (q13 SingleLocalQ13) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	q13.newQ13.newCustOldOrders, q13.newQ13.newCustWordsOldOrders, q13.newQ13.newWords = q13.newQ13.calcHelper(tables, newOrder, newItems)
	q13.remQ13.remCustOldOrders, q13.remQ13.remCustWordsOldOrders, q13.remQ13.remWords = q13.remQ13.calcHelper(tables, remOrder, remItems)
	q13.newQ13.done, q13.remQ13.done = false, false
	return q13
}

func (q13 SingleLocalQ13) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	nUpdsN, nUpdsStatsN, nBlockUpdsN := q13.newQ13.countUpds()
	nUpdsR, nUpdsStatsR, nBlockUpdsR := q13.remQ13.countUpds()
	return nUpdsN + nUpdsR, nUpdsStatsN + nUpdsStatsR, getNBlockUpds(nBlockUpdsN + nBlockUpdsR)
}

func (q13 SingleLocalQ13) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q13.newQ13.makeNewUpdArgs(tables, newOrder, buf, bufI, bucketI)
}

func (q13 SingleLocalQ13) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q13.remQ13.makeRemUpdArgs(tables, remOrder, buf, bufI, bucketI)
}
*/

//*****SingleQ17, Q17**+**

type SingleQ17 struct {
	info  Q17Info //A new/rem order has multiple lineitems -> multiple brand+containers
	nUpds int
	done  *bool
}

func (q17 SingleQ17) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	q17.info, q17.done = Q17Info{sum: make(map[string]int64), count: make(map[string]int64), pricePerQuantity: make(map[string]map[int8]float64)}, new(bool)
	q17.nUpds += q17.calcHelper(tables, q17.info, newOrder, newItems, 1)
	q17.nUpds += q17.calcHelper(tables, q17.info, remOrder, remItems, -1)
	return q17
}

func (q17 SingleQ17) calcHelper(tables *tpch.Tables, info Q17Info, newOrder *tpch.Orders, newItems []*tpch.LineItem, signal int64) (nUpds int) {
	var bc string
	var part *tpch.Part
	var has bool
	var innerInfo map[int8]float64
	for _, item := range newItems {
		part = tables.Parts[item.L_PARTKEY]
		bc = part.P_BRAND + part.P_CONTAINER
		innerInfo, has = info.pricePerQuantity[bc]
		if !has {
			innerInfo = make(map[int8]float64)
			info.pricePerQuantity[bc] = innerInfo
			nUpds++
		}
		info.sum[bc] += int64(item.L_QUANTITY) * signal
		info.count[bc] += signal
		info.pricePerQuantity[bc][item.L_QUANTITY] += (item.L_EXTENDEDPRICE * float64(signal))
	}
	return
}
func (q17 SingleQ17) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q17.nUpds, q17.nUpds, getNBlockUpds(q17.nUpds)
}

func (q17 SingleQ17) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	if !*q17.done {
		*q17.done = true
		return makeQ17IndexUpdsHelper(q17.info, buf, bufI, bucketI)
	}
	return bufI
}

func (q17 SingleQ17) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q17.makeNewUpdArgs(tables, remOrder, buf, bufI, bucketI)
}

type SingleLocalQ17 struct {
	addQ17, remQ17 SingleQ17
}

func (q17 SingleLocalQ17) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	q17.addQ17 = SingleQ17{info: Q17Info{sum: make(map[string]int64), count: make(map[string]int64), pricePerQuantity: make(map[string]map[int8]float64)}, done: new(bool)}
	q17.remQ17 = SingleQ17{info: Q17Info{sum: make(map[string]int64), count: make(map[string]int64), pricePerQuantity: make(map[string]map[int8]float64)}, done: new(bool)}
	q17.addQ17.nUpds += q17.addQ17.calcHelper(tables, q17.addQ17.info, newOrder, newItems, 1)
	q17.remQ17.nUpds += q17.remQ17.calcHelper(tables, q17.remQ17.info, remOrder, remItems, -1)
	return q17
}

func (q17 SingleLocalQ17) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q17.addQ17.nUpds + q17.remQ17.nUpds, q17.addQ17.nUpds + q17.remQ17.nUpds, getNBlockUpds(q17.addQ17.nUpds + q17.remQ17.nUpds)
}

func (q17 SingleLocalQ17) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q17.addQ17.makeNewUpdArgs(tables, newOrder, buf, bufI, bucketI)
}

func (q17 SingleLocalQ17) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q17.remQ17.makeRemUpdArgs(tables, remOrder, buf, bufI, bucketI)
}

/*
type SingleQ17 struct {
	addInfo, remInfo Q17Info //A new/rem order has multiple lineitems -> multiple brand+containers
	nAdds, nRems     int
}

func (q17 SingleQ17) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	q17.addInfo = Q17Info{sum: make(map[string]int64), count: make(map[string]int64), topInfo: make(map[string]map[int32]PriceQuantityPair)}
	q17.remInfo = Q17Info{sum: make(map[string]int64), count: make(map[string]int64), topInfo: make(map[string]map[int32]PriceQuantityPair)}
	q17.nAdds, q17.nRems = q17.calcHelper(tables, q17.addInfo, newOrder, newItems, 1), q17.calcHelper(tables, q17.remInfo, remOrder, remItems, -1)
	return q17
}

func (q17 SingleQ17) calcHelper(tables *tpch.Tables, info Q17Info, newOrder *tpch.Orders, newItems []*tpch.LineItem, signal int64) (nUpds int) {
	var bc string
	var part *tpch.Part
	var has bool
	var innerInfo map[int32]PriceQuantityPair
	for _, item := range newItems {
		part = tables.Parts[item.L_PARTKEY]
		bc = part.P_BRAND + part.P_CONTAINER
		innerInfo, has = info.topInfo[bc]
		if !has {
			innerInfo = make(map[int32]PriceQuantityPair)
			info.topInfo[bc] = innerInfo
		}
		info.sum[bc] += int64(item.L_QUANTITY) * signal
		info.count[bc] += signal
		info.topInfo[bc][item.L_ORDERKEY+(int32(TableEntries[tpch.ORDERS])*int32(item.L_LINENUMBER))] = PriceQuantityPair{price: item.L_EXTENDEDPRICE, quantity: item.L_QUANTITY}
		nUpds++
	}
	return
}

func (q17 SingleQ17) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q17.nAdds + q17.nRems, q17.nAdds + q17.nRems, getNBlockUpds(q17.nAdds + q17.nRems)
}

func (q17 SingleQ17) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return makeQ17IndexUpdsHelper(q17.addInfo, buf, bufI, bucketI)
}

func (q17 SingleQ17) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	var keyParams crdt.KeyParams
	var topKAllUpd crdt.TopKRemoveAll
	var avgUpd crdt.AddMultipleValue
	var sum, count int64
	j := 0
	for bc, info := range q17.remInfo.topInfo {
		sum, count = q17.remInfo.sum[bc], q17.remInfo.count[bc]
		keyParams = crdt.KeyParams{Key: Q17_KEY + bc, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}
		avgUpd = crdt.AddMultipleValue{SumValue: sum, NAdds: count}
		if useTopKAll {
			topKAllUpd = crdt.TopKRemoveAll{Ids: make([]int32, len(info))}
			j = 0
			for id := range info {
				topKAllUpd.Ids[j] = id
				j++
			}
			var currUpd crdt.UpdateArguments = crdt.EmbMapUpdateAll{Upds: map[string]crdt.UpdateArguments{Q17_AVG: avgUpd, Q17_TOP: topKAllUpd}}
			buf[bufI] = crdt.UpdateObjectParams{KeyParams: keyParams, UpdateArgs: &currUpd}
			bufI++
		} else {
			var currUpd crdt.UpdateArguments = crdt.EmbMapUpdate{Key: Q17_AVG, Upd: avgUpd}
			buf[bufI] = crdt.UpdateObjectParams{KeyParams: keyParams, UpdateArgs: &currUpd}
			bufI++
			for id := range info {
				var currUpd crdt.UpdateArguments = crdt.EmbMapUpdate{Key: Q17_TOP, Upd: crdt.TopKRemove{Id: id}}
				buf[bufI] = crdt.UpdateObjectParams{KeyParams: keyParams, UpdateArgs: &currUpd}
				bufI++
			}
		}
	}
	return bufI
}
*/

//*****SingleQ19, Q19**+**

// Each order has multiple lineitems... so can be multiple products
type SingleQ19 struct {
	q19Map map[string]map[string]map[int8]float64
	done   *bool
	nUpds  int
}

func (q19 SingleQ19) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	q19.q19Map, q19.nUpds, q19.done = make(map[string]map[string]map[int8]float64), 0, new(bool)
	q19.nUpds += q19.calcHelper(tables, newItems, 1)
	q19.nUpds += q19.calcHelper(tables, remItems, -1)
	return q19
}

func (q19 SingleQ19) calcHelper(tables *tpch.Tables, items []*tpch.LineItem, signal float64) (nUpds int) {
	var itemEligible bool
	var containerType, brand string
	for _, item := range items {
		itemEligible, containerType, brand = isLineItemEligibleForQ19(tables, item)
		if itemEligible {
			brandMap, has := q19.q19Map[brand]
			if !has {
				brandMap = map[string]map[int8]float64{containerType: {item.L_QUANTITY: item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT) * signal}}
				q19.q19Map[brand] = brandMap
				nUpds++
			} else {
				containerMap, has := brandMap[containerType]
				if !has {
					containerMap = map[int8]float64{item.L_QUANTITY: item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT) * signal}
					brandMap[containerType] = containerMap
					nUpds++
				} else {
					if containerMap[item.L_QUANTITY] == 0 {
						nUpds++
					}
					containerMap[item.L_QUANTITY] += item.L_EXTENDEDPRICE * (1 - item.L_DISCOUNT) * signal
				}
			}
		}
	}
	return
}

func (q19 SingleQ19) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q19.nUpds, q19.nUpds, getNBlockUpds(q19.nUpds)
}

func (q19 SingleQ19) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	if !*q19.done {
		*q19.done = true
		newBufI, _ = makeQ19IndexUpdsHelper(q19.q19Map, buf, bufI, bucketI)
		return newBufI
	}
	return bufI
}

func (q19 SingleQ19) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q19.makeNewUpdArgs(tables, remOrder, buf, bufI, bucketI)
}

// *****SingleLocalQ19, Q19*****
type SingleLocalQ19 struct {
	addQ19, remQ19 SingleQ19
	nUpds          int
}

func (q19 SingleLocalQ19) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	q19.addQ19, q19.remQ19, q19.nUpds = SingleQ19{q19Map: make(map[string]map[string]map[int8]float64), nUpds: 0, done: new(bool)}, SingleQ19{q19Map: make(map[string]map[string]map[int8]float64), nUpds: 0, done: new(bool)}, 0
	q19.nUpds += q19.addQ19.calcHelper(tables, newItems, 1)
	q19.nUpds += q19.remQ19.calcHelper(tables, remItems, -1)
	return q19
}

func (q19 SingleLocalQ19) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q19.nUpds, q19.nUpds, getNBlockUpds(q19.nUpds)
}

func (q19 SingleLocalQ19) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q19.addQ19.makeNewUpdArgs(tables, newOrder, buf, bufI, bucketI)
}

func (q19 SingleLocalQ19) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q19.remQ19.makeRemUpdArgs(tables, remOrder, buf, bufI, bucketI)
}

// *****SingleQ20, Q20**+**
// May need to update multiple nations/regions since it is based on supplier's nation
type SingleQ20 struct {
	q20Map map[int8]map[string]map[PairInt]map[int16]int32
	nUpds  int
	done   *bool
}

func (q20 SingleQ20) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	q20.q20Map, q20.nUpds, q20.done = make(map[int8]map[string]map[PairInt]map[int16]int32, len(tpchData.Tables.Nations)), 0, new(bool)
	q20.nUpds += q20.calcHelper(newItems, 1)
	q20.nUpds += q20.calcHelper(remItems, -1)
	return q20
}

func (q20 SingleQ20) calcHelper(items []*tpch.LineItem, signal int32) (nUpds int) {
	var part *tpch.Part
	var spaceIndex int
	var nationKey int8
	var color string
	var has bool
	var natMap map[string]map[PairInt]map[int16]int32
	var colorMap map[PairInt]map[int16]int32
	var partSupMap map[int16]int32
	for _, item := range items {
		if item.L_SHIPDATE.YEAR >= 1993 && item.L_SHIPDATE.YEAR <= 1997 {
			part = tpchData.Tables.Parts[item.L_PARTKEY]
			spaceIndex, nationKey = strings.Index(part.P_NAME, " "), tpchData.Tables.SupplierkeyToNationkey(item.L_SUPPKEY)
			color = part.P_NAME[:spaceIndex]
			natMap, has = q20.q20Map[nationKey]
			if !has {
				natMap, colorMap, partSupMap = make(map[string]map[PairInt]map[int16]int32), make(map[PairInt]map[int16]int32), make(map[int16]int32)
				partSupMap[item.L_SHIPDATE.YEAR] = int32(item.L_QUANTITY) * signal
				colorMap[PairInt{first: item.L_PARTKEY, second: item.L_SUPPKEY}] = partSupMap
				natMap[color] = colorMap
				q20.q20Map[nationKey] = natMap
				nUpds++
				continue
			}
			colorMap, has = natMap[color]
			if !has {
				colorMap, partSupMap = make(map[PairInt]map[int16]int32), make(map[int16]int32)
				partSupMap[item.L_SHIPDATE.YEAR] = int32(item.L_QUANTITY) * signal
				colorMap[PairInt{first: item.L_PARTKEY, second: item.L_SUPPKEY}] = partSupMap
				natMap[color] = colorMap
				nUpds++
				continue
			}
			pair := PairInt{first: item.L_PARTKEY, second: item.L_SUPPKEY}
			partSupMap, has = colorMap[pair]
			if !has {
				partSupMap = make(map[int16]int32)
				partSupMap[item.L_SHIPDATE.YEAR] = int32(item.L_QUANTITY) * signal
				colorMap[pair] = partSupMap
				nUpds++
				continue
			}
			partSupMap[item.L_SHIPDATE.YEAR] += int32(item.L_QUANTITY) * signal
		}
	}
	return
}

func (q20 SingleQ20) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q20.nUpds, q20.nUpds, getNBlockUpds(q20.nUpds)
}

func (q20 SingleQ20) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	//Note: We only need to update the counter of quantity. Availqty never changes, nor does PartSupp
	if !*q20.done {
		var natMapUpd, colorMapUpd, pairMapUpd crdt.EmbMapUpdateAll
		for natId, natMap := range q20.q20Map {
			natMapUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
			for color, colorMap := range natMap {
				colorMapUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
				for pairInfo, yearMap := range colorMap {
					pairMapUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
					for year, count := range yearMap {
						pairMapUpd.Upds[strconv.FormatInt(int64(year), 10)] = crdt.Increment{Change: count}
					}
					colorMapUpd.Upds[strconv.FormatInt(int64(pairInfo.first), 10)+"_"+strconv.FormatInt(int64(pairInfo.second), 10)] = pairMapUpd
				}
				natMapUpd.Upds[color] = pairMapUpd
			}
			buf[bufI] = crdt.UpdateObjectParams{KeyParams: crdt.KeyParams{Key: Q20_KEY + strconv.FormatInt(int64(natId), 10), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}, UpdateArgs: natMapUpd}
			bufI++
		}
		*q20.done = true
	}
	return bufI
}

func (q20 SingleQ20) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q20.makeNewUpdArgs(tables, remOrder, buf, bufI, bucketI)
}

/*
// *****SingleQ20, Q20**+**
// May need to update multiple nations/regions since it is based on supplier's nation
type SingleQ20 struct {
	q20Map map[int8]map[string]map[int16]map[PairInt]int32
	nUpds  int
	done   *bool
}

func (q20 SingleQ20) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	q20.q20Map, q20.nUpds, q20.done = make(map[int8]map[string]map[int16]map[PairInt]int32, len(procTables.Nations)), 0, new(bool)
	q20.nUpds += q20.calcHelper(newItems, 1)
	q20.nUpds += q20.calcHelper(remItems, -1)
	return q20
}

func (q20 SingleQ20) calcHelper(items []*tpch.LineItem, signal int32) (nUpds int) {
	var part *tpch.Part
	var spaceIndex int
	var nationKey int8
	var color string
	var has bool
	var natMap map[string]map[int16]map[PairInt]int32
	var yearMap map[int16]map[PairInt]int32
	var partSupMap map[PairInt]int32
	for _, item := range items {
		if item.L_SHIPDATE.YEAR >= 1993 && item.L_SHIPDATE.YEAR <= 1997 {
			part = procTables.Parts[item.L_PARTKEY]
			spaceIndex, nationKey = strings.Index(part.P_NAME, " "), procTables.SupplierkeyToNationkey(item.L_SUPPKEY)
			color = part.P_NAME[:spaceIndex]
			natMap, has = q20.q20Map[nationKey]
			if !has {
				natMap, yearMap, partSupMap = make(map[string]map[int16]map[PairInt]int32), make(map[int16]map[PairInt]int32), make(map[PairInt]int32)
				partSupMap[PairInt{first: item.L_PARTKEY, second: item.L_SUPPKEY}] = int32(item.L_QUANTITY) * signal
				yearMap[item.L_SHIPDATE.YEAR] = partSupMap
				natMap[color] = yearMap
				q20.q20Map[nationKey] = natMap
				nUpds++
				continue
			}
			yearMap, has = natMap[color]
			if !has {
				yearMap, partSupMap = make(map[int16]map[PairInt]int32), make(map[PairInt]int32)
				partSupMap[PairInt{first: item.L_PARTKEY, second: item.L_SUPPKEY}] = int32(item.L_QUANTITY) * signal
				yearMap[item.L_SHIPDATE.YEAR] = partSupMap
				natMap[color] = yearMap
				nUpds++
				continue
			}
			partSupMap, has = yearMap[item.L_SHIPDATE.YEAR]
			if !has {
				partSupMap = make(map[PairInt]int32)
				partSupMap[PairInt{first: item.L_PARTKEY, second: item.L_SUPPKEY}] = int32(item.L_QUANTITY) * signal
				yearMap[item.L_SHIPDATE.YEAR] = partSupMap
				nUpds++
				continue
			}
			partSupMap[PairInt{first: item.L_PARTKEY, second: item.L_SUPPKEY}] += int32(item.L_QUANTITY) * signal
		}
	}
	return
}

func (q20 SingleQ20) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q20.nUpds, q20.nUpds, getNBlockUpds(q20.nUpds)
}

func (q20 SingleQ20) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	//Note: We only need to update the counter of quantity. Availqty never changes, nor does PartSupp - thus there's already entries for all PairInts.
	if !*q20.done {
		var natMapUpd, yearMapUpd crdt.EmbMapUpdateAll
		for natId, natMap := range q20.q20Map {
			natMapUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
			for color, colorMap := range natMap {
				for year, yearMap := range colorMap {
					yearMapUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
					for partSuppPair, count := range yearMap {
						yearMapUpd.Upds[strconv.FormatInt(int64(partSuppPair.first), 10)+"_"+strconv.FormatInt(int64(partSuppPair.second), 10)] =
							crdt.EmbMapUpdate{Key: Q20_SUM, Upd: crdt.Increment{Change: count}}
					}
					natMapUpd.Upds[color+strconv.FormatInt(int64(year), 10)] = yearMapUpd
				}
			}
			var args crdt.UpdateArguments = natMapUpd
			buf[bufI] = crdt.UpdateObjectParams{KeyParams: crdt.KeyParams{Key: Q20_KEY + strconv.FormatInt(int64(natId), 10), CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}, UpdateArgs: &args}
			bufI++
		}
		*q20.done = true
	}
	return bufI
}

func (q20 SingleQ20) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q20.makeNewUpdArgs(tables, remOrder, buf, bufI, bucketI)
}
*/

// *****SingleLocalQ20*****
// Since the region/nation is based on the supplier's nation, an update may have multiple regions :(
type SingleLocalQ20 struct {
	q20PerReg         []SingleQ20
	nUpds, nBlockUpds int
	done              *bool
}

func (q20 SingleLocalQ20) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	q20.q20PerReg, q20.nUpds, q20.nBlockUpds, q20.done = make([]SingleQ20, len(tables.Regions)), 0, 0, new(bool)
	newItemsReg, remItemsReg := tables.GetOrderItemsPerSupplier(newItems), tables.GetOrderItemsPerSupplier(remItems)
	for i, newItems := range newItemsReg {
		remItems := remItemsReg[i]
		if len(newItems) > 0 || len(remItems) > 0 {
			q20.q20PerReg[i] = SingleQ20{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems).(SingleQ20) //It's okay even if one of the slices is nil
			q20.nBlockUpds++
			regNUpds, _, _ := q20.q20PerReg[i].countUpds()
			q20.nUpds += regNUpds
		}
	}
	return q20
}

func (q20 SingleLocalQ20) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q20.nUpds, q20.nUpds, q20.nBlockUpds
}

func (q20 SingleLocalQ20) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	if !*q20.done {
		for i, q20Reg := range q20.q20PerReg {
			if q20Reg.q20Map != nil { //If it's nil, then it means that region has no updates
				bufI = q20Reg.makeNewUpdArgs(tables, newOrder, buf, bufI, INDEX_BKT+i)
			}
		}
		*q20.done = true
	}
	return bufI
}

func (q20 SingleLocalQ20) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q20.makeNewUpdArgs(tables, remOrder, buf, bufI, bucketI)
}

// *****SingleQ21, Q21**+**
// Only one supplier can be late for there to be an update. Thus, only one update per add/remove (at most)
type SingleQ21 struct {
	addSupId, remSupId int32
}

func (q21 SingleQ21) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	q21.addSupId, q21.remSupId = q21.calcHelper(newItems), q21.calcHelper(remItems)
	return q21
}

// Returns -1 if no update should be issued
func (q21 SingleQ21) calcHelper(items []*tpch.LineItem) (suppId int32) {
	lateSuppKey := int32(-1)
	for _, item := range items {
		if item.L_RECEIPTDATE.IsHigher(&item.L_COMMITDATE) {
			if lateSuppKey != -1 && lateSuppKey != item.L_SUPPKEY {
				return -1 //More than one supplier late, thus no update.
			}
			lateSuppKey = item.L_SUPPKEY
		}
	}
	if lateSuppKey == -1 {
		return -1 //No supplier late, thus no update.
	}
	for _, item := range items { //Check if there exists a different supplierKey
		if item.L_SUPPKEY != lateSuppKey { //There is at least one other supplier, so can update
			return lateSuppKey
		}
	}
	//If it reaches here, it means the supplier is late but he's the only supplier of the order - so no update.
	return lateSuppKey
}

func (q21 SingleQ21) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	if q21.addSupId != -1 {
		nUpds++
	}
	if q21.remSupId != -1 {
		nUpds++
	}
	return nUpds, nUpds, getNBlockUpds(nUpds)
}

func (q21 SingleQ21) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	if q21.addSupId != -1 {
		buf[bufI] = crdt.UpdateObjectParams{
			KeyParams:  crdt.KeyParams{Key: Q21_KEY + strconv.FormatInt(int64(tables.Suppliers[q21.addSupId].S_NATIONKEY), 10), CrdtType: proto.CRDTType_TOPSUM, Bucket: buckets[bucketI]},
			UpdateArgs: crdt.TopSAdd{TopKScore: crdt.TopKScore{Id: q21.addSupId, Score: 1}},
		}
		bufI++
	}
	return bufI
}

func (q21 SingleQ21) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	if q21.remSupId != -1 {
		buf[bufI] = crdt.UpdateObjectParams{
			KeyParams:  crdt.KeyParams{Key: Q21_KEY + strconv.FormatInt(int64(tables.Suppliers[q21.remSupId].S_NATIONKEY), 10), CrdtType: proto.CRDTType_TOPSUM, Bucket: buckets[bucketI]},
			UpdateArgs: crdt.TopSSub{TopKScore: crdt.TopKScore{Id: q21.addSupId, Score: 1}},
		}
		bufI++
	}
	return bufI
}

// *****SingleQ22, Q22**+**
// A new/rem order increases/decreases only one entry
type SingleQ22 struct {
	incCustId, decCustId   int32
	incPhoneId, decPhoneId int8
}

func (q22 SingleQ22) buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo {
	q22.incCustId, q22.decCustId = newOrder.O_CUSTKEY, remOrder.O_CUSTKEY
	q22.incPhoneId, q22.decPhoneId = tables.OrderToNationkey(newOrder)+10, tables.OrderToNationkey(remOrder)+10
	return q22
}

func (q22 SingleQ22) countUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return 2, 2, 1
}

func (q22 SingleQ22) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	keyP := crdt.KeyParams{Key: Q22_KEY + strconv.FormatInt(int64(q22.incPhoneId), 10), Bucket: buckets[bucketI], CrdtType: proto.CRDTType_RRMAP}
	upd := crdt.EmbMapUpdate{Key: strconv.FormatInt(int64(q22.incCustId), 10), Upd: crdt.EmbMapUpdate{Key: "C", Upd: crdt.Increment{Change: 1}}}
	buf[bufI] = crdt.UpdateObjectParams{KeyParams: keyP, UpdateArgs: upd}
	return bufI + 1
}

func (q22 SingleQ22) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	keyP := crdt.KeyParams{Key: Q22_KEY + strconv.FormatInt(int64(q22.decPhoneId), 10), Bucket: buckets[bucketI], CrdtType: proto.CRDTType_RRMAP}
	upd := crdt.EmbMapUpdate{Key: strconv.FormatInt(int64(q22.decCustId), 10), Upd: crdt.EmbMapUpdate{Key: "C", Upd: crdt.Increment{Change: -1}}}
	buf[bufI] = crdt.UpdateObjectParams{KeyParams: keyP, UpdateArgs: upd}
	return bufI + 1
}

//*****Making lists of index infos*****

func makeIndexInfos(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) (infos []SingleQueryInfo) {
	if !UPDATE_SPECIFIC_INDEX_ONLY {
		return makeIndexInfosFull(tables, newOrder, remOrder, newItems, remItems)
	}
	return makeIndexInfosFromList(tables, indexesToUpd, newOrder, remOrder, newItems, remItems)
}

func makeIndexInfosFull(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) (infos []SingleQueryInfo) {
	infos = make([]SingleQueryInfo, 19)
	infos[1] = SingleQ3{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
	infos[2] = SingleQ4{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
	infos[3] = SingleQ5{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
	infos[8] = SingleQ10{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
	infos[13] = SingleQ17{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
	infos[14] = SingleQ18{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
	infos[17] = SingleQ21{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
	infos[18] = SingleQ22{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
	if isIndexGlobal {
		infos[0] = SingleQ1{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
		infos[4] = SingleQ6{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
		infos[5] = SingleQ7{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
		infos[6] = SingleQ8{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
		infos[7] = SingleQ9{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
		infos[9] = SingleQ12{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
		infos[10] = SingleQ13{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
		infos[11] = SingleQ14{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
		infos[12] = SingleQ15{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
		infos[15] = SingleQ19{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
		infos[16] = SingleQ20{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
	} else {
		infos[0] = SingleLocalQ1{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
		infos[4] = SingleLocalQ6{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
		infos[5] = SingleLocalQ7{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
		infos[6] = SingleLocalQ8{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
		infos[7] = SingleLocalQ9{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
		infos[9] = SingleLocalQ12{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
		infos[10] = SingleLocalQ13{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
		infos[11] = SingleLocalQ14{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
		infos[12] = SingleLocalQ15{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
		infos[15] = SingleLocalQ19{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
		infos[16] = SingleLocalQ20{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
	}
	return
}

func makeIndexInfosFromList(tables *tpch.Tables, indexes []int, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) (infos []SingleQueryInfo) {
	infos = make([]SingleQueryInfo, len(indexes))
	if isIndexGlobal {
		for i, queryN := range indexes {
			switch queryN {
			case 1:
				infos[i] = SingleQ1{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 3:
				infos[i] = SingleQ3{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 4:
				infos[i] = SingleQ4{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 5:
				infos[i] = SingleQ5{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 6:
				infos[i] = SingleQ6{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 7:
				infos[i] = SingleQ7{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 8:
				infos[i] = SingleQ8{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 9:
				infos[i] = SingleQ9{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 10:
				infos[i] = SingleQ10{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 12:
				infos[i] = SingleQ12{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 13:
				infos[i] = SingleQ13{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 14:
				infos[i] = SingleQ14{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 15:
				if useTopSum {
					infos[i] = SingleQ15TopSum{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
				} else {
					infos[i] = SingleQ15{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
				}
			case 17:
				infos[i] = SingleQ17{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 18:
				infos[i] = SingleQ18{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 19:
				infos[i] = SingleQ19{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 20:
				infos[i] = SingleQ20{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 21:
				infos[i] = SingleQ21{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 22:
				infos[i] = SingleQ22{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			}
		}
	} else {
		for i, queryN := range indexes {
			switch queryN {
			case 1:
				infos[i] = SingleLocalQ1{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 3:
				infos[i] = SingleQ3{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 4:
				infos[i] = SingleQ4{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 5:
				infos[i] = SingleQ5{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 6:
				infos[i] = SingleLocalQ6{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 7:
				infos[i] = SingleLocalQ7{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 8:
				infos[i] = SingleLocalQ8{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 9:
				infos[i] = SingleLocalQ9{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 10:
				infos[i] = SingleQ10{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 12:
				infos[i] = SingleLocalQ12{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 13:
				infos[i] = SingleLocalQ13{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 14:
				infos[i] = SingleLocalQ14{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 15:
				if useTopSum {
					infos[i] = SingleLocalQ15TopSum{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
				} else {
					infos[i] = SingleLocalQ15{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
				}
			case 17:
				infos[i] = SingleQ17{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 18:
				infos[i] = SingleQ18{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 19:
				infos[i] = SingleLocalQ19{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 20:
				infos[i] = SingleLocalQ20{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 21:
				infos[i] = SingleQ21{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			case 22:
				infos[i] = SingleQ22{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			}
		}
	}
	return
}

/*
func makeIndexInfosFull(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) (infos []SingleQueryInfo) {
	infos = make([]SingleQueryInfo, 5)
	infos[0] = SingleQ3{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
	infos[1] = SingleQ5{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
	infos[4] = SingleQ18{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
	if isIndexGlobal {
		infos[2] = SingleQ14{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
		if !useTopSum {
			infos[3] = SingleQ15{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
		} else {
			infos[3] = SingleQ15TopSum{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
		}
	} else {
		infos[2] = SingleLocalQ14{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
		if !useTopSum {
			infos[3] = SingleLocalQ15{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
		} else {
			infos[3] = SingleLocalQ15TopSum{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
		}
	}
	return
}
*/

/*
func makeIndexInfosFromList(tables *tpch.Tables, indexes []int, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) (infos []SingleQueryInfo) {
	infos = make([]SingleQueryInfo, len(indexes))
	for i, queryN := range indexes {
		switch queryN {
		case 3:
			infos[i] = SingleQ3{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
		case 5:
			infos[i] = SingleQ5{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
		case 14:
			if isIndexGlobal {
				infos[i] = SingleQ14{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			} else {
				infos[i] = SingleLocalQ14{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
			}
		case 15:
			if isIndexGlobal {
				if !useTopSum {
					infos[i] = SingleQ15{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
				} else {
					infos[i] = SingleQ15TopSum{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
				}
			} else {
				if !useTopSum {
					infos[i] = SingleLocalQ15{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
				} else {
					infos[i] = SingleLocalQ15TopSum{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
				}
			}
		case 18:
			infos[i] = SingleQ18{}.buildUpdInfo(tables, newOrder, remOrder, newItems, remItems)
		}
	}
	return
}
*/

func countUpdsIndexInfo(infos []SingleQueryInfo) (nUpds, nUpdsStats, nBlockUpds int) {
	currNUpds, currNUpdsStats, currNBlockUpds := 0, 0, 0
	for _, info := range infos {
		currNUpds, currNUpdsStats, currNBlockUpds = info.countUpds()
		nUpds, nUpdsStats, nBlockUpds = nUpds+currNUpds, nUpdsStats+currNUpdsStats, nBlockUpds+currNBlockUpds
	}
	return
}
