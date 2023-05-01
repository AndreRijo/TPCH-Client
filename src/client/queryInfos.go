package client

import (
	"potionDB/src/antidote"
	"potionDB/src/crdt"
	"potionDB/src/proto"
	"strconv"
	"potionDB/tpch_helper"
)

//Contains all the methods related to each query's structure (SingleQ3, etc.)
//Methods to build only part of a query structure (i.e.., add-only or rem-only) are in queryInfosHelper.go

/*
type IndexInfo struct {
	q3         SingleQ3
	q5         SingleQ5
	q14        SingleQ14
	q15        SingleQ15
	q15TopSum  SingleQ15TopSum
	q18        SingleQ18
	lq14       SingleLocalQ14
	lq15       SingleLocalQ15
	lq15TopSum SingleLocalQ15TopSum
}*/

type SingleQueryInfo interface {
	buildUpdInfo(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) SingleQueryInfo
	countUpds() (nUpds, nUpdsStats, nBlockUpds int)
	makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int)
	makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int)
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
	if newOrder.O_ORDERDATE.IsSmallerOrEqual(MAX_DATE_Q3) {
		//Need to do add
		q3.newsCalcHelper(newOrder, newItems)
		nUpds += int(MAX_MONTH_DAY - q3.updStartPos + 1)
	}
	//Single segment, but multiple days (as days are cumulative).
	if remOrder.O_ORDERDATE.IsSmallerOrEqual(MAX_DATE_Q3) {
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

func (q3 SingleQ3) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {

	var keyArgs antidote.KeyParams
	//Adds. Due to the date restriction, there may be no adds.
	if q3.updStartPos != 0 && q3.updStartPos <= MAX_MONTH_DAY {
		updSegKey := SEGM_DELAY + q3.newSeg
		for day := q3.updStartPos; day <= MAX_MONTH_DAY; day++ {
			keyArgs = antidote.KeyParams{
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
			buf[bufI] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
			bufI++
		}
	}
	return bufI
}

func (q3 SingleQ3) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {

	var keyArgs antidote.KeyParams
	//Rems
	if len(q3.remMap) > 0 {
		remSegKey := SEGM_DELAY + q3.oldSeg
		for day := range q3.remMap {
			keyArgs = antidote.KeyParams{
				Key:      remSegKey + strconv.FormatInt(int64(day), 10),
				CrdtType: proto.CRDTType_TOPK_RMV,
				Bucket:   buckets[bucketI],
			}
			//useTopKAll not relevant as it is only one order
			var currUpd crdt.UpdateArguments = crdt.TopKRemove{Id: remOrder.O_ORDERKEY}
			buf[bufI] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
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

func (q5 SingleQ5) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q5.makeUpdsArgsHelper(*q5.updValue, 1.0, tables, newOrder, buf, bufI, bucketI)
}

func (q5 SingleQ5) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q5.makeUpdsArgsHelper(*q5.remValue, -1.0, tables, remOrder, buf, bufI, bucketI)
}

func (q5 SingleQ5) makeUpdsArgsHelper(value float64, signal float64, tables *tpch.Tables, order *tpch.Orders, buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	if value != 0 {
		remYear, remNation := order.O_ORDERDATE.YEAR, tables.Nations[tables.Customers[order.O_CUSTKEY].C_NATIONKEY]
		embMapUpd := crdt.EmbMapUpdate{Key: remNation.N_NAME, Upd: crdt.Increment{Change: int32(signal * value)}}
		var args crdt.UpdateArguments = embMapUpd
		buf[bufI] = antidote.UpdateObjectParams{
			KeyParams: antidote.KeyParams{Key: NATION_REVENUE + tables.Regions[remNation.N_REGIONKEY].R_NAME + strconv.FormatInt(int64(remYear-1993), 10),
				CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]},
			UpdateArgs: &args,
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

func (q14 SingleQ14) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	for key, totalP := range q14.mapTotal {
		promo := *q14.mapPromo[key]
		var currUpd crdt.UpdateArguments = crdt.AddMultipleValue{
			SumValue: int64(100.0 * promo),
			NAdds:    int64(*totalP),
		}
		buf[bufI] = antidote.UpdateObjectParams{
			KeyParams:  antidote.KeyParams{Key: PROMO_PERCENTAGE + key, CrdtType: proto.CRDTType_AVG, Bucket: buckets[bucketI]},
			UpdateArgs: &currUpd,
		}
		bufI++
	}

	return bufI
}
func (q14 SingleQ14) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
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

func (q14 SingleLocalQ14) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q14.newQ14.makeNewUpdArgs(tables, newOrder, buf, bufI, bucketI)
}
func (q14 SingleLocalQ14) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
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

func (q15 SingleQ15) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	newBufI, _ = makeQ15IndexUpdsDeletesHelper(q15Map, q15.updEntries, bucketI, buf, bufI)
	return
}
func (q15 SingleQ15) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
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

func (q15 SingleLocalQ15) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	regKey := tables.OrderToRegionkey(newOrder)
	newBufI, _ = makeQ15IndexUpdsDeletesHelper(q15LocalMap[regKey], q15.newQ15.updEntries, bucketI, buf, bufI)
	return newBufI
}
func (q15 SingleLocalQ15) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
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

func (q15 SingleQ15TopSum) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	newBufI, _ = makeQ15TopSumIndexUpdsDeletesHelper(q15Map, q15.diffEntries, bucketI, buf, bufI)
	return
}
func (q15 SingleQ15TopSum) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
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

func (q15 SingleLocalQ15TopSum) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	regKey := tables.OrderToRegionkey(newOrder)
	newBufI, _ = makeQ15TopSumIndexUpdsDeletesHelper(q15LocalMap[regKey], q15.newQ15.diffEntries, bucketI, buf, bufI)
	return newBufI
}
func (q15 SingleLocalQ15TopSum) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
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

func (q18 SingleQ18) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	updQ := q18.updQuantity
	var keyArgs antidote.KeyParams

	if updQ >= 312 {
		//fmt.Println("[Q18]Upd quantity is actually >= 312", updQ, newOrder.O_ORDERKEY)
		if updQ > 315 {
			updQ = 315
		}
		for i := int64(312); i <= updQ; i++ {
			keyArgs = antidote.KeyParams{
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
			buf[bufI] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
			bufI++
		}
	}
	return bufI
}
func (q18 SingleQ18) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	remQ := q18.remQuantity
	var keyArgs antidote.KeyParams

	if remQ >= 312 {
		if remQ > 315 {
			remQ = 315
		}
		for i := int64(312); i <= remQ; i++ {
			keyArgs = antidote.KeyParams{
				Key:      LARGE_ORDERS + strconv.FormatInt(i, 10),
				CrdtType: proto.CRDTType_TOPK_RMV,
				Bucket:   buckets[bucketI],
			}
			//useTopKAll is irrelevant since it is only 1 order
			var currUpd crdt.UpdateArguments = crdt.TopKRemove{Id: remOrder.O_ORDERKEY}
			buf[bufI] = antidote.UpdateObjectParams{KeyParams: keyArgs, UpdateArgs: &currUpd}
			bufI++
		}
	}
	return bufI
}

func getNBlockUpds(nUpds int) int {
	if nUpds > 0 {
		return 1
	}
	return 0
}

//*****Making lists of index infos*****

func makeIndexInfos(tables *tpch.Tables, newOrder, remOrder *tpch.Orders, newItems, remItems []*tpch.LineItem) (infos []SingleQueryInfo) {
	if !UPDATE_SPECIFIC_INDEX_ONLY {
		return makeIndexInfosFull(tables, newOrder, remOrder, newItems, remItems)
	}
	return makeIndexInfosFromList(tables, indexesToUpd, newOrder, remOrder, newItems, remItems)
}

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

func countUpdsIndexInfo(infos []SingleQueryInfo) (nUpds, nUpdsStats, nBlockUpds int) {
	currNUpds, currNUpdsStats, currNBlockUpds := 0, 0, 0
	for _, info := range infos {
		currNUpds, currNUpdsStats, currNBlockUpds = info.countUpds()
		nUpds, nUpdsStats, nBlockUpds = nUpds+currNUpds, nUpdsStats+currNUpdsStats, nBlockUpds+currNBlockUpds
	}
	return
}
