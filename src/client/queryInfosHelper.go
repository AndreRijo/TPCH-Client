package client

import (
	"potionDB/crdt/crdt"
	"potionDB/crdt/proto"
	"strconv"
	"strings"
	tpch "tpch_data_processor/tpch"
)

//Contains all the methods for building only part of the query info (add or remove)

// Repeating other methods so that this interface has access to all.
type SingleSplitQueryInfo interface {
	buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo
	buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo
	countAddUpds() (nUpds, nUpdsStats, nBlockUpds int)
	countRemUpds() (nUpds, nUpdsStats, nBlockUpds int)
	makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int)
	makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int)
}

//*****Q1*****

func (q1 SingleQ1) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	q1.q1Map, q1.done = make(map[int8]map[string]*Q1Data), new(bool)
	q1.q1CalcHelper(newOrder, newItems, true)
	return q1
}

func (q1 SingleQ1) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	q1.q1Map, q1.done = make(map[int8]map[string]*Q1Data), new(bool)
	q1.q1CalcHelper(remOrder, remItems, false)
	return q1
}

func (q1 SingleQ1) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return len(q1.q1Map), len(q1.q1Map), getNBlockUpds(len(q1.q1Map))
}

func (q1 SingleQ1) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return len(q1.q1Map), len(q1.q1Map), getNBlockUpds(len(q1.q1Map))
}

func (q1 SingleLocalQ1) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	q1.q1New = SingleQ1{q1Map: make(map[int8]map[string]*Q1Data), done: new(bool)}
	q1.q1New.q1CalcHelper(newOrder, newItems, true)
	return q1
}

func (q1 SingleLocalQ1) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	q1.q1Rem = SingleQ1{q1Map: make(map[int8]map[string]*Q1Data), done: new(bool)}
	q1.q1Rem.q1CalcHelper(remOrder, remItems, false)
	return q1
}

func (q1 SingleLocalQ1) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q1.q1New.countUpds()
}

func (q1 SingleLocalQ1) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q1.q1Rem.countUpds()
}

//*****Q4*****

func (q4 SingleQ4) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	return SingleQ4{doesInc: q4.calcHelper(newOrder, newItems), doesDec: false}
}

func (q4 SingleQ4) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	return SingleQ4{doesInc: q4.calcHelper(remOrder, remItems), doesDec: false}
}

func (q4 SingleQ4) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	if q4.doesInc {
		return 1, 1, getNBlockUpds(1)
	}
	return 0, 0, getNBlockUpds(0)
}

func (q4 SingleQ4) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	if q4.doesDec {
		return 1, 1, getNBlockUpds(1)
	}
	return 0, 0, getNBlockUpds(0)
}

//*****Q6*****

func (q6 SingleQ6) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	q6.diffMap, q6.done = make(map[int16]map[int8]map[int8]*float64), new(bool)
	for year := int16(1993); year <= 1997; year++ {
		q6.diffMap[year] = map[int8]map[int8]*float64{24: make(map[int8]*float64), 25: make(map[int8]*float64)}
	}
	q6.nUpds += q6.calcHelper(newItems, 1)
	return nil
}

func (q6 SingleQ6) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	q6.diffMap = make(map[int16]map[int8]map[int8]*float64)
	for year := int16(1993); year <= 1997; year++ {
		q6.diffMap[year] = map[int8]map[int8]*float64{24: make(map[int8]*float64), 25: make(map[int8]*float64)}
	}
	q6.nUpds += q6.calcHelper(remItems, 1)
	return nil
}

func (q6 SingleQ6) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q6.nUpds, q6.nUpds, getNBlockUpds(q6.nUpds)
}

func (q6 SingleQ6) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q6.nUpds, q6.nUpds, getNBlockUpds(q6.nUpds)
}

func (q6 SingleLocalQ6) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	q6.newQ6 = SingleQ6{diffMap: make(map[int16]map[int8]map[int8]*float64), nUpds: 0, done: new(bool)}
	for year := int16(1993); year <= 1997; year++ {
		q6.newQ6.diffMap[year] = map[int8]map[int8]*float64{24: make(map[int8]*float64), 25: make(map[int8]*float64)}
	}
	q6.newQ6.nUpds += q6.newQ6.calcHelper(newItems, 1)
	return q6
}

func (q6 SingleLocalQ6) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	q6.remQ6 = SingleQ6{diffMap: make(map[int16]map[int8]map[int8]*float64), nUpds: 0, done: new(bool)}
	for year := int16(1993); year <= 1997; year++ {
		q6.remQ6.diffMap[year] = map[int8]map[int8]*float64{24: make(map[int8]*float64), 25: make(map[int8]*float64)}
	}
	q6.remQ6.nUpds += q6.remQ6.calcHelper(remItems, -1)
	return q6
}

func (q6 SingleLocalQ6) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q6.newQ6.countUpds()
}

func (q6 SingleLocalQ6) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q6.remQ6.countUpds()
}

//*****Q7*****

func (q7 SingleQ7) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	q7.q7Map, q7.nUpds, q7.done = make(map[int8]map[int16]map[int8]*float64), 0, new(bool)
	q7.calcHelper(tables, newOrder, newItems, 1)
	return q7
}

func (q7 SingleQ7) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	q7.q7Map, q7.nUpds, q7.done = make(map[int8]map[int16]map[int8]*float64), 0, new(bool)
	q7.calcHelper(tables, remOrder, remItems, -1)
	return q7
}

func (q7 SingleQ7) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return nUpds, nUpds, getNBlockUpds(nUpds)
}

func (q7 SingleQ7) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return nUpds, nUpds, getNBlockUpds(nUpds)
}

func (q7 SingleLocalQ7) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	q7.newQ7 = SingleQ7{q7Map: make(map[int8]map[int16]map[int8]*float64), nUpds: 0, done: new(bool)}
	q7.newQ7.calcHelper(tables, newOrder, newItems, 1)
	return q7
}

func (q7 SingleLocalQ7) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	q7.remQ7 = SingleQ7{q7Map: make(map[int8]map[int16]map[int8]*float64), nUpds: 0, done: new(bool)}
	q7.remQ7.calcHelper(tables, remOrder, remItems, -1)
	return q7
}

func (q7 SingleLocalQ7) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q7.newQ7.countUpds()
}

func (q7 SingleLocalQ7) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q7.remQ7.countUpds()
}

//*****Q8*****

func (q8 SingleQ8) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	q8.nationMap, q8.regionMap, q8.nUpds, q8.done = make(map[string]map[int8]*Q8YearPair), make(map[string]map[int8]*Q8YearPair), 0, new(bool)
	q8.nUpds += q8.calcHelper(tables, newOrder, newItems, 1)
	return q8
}

func (q8 SingleQ8) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	q8.nationMap, q8.regionMap, q8.nUpds, q8.done = make(map[string]map[int8]*Q8YearPair), make(map[string]map[int8]*Q8YearPair), 0, new(bool)
	q8.nUpds += q8.calcHelper(tables, remOrder, remItems, -1)
	return q8
}

func (q8 SingleQ8) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q8.nUpds, q8.nUpds, getNBlockUpds(q8.nUpds)
}

func (q8 SingleQ8) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q8.nUpds, q8.nUpds, getNBlockUpds(q8.nUpds)
}

func (q8 SingleLocalQ8) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	q8.q8PerReg, q8.nUpds, q8.done = make([]SingleQ8, len(tables.Regions)), 0, new(bool)
	newItemsReg := tables.GetOrderItemsPerSupplier(newItems)
	for i, newItemsR := range newItemsReg {
		if len(newItemsR) > 0 {
			q8.q8PerReg[i] = SingleQ8{}.buildAddInfo(tables, newOrder, newItemsR).(SingleQ8)
			q8.nBlockUpds++
			regNUpds, _, _ := q8.q8PerReg[i].countAddUpds()
			q8.nUpds += regNUpds
		}
	}
	return q8
}

func (q8 SingleLocalQ8) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	q8.q8PerReg, q8.nUpds, q8.done = make([]SingleQ8, len(tables.Regions)), 0, new(bool)
	remItemsReg := tables.GetOrderItemsPerSupplier(remItems)
	for i, remItemsR := range remItemsReg {
		if len(remItemsR) > 0 {
			q8.q8PerReg[i] = SingleQ8{}.buildRemInfo(tables, remOrder, remItemsR).(SingleQ8)
			q8.nBlockUpds++
			regNUpds, _, _ := q8.q8PerReg[i].countRemUpds()
			q8.nUpds += regNUpds
		}
	}
	return q8
}

func (q8 SingleLocalQ8) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q8.nUpds, q8.nUpds, getNBlockUpds(q8.nUpds)
}

func (q8 SingleLocalQ8) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q8.nUpds, q8.nUpds, getNBlockUpds(q8.nUpds)
}

// *****Q9*****
// When doing only add or only rem, the update process can be slightly optimized as only one year is possible
type SingleSplitQ9 struct {
	q9Map map[string]map[int8]*float64
	year  int16
	nUpds int
}

func (q9 SingleSplitQ9) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	q9.q9Map, q9.year, q9.nUpds = make(map[string]map[int8]*float64), newOrder.O_ORDERDATE.YEAR, 0
	q9.nUpds += q9.calcHelper(tables, newItems, 1)
	return q9
}

func (q9 SingleSplitQ9) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	q9.q9Map, q9.year, q9.nUpds = make(map[string]map[int8]*float64), remOrder.O_ORDERDATE.YEAR, 0
	q9.nUpds += q9.calcHelper(tables, remItems, -1)
	return q9
}

func (q9 SingleSplitQ9) calcHelper(tables *tpch.Tables, items []*tpch.LineItem, signal float64) (nUpds int) {
	var natId int8
	var colors []string
	var partSupp *tpch.PartSupp
	var value float64
	var colorMap map[int8]*float64
	var has bool
	var sum *float64
	itemColors := make([][]string, len(items))

	//Pre-processing: create all needed color maps
	for i, item := range items {
		itemColors[i] = strings.Split(tables.Parts[item.L_PARTKEY].P_NAME, " ")
	}

	for i, item := range items {
		natId = tables.SupplierkeyToNationkey(item.L_SUPPKEY)
		colors = itemColors[i]
		partSupp = tables.GetPartSuppOfLineitem(item.L_PARTKEY, item.L_SUPPKEY)
		value = item.L_EXTENDEDPRICE*(1-item.L_DISCOUNT) - partSupp.PS_SUPPLYCOST*float64(item.L_QUANTITY)*signal
		for _, color := range colors {
			colorMap = q9.q9Map[color]
			sum, has = colorMap[natId]
			if !has {
				sum = new(float64)
				colorMap[natId] = sum
				nUpds++
			}
			*sum += value
		}
	}
	return
}

func (q9 SingleSplitQ9) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q9.nUpds, q9.nUpds, getNBlockUpds(q9.nUpds)
}

func (q9 SingleSplitQ9) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q9.nUpds, q9.nUpds, getNBlockUpds(q9.nUpds)
}

func (q9 SingleSplitQ9) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	var keyParams crdt.KeyParams
	yearString := strconv.FormatInt(int64(q9.year), 10)
	var mapNationUpd crdt.EmbMapUpdateAll
	for color, colorMap := range q9.q9Map {
		keyParams = crdt.KeyParams{Key: Q9_KEY + color, CrdtType: proto.CRDTType_RRMAP, Bucket: buckets[bucketI]}
		mapNationUpd = crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
		for natId, value := range colorMap {
			mapNationUpd.Upds[tables.Nations[natId].N_NAME] = crdt.EmbMapUpdate{Key: yearString, Upd: crdt.Increment{Change: int32(*value)}}
		}
		buf[bufI] = crdt.UpdateObjectParams{KeyParams: keyParams, UpdateArgs: mapNationUpd}
		bufI++
	}
	return bufI
}

func (q9 SingleSplitQ9) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q9.makeNewUpdArgs(tables, remOrder, buf, bufI, bucketI) //Same logic
}

type SingleSplitLocalQ9 struct {
	q9PerReg          []SingleSplitQ9
	nUpds, nBlockUpds int
}

func (q9 SingleSplitLocalQ9) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	q9.q9PerReg, q9.nUpds, q9.nBlockUpds = make([]SingleSplitQ9, len(tables.Regions)), 0, 0
	newItemsReg := tables.GetOrderItemsPerSupplier(newItems)
	for i, newItems := range newItemsReg {
		if len(newItems) > 0 {
			q9.q9PerReg[i] = SingleSplitQ9{}.buildAddInfo(tables, newOrder, newItems).(SingleSplitQ9)
			q9.nBlockUpds++
			regNUpds, _, _ := q9.q9PerReg[i].countAddUpds()
			q9.nUpds += regNUpds
		}
	}
	return q9
}

func (q9 SingleSplitLocalQ9) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	q9.q9PerReg, q9.nUpds, q9.nBlockUpds = make([]SingleSplitQ9, len(tables.Regions)), 0, 0
	remItemsReg := tables.GetOrderItemsPerSupplier(remItems)
	for i, remItems := range remItemsReg {
		if len(remItems) > 0 {
			q9.q9PerReg[i] = SingleSplitQ9{}.buildRemInfo(tables, remOrder, remItems).(SingleSplitQ9)
			q9.nBlockUpds++
			regNUpds, _, _ := q9.q9PerReg[i].countRemUpds()
			q9.nUpds += regNUpds
		}
	}
	return q9
}

func (q9 SingleSplitLocalQ9) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q9.nUpds, q9.nUpds, q9.nBlockUpds
}

func (q9 SingleSplitLocalQ9) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q9.nUpds, q9.nUpds, q9.nBlockUpds
}

func (q9 SingleSplitLocalQ9) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	for i, q9Reg := range q9.q9PerReg {
		if q9Reg.q9Map != nil { //If it's nil, then it means that region has no updates
			bufI = q9Reg.makeNewUpdArgs(tables, newOrder, buf, bufI, bucketI+i)
		}
	}
	return bufI
}

func (q9 SingleSplitLocalQ9) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q9.makeNewUpdArgs(tables, remOrder, buf, bufI, bucketI)
}

//*****Q10*****

func (q10 SingleQ10) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	q10.addValue = q10.calcHelper(tables, newOrder, newItems, 1)
	return q10
}

func (q10 SingleQ10) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	q10.remValue = q10.calcHelper(tables, remOrder, remItems, -1)
	return q10
}

func (q10 SingleQ10) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	if q10.addValue != 0 {
		nUpds++
	}
	return nUpds, nUpds, getNBlockUpds(nUpds)
}

func (q10 SingleQ10) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	if q10.remValue != 0 {
		nUpds++
	}
	return nUpds, nUpds, getNBlockUpds(nUpds)
}

//*****Q12*****

func (q12 SingleQ12) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	q12.q12Map, q12.nUpds = make(map[int16]map[string]map[string]*int32), 0
	q12.nUpds += q12.calcHelper(tables, newOrder, newItems, 1)
	return q12
}

func (q12 SingleQ12) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	q12.q12Map, q12.nUpds = make(map[int16]map[string]map[string]*int32), 0
	q12.nUpds += q12.calcHelper(tables, remOrder, remItems, -1)
	return q12
}

func (q12 SingleQ12) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q12.nUpds, q12.nUpds, getNBlockUpds(q12.nUpds)
}

func (q12 SingleQ12) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q12.nUpds, q12.nUpds, getNBlockUpds(q12.nUpds)
}

func (q12 SingleLocalQ12) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	q12.addQ12 = SingleQ12{q12Map: make(map[int16]map[string]map[string]*int32), nUpds: 0, done: new(bool)}
	q12.nUpds += q12.addQ12.calcHelper(tables, newOrder, newItems, 1)
	return q12
}

func (q12 SingleLocalQ12) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	q12.remQ12 = SingleQ12{q12Map: make(map[int16]map[string]map[string]*int32), nUpds: 0, done: new(bool)}
	q12.nUpds += q12.remQ12.calcHelper(tables, remOrder, remItems, -1)
	return q12
}

func (q12 SingleLocalQ12) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q12.nUpds, q12.nUpds, getNBlockUpds(q12.nUpds)
}

func (q12 SingleLocalQ12) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q12.nUpds, q12.nUpds, getNBlockUpds(q12.nUpds)
}

//*****Q13*****

func (q13 SingleQ13) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	q13.newCustWordsOldOrders = q13.calcHelper(tables, newOrder, newItems, 1)
	q13.done = new(bool)
	return q13
}

func (q13 SingleQ13) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	q13.remCustWordsOldOrders = q13.calcHelper(tables, remOrder, remItems, -1)
	q13.done = new(bool)
	return q13
}

func (q13 SingleQ13) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q13.countUpds()
}

func (q13 SingleQ13) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q13.countUpds()
}

func (q13 SingleLocalQ13) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	q13.newCustWordsOldOrders = q13.calcHelper(tables, newOrder, newItems, 1)
	q13.done, q13.sameNation = new(bool), false
	return q13
}

func (q13 SingleLocalQ13) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	q13.remCustWordsOldOrders = q13.calcHelper(tables, remOrder, remItems, -1)
	q13.done, q13.sameNation = new(bool), false
	return q13
}

func (q13 SingleLocalQ13) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q13.countUpds()
}

func (q13 SingleLocalQ13) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q13.countUpds()
}

//*****Q17*****

func (q17 SingleQ17) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	q17.info = Q17Info{sum: make(map[string]int64), count: make(map[string]int64), pricePerQuantity: make(map[string]map[int8]float64)}
	q17.nUpds = q17.calcHelper(tables, q17.info, newOrder, newItems, 1)
	return q17
}

func (q17 SingleQ17) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	q17.info = Q17Info{sum: make(map[string]int64), count: make(map[string]int64), pricePerQuantity: make(map[string]map[int8]float64)}
	q17.nUpds = q17.calcHelper(tables, q17.info, remOrder, remItems, -1)
	return q17
}

func (q17 SingleQ17) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q17.nUpds, q17.nUpds, getNBlockUpds(q17.nUpds)
}

func (q17 SingleQ17) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q17.nUpds, q17.nUpds, getNBlockUpds(q17.nUpds)
}

func (q17 SingleLocalQ17) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	q17.addQ17.info = Q17Info{sum: make(map[string]int64), count: make(map[string]int64), pricePerQuantity: make(map[string]map[int8]float64)}
	q17.addQ17.nUpds = q17.addQ17.calcHelper(tables, q17.addQ17.info, newOrder, newItems, 1)
	return q17
}

func (q17 SingleLocalQ17) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	q17.remQ17.info = Q17Info{sum: make(map[string]int64), count: make(map[string]int64), pricePerQuantity: make(map[string]map[int8]float64)}
	q17.remQ17.nUpds = q17.remQ17.calcHelper(tables, q17.remQ17.info, remOrder, remItems, -1)
	return q17
}

func (q17 SingleLocalQ17) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q17.addQ17.nUpds, q17.addQ17.nUpds, getNBlockUpds(q17.addQ17.nUpds)
}

func (q17 SingleLocalQ17) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q17.remQ17.nUpds, q17.remQ17.nUpds, getNBlockUpds(q17.remQ17.nUpds)
}

//*****Q19*****

func (q19 SingleQ19) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	q19.q19Map, q19.nUpds, q19.done = make(map[string]map[string]map[int8]float64), 0, new(bool)
	q19.nUpds += q19.calcHelper(tables, newItems, 1)
	return q19
}

func (q19 SingleQ19) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	q19.q19Map, q19.nUpds, q19.done = make(map[string]map[string]map[int8]float64), 0, new(bool)
	q19.nUpds += q19.calcHelper(tables, remItems, -1)
	return q19
}

func (q19 SingleQ19) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q19.nUpds, q19.nUpds, getNBlockUpds(q19.nUpds)
}

func (q19 SingleQ19) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q19.nUpds, q19.nUpds, getNBlockUpds(q19.nUpds)
}

func (q19 SingleLocalQ19) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	q19.addQ19 = SingleQ19{q19Map: make(map[string]map[string]map[int8]float64), nUpds: 0, done: new(bool)}
	q19.nUpds += q19.addQ19.calcHelper(tables, newItems, 1)
	return q19
}

func (q19 SingleLocalQ19) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	q19.remQ19 = SingleQ19{q19Map: make(map[string]map[string]map[int8]float64), nUpds: 0, done: new(bool)}
	q19.nUpds += q19.remQ19.calcHelper(tables, remItems, 1)
	return q19
}

func (q19 SingleLocalQ19) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q19.nUpds, q19.nUpds, getNBlockUpds(q19.nUpds)
}

func (q19 SingleLocalQ19) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q19.nUpds, q19.nUpds, getNBlockUpds(q19.nUpds)
}

//*****Q20*****

func (q20 SingleQ20) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	q20.q20Map, q20.nUpds, q20.done = make(map[int8]map[string]map[PairInt]map[int16]int32, len(tpchData.Tables.Nations)), 0, new(bool)
	q20.nUpds += q20.calcHelper(newItems, 1)
	return q20
}

func (q20 SingleQ20) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	q20.q20Map, q20.nUpds, q20.done = make(map[int8]map[string]map[PairInt]map[int16]int32, len(tpchData.Tables.Nations)), 0, new(bool)
	q20.nUpds += q20.calcHelper(remItems, -1)
	return q20
}

func (q20 SingleQ20) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q20.nUpds, q20.nUpds, getNBlockUpds(q20.nUpds)
}

func (q20 SingleQ20) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q20.nUpds, q20.nUpds, getNBlockUpds(q20.nUpds)
}

func (q20 SingleLocalQ20) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	q20.q20PerReg, q20.nUpds, q20.done = make([]SingleQ20, len(tables.Regions)), 0, new(bool)
	newItemsReg := tables.GetOrderItemsPerSupplier(newItems)
	for i, newItemsR := range newItemsReg {
		if len(newItemsR) > 0 {
			q20.q20PerReg[i] = SingleQ20{}.buildAddInfo(tables, newOrder, newItemsR).(SingleQ20)
			q20.nBlockUpds++
			regNUpds, _, _ := q20.q20PerReg[i].countAddUpds()
			q20.nUpds += regNUpds
		}
	}
	return q20
}

func (q20 SingleLocalQ20) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	q20.q20PerReg, q20.nUpds, q20.done = make([]SingleQ20, len(tables.Regions)), 0, new(bool)
	remItemsReg := tables.GetOrderItemsPerSupplier(remItems)
	for i, remItemsR := range remItemsReg {
		if len(remItemsR) > 0 {
			q20.q20PerReg[i] = SingleQ20{}.buildRemInfo(tables, remOrder, remItemsR).(SingleQ20)
			q20.nBlockUpds++
			regNUpds, _, _ := q20.q20PerReg[i].countRemUpds()
			q20.nUpds += regNUpds
		}
	}
	return q20
}

func (q20 SingleLocalQ20) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q20.nUpds, q20.nUpds, getNBlockUpds(q20.nUpds)
}

func (q20 SingleLocalQ20) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q20.nUpds, q20.nUpds, getNBlockUpds(q20.nUpds)
}

//*****Q21*****

func (q21 SingleQ21) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	q21.addSupId = q21.calcHelper(newItems)
	return q21
}

func (q21 SingleQ21) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	q21.remSupId = q21.calcHelper(remItems)
	return nil
}

func (q21 SingleQ21) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	if q21.addSupId != -1 {
		return 1, 1, 1
	}
	return 0, 0, 0
}

func (q21 SingleQ21) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	if q21.remSupId != -1 {
		return 1, 1, 1
	}
	return 0, 0, 0
}

//*****Q22*****

func (q22 SingleQ22) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	q22.incCustId = newOrder.O_CUSTKEY
	q22.incPhoneId = tables.OrderToNationkey(newOrder) + 10
	return q22
}

func (q22 SingleQ22) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	q22.decCustId = remOrder.O_CUSTKEY
	q22.decPhoneId = tables.OrderToNationkey(remOrder) + 10
	return nil
}

func (q22 SingleQ22) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return 1, 1, 1
}

func (q22 SingleQ22) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return 1, 1, 1
}

//*****Q3*****

func (q3 SingleQ3) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	if newOrder.O_ORDERDATE.IsLowerOrEqual(MAX_DATE_Q3) {
		//Need to do add
		q3.newsCalcHelper(newOrder, newItems)
		q3.nUpds = int(MAX_MONTH_DAY - q3.updStartPos + 1)
	}
	q3.newSeg = tables.Customers[newOrder.O_CUSTKEY].C_MKTSEGMENT
	return q3
}

func (q3 SingleQ3) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	if remOrder.O_ORDERDATE.IsLowerOrEqual(MAX_DATE_Q3) {
		//Need to do rem
		q3.remsCalcHelper(remOrder, remItems)
		q3.nUpds = len(q3.remMap)
	}
	q3.oldSeg = tables.Customers[remOrder.O_CUSTKEY].C_MKTSEGMENT
	return q3
}

func (q3 SingleQ3) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q3.nUpds, q3.nUpds, getNBlockUpds(q3.nUpds)
}

func (q3 SingleQ3) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q3.nUpds, q3.nUpds, getNBlockUpds(q3.nUpds)
}

//*****Q5*****

func (q5 SingleQ5) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	q5.updValue = q5.updsCalcHelper(tables, newOrder, newItems)
	return q5
}

func (q5 SingleQ5) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	q5.remValue = q5.updsCalcHelper(tables, remOrder, remItems)
	return q5
}

func (q5 SingleQ5) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	if *q5.updValue != 0 {
		return 1, 1, 1
	}
	return 0, 0, 0
}

func (q5 SingleQ5) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	if *q5.remValue != 0 {
		return 1, 1, 1
	}
	return 0, 0, 0
}

//*****Q14*****

type SingleSplitQ14 struct {
	SingleQ14
}

func (q14 SingleSplitQ14) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	q14.mapPromo, q14.mapTotal = make(map[string]*float64), make(map[string]*float64)
	q14.updsCalcHelper(1, tables, newOrder, newItems)
	return q14
}

func (q14 SingleSplitQ14) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	q14.mapPromo, q14.mapTotal = make(map[string]*float64), make(map[string]*float64)
	q14.updsCalcHelper(-1, tables, remOrder, remItems)
	return q14
}

func (q14 SingleSplitQ14) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q14.SingleQ14.makeNewUpdArgs(tables, newOrder, buf, bufI, bucketI)
}

func (q14 SingleSplitQ14) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q14.SingleQ14.makeNewUpdArgs(tables, remOrder, buf, bufI, bucketI)
}

func (q14 SingleSplitQ14) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	size := len(q14.mapTotal)
	return size, size, getNBlockUpds(size)
}

func (q14 SingleSplitQ14) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	size := len(q14.mapTotal)
	return size, size, getNBlockUpds(size)
}

func (q14 SingleLocalQ14) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	q14.newQ14.mapPromo, q14.newQ14.mapTotal = make(map[string]*float64), make(map[string]*float64)
	q14.newQ14.updsCalcHelper(1, tables, newOrder, newItems)
	return q14
}

func (q14 SingleLocalQ14) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	q14.remQ14.mapPromo, q14.remQ14.mapTotal = make(map[string]*float64), make(map[string]*float64)
	q14.remQ14.updsCalcHelper(-1, tables, remOrder, remItems)
	return q14
}

func (q14 SingleLocalQ14) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q14.newQ14.countUpds()
}

func (q14 SingleLocalQ14) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q14.remQ14.countUpds()
}

//*****Q15*****

type SingleSplitQ15 struct {
	SingleQ15
}

func (q15 SingleSplitQ15) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	updEntries := createQ15EntriesMap()
	q15UpdsNewCalcHelper(newItems, q15Map, updEntries)
	return SingleSplitQ15{SingleQ15: SingleQ15{updEntries: updEntries}}
}

func (q15 SingleSplitQ15) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	updEntries := createQ15EntriesMap()
	q15UpdsRemCalcHelper(remItems, q15Map, updEntries)
	return SingleSplitQ15{SingleQ15: SingleQ15{updEntries: updEntries}}
}

func (q15 SingleSplitQ15) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q15.SingleQ15.makeNewUpdArgs(tables, newOrder, buf, bufI, bucketI)
}

func (q15 SingleSplitQ15) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q15.SingleQ15.makeNewUpdArgs(tables, remOrder, buf, bufI, bucketI)
}

func (q15 SingleSplitQ15) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	if !useTopKAll {
		for _, monthMap := range q15.updEntries {
			for _, suppMap := range monthMap {
				nUpds += len(suppMap)
			}
		}
		//fmt.Printf("[Q15]nUpds: %d, nUpdsStats: %d, nBlockUpds: %d\n", nUpds, nUpds, getNBlockUpds(nUpds))
		return nUpds, nUpds, getNBlockUpds(nUpds)
	} else {
		found := 0
		for year, monthMap := range q15.updEntries {
			for month, suppMap := range monthMap {
				if len(suppMap) == 1 {
					nUpds++
					nUpdsStats++
				} else {
					found = len(q15Map[year][month])
					nUpdsStats += found
					if found > 0 {
						nUpds++
						found = 0
					}
				}
			}
		}
	}
	return nUpds, nUpdsStats, getNBlockUpds(nUpds)
}

func (q15 SingleSplitQ15) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q15.countAddUpds()
}

func (q15 SingleLocalQ15) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	updKey := tables.OrderToRegionkey(newOrder)
	updEntries := createQ15EntriesMap()
	q15UpdsNewCalcHelper(newItems, q15LocalMap[updKey], updEntries)
	return SingleLocalQ15{newQ15: SingleQ15{updEntries: updEntries}}
}

func (q15 SingleLocalQ15) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	remKey := tables.OrderToRegionkey(remOrder)
	remEntries := createQ15EntriesMap()
	q15UpdsRemCalcHelper(remItems, q15LocalMap[remKey], remEntries)
	return SingleLocalQ15{remQ15: SingleQ15{updEntries: remEntries}}
}

func (q15 SingleLocalQ15) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q15.newQ15.countUpds()
}

func (q15 SingleLocalQ15) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q15.remQ15.countUpds()
}

type SingleSplitQ15TopSum struct {
	SingleQ15TopSum
}

func (q15 SingleSplitQ15TopSum) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	diffEntries := createQ15TopSumEntriesMap()
	q15TopSumUpdsNewCalcHelper(newItems, q15Map, diffEntries)
	return SingleSplitQ15TopSum{SingleQ15TopSum: SingleQ15TopSum{diffEntries: diffEntries}}
}

func (q15 SingleSplitQ15TopSum) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	diffEntries := createQ15TopSumEntriesMap()
	q15TopSumUpdsRemCalcHelper(remItems, q15Map, diffEntries)
	return SingleSplitQ15TopSum{SingleQ15TopSum: SingleQ15TopSum{diffEntries: diffEntries}}
}

func (q15 SingleSplitQ15TopSum) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q15.SingleQ15TopSum.makeNewUpdArgs(tables, newOrder, buf, bufI, bucketI)
}

func (q15 SingleSplitQ15TopSum) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []crdt.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q15.SingleQ15TopSum.makeNewUpdArgs(tables, remOrder, buf, bufI, bucketI)
}

func (q15 SingleSplitQ15TopSum) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	if !useTopKAll {
		for _, monthMap := range q15.diffEntries {
			for _, suppMap := range monthMap {
				nUpds += len(suppMap)
			}
		}
		//fmt.Printf("[Q15]nUpds: %d, nUpdsStats: %d, nBlockUpds: %d\n", nUpds, nUpds, getNBlockUpds(nUpds))
		return nUpds, nUpds, getNBlockUpds(nUpds)
	} else {
		found := 0
		for year, monthMap := range q15.diffEntries {
			for month, suppMap := range monthMap {
				if len(suppMap) == 1 {
					nUpds++
					nUpdsStats++
				} else {
					found = len(q15Map[year][month])
					nUpdsStats += found
					if found > 0 {
						nUpds++
						found = 0
					}
				}
			}
		}
	}
	return nUpds, nUpdsStats, getNBlockUpds(nUpds)
}

func (q15 SingleSplitQ15TopSum) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q15.countAddUpds()
}

func (q15 SingleLocalQ15TopSum) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	updKey := tables.OrderToRegionkey(newOrder)
	updEntries := createQ15TopSumEntriesMap()
	q15TopSumUpdsNewCalcHelper(newItems, q15LocalMap[updKey], updEntries)
	return SingleLocalQ15TopSum{newQ15: SingleQ15TopSum{diffEntries: updEntries}}
}

func (q15 SingleLocalQ15TopSum) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	remKey := tables.OrderToRegionkey(remOrder)
	remEntries := createQ15TopSumEntriesMap()
	q15TopSumUpdsRemCalcHelper(remItems, q15LocalMap[remKey], remEntries)
	return SingleLocalQ15TopSum{remQ15: SingleQ15TopSum{diffEntries: remEntries}}
}

func (q15 SingleLocalQ15TopSum) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q15.newQ15.countUpds()
}

func (q15 SingleLocalQ15TopSum) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	return q15.remQ15.countUpds()
}

//*****Q18*****

func (q18 SingleQ18) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	for _, item := range newItems {
		q18.updQuantity += int64(item.L_QUANTITY)
	}
	return q18
}

func (q18 SingleQ18) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	for _, item := range remItems {
		q18.remQuantity += int64(item.L_QUANTITY)
	}
	return q18
}

func (q18 SingleQ18) countAddUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	nUpds = q18.countUpdsHelper(q18.updQuantity)
	return nUpds, nUpds, getNBlockUpds(nUpds)
}

func (q18 SingleQ18) countRemUpds() (nUpds, nUpdsStats, nBlockUpds int) {
	nUpds = q18.countUpdsHelper(q18.remQuantity)
	return nUpds, nUpds, getNBlockUpds(nUpds)
}

//*****Making lists of index infos*****

func makeAddIndexInfos(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) (infos []SingleSplitQueryInfo) {
	if !UPDATE_SPECIFIC_INDEX_ONLY {
		return makeAddIndexInfosFull(tables, newOrder, newItems)
	}
	return makeAddIndexInfosFromList(tables, indexesToUpd, newOrder, newItems)
}

func makeAddIndexInfosFull(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) (infos []SingleSplitQueryInfo) {
	infos = make([]SingleSplitQueryInfo, 19)
	infos[1] = SingleQ3{}.buildAddInfo(tables, newOrder, newItems)
	infos[2] = SingleQ4{}.buildAddInfo(tables, newOrder, newItems)
	infos[3] = SingleQ5{}.buildAddInfo(tables, newOrder, newItems)
	infos[8] = SingleQ10{}.buildAddInfo(tables, newOrder, newItems)
	infos[13] = SingleQ17{}.buildAddInfo(tables, newOrder, newItems)
	infos[14] = SingleQ18{}.buildAddInfo(tables, newOrder, newItems)
	infos[17] = SingleQ21{}.buildAddInfo(tables, newOrder, newItems)
	infos[18] = SingleQ22{}.buildAddInfo(tables, newOrder, newItems)
	if isIndexGlobal {
		infos[0] = SingleQ1{}.buildAddInfo(tables, newOrder, newItems)
		infos[4] = SingleQ6{}.buildAddInfo(tables, newOrder, newItems)
		infos[5] = SingleQ7{}.buildAddInfo(tables, newOrder, newItems)
		infos[6] = SingleQ8{}.buildAddInfo(tables, newOrder, newItems)
		infos[7] = SingleSplitQ9{}.buildAddInfo(tables, newOrder, newItems)
		infos[9] = SingleQ12{}.buildAddInfo(tables, newOrder, newItems)
		infos[10] = SingleQ13{}.buildAddInfo(tables, newOrder, newItems)
		infos[11] = SingleSplitQ14{}.buildAddInfo(tables, newOrder, newItems)
		if useTopSum {
			infos[12] = SingleSplitQ15TopSum{}.buildAddInfo(tables, newOrder, newItems)
		} else {
			infos[12] = SingleSplitQ15{}.buildAddInfo(tables, newOrder, newItems)
		}
		infos[15] = SingleQ19{}.buildAddInfo(tables, newOrder, newItems)
		infos[16] = SingleQ20{}.buildAddInfo(tables, newOrder, newItems)
	} else {
		infos[0] = SingleLocalQ1{}.buildAddInfo(tables, newOrder, newItems)
		infos[4] = SingleLocalQ6{}.buildAddInfo(tables, newOrder, newItems)
		infos[5] = SingleLocalQ7{}.buildAddInfo(tables, newOrder, newItems)
		infos[6] = SingleLocalQ8{}.buildAddInfo(tables, newOrder, newItems)
		infos[7] = SingleSplitLocalQ9{}.buildAddInfo(tables, newOrder, newItems)
		infos[9] = SingleLocalQ12{}.buildAddInfo(tables, newOrder, newItems)
		infos[10] = SingleLocalQ13{}.buildAddInfo(tables, newOrder, newItems)
		infos[11] = SingleLocalQ14{}.buildAddInfo(tables, newOrder, newItems)
		if useTopSum {
			infos[12] = SingleLocalQ15TopSum{}.buildAddInfo(tables, newOrder, newItems)
		} else {
			infos[12] = SingleLocalQ15{}.buildAddInfo(tables, newOrder, newItems)
		}
		infos[15] = SingleLocalQ19{}.buildAddInfo(tables, newOrder, newItems)
		infos[16] = SingleLocalQ20{}.buildAddInfo(tables, newOrder, newItems)
	}
	return
}

func makeAddIndexInfosFromList(tables *tpch.Tables, indexes []int, newOrder *tpch.Orders, newItems []*tpch.LineItem) (infos []SingleSplitQueryInfo) {
	infos = make([]SingleSplitQueryInfo, len(indexes))
	if isIndexGlobal {
		for i, queryN := range indexes {
			switch queryN {
			case 1:
				infos[i] = SingleQ1{}.buildAddInfo(tables, newOrder, newItems)
			case 3:
				infos[i] = SingleQ3{}.buildAddInfo(tables, newOrder, newItems)
			case 4:
				infos[i] = SingleQ4{}.buildAddInfo(tables, newOrder, newItems)
			case 5:
				infos[i] = SingleQ5{}.buildAddInfo(tables, newOrder, newItems)
			case 6:
				infos[i] = SingleQ6{}.buildAddInfo(tables, newOrder, newItems)
			case 7:
				infos[i] = SingleQ7{}.buildAddInfo(tables, newOrder, newItems)
			case 8:
				infos[i] = SingleQ8{}.buildAddInfo(tables, newOrder, newItems)
			case 9:
				infos[i] = SingleSplitQ9{}.buildAddInfo(tables, newOrder, newItems)
			case 10:
				infos[i] = SingleQ10{}.buildAddInfo(tables, newOrder, newItems)
			case 12:
				infos[i] = SingleQ12{}.buildAddInfo(tables, newOrder, newItems)
			case 13:
				infos[i] = SingleQ13{}.buildAddInfo(tables, newOrder, newItems)
			case 14:
				infos[i] = SingleSplitQ14{}.buildAddInfo(tables, newOrder, newItems)
			case 15:
				if useTopSum {
					infos[i] = SingleSplitQ15TopSum{}.buildAddInfo(tables, newOrder, newItems)
				} else {
					infos[i] = SingleSplitQ15{}.buildAddInfo(tables, newOrder, newItems)
				}
			case 17:
				infos[i] = SingleQ17{}.buildAddInfo(tables, newOrder, newItems)
			case 18:
				infos[i] = SingleQ18{}.buildAddInfo(tables, newOrder, newItems)
			case 19:
				infos[i] = SingleQ19{}.buildAddInfo(tables, newOrder, newItems)
			case 20:
				infos[i] = SingleQ20{}.buildAddInfo(tables, newOrder, newItems)
			case 21:
				infos[i] = SingleQ21{}.buildAddInfo(tables, newOrder, newItems)
			case 22:
				infos[i] = SingleQ22{}.buildAddInfo(tables, newOrder, newItems)
			}
		}
	} else {
		for i, queryN := range indexes {
			switch queryN {
			case 1:
				infos[i] = SingleLocalQ1{}.buildAddInfo(tables, newOrder, newItems)
			case 3:
				infos[i] = SingleQ3{}.buildAddInfo(tables, newOrder, newItems)
			case 4:
				infos[i] = SingleQ4{}.buildAddInfo(tables, newOrder, newItems)
			case 5:
				infos[i] = SingleQ5{}.buildAddInfo(tables, newOrder, newItems)
			case 6:
				infos[i] = SingleLocalQ6{}.buildAddInfo(tables, newOrder, newItems)
			case 7:
				infos[i] = SingleLocalQ7{}.buildAddInfo(tables, newOrder, newItems)
			case 8:
				infos[i] = SingleLocalQ8{}.buildAddInfo(tables, newOrder, newItems)
			case 9:
				infos[i] = SingleSplitLocalQ9{}.buildAddInfo(tables, newOrder, newItems)
			case 10:
				infos[i] = SingleQ10{}.buildAddInfo(tables, newOrder, newItems)
			case 12:
				infos[i] = SingleLocalQ12{}.buildAddInfo(tables, newOrder, newItems)
			case 13:
				infos[i] = SingleLocalQ13{}.buildAddInfo(tables, newOrder, newItems)
			case 14:
				infos[i] = SingleLocalQ14{}.buildAddInfo(tables, newOrder, newItems)
			case 15:
				if useTopSum {
					infos[i] = SingleLocalQ15TopSum{}.buildAddInfo(tables, newOrder, newItems)
				} else {
					infos[i] = SingleLocalQ15{}.buildAddInfo(tables, newOrder, newItems)
				}
			case 17:
				infos[i] = SingleQ17{}.buildAddInfo(tables, newOrder, newItems)
			case 18:
				infos[i] = SingleQ18{}.buildAddInfo(tables, newOrder, newItems)
			case 19:
				infos[i] = SingleLocalQ19{}.buildAddInfo(tables, newOrder, newItems)
			case 20:
				infos[i] = SingleLocalQ20{}.buildAddInfo(tables, newOrder, newItems)
			case 21:
				infos[i] = SingleQ21{}.buildAddInfo(tables, newOrder, newItems)
			case 22:
				infos[i] = SingleQ22{}.buildAddInfo(tables, newOrder, newItems)
			}
		}
	}
	return
}

/*
func makeAddIndexInfosFull(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) (infos []SingleSplitQueryInfo) {
	infos = make([]SingleSplitQueryInfo, 5)
	infos[0] = SingleQ3{}.buildAddInfo(tables, newOrder, newItems)
	infos[1] = SingleQ5{}.buildAddInfo(tables, newOrder, newItems)
	infos[4] = SingleQ18{}.buildAddInfo(tables, newOrder, newItems)
	if isIndexGlobal {
		infos[2] = SingleSplitQ14{}.buildAddInfo(tables, newOrder, newItems)
		if !useTopSum {
			infos[3] = SingleSplitQ15{}.buildAddInfo(tables, newOrder, newItems)
		} else {
			infos[3] = SingleSplitQ15TopSum{}.buildAddInfo(tables, newOrder, newItems)
		}
	} else {
		infos[2] = SingleLocalQ14{}.buildAddInfo(tables, newOrder, newItems)
		if !useTopSum {
			infos[3] = SingleLocalQ15{}.buildAddInfo(tables, newOrder, newItems)
		} else {
			infos[3] = SingleLocalQ15TopSum{}.buildAddInfo(tables, newOrder, newItems)
		}
	}
	return
}

func makeAddIndexInfosFromList(tables *tpch.Tables, indexes []int, newOrder *tpch.Orders, newItems []*tpch.LineItem) (infos []SingleSplitQueryInfo) {
	infos = make([]SingleSplitQueryInfo, len(indexes))
	for i, queryN := range indexes {
		switch queryN {
		case 3:
			infos[i] = SingleQ3{}.buildAddInfo(tables, newOrder, newItems)
		case 5:
			infos[i] = SingleQ5{}.buildAddInfo(tables, newOrder, newItems)
		case 14:
			if isIndexGlobal {
				infos[i] = SingleSplitQ14{}.buildAddInfo(tables, newOrder, newItems)
			} else {
				infos[i] = SingleLocalQ14{}.buildAddInfo(tables, newOrder, newItems)
			}
		case 15:
			if isIndexGlobal {
				if !useTopSum {
					infos[i] = SingleSplitQ15{}.buildAddInfo(tables, newOrder, newItems)
				} else {
					infos[i] = SingleSplitQ15TopSum{}.buildAddInfo(tables, newOrder, newItems)
				}
			} else {
				if !useTopSum {
					infos[i] = SingleLocalQ15{}.buildAddInfo(tables, newOrder, newItems)
				} else {
					infos[i] = SingleLocalQ15TopSum{}.buildAddInfo(tables, newOrder, newItems)
				}
			}
		case 18:
			infos[i] = SingleQ18{}.buildAddInfo(tables, newOrder, newItems)
		}
	}
	return
}
*/

func countAddsIndexInfo(infos []SingleSplitQueryInfo) (nUpds, nUpdsStats, nBlockUpds int) {
	currNUpds, currNUpdsStats, currNBlockUpds := 0, 0, 0
	for _, info := range infos {
		currNUpds, currNUpdsStats, currNBlockUpds = info.countAddUpds()
		nUpds, nUpdsStats, nBlockUpds = nUpds+currNUpds, nUpdsStats+currNUpdsStats, nBlockUpds+currNBlockUpds
	}
	return
}

func makeRemIndexInfos(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) (infos []SingleSplitQueryInfo) {
	if !UPDATE_SPECIFIC_INDEX_ONLY {
		return makeRemIndexInfosFull(tables, remOrder, remItems)
	}
	return makeRemIndexInfosFromList(tables, indexesToUpd, remOrder, remItems)
}

func makeRemIndexInfosFull(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) (infos []SingleSplitQueryInfo) {
	infos = make([]SingleSplitQueryInfo, 19)
	infos[1] = SingleQ3{}.buildRemInfo(tables, remOrder, remItems)
	infos[2] = SingleQ4{}.buildRemInfo(tables, remOrder, remItems)
	infos[3] = SingleQ5{}.buildRemInfo(tables, remOrder, remItems)
	infos[8] = SingleQ10{}.buildRemInfo(tables, remOrder, remItems)
	infos[13] = SingleQ17{}.buildRemInfo(tables, remOrder, remItems)
	infos[14] = SingleQ18{}.buildRemInfo(tables, remOrder, remItems)
	infos[17] = SingleQ21{}.buildRemInfo(tables, remOrder, remItems)
	infos[18] = SingleQ22{}.buildRemInfo(tables, remOrder, remItems)
	if isIndexGlobal {
		infos[0] = SingleQ1{}.buildRemInfo(tables, remOrder, remItems)
		infos[4] = SingleQ6{}.buildRemInfo(tables, remOrder, remItems)
		infos[5] = SingleQ7{}.buildRemInfo(tables, remOrder, remItems)
		infos[6] = SingleQ8{}.buildRemInfo(tables, remOrder, remItems)
		infos[7] = SingleSplitQ9{}.buildRemInfo(tables, remOrder, remItems)
		infos[9] = SingleQ12{}.buildRemInfo(tables, remOrder, remItems)
		infos[10] = SingleQ13{}.buildRemInfo(tables, remOrder, remItems)
		infos[11] = SingleSplitQ14{}.buildRemInfo(tables, remOrder, remItems)
		infos[12] = SingleSplitQ15{}.buildRemInfo(tables, remOrder, remItems)
		infos[15] = SingleQ19{}.buildRemInfo(tables, remOrder, remItems)
		infos[16] = SingleQ20{}.buildRemInfo(tables, remOrder, remItems)
	} else {
		infos[0] = SingleLocalQ1{}.buildRemInfo(tables, remOrder, remItems)
		infos[4] = SingleLocalQ6{}.buildRemInfo(tables, remOrder, remItems)
		infos[5] = SingleLocalQ7{}.buildRemInfo(tables, remOrder, remItems)
		infos[6] = SingleLocalQ8{}.buildRemInfo(tables, remOrder, remItems)
		infos[7] = SingleSplitLocalQ9{}.buildRemInfo(tables, remOrder, remItems)
		infos[9] = SingleLocalQ12{}.buildRemInfo(tables, remOrder, remItems)
		infos[10] = SingleLocalQ13{}.buildRemInfo(tables, remOrder, remItems)
		infos[11] = SingleLocalQ14{}.buildRemInfo(tables, remOrder, remItems)
		infos[12] = SingleLocalQ15{}.buildRemInfo(tables, remOrder, remItems)
		infos[15] = SingleLocalQ19{}.buildRemInfo(tables, remOrder, remItems)
		infos[16] = SingleLocalQ20{}.buildRemInfo(tables, remOrder, remItems)
	}
	return
}

func makeRemIndexInfosFromList(tables *tpch.Tables, indexes []int, remOrder *tpch.Orders, remItems []*tpch.LineItem) (infos []SingleSplitQueryInfo) {
	infos = make([]SingleSplitQueryInfo, len(indexes))
	if isIndexGlobal {
		for i, queryN := range indexes {
			switch queryN {
			case 1:
				infos[i] = SingleQ1{}.buildRemInfo(tables, remOrder, remItems)
			case 3:
				infos[i] = SingleQ3{}.buildRemInfo(tables, remOrder, remItems)
			case 4:
				infos[i] = SingleQ4{}.buildRemInfo(tables, remOrder, remItems)
			case 5:
				infos[i] = SingleQ5{}.buildRemInfo(tables, remOrder, remItems)
			case 6:
				infos[i] = SingleQ6{}.buildRemInfo(tables, remOrder, remItems)
			case 7:
				infos[i] = SingleQ7{}.buildRemInfo(tables, remOrder, remItems)
			case 8:
				infos[i] = SingleQ8{}.buildRemInfo(tables, remOrder, remItems)
			case 9:
				infos[i] = SingleSplitQ9{}.buildRemInfo(tables, remOrder, remItems)
			case 10:
				infos[i] = SingleQ10{}.buildRemInfo(tables, remOrder, remItems)
			case 12:
				infos[i] = SingleQ12{}.buildRemInfo(tables, remOrder, remItems)
			case 13:
				infos[i] = SingleQ13{}.buildRemInfo(tables, remOrder, remItems)
			case 14:
				infos[i] = SingleSplitQ14{}.buildRemInfo(tables, remOrder, remItems)
			case 15:
				infos[i] = SingleSplitQ15{}.buildRemInfo(tables, remOrder, remItems)
			case 17:
				infos[i] = SingleQ17{}.buildRemInfo(tables, remOrder, remItems)
			case 18:
				infos[i] = SingleQ18{}.buildRemInfo(tables, remOrder, remItems)
			case 19:
				infos[i] = SingleQ19{}.buildRemInfo(tables, remOrder, remItems)
			case 20:
				infos[i] = SingleQ20{}.buildRemInfo(tables, remOrder, remItems)
			case 21:
				infos[i] = SingleQ21{}.buildRemInfo(tables, remOrder, remItems)
			case 22:
				infos[i] = SingleQ22{}.buildRemInfo(tables, remOrder, remItems)
			}
		}
	} else {
		for i, queryN := range indexes {
			switch queryN {
			case 1:
				infos[i] = SingleLocalQ1{}.buildRemInfo(tables, remOrder, remItems)
			case 3:
				infos[i] = SingleQ3{}.buildRemInfo(tables, remOrder, remItems)
			case 4:
				infos[i] = SingleQ4{}.buildRemInfo(tables, remOrder, remItems)
			case 5:
				infos[i] = SingleQ5{}.buildRemInfo(tables, remOrder, remItems)
			case 6:
				infos[i] = SingleLocalQ6{}.buildRemInfo(tables, remOrder, remItems)
			case 7:
				infos[i] = SingleLocalQ7{}.buildRemInfo(tables, remOrder, remItems)
			case 8:
				infos[i] = SingleLocalQ8{}.buildRemInfo(tables, remOrder, remItems)
			case 9:
				infos[i] = SingleSplitLocalQ9{}.buildRemInfo(tables, remOrder, remItems)
			case 10:
				infos[i] = SingleQ10{}.buildRemInfo(tables, remOrder, remItems)
			case 12:
				infos[i] = SingleLocalQ12{}.buildRemInfo(tables, remOrder, remItems)
			case 13:
				infos[i] = SingleLocalQ13{}.buildRemInfo(tables, remOrder, remItems)
			case 14:
				infos[i] = SingleLocalQ14{}.buildRemInfo(tables, remOrder, remItems)
			case 15:
				infos[i] = SingleLocalQ15{}.buildRemInfo(tables, remOrder, remItems)
			case 17:
				infos[i] = SingleQ17{}.buildRemInfo(tables, remOrder, remItems)
			case 18:
				infos[i] = SingleQ18{}.buildRemInfo(tables, remOrder, remItems)
			case 19:
				infos[i] = SingleLocalQ19{}.buildRemInfo(tables, remOrder, remItems)
			case 20:
				infos[i] = SingleLocalQ20{}.buildRemInfo(tables, remOrder, remItems)
			case 21:
				infos[i] = SingleQ21{}.buildRemInfo(tables, remOrder, remItems)
			case 22:
				infos[i] = SingleQ22{}.buildRemInfo(tables, remOrder, remItems)
			}
		}
	}
	return
}

/*
func makeRemIndexInfosFull(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) (infos []SingleSplitQueryInfo) {
	infos = make([]SingleSplitQueryInfo, 5)
	infos[0] = SingleQ3{}.buildRemInfo(tables, remOrder, remItems)
	infos[1] = SingleQ5{}.buildRemInfo(tables, remOrder, remItems)
	infos[4] = SingleQ18{}.buildRemInfo(tables, remOrder, remItems)
	if isIndexGlobal {
		infos[2] = SingleSplitQ14{}.buildRemInfo(tables, remOrder, remItems)
		if !useTopSum {
			infos[3] = SingleSplitQ15{}.buildRemInfo(tables, remOrder, remItems)
		} else {
			infos[3] = SingleSplitQ15TopSum{}.buildRemInfo(tables, remOrder, remItems)
		}
	} else {
		infos[2] = SingleLocalQ14{}.buildRemInfo(tables, remOrder, remItems)
		if !useTopSum {
			infos[3] = SingleLocalQ15{}.buildRemInfo(tables, remOrder, remItems)
		} else {
			infos[3] = SingleLocalQ15TopSum{}.buildRemInfo(tables, remOrder, remItems)
		}
	}
	return
}

func makeRemIndexInfosFromList(tables *tpch.Tables, indexes []int, remOrder *tpch.Orders, remItems []*tpch.LineItem) (infos []SingleSplitQueryInfo) {
	infos = make([]SingleSplitQueryInfo, len(indexes))
	for i, queryN := range indexes {
		switch queryN {
		case 3:
			infos[i] = SingleQ3{}.buildRemInfo(tables, remOrder, remItems)
		case 5:
			infos[i] = SingleQ5{}.buildRemInfo(tables, remOrder, remItems)
		case 14:
			if isIndexGlobal {
				infos[i] = SingleSplitQ14{}.buildRemInfo(tables, remOrder, remItems)
			} else {
				infos[i] = SingleLocalQ14{}.buildRemInfo(tables, remOrder, remItems)
			}
		case 15:
			if isIndexGlobal {
				if !useTopSum {
					infos[i] = SingleSplitQ15{}.buildRemInfo(tables, remOrder, remItems)
				} else {
					infos[i] = SingleSplitQ15TopSum{}.buildRemInfo(tables, remOrder, remItems)
				}
			} else {
				if !useTopSum {
					infos[i] = SingleLocalQ15{}.buildRemInfo(tables, remOrder, remItems)
				} else {
					infos[i] = SingleLocalQ15TopSum{}.buildRemInfo(tables, remOrder, remItems)
				}
			}
		case 18:
			infos[i] = SingleQ18{}.buildRemInfo(tables, remOrder, remItems)
		}
	}
	return
}
*/

func countRemsIndexInfo(infos []SingleSplitQueryInfo) (nUpds, nUpdsStats, nBlockUpds int) {
	currNUpds, currNUpdsStats, currNBlockUpds := 0, 0, 0
	for _, info := range infos {
		currNUpds, currNUpdsStats, currNBlockUpds = info.countRemUpds()
		nUpds, nUpdsStats, nBlockUpds = nUpds+currNUpds, nUpdsStats+currNUpdsStats, nBlockUpds+currNBlockUpds
	}
	return
}
