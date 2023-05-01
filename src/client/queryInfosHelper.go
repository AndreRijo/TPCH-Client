package client

import (
	"potionDB/src/antidote"
	"potionDB/tpch_helper"
)

//Contains all the methods for building only part of the query info (add or remove)

//Repeating other methods so that this interface has access to all.
type SingleSplitQueryInfo interface {
	buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo
	buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo
	countAddUpds() (nUpds, nUpdsStats, nBlockUpds int)
	countRemUpds() (nUpds, nUpdsStats, nBlockUpds int)
	makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int)
	makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int)
}

//TODO: I may need a new Q14 that has a Q14 inside and replaces the makeRemUpdArgs

//*****SingleQ3, Q3*****

func (q3 SingleQ3) buildAddInfo(tables *tpch.Tables, newOrder *tpch.Orders, newItems []*tpch.LineItem) SingleSplitQueryInfo {
	if newOrder.O_ORDERDATE.IsSmallerOrEqual(MAX_DATE_Q3) {
		//Need to do add
		q3.newsCalcHelper(newOrder, newItems)
		q3.nUpds = int(MAX_MONTH_DAY - q3.updStartPos + 1)
	}
	q3.newSeg = tables.Customers[newOrder.O_CUSTKEY].C_MKTSEGMENT
	return q3
}

func (q3 SingleQ3) buildRemInfo(tables *tpch.Tables, remOrder *tpch.Orders, remItems []*tpch.LineItem) SingleSplitQueryInfo {
	if remOrder.O_ORDERDATE.IsSmallerOrEqual(MAX_DATE_Q3) {
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

func (q14 SingleSplitQ14) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q14.SingleQ14.makeNewUpdArgs(tables, newOrder, buf, bufI, bucketI)
}

func (q14 SingleSplitQ14) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
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

func (q15 SingleSplitQ15) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q15.SingleQ15.makeNewUpdArgs(tables, newOrder, buf, bufI, bucketI)
}

func (q15 SingleSplitQ15) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
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

func (q15 SingleSplitQ15TopSum) makeNewUpdArgs(tables *tpch.Tables, newOrder *tpch.Orders, buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
	return q15.SingleQ15TopSum.makeNewUpdArgs(tables, newOrder, buf, bufI, bucketI)
}

func (q15 SingleSplitQ15TopSum) makeRemUpdArgs(tables *tpch.Tables, remOrder *tpch.Orders, buf []antidote.UpdateObjectParams, bufI int, bucketI int) (newBufI int) {
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

func countRemsIndexInfo(infos []SingleSplitQueryInfo) (nUpds, nUpdsStats, nBlockUpds int) {
	currNUpds, currNUpdsStats, currNBlockUpds := 0, 0, 0
	for _, info := range infos {
		currNUpds, currNUpdsStats, currNBlockUpds = info.countRemUpds()
		nUpds, nUpdsStats, nBlockUpds = nUpds+currNUpds, nUpdsStats+currNUpdsStats, nBlockUpds+currNBlockUpds
	}
	return
}
