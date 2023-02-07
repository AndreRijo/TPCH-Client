package main

import (
	"tpch_client/src/autoindex"
	"tpch_client/src/client"
)

/**
Used:
Q3: SELECT: L_ORDERKEY, L_EXTENDEDPRICE, L_DISCOUNT; O_ORDERDATE, O_SHIPPRIORITY
	WHERE: C_MKTSEGMENT, C_CUSTKEY, L_ORDERDATE, L_SHIPDATE

Q5: SELECT: N_NAME, L_EXTENDEDPRICE, L_DISCOUNT
	WHERE: C_CUSTKEY, O_CUSTKEY, L_ORDERKEY, O_ORDERKEY, L_SUPPKEY, S_SUPPKEY, C_NATIONKEY, S_NATIONKEY,
	N_REGIONKEY, R_REGIONKEY, R_NAME, O_ORDERDATE

Q11: SELECT: PS_PARTKEY, PS_SUPPLYCOST, PS_AVAILQTY
	 WHERE: PS_SUPPKEY, S_SUPPKEY, S_NATIONKEY, N_NATIONKEY, N_NAME

Q14: SELECT: P_TYPE, L_EXTENDEDPRICE, L_DISCOUNT
	 WHERE: L_PARTKEY, P_PARTKEY, L_SHIPDATE

Q15: SELECT: L_SUPPKEY, L_EXTENDEDPRICE, L_DISCOUNT
	 WHERE: L_SHIPDATE

Q18: SELCT: C_NAME, C_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE, L_QUANTITY
	 WHERE: O_ORDERKEY, L_ORDERKEY, L_QUANTITY, C_CUSTKEY, O_CUSTKEY

-----

Q4:	SELECT: O_ORDERPRIORITY
	WHERE: O_ORDERDATE, L_ORDERKEY, L_COMMITDATE

----

With small; VM, Repl, Log disabled: 1022MB
With small; VM, disabled: 1644MB
With small; VM, disabled, replicaID of 2 bytes intead of 8: 1604MB
*/

//Clause 5.3.7.7: one pair of refresh functions per query stream.
//I.e., if there's only 1 query client, then each refresh function is only executed once.
//Scheduling of the refresh functions is left to the tester.
//Theoretically, according to 5.3.7.10, the update data could be stored in the DB before the benchmark begins

//TODO: Possibly support TopK returning results sorted?

/*
	Stuff todo:
	a) lineitems should be replicated in two servers (DONE, but may incour in unecessary updates when running with single replica)
	b) need to support updates; including updating the indexes (done? Still need to integrate a bit better.)
	c) [NEXT] topk should support a list of extra data, which can be implemented as an array of strings, an array of bytes or another CRDT
	d) Need to support doing queries without index (done Q3, not needed anymore)
	e) Need to measure overhead of doing updates with indexes. - later
	f) [NEXT] One CRDT per object in the DB.
	g) Mechanism to clear old operations from the VM. Need to think well about this.
*/
/*
	New stuff todo:
	a) [NEXT] existing aggregor CRDTs like topK should have optional fields for "any data", or "any other CRDT".
		With this, we can avoid having queries that need to consult other CRDTs.
	b) [NEXT] support queries which fetch aditional info separately and queries that fetch all data from the index crdt.
*/
/*
	TODO debug:
//TODO: Analyze how clocks returned in transactions are generated - seems like only the self replica is included (should be fixed now?)
//TODO: For some reason, not only are clocks weird, but it seems like when there's only 1 op in a crdt it doesn't get propagated (is it fixed now?)
//TODO: After all upds are sent, one thread (materializer?) gets stuck for quite a while on 100%
	//This seems to increase exponentially with the number of upds. E.g., 0.1SF is a few (<10) seconds, 1SF is over 3min.
*/

var (
	USE_AUTO_INDEX_CLIENT = false
)

func main() {

	client.LoadConfigs()
	if !USE_AUTO_INDEX_CLIENT {
		client.StartClient()
	} else {
		autoindex.StartAutoIndexClient()
	}

	//client.ProtoTest()
}
