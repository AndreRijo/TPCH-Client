module tpch_client

go 1.16

require (
	google.golang.org/protobuf v1.34.2
	gotools v0.0.0
	potionDB/crdt v0.0.0
	potionDB/potionDB v0.0.0-00010101000000-000000000000
	tpch_data_processor v0.0.0
)

replace potionDB/potionDB => ../potionDB/potionDB

replace potionDB/shared => ../potionDB/shared

replace potionDB/crdt => ../potionDB/crdt

replace tpch_data_processor v0.0.0 => ../tpch_data_processor

replace sqlToKeyValue v0.0.0 => ../sqlToKeyValue


