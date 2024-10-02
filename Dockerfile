# Debian image with go installed and configured at /go
FROM golang:1.20.4 as base

# Dependencies
#RUN go get github.com/golang/protobuf/proto; \
	#go get github.com/twmb/murmur3; \
	#go get github.com/streadway/amqp

# Adding src and building
COPY potionDB/potionDB/go.mod potionDB/potionDB/go.sum /go/potionDB/potionDB/
COPY potionDB/crdt/go.mod potionDB/crdt/go.sum /go/potionDB/crdt/
COPY potionDB/shared/go.mod /go/potionDB/shared/
COPY tpch_data_processor/go.mod tpch_data_processor/go.sum /go/tpch_data_processor/
COPY sqlToKeyValue/go.mod sqlToKeyValue/go.sum /go/sqlToKeyValue/
COPY goTools/go.mod goTools/go.sum /go/goTools/
COPY tpch_client/go.mod tpch_client/go.sum /go/tpch_client/
RUN cd tpch_client && go mod download

COPY potionDB/potionDB /go/potionDB/potionDB
COPY potionDB/crdt /go/potionDB/crdt
COPY potionDB/shared /go/potionDB/shared

COPY tpch_data_processor/ /go/tpch_data_processor/
COPY sqlToKeyValue/ /go/sqlToKeyValue/
COPY goTools/ /go/goTools/

COPY tpch_client/dockerstuff tpch_client/
COPY tpch_client/src tpch_client/src

RUN cd tpch_client/src/main && go build && cd /go && rm -r bin goTools pkg potionDB tpch_data_processor sqlToKeyValue tpch_client/src/autoindex tpch_client/src/client
#RUN go install main

#Final image
FROM golang:1.20.4

#Copy needed files from base image
COPY --from=base /go/tpch_client /go/tpch_client

#Bench args
#Arguments
ENV CONFIG "configs/docker/default" \
DATA_FOLDER "none" \
QUERY_CLIENTS "none" \
GLOBAL_INDEX "none" \
TEST_NAME "none" \
SINGLE_INDEX_SERVER "none" \
UPD_RATE "none" \
RESET "none" \
ID "none" \
SPLIT_UPDATES "none" \
SPLIT_UPDATES_NO_WAIT "none" \
N_READS_TXN "none" \
BATCH_MODE "none" \
LATENCY_MODE "none" \
SERVERS "none" \
USE_TOP_SUM "none" \
LOCAL_MODE "none" \
SF "none" \
NON_RANDOM_SERVERS "none" \
LOCAL_REGION_ONLY "none" \
Q15_SIZE "none" \
UPDATE_SPECIFIC_INDEX "none" \
IS_BENCH "none" \
INITIAL_MEM "none" \
B_N_KEYS "none" \
B_KEY_TYPE "none" \
B_ADD_RATE "none" \
B_PART_READ_RATE "none" \
B_QUERY_RATES "none" \
B_OPS_PER_TXN "none" \
B_TXNS_BEFORE_WAIT "none" \
B_N_ELEMS "none" \
B_MAX_ID "none" \
B_MAX_SCORE "none" \
B_TOP_N "none" \
B_TOP_ABOVE "none" \
B_RND_SIZE "none" \
B_MIN_CHANGE "none" \
B_MAX_CHANGE "none" \
B_MAX_SUM "none" \
B_MAX_N_ADDS "none" \
B_CRDT_TYPE "none" \
B_UPDATE_FUNS "none" \
B_QUERY_FUNS "none" \
B_DO_PRELOAD "none" \
B_DO_QUERY "none" \
P_N_PROTOS "none" \
P_N_BYTES "none" \
P_N_ROUTINES "none" \
P_N_MODE "none" 


#Add config folders late to avoid having to rebuild multiple images
ADD tpch_client/configs configs/

# Run the client
CMD ["bash", "tpch_client/start.sh"]
#CMD ["bash"]