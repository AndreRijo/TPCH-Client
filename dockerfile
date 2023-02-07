# Debian image with go installed and configured at /go
FROM golang

# Dependencies
#RUN go get github.com/golang/protobuf/proto; \
	#go get github.com/twmb/murmur3; \
	#go get github.com/streadway/amqp

# Adding src and building
COPY potionDB/go.mod potionDB/go.sum /go/potionDB/
COPY tpch_client/go.mod tpch_client/go.sum /go/tpch_client/
COPY tpch_data/go.mod /go/tpch_data/
RUN cd tpch_client && go mod download

COPY potionDB/src/clocksi /go/potionDB/src/clocksi
COPY potionDB/src/tools /go/potionDB/src/tools
COPY potionDB/src/crdt /go/potionDB/src/crdt
COPY potionDB/src/proto /go/potionDB/src/proto
COPY potionDB/src/antidote /go/potionDB/src/antidote
COPY potionDB/src/shared /go/potionDB/src/shared
COPY tpch_data/tpch /go/tpch_data/tpch
COPY tpch_client/src /go/tpch_client/src
COPY tpch_client/dockerstuff /go/tpch_client/
RUN cd tpch_client/src/main && go build
#RUN go install main


#Bench args
#Arguments
ENV CONFIG "configs/docker/default" \
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