# Debian image with go installed and configured at /go
FROM golang

# Dependencies
RUN go get github.com/golang/protobuf/proto; \
	go get github.com/twmb/murmur3; \
	go get github.com/streadway/amqp

# Adding src and building
COPY potionDB/src/clocksi /go/src/clocksi
COPY potionDB/src/tools /go/src/tools
COPY potionDB/src/crdt /go/src/crdt
COPY potionDB/src/proto /go/src/proto
COPY potionDB/src/antidote /go/src/antidote
COPY potionDB/src/shared /go/src/shared
COPY tpch_client/src/ /go/src/
COPY tpch_client/dockerstuff/ /go/
RUN go install main

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
USE_TOP_SUM "none" 
#Bench args
ENV B_N_KEYS "none" \
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
B_MAX_N_ADDS "none"


#Add config folders late to avoid having to rebuild multiple images
ADD tpch_client/configs configs/

# Run the client
CMD ["bash", "start.sh"]
#CMD ["bash"]