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
ENV CONFIG "configs/docker/default"

#Add config folders late to avoid having to rebuild multiple images
ADD tpch_client/configs configs/

# Run the client
CMD ["bash", "start.sh"]
#CMD ["bash"]