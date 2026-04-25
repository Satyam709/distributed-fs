# build app
FROM  golang:1.26.2-alpine3.23 AS builder
WORKDIR /app
COPY . .
# download dependencies
RUN apk add --no-cache make
RUN go mod verify && go mod download
RUN make build-metadata

# get the build image to the app image
FROM  golang:1.26.2-alpine3.23 
COPY --from=builder /app/bin/metadata /bin
# make the raft data-directory
RUN mkdir -p /metadata-node/data/raft
ENV METADATA_RAFT_DIR="/metadata-node/data/raft"

RUN which metadata || { echo "metadata not found" ; exit 1; }

CMD ["sh", "-c",  "/bin/metadata  2>&1 | tee /var/log/metadata_node.log"]