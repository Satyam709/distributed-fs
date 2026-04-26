# build app
FROM  golang:1.26.2-alpine3.23 AS builder
WORKDIR /app
COPY . .
# download dependencies
RUN apk add --no-cache make
RUN go mod verify && go mod download
RUN make build-storage

# get the binary from the build image to the app image
FROM  golang:1.26.2-alpine3.23 
COPY --from=builder /app/bin/storage /bin
RUN which storage || { echo "storage bin not found" ; exit 1; }
RUN mkdir -p /storage_node/data
ENV STORAGE_DATA_DIR="/storage_node/data"
CMD [ "sh", "-c" , "storage  2>&1 | tee /var/log/storage_node.log"]
