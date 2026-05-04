#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"

METADATA_NODE_COUNT=1
STORAGE_NODE_COUNT=3
STORAGE_METADATA_ADDR="metadata-1:4001"
IMAGE_PREFIX="devcon"
METADATA_BUILD_DIR=".."
STORAGE_BUILD_DIR=".."
METADATA_DOCKERFILE="metadata/Dockerfile"
STORAGE_DOCKERFILE="storage/Dockerfile"
CLUSTER_DATA_DIR="./cluster-data"
CLIENT_ENABLED=false

usage() {
    echo "Usage: $0 [OPTIONS] [docker-compose-args]"
    echo ""
    echo "Options:"
    echo "  -m, --metadata-count N    Number of metadata nodes (default: 1)"
    echo "  -s, --storage-count N     Number of storage nodes (default: 3)"
    echo "  -a, --metadata-addr      Metadata service address (default: metadata-1:4001)"
    echo "  -i, --image-prefix       Image prefix (default: devcon)"
    echo "  -c, --client             Add client container service (builds dfs-cli, preconfigured for cluster network)"
    echo "  -h, --help              Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 up -d --build        # Build and run"
    echo "  $0 up -d                # Use existing local images"
    echo "  $0 -m 3 -s 5 up -d"
    echo "  $0 -m 3 up -d"
    echo "  $0 -i myprefix up -d    # Custom image prefix"
    echo "  $0 -c up -d --build     # Build all + client, run daemonized"
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -m|--metadata-count)
            METADATA_NODE_COUNT="$2"
            shift 2
            ;;
        -s|--storage-count)
            STORAGE_NODE_COUNT="$2"
            shift 2
            ;;
        -a|--metadata-addr)
            STORAGE_METADATA_ADDR="$2"
            shift 2
            ;;
        -i|--image-prefix)
            IMAGE_PREFIX="$2"
            shift 2
            ;;
        -c|--client)
            CLIENT_ENABLED=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            break
            ;;
    esac
done

echo "Generating docker-compose.yml with:"
echo "  Metadata nodes: $METADATA_NODE_COUNT"
echo "  Storage nodes: $STORAGE_NODE_COUNT"
echo "  Metadata addr: $STORAGE_METADATA_ADDR"
echo "  Image prefix: $IMAGE_PREFIX"
echo "  Data dir: $CLUSTER_DATA_DIR"
echo "  Client service: $CLIENT_ENABLED"

mkdir -p "$CLUSTER_DATA_DIR"

{
echo "services:"

for i in $(seq 1 "$METADATA_NODE_COUNT"); do
    GRPC_PORT=$((4000 + i))
    RAFT_PORT=$((5000 + i))
    
    PEER_ADDRS=""
    for j in $(seq 1 "$METADATA_NODE_COUNT"); do
        if [ "$j" -ne "$i" ]; then
            RAFT_PORT_PEER=$((5000 + j))
            if [ -n "$PEER_ADDRS" ]; then
                PEER_ADDRS="${PEER_ADDRS},"
            fi
            PEER_ADDRS="${PEER_ADDRS}metadata-${j}:metadata-${j}:${RAFT_PORT_PEER}"
        fi
    done
    
    if [ "$i" -eq 1 ]; then
        IS_BOOTSTRAP="true"
    else
        IS_BOOTSTRAP="false"
    fi

    echo "  metadata-$i:"
    echo "    image: meta-${IMAGE_PREFIX}"
    echo "    build:"
    echo "      context: ${METADATA_BUILD_DIR}"
    echo "      dockerfile: ${METADATA_DOCKERFILE}"
    echo "    container_name: metadata-$i"
    echo "    ports:"
    echo "      - \"$GRPC_PORT:$GRPC_PORT\""
    echo "      - \"$RAFT_PORT:$RAFT_PORT\""
    echo "    environment:"
    echo "      - METADATA_NODE_ID=metadata-$i"
    echo "      - METADATA_GRPC_ADDR=:$GRPC_PORT"
    echo "      - METADATA_RAFT_ADDR=0.0.0.0:$RAFT_PORT"
    echo "      - METADATA_RAFT_ADVERTISE=metadata-${i}:$RAFT_PORT"
    echo "      - METADATA_RAFT_DIR=/metadata-node/data/raft"
    echo "      - METADATA_BOOTSTRAP=$IS_BOOTSTRAP"
    if [ -n "$PEER_ADDRS" ]; then
        echo "      - METADATA_PEER_ADDRS=$PEER_ADDRS"
    fi
    echo "    volumes:"
    echo "      - ${CLUSTER_DATA_DIR}/meta-${i}:/metadata-node/data"
    echo "      - ${CLUSTER_DATA_DIR}/meta-${i}/logs:/var/log"
    if [ "$i" -gt 1 ]; then
        echo "    depends_on:"
        echo "      - metadata-1"
    fi
done

for i in $(seq 1 "$STORAGE_NODE_COUNT"); do
    HOST_PORT=$((4100 + i))
    
    echo "  storage-$i:"
    echo "    image: storage-${IMAGE_PREFIX}"
    echo "    build:"
    echo "      context: ${STORAGE_BUILD_DIR}"
    echo "      dockerfile: ${STORAGE_DOCKERFILE}"
    echo "    container_name: storage-$i"
    echo "    ports:"
    echo "      - \"$HOST_PORT:4000\""
    echo "    environment:"
    echo "      - STORAGE_NODE_ID=storage-$i"
    echo "      - STORAGE_GRPC_ADDR=:4000"
    echo "      - STORAGE_ADVERTISE_ADDR=storage-${i}:4000"
    echo "      - STORAGE_METADATA_ADDRS=${STORAGE_METADATA_ADDR}"
    echo "      - STORAGE_DATA_DIR=/storage_node/data"
    echo "    volumes:"
    echo "      - ${CLUSTER_DATA_DIR}/storage-${i}:/storage_node/data"
    echo "      - ${CLUSTER_DATA_DIR}/storage-${i}/logs:/var/log"
    echo "    depends_on:"
    echo "      - metadata-1"
done

if [[ "$CLIENT_ENABLED" == "true" ]]; then
    META_ADDRS=""
    for i in $(seq 1 "$METADATA_NODE_COUNT"); do
        GRPC_PORT=$((4000 + i))
        if [[ -n "$META_ADDRS" ]]; then
            META_ADDRS="${META_ADDRS},"
        fi
        META_ADDRS="${META_ADDRS}metadata-${i}:${GRPC_PORT}"
    done

    CLIENT_DOCKERFILE="scripts/Dockerfile.client"

    echo "  client-1:"
    echo "    image: client-${IMAGE_PREFIX}"
    echo "    build:"
    echo "      context: ${STORAGE_BUILD_DIR}"
    echo "      dockerfile: ${CLIENT_DOCKERFILE}"
    echo "    container_name: dfs-client"
    echo "    environment:"
    echo "      - METADATA_ADDRS=${META_ADDRS}"
    echo "    volumes:"
    echo "      - ${CLUSTER_DATA_DIR}/client:/client_data"
    echo "    depends_on:"
    echo "      - metadata-1"
fi

echo ""
echo "networks:"
echo "  default:"
echo "    name: dfs-cluster"

} > "$COMPOSE_FILE"

echo "Generated $COMPOSE_FILE"

if [[ $# -eq 0 ]]; then
    echo "No docker-compose args provided. Run manually with:"
    echo "  docker compose -f $COMPOSE_FILE up"
else
    exec docker compose -f "$COMPOSE_FILE" "$@"
fi