#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"

METADATA_NODE_COUNT=1
STORAGE_NODE_COUNT=3
STORAGE_METADATA_ADDR="metadata-1:3000"
IMAGE_PREFIX="devcon"
METADATA_BUILD_DIR=".."
STORAGE_BUILD_DIR=".."
METADATA_DOCKERFILE="metadata/Dockerfile"
STORAGE_DOCKERFILE="storage/Dockerfile"
CLUSTER_DATA_DIR="./cluster-data"

usage() {
    echo "Usage: $0 [OPTIONS] [docker-compose-args]"
    echo ""
    echo "Options:"
    echo "  -m, --metadata-count N    Number of metadata nodes (default: 1)"
    echo "  -s, --storage-count N     Number of storage nodes (default: 3)"
    echo "  -a, --metadata-addr      Metadata service address (default: metadata-1:3000)"
    echo "  -i, --image-prefix       Image prefix (default: devcon)"
    echo "  -h, --help              Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 up -d --build        # Build and run"
    echo "  $0 up -d                # Use existing local images"
    echo "  $0 -m 3 -s 5 up -d"
    echo "  $0 -m 3 up -d"
    echo "  $0 -i myprefix up -d    # Custom image prefix"
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

mkdir -p "$CLUSTER_DATA_DIR"

{
echo "services:"

for i in $(seq 1 "$METADATA_NODE_COUNT"); do
    GRPC_PORT=$((4000 + i))
    RAFT_PORT=$((5000 + i))
    if [ "$i" -eq 1 ]; then
        IS_BOOTSTRAP="true"
        PEER_ADDRS=""
    else
        IS_BOOTSTRAP="false"
        PEER_ADDRS=""
        for j in $(seq 1 "$METADATA_NODE_COUNT"); do
            if [ "$j" -ne "$i" ]; then
                if [ -n "$PEER_ADDRS" ]; then
                    PEER_ADDRS="${PEER_ADDRS},"
                fi
                PEER_ADDRS="${PEER_ADDRS}metadata-${j}:5001"
            fi
        done
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
    echo "      - METADATA_RAFT_ADDR=:$RAFT_PORT"
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
    echo "      - STORAGE_METADATA_ADDR=${STORAGE_METADATA_ADDR}"
    echo "      - STORAGE_DATA_DIR=/storage_node/data"
    echo "    volumes:"
    echo "      - ${CLUSTER_DATA_DIR}/storage-${i}:/storage_node/data"
    echo "      - ${CLUSTER_DATA_DIR}/storage-${i}/logs:/var/log"
    echo "    depends_on:"
    echo "      - metadata-1"
done

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