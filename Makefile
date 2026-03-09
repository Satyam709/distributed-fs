.PHONY: lint go-fmt pb-fmt fmt lint go-lint pb-lint check-tools proto-gen
proto-gen: check-tools     # regenerate all pb.go files from protos
	buf generate
build-storage:   # build storage node binary
	go build -o bin/storage ./storage/cmd/main.go
build-metadata:  # build metadata node binary  
build-client:    # build client binary
	go build -o bin/client ./client/cmd/main.go
build-all: 	build-storage build-client       # all three
test-storage:    # run storage package tests

test-all:        # all tests
	go test ./...
run-cluster:	build-all    # start 1 metadata + 4 storage nodes locally
	@echo "Starting cluster..."
demo:            # run demo script
	./scripts/test_upload_download.sh

# func to check for a specific tool
define check_tool
	@which $(1) > /dev/null 2>&1 || ( \
		echo "Error: $(1) is not installed."; \
		exit 1)
endef

# checks if required tools are installed
check-tools:
	$(call check_tool,golangci-lint)
	$(call check_tool,buf)
 
lint: check-tools go-lint pb-lint

go-lint:
	golangci-lint run ./...

pb-lint:
	buf format --diff --exit-code
	buf lint

go-fmt:
	go fmt ./...

pb-fmt:
	buf format -w

fmt: go-fmt pb-fmt
