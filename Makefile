.PHONY: lint go-fmt pb-fmt fmt lint go-lint pb-lint check-tools proto-gen
proto-gen: check-tools     # regenerate all pb.go files from protos
	buf generate
build-storage:   # build storage node binary
	go build -o bin/storage ./storage/cmd
build-metadata:  # build metadata node binary  
build-client:    # build client binary
build-all:       # all three
test-storage:    # run storage package tests
test-all:        # all tests
	go test -count=1 ./...
run-cluster:     # start 1 metadata + 4 storage nodes locally
demo:            # run demo script

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
