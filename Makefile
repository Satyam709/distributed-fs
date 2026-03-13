.PHONY: proto-gen build-storage build-metadata build-client build-all
.PHONY: test-storage test-all
.PHONY: fmt go-fmt pb-fmt
.PHONY: fmt-check go-fmt-check pb-fmt-check
.PHONY: lint go-lint pb-lint
.PHONY: check-tools run-cluster demo
proto-gen: check-buf     # regenerate all pb.go files from protos
	buf generate
build-storage:   # build storage node binary
	go build -o bin/storage ./storage/cmd
build-metadata:  # build metadata node binary
	go build -o bin/metadata ./metadata/cmd
build-client:    # build client binary
	go build -o bin/client ./client/cmd
build-all: 	build-storage build-client build-metadata      # all three
test-storage:    # run storage package tests

test-all:        # all tests
	go test -count=1 ./...
run-cluster:     # start 1 metadata + 4 storage nodes locally
demo:            # run demo script
	./scripts/test_upload_download.sh

# func to check for a specific tool
define check_tool
	@which $(1) > /dev/null 2>&1 || ( \
		echo "Error: $(1) is not installed."; \
		exit 1)
endef

# checks if required tools are installed
check-buf:
	$(call check_tool,buf)

check-golangci-lint:
	$(call check_tool,golangci-lint)

fmt: go-fmt pb-fmt

go-fmt:
	go fmt ./...

pb-fmt:
	buf format -w

fmt-check: go-fmt-check pb-fmt-check

go-fmt-check:
	@if [ -n "$$(gofmt -l .)" ]; then \
		echo "Go code is not formatted. Run 'make go-fmt' to fix."; \
		gofmt -d .; \
		exit 1; \
	fi

pb-fmt-check:
	buf format --diff --exit-code

lint: go-lint pb-lint

go-lint: check-golangci-lint
	golangci-lint run ./...

pb-lint: check-buf
	buf lint

install-golangci-lint:
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.11.3

clean: 
	@echo "Cleaning project directory..."
# 	check root
	@grep -q 'github.com/satyam709/distributed-fs' "go.mod" 2>/dev/null || { echo "go.mod not found: its not root aborting clean" ; exit 2; } 

# remove files
	rm -rf ./data/*
	rm -rf ./manifests_logs