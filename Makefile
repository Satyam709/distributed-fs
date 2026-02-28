proto-gen:       # regenerate all pb.go files from protos
build-storage:   # build storage node binary
	go build -o bin/storage ./storage/cmd
build-metadata:  # build metadata node binary  
build-client:    # build client binary
build-all:       # all three
test-storage:    # run storage package tests
test-all:        # all tests
	go test ./...
run-cluster:     # start 1 metadata + 4 storage nodes locally
demo:            # run demo script

check-golangci-lint:
	@which golangci-lint > /dev/null 2>&1 || (echo "Error: golangci-lint is not installed. Please install it by running: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest" && exit 1)

lint: check-golangci-lint
	golangci-lint run

fmt:
	go fmt ./...