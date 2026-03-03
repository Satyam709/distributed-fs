package main

import (
	"log"
	"net"
	"google.golang.org/grpc"
	"github.com/satyam709/distributed-fs/storage/server" // your new package
    pb_storage "github.com/satyam709/distributed-fs/gen/proto/storage/v1"
)

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	
	// You'll need to initialize your store here first
	// st := yourStore.New(...) 
	
	// Register the server you just wrote
	pb_storage.RegisterStorageServiceServer(s, &server.StorageServer{
        // store: st,
    })

	log.Println("Storage server listening on :50051...")
	s.Serve(lis)
}