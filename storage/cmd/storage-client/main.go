package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	pb_storage "github.com/satyam709/distributed-fs/gen/proto/storage/v1"

	grpc "google.golang.org/grpc"
	insecure "google.golang.org/grpc/credentials/insecure"
)

const (
	serverAddr = "localhost:50051"
	frameSize  = 32 * 1024 // 32KB
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: upload <file> <chunkID> OR download <chunkID> <outputFile>")
	}

	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := pb_storage.NewStorageServiceClient(conn)

	switch os.Args[1] {
	case "upload":
		if len(os.Args) != 4 {
			log.Fatal("usage: upload <file> <chunkID>")
		}
		upload(client, os.Args[2], os.Args[3])

	case "download":
		if len(os.Args) != 4 {
			log.Fatal("usage: download <chunkID> <outputFile>")
		}
		download(client, os.Args[2], os.Args[3])

	default:
		log.Fatal("unknown command")
	}
}

func upload(client pb_storage.StorageServiceClient, filePath, chunkID string) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream, err := client.PutChunk(ctx)
	if err != nil {
		log.Fatal(err)
	}

	buffer := make([]byte, frameSize)

	for {
		n, err := file.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		frame := buffer[:n]

		sum := sha256.Sum256(frame)
		checksum := hex.EncodeToString(sum[:])

		req := &pb_storage.PutChunkRequest{
			ChunkId: chunkID,
			FileId:  "test-file",
			Data:    frame,
			Checksum: checksum,
			IsLast:  false,
		}

		if err := stream.Send(req); err != nil {
			log.Fatal(err)
		}
	}

	// send final frame with is_last = true
	finalReq := &pb_storage.PutChunkRequest{
		ChunkId: chunkID,
		FileId:  "test-file",
		IsLast:  true,
	}

	if err := stream.Send(finalReq); err != nil {
		log.Fatal(err)
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Upload response:", resp)
}

func download(client pb_storage.StorageServiceClient, chunkID, outputPath string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream, err := client.GetChunk(ctx, &pb_storage.GetChunkRequest{
		ChunkId: chunkID,
	})
	if err != nil {
		log.Fatal(err)
	}

	outFile, err := os.Create(outputPath)
	if err != nil {
		log.Fatal(err)
	}
	defer outFile.Close()

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		if len(resp.GetData()) > 0 {
			if _, err := outFile.Write(resp.GetData()); err != nil {
				log.Fatal(err)
			}
		}

		if resp.GetIsLast() {
			break
		}
	}

	fmt.Println("Download complete")
}