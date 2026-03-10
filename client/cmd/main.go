// client/cmd/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/satyam709/distributed-fs/client" // Import your config package
	"github.com/satyam709/distributed-fs/client/chunker"
	"github.com/satyam709/distributed-fs/client/manifest"
	"github.com/satyam709/distributed-fs/client/uploader"
	pb_storage "github.com/satyam709/distributed-fs/gen/proto/storage/v1"
)

var cfg = client.LoadFromEnv()

var uploadCmd = &cobra.Command{
	Use:   "upload [file path]",
	Short: "Upload a file to the distributed FS",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		filePath := args[0]

		// Ensure manifest directory exists
		if err := os.MkdirAll(cfg.ManifestDir, 0755); err != nil {
			log.Fatalf("Failed to create manifest directory: %v", err)
		}

		// 1. Chunker: Use ChunkSize from Config
		descriptors, err := chunker.GenerateDescriptors(filePath, cfg.ChunkSize)
		if err != nil {
			log.Fatalf("Failed to chunk file: %v", err)
		}

		// 2. Manifest: Setup
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			log.Fatalf("Failed to stat file: %v", err)
		}
		m := &manifest.UploadManifest{
			FileID:      descriptors[0].FileID,
			Filename:    filePath,
			TotalSize:   fileInfo.Size(),
			ChunkStatus: make(map[string]bool),
		}
		for _, d := range descriptors {
			m.ChunkStatus[d.ChunkID] = false
		}

		if err := manifest.Save(cfg.ManifestDir, m); err != nil {
			log.Fatalf("Failed to save manifest: %v", err)
		}

		// 3. Storage Connection (Using dummy address for now)
		conn, err := grpc.NewClient("localhost:4000", grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("Failed to connect to storage: %v", err)
		}
		defer conn.Close()
		storageClient := pb_storage.NewStorageServiceClient(conn)

		// 4. ParallelUploader: Use concurrency limit from Config
		up := uploader.NewParallelUploader(cfg.MaxParallelUploads, storageClient)

		fmt.Printf("Starting parallel upload (Max: %d concurrent chunks)...\n", cfg.MaxParallelUploads)
		if err := up.Upload(context.Background(), filePath, descriptors); err != nil {
			log.Fatalf("Upload failed: %v", err)
		}

		for _, d := range descriptors {
			m.ChunkStatus[d.ChunkID] = true
		}

		if err := manifest.Save(cfg.ManifestDir, m); err != nil {
			log.Fatalf("Failed to save final manifest: %v", err)
		}

		fmt.Println("Upload successful!")
	},
}

var downloadCmd = &cobra.Command{
	Use:   "download [filename] [output path]",
	Short: "Download a file from the distributed FS",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		fileName := args[0]
		outputPath := args[1]

		// 1. Metadata: GetFile (Placeholder for Section 4)
		// You'll need to fetch the ordered chunk list and node addresses.
		// For now, this is where you'd call: metadataClient.GetFile(fileName)
		fmt.Printf("Fetching metadata for %s...\n", fileName)

		// 2. ParallelDownloader: Orchestrate the retrieval
		// This is where you use the downloader component we wrote.
		// d := downloader.NewParallelDownloader(cfg.MaxParallelDownloads)

		fmt.Printf("Downloading to %s using %d parallel workers...\n", outputPath, cfg.MaxParallelDownloads)

		// Example call logic:
		// err := d.Download(context.Background(), outputPath, totalSize, placementMap)
		// if err != nil {
		//     log.Fatalf("Download failed: %v", err)
		// }

		fmt.Println("Download complete and verified!")
	},
}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List all files in the distributed FS",
	Run: func(cmd *cobra.Command, args []string) {
		// Flow: MetadataClient.ListFiles → print table
		fmt.Println("Fetching file list from metadata...")
	},
}

func main() {
	fmt.Println("start")
	var rootCmd = &cobra.Command{Use: "client"}
	rootCmd.AddCommand(uploadCmd)
	rootCmd.AddCommand(downloadCmd)
	rootCmd.AddCommand(listCmd)
	err := rootCmd.Execute()
	if err != nil {
		log.Fatal("cli error while executing: ", err)
	}
}
