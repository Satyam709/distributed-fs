// client/cmd/main.go — CLI entrypoint. Thin glue over service layer.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/satyam709/distributed-fs/client"
	"github.com/satyam709/distributed-fs/client/manifest"
	"github.com/satyam709/distributed-fs/client/metadataclient"
	"github.com/satyam709/distributed-fs/client/service"
	"github.com/satyam709/distributed-fs/client/storageclient"
)

var cfg = client.LoadFromEnv()

// --- Upload Command ---

var uploadCmd = &cobra.Command{
	Use:   "upload",
	Short: "Upload a file to the distributed FS",
	Run: func(cmd *cobra.Command, args []string) {
		filePath, _ := cmd.Flags().GetString("file")
		fileName, _ := cmd.Flags().GetString("name")

		if filePath == "" {
			log.Fatal("--file is required")
		}
		if fileName == "" {
			// Default to base name of the file
			fileName = filePath
		}

		meta, storage, cleanup := connect()
		defer cleanup()

		mgr := manifest.NewManager(cfg.ManifestDir)
		svc := service.NewUploadService(cfg, meta, storage, mgr)

		progress := func(chunkIndex int, total int, err error) {
			if err != nil {
				fmt.Printf("  ✗ Chunk %d/%d failed: %v\n", chunkIndex+1, total, err)
			} else {
				fmt.Printf("  ✓ Chunk %d/%d uploaded\n", chunkIndex+1, total)
			}
		}

		fmt.Printf("Uploading %s as %q ...\n", filePath, fileName)
		start := time.Now()

		result, err := svc.Upload(context.Background(), filePath, fileName, progress)
		elapsed := time.Since(start)

		if err != nil {
			fmt.Printf("\n✗ Upload failed: %v\n", err)
			if result != nil {
				fmt.Printf("  Chunks: %d/%d done, %d failed\n",
					result.ChunksDone, result.ChunksTotal, result.ChunksFailed)
			}
			os.Exit(1)
		}

		fmt.Printf("\n✓ Upload complete in %s\n", elapsed.Round(time.Millisecond))
		fmt.Printf("  File ID:  %s\n", result.FileID)
		fmt.Printf("  Size:     %s\n", formatBytes(result.TotalSize))
		fmt.Printf("  Chunks:   %d\n", result.ChunksTotal)
	},
}

// --- Download Command ---

var downloadCmd = &cobra.Command{
	Use:   "download",
	Short: "Download a file from the distributed FS",
	Run: func(cmd *cobra.Command, args []string) {
		fileName, _ := cmd.Flags().GetString("name")
		outputPath, _ := cmd.Flags().GetString("out")

		if fileName == "" {
			log.Fatal("--name is required")
		}
		if outputPath == "" {
			outputPath = fileName
		}

		meta, storage, cleanup := connect()
		defer cleanup()

		svc := service.NewDownloadService(cfg, meta, storage)

		progress := func(chunkIndex int, total int, err error) {
			if err != nil {
				fmt.Printf("  ✗ Chunk %d/%d failed: %v\n", chunkIndex+1, total, err)
			} else {
				fmt.Printf("  ✓ Chunk %d/%d downloaded\n", chunkIndex+1, total)
			}
		}

		fmt.Printf("Downloading %q to %s ...\n", fileName, outputPath)
		start := time.Now()

		result, err := svc.Download(context.Background(), fileName, outputPath, progress)
		elapsed := time.Since(start)

		if err != nil {
			fmt.Printf("\n✗ Download failed: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("\n✓ Download complete in %s\n", elapsed.Round(time.Millisecond))
		fmt.Printf("  File:   %s\n", result.OutputPath)
		fmt.Printf("  Size:   %s\n", formatBytes(result.TotalSize))
	},
}

// --- List Command ---

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List all files in the distributed FS",
	Run: func(cmd *cobra.Command, args []string) {
		prefix, _ := cmd.Flags().GetString("prefix")

		meta, _, cleanup := connect()
		defer cleanup()

		svc := service.NewListService(meta)

		files, err := svc.ListFiles(context.Background(), prefix)
		if err != nil {
			log.Fatalf("List failed: %v", err)
		}

		if len(files) == 0 {
			fmt.Println("No files found.")
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "FILE ID\tNAME\tSIZE\tSTATUS\tCHUNKS")
		fmt.Fprintln(w, "-------\t----\t----\t------\t------")
		for _, f := range files {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\n",
				f.FileID,
				f.FileName,
				formatBytes(f.FileSize),
				f.Status,
				len(f.ChunkIDs),
			)
		}
		w.Flush()
	},
}

// --- Delete Command ---

var deleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete a file from the distributed FS",
	Run: func(cmd *cobra.Command, args []string) {
		fileID, _ := cmd.Flags().GetString("id")

		if fileID == "" {
			log.Fatal("--id is required")
		}

		meta, _, cleanup := connect()
		defer cleanup()

		svc := service.NewListService(meta)

		if err := svc.DeleteFile(context.Background(), fileID); err != nil {
			log.Fatalf("Delete failed: %v", err)
		}

		fmt.Printf("✓ File %s deleted\n", fileID)
	},
}

// --- Helper Functions ---

// connect creates metadata and storage client instances.
// Returns a cleanup function to close connections.
func connect() (metadataclient.Client, storageclient.Client, func()) {
	metaAddr := cfg.MetadataAddrs[0]
	meta, err := metadataclient.NewGRPCClient(metaAddr)
	if err != nil {
		log.Fatalf("Failed to connect to metadata service at %s: %v", metaAddr, err)
	}

	storage := storageclient.NewGRPCClient(cfg.FrameSize)

	cleanup := func() {
		if err := meta.Close(); err != nil {
			log.Printf("Warning: failed to close metadata connection: %v", err)
		}
		if err := storage.Close(); err != nil {
			log.Printf("Warning: failed to close storage connections: %v", err)
		}
	}

	return meta, storage, cleanup
}

func formatBytes(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

// --- Main ---

func main() {
	rootCmd := &cobra.Command{
		Use:   "dfs",
		Short: "DFS client — distributed file system CLI",
	}

	// Upload flags
	uploadCmd.Flags().StringP("file", "f", "", "Path to the file to upload")
	uploadCmd.Flags().StringP("name", "n", "", "Name to store the file as (defaults to file path)")

	// Download flags
	downloadCmd.Flags().StringP("name", "n", "", "Name of the file to download")
	downloadCmd.Flags().StringP("out", "o", "", "Output file path (defaults to file name)")

	// List flags
	listCmd.Flags().StringP("prefix", "p", "", "Filter files by name prefix")

	// Delete flags
	deleteCmd.Flags().String("id", "", "File ID to delete")

	rootCmd.AddCommand(uploadCmd)
	rootCmd.AddCommand(downloadCmd)
	rootCmd.AddCommand(listCmd)
	rootCmd.AddCommand(deleteCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
