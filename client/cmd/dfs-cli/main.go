// client/cmd/dfs/main.go — CLI entrypoint. Thin wrapper over the dfsclient SDK.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/satyam709/distributed-fs/client/dfsclient"
	"github.com/satyam709/distributed-fs/client/internal/dfsclientconfig"
)

func main() {
	cfg := dfsclientconfig.LoadFromEnv()

	rootCmd := &cobra.Command{
		Use:   "dfs",
		Short: "DFS client — distributed file system CLI",
	}

	rootCmd.AddCommand(
		uploadCmd(cfg),
		downloadCmd(cfg),
		listCmd(cfg),
		deleteCmd(cfg),
	)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

// --- Upload Command ---

func uploadCmd(cfg *dfsclientconfig.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "upload",
		Short: "Upload a file to the distributed FS",
		Run: func(cmd *cobra.Command, args []string) {
			filePath, _ := cmd.Flags().GetString("file")
			fileName, _ := cmd.Flags().GetString("name")

			if filePath == "" {
				log.Fatal("--file is required")
			}
			if fileName == "" {
				fileName = filePath
			}

			c, err := dfsclient.New(dfsclient.WithConfig(cfg))
			if err != nil {
				log.Fatalf("Failed to create client: %v", err)
			}
			defer func() { _ = c.Close() }()

			progress := func(info dfsclient.ProgressInfo) {
				if info.Err != nil {
					fmt.Printf("  ✗ Chunk %d/%d failed: %v\n", info.ChunkIndex+1, info.ChunksTotal, info.Err)
				} else {
					fmt.Printf("  ✓ Chunk %d/%d uploaded (%.1f%%)\n", info.ChunkIndex+1, info.ChunksTotal, info.Percent())
				}
			}

			fmt.Printf("Uploading %s as %q ...\n", filePath, fileName)
			start := time.Now()

			result, err := c.Upload(context.Background(), filePath, fileName, progress)
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
	cmd.Flags().StringP("file", "f", "", "Path to the file to upload")
	cmd.Flags().StringP("name", "n", "", "Name to store the file as (defaults to file path)")
	return cmd
}

// --- Download Command ---

func downloadCmd(cfg *dfsclientconfig.Config) *cobra.Command {
	cmd := &cobra.Command{
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

			c, err := dfsclient.New(dfsclient.WithConfig(cfg))
			if err != nil {
				log.Fatalf("Failed to create client: %v", err)
			}
			defer func() { _ = c.Close() }()

			progress := func(info dfsclient.ProgressInfo) {
				if info.Err != nil {
					fmt.Printf("  ✗ Chunk %d/%d failed: %v\n", info.ChunkIndex+1, info.ChunksTotal, info.Err)
				} else {
					fmt.Printf("  ✓ Chunk %d/%d downloaded\n", info.ChunkIndex+1, info.ChunksTotal)
				}
			}

			fmt.Printf("Downloading %q to %s ...\n", fileName, outputPath)
			start := time.Now()

			result, err := c.Download(context.Background(), fileName, outputPath, progress)
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
	cmd.Flags().StringP("name", "n", "", "Name of the file to download")
	cmd.Flags().StringP("out", "o", "", "Output file path (defaults to file name)")
	return cmd
}

// --- List Command ---

func listCmd(cfg *dfsclientconfig.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all files in the distributed FS",
		Run: func(cmd *cobra.Command, args []string) {
			prefix, _ := cmd.Flags().GetString("prefix")

			c, err := dfsclient.New(dfsclient.WithConfig(cfg))
			if err != nil {
				log.Fatalf("Failed to create client: %v", err)
			}
			defer func() { _ = c.Close() }()

			files, err := c.List(context.Background(), prefix)
			if err != nil {
				log.Fatalf("List failed: %v", err)
			}

			if len(files) == 0 {
				fmt.Println("No files found.")
				return
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			_, _ = fmt.Fprintln(w, "FILE ID\tNAME\tSIZE\tSTATUS\tCHUNKS")
			_, _ = fmt.Fprintln(w, "-------\t----\t----\t------\t------")
			for _, f := range files {
				_, _ = fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\n",
					f.FileID,
					f.FileName,
					formatBytes(f.FileSize),
					f.Status,
					f.ChunkCount,
				)
			}
			_ = w.Flush()
		},
	}
	cmd.Flags().StringP("prefix", "p", "", "Filter files by name prefix")
	return cmd
}

// --- Delete Command ---

func deleteCmd(cfg *dfsclientconfig.Config) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete a file from the distributed FS",
		Run: func(cmd *cobra.Command, args []string) {
			fileID, _ := cmd.Flags().GetString("id")

			if fileID == "" {
				log.Fatal("--id is required")
			}

			c, err := dfsclient.New(dfsclient.WithConfig(cfg))
			if err != nil {
				log.Fatalf("Failed to create client: %v", err)
			}
			defer func() { _ = c.Close() }()

			if err := c.Delete(context.Background(), fileID); err != nil {
				log.Fatalf("Delete failed: %v", err)
			}

			fmt.Printf("✓ File %s deleted\n", fileID)
		},
	}
	cmd.Flags().String("id", "", "File ID to delete")
	return cmd
}

// --- Helpers ---

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
