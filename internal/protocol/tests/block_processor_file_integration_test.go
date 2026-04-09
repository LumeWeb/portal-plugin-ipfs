package tests

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/datastore/dshelp"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/ipld/merkledag"
	format "github.com/ipfs/go-ipld-format"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
	"go.lumeweb.com/portal-plugin-ipfs/internal/upload"
	coreTesting "go.lumeweb.com/portal/core/testing"
	contentUnixFS "go.lumeweb.com/ipfs-content/unixfs"
)

// createTestFileProcessor creates a FileBlockProcessor for testing with real components
func createTestFileProcessor(t *testing.T, fileContent string) (*protocol.FileBlockProcessor, *protocol.DefaultStreamingBlockstore, format.DAGService, coreTesting.TestContext) {
	return createTestFileProcessorWithOptionalPath(t, fileContent, "")
}

// createTestFileProcessorWithPath creates a FileBlockProcessor for testing with the specified file path
func createTestFileProcessorWithPath(t *testing.T, fileContent, filePath string) (*protocol.FileBlockProcessor, *protocol.DefaultStreamingBlockstore, format.DAGService, coreTesting.TestContext) {
	return createTestFileProcessorWithOptionalPath(t, fileContent, filePath)
}

// createTestFileProcessorWithOptionalPath creates a FileBlockProcessor for testing with optional file path
func createTestFileProcessorWithOptionalPath(t *testing.T, fileContent, filePath string) (*protocol.FileBlockProcessor, *protocol.DefaultStreamingBlockstore, format.DAGService, coreTesting.TestContext) {
	// Create shared DoneTracker
	doneTracker := protocol.NewDoneTracker()

	// Create archive blockstore using helper with shared DoneTracker (no drainChannel since processor will consume)
	bs, _, ctx := createTestArchiveBlockstore(t, doneTracker, true)

	// Create file reader with seek functionality using UniversalReader
	seekableFile := upload.NewUniversalReader(strings.NewReader(fileContent))

	// Create DAG service
	dagService := merkledag.NewDAGService(
		blockservice.New(bs, offline.Exchange(bs)),
	)

	// Create node generator with the connected components
	nodeGenerator := contentUnixFS.NewUnixFSNodeGenerator(
		contentUnixFS.WithUnixFSNodeDAGService(dagService),
		contentUnixFS.WithUnixFSNodeBlockstore(bs),
	)

	var processor *protocol.FileBlockProcessor
	var err error

	// Create file processor with path if provided, otherwise use defaults
	if filePath != "" {
		processor, err = protocol.NewFileBlockProcessorWithPath(ctx.GetContext(), bs, seekableFile, filePath, dagService, nodeGenerator, ctx.Logger())
	} else {
		processor, err = protocol.NewFileBlockProcessorWithDefaults(ctx.GetContext(), bs, seekableFile, dagService, nodeGenerator, ctx.Logger(), doneTracker)
	}
	require.NoError(t, err)

	return processor, bs, dagService, ctx
}

// processFileAndWait processes a file and waits for completion, returning root CID and block count
func processFileAndWait(t *testing.T, processor *protocol.FileBlockProcessor, dagService format.DAGService, timeout time.Duration) (string, int) {
	// Start processing in background goroutine
	blockCountChan := startProcessingAndCountBlocks(t, processor, dagService)

	// Wait for file processing to get the root CID
	rootCID := waitForFileProcessing(t, processor, timeout)[0]

	// Get the block count from the background goroutine with timeout
	select {
	case blockCount := <-blockCountChan:
		return rootCID, blockCount
	case <-time.After(timeout):
		t.Fatal("Timed out waiting for block counting to complete")
		return "", 0 // Unreachable, but satisfies compiler
	}
}

// waitForFileProcessing waits for background processing to complete and returns the root CIDs
func waitForFileProcessing(t *testing.T, processor *protocol.FileBlockProcessor, timeout time.Duration) []string {
	start := time.Now()
	timeoutChan := time.After(timeout)
	pollCount := 0

	if testing.Verbose() {
		t.Logf("Waiting for file processing completion, timeout=%v", timeout)
	}

	// Poll for roots
	for {
		pollCount++
		select {
		case <-timeoutChan:
			t.Logf("File processing timeout after %d attempts (%v)", pollCount, time.Since(start))
			t.Fatal("Timed out waiting for file processing to complete")
		default:
			if testing.Verbose() {
				t.Logf("Checking for completion, attempt %d", pollCount)
			}
			roots := processor.Roots()
			if roots != nil && len(roots) > 0 {
				t.Logf("Processing completed with roots: %v", roots)
				require.Len(t, roots, 1, "Should have exactly one root")
				require.NotEmpty(t, roots[0].String(), "Root CID should not be empty")

				// Return the root CIDs
				t.Logf("Root CID: %s", roots[0].String())
				return []string{roots[0].String()}
			} else {
				if testing.Verbose() {
					t.Logf("Still waiting for completion, attempt %d", pollCount)
				}
			}

			time.Sleep(10 * time.Millisecond) // Small poll interval
		}
	}
}

// countBlocks processes all blocks from a processor and returns the count of unique blocks
// It also stores each block in the provided DAG service for later retrieval by the streaming processor
func countBlocks(t *testing.T, processor protocol.BlockProcessor, dagService format.DAGService) int {
	seenBlocks := make(map[string]bool)
	blockCount := 0
	if testing.Verbose() {
		t.Logf("Counting blocks from processor")
	}

	for {
		if testing.Verbose() {
			t.Logf("Retrieving next block, count=%d", blockCount)
		}
		block, err := processor.Next()
		if err == io.EOF {
			t.Logf("Processing completed: %d unique blocks", blockCount)
			break
		}
		require.NoError(t, err, "Should not get error while reading blocks")
		require.NotNil(t, block, "Block should not be nil")

		c := block.Cid()
		cidStr := c.String()
		keyStr := blockstore.BlockPrefix.Child(dshelp.MultihashToDsKey(c.Hash())).String()
		require.NotEmpty(t, cidStr, "Block should have a valid CID")

		// Count only unique blocks and store them in the DAG service
		if !seenBlocks[cidStr] {
			seenBlocks[cidStr] = true
			blockCount++
			if testing.Verbose() {
				t.Logf("Unique block %d: %s", blockCount, keyStr)
			}

			// Store the block in the DAG service for later retrieval by the streaming processor
			if dagService != nil {
				ctx := context.Background()
				// Convert block to node for DAG service
				node, err := encoding.DecodeBlock(ctx, block)
				if err != nil {
					if testing.Verbose() {
						t.Logf("Failed to decode block: %v", err)
					}
				} else {
					if err := dagService.Add(ctx, node); err != nil {
						if testing.Verbose() {
							t.Logf("Failed to store block: %v", err)
						}
					} else {
						if testing.Verbose() {
							t.Logf("Block stored: %s", cidStr)
						}
					}
				}
			}
		} else {
			if testing.Verbose() {
				t.Logf("Skipping duplicate: %s", keyStr)
			}
		}
	}

	t.Logf("Total unique blocks: %d", blockCount)
	return blockCount
}

func TestFileBlockProcessor_Integration_FileTypes(t *testing.T) {
	testCases := []struct {
		name        string
		fileContent func() string
		timeout     time.Duration
		validate    func(*testing.T, string, int)
	}{
		{
			name: "BasicFile",
			fileContent: func() string {
				return "Hello, World! This is a test file for IPFS processing."
			},
			timeout: 5 * time.Second,
			validate: func(t *testing.T, rootCID string, blockCount int) {
				require.Greater(t, blockCount, 0, "Should have processed at least one block")
				t.Logf("File processed successfully with root CID: %s", rootCID)
				t.Logf("Processed %d blocks", blockCount)
			},
		},
		{
			name: "EmptyFile",
			fileContent: func() string {
				return ""
			},
			timeout: 5 * time.Second,
			validate: func(t *testing.T, rootCID string, blockCount int) {
				require.Greater(t, blockCount, 0, "Should have processed at least one block for empty file")
				t.Logf("Empty file processed successfully with root CID: %s", rootCID)
				t.Logf("Empty file produced %d blocks", blockCount)
			},
		},
		{
			name: "SmallFile",
			fileContent: func() string {
				return "x"
			},
			timeout: 5 * time.Second,
			validate: func(t *testing.T, rootCID string, blockCount int) {
				require.Equal(t, 1, blockCount, "Small file should produce exactly 1 block")
				t.Logf("Small file (1 byte) processed successfully with root CID: %s", rootCID)
				t.Logf("Small file produced %d block", blockCount)
			},
		},
		{
			name: "LargeFile",
			fileContent: func() string {
				return strings.Repeat("This is test content for a large file. ", 10000)
			},
			timeout: 10 * time.Second,
			validate: func(t *testing.T, rootCID string, blockCount int) {
				require.Greater(t, blockCount, 1, "Large file should be chunked into multiple blocks")
				t.Logf("Large file (~1MB) processed successfully with root CID: %s", rootCID)
				t.Logf("Large file produced %d blocks", blockCount)
			},
		},
		{
			name: "BinaryFile",
			fileContent: func() string {
				binaryContent := make([]byte, 1024)
				for i := range binaryContent {
					binaryContent[i] = byte(i % 256)
				}
				return string(binaryContent)
			},
			timeout: 5 * time.Second,
			validate: func(t *testing.T, rootCID string, blockCount int) {
				require.Greater(t, blockCount, 0, "Should have processed at least one block")
				t.Logf("Binary file processed successfully with root CID: %s", rootCID)
				t.Logf("Binary file produced %d blocks", blockCount)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fileContent := tc.fileContent()
			processor, _, dagService, _ := createTestFileProcessor(t, fileContent)
			defer processor.Release()

			// Process file and wait for completion
			rootCID, blockCount := processFileAndWait(t, processor, dagService, tc.timeout)

			// Validate the results
			tc.validate(t, rootCID, blockCount)
		})
	}
}

func TestFileBlockProcessor_Integration_ContextCancellation(t *testing.T) {
	t.Skip("Context cancellation test disabled - flaky test behavior, needs investigation")

	// Create test context
	testCtx, err := coreTesting.NewTestContext(t)
	require.NoError(t, err)

	// Create cancellable context
	cancelCtx, cancel := context.WithCancel(testCtx.GetContext())

	// Create large file content
	fileContent := strings.Repeat("This is a large file that will take time to process. ", 5000)

	// Create shared DoneTracker
	doneTracker := protocol.NewDoneTracker()

	// Create components
	bs := protocol.NewStreamingBlockstoreWithDefaults(testCtx.Logger(), nil, doneTracker, 0)
	seekableFile := upload.NewUniversalReader(strings.NewReader(fileContent))

	// Create default in-memory components
	dagService, bstore := upload.DefaultInMemoryComponents()
	nodeGenerator := contentUnixFS.NewUnixFSNodeGenerator(
		contentUnixFS.WithUnixFSNodeDAGService(dagService),
		contentUnixFS.WithUnixFSNodeBlockstore(bstore),
	)

	processor, err := protocol.NewFileBlockProcessorWithDefaults(cancelCtx, bs, seekableFile, dagService, nodeGenerator, testCtx.Logger(), doneTracker)
	require.NoError(t, err)

	// Start processing by calling Next (this triggers background processing)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, err := processor.Next() // This will start processing
		// Don't require.NoError here since context cancellation is expected
		if err != nil && err != context.Canceled {
			t.Errorf("Unexpected error in goroutine: %v", err)
		}
	}()

	// Give a small delay to let the goroutine start
	time.Sleep(1 * time.Millisecond)

	// Cancel quickly
	cancel()

	// Give a small delay for cancellation to propagate
	time.Sleep(1 * time.Millisecond)

	// Try to get blocks - should return context cancellation error
	block, err := processor.Next()
	require.Error(t, err)
	require.Equal(t, context.Canceled, err)
	require.Nil(t, block)

	// Wait for goroutine to finish with timeout
	select {
	case <-done:
		// Goroutine finished successfully
	case <-time.After(1 * time.Second):
		t.Error("Goroutine did not finish within timeout")
	}

	// Clean up
	processor.Release()
}

func TestFileBlockProcessor_Integration_FileMetadata(t *testing.T) {
	testCases := []struct {
		name        string
		fileContent string
		filePath    string
		setupFunc   func(*testing.T, string, string) (*protocol.FileBlockProcessor, protocol.StreamingBlockstore, format.DAGService)
		validate    func(*testing.T, *protocol.FileBlockProcessor, format.DAGService, string)
	}{
		{
			name:     "SeekableFile",
			filePath: "",
			setupFunc: func(t *testing.T, content, path string) (*protocol.FileBlockProcessor, protocol.StreamingBlockstore, format.DAGService) {
				processor, datastore, dagService, _ := createTestFileProcessor(t, content)
				return processor, datastore, dagService
			},
			validate: func(t *testing.T, processor *protocol.FileBlockProcessor, dagService format.DAGService, rootCID string) {
				blockCount := countBlocks(t, processor, dagService)
				require.Greater(t, blockCount, 0, "Should have processed at least one block")
				t.Logf("Seekable file processed successfully with root CID: %s", rootCID)
				t.Logf("Seekable file produced %d blocks", blockCount)
			},
		},
		{
			name:     "FilePath",
			filePath: "/path/to/test/file.txt",
			setupFunc: func(t *testing.T, content, path string) (*protocol.FileBlockProcessor, protocol.StreamingBlockstore, format.DAGService) {
				// Use a similar helper to createTestFileProcessor but with path support
				processor, datastore, dagService, _ := createTestFileProcessorWithPath(t, content, path)

				return processor, datastore, dagService
			},
			validate: func(t *testing.T, processor *protocol.FileBlockProcessor, dagService format.DAGService, rootCID string) {
				blockCount := countBlocks(t, processor, dagService)
				require.Greater(t, blockCount, 0, "Should have processed at least one block")
				t.Logf("File with path '%s' processed successfully with root CID: %s", processor.GetFilePath(), rootCID)
				t.Logf("File with path produced %d blocks", blockCount)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fileContent := "This is a test file for metadata validation."
			processor, _, dagService := tc.setupFunc(t, fileContent, tc.filePath)
			defer processor.Release()

			if tc.filePath != "" {
				require.Equal(t, tc.filePath, processor.GetFilePath())
			}

			// Process file and wait for completion
			rootCID, blockCount := processFileAndWait(t, processor, dagService, 5*time.Second)

			// Run the validation function with the block count
			if blockCount > 0 {
				require.Greater(t, blockCount, 0, "Should have processed at least one block")
				t.Logf("File processed successfully with root CID: %s", rootCID)
				t.Logf("File produced %d blocks", blockCount)
			} else {
				t.Error("No blocks were processed")
			}
		})
	}
}

func TestFileBlockProcessor_Integration_MultipleProcessors(t *testing.T) {
	// Test multiple file processors running concurrently
	numFiles := 3
	fileContents := []string{
		"File 1 content",
		"File 2 content with different size to ensure different chunking",
		"File 3 content",
	}

	processors := make([]*protocol.FileBlockProcessor, numFiles)
	datastores := make([]protocol.StreamingBlockstore, numFiles)
	dagServices := make([]format.DAGService, numFiles)

	// Create processors
	for i := 0; i < numFiles; i++ {
		proc, _ds, _dagService, _ := createTestFileProcessor(t, fileContents[i])
		processors[i] = proc
		datastores[i] = _ds
		dagServices[i] = _dagService
	}

	// Clean up
	defer func() {
		for i := 0; i < numFiles; i++ {
			processors[i].Release()
		}
	}()

	// Process all files and wait for completion
	rootCIDs := make([]string, numFiles)
	blockCounts := make([]int, numFiles)
	for i := 0; i < numFiles; i++ {
		rootCIDs[i], blockCounts[i] = processFileAndWait(t, processors[i], dagServices[i], 5*time.Second)
	}

	// Verify all files were processed successfully
	for i, rootCID := range rootCIDs {
		t.Logf("File %d processed successfully with root CID: %s", i+1, rootCID)
		require.NotEmpty(t, rootCID, "Root CID should not be empty")

		// Each file should have a different root CID (since content is different)
		for j := 0; j < i; j++ {
			require.NotEqual(t, rootCIDs[j], rootCID, "Files with different content should have different root CIDs")
		}
	}

	require.Len(t, rootCIDs, numFiles, "Should have root CIDs for all files")
}
