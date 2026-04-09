package tests

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/ipld/merkledag"
	dssync "github.com/ipfs/go-datastore/sync"
	format "github.com/ipfs/go-ipld-format"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/upload"
	coreTesting "go.lumeweb.com/portal/core/testing"
	contentArchive "go.lumeweb.com/ipfs-content/archive"
	contentUnixFS "go.lumeweb.com/ipfs-content/unixfs"
)

// simulateImageContent creates a simple PNG image using the standard library
func simulateImageContent(name string, width, height int) []byte {
	// Create a new RGBA image
	img := image.NewRGBA(image.Rect(0, 0, width, height))

	// Generate different patterns based on the image name for variety
	switch {
	case name == "logo":
		// Create a simple gradient logo
		for y := 0; y < height; y++ {
			for x := 0; x < width; x++ {
				// Blue to cyan gradient
				img.Set(x, y, color.RGBA{0, uint8(255 * x / width), 255, 255})
			}
		}
	case name == "banner":
		// Create a striped banner pattern
		for y := 0; y < height; y++ {
			for x := 0; x < width; x++ {
				if (x/50)%2 == 0 {
					img.Set(x, y, color.RGBA{255, 100, 50, 255}) // Orange
				} else {
					img.Set(x, y, color.RGBA{50, 100, 255, 255}) // Blue
				}
			}
		}
	case name == "favicon":
		// Create a simple square favicon
		for y := 0; y < height; y++ {
			for x := 0; x < width; x++ {
				if x < width/2 && y < height/2 {
					img.Set(x, y, color.RGBA{255, 0, 0, 255}) // Red
				} else if x >= width/2 && y < height/2 {
					img.Set(x, y, color.RGBA{0, 255, 0, 255}) // Green
				} else if x < width/2 && y >= height/2 {
					img.Set(x, y, color.RGBA{0, 0, 255, 255}) // Blue
				} else {
					img.Set(x, y, color.RGBA{255, 255, 0, 255}) // Yellow
				}
			}
		}
	default:
		// Default: simple checkerboard pattern
		for y := 0; y < height; y++ {
			for x := 0; x < width; x++ {
				if (x/10+y/10)%2 == 0 {
					img.Set(x, y, color.RGBA{200, 200, 200, 255}) // Light gray
				} else {
					img.Set(x, y, color.RGBA{100, 100, 100, 255}) // Dark gray
				}
			}
		}
	}

	// Encode the image to PNG format in a buffer
	var buf bytes.Buffer
	err := png.Encode(&buf, img)
	if err != nil {
		// Fallback to simple byte pattern if encoding fails
		return []byte("fallback-image-data-" + name)
	}

	return buf.Bytes()
}

// createTestArchiveProcessor creates a processor with real components for the given archive data
func createTestArchiveProcessor(t *testing.T, archiveData []byte) (*protocol.ArchiveBlockProcessor, upload.StreamingArchiveProcessor, format.DAGService, coreTesting.TestContext) {
	// Create test context
	ctx, err := coreTesting.NewTestContext(t)
	require.NoError(t, err)

	// Register extractors
	contentArchive.RegisterTarExtractor()
	contentArchive.RegisterZipExtractor()

	// Create extractor for the archive
	extractor, err := contentArchive.CreateExtractor(bytes.NewReader(archiveData))
	require.NoError(t, err)

	// Create done tracker for tracking completion
	doneTracker := protocol.NewDoneTracker()

	// Create archive blockstore (used for coordination) and get the shared passthrough datastore
	archiveBlockstore, passthroughDatastore, _ := createTestArchiveBlockstore(t, doneTracker, true)

	// Create archive DAG service (used by streaming processor for coordination)
	archiveDagService := merkledag.NewDAGService(
		blockservice.New(archiveBlockstore, offline.Exchange(archiveBlockstore)),
	)

	// Create raw DAG service using the same shared passthrough datastore (used by test)
	rawBstore := blockstore.NewBlockstore(dssync.MutexWrap(passthroughDatastore))
	rawDagService := merkledag.NewDAGService(
		blockservice.New(rawBstore, offline.Exchange(rawBstore)),
	)

	// Create UnixFS node generator manually (using archive DAG service for streaming processor)
	nodeGenerator := contentUnixFS.NewUnixFSNodeGenerator(
		contentUnixFS.WithUnixFSNodeDAGService(archiveDagService),
		contentUnixFS.WithUnixFSNodeBlockstore(archiveBlockstore),
	)

	// Create streaming processor with the manual components (using archive DAG service)
	streamProcessor := upload.NewStreamingProcessorWithOptions(
		upload.WithStreamingProcessorNodeGenerator(nodeGenerator),
		upload.WithStreamingProcessorDAGService(archiveDagService),
		upload.WithStreamingProcessorBlockstore(archiveBlockstore),
		upload.WithStreamingProcessorLogger(ctx.Logger()),
	)

	// Create the processor using the archiveBlockstore directly since it now implements the StreamingBlockstore interface
	processor, err := protocol.NewArchiveBlockProcessor(
		ctx.GetContext(),
		archiveBlockstore,
		extractor,
		streamProcessor,
		ctx.Logger(),
		doneTracker,
	)
	require.NoError(t, err)

	return processor, streamProcessor, rawDagService, ctx
}

// processArchiveAndWait processes an archive and waits for completion, returning root CID and block count
func processArchiveAndWait(t *testing.T, processor *protocol.ArchiveBlockProcessor, streamProcessor upload.StreamingArchiveProcessor, dagService format.DAGService, timeout time.Duration, expectedFiles []upload.TestFile) (string, int) {
	// Start processing in background goroutine
	blockCountChan := startProcessingAndCountBlocks(t, processor, dagService)

	// Wait for archive processing to get the root CID
	rootCID := waitForProcessing(t, processor)[0]

	// Verify the processed files match our expectations
	if testing.Verbose() {
		t.Logf("Verifying processed files")
	}
	verifyProcessedFiles(t, streamProcessor, expectedFiles)

	// Get the block count from the background goroutine with timeout
	select {
	case blockCount := <-blockCountChan:
		return rootCID, blockCount
	case <-time.After(timeout):
		t.Fatal("Timed out waiting for block counting to complete")
		return "", 0 // Unreachable, but satisfies compiler
	}
}

// waitForProcessing waits for background processing to complete and returns the root CIDs
func waitForProcessing(t *testing.T, processor *protocol.ArchiveBlockProcessor) []string {
	// Poll for roots instead of sleeping
	roots := processor.Roots()
	start := time.Now()
	timeout := 5 * time.Second
	for roots == nil {
		if time.Since(start) > timeout {
			t.Fatal("Timed out waiting for roots to become available")
		}
		time.Sleep(50 * time.Millisecond) // Small poll interval
		roots = processor.Roots()
	}

	require.Len(t, roots, 1, "Should have exactly one root")
	require.NotEmpty(t, roots[0].String(), "Root CID should not be empty")

	// Return the root CIDs
	return []string{roots[0].String()}
}

// verifyProcessedFiles verifies that the processed files match the expected test files
func verifyProcessedFiles(t *testing.T, streamProcessor upload.StreamingArchiveProcessor, expectedFiles []upload.TestFile) {
	processedFiles := streamProcessor.GetProcessedFiles()

	// Filter expected files to only include files (not directories) since GetProcessedFiles only returns files
	expectedFileMap := make(map[string]upload.TestFile)
	expectedFileCount := 0
	for _, file := range expectedFiles {
		if !file.IsDir {
			expectedFileMap[file.Name] = file
			expectedFileCount++
		}
	}

	// Verify we have the expected number of processed files (files only, not directories)
	require.Equal(t, expectedFileCount, len(processedFiles),
		"Expected %d processed files, got %d", expectedFileCount, len(processedFiles))

	// Create a helper to extract basename from path
	getBasename := func(path string) string {
		lastSlash := strings.LastIndex(path, "/")
		if lastSlash != -1 {
			return path[lastSlash+1:]
		}
		return path
	}

	// Verify each processed file matches expectations
	for _, processedFile := range processedFiles {
		expected, exists := expectedFileMap[processedFile.Path]
		require.True(t, exists, "Unexpected processed file: %s", processedFile.Path)

		// Verify file properties - Name should be the basename of the expected file name
		expectedBasename := getBasename(expected.Name)
		require.Equal(t, expectedBasename, processedFile.Name, "Name mismatch for %s", processedFile.Path)
		require.False(t, processedFile.IsDir, "Processed files should not be directories: %s", processedFile.Path)
		require.Equal(t, expected.Mode, processedFile.Mode, "Mode mismatch for %s", processedFile.Path)
		require.True(t, processedFile.Processed, "File should be marked as processed: %s", processedFile.Path)
		require.NoError(t, processedFile.Error, "File should have no processing errors: %s", processedFile.Path)

		// For files, verify size matches content length
		expectedSize := int64(len(expected.Content))
		require.Equal(t, expectedSize, processedFile.Size, "Size mismatch for %s", processedFile.Path)
		require.NotEmpty(t, processedFile.CID, "File should have a CID: %s", processedFile.Path)

		// Verify parent path is correct - "." represents the root to the processor
		if strings.Contains(expected.Name, "/") {
			// Nested file should have correct parent path
			expectedParent := ""
			lastSlash := strings.LastIndex(expected.Name, "/")
			if lastSlash != -1 {
				expectedParent = expected.Name[:lastSlash]
			}
			require.Equal(t, expectedParent, processedFile.ParentPath, "Parent path mismatch for %s", processedFile.Path)
		} else {
			// Root level file should have "." as parent path (represents root to the processor)
			require.Equal(t, ".", processedFile.ParentPath, "Root level file should have '.' as parent path: %s", processedFile.Path)
		}

		if testing.Verbose() {
			t.Logf("Verified file %s: size=%d, cid=%s",
				processedFile.Path, processedFile.Size, processedFile.CID)
		}
	}
}

func TestArchiveBlockProcessor_Integration_ArchiveFormats(t *testing.T) {
	tests := []struct {
		name      string
		testFiles []upload.TestFile
		create    upload.ArchiveCreator
	}{
		{
			name:      "TAR with default files",
			testFiles: upload.GetDefaultTestFiles(),
			create:    upload.CreateTARArchive,
		},
		{
			name:      "ZIP with default files",
			testFiles: upload.GetDefaultTestFiles(),
			create:    upload.CreateZIPArchive,
		},
		{
			name: "TAR with small file",
			testFiles: []upload.TestFile{
				{Name: "small.txt", Content: "x", Mode: 0644},
			},
			create: upload.CreateTARArchive,
		},
		{
			name: "ZIP with small file",
			testFiles: []upload.TestFile{
				{Name: "small.txt", Content: "x", Mode: 0644},
			},
			create: upload.CreateZIPArchive,
		},
		{
			name: "Website content simulation",
			testFiles: []upload.TestFile{
				{Name: "index.html", Content: `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Sample Website</title>
    <link rel="stylesheet" href="styles/main.css">
    <script src="js/app.js"></script>
</head>
<body>
    <header>
        <h1>Welcome to My Website</h1>
    </header>
    <nav>
        <ul>
            <li><a href="/">Home</a></li>
            <li><a href="/about">About</a></li>
            <li><a href="/contact">Contact</a></li>
        </ul>
    </nav>
    <main>
        <section>
            <h2>About Us</h2>
            <p>This is a sample website for testing archive processing.</p>
        </section>
    </main>
    <footer>
        <p>&copy; 2024 My Website</p>
    </footer>
</body>
</html>`, Mode: 0644},
				{Name: "styles/main.css", Content: `* {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
}

body {
    font-family: Arial, sans-serif;
    line-height: 1.6;
    color: #333;
    max-width: 1200px;
    margin: 0 auto;
    padding: 20px;
}

header {
    text-align: center;
    margin-bottom: 40px;
}

nav ul {
    list-style: none;
    display: flex;
    justify-content: center;
    background: #f4f4f4;
    padding: 10px;
    margin-bottom: 30px;
}

nav li {
    margin: 0 10px;
}

nav a {
    text-decoration: none;
    color: #007bff;
    font-weight: bold;
}

nav a:hover {
    color: #0056b3;
}

main {
    min-height: 400px;
}

section {
    margin-bottom: 30px;
}

footer {
    text-align: center;
    margin-top: 40px;
    padding-top: 20px;
    border-top: 1px solid #ddd;
}`, Mode: 0644},
				{Name: "js/app.js", Content: `// Main application JavaScript
document.addEventListener('DOMContentLoaded', function() {
    // Navigation handling
    const nav = document.querySelector('nav');
    const links = nav.querySelectorAll('a');

    links.forEach(link => {
        link.addEventListener('click', function(e) {
            if (this.getAttribute('href').startsWith('/')) {
                e.preventDefault();
                console.log('Navigation to:', this.getAttribute('href'));
                // SPA navigation logic would go here
            }
        });
    });

    // Dynamic content loading
    const main = document.querySelector('main');
    console.log('Website loaded successfully');
});

// Utility functions
function loadContent(url) {
    fetch(url)
        .then(response => response.text())
        .then(html => {
            main.innerHTML = html;
        })
        .catch(error => {
            console.error('Error loading content:', error);
        });
}

// Initialize application
init();`, Mode: 0644},
				{Name: "images/logo.png", Content: string(simulateImageContent("logo", 200, 50)), Mode: 0644},
				{Name: "images/banner.jpg", Content: string(simulateImageContent("banner", 800, 300)), Mode: 0644},
				{Name: "favicon.ico", Content: string(simulateImageContent("favicon", 16, 16)), Mode: 0644},
				{Name: "robots.txt", Content: `User-agent: *
Allow: /
Sitemap: https://example.com/sitemap.xml`, Mode: 0644},
				{Name: "manifest.json", Content: `{
  "name": "Sample Website",
  "short_name": "Sample",
  "description": "A sample website for testing",
  "start_url": "/",
  "display": "standalone",
  "background_color": "#ffffff",
  "theme_color": "#000000"
}`, Mode: 0644},
			},
			create: upload.CreateTARArchive,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test context to get logger
			ctx, err := coreTesting.NewTestContext(t)
			require.NoError(t, err)
			archiveData := tt.create(t, ctx, tt.testFiles)

			processor, streamProcessor, dagService, _ := createTestArchiveProcessor(t, archiveData)
			defer processor.Release()
			defer require.NoError(t, streamProcessor.Close())

			// Process archive and wait for completion
			rootCID, blockCount := processArchiveAndWait(t, processor, streamProcessor, dagService, 5*time.Minute, tt.testFiles)

			t.Logf("%s: root=%s, blocks=%d", tt.name, rootCID, blockCount)
		})
	}
}

func TestArchiveBlockProcessor_Integration_ContextCancellation(t *testing.T) {
	// Create test context
	ctx, err := coreTesting.NewTestContext(t)
	require.NoError(t, err)

	// Create cancellable context
	cancelCtx, cancel := context.WithCancel(ctx.GetContext())

	// Create test archive
	testFiles := upload.GetDefaultTestFiles()
	archiveData := upload.CreateTARArchive(t, ctx, testFiles)

	// Register extractors
	contentArchive.RegisterTarExtractor()

	// Create components
	extractor, err := contentArchive.CreateExtractor(bytes.NewReader(archiveData))
	require.NoError(t, err)

	streamProcessor := upload.NewStreamingProcessorWithDefaults(ctx.Logger())
	archiveBlockstore := protocol.NewStreamingBlockstore(ctx.Logger(), nil)

	processor, err := protocol.NewArchiveBlockProcessor(cancelCtx, archiveBlockstore, extractor, streamProcessor, ctx.Logger(), protocol.NewDoneTracker())
	require.NoError(t, err)
	defer processor.Release()

	// Cancel immediately before any processing
	cancel()

	// Now try to process blocks - should get context cancellation immediately
	block, err := processor.Next()
	require.Error(t, err, "Expected error when processing with cancelled context")
	require.Contains(t, err.Error(), "context canceled", "Expected context cancellation error")
	require.Nil(t, block, "Block should be nil on context cancellation")

	if testing.Verbose() {
		t.Log("Context cancellation test passed")
	}
}

func TestArchiveBlockProcessor_Integration_Scenarios(t *testing.T) {
	tests := []struct {
		name            string
		testFiles       []upload.TestFile
		skipInShortMode bool
		maxDuration     time.Duration
	}{
		{
			name: "Multiple files with subdirectories",
			testFiles: []upload.TestFile{
				{Name: "file1.txt", Content: "Hello World 1", Mode: 0644},
				{Name: "file2.txt", Content: "Hello World 2", Mode: 0644},
				{Name: "file3.txt", Content: "Hello World 3", Mode: 0644},
				{Name: "subdir/file4.txt", Content: "Hello World 4", Mode: 0644},
				{Name: "subdir/file5.txt", Content: "Hello World 5", Mode: 0644},
			},
			skipInShortMode: false,
			maxDuration:     2 * time.Second,
		},
		{
			name: "Large archive with many files",
			testFiles: func() []upload.TestFile {
				files := make([]upload.TestFile, 20)
				for i := 0; i < 20; i++ {
					content := fmt.Sprintf("This is test file %d with some content to make it larger.", i)
					files[i] = upload.TestFile{
						Name:    fmt.Sprintf("large_file_%d.txt", i),
						Content: content,
						Mode:    0644,
					}
				}
				return files
			}(),
			skipInShortMode: true,
			maxDuration:     5 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skipInShortMode && testing.Short() {
				t.Skip("Skipping large archive test in short mode")
			}

			// Create test context to get logger
			ctx, err := coreTesting.NewTestContext(t)
			require.NoError(t, err)
			archiveData := upload.CreateTARArchive(t, ctx, tt.testFiles)

			start := time.Now()
			processor, streamProcessor, dagService, _ := createTestArchiveProcessor(t, archiveData)
			defer processor.Release()
			defer require.NoError(t, streamProcessor.Close())

			// Start processing in background goroutine to consume blocks
			blockCountChan := startProcessingAndCountBlocks(t, processor, dagService)

			// Wait for processing and get root CID
			rootCID := waitForProcessing(t, processor)[0]
			duration := time.Since(start)

			// Get the block count from the background goroutine with timeout
			select {
			case blockCount := <-blockCountChan:
				if testing.Verbose() {
					t.Logf("Background processing completed: %d blocks", blockCount)
				}
			case <-time.After(tt.maxDuration):
				t.Fatal("Timed out waiting for background processing to complete")
			}

			t.Logf("%s: %d files in %v, root=%s",
				tt.name, len(tt.testFiles), duration, rootCID)

			// Verify it processes within reasonable time
			require.Less(t, duration, tt.maxDuration, "Archive should process within time limit")
		})
	}
}
