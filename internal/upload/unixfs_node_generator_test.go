package upload

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/docker/go-units"
	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap/zaptest"
)

// setupNodeGeneratorTest creates real in-memory IPFS components for testing
func setupNodeGeneratorTest(t *testing.T) (UnixFSNodeGenerator, context.Context, func()) {
	ctx := context.Background()
	logger := &core.Logger{Logger: zaptest.NewLogger(t)}

	// Create real in-memory implementations
	dstore := dssync.MutexWrap(ds.NewMapDatastore())
	bstore := blockstore.NewBlockstore(dstore)
	dagService := merkledag.NewDAGService(blockservice.New(bstore, offline.Exchange(bstore)))

	// Create the generator with real components
	generator := NewUnixFSNodeGenerator(dagService, logger)

	// Return cleanup function
	cleanup := func() {
		err := dstore.Close()
		require.NoError(t, err)
	}

	return generator, ctx, cleanup
}

// TestNewUnixFSNodeGenerator tests the constructor with real components
func TestNewUnixFSNodeGenerator(t *testing.T) {
	generator, _, cleanup := setupNodeGeneratorTest(t)
	defer cleanup()

	require.NotNil(t, generator)

	// Type assert to check implementation
	_, ok := generator.(*IPFSUnixFSNodeGenerator)
	assert.True(t, ok, "Should return IPFSUnixFSNodeGenerator implementation")
}

// TestIPFSUnixFSNodeGenerator_CreateDirectory tests directory creation with real components
func TestIPFSUnixFSNodeGenerator_CreateDirectory(t *testing.T) {
	tests := []struct {
		name        string
		expectError bool
	}{
		{
			name:        "successful directory creation",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			generator, _, cleanup := setupNodeGeneratorTest(t)
			defer cleanup()

			dir, err := generator.CreateDirectory()

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, dir)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, dir)

				// Verify it's a UnixFS directory by getting its node
				node, err := dir.GetNode()
				assert.NoError(t, err)
				assert.NotNil(t, node)
				assert.NotEqual(t, cid.Undef, node.Cid())
			}
		})
	}
}

// TestIPFSUnixFSNodeGenerator_CreateNode tests basic node creation with real components
func TestIPFSUnixFSNodeGenerator_CreateNode(t *testing.T) {
	tests := []struct {
		name         string
		content      []byte
		expectError  bool
		expectedSize int64
	}{
		{
			name:         "small content",
			content:      []byte("hello world"),
			expectError:  false,
			expectedSize: 11,
		},
		{
			name:         "empty content",
			content:      []byte(""),
			expectError:  false,
			expectedSize: 0,
		},
		{
			name:         "medium content",
			content:      bytes.Repeat([]byte("test content "), 100),
			expectError:  false,
			expectedSize: 1300, // 13 bytes * 100
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			generator, ctx, cleanup := setupNodeGeneratorTest(t)
			defer cleanup()

			reader := io.NopCloser(bytes.NewReader(tt.content))

			node, err := generator.CreateNode(ctx, NewUniversalReader(reader))

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, node)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, node)
				assert.NotEqual(t, cid.Undef, node.Cid())

				// Verify node size
				nodeSize, err := node.Size()
				assert.NoError(t, err)
				assert.GreaterOrEqual(t, nodeSize, uint64(tt.expectedSize)) // UnixFS adds metadata overhead
			}
		})
	}
}

// TestIPFSUnixFSNodeGenerator_CreateUnixFSNode tests node creation with custom parameters
func TestIPFSUnixFSNodeGenerator_CreateUnixFSNode(t *testing.T) {
	tests := []struct {
		name         string
		content      []byte
		maxLinks     int
		chunkSize    int64
		expectError  bool
		expectedSize int64
	}{
		{
			name:         "custom maxlinks and chunksize",
			content:      bytes.Repeat([]byte("test"), 1000),
			maxLinks:     100,
			chunkSize:    512,
			expectError:  false,
			expectedSize: 4000, // "test" * 1000
		},
		{
			name:         "small chunk size",
			content:      []byte("small content"),
			maxLinks:     10,
			chunkSize:    64,
			expectError:  false,
			expectedSize: 14,
		},
		{
			name:         "large maxlinks",
			content:      bytes.Repeat([]byte("x"), 10000),
			maxLinks:     1000,
			chunkSize:    1024,
			expectError:  false,
			expectedSize: 10000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			generator, ctx, cleanup := setupNodeGeneratorTest(t)
			defer cleanup()

			reader := io.NopCloser(bytes.NewReader(tt.content))

			node, err := generator.CreateUnixFSNode(ctx, NewUniversalReader(reader), tt.maxLinks, tt.chunkSize)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, node)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, node)
				assert.NotEqual(t, cid.Undef, node.Cid())

				// Verify node size
				nodeSize, err := node.Size()
				assert.NoError(t, err)
				assert.GreaterOrEqual(t, nodeSize, uint64(tt.expectedSize))
			}
		})
	}
}

// TestIPFSUnixFSNodeGenerator_CreateDAGFromReader tests the core DAG creation logic with real components
func TestIPFSUnixFSNodeGenerator_CreateDAGFromReader(t *testing.T) {
	tests := []struct {
		name         string
		content      []byte
		maxLinks     int
		chunkSize    int64
		rawLeaves    bool
		expectError  bool
		expectedSize int64
	}{
		{
			name:         "raw leaves false",
			content:      []byte("test content"),
			maxLinks:     10,
			chunkSize:    256,
			rawLeaves:    false,
			expectError:  false,
			expectedSize: 12,
		},
		{
			name:         "raw leaves true",
			content:      []byte("test content"),
			maxLinks:     10,
			chunkSize:    256,
			rawLeaves:    true,
			expectError:  false,
			expectedSize: 12,
		},
		{
			name:         "large content with raw leaves",
			content:      bytes.Repeat([]byte("large content chunk"), 1000),
			maxLinks:     100,
			chunkSize:    512,
			rawLeaves:    true,
			expectError:  false,
			expectedSize: 19000, // 19 bytes * 1000
		},
		{
			name:         "zero chunk size",
			content:      []byte("test"),
			maxLinks:     10,
			chunkSize:    0,
			rawLeaves:    false,
			expectError:  false,
			expectedSize: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			generator, ctx, cleanup := setupNodeGeneratorTest(t)
			defer cleanup()

			reader := bytes.NewReader(tt.content)

			node, err := generator.CreateDAGFromReader(ctx, reader, tt.maxLinks, tt.chunkSize, tt.rawLeaves)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, node)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, node)
				assert.NotEqual(t, cid.Undef, node.Cid())

				// Verify node size
				nodeSize, err := node.Size()
				assert.NoError(t, err)
				assert.GreaterOrEqual(t, nodeSize, uint64(tt.expectedSize))
			}
		})
	}
}

// TestIPFSUnixFSNodeGenerator_ContextCancellation tests context cancellation scenarios with real components
func TestIPFSUnixFSNodeGenerator_ContextCancellation(t *testing.T) {
	tests := []struct {
		name         string
		cancelBefore bool
		cancelAfter  bool
		method       string
	}{
		{
			name:         "CreateNode cancelled before",
			cancelBefore: true,
			cancelAfter:  false,
			method:       "CreateNode",
		},
		{
			name:         "CreateUnixFSNode cancelled before",
			cancelBefore: true,
			cancelAfter:  false,
			method:       "CreateUnixFSNode",
		},
		{
			name:         "CreateDAGFromReader cancelled before",
			cancelBefore: true,
			cancelAfter:  false,
			method:       "CreateDAGFromReader",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			generator, _, cleanup := setupNodeGeneratorTest(t)
			defer cleanup()

			ctx, cancel := context.WithCancel(context.Background())

			if tt.cancelBefore {
				cancel() // Cancel before calling
			}

			content := []byte("test content")
			reader := io.NopCloser(bytes.NewReader(content))

			var err error
			switch tt.method {
			case "CreateNode":
				_, err = generator.CreateNode(ctx, NewUniversalReader(reader))
			case "CreateUnixFSNode":
				_, err = generator.CreateUnixFSNode(ctx, NewUniversalReader(reader), 10, 256)
			case "CreateDAGFromReader":
				_, err = generator.CreateDAGFromReader(ctx, bytes.NewReader(content), 10, 256, false)
			}

			if tt.cancelBefore {
				assert.Error(t, err)
				assert.True(t, errors.Is(err, context.Canceled))
			}

			if !tt.cancelBefore {
				cancel()
			}
		})
	}
}

// TestIPFSUnixFSNodeGenerator_LargeContentFallback tests the fallback logic for large content with real components
func TestIPFSUnixFSNodeGenerator_LargeContentFallback(t *testing.T) {
	// Create content that might trigger verifcid.ErrDigestTooLarge
	// This is typically content with very large chunks that exceed digest limits
	largeContent := bytes.Repeat([]byte("x"), 2*1024*1024) // 2MB of 'x's

	tests := []struct {
		name        string
		content     []byte
		expectError bool
	}{
		{
			name:        "normal content no retry",
			content:     []byte("normal content"),
			expectError: false,
		},
		{
			name:        "large content should succeed with fallback",
			content:     largeContent,
			expectError: false, // Should succeed with rawLeaves=true fallback
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			generator, ctx, cleanup := setupNodeGeneratorTest(t)
			defer cleanup()

			reader := io.NopCloser(bytes.NewReader(tt.content))

			node, err := generator.CreateUnixFSNode(ctx, NewUniversalReader(reader), 10, 1024*1024) // 1MB chunks

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, node)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, node)
				assert.NotEqual(t, cid.Undef, node.Cid())
			}
		})
	}
}

// TestIPFSUnixFSNodeGenerator_InvalidReaderScenarios tests invalid reader scenarios with real components
func TestIPFSUnixFSNodeGenerator_InvalidReaderScenarios(t *testing.T) {
	tests := []struct {
		name        string
		reader      io.ReadSeekCloser
		expectError bool
	}{
		{
			name:        "nil reader",
			reader:      nil,
			expectError: true,
		},
		{
			name:        "reader that fails on read",
			reader:      &failingReader{readErr: errors.New("read failed")},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			generator, ctx, cleanup := setupNodeGeneratorTest(t)
			defer cleanup()

			_, err := generator.CreateNode(ctx, tt.reader)

			if tt.expectError {
				assert.Error(t, err)
			}
		})
	}
}

// TestIPFSUnixFSNodeGenerator_VariousContentSizes tests different content sizes with real components
func TestIPFSUnixFSNodeGenerator_VariousContentSizes(t *testing.T) {
	sizes := []struct {
		name string
		size int
		desc string
	}{
		{"empty", 0, "empty"},
		{"single", 1, "single byte"},
		{"small", 256, "small"},
		{"1KB", 1024, "1KB"},
		{"10KB", 10 * 1024, "10KB"},
		{"1MB", 1024 * 1024, "1MB"},
		{"5MB", 5 * 1024 * 1024, "5MB"},
	}

	for _, sizeTest := range sizes {
		t.Run(fmt.Sprintf("size_%s", sizeTest.name), func(t *testing.T) {
			generator, ctx, cleanup := setupNodeGeneratorTest(t)
			defer cleanup()

			content := make([]byte, sizeTest.size)
			if sizeTest.size > 0 {
				// Fill with some pattern
				for i := range content {
					content[i] = byte(i % 256)
				}
			}

			reader := io.NopCloser(bytes.NewReader(content))

			node, err := generator.CreateNode(ctx, NewUniversalReader(reader))

			assert.NoError(t, err)
			assert.NotNil(t, node)
			assert.NotEqual(t, cid.Undef, node.Cid())

			// Verify node size
			nodeSize, err := node.Size()
			assert.NoError(t, err)
			assert.GreaterOrEqual(t, nodeSize, uint64(sizeTest.size))

			t.Logf("Created node for %s content: CID=%s, NodeSize=%d bytes",
				sizeTest.desc, node.Cid().String(), nodeSize)
		})
	}
}

// TestIPFSUnixFSNodeGenerator_ParameterComparison tests parameter differences within unit scope
func TestIPFSUnixFSNodeGenerator_ParameterComparison(t *testing.T) {
	generator, ctx, cleanup := setupNodeGeneratorTest(t)
	defer cleanup()

	content := []byte("Hello, World! This is a test for parameter comparison.")

	// Test CreateNode (uses defaults)
	reader1 := io.NopCloser(bytes.NewReader(content))
	node1, err := generator.CreateNode(ctx, NewUniversalReader(reader1))
	require.NoError(t, err)
	require.NotNil(t, node1)

	// Test CreateUnixFSNode with custom parameters
	reader2 := io.NopCloser(bytes.NewReader(content))
	node2, err := generator.CreateUnixFSNode(ctx, NewUniversalReader(reader2), 50, 512)
	require.NoError(t, err)
	require.NotNil(t, node2)

	// Both should create valid nodes
	assert.NotEqual(t, cid.Undef, node1.Cid())
	assert.NotEqual(t, cid.Undef, node2.Cid())

	// Test that different parameters can produce different results
	t.Logf("Default params: CID=%s", node1.Cid().String())
	t.Logf("Custom params: CID=%s", node2.Cid().String())
}

// TestIPFSUnixFSNodeGenerator_PerformanceEdgeCases tests performance-related edge cases with real components
func TestIPFSUnixFSNodeGenerator_PerformanceEdgeCases(t *testing.T) {
	tests := []struct {
		name          string
		maxLinks      int
		chunkSize     int64
		expectError   bool
		expectedError string
	}{
		{
			name:        "zero maxlinks",
			maxLinks:    0,
			chunkSize:   256,
			expectError: false, // Should still work with reasonable defaults
		},
		{
			name:        "negative maxlinks",
			maxLinks:    -1,
			chunkSize:   256,
			expectError: false, // IPFS library should handle this
		},
		{
			name:        "very large maxlinks",
			maxLinks:    1000000,
			chunkSize:   256,
			expectError: false, // Should work but might be slow
		},
		{
			name:        "very small chunk size",
			maxLinks:    10,
			chunkSize:   1,
			expectError: false, // Should create many small blocks
		},
		{
			name:        "very large chunk size",
			maxLinks:    10,
			chunkSize:   100 * 1024 * 1024, // 100MB
			expectError: false,             // Should work for small content
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			generator, ctx, cleanup := setupNodeGeneratorTest(t)
			defer cleanup()

			content := []byte("test content")
			reader := io.NopCloser(bytes.NewReader(content))

			node, err := generator.CreateUnixFSNode(ctx, NewUniversalReader(reader), tt.maxLinks, tt.chunkSize)

			if tt.expectError {
				assert.Error(t, err)
				if tt.expectedError != "" {
					assert.Contains(t, err.Error(), tt.expectedError)
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, node)
				assert.NotEqual(t, cid.Undef, node.Cid())
			}
		})
	}
}

// TestIPFSUnixFSNodeGenerator_RealWorldScenarios tests real-world scenarios with real components
func TestIPFSUnixFSNodeGenerator_RealWorldScenarios(t *testing.T) {
	generator, ctx, cleanup := setupNodeGeneratorTest(t)
	defer cleanup()

	tests := []struct {
		name        string
		contentGen  func() []byte
		description string
	}{
		{
			name: "text_file",
			contentGen: func() []byte {
				return []byte("This is a text file with some content.\nMultiple lines.\nEnd.")
			},
			description: "Simple text file content",
		},
		{
			name: "json_data",
			contentGen: func() []byte {
				return []byte(`{"name": "test", "value": 123, "nested": {"key": "value"}}`)
			},
			description: "JSON structured data",
		},
		{
			name: "binary_data",
			contentGen: func() []byte {
				data := make([]byte, 1024)
				for i := range data {
					data[i] = byte(i % 256)
				}
				return data
			},
			description: "1KB of binary data",
		},
		{
			name: "large_text",
			contentGen: func() []byte {
				text := "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
				return bytes.Repeat([]byte(text), 1000) // ~42KB
			},
			description: "Large repeated text content",
		},
		{
			name: "unicode_content",
			contentGen: func() []byte {
				content := "Hello 世界 🌍 Café résumé naïve 中文 русский العربية"
				return []byte(content)
			},
			description: "Unicode text with various character sets",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			content := tt.contentGen()
			reader := io.NopCloser(bytes.NewReader(content))

			node, err := generator.CreateNode(ctx, NewUniversalReader(reader))

			assert.NoError(t, err)
			assert.NotNil(t, node)
			assert.NotEqual(t, cid.Undef, node.Cid())

			// Verify node size
			nodeSize, err := node.Size()
			assert.NoError(t, err)
			assert.GreaterOrEqual(t, nodeSize, uint64(len(content)))

			t.Logf("Created node for %s: %s - CID: %s, Size: %d bytes",
				tt.name, tt.description, node.Cid().String(), nodeSize)
		})
	}
}

// TestIPFSUnixFSNodeGenerator_ChunkingBehavior tests chunking behavior with different sizes
func TestIPFSUnixFSNodeGenerator_ChunkingBehavior(t *testing.T) {
	generator, ctx, cleanup := setupNodeGeneratorTest(t)
	defer cleanup()

	// Test with different chunk sizes to verify chunking behavior
	chunkSizes := []int64{
		64,        // Very small chunks
		256,       // Small chunks
		1024,      // 1KB chunks
		8 * 1024,  // 8KB chunks
		64 * 1024, // 64KB chunks
	}

	// Create content that spans multiple chunks for most chunk sizes
	content := bytes.Repeat([]byte("This is test content for chunking. "), 500) // ~11.5KB

	for _, chunkSize := range chunkSizes {
		t.Run(fmt.Sprintf("chunk_size_%s", units.HumanSize(float64(chunkSize))), func(t *testing.T) {
			reader := io.NopCloser(bytes.NewReader(content))

			node, err := generator.CreateUnixFSNode(ctx, NewUniversalReader(reader), 10, chunkSize)

			assert.NoError(t, err)
			assert.NotNil(t, node)
			assert.NotEqual(t, cid.Undef, node.Cid())

			nodeSize, err := node.Size()
			assert.NoError(t, err)
			assert.GreaterOrEqual(t, nodeSize, uint64(len(content)))

			t.Logf("Chunk size %s: CID=%s, NodeSize=%d bytes",
				units.HumanSize(float64(chunkSize)), node.Cid().String(), nodeSize)
		})
	}
}

// Helper types for testing

// failingReader simulates a reader that fails on read
type failingReader struct {
	readErr error
	pos     int64
}

func (r *failingReader) Read(p []byte) (int, error) {
	if r.readErr != nil {
		return 0, r.readErr
	}
	return 0, io.EOF
}

func (r *failingReader) Seek(offset int64, whence int) (int64, error) {
	return 0, errors.New("seek not supported")
}

func (r *failingReader) Close() error {
	return nil
}

// failingSeekReader simulates a reader that can fail on seek
type failingSeekReader struct {
	data      []byte
	pos       int64
	seekFails bool
	closed    bool
}
