package upload

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"testing"

	"time"

	"go.lumeweb.com/portal-plugin-ipfs/internal/upload/common"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs"
	unixfsio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/ipfs/boxo/ipld/unixfs/pb"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

// CARTestHelper provides unified test utilities for CAR-related tests
type CARTestHelper struct {
	tb     testing.TB
	ctx    core.Context
	logger *core.Logger
}

// NewCARTestHelper creates a new CAR test helper
func NewCARTestHelper(tb testing.TB) *CARTestHelper {
	ctx, err := coreTesting.NewTestContext(tb)
	require.NoError(tb, err)

	return &CARTestHelper{
		tb:     tb,
		ctx:    ctx,
		logger: ctx.Logger(),
	}
}

// GetContext returns the test context
func (h *CARTestHelper) GetContext() context.Context {
	return h.ctx
}

// GetLogger returns the test logger
func (h *CARTestHelper) GetLogger() *core.Logger {
	return h.logger
}

// ValidateCAR validates that the buffer contains a valid CAR file
func (h *CARTestHelper) ValidateCAR(reader *bytes.Reader, expectedCID cid.Cid) {
	require.NotNil(h.tb, reader, "CAR buffer should not be nil")
	require.Greater(h.tb, reader.Len(), 0, "CAR buffer should not be empty")

	carReader, err := carv2.NewReader(reader)
	require.NoError(h.tb, err, "Should be able to create CAR reader")

	roots, err := carReader.Roots()
	require.NoError(h.tb, err, "Should be able to get CAR roots")
	require.Len(h.tb, roots, 1, "CAR should have exactly one root")
	require.Equal(h.tb, expectedCID, roots[0], "CAR root should match expected CID")
}

// TestFileContent represents a file for testing
type TestFileContent struct {
	Name     string
	Content  string
	IsDir    bool
	Modified time.Time
}

// CreateTestArchive creates an archive from test files using the unified creator
func (h *CARTestHelper) CreateTestArchive(format Format, files map[string]string) *bytes.Buffer {
	creator := NewTestArchiveCreator(h.tb.(*testing.T), h.ctx)
	buf, err := creator.CreateArchiveFromMap(h.ctx, format, files)
	require.NoError(h.tb, err, "Should create archive")
	return buf
}

// ReadFileFromCAR reads content from a CAR buffer, supporting both UnixFS and raw data
func (h *CARTestHelper) ReadFileFromCAR(buf *bytes.Buffer, rootCID cid.Cid) string {
	store, err := blockstore.NewReadOnly(bytes.NewReader(buf.Bytes()), nil)
	require.NoError(h.tb, err, "Should create blockstore from CAR")
	defer closeIo(h.tb, store)

	dagService := merkledag.NewDAGService(blockservice.New(store, offline.Exchange(store)))

	node, err := dagService.Get(h.ctx, rootCID)
	require.NoError(h.tb, err, "Should get root node from CAR")

	// Convert node to blocks.Block (node already implements blocks.Block interface)
	block := node.(blocks.Block)

	// Use internal.AnalyzeNode for proper node analysis
	info, err := internal.AnalyzeNode(h.ctx, block)
	require.NoError(h.tb, err, "Should analyze node")

	// Check if this is a UnixFS file
	if info.IsUnixFS && info.UnixFSType == pb.Data_File {
		reader, err := unixfsio.NewDagReader(h.ctx, node, dagService)
		require.NoError(h.tb, err, "Should create DAG reader for UnixFS file")
		defer closeIo(h.tb, reader)

		content, err := io.ReadAll(reader)
		require.NoError(h.tb, err, "Should read UnixFS file content")
		return string(content)
	}

	// Check if this is a UnixFS directory
	if info.IsUnixFS && info.UnixFSType == pb.Data_Directory {
		return h.readFilesFromDirectory(dagService, node.(*merkledag.ProtoNode), "")
	}

	// Fall back to raw content for non-UnixFS nodes
	return h.readRawContent(node)
}

// readUnixFSContent handles UnixFS-formatted content
func (h *CARTestHelper) readUnixFSContent(dagService format.DAGService, node format.Node, fsNode *unixfs.FSNode) string {
	switch fsNode.Type() {
	case unixfs.TDirectory:
		return h.readFilesFromDirectory(dagService, node.(*merkledag.ProtoNode), "")
	case unixfs.TFile:
		reader, err := unixfsio.NewDagReader(h.ctx, node, dagService)
		require.NoError(h.tb, err, "Should create DAG reader")
		defer closeIo(h.tb, reader)

		content, err := io.ReadAll(reader)
		require.NoError(h.tb, err, "Should read file content")
		return string(content)
	default:
		h.tb.Fatalf("Unsupported UnixFS type: %v", fsNode.Type())
		return ""
	}
}

// readRawContent handles raw data blocks
func (h *CARTestHelper) readRawContent(node format.Node) string {
	data := node.RawData()
	if len(data) == 0 {
		return ""
	}
	return string(data)
}

// readFilesFromDirectory recursively reads files from a directory node
func (h *CARTestHelper) readFilesFromDirectory(dagService format.DAGService, node *merkledag.ProtoNode, currentPath string) string {
	var allContent strings.Builder

	fsNode, err := unixfs.FSNodeFromBytes(node.RawData())
	require.NoError(h.tb, err, "Should extract UnixFS metadata")

	if fsNode.Type() != unixfs.TDirectory {
		h.tb.Errorf("readFilesFromDirectory called on non-directory node (type: %v) at path: %s", fsNode.Type(), currentPath)
		return ""
	}

	for _, link := range node.Links() {
		childNode, err := dagService.Get(h.ctx, link.Cid)
		if err != nil {
			continue
		}

		childFsNode, err := unixfs.FSNodeFromBytes(childNode.RawData())
		if err != nil {
			content := h.readRawContent(childNode)
			if content != "" {
				allContent.WriteString(fmt.Sprintf("--- Raw Block: %s ---\n", link.Cid))
				allContent.WriteString(content)
				allContent.WriteString("\n")
			}
			continue
		}

		childPath := currentPath + "/" + link.Name

		if childFsNode.Type() == unixfs.TFile {
			reader, err := unixfsio.NewDagReader(h.ctx, childNode, dagService)
			if err != nil {
				continue
			}
			func() {
				defer closeIo(h.tb, reader)

				content, err := io.ReadAll(reader)
				if err == nil {
					allContent.WriteString(fmt.Sprintf("--- File: %s ---\n", childPath))
					allContent.Write(content)
					allContent.WriteString("\n")
				}
			}()
		} else if childFsNode.Type() == unixfs.TDirectory {
			if protoNode, ok := childNode.(*merkledag.ProtoNode); ok {
				allContent.WriteString(h.readFilesFromDirectory(dagService, protoNode, childPath))
			}
		}
	}

	return allContent.String()
}

// ReadDirectoryStructure reads a CAR buffer and returns a map of file paths to their content.
// It expects the root CID to be a UnixFS directory.
func (h *CARTestHelper) ReadDirectoryStructure(buf *bytes.Buffer, rootCID cid.Cid) map[string]string {
	store, err := blockstore.NewReadOnly(bytes.NewReader(buf.Bytes()), nil)
	require.NoError(h.tb, err, "Should create blockstore from CAR")
	defer closeIo(h.tb, store)

	dagService := merkledag.NewDAGService(blockservice.New(store, offline.Exchange(store)))

	node, err := dagService.Get(h.ctx, rootCID)
	require.NoError(h.tb, err, "Should get root node from CAR")

	return h.readFilesFromDirectoryStructured(dagService, node.(*merkledag.ProtoNode), common.ROOT)
}

// readFilesFromDirectoryStructured recursively reads files from a directory node into a map.
func (h *CARTestHelper) readFilesFromDirectoryStructured(dagService format.DAGService, node *merkledag.ProtoNode, currentPath string) map[string]string {
	directoryStructure := make(map[string]string)

	info, err := internal.AnalyzeNode(h.ctx, node)
	if err != nil {
		require.NoError(h.tb, err)
	}

	// Use AnalyzeNode to determine if the current node is a directory
	if !info.IsUnixFS || info.UnixFSType != pb.Data_Directory {
		h.tb.Errorf("readFilesFromDirectoryStructured called on non-directory node (type: %v, unixFS: %v) at path: %s", info.Type, info.IsUnixFS, currentPath)
		return directoryStructure
	}

	for _, link := range node.Links() {
		childNode, err := dagService.Get(h.ctx, link.Cid)
		if err != nil {
			continue
		}

		// Use AnalyzeNode to inspect the child node
		childInfo, err := internal.AnalyzeNode(h.ctx, childNode)
		if err != nil {
			// Skip nodes that can't be analyzed
			continue
		}

		childPath := link.Name
		if currentPath != "" && currentPath != common.ROOT {
			childPath = currentPath + "/" + link.Name
		}

		// Remove "./" prefix if present to normalize paths
		if strings.HasPrefix(childPath, "./") {
			childPath = childPath[2:]
		}

		// Decide if the child is a file or directory based on AnalyzeNode's output
		if childInfo.IsUnixFS && childInfo.UnixFSType == pb.Data_File {
			reader, err := unixfsio.NewDagReader(h.ctx, childNode, dagService)
			require.NoError(h.tb, err, "Should create DAG reader")

			content, err := io.ReadAll(reader)
			require.NoError(h.tb, err, "Should read file content")

			// Close reader explicitly instead of deferring to avoid resource leaks in loop
			closeIo(h.tb, reader)

			directoryStructure[childPath] = string(content)
		} else if childInfo.IsUnixFS && childInfo.UnixFSType == pb.Data_Directory {
			// Recursively process subdirectories and merge their contents
			subDirStructure := h.readFilesFromDirectoryStructured(dagService, childNode.(*merkledag.ProtoNode), childPath)
			for path, content := range subDirStructure {
				directoryStructure[path] = content
			}
		}
	}

	return directoryStructure
}

// TestCARGeneration tests CAR generation from an archive
func (h *CARTestHelper) TestCARGeneration(format Format, files map[string]string) {
	// Register appropriate extractor
	switch format {
	case FormatZIP:
		RegisterZipExtractor()
	case FormatTAR:
		RegisterTarExtractor()
	case FormatTAR_GZ:
		RegisterTarGzExtractor()
	case FormatTAR_BZ2:
		RegisterTarBz2Extractor()
	case Format7Z:
		Register7ZipExtractor()
	}

	archiveBuf := h.CreateTestArchive(format, files)

	extractor, err := CreateExtractor(bytes.NewReader(archiveBuf.Bytes()))
	require.NoError(h.tb, err, "Should create extractor")
	defer closeIo(h.tb, extractor)

	generator := NewCARGeneratorWithDefaults(h.logger)

	buf, rootCID, err := generator.ArchiveToCAR(h.ctx, extractor)
	require.NoError(h.tb, err, "ArchiveToCAR should not return error")
	require.NotNil(h.tb, buf, "Buffer should not be nil")
	require.NotEqual(h.tb, cid.Undef, rootCID, "Root CID should not be undefined")

	h.ValidateCAR(bytes.NewReader(buf.Bytes()), rootCID)

	directoryStructure := h.ReadDirectoryStructure(buf, rootCID)

	// Create an expected map from the input files map
	expectedFiles := make(map[string]string)
	for path, content := range files {
		if content != "" { // Skip empty directory entries
			expectedFiles[path] = content
		}
	}

	// Assert that the directory structure is exactly as expected
	require.Equal(h.tb, expectedFiles, directoryStructure, "CAR directory structure should match the expected structure")
}

// debugPrintCARNodes traverses the CAR file and prints all stored nodes
func (h *CARTestHelper) debugPrintCARNodes(buf *bytes.Buffer, rootCID cid.Cid) {
	cr, err := carv2.NewBlockReader(bytes.NewReader(buf.Bytes()))
	require.NoError(h.tb, err, "Should create CAR block reader")

	// Read all blocks from CAR using the BlockReader approach
	for {
		_, err := cr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
	}
}

// Test helper implementations
type testFileInfo struct {
	name    string
	size    int64
	modTime time.Time
	isDir   bool
}

func (f *testFileInfo) Name() string { return f.name }
func (f *testFileInfo) Size() int64  { return f.size }
func (f *testFileInfo) Mode() fs.FileMode {
	if f.isDir {
		return fs.ModeDir | 0755
	}
	return 0644
}
func (f *testFileInfo) ModTime() time.Time { return f.modTime }
func (f *testFileInfo) IsDir() bool        { return f.isDir }
func (f *testFileInfo) Sys() any           { return nil }

type testMemFile struct {
	name    string
	content []byte
	pos     int
	modTime time.Time
}

func (f *testMemFile) Stat() (fs.FileInfo, error) {
	return &testFileInfo{
		name:    f.name,
		size:    int64(len(f.content)),
		modTime: f.modTime,
		isDir:   false,
	}, nil
}

func (f *testMemFile) Read(p []byte) (int, error) {
	if f.pos >= len(f.content) {
		return 0, io.EOF
	}
	n := copy(p, f.content[f.pos:])
	f.pos += n
	return n, nil
}

func (f *testMemFile) Close() error {
	return nil
}

type testDirFile struct {
	name    string
	modTime time.Time
}

func (f *testDirFile) Stat() (fs.FileInfo, error) {
	return &testFileInfo{
		name:    f.name,
		size:    0,
		modTime: f.modTime,
		isDir:   true,
	}, nil
}

func (f *testDirFile) Read(p []byte) (int, error) {
	return 0, io.EOF
}

func (f *testDirFile) Close() error {
	return nil
}
