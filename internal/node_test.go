package internal_test

import (
	"context"
	"encoding/json"
	"github.com/ipfs/boxo/ipld/unixfs/pb"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.lumeweb.com/portal-plugin-ipfs/internal"
)

var (
	finalDataDir = ""
)

const testDataDir = "testing/fixtures/data"

func init() {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		// Fallback to relative path if runtime.Caller fails
		finalDataDir = testDataDir
	} else {
		finalDataDir = path.Join(path.Dir(file), testDataDir)
	}
}

type InfoFile struct {
	File         string `json:"file"`
	Dir          string `json:"dir"`
	CID          string `json:"cid"`
	Size         uint64 `json:"size"`
	MessageSize  uint64 `json:"message_size"`
	RawBlockSize uint64 `json:"raw_block_size"`
	Missing      uint   `json:"missing"`
	IsPartial    bool   `json:"is_partial"`
	Type         string `json:"type"`
}

func loadInfoFromFile(t *testing.T, filename string) InfoFile {
	t.Helper()
	// Change .info to .info.json
	jsonFile := strings.ReplaceAll(filename, ".info", ".info.json")
	data, err := os.ReadFile(filepath.Join(testDataDir, jsonFile))
	require.NoError(t, err)

	var info InfoFile
	err = json.Unmarshal(data, &info)
	require.NoError(t, err)
	return info
}

func loadBlockFromFile(t *testing.T, filename string) blocks.Block {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(finalDataDir, filename))
	if err != nil {
		t.Fatalf("failed to read block file: %v", err)
	}

	// Determine CID based on filename
	infoFilename := strings.ReplaceAll(filename, ".block", ".info")
	infoFile := loadInfoFromFile(t, infoFilename)

	if infoFile.CID == "N/A" || infoFile.CID == "" {
		t.Fatalf("CID is missing in info file %s", infoFilename)
	}

	_cid, err := cid.Decode(infoFile.CID)
	if err != nil {
		t.Fatalf("failed to decode CID from info file %s: %v", infoFilename, err)
	}

	block, err := blocks.NewBlockWithCid(data, _cid)
	if err != nil {
		t.Fatalf("failed to create block: %v", err)
	}
	return block
}

func TestAnalyzeNode_RawData(t *testing.T) {
	tests := []struct {
		name     string
		filename string
	}{
		{"data1024", "data_1024_0.block"},
		{"data256000", "data_256000_0.block"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			block := loadBlockFromFile(t, tt.filename)
			info, err := internal.AnalyzeNode(context.Background(), block)
			require.NoError(t, err)

			assert.Equal(t, internal.NodeTypeRaw, info.Type)

			infoFile := loadInfoFromFile(t, strings.ReplaceAll(tt.filename, ".block", ".info"))
			assert.Equal(t, infoFile.Size, info.BlockSize)
		})
	}
}

func TestDetectPartialFile_RawData(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(finalDataDir, "data_*_0.block"))
	require.NoError(t, err)

	for _, file := range files {
		filename := filepath.Base(file)
		t.Run(filename, func(t *testing.T) {
			block := loadBlockFromFile(t, filename)
			isPartial, err := internal.DetectPartialFile(context.Background(), block)
			assert.NoError(t, err)

			// Load info from file
			infoFile := loadInfoFromFile(t, strings.ReplaceAll(filename, ".block", ".info"))

			assert.Equal(t, infoFile.IsPartial, isPartial)
		})
	}
}

func TestAnalyzeNode_ProtobufData(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(finalDataDir, "protobuf_*_0.block"))
	require.NoError(t, err)

	for _, file := range files {
		filename := filepath.Base(file)
		t.Run(filename, func(t *testing.T) {
			block := loadBlockFromFile(t, filename)
			info, err := internal.AnalyzeNode(context.Background(), block)
			assert.NoError(t, err)
			assert.Equal(t, internal.NodeTypeProtobuf, info.Type)

			// Load info from file
			infoFile := loadInfoFromFile(t, strings.ReplaceAll(filename, ".block", ".info"))

			// Validate CID matches
			if infoFile.CID != "" && infoFile.CID != "N/A" {
				_cid, err := info.GetCID()
				require.NoError(t, err)
				assert.Equal(t, infoFile.CID, _cid.String(), "CID mismatch")
			}

			// Validate size - protobuf size should match the generated size from info file
			assert.Equal(t, infoFile.RawBlockSize, info.BlockSize, "Protobuf size mismatch")

			// Additional protobuf-specific validations
			assert.Equal(t, infoFile.MessageSize, info.DataSize, "Data size should match protobuf message size")
			assert.Equal(t, infoFile.RawBlockSize, uint64(len(block.RawData())), "Raw block size should match")
			assert.False(t, info.IsUnixFS, "Protobuf data should not be UnixFS")
			assert.Equal(t, pb.Data_Raw, info.UnixFSType, "Protobuf data should have Raw type")
		})
	}
}

func TestDetectPartialFile_ProtobufData(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(finalDataDir, "protobuf_*_0.block"))
	require.NoError(t, err)

	for _, file := range files {
		filename := filepath.Base(file)
		t.Run(filename, func(t *testing.T) {
			block := loadBlockFromFile(t, filename)
			isPartial, err := internal.DetectPartialFile(context.Background(), block)
			assert.NoError(t, err)

			// Load info from file
			infoFile := loadInfoFromFile(t, strings.ReplaceAll(filename, ".block", ".info"))

			assert.Equal(t, infoFile.IsPartial, isPartial)
		})
	}
}

func TestAnalyzeNode_UnixFSData(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(finalDataDir, "unixfs_*.block"))
	require.NoError(t, err)

	for _, file := range files {
		filename := filepath.Base(file)
		t.Run(filename, func(t *testing.T) {
			block := loadBlockFromFile(t, filename)
			info, err := internal.AnalyzeNode(context.Background(), block)
			assert.NoError(t, err)

			// Load info from file
			infoFile := loadInfoFromFile(t, strings.ReplaceAll(filename, ".block", ".info"))

			// Check the type of the block
			if infoFile.Type == "raw_data" {
				// Skip UnixFS-specific assertions for raw data blocks
				assert.Equal(t, internal.NodeTypeRaw, info.Type)
				assert.Equal(t, infoFile.MessageSize, info.DataSize)
				assert.Equal(t, infoFile.RawBlockSize, info.BlockSize)
				return // Skip the rest of the UnixFS assertions
			}

			assert.Equal(t, internal.NodeTypeProtobuf, info.Type)
			assert.True(t, info.IsUnixFS)

			// Extract UnixFS type from filename
			var expectedUnixFSType pb.Data_DataType
			switch {
			case strings.Contains(filename, "file"):
				// _file.block files are the UnixFS protobuf root nodes
				if strings.HasSuffix(filename, "_file.block") {
					expectedUnixFSType = pb.Data_File
				} else {
					// Other file blocks are raw data chunks
					expectedUnixFSType = pb.Data_Raw
				}
			case strings.Contains(filename, "directory"):
				expectedUnixFSType = pb.Data_Directory
			case strings.Contains(filename, "symlink"):
				expectedUnixFSType = pb.Data_Symlink
			default:
				t.Fatalf("unknown UnixFS type in filename: %s", filename)
			}

			assert.Equal(t, expectedUnixFSType, info.UnixFSType)

			// Additional assertions based on UnixFS type
			switch expectedUnixFSType {
			case pb.Data_File:
				assert.Equal(t, infoFile.Size, info.BlockSize, "File data size should match info file")
				assert.GreaterOrEqual(t, info.LinkCount(), 0, "File should have zero or more links")

				if info.IsFileRoot {
					assert.Greater(t, info.BlockSize, uint64(0), "File root block should have non-zero size")
					assert.Greater(t, len(info.ChunkSizes), 0, "File root should have one or more block sizes")
				}

			case pb.Data_Directory:
				if infoFile.CID != "" && infoFile.CID != "N/A" {
					_cid, err := info.GetCID()
					require.NoError(t, err)
					assert.Equal(t, infoFile.CID, _cid.String(), "Directory CID should match info file")
				}
				assert.Greater(t, info.LinkCount(), 0, "Directory should have one or more links")
				assert.Equal(t, infoFile.RawBlockSize, info.BlockSize, "Directory data size should match info file")
				assert.Equal(t, infoFile.Size, info.BlockSize, "Directory data size should match info file")
				assert.Equal(t, infoFile.MessageSize, info.DataSize, "Directory message size should match data size")
				assert.Equal(t, infoFile.IsPartial, false, "Directory should never be marked as partial")

			}
		})
	}
}

func TestDetectPartialFile_UnixFSData(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(finalDataDir, "unixfs_*.block"))
	require.NoError(t, err)

	for _, file := range files {
		filename := filepath.Base(file)
		t.Run(filename, func(t *testing.T) {
			block := loadBlockFromFile(t, filename)
			isPartial, err := internal.DetectPartialFile(context.Background(), block)
			assert.NoError(t, err)

			// Load info from file
			infoFile := loadInfoFromFile(t, strings.ReplaceAll(filename, ".block", ".info"))

			assert.Equal(t, infoFile.IsPartial, isPartial)
		})
	}
}
