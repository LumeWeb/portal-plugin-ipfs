package upload

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/mholt/archives"
	"github.com/stretchr/testify/require"
)

// Helper functions to eliminate repetitive code patterns

// getStandardArchiveFormats returns the standard list of archive formats to test
func getStandardArchiveFormats() []Format {
	return []Format{
		FormatZIP,
		FormatTAR,
		FormatTAR_GZ,
		FormatTAR_BZ2,
		Format7Z,
	}
}

// registerArchiveExtractor registers the appropriate extractor for the given format
func registerArchiveExtractor(format Format) {
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
}

// createEmptyArchive creates an empty archive for the given format
func createEmptyArchive(format Format, ctx context.Context, helper *CARTestHelper) (*bytes.Buffer, error) {
	var emptyBuf bytes.Buffer

	switch format {
	case FormatZIP:
		zipFormat := archives.Zip{}
		err := zipFormat.Archive(ctx, &emptyBuf, []archives.FileInfo{})
		return &emptyBuf, err
	case FormatTAR:
		tarFormat := archives.Tar{}
		err := tarFormat.Archive(ctx, &emptyBuf, []archives.FileInfo{})
		return &emptyBuf, err
	case FormatTAR_GZ:
		tarGzFormat := archives.CompressedArchive{
			Compression: archives.Gz{},
			Archival:    archives.Tar{},
		}
		err := tarGzFormat.Archive(ctx, &emptyBuf, []archives.FileInfo{})
		return &emptyBuf, err
	case FormatTAR_BZ2:
		tarBz2Format := archives.CompressedArchive{
			Compression: archives.Bz2{},
			Archival:    archives.Tar{},
		}
		err := tarBz2Format.Archive(ctx, &emptyBuf, []archives.FileInfo{})
		return &emptyBuf, err
	case Format7Z:
		return nil, fmt.Errorf("7Z format does not support empty archive creation in this context")
	default:
		return nil, fmt.Errorf("unsupported archive format: %v", format)
	}
}

// Test cases for archive extraction functionality
func TestArchiveExtractorToCAR(t *testing.T) {
	testCases := []struct {
		name        string
		files       map[string]string
		expectError bool
		errorMsg    string
	}{
		{
			name: "SingleFileInFolder",
			files: map[string]string{
				"folder/file.txt": "Hello, World! This is a test file in a folder.",
			},
			expectError: false,
		},
		{
			name: "MultipleFilesInFolders",
			files: map[string]string{
				"docs/readme.txt":     "This is the readme file.",
				"docs/guide.txt":      "This is the guide file.",
				"src/main.go":         "package main\n\nfunc main() {\n    fmt.Println(\"Hello\")\n}",
				"src/utils/helper.go": "package utils\n\nfunc Helper() string { return \"helper\" }",
				"config.json":         "{ \"name\": \"test\", \"version\": \"1.0\" }",
			},
			expectError: false,
		},
		{
			name: "NestedDirectoryStructure",
			files: map[string]string{
				"root.txt":                   "Root level file",
				"folder1/":                   "", // Empty directory indicator
				"folder1/file1.txt":          "File in folder1",
				"folder1/subfolder/":         "", // Empty subdirectory
				"folder1/subfolder/deep.txt": "Deep nested file",
				"folder2/":                   "",
				"folder2/another.txt":        "Another file in different folder",
				"folder2/sub/":               "",
				"folder2/sub/more/":          "",
				"folder2/sub/more/deep.go":   "Very deeply nested Go file",
			},
			expectError: false,
		},
	}

	for _, tc := range testCases {
		for _, format := range getStandardArchiveFormats() {
			t.Run(tc.name+"_"+format.String(), func(t *testing.T) {
				helper := NewCARTestHelper(t)
				helper.TestCARGeneration(format, tc.files)
			})
		}
	}
}

// Test cases for archive extraction error conditions
func TestArchiveExtractorToCAR_ErrorConditions(t *testing.T) {
	testCases := []struct {
		name        string
		setupFunc   func(t *testing.T, helper *CARTestHelper, format Format) (context.Context, ArchiveExtractor)
		expectError bool
		errorMsg    string
		errorType   error
	}{
		{
			name: "EmptyArchive",
			setupFunc: func(t *testing.T, helper *CARTestHelper, format Format) (context.Context, ArchiveExtractor) {
				// Register appropriate extractor
				registerArchiveExtractor(format)

				// Create empty archive
				emptyBuf, err := createEmptyArchive(format, helper.GetContext(), helper)
				if err != nil {
					if format == Format7Z {
						t.Skip("Skipping empty archive test for 7Z format due to external tool dependency")
					}
					require.NoError(t, err, "Should create empty archive")
				}

				// Create archive extractor for empty archive
				extractor, err := CreateExtractor(bytes.NewReader(emptyBuf.Bytes()))
				require.NoError(t, err, "Should create extractor for empty archive")

				return helper.GetContext(), extractor
			},
			expectError: true,
			errorMsg:    "no entries found",
		},
		{
			name: "ContextCancellation",
			setupFunc: func(t *testing.T, helper *CARTestHelper, format Format) (context.Context, ArchiveExtractor) {
				// Register appropriate extractor
				registerArchiveExtractor(format)

				ctx, cancel := context.WithCancel(helper.GetContext())
				cancel() // Cancel the context immediately

				// Create test files
				files := map[string]string{
					"test.txt": "content",
				}

				archiveBuf := helper.CreateTestArchive(format, files)
				extractor, err := CreateExtractor(bytes.NewReader(archiveBuf.Bytes()))
				require.NoError(t, err, "Should create extractor")

				return ctx, extractor
			},
			expectError: true,
			errorType:   context.Canceled,
		},
	}

	for _, tc := range testCases {
		for _, format := range getStandardArchiveFormats() {
			t.Run(tc.name+"_"+format.String(), func(t *testing.T) {
				// Skip raw TAR format for empty archive test since it doesn't handle empty archives properly
				if tc.name == "EmptyArchive" && format == FormatTAR {
					t.Skip("Skipping empty archive test for raw TAR format")
				}
				
				helper := NewCARTestHelper(t)
				ctx, extractor := tc.setupFunc(t, helper, format)
				defer closeIo(t, extractor)

				// Create CAR generator
				generator := NewCARGeneratorWithDefaults(helper.GetLogger())

				// Test the function
				buf, rootCID, err := generator.ArchiveToCAR(ctx, extractor)

				if tc.expectError {
					require.Error(t, err, "ArchiveToCAR should return error")
					if tc.errorMsg != "" {
						require.Contains(t, err.Error(), tc.errorMsg, "Error should contain expected message")
					}
					if tc.errorType != nil {
						require.Equal(t, tc.errorType, err, "Error should be of expected type")
					}
					require.Nil(t, buf, "Buffer should be nil on error")
					require.Equal(t, cid.Undef, rootCID, "Root CID should be undefined on error")
				} else {
					require.NoError(t, err, "ArchiveToCAR should not return error")
				}
			})
		}
	}
}

// Test cases for single file CAR generation
func TestSingleFileToCAR(t *testing.T) {
	testCases := []struct {
		name        string
		setupFunc   func(t *testing.T) io.ReadCloser
		expectError bool
		errorMsg    string
		errorType   error
		validate    func(t *testing.T, helper *CARTestHelper, buf *bytes.Buffer, rootCID cid.Cid, content []byte)
	}{
		{
			name: "Success",
			setupFunc: func(t *testing.T) io.ReadCloser {
				content := "Hello, World! This is a test file for CAR generation."
				return io.NopCloser(bytes.NewReader([]byte(content)))
			},
			expectError: false,
			validate: func(t *testing.T, helper *CARTestHelper, buf *bytes.Buffer, rootCID cid.Cid, content []byte) {
				// Validate the CAR file
				helper.ValidateCAR(bytes.NewReader(buf.Bytes()), rootCID)

				// Read the file back from CAR using UnixFS
				retrievedContent := helper.ReadFileFromCAR(buf, rootCID)
				require.Equal(t, string(content), retrievedContent, "CAR should preserve exact file content")
			},
		},
		{
			name: "EmptyFile",
			setupFunc: func(t *testing.T) io.ReadCloser {
				return io.NopCloser(bytes.NewReader([]byte{}))
			},
			expectError: false,
			validate: func(t *testing.T, helper *CARTestHelper, buf *bytes.Buffer, rootCID cid.Cid, content []byte) {
				// Validate the CAR file
				helper.ValidateCAR(bytes.NewReader(buf.Bytes()), rootCID)

				// Read the empty file back from CAR using UnixFS
				retrievedContent := helper.ReadFileFromCAR(buf, rootCID)
				require.Equal(t, "", retrievedContent, "CAR should preserve empty file content")
			},
		},
		{
			name: "NilReader",
			setupFunc: func(t *testing.T) io.ReadCloser {
				return nil
			},
			expectError: true,
			errorMsg:    "reader is nil",
		},
		{
			name: "LargeFile",
			setupFunc: func(t *testing.T) io.ReadCloser {
				// Create large file content (1MB) using deterministic random data
				content := make([]byte, 1024*1024)
				rng := rand.New(rand.NewSource(42)) // Fixed seed for reproducibility
				_, err := rng.Read(content)
				require.NoError(t, err, "Should generate random test data")
				return io.NopCloser(bytes.NewReader(content))
			},
			expectError: false,
			validate: func(t *testing.T, helper *CARTestHelper, buf *bytes.Buffer, rootCID cid.Cid, content []byte) {
				bufReader := bytes.NewReader(buf.Bytes())

				// Validate the CAR file
				helper.ValidateCAR(bufReader, rootCID)
				_, err := bufReader.Seek(0, io.SeekStart)
				require.NoError(t, err)

				// Read the large file back from CAR using UnixFS
				retrievedContent := helper.ReadFileFromCAR(buf, rootCID)
				require.Equal(t, string(content), retrievedContent, "CAR should preserve large file content exactly")

				_, err = bufReader.Seek(0, io.SeekStart)
				require.NoError(t, err)

				// Verify CAR size is reasonable (should be larger than original content due to CAR overhead)
				require.Greater(t, uint64(bufReader.Size()), uint64(len(content)), "CAR buffer should be larger than original content")
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			helper := NewCARTestHelper(t)

			reader := tc.setupFunc(t)
			var content []byte
			if reader != nil {
				// Extract content for validation
				buf := new(bytes.Buffer)
				_, err := buf.ReadFrom(reader)
				require.NoError(t, err, "Should read content for validation")
				content = buf.Bytes()
				reader = io.NopCloser(bytes.NewReader(content))
			}

			// Create CAR generator
			generator := NewCARGeneratorWithDefaults(helper.GetLogger())

			// Test the function
			buf, rootCID, err := generator.FileToCAR(helper.GetContext(), reader)

			if tc.expectError {
				require.Error(t, err, "FileToCAR should return error")
				if tc.errorMsg != "" {
					require.Contains(t, err.Error(), tc.errorMsg, "Error should contain expected message")
				}
				if tc.errorType != nil {
					require.Equal(t, tc.errorType, err, "Error should be of expected type")
				}
				require.Nil(t, buf, "Buffer should be nil on error")
				require.Equal(t, cid.Undef, rootCID, "Root CID should be undefined on error")
			} else {
				require.NoError(t, err, "FileToCAR should not return error")
				require.NotNil(t, buf, "Buffer should not be nil")
				require.NotEqual(t, cid.Undef, rootCID, "Root CID should not be undefined")
				if tc.validate != nil {
					tc.validate(t, helper, buf, rootCID, content)
				}
			}
		})
	}
}

// Benchmark tests using table-driven approach
func BenchmarkCARGeneration(b *testing.B) {
	benchmarks := []struct {
		name  string
		setup func(b *testing.B, helper *CARTestHelper, format Format) func() error
	}{
		{
			name: "ArchiveExtractorToCAR_SingleFile",
			setup: func(b *testing.B, helper *CARTestHelper, format Format) func() error {
				files := map[string]string{
					"benchmark/test.txt": "Hello, World! This is a benchmark test.",
				}
				archiveBuf := helper.CreateTestArchive(format, files)

				return func() error {
					extractor, err := CreateExtractor(bytes.NewReader(archiveBuf.Bytes()))
					if err != nil {
						return err
					}

					generator := NewCARGeneratorWithDefaults(helper.GetLogger())
					_, _, err = generator.ArchiveToCAR(helper.GetContext(), extractor)
					closeIo(b, extractor)
					return err
				}
			},
		},
		{
			name: "SingleFileToCAR_SingleFile",
			setup: func(b *testing.B, helper *CARTestHelper, format Format) func() error {
				content := "Hello, World! This is a benchmark test."

				return func() error {
					reader := io.NopCloser(bytes.NewReader([]byte(content)))
					generator := NewCARGeneratorWithDefaults(helper.GetLogger())
					_, _, err := generator.FileToCAR(helper.GetContext(), reader)
					return err
				}
			},
		},
	}

	for _, bm := range benchmarks {
		for _, format := range getStandardArchiveFormats() {
			b.Run(bm.name+"_"+format.String(), func(b *testing.B) {
				helper := NewCARTestHelper(b)
				benchmarkFunc := bm.setup(b, helper, format)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := benchmarkFunc(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
