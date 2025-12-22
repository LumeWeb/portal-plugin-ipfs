package upload

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"math/rand"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/docker/go-units"
	"github.com/mholt/archives"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

// setupStreamProcessorTest creates a streaming processor with default in-memory implementations
func setupStreamProcessorTest(t *testing.T) (*StreamingProcessor, context.Context, coreTesting.TestContext) {
	ctx, err := coreTesting.NewTestContext(t)
	require.NoError(t, err)

	logger := ctx.Logger()

	// Create streaming processor with default implementations
	processor := NewStreamingProcessorWithDefaults(logger)

	return processor, context.Background(), ctx
}

// createStreamProcessorTestArchive creates a real archive extractor from test files
func createStreamProcessorTestArchive(t *testing.T, ctx core.Context, files map[string]*fstest.MapFile) ArchiveExtractor {
	// Convert fstest.MapFile to TestFile format
	testFiles := make([]TestFile, 0)
	for path, file := range files {
		testFile := TestFile{
			Name:    path,
			Content: string(file.Data),
			IsDir:   file.Mode.IsDir(),
			Mode:    int64(file.Mode.Perm()),
		}
		testFiles = append(testFiles, testFile)
	}

	// Create a real ZIP archive
	archiveData := CreateZIPArchive(t, ctx, testFiles)

	// Create a real archive extractor
	RegisterZipExtractor()
	extractor, err := CreateExtractor(createSeekableReader(archiveData))
	require.NoError(t, err)

	return extractor
}

// createSeekableReader creates a seekable reader from byte data that implements archives.ReaderAtSeeker
func createSeekableReader(data []byte) archives.ReaderAtSeeker {
	return bytes.NewReader(data)
}

func TestNewStreamingProcessor(t *testing.T) {
	processor, _, _ := setupStreamProcessorTest(t)

	require.NotNil(t, processor)
	assert.NotNil(t, processor.nodeGenerator)
	assert.NotNil(t, processor.dagService)
	assert.NotNil(t, processor.blockstore)
	assert.NotNil(t, processor.logger)
	assert.Equal(t, 0, len(processor.processedFiles))
	assert.Equal(t, 0, len(processor.directoryMetadata))
	assert.Equal(t, "", processor.rootCID)
}

func TestProcessArchive_FilesOnly(t *testing.T) {
	tests := []struct {
		name        string
		files       map[string]*fstest.MapFile
		expectFiles int
	}{
		{
			name: "single small file",
			files: map[string]*fstest.MapFile{
				"test.txt": {
					Data: []byte("test content"),
					Mode: 0644,
				},
			},
			expectFiles: 1,
		},
		{
			name: "multiple files",
			files: map[string]*fstest.MapFile{
				"test1.txt": {
					Data: []byte("test content 1"),
					Mode: 0644,
				},
				"test2.txt": {
					Data: []byte("test content 2"),
					Mode: 0644,
				},
			},
			expectFiles: 2,
		},
		{
			name: "empty file",
			files: map[string]*fstest.MapFile{
				"empty.txt": {
					Data: []byte(""),
					Mode: 0644,
				},
			},
			expectFiles: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor, ctx, testCtx := setupStreamProcessorTest(t)
			extractor := createStreamProcessorTestArchive(t, testCtx, tt.files)

			err := processor.ProcessArchive(ctx, extractor)
			assert.NoError(t, err)
			assert.NotEmpty(t, processor.rootCID)
			assert.Equal(t, tt.expectFiles, len(processor.processedFiles))

			// Verify all files were processed successfully
			for _, file := range processor.processedFiles {
				assert.True(t, file.Processed)
				assert.NoError(t, file.Error)
			}
		})
	}
}

func TestProcessArchive_DirectoryStructures(t *testing.T) {
	tests := []struct {
		name         string
		dirs         map[string]*fstest.MapFile
		expectedDirs int
	}{
		{
			name: "single directory",
			dirs: map[string]*fstest.MapFile{
				"subdir": {
					Mode: fs.ModeDir | 0755,
				},
			},
			expectedDirs: 2, // root + subdir
		},
		{
			name: "nested directories",
			dirs: map[string]*fstest.MapFile{
				"level1": {
					Mode: fs.ModeDir | 0755,
				},
				"level1/level2": {
					Mode: fs.ModeDir | 0755,
				},
			},
			expectedDirs: 3, // root + level1 + level1/level2
		},
		{
			name: "deeply nested directories",
			dirs: map[string]*fstest.MapFile{
				"a": {
					Mode: fs.ModeDir | 0755,
				},
				"a/b": {
					Mode: fs.ModeDir | 0755,
				},
				"a/b/c": {
					Mode: fs.ModeDir | 0755,
				},
			},
			expectedDirs: 4, // root + a + a/b + a/b/c
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor, ctx, testCtx := setupStreamProcessorTest(t)
			extractor := createStreamProcessorTestArchive(t, testCtx, tt.dirs)

			err := processor.ProcessArchive(ctx, extractor)
			assert.NoError(t, err)
			assert.NotEmpty(t, processor.rootCID)
			assert.Equal(t, tt.expectedDirs, len(processor.directoryMetadata))
		})
	}
}

func TestProcessArchive_ErrorCases(t *testing.T) {
	tests := []struct {
		name          string
		setupTest     func(t *testing.T, ctx core.Context) (ArchiveExtractor, context.Context)
		expectedError string
		expectError   bool
	}{
		{
			name: "empty archive",
			setupTest: func(t *testing.T, testCtx core.Context) (ArchiveExtractor, context.Context) {
				_, ctx, _ := setupStreamProcessorTest(t)
				emptyFiles := map[string]*fstest.MapFile{}
				extractor := createStreamProcessorTestArchive(t, testCtx, emptyFiles)
				return extractor, ctx
			},
			expectedError: "no entries found",
			expectError:   true,
		},
		{
			name: "context cancellation",
			setupTest: func(t *testing.T, testCtx core.Context) (ArchiveExtractor, context.Context) {
				_, _, _ = setupStreamProcessorTest(t)

				ctx, cancel := context.WithCancel(context.Background())
				cancel() // Cancel immediately

				files := map[string]*fstest.MapFile{
					"test.txt": {
						Data: []byte("test content"),
						Mode: 0644,
					},
				}
				extractor := createStreamProcessorTestArchive(t, testCtx, files)
				return extractor, ctx
			},
			expectedError: "context canceled",
			expectError:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor, _, testCtx := setupStreamProcessorTest(t)
			extractor, ctx := tt.setupTest(t, testCtx)

			err := processor.ProcessArchive(ctx, extractor)

			if tt.expectError {
				assert.Error(t, err)
				if tt.expectedError != "" {
					assert.Contains(t, err.Error(), tt.expectedError)
				}
			}
		})
	}
}

func TestGetRootNode_ErrorCases(t *testing.T) {
	tests := []struct {
		name           string
		setupProcessor func(*StreamingProcessor, core.Context)
		expectedError  string
		expectError    bool
	}{
		{
			name: "root node not available",
			setupProcessor: func(sp *StreamingProcessor, testCtx core.Context) {
				// Don't set rootCID
			},
			expectedError: "root node not available",
			expectError:   true,
		},
		{
			name: "successful root node retrieval",
			setupProcessor: func(sp *StreamingProcessor, testCtx core.Context) {
				// Process an archive to set rootCID
				files := map[string]*fstest.MapFile{
					"test.txt": {
						Data: []byte("test content"),
						Mode: 0644,
					},
				}
				extractor := createStreamProcessorTestArchive(t, testCtx, files)
				ctx := context.Background()
				err := sp.ProcessArchive(ctx, extractor)
				require.NoError(t, err, "Setup should successfully process archive")
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor, _, testCtx := setupStreamProcessorTest(t)

			tt.setupProcessor(processor, testCtx)

			node, err := processor.GetRootNode(context.Background())

			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
				assert.Nil(t, node)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, node)
			}
		})
	}
}

func TestGetProcessedFiles_Empty(t *testing.T) {
	processor, _, _ := setupStreamProcessorTest(t)

	files := processor.GetProcessedFiles()
	assert.Equal(t, 0, len(files))
}

func TestGetProcessedFiles_WithData(t *testing.T) {
	processor, ctx, testCtx := setupStreamProcessorTest(t)

	// Process some files to generate data
	files := map[string]*fstest.MapFile{
		"test.txt": {
			Data: []byte("test content"),
			Mode: 0644,
		},
		"test2.txt": {
			Data: []byte("test content 2"),
			Mode: 0644,
		},
	}
	extractor := createStreamProcessorTestArchive(t, testCtx, files)

	err := processor.ProcessArchive(ctx, extractor)
	require.NoError(t, err)

	retrievedFiles := processor.GetProcessedFiles()
	assert.Equal(t, 2, len(retrievedFiles))

	// Find the test.txt file to test copy behavior
	var testFile *FileInfo
	for i := range retrievedFiles {
		if retrievedFiles[i].Path == "test.txt" {
			testFile = &retrievedFiles[i]
			break
		}
	}
	require.NotNil(t, testFile, "test.txt should be found in processed files")

	// Test that returned slice is a copy by modifying it
	originalPath := testFile.Path
	testFile.Path = "modified.txt"

	// Verify the original processor data is unchanged
	var originalTestFile *FileInfo
	for i := range processor.processedFiles {
		if processor.processedFiles[i].Path == originalPath {
			originalTestFile = &processor.processedFiles[i]
			break
		}
	}
	require.NotNil(t, originalTestFile, "test.txt should still exist in processor files")
	assert.Equal(t, originalPath, originalTestFile.Path)
}

func TestClose(t *testing.T) {
	processor, ctx, testCtx := setupStreamProcessorTest(t)

	// Add some test data
	files := map[string]*fstest.MapFile{
		"test.txt": {
			Data: []byte("test content"),
			Mode: 0644,
		},
	}
	extractor := createStreamProcessorTestArchive(t, testCtx, files)

	err := processor.ProcessArchive(ctx, extractor)
	require.NoError(t, err)

	// Verify data exists before close
	assert.NotEmpty(t, processor.rootCID)
	assert.Greater(t, len(processor.processedFiles), 0)

	err = processor.Close()
	assert.NoError(t, err)
	assert.Equal(t, "", processor.rootCID)
	assert.Equal(t, 0, len(processor.processedFiles))
	assert.Equal(t, 0, len(processor.directoryMetadata))
}

func TestStreamingProcessor_Integration(t *testing.T) {
	processor, ctx, testCtx := setupStreamProcessorTest(t)

	// Create test files with a realistic structure
	files := map[string]*fstest.MapFile{
		"test.txt": {
			Data: []byte("test content"),
			Mode: 0644,
		},
		"subdir/": {
			Mode: fs.ModeDir | 0755,
		},
		"subdir/nested.txt": {
			Data: []byte("nested content"),
			Mode: 0644,
		},
		"subdir/deeper/": {
			Mode: fs.ModeDir | 0755,
		},
		"subdir/deeper/file.txt": {
			Data: []byte("deeply nested content"),
			Mode: 0644,
		},
	}

	extractor := createStreamProcessorTestArchive(t, testCtx, files)

	// Process the archive
	err := processor.ProcessArchive(ctx, extractor)
	assert.NoError(t, err)

	// Get the root node
	rootNode, err := processor.GetRootNode(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, rootNode)

	// Get processed files
	retrievedFiles := processor.GetProcessedFiles()
	assert.Equal(t, 3, len(retrievedFiles)) // 3 files, 2 directories

	// Verify file metadata
	expectedFiles := []string{"test.txt", "subdir/nested.txt", "subdir/deeper/file.txt"}
	for _, expectedFile := range expectedFiles {
		found := false
		for _, file := range retrievedFiles {
			if file.Path == expectedFile {
				found = true
				assert.True(t, file.Processed)
				assert.NoError(t, file.Error)
				assert.Greater(t, file.Size, int64(0))
				break
			}
		}
		assert.True(t, found, "Expected file %s not found", expectedFile)
	}

	// Close the processor
	err = processor.Close()
	assert.NoError(t, err)
}

// Property-based test for various content sizes
func TestProcessArchive_PropertyBased(t *testing.T) {
	// Test various content sizes to ensure robustness
	contentSizes := []int{
		0,             // empty
		1,             // single byte
		units.KB,      // 1KB
		10 * units.KB, // 10KB
		units.MB,      // 1MB
	}

	for _, size := range contentSizes {
		t.Run(fmt.Sprintf("content_size_%d", size), func(t *testing.T) {
			processor, ctx, testCtx := setupStreamProcessorTest(t)

			// Generate content of specified size
			content := make([]byte, size)
			if size > 0 {
				// Fill with some pattern for better testing
				for i := range content {
					content[i] = byte(i % 256)
				}
			}

			files := map[string]*fstest.MapFile{
				"test.bin": {
					Data: content,
					Mode: 0644,
				},
			}

			extractor := createStreamProcessorTestArchive(t, testCtx, files)

			err := processor.ProcessArchive(ctx, extractor)
			assert.NoError(t, err)
			assert.NotEmpty(t, processor.rootCID)
			assert.Equal(t, 1, len(processor.processedFiles))

			// Verify file metadata
			fileInfo := processor.processedFiles[0]
			assert.Equal(t, "test.bin", fileInfo.Name)
			assert.Equal(t, int64(size), fileInfo.Size)
			assert.True(t, fileInfo.Processed)
			assert.NoError(t, fileInfo.Error)

			// Log human-readable size for debugging
			t.Logf("Processed file of size: %s", units.HumanSize(float64(size)))
		})
	}
}

// Test mixed content within unit test scope - focused on specific processor behavior
// TestProcessArchive_DeeplyNestedStructures tests processing of deeply nested directory hierarchies
func TestProcessArchive_DeeplyNestedStructures(t *testing.T) {
	tests := []struct {
		name          string
		depth         int
		filesPerLevel int
		expectedTotal int
		expectedDepth int
	}{
		{
			name:          "moderate depth with multiple files",
			depth:         5,
			filesPerLevel: 3,
			expectedTotal: 15,
			expectedDepth: 6, // +1 for root
		},
		{
			name:          "very deep hierarchy",
			depth:         10,
			filesPerLevel: 1,
			expectedTotal: 10,
			expectedDepth: 11,
		},
		{
			name:          "wide and shallow",
			depth:         3,
			filesPerLevel: 5,
			expectedTotal: 15,
			expectedDepth: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			processor, ctx, testCtx := setupStreamProcessorTest(t)

			// Generate nested directory structure
			files := make(map[string]*fstest.MapFile)

			for depth := 0; depth < tt.depth; depth++ {
				// Create directory path
				dirPath := ""
				for i := 0; i <= depth; i++ {
					if i > 0 {
						dirPath += "/"
					}
					dirPath += fmt.Sprintf("level%d", i)
				}

				// Add directory
				if depth > 0 {
					files[dirPath+"/"] = &fstest.MapFile{
						Mode: fs.ModeDir | 0755,
					}
				}

				// Add files at this level
				for fileIdx := 0; fileIdx < tt.filesPerLevel; fileIdx++ {
					filePath := fmt.Sprintf("%s/file%d.txt", dirPath, fileIdx)
					files[filePath] = &fstest.MapFile{
						Data: []byte(fmt.Sprintf("content at depth %d, file %d", depth, fileIdx)),
						Mode: 0644,
					}
				}
			}

			extractor := createStreamProcessorTestArchive(t, testCtx, files)

			err := processor.ProcessArchive(ctx, extractor)
			assert.NoError(t, err)
			assert.NotEmpty(t, processor.rootCID)
			assert.Equal(t, tt.expectedTotal, len(processor.processedFiles))
			assert.GreaterOrEqual(t, len(processor.directoryMetadata), tt.depth)

			// Verify all files processed successfully
			for _, file := range processor.processedFiles {
				assert.True(t, file.Processed)
				assert.NoError(t, file.Error)
			}

			// Calculate maximum depth from processed files
			maxDepth := 0
			for _, file := range processor.processedFiles {
				depth := len(strings.Split(file.Path, "/")) - 1
				if depth > maxDepth {
					maxDepth = depth
				}
			}
			assert.GreaterOrEqual(t, maxDepth, tt.depth-1)
		})
	}
}

// TestProcessArchive_SpecialCharacters tests handling of special characters in filenames
func TestProcessArchive_SpecialCharacters(t *testing.T) {
	processor, ctx, testCtx := setupStreamProcessorTest(t)

	// Create files with various special characters
	files := map[string]*fstest.MapFile{
		"file with spaces.txt": {
			Data: []byte("content with spaces in filename"),
			Mode: 0644,
		},
		"file-with-dashes.txt": {
			Data: []byte("content with dashes"),
			Mode: 0644,
		},
		"file_with_underscores.txt": {
			Data: []byte("content with underscores"),
			Mode: 0644,
		},
		"file.with.dots.txt": {
			Data: []byte("content with dots"),
			Mode: 0644,
		},
		"file+with+pluses.txt": {
			Data: []byte("content with pluses"),
			Mode: 0644,
		},
		"file(with)parentheses.txt": {
			Data: []byte("content with parentheses"),
			Mode: 0644,
		},
		"file[with]brackets.txt": {
			Data: []byte("content with brackets"),
			Mode: 0644,
		},
		"file{with}braces.txt": {
			Data: []byte("content with braces"),
			Mode: 0644,
		},
		"file@with@at.txt": {
			Data: []byte("content with at symbol"),
			Mode: 0644,
		},
		"file#with#hash.txt": {
			Data: []byte("content with hash"),
			Mode: 0644,
		},
		"file$with$dollar.txt": {
			Data: []byte("content with dollar"),
			Mode: 0644,
		},
		"file%with%percent.txt": {
			Data: []byte("content with percent"),
			Mode: 0644,
		},
		"file^with^caret.txt": {
			Data: []byte("content with caret"),
			Mode: 0644,
		},
		"file&with&ampersand.txt": {
			Data: []byte("content with ampersand"),
			Mode: 0644,
		},
		"file*with*asterisk.txt": {
			Data: []byte("content with asterisk"),
			Mode: 0644,
		},
		"special chars!@#$%^&*()[]{}+.txt": {
			Data: []byte("content with many special chars"),
			Mode: 0644,
		},
		// Directories with special characters
		"folder with spaces/": {
			Mode: fs.ModeDir | 0755,
		},
		"folder with spaces/file.txt": {
			Data: []byte("file in folder with spaces"),
			Mode: 0644,
		},
		"folder-with-dashes/": {
			Mode: fs.ModeDir | 0755,
		},
		"folder-with-dashes/file.txt": {
			Data: []byte("file in folder with dashes"),
			Mode: 0644,
		},
	}

	extractor := createStreamProcessorTestArchive(t, testCtx, files)

	err := processor.ProcessArchive(ctx, extractor)
	assert.NoError(t, err)
	assert.NotEmpty(t, processor.rootCID)

	// Count actual files (not directories)
	expectedFileCount := 18
	assert.Equal(t, expectedFileCount, len(processor.processedFiles))

	// Verify all files processed successfully
	for _, file := range processor.processedFiles {
		assert.True(t, file.Processed)
		assert.NoError(t, file.Error)
		assert.NotEmpty(t, file.Name)
	}

	// Verify specific special character files are present
	fileNames := make(map[string]bool)
	for _, file := range processor.processedFiles {
		fileNames[file.Name] = true
	}

	assert.True(t, fileNames["file with spaces.txt"])
	assert.True(t, fileNames["special chars!@#$%^&*()[]{}+.txt"])
}

// TestProcessArchive_MixedComplexStructure tests a realistic complex folder structure
func TestProcessArchive_MixedComplexStructure(t *testing.T) {
	processor, ctx, testCtx := setupStreamProcessorTest(t)

	// Create a realistic project structure
	files := map[string]*fstest.MapFile{
		// Root level files
		"README.md": {
			Data: []byte("# Project Documentation\n\nThis is a test project."),
			Mode: 0644,
		},
		"package.json": {
			Data: []byte(`{"name": "test-project", "version": "1.0.0", "scripts": {"test": "jest"}}`),
			Mode: 0644,
		},
		".gitignore": {
			Data: []byte("node_modules/\n*.log\n.env"),
			Mode: 0644,
		},
		".env.example": {
			Data: []byte("API_KEY=your_api_key_here\nDATABASE_URL=your_database_url"),
			Mode: 0644,
		},

		// Source directory structure
		"src/": {
			Mode: fs.ModeDir | 0755,
		},
		"src/index.js": {
			Data: []byte("const express = require('express');\nconst app = express();\napp.listen(3000);"),
			Mode: 0644,
		},
		"src/utils/": {
			Mode: fs.ModeDir | 0755,
		},
		"src/utils/logger.js": {
			Data: []byte("const winston = require('winston');\nmodule.exports = winston.createLogger({});"),
			Mode: 0644,
		},
		"src/utils/database.js": {
			Data: []byte("const mongoose = require('mongoose');\nmodule.exports = mongoose.connection;"),
			Mode: 0644,
		},
		"src/controllers/": {
			Mode: fs.ModeDir | 0755,
		},
		"src/controllers/user.js": {
			Data: []byte("exports.getUser = (req, res) => { res.json({user: 'test'}); };"),
			Mode: 0644,
		},
		"src/controllers/auth.js": {
			Data: []byte("exports.login = (req, res) => { res.json({token: 'test'}); };"),
			Mode: 0644,
		},
		"src/middleware/": {
			Mode: fs.ModeDir | 0755,
		},
		"src/middleware/auth.js": {
			Data: []byte("exports.authenticate = (req, res, next) => { next(); };"),
			Mode: 0644,
		},

		// Test directory structure
		"tests/": {
			Mode: fs.ModeDir | 0755,
		},
		"tests/unit/": {
			Mode: fs.ModeDir | 0755,
		},
		"tests/unit/user.test.js": {
			Data: []byte("const userController = require('../src/controllers/user');\ntest('getUser returns user', () => {});"),
			Mode: 0644,
		},
		"tests/integration/": {
			Mode: fs.ModeDir | 0755,
		},
		"tests/integration/api.test.js": {
			Data: []byte("const request = require('supertest');\ntest('API endpoints', async () => {});"),
			Mode: 0644,
		},
		"tests/fixtures/": {
			Mode: fs.ModeDir | 0755,
		},
		"tests/fixtures/user.json": {
			Data: []byte(`{"id": 1, "name": "Test User", "email": "test@example.com"}`),
			Mode: 0644,
		},

		// Config directory
		"config/": {
			Mode: fs.ModeDir | 0755,
		},
		"config/database.json": {
			Data: []byte(`{"development": {"host": "localhost"}, "production": {"host": "prod-db"}}`),
			Mode: 0644,
		},
		"config/redis.conf": {
			Data: []byte("port 6379\nbind 127.0.0.1\nmaxmemory 256mb"),
			Mode: 0644,
		},

		// Public directory with web assets
		"public/": {
			Mode: fs.ModeDir | 0755,
		},
		"public/index.html": {
			Data: []byte("<!DOCTYPE html><html><head><title>Test App</title></head><body><h1>Hello World</h1></body></html>"),
			Mode: 0644,
		},
		"public/css/": {
			Mode: fs.ModeDir | 0755,
		},
		"public/css/style.css": {
			Data: []byte("body { font-family: Arial, sans-serif; }\n.container { max-width: 1200px; }"),
			Mode: 0644,
		},
		"public/js/": {
			Mode: fs.ModeDir | 0755,
		},
		"public/js/app.js": {
			Data: []byte("console.log('App loaded');\ndocument.addEventListener('DOMContentLoaded', function() {});"),
			Mode: 0644,
		},
		"public/images/": {
			Mode: fs.ModeDir | 0755,
		},
		"public/images/logo.png": {
			Data: []byte("\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\nIDATx\x9cc\x00\x01\x00\x00\x05\x00\x01\r\n-\xdb\x00\x00\x00\x00IEND\xaeB`\x82"),
			Mode: 0644,
		},

		// Logs directory (typically empty)
		"logs/": {
			Mode: fs.ModeDir | 0755,
		},

		// Empty directory
		"temp/": {
			Mode: fs.ModeDir | 0755,
		},

		// Hidden directories and files
		".vscode/": {
			Mode: fs.ModeDir | 0755,
		},
		".vscode/settings.json": {
			Data: []byte(`{"editor.tabSize": 2, "editor.insertSpaces": true}`),
			Mode: 0644,
		},
		".vscode/launch.json": {
			Data: []byte(`{"version": "0.2.0", "configurations": [{"name": "Debug", "type": "node", "request": "launch"}]}`),
			Mode: 0644,
		},
	}

	extractor := createStreamProcessorTestArchive(t, testCtx, files)

	err := processor.ProcessArchive(ctx, extractor)
	assert.NoError(t, err)
	assert.NotEmpty(t, processor.rootCID)

	// Expected file count (excluding directories)
	expectedFileCount := 21
	assert.Equal(t, expectedFileCount, len(processor.processedFiles))

	// Verify all files processed successfully
	for _, file := range processor.processedFiles {
		assert.True(t, file.Processed)
		assert.NoError(t, file.Error)
	}

	// Verify specific important files are present
	filePaths := make(map[string]bool)
	for _, file := range processor.processedFiles {
		filePaths[file.Path] = true
	}

	// Check root files
	assert.True(t, filePaths["README.md"])
	assert.True(t, filePaths["package.json"])
	assert.True(t, filePaths[".gitignore"])
	assert.True(t, filePaths[".env.example"])

	// Check source structure
	assert.True(t, filePaths["src/index.js"])
	assert.True(t, filePaths["src/utils/logger.js"])
	assert.True(t, filePaths["src/controllers/user.js"])
	assert.True(t, filePaths["src/middleware/auth.js"])

	// Check test structure
	assert.True(t, filePaths["tests/unit/user.test.js"])
	assert.True(t, filePaths["tests/integration/api.test.js"])
	assert.True(t, filePaths["tests/fixtures/user.json"])

	// Check web assets
	assert.True(t, filePaths["public/index.html"])
	assert.True(t, filePaths["public/css/style.css"])
	assert.True(t, filePaths["public/js/app.js"])
	assert.True(t, filePaths["public/images/logo.png"])

	// Check hidden files
	assert.True(t, filePaths[".vscode/settings.json"])
	assert.True(t, filePaths[".vscode/launch.json"])

	// Verify directory metadata was created
	assert.Greater(t, len(processor.directoryMetadata), 10) // Should have many directories
}

// TestProcessArchive_LargeNumberOfFiles tests performance with many small files
func TestProcessArchive_LargeNumberOfFiles(t *testing.T) {
	processor, ctx, testCtx := setupStreamProcessorTest(t)

	// Generate a large number of small files
	files := make(map[string]*fstest.MapFile)
	const numFiles = 100
	const numDirectories = 10

	// Create directory structure
	for dir := 0; dir < numDirectories; dir++ {
		dirName := fmt.Sprintf("dir%d", dir)
		files[dirName+"/"] = &fstest.MapFile{
			Mode: fs.ModeDir | 0755,
		}

		// Add files to each directory
		filesPerDir := numFiles / numDirectories
		for file := 0; file < filesPerDir; file++ {
			filePath := fmt.Sprintf("%s/file%d.txt", dirName, file)
			files[filePath] = &fstest.MapFile{
				Data: []byte(fmt.Sprintf("Content for file %d in directory %d", file, dir)),
				Mode: 0644,
			}
		}
	}

	extractor := createStreamProcessorTestArchive(t, testCtx, files)

	// Measure processing time
	start := time.Now()
	err := processor.ProcessArchive(ctx, extractor)
	processingTime := time.Since(start)

	assert.NoError(t, err)
	assert.NotEmpty(t, processor.rootCID)
	assert.Equal(t, numFiles, len(processor.processedFiles))

	// Verify all files processed successfully
	for _, file := range processor.processedFiles {
		assert.True(t, file.Processed)
		assert.NoError(t, file.Error)
	}

	// Performance assertions (adjust thresholds based on your environment)
	assert.Less(t, processingTime, 5*time.Second, "Processing should complete in reasonable time")

	// Memory usage should be reasonable
	assert.Greater(t, len(processor.directoryMetadata), numDirectories)

	t.Logf("Processed %d files in %v (%.2f files/sec)",
		numFiles, processingTime, float64(numFiles)/processingTime.Seconds())
}

// TestProcessArchive_EmptyDirectoryEdgeCases tests various empty directory scenarios
func TestProcessArchive_EmptyDirectoryEdgeCases(t *testing.T) {
	processor, ctx, testCtx := setupStreamProcessorTest(t)

	// Create structure with various empty directory scenarios
	files := map[string]*fstest.MapFile{
		// Empty directory at root
		"empty/": {
			Mode: fs.ModeDir | 0755,
		},

		// Empty directory nested under files
		"has_files.txt": {
			Data: []byte("some content"),
			Mode: 0644,
		},
		"nested_empty/": {
			Mode: fs.ModeDir | 0755,
		},

		// Deep empty directory chain
		"deep/": {
			Mode: fs.ModeDir | 0755,
		},
		"deep/level1/": {
			Mode: fs.ModeDir | 0755,
		},
		"deep/level1/level2/": {
			Mode: fs.ModeDir | 0755,
		},
		"deep/level1/level2/level3/": {
			Mode: fs.ModeDir | 0755,
		},

		// Mixed empty and non-empty
		"mixed/": {
			Mode: fs.ModeDir | 0755,
		},
		"mixed/sub_empty/": {
			Mode: fs.ModeDir | 0755,
		},
		"mixed/has_file.txt": {
			Data: []byte("file in mixed directory"),
			Mode: 0644,
		},
		"mixed/sub_with_file/": {
			Mode: fs.ModeDir | 0755,
		},
		"mixed/sub_with_file/file.txt": {
			Data: []byte("file in subdirectory"),
			Mode: 0644,
		},

		// Only files at various levels
		"level0.txt": {
			Data: []byte("level 0"),
			Mode: 0644,
		},
		"level1/": {
			Mode: fs.ModeDir | 0755,
		},
		"level1/level1.txt": {
			Data: []byte("level 1"),
			Mode: 0644,
		},
	}

	extractor := createStreamProcessorTestArchive(t, testCtx, files)

	err := processor.ProcessArchive(ctx, extractor)
	assert.NoError(t, err)
	assert.NotEmpty(t, processor.rootCID)

	// Expected files (non-directory entries)
	expectedFileCount := 5
	assert.Equal(t, expectedFileCount, len(processor.processedFiles))

	// Verify all files processed successfully
	for _, file := range processor.processedFiles {
		assert.True(t, file.Processed)
		assert.NoError(t, file.Error)
	}

	// Should have directory metadata for all directories
	assert.Greater(t, len(processor.directoryMetadata), 8) // At least the empty directories

	// Verify specific files exist
	filePaths := make(map[string]bool)
	for _, file := range processor.processedFiles {
		filePaths[file.Path] = true
	}

	assert.True(t, filePaths["has_files.txt"])
	assert.True(t, filePaths["level0.txt"])
	assert.True(t, filePaths["level1/level1.txt"])
	assert.True(t, filePaths["mixed/has_file.txt"])
	assert.True(t, filePaths["mixed/sub_with_file/file.txt"])
}

// TestProcessArchive_PermissionsAndModes tests various file permission scenarios
func TestProcessArchive_PermissionsAndModes(t *testing.T) {
	processor, ctx, testCtx := setupStreamProcessorTest(t)

	// Create files with different permission modes
	files := map[string]*fstest.MapFile{
		"executable.sh": {
			Data: []byte("#!/bin/bash\necho 'Hello World'"),
			Mode: 0755, // Executable
		},
		"readonly.txt": {
			Data: []byte("This file is read-only"),
			Mode: 0444, // Read-only
		},
		"private.txt": {
			Data: []byte("This file is private"),
			Mode: 0600, // Read/write for owner only
		},
		"shared.txt": {
			Data: []byte("This file is shared"),
			Mode: 0666, // Read/write for all
		},
		"normal.txt": {
			Data: []byte("Normal file permissions"),
			Mode: 0644, // Standard file permissions
		},
		// Directories with different permissions
		"restricted/": {
			Mode: fs.ModeDir | 0700, // Restricted directory
		},
		"restricted/file.txt": {
			Data: []byte("File in restricted directory"),
			Mode: 0644,
		},
		"public/": {
			Mode: fs.ModeDir | 0755, // Public directory
		},
		"public/file.txt": {
			Data: []byte("File in public directory"),
			Mode: 0644,
		},
		// Create some files with unusual permission bits (though these might be filtered by OS)
		"suid_program": {
			Data: []byte("program with setuid"),
			Mode: 04755, // Setuid bit set
		},
		"sgid_program": {
			Data: []byte("program with setgid"),
			Mode: 02755, // Setgid bit set
		},
	}

	extractor := createStreamProcessorTestArchive(t, testCtx, files)

	err := processor.ProcessArchive(ctx, extractor)
	assert.NoError(t, err)
	assert.NotEmpty(t, processor.rootCID)

	// Expected file count
	expectedFileCount := 9
	assert.Equal(t, expectedFileCount, len(processor.processedFiles))

	// Verify all files processed successfully
	for _, file := range processor.processedFiles {
		assert.True(t, file.Processed)
		assert.NoError(t, file.Error)
	}

	// Verify files are processed regardless of permissions
	fileNames := make(map[string]bool)
	for _, file := range processor.processedFiles {
		fileNames[file.Name] = true
	}

	assert.True(t, fileNames["executable.sh"])
	assert.True(t, fileNames["readonly.txt"])
	assert.True(t, fileNames["private.txt"])
	assert.True(t, fileNames["shared.txt"])
	assert.True(t, fileNames["normal.txt"])
	assert.True(t, fileNames["suid_program"])
	assert.True(t, fileNames["sgid_program"])

	// Verify directory metadata was created
	assert.GreaterOrEqual(t, len(processor.directoryMetadata), 2) // restricted/ and public/
}

// Test mixed content within unit test scope - focused on specific processor behavior
// TestProcessArchive_ComplexArchive creates and processes a 10MB archive with many small files
// This test is designed to stress test the streaming processor with large amounts of data
// It creates both nested and sharded directory structures to test comprehensive scenarios
func TestProcessArchive_ComplexArchive(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping 10MB archive test in short mode")
	}

	processor, ctx, testCtx := setupStreamProcessorTest(t)

	// Generate a large number of small files to reach approximately 10MB
	const targetSizeBytes = 10 * units.MiB
	const avgFileSize = units.KiB                  // 1KB average file size
	const numFiles = targetSizeBytes / avgFileSize // Approximately 1M files

	// Split between nested and sharded structures (50/50)
	const nestedFiles = numFiles / 2
	const shardedFiles = numFiles - nestedFiles

	const nestedDepth = 5                                 // Depth for nested structure
	const shardedDirs = 500                               // Number of sharded directories
	const nestedFilesPerLevel = nestedFiles / nestedDepth // Files per level in nested structure

	t.Logf("Generating archive with ~%d files (nested: %d, sharded: %d) across mixed structures (target: %s)",
		numFiles, nestedFiles, shardedFiles, units.HumanSize(float64(targetSizeBytes)))

	// Create files map
	files := make(map[string]*fstest.MapFile)

	// Generate deterministic content to ensure reproducible tests
	rng := rand.New(rand.NewSource(42)) // Fixed seed for reproducibility
	filesGenerated := 0

	// === NESTED DIRECTORY STRUCTURE ===
	// Creates deep nesting: level0/file.txt, level1/level1/file.txt, level2/level2/level2/file.txt, etc.
	for depth := 0; depth < nestedDepth; depth++ {
		// Create nested directory path
		dirPath := ""
		for i := 0; i <= depth; i++ {
			if i > 0 {
				dirPath += "/"
			}
			dirPath += fmt.Sprintf("level%d", i)
		}

		// Create directory entry
		files[dirPath+"/"] = &fstest.MapFile{
			Mode: fs.ModeDir | 0755,
		}

		// Add files at this level
		filesAtThisLevel := nestedFilesPerLevel
		if depth == nestedDepth-1 {
			// Add remaining files to deepest level
			filesAtThisLevel = nestedFiles - (nestedFilesPerLevel * (nestedDepth - 1))
		}

		for fileIdx := 0; fileIdx < filesAtThisLevel; fileIdx++ {
			fileName := fmt.Sprintf("nested%06d.txt", filesGenerated)
			filePath := fmt.Sprintf("%s/%s", dirPath, fileName)

			// Generate pseudo-random content of 1KB
			content := make([]byte, avgFileSize)
			rng.Read(content)

			// Add some structure to make it look like real text
			for i := 0; i < avgFileSize; i++ {
				// Make it printable ASCII characters
				content[i] = 32 + (content[i] % 95) // ASCII printable range
			}

			// Add file metadata to make it look like real files
			header := fmt.Sprintf("// File: %s\n// Directory: %s\n// Index: %d\n// Type: NESTED\n\n",
				fileName, dirPath, filesGenerated)
			copy(content[:len(header)], []byte(header))

			files[filePath] = &fstest.MapFile{
				Data: content,
				Mode: 0644,
			}

			filesGenerated++
		}
	}

	// === SHARDED DIRECTORY STRUCTURE ===
	// Creates flat sharded structure: dir000/file.txt, dir001/file.txt, dir002/file.txt, etc.
	for dirIdx := 0; dirIdx < shardedDirs; dirIdx++ {
		dirPath := fmt.Sprintf("shard%03d", dirIdx)
		files[dirPath+"/"] = &fstest.MapFile{
			Mode: fs.ModeDir | 0755,
		}

		// Calculate files per directory
		filesPerDir := shardedFiles / shardedDirs
		if dirIdx < shardedFiles%shardedDirs {
			filesPerDir++ // Distribute remainder
		}

		// Add files to this directory
		for fileIdx := 0; fileIdx < filesPerDir; fileIdx++ {
			fileName := fmt.Sprintf("shard%06d.txt", filesGenerated)
			filePath := fmt.Sprintf("%s/%s", dirPath, fileName)

			// Generate pseudo-random content of 1KB
			content := make([]byte, avgFileSize)
			rng.Read(content)

			// Add some structure to make it look like real text
			for i := 0; i < avgFileSize; i++ {
				// Make it printable ASCII characters
				content[i] = 32 + (content[i] % 95) // ASCII printable range
			}

			// Add file metadata to make it look like real files
			header := fmt.Sprintf("// File: %s\n// Directory: %s\n// Index: %d\n// Type: SHARDED\n\n",
				fileName, dirPath, filesGenerated)
			copy(content[:len(header)], []byte(header))

			files[filePath] = &fstest.MapFile{
				Data: content,
				Mode: 0644,
			}

			filesGenerated++
		}
	}

	t.Logf("Generated %d files across mixed nested/sharded directories", filesGenerated)

	// Create the archive (this may take some time)
	start := time.Now()
	extractor := createStreamProcessorTestArchive(t, testCtx, files)
	archiveCreationTime := time.Since(start)

	t.Logf("Archive created in %v", archiveCreationTime)

	processingStart := time.Now()
	err := processor.ProcessArchive(ctx, extractor)
	processingTime := time.Since(processingStart)

	if err != nil {
		t.Fatalf("Failed to process 10MB archive: %v", err)
	}

	// Validate results
	t.Logf("Archive processing completed in %v", processingTime)
	t.Logf("Processed %d files", len(processor.processedFiles))
	t.Logf("Created %d directory entries", len(processor.directoryMetadata))

	// Basic validation
	assert.NotEmpty(t, processor.rootCID, "Root CID should be set")
	assert.Equal(t, filesGenerated, len(processor.processedFiles), "All files should be processed")
	assert.Greater(t, len(processor.directoryMetadata), (nestedDepth+shardedDirs)/2, "Should have processed both nested and sharded directories")

	// Verify all files were processed successfully
	processedCount := 0
	errorCount := 0
	processingErrors := make([]error, 0)
	for _, file := range processor.processedFiles {
		if file.Processed {
			processedCount++
		}
		if file.Error != nil {
			errorCount++
			processingErrors = append(processingErrors, file.Error)
		}
	}

	t.Logf("Successfully processed: %d files", processedCount)
	t.Logf("Failed processing: %d files", errorCount)

	assert.Equal(t, filesGenerated, processedCount, "All files should be successfully processed")
	assert.Equal(t, 0, errorCount, "No files should have processing errors")

	// Performance assertions (adjust these based on your environment)
	// These are generous bounds to account for different test environments
	assert.Less(t, processingTime, 5*time.Minute, "Processing should complete within 5 minutes")
	assert.Less(t, archiveCreationTime, 2*time.Minute, "Archive creation should complete within 2 minutes")

	// Archive creation throughput
	totalDataMB := float64(targetSizeBytes) / (1024 * 1024)
	t.Logf("Archive creation throughput: %.2f MB/s", totalDataMB/archiveCreationTime.Seconds())

	// Verify specific files exist to ensure content integrity for both nested and sharded structures
	filePaths := make(map[string]bool)
	for _, file := range processor.processedFiles {
		filePaths[file.Path] = true
	}

	// === VALIDATE NESTED STRUCTURE ===

	// Count nested files to ensure proper distribution
	nestedFileCount := 0
	for path := range filePaths {
		if strings.Contains(path, "/nested") && (strings.Contains(path, "/level") || strings.HasPrefix(path, "level")) {
			nestedFileCount++
		}
	}
	assert.GreaterOrEqual(t, nestedFileCount, nestedFiles/10, "Should have processed significant number of nested files")
	t.Logf("✓ Processed %d nested files", nestedFileCount)

	// === VALIDATE SHARDED STRUCTURE ===

	// Count sharded files to ensure proper distribution
	shardedFileCount := 0
	for path := range filePaths {
		if strings.Contains(path, "/shard") {
			shardedFileCount++
		}
	}
	assert.GreaterOrEqual(t, shardedFileCount, shardedFiles/10, "Should have processed significant number of sharded files")
	t.Logf("✓ Processed %d sharded files", shardedFileCount)

	// === OVERALL VALIDATION ===
	totalProcessedFiles := nestedFileCount + shardedFileCount
	assert.GreaterOrEqual(t, totalProcessedFiles, filesGenerated/10, "Should have processed significant portion of total files")
	t.Logf("✓ Processed %d total files (nested: %d, sharded: %d)", totalProcessedFiles, nestedFileCount, shardedFileCount)

	// Verify mixed structure by checking we have both types
	assert.True(t, nestedFileCount > 0, "Should have nested files")
	assert.True(t, shardedFileCount > 0, "Should have sharded files")

	// Verify root CID is valid
	assert.NotEmpty(t, processor.rootCID, "Root CID should be valid")

	t.Logf("✓ Successfully processed %s archive with mixed nested/sharded structure: %d files in %v",
		units.HumanSize(float64(targetSizeBytes)), filesGenerated, processingTime)
}
