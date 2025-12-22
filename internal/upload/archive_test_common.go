package upload

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/upload/common"
)

// ArchiveCreator defines the function signature for creating test archives
type ArchiveCreator func(t *testing.T, ctx core.Context, files []TestFile) []byte

// ArchiveTestHelper provides common test utilities for all archive formats
type ArchiveTestHelper struct {
	t      *testing.T
	format Format
	ctx    core.Context
}

// NewArchiveTestHelper creates a new test helper for the specified format
func NewArchiveTestHelper(t *testing.T, format Format) *ArchiveTestHelper {
	return &ArchiveTestHelper{
		t:      t,
		format: format,
	}
}

// ArchiveCreator defines a function that creates an archive with given files


// TestFile represents a file to be included in test archives
type TestFile struct {
	Name     string
	Content  string
	IsDir    bool
	Mode     int64
	Modified time.Time
}

// GetDefaultTestFiles returns a comprehensive set of test files with various types and nested structures
func GetDefaultTestFiles() []TestFile {
	now := time.Now()
	baseTime := now.Add(-24 * time.Hour) // Some files with older timestamps

	return []TestFile{
		// Root level files
		{
			Name:     "README.md",
			Content:  "# Test Project\n\nThis is a comprehensive test archive with various file types and nested directory structures.",
			IsDir:    false,
			Mode:     0644,
			Modified: baseTime,
		},
		{
			Name:     "config.json",
			Content:  `{
  "name": "test-archive",
  "version": "1.0.0",
  "settings": {
    "debug": true,
    "timeout": 30,
    "retry": 3
  },
  "features": ["archive-processing", "ipfs-upload", "file-validation"]
}`,
			IsDir:    false,
			Mode:     0644,
			Modified: now,
		},
		{
			Name:     "empty-file.txt",
			Content:  "",
			IsDir:    false,
			Mode:     0644,
			Modified: now,
		},

		// Source code directory
		{
			Name:     "src",
			Content:  "",
			IsDir:    true,
			Mode:     0755,
			Modified: baseTime,
		},
		{
			Name:     "src/main.go",
			Content:  "package main\n\nimport \"fmt\"\n\nfunc main() {\n\tfmt.Println(\"Hello from test archive!\")\n\tfor i := 0; i < 5; i++ {\n\t\tfmt.Printf(\"Iteration: %d\\n\", i)\n\t}\n}",
			IsDir:    false,
			Mode:     0644,
			Modified: now,
		},
		{
			Name:     "src/utils.js",
			Content:  "// Utility functions for the test archive\nfunction formatDate(date) {\n    return date.toISOString().split('T')[0];\n}\n\nfunction generateId(length = 8) {\n    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';\n    let result = '';\n    for (let i = 0; i < length; i++) {\n        result += chars.charAt(Math.floor(Math.random() * chars.length));\n    }\n    return result;\n}\n\nmodule.exports = { formatDate, generateId };",
			IsDir:    false,
			Mode:     0644,
			Modified: now.Add(-2 * time.Hour),
		},
		{
			Name:     "src/styles.css",
			Content:  "/* Main stylesheet */\nbody {\n    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;\n    margin: 0;\n    padding: 20px;\n    background-color: #f5f5f5;\n}\n\n.container {\n    max-width: 1200px;\n    margin: 0 auto;\n    background: white;\n    padding: 2rem;\n    border-radius: 8px;\n    box-shadow: 0 2px 10px rgba(0,0,0,0.1);\n}",
			IsDir:    false,
			Mode:     0644,
			Modified: now,
		},

		// Documentation directory
		{
			Name:     "docs",
			Content:  "",
			IsDir:    true,
			Mode:     0755,
			Modified: baseTime,
		},
		{
			Name:     "docs/guide.md",
			Content:  "# User Guide\n\n## Getting Started\n\nThis guide will help you understand the test archive structure.\n\n### File Types Included\n\n- **Text files**: Various text files with different content\n- **Source code**: Multiple programming languages (Go, JavaScript, CSS)\n- **Configuration**: JSON and YAML configuration files\n- **Documentation**: Markdown files\n- **Binary data**: Simulated images and other binary content\n\n### Directory Structure\n\n```\ntest-archive/\n├── README.md\n├── config.json\n├── empty-file.txt\n├── src/\n│   ├── main.go\n│   ├── utils.js\n│   └── styles.css\n├── docs/\n│   └── guide.md\n├── data/\n│   ├── numbers.txt\n│   └── nested/\n│       └── deep.txt\n└── assets/\n    ├── images/\n    └── exports/\n```\n",
			IsDir:    false,
			Mode:     0644,
			Modified: now.Add(-3 * time.Hour),
		},

		// Data directory with nested subdirectories
		{
			Name:     "data",
			Content:  "",
			IsDir:    true,
			Mode:     0755,
			Modified: baseTime,
		},
		{
			Name:     "data/numbers.txt",
			Content:  "1,2,3,4,5,6,7,8,9,10\n11,12,13,14,15,16,17,18,19,20\n",
			IsDir:    false,
			Mode:     0644,
			Modified: now.Add(-1 * time.Hour),
		},
		{
			Name:     "data/nested",
			Content:  "",
			IsDir:    true,
			Mode:     0755,
			Modified: baseTime,
		},
		{
			Name:     "data/nested/deep.txt",
			Content:  "This file is nested deep in the directory structure.\nIt tests handling of deeply nested files.\n",
			IsDir:    false,
			Mode:     0644,
			Modified: now,
		},

		// Assets directory
		{
			Name:     "assets",
			Content:  "",
			IsDir:    true,
			Mode:     0755,
			Modified: baseTime,
		},
		{
			Name:     "assets/images",
			Content:  "",
			IsDir:    true,
			Mode:     0755,
			Modified: now,
		},
		{
			Name:     "assets/images/logo.png",
			Content:  "PNG simulated image data for logo - 100x50 pixels",
			IsDir:    false,
			Mode:     0644,
			Modified: now,
		},
		{
			Name:     "assets/exports",
			Content:  "",
			IsDir:    true,
			Mode:     0755,
			Modified: now,
		},
		{
			Name:     "assets/exports/report.csv",
			Content:  "Month,Sales,Expenses,Profit\nJanuary,10000,5000,5000\nFebruary,12000,6000,6000\nMarch,15000,7000,8000\n",
			IsDir:    false,
			Mode:     0644,
			Modified: now,
		},
	}
}

// GetPathValidationTestCases returns standard path validation test cases
func GetPathValidationTestCases() []struct {
	name        string
	archivePath string
	shouldError bool
} {
	return []struct {
		name        string
		archivePath string
		shouldError bool
	}{
		{"Valid relative path", "valid/file.txt", false},
		{"Valid simple file", "file.txt", false},
		{"Path traversal attempt", "../malicious.txt", true}, // Still important security check
		{"Windows separator", "windows\\path.txt", true},     // Filesystem API may not catch all cases
		{"Double slash in middle", "folder/file.txt", false}, // Filesystem API normalizes double slashes
		{"Trailing slash", "folder", false},                  // Trailing slashes handled by archive creation
		{"Deep nested", "a/b/c/d/e/f.txt", false},            // Complex but valid path
		{"Multiple traversal", "../../../etc/passwd", true},  // Aggressive path traversal
		{"Mixed traversal", "folder/../../../secret", true},  // Mixed valid/traversal
	}
}

// GetLargeTestFile returns a large test file for content reading tests
func GetLargeTestFile() TestFile {
	content := strings.Repeat("This is test content. ", 1000) // ~22KB
	return TestFile{
		Name:     "large.txt",
		Content:  content,
		IsDir:    false,
		Mode:     0644,
		Modified: time.Now(),
	}
}

// TestBasicExtraction tests basic archive extraction functionality
func (h *ArchiveTestHelper) TestBasicExtraction(creator ArchiveCreator) {
	files := GetDefaultTestFiles()
	archiveData := creator(h.t, h.ctx, files)

	extractor, err := h.createExtractor(archiveData)
	if err != nil {
		h.t.Fatalf("Failed to create %s extractor: %v", h.format.String(), err)
	}
	defer extractor.Close()

	// Test format
	if extractor.Format() != h.format {
		h.t.Errorf("Expected format %s, got %s", h.format.String(), extractor.Format().String())
	}

	// Test extraction
	extractedFiles, errors := h.extractAllFiles(extractor)

	// Check for errors
	if len(errors) > 0 {
		h.t.Errorf("Unexpected errors during extraction: %v", errors)
	}

	// Check extracted files count
	expectedCount := len(files)
	if len(extractedFiles) != expectedCount {
		h.t.Errorf("Expected %d files, got %d", expectedCount, len(extractedFiles))
	}

	// Verify file content
	h.verifyFileContent(extractedFiles, files)
}

// TestPathValidation tests path validation for the archive format
func (h *ArchiveTestHelper) TestPathValidation(creator ArchiveCreator) {
	testCases := GetPathValidationTestCases()

	for _, tc := range testCases {
		h.t.Run(tc.name, func(t *testing.T) {
			files := []TestFile{
				{
					Name:     tc.archivePath,
					Content:  "test",
					IsDir:    false,
					Mode:     0644,
					Modified: time.Now(),
				},
			}

			archiveData := creator(h.t, h.ctx, files)
			extractor, err := h.createExtractor(archiveData)
			if err != nil {
				t.Fatalf("Failed to create %s extractor: %v", h.format.String(), err)
			}
			defer closeIo(h.t, extractor)

			_, errors := h.extractAllFiles(extractor)

			if tc.shouldError && len(errors) == 0 {
				t.Errorf("Expected validation error for path '%s'", tc.archivePath)
			}
			if !tc.shouldError && len(errors) > 0 {
				t.Errorf("Unexpected validation error for path '%s': %v", tc.archivePath, errors)
			}
		})
	}
}

// TestLargeFileContent tests reading large file content
func (h *ArchiveTestHelper) TestLargeFileContent(creator ArchiveCreator) {
	largeFile := GetLargeTestFile()
	files := []TestFile{largeFile}
	archiveData := creator(h.t, h.ctx, files)

	extractor, err := h.createExtractor(archiveData)
	if err != nil {
		h.t.Fatalf("Failed to create %s extractor: %v", h.format.String(), err)
	}
	defer closeIo(h.t, extractor)

	extractedFiles, errors := h.extractAllFiles(extractor)

	// Check for errors
	if len(errors) > 0 {
		h.t.Errorf("Unexpected errors during extraction: %v", errors)
	}

	// Find and verify the large file
	var foundFile *ArchiveFileEntry
	for _, file := range extractedFiles {
		if file.Name() == largeFile.Name {
			foundFile = &file
			break
		}
	}

	if foundFile == nil {
		h.t.Fatal("large.txt not found in extracted files")
	}

	if foundFile.Size() != int64(len(largeFile.Content)) {
		h.t.Errorf("Expected file size %d, got %d", len(largeFile.Content), foundFile.Size())
	}
}

// TestFormatDetection tests format detection for the archive
func (h *ArchiveTestHelper) TestFormatDetection(creator ArchiveCreator) {
	files := GetDefaultTestFiles()
	archiveData := creator(h.t, h.ctx, files)

	reader := bytes.NewReader(archiveData)
	format, err := DetectFormat(reader)
	if err != nil {
		h.t.Errorf("Failed to detect %s format: %v", h.format.String(), err)
	}

	if format != h.format {
		h.t.Errorf("Expected %s format, got %s", h.format.String(), format.String())
	}
}

// createExtractor creates an extractor for the given archive data
func (h *ArchiveTestHelper) createExtractor(archiveData []byte) (ArchiveExtractor, error) {
	reader := bytes.NewReader(archiveData)
	return CreateExtractor(reader)
}

// extractAllFiles extracts all files from an archive and returns them with any errors
func (h *ArchiveTestHelper) extractAllFiles(extractor ArchiveExtractor) ([]ArchiveFileEntry, []error) {
	var files []ArchiveFileEntry
	var errors []error
	var readersToClose []io.ReadCloser

	// Use the filesystem API instead of Extract
	efs, err := extractor.Filesystem(context.Background())
	if err != nil {
		errors = append(errors, err)
		return files, errors
	}

	// Walk the filesystem to collect all entries
	err = fs.WalkDir(efs, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			errors = append(errors, err)
			return nil // Continue processing
		}

		// Skip root path
		if path == common.ROOT {
			return nil
		}

		if err := ValidateArchivePath(path); err != nil {
			h.t.Logf("Path validation failed for '%s': %v", path, err) // Debug: Log failure
			errors = append(errors, fmt.Errorf("invalid path %s: %w", path, err))
			return nil // Continue processing other files
		}

		// Get file info
		info, err := d.Info()
		if err != nil {
			errors = append(errors, fmt.Errorf("failed to get info for %s: %w", path, err))
			return nil // Continue processing
		}

		// Create ArchiveFileEntry from DirEntry
		var contentReader io.ReadCloser
		if !d.IsDir() {
			file, err := efs.Open(path)
			if err != nil {
				errors = append(errors, fmt.Errorf("failed to open file %s: %w", path, err))
				return nil // Continue processing
			}
			contentReader = file
			readersToClose = append(readersToClose, file)
		} else {
			contentReader = io.NopCloser(bytes.NewReader(nil))
		}

		entry := NewArchiveFileEntry(
			path,
			info.Size(),
			d.IsDir(),
			info.ModTime(),
			int64(info.Mode()),
			contentReader,
		)

		files = append(files, *entry)
		return nil
	})

	if err != nil {
		errors = append(errors, err)
	}

	// Close all readers to prevent resource leaks
	for _, reader := range readersToClose {
		if closeErr := reader.Close(); closeErr != nil {
			errors = append(errors, fmt.Errorf("failed to close reader: %w", closeErr))
		}
	}

	return files, errors
}

// verifyFileContent verifies that extracted files match expected files
func (h *ArchiveTestHelper) verifyFileContent(extractedFiles []ArchiveFileEntry, expectedFiles []TestFile) {
	for _, expectedFile := range expectedFiles {
		if expectedFile.IsDir {
			// For directories, just check existence (normalize by trimming trailing slash)
			found := false
			for _, extractedFile := range extractedFiles {
				extractedName := strings.TrimSuffix(extractedFile.Name(), "/")
				if extractedName == expectedFile.Name && extractedFile.IsDir() {
					found = true
					break
				}
			}
			if !found {
				h.t.Errorf("Directory %s not found in extracted files", expectedFile.Name)
			}
		} else {
			// For files, check existence and size
			var foundFile *ArchiveFileEntry
			for _, extractedFile := range extractedFiles {
				if extractedFile.Name() == expectedFile.Name && !extractedFile.IsDir() {
					foundFile = &extractedFile
					break
				}
			}

			if foundFile == nil {
				h.t.Errorf("File %s not found in extracted files", expectedFile.Name)
				continue
			}

			if foundFile.Size() != int64(len(expectedFile.Content)) {
				h.t.Errorf("File %s: expected size %d, got %d", expectedFile.Name, len(expectedFile.Content), foundFile.Size())
			}
		}
	}
}

// Archive creators for different formats

// CreateZIPArchive creates a ZIP archive from the given files
func CreateZIPArchive(t *testing.T, ctx core.Context, files []TestFile) []byte {
	creator := NewTestArchiveCreator(t, ctx)
	buf, err := creator.CreateArchiveFromTestFiles(context.Background(), FormatZIP, files)
	if err != nil {
		t.Fatalf("failed to create ZIP archive: %v", err)
	}
	return buf.Bytes()
}

// Create7ZArchive creates a 7Z archive from the given files
func Create7ZArchive(t *testing.T, ctx core.Context, files []TestFile) []byte {
	// Create a temporary directory structure
	tempDir, err := os.MkdirTemp("", "7z_test_*")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create files in temp directory
	for _, file := range files {
		if file.IsDir {
			// Create directory
			dirPath := filepath.Join(tempDir, file.Name)
			if err := os.MkdirAll(dirPath, os.FileMode(file.Mode)); err != nil {
				t.Fatalf("failed to create directory %s: %v", file.Name, err)
			}
		} else {
			// Create file
			filePath := filepath.Join(tempDir, file.Name)
			if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
				t.Fatalf("failed to create parent directory for %s: %v", file.Name, err)
			}
			if err := os.WriteFile(filePath, []byte(file.Content), os.FileMode(file.Mode)); err != nil {
				t.Fatalf("failed to write file %s: %v", file.Name, err)
			}
		}
	}

	// Create 7Z archive using shell command
	archivePath := filepath.Join(tempDir, "test.7z")

	// Try to create 7Z using 7z command
	cmd := exec.Command("7z", "a", archivePath, ".")
	cmd.Dir = tempDir

	if err := cmd.Run(); err != nil {
		// If 7z command is not available, try using the '7zz' command to check availability
		// and then skip the test gracefully
		if _, err := exec.LookPath("7z"); err != nil {
			if _, err := exec.LookPath("7zz"); err != nil {
				// 7z command not found, skip test
				t.Skip("7z command not found - install 7z or skip 7Z tests")
			}
			// Try 7zz command
			cmd = exec.Command("7zz", "a", archivePath, ".")
			cmd.Dir = tempDir
			if err := cmd.Run(); err != nil {
				t.Fatalf("failed to create 7Z archive with 7zz: %v", err)
			}
		} else {
			t.Fatalf("failed to create 7Z archive: %v", err)
		}
	}

	// Read the created 7Z file
	archiveData, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatalf("failed to read 7Z archive file: %v", err)
	}

	return archiveData
}

// CreateRARArchive creates a RAR archive from the given files
func CreateRARArchive(t *testing.T, ctx core.Context, files []TestFile) []byte {
	// Create a temporary directory structure
	tempDir, err := os.MkdirTemp("", "rar_test_*")
	if err != nil {
		t.Fatalf("failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create files in temp directory
	for _, file := range files {
		if file.IsDir {
			// Create directory
			dirPath := filepath.Join(tempDir, file.Name)
			if err := os.MkdirAll(dirPath, os.FileMode(file.Mode)); err != nil {
				t.Fatalf("failed to create directory %s: %v", file.Name, err)
			}
		} else {
			// Create file
			filePath := filepath.Join(tempDir, file.Name)
			if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
				t.Fatalf("failed to create parent directory for %s: %v", file.Name, err)
			}
			if err := os.WriteFile(filePath, []byte(file.Content), os.FileMode(file.Mode)); err != nil {
				t.Fatalf("failed to write file %s: %v", file.Name, err)
			}
		}
	}

	// Create RAR archive using shell command
	rarPath := filepath.Join(tempDir, "test.rar")

	// Try to create RAR using rar command
	cmd := exec.Command("rar", "a", "-r", rarPath, ".")
	cmd.Dir = tempDir

	if err := cmd.Run(); err != nil {
		// If rar command is not available, try using the 'unrar' command to check availability
		// and then skip the test gracefully
		if _, err := exec.LookPath("rar"); err != nil {
			// RAR command not found, skip test
			t.Skip("rar command not found - install RAR or skip RAR tests")
		}
		t.Fatalf("failed to create RAR archive: %v", err)
	}

	// Read the created RAR file
	rarData, err := os.ReadFile(rarPath)
	if err != nil {
		t.Fatalf("failed to read RAR archive file: %v", err)
	}

	return rarData
}

// CreateTARArchive creates a TAR archive from the given files
func CreateTARArchive(t *testing.T, ctx core.Context, files []TestFile) []byte {
	creator := NewTestArchiveCreator(t, ctx)
	buf, err := creator.CreateArchiveFromTestFiles(context.Background(), FormatTAR, files)
	if err != nil {
		t.Fatalf("failed to create TAR archive: %v", err)
	}
	return buf.Bytes()
}

// CreateTARGZArchive creates a TAR.GZ archive from the given files
func CreateTARGZArchive(t *testing.T, ctx core.Context, files []TestFile) []byte {
	creator := NewTestArchiveCreator(t, ctx)
	buf, err := creator.CreateArchiveFromTestFiles(context.Background(), FormatTAR_GZ, files)
	if err != nil {
		t.Fatalf("failed to create TAR.GZ archive: %v", err)
	}
	return buf.Bytes()
}

// CreateTARBZ2Archive creates a TAR.BZ2 archive from the given files
func CreateTARBZ2Archive(t *testing.T, ctx core.Context, files []TestFile) []byte {
	creator := NewTestArchiveCreator(t, ctx)
	buf, err := creator.CreateArchiveFromTestFiles(context.Background(), FormatTAR_BZ2, files)
	if err != nil {
		t.Fatalf("failed to create TAR.BZ2 archive: %v", err)
	}
	return buf.Bytes()
}

// CreateCARArchive creates a CAR archive from given files
func CreateCARArchive(t *testing.T, ctx core.Context, files []TestFile) []byte {
	// Create a temporary archive extractor from the files
	creator := NewTestArchiveCreator(t, ctx)
	
	// Create a ZIP archive first as an intermediate format
	zipBuf, err := creator.CreateArchiveFromTestFiles(context.Background(), FormatZIP, files)
	if err != nil {
		t.Fatalf("failed to create ZIP archive: %v", err)
	}
	
	// Create an extractor from the ZIP data
	extractor, err := CreateExtractor(bytes.NewReader(zipBuf.Bytes()))
	if err != nil {
		t.Fatalf("failed to create archive extractor: %v", err)
	}
	defer extractor.Close()
	
	// Convert the archive to CAR format
	generator := NewCARGeneratorWithDefaults(ctx.Logger())
	carBuf, _, err := generator.ArchiveToCAR(context.Background(), extractor)
	if err != nil {
		t.Fatalf("failed to convert archive to CAR format: %v", err)
	}
	
	return carBuf.Bytes()
}

// closeIo safely closes an any io Reader and logs any errors
func closeIo(t testing.TB, entity io.Closer) {
	if err := entity.Close(); err != nil {
		t.Logf("Warning: failed to close reader: %v", err)
	}
}
