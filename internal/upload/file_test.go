package upload

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// FileUploadTestCase represents a single file upload test case
type FileUploadTestCase struct {
	name    string
	content string
	typeTag string
}

// GetDefaultFileUploadCases returns common file upload scenarios
func GetDefaultFileUploadCases() []FileUploadTestCase {
	return []FileUploadTestCase{
		{
			name:    "plain_text.txt",
			content: "# Test File\n\nThis is a simple plain text file for testing\nwith multiple lines of content.",
			typeTag: "text",
		},
		{
			name: "config.json",
			content: `{
  "name": "test-file",
  "version": "1.0.0",
  "settings": {
    "debug": true,
    "timeout": 30
  }
}`,
			typeTag: "json",
		},
		{
			name:    "empty_file.txt",
			content: "",
			typeTag: "empty",
		},
		{
			name:    "single_line.txt",
			content: "Single line",
			typeTag: "text",
		},
		{
			name:    "readme.md",
			content: "# README\n\nTest documentation file",
			typeTag: "text",
		},
		{
			name:    "data.xml",
			content: `<?xml version="1.0"?><root><item>test</item></root>`,
			typeTag: "text",
		},
		{
			name:    "log.txt",
			content: "2026-03-13 10:00:00 INFO Starting test\n2026-03-13 10:00:01 INFO Test complete",
			typeTag: "text",
		},
	}
}

// TestPlainFileFormatDetection tests that plain files are detected correctly as FormatFile
func TestPlainFileFormatDetection(t *testing.T) {
	testCases := GetDefaultFileUploadCases()

	for _, tc := range testCases {
		t.Run(tc.typeTag+"/"+tc.name, func(t *testing.T) {
			// Empty files cannot be detected
			if tc.content == "" {
				t.Skip("Empty files cannot undergo format detection")
			}

			// Create reader from file content
			reader := bytes.NewReader([]byte(tc.content))
			newReader := NewUniversalReader(reader)

			// Seek back to start after reading
			_, err := reader.Seek(0, io.SeekStart)
			require.NoError(t, err)

			// Detect format
			detectedFormat, err := DetectFormat(newReader)
			require.NoError(t, err, "Format detection should succeed")
			require.Equal(t, FormatFile, detectedFormat,
				"Should detect FormatFile for plain files (%s)", tc.name)
		})
	}
}

// TestPlainFileWithArchivePrefix ensures plain files are NOT detected as archives when incomplete
func TestPlainFileWithArchivePrefix(t *testing.T) {
	testCases := []struct {
		name            string
		content         string
		expectedFormat  Format
	}{
		{
			name:           "text_looks_like_zip_but_isnt",
			content:        "PK\x03\x04This is just text",
			expectedFormat: FormatZIP, // ZIP magic bytes found
		},
		{
			name:           "tar_like_text_content",
			content:        "ustarThis is a plain file that starts with tar header text",
			expectedFormat: FormatFile, // Not a valid tar file
		},
		{
			name:           "binary_prefix",
			content:        "\x00\x01\x02\x03\x04\x05Regular text content after binary prefix",
			expectedFormat: FormatFile, // No magic bytes detected
		},
		{
			name:           "xml_looks_like_archive",
			content:        `<archive><file>test</file></archive>`,
			expectedFormat: FormatFile, // XML is text, not an archive
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reader := bytes.NewReader([]byte(tc.content))
			newReader := NewUniversalReader(reader)

			// Detect format
			detectedFormat, err := DetectFormat(newReader)
			require.NoError(t, err, "Format detection should succeed")
			require.Equal(t, tc.expectedFormat, detectedFormat,
				"Format should match expected for plain files with archive-like content (%s, expected %s, got %s)",
				tc.name, tc.expectedFormat, detectedFormat)
		})
	}
}

// TestEmptyFileFormatDetection tests that empty files cannot be detected
func TestEmptyFileFormatDetection(t *testing.T) {
	// Empty file - format detection should fail
	reader := bytes.NewReader([]byte(""))
	newReader := NewUniversalReader(reader)

	_, err := DetectFormat(newReader)
	require.Error(t, err, "Empty files should fail format detection")
	require.Contains(t, err.Error(), "no data available",
		"Error message should indicate no data is available")
}

// TestSmallFilesFormatDetection tests various small file sizes
func TestSmallFilesFormatDetection(t *testing.T) {
	sizes := []int{1, 2, 3, 4, 5, 10, 20, 50, 100}

	for _, size := range sizes {
		t.Run(string(rune(size))+"_bytes", func(t *testing.T) {
			content := strings.Repeat("x", size)
			reader := bytes.NewReader([]byte(content))
			newReader := NewUniversalReader(reader)

			detectedFormat, err := DetectFormat(newReader)
			require.NoError(t, err)
			require.Equal(t, FormatFile, detectedFormat,
				"Small files (%d bytes) should be detected as FormatFile", size)
		})
	}
}

// TestLargeFileFormatDetection tests large file format detection
func TestLargeFileFormatDetection(t *testing.T) {
	// Create a large file (larger than minimum detection bytes)
	content := strings.Repeat("This is a test line\n", 10000)
	reader := bytes.NewReader([]byte(content))
	newReader := NewUniversalReader(reader)

	detectedFormat, err := DetectFormat(newReader)
	require.NoError(t, err)
	require.Equal(t, FormatFile, detectedFormat,
		"Large files should be detected as FormatFile")
}

// TestUnicodeContentFormatDetection tests unicode content is detected as FormatFile
func TestUnicodeContentFormatDetection(t *testing.T) {
	unicodeContent := "Hello 世界 🌍 Привет مرحبا"
	reader := bytes.NewReader([]byte(unicodeContent))
	newReader := NewUniversalReader(reader)

	detectedFormat, err := DetectFormat(newReader)
	require.NoError(t, err)
	require.Equal(t, FormatFile, detectedFormat,
		"Unicode content should be detected as FormatFile")
}

// TestSpecialCharactersFormatDetection tests files with special characters
func TestSpecialCharactersFormatDetection(t *testing.T) {
	specialContent := "!@#$%^&*()_+-=[]{}|;':\",./<>?`~"
	reader := bytes.NewReader([]byte(specialContent))
	newReader := NewUniversalReader(reader)

	detectedFormat, err := DetectFormat(newReader)
	require.NoError(t, err)
	require.Equal(t, FormatFile, detectedFormat,
		"Files with special characters should be detected as FormatFile")
}
