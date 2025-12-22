package upload

import (
	"os/exec"
	"testing"
)

func Test7ZipArchiveExtractor(t *testing.T) {
	Register7ZipExtractor()
	// Check if 7z command is available
	if _, err := exec.LookPath("7z"); err != nil {
		if _, err := exec.LookPath("7zz"); err != nil {
			t.Skip("7z/7zz command not available, skipping 7Z tests")
		}
	}

	helper := NewArchiveTestHelper(t, Format7Z)
	helper.TestBasicExtraction(Create7ZArchive)
}

func Test7ZipFormatDetection(t *testing.T) {
	Register7ZipExtractor()
	// Check if 7z command is available
	if _, err := exec.LookPath("7z"); err != nil {
		if _, err := exec.LookPath("7zz"); err != nil {
			t.Skip("7z/7zz command not available, skipping 7Z tests")
		}
	}

	helper := NewArchiveTestHelper(t, Format7Z)
	helper.TestFormatDetection(Create7ZArchive)
}

func Test7ZipContentReader(t *testing.T) {
	Register7ZipExtractor()
	// Check if 7z command is available
	if _, err := exec.LookPath("7z"); err != nil {
		if _, err := exec.LookPath("7zz"); err != nil {
			t.Skip("7z/7zz command not available, skipping 7Z tests")
		}
	}

	helper := NewArchiveTestHelper(t, Format7Z)
	helper.TestLargeFileContent(Create7ZArchive)
}
