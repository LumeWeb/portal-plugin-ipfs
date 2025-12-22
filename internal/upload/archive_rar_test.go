package upload

import (
	"os/exec"
	"testing"
)

func TestRarArchiveExtractor(t *testing.T) {
	RegisterRarExtractor()
	// Check if rar command is available
	if _, err := exec.LookPath("rar"); err != nil {
		t.Skip("rar command not available, skipping RAR tests")
	}

	helper := NewArchiveTestHelper(t, FormatRAR)
	helper.TestBasicExtraction(CreateRARArchive)
}

func TestRarFormatDetection(t *testing.T) {
	RegisterRarExtractor()
	// Check if rar command is available
	if _, err := exec.LookPath("rar"); err != nil {
		t.Skip("rar command not available, skipping RAR tests")
	}

	helper := NewArchiveTestHelper(t, FormatRAR)
	helper.TestFormatDetection(CreateRARArchive)
}

func TestRarContentReader(t *testing.T) {
	RegisterRarExtractor()
	// Check if rar command is available
	if _, err := exec.LookPath("rar"); err != nil {
		t.Skip("rar command not available, skipping RAR tests")
	}

	helper := NewArchiveTestHelper(t, FormatRAR)
	helper.TestLargeFileContent(CreateRARArchive)
}
