package upload

import (
	"testing"
)

func TestZipArchiveExtractor(t *testing.T) {
	RegisterZipExtractor()
	helper := NewArchiveTestHelper(t, FormatZIP)
	helper.TestBasicExtraction(CreateZIPArchive)
}

func TestZipPathValidation(t *testing.T) {
	RegisterZipExtractor()
	helper := NewArchiveTestHelper(t, FormatZIP)
	helper.TestPathValidation(CreateZIPArchive)
}

func TestZipFormatDetection(t *testing.T) {
	RegisterZipExtractor()
	helper := NewArchiveTestHelper(t, FormatZIP)
	helper.TestFormatDetection(CreateZIPArchive)
}
