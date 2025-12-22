package upload

import (
	"testing"
)

func TestTarArchiveExtractor(t *testing.T) {
	RegisterTarExtractor()
	helper := NewArchiveTestHelper(t, FormatTAR)
	helper.TestBasicExtraction(CreateTARArchive)
}

func TestTarPathValidation(t *testing.T) {
	RegisterTarExtractor()
	helper := NewArchiveTestHelper(t, FormatTAR)
	helper.TestPathValidation(CreateTARArchive)
}

func TestTarFormatDetection(t *testing.T) {
	RegisterTarExtractor()
	helper := NewArchiveTestHelper(t, FormatTAR)
	helper.TestFormatDetection(CreateTARArchive)
}

func TestTarContentReader(t *testing.T) {
	RegisterTarExtractor()
	helper := NewArchiveTestHelper(t, FormatTAR)
	helper.TestLargeFileContent(CreateTARArchive)
}
