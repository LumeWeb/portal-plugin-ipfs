package upload

import (
	"testing"
)

func TestTarGzArchiveExtractor(t *testing.T) {
	RegisterTarGzExtractor()
	helper := NewArchiveTestHelper(t, FormatTAR_GZ)
	helper.TestBasicExtraction(CreateTARGZArchive)
}

func TestTarBz2ArchiveExtractor(t *testing.T) {
	RegisterTarBz2Extractor()
	helper := NewArchiveTestHelper(t, FormatTAR_BZ2)
	helper.TestBasicExtraction(CreateTARBZ2Archive)
}

func TestCompressedTarPathValidation(t *testing.T) {
	// Test TAR.GZ path validation
	RegisterTarGzExtractor()
	helper := NewArchiveTestHelper(t, FormatTAR_GZ)
	helper.TestPathValidation(CreateTARGZArchive)

	// Test TAR.BZ2 path validation
	RegisterTarBz2Extractor()
	helperBz2 := NewArchiveTestHelper(t, FormatTAR_BZ2)
	helperBz2.TestPathValidation(CreateTARBZ2Archive)
}

func TestCompressedTarLargeFile(t *testing.T) {
	// Test TAR.GZ large file
	RegisterTarGzExtractor()
	helper := NewArchiveTestHelper(t, FormatTAR_GZ)
	helper.TestLargeFileContent(CreateTARGZArchive)

	// Test TAR.BZ2 large file
	RegisterTarBz2Extractor()
	helperBz2 := NewArchiveTestHelper(t, FormatTAR_BZ2)
	helperBz2.TestLargeFileContent(CreateTARBZ2Archive)
}

func TestCompressedTarFormatDetection(t *testing.T) {
	// Test TAR.GZ format detection
	RegisterTarGzExtractor()
	helper := NewArchiveTestHelper(t, FormatTAR_GZ)
	helper.TestFormatDetection(CreateTARGZArchive)

	// Test TAR.BZ2 format detection
	RegisterTarBz2Extractor()
	helperBz2 := NewArchiveTestHelper(t, FormatTAR_BZ2)
	helperBz2.TestFormatDetection(CreateTARBZ2Archive)
}
