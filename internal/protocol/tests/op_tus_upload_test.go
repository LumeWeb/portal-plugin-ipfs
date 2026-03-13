package tests

import (
	"testing"

	"go.lumeweb.com/portal-plugin-ipfs/internal/upload"
)

func TestTUSUploadOperationHandler_Execute_Integration(t *testing.T) {
	// Test CAR file upload using TUS-specific logic
	t.Run("CAR file upload", func(t *testing.T) {
		testTUSArchiveUpload(t, upload.FormatCAR, upload.CreateCARArchive, upload.ArchiveConvert, GetStandardTestOptions()...)
	})

	// Define test cases for all archive formats with convert mode only (TUS doesn't support preserve mode yet)
	archiveFormats := []struct {
		name    string
		format  upload.Format
		creator upload.ArchiveCreator
	}{
		{
			name:    "ZIP",
			format:  upload.FormatZIP,
			creator: upload.CreateZIPArchive,
		},
		{
			name:    "TAR",
			format:  upload.FormatTAR,
			creator: upload.CreateTARArchive,
		},
		{
			name:    "TAR.GZ",
			format:  upload.FormatTAR_GZ,
			creator: upload.CreateTARGZArchive,
		},
		{
			name:    "TAR.BZ2",
			format:  upload.FormatTAR_BZ2,
			creator: upload.CreateTARBZ2Archive,
		},
	}

	for _, af := range archiveFormats {
		t.Run(af.name+" archive upload (convert mode)", func(t *testing.T) {
			testTUSArchiveUpload(t, af.format, af.creator, upload.ArchiveConvert, GetStandardTestOptions()...)
		})
	}

	// Test 7Z format separately since it requires external tools
	t.Run("7Z archive upload (convert mode)", func(t *testing.T) {
		testTUSArchiveUpload(t, upload.Format7Z, upload.Create7ZArchive, upload.ArchiveConvert, GetStandardTestOptions()...)
	})

	// Test RAR format separately since it requires external tools
	t.Run("RAR archive upload (convert mode)", func(t *testing.T) {
		testTUSArchiveUpload(t, upload.FormatRAR, upload.CreateRARArchive, upload.ArchiveConvert, GetStandardTestOptions()...)
	})

	// Test plain file uploads via TUS
	t.Run("Plain text file upload", func(t *testing.T) {
		testTUSFileUpload(t, "Hello, World! This is a plain text file for TUS upload test.", "test.txt")
	})

	t.Run("JSON config file upload", func(t *testing.T) {
		content := `{
  "name": "test",
  "version": "1.0"
}`
		testTUSFileUpload(t, content, "config.json")
	})
}
