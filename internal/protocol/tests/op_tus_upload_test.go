package tests

import (
	"testing"

	"go.lumeweb.com/portal-plugin-ipfs/internal/upload"
)

func TestTUSUploadOperationHandler_Execute_Integration(t *testing.T) {
	// Test CAR file upload using TUS-specific logic
	t.Run("CAR file upload", func(t *testing.T) {
		testTUSArchiveUpload(t, upload.FormatCAR, createCARArchive, upload.ArchiveConvert, GetTUSUploadTestOptions()...)
	})

	// Define test cases for all archive formats with convert mode only (TUS doesn't support preserve mode yet)
	archiveFormats := []struct {
		name    string
		format  upload.Format
		creator func(files []upload.TestFile) []byte
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
			testTUSArchiveUpload(t, af.format, af.creator, upload.ArchiveConvert, GetTUSUploadTestOptions()...)
		})
	}

	// Test 7Z format separately since it requires external tools
	t.Run("7Z archive upload (convert mode)", func(t *testing.T) {
		testTUSArchiveUpload(t, upload.Format7Z, upload.Create7ZArchive, upload.ArchiveConvert, GetTUSUploadTestOptions()...)
	})

	// Test RAR format separately since it requires external tools
	t.Run("RAR archive upload (convert mode)", func(t *testing.T) {
		testTUSArchiveUpload(t, upload.FormatRAR, upload.CreateRARArchive, upload.ArchiveConvert, GetTUSUploadTestOptions()...)
	})
}
