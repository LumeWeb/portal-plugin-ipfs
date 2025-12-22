package tests

import (
	"testing"

	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/upload"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

func TestPostUploadOperationHandler_Execute_Integration(t *testing.T) {
	// Define test cases for CAR files
	t.Run("CAR file upload", func(t *testing.T) {
		testArchiveUpload(t, upload.FormatCAR, upload.CreateCARArchive, upload.ArchiveConvert, testPostUploadWorkflow)
	})

	// Define test cases for all archive formats with convert mode
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
			t.Parallel()
			testArchiveUpload(t, af.format, af.creator, upload.ArchiveConvert, testPostUploadWorkflow)
		})
	}

	// Test 7Z format separately since it requires external tools
	t.Run("7Z archive upload (convert mode)", func(t *testing.T) {
		t.Parallel()
		testArchiveUpload(t, upload.Format7Z, upload.Create7ZArchive, upload.ArchiveConvert, testPostUploadWorkflow)
	})

	// Test RAR format separately since it requires external tools
	t.Run("RAR archive upload (convert mode)", func(t *testing.T) {
		t.Parallel()
		testArchiveUpload(t, upload.FormatRAR, upload.CreateRARArchive, upload.ArchiveConvert, testPostUploadWorkflow)
	})

	// Test all archive formats with preserve mode
	for _, af := range archiveFormats {
		t.Run(af.name+" archive upload (preserve mode)", func(t *testing.T) {
			t.Parallel()
			testArchiveUpload(t, af.format, af.creator, upload.ArchivePreserve, testPostUploadWorkflow)
		})
	}

	// Test 7Z format separately since it requires external tools
	t.Run("7Z archive upload (preserve mode)", func(t *testing.T) {
		t.Parallel()
		testArchiveUpload(t, upload.Format7Z, upload.Create7ZArchive, upload.ArchivePreserve, testPostUploadWorkflow)
	})

	// Test RAR format separately since it requires external tools
	t.Run("RAR archive upload (preserve mode)", func(t *testing.T) {
		t.Parallel()
		testArchiveUpload(t, upload.FormatRAR, upload.CreateRARArchive, upload.ArchivePreserve, testPostUploadWorkflow)
	})

	
}

// testPostUploadWorkflow is a helper function that runs the complete upload workflow test for POST uploads
func testPostUploadWorkflow(t *testing.T, ctx coreTesting.TestContext, universalReader *upload.UniversalReader, format upload.Format, mode upload.ArchiveMode) {
	// Use the generic testUploadWorkflow function with POST-specific parameters
	testUploadWorkflow(
		t,
		ctx,
		universalReader,
		format,
		mode,
		"ipfs.post.upload",
		assertWorkflowSuccess,
		// POST uploads require PostUploadWorkflowData builder
		func(uploadId string) interface{} {
			return core.WithWorkflowStructData(protocol.PostUploadWorkflowData{
				UploadID: uploadId,
			}, "json")
		},
	)
}
