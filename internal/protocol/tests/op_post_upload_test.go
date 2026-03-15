package tests

import (
	"bytes"
	"testing"

	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/upload"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/queryutil"
	"go.lumeweb.com/queryutil/filter"
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

	// Test plain file uploads (FormatFile)
	t.Run("Plain text file upload", func(t *testing.T) {
		t.Parallel()
		testFileUpload(t, "Hello, World! This is a plain text file upload test.", "test.txt")
	})

	t.Run("JSON config file upload", func(t *testing.T) {
		t.Parallel()
		content := `{
  "name": "test",
  "version": "1.0"
}`
		testFileUpload(t, content, "config.json")
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

// testFileUpload tests plain file uploads (FormatFile) through the POST upload workflow
func testFileUpload(t *testing.T, fileContent string, filename string) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Create a reader from the plain file content
		fileReader := bytes.NewReader([]byte(fileContent))
		universalReader := upload.NewUniversalReader(fileReader)

		// Run the upload workflow test
		testPostUploadWorkflow(t, ctx, universalReader, upload.FormatFile, upload.ArchiveConvert)
	}, GetStandardTestOptions()...)
}

// TestPostUploadOperation_PinStatus verifies that pin status transitions from queued to pinned
func TestPostUploadOperation_PinStatus(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		fileContent := "Test content for pin status verification"
		fileReader := bytes.NewReader([]byte(fileContent))
		universalReader := upload.NewUniversalReader(fileReader)

		// Run the upload workflow
		testPostUploadWorkflow(t, ctx, universalReader, upload.FormatFile, upload.ArchiveConvert)

		// Retrieve the pin service
		pinSvc := core.GetService[pluginCore.IPFSPinService](ctx, pluginCore.PIN_SERVICE)
		if pinSvc == nil {
			t.Fatal("Pin service not available")
		}

		// Get the most recent pin for this user
		sort := []filter.Sort{
			{Field: "created_at", Order: filter.OrderDesc},
		}
		pins, total, err := pinSvc.ListPins(ctx, nil, sort, queryutil.DefaultPagination)
		if err != nil {
			t.Fatalf("Failed to list pins: %v", err)
		}

		if total == 0 || len(pins) == 0 {
			t.Fatal("No pins found")
		}

		// Get the most recent pin (pins should be ordered by created_at desc)
		pin := pins[0]

		// Verify the pin status is "pinned"
		if pin.Status != db.PinningStatusPinned {
			t.Errorf("Expected pin status to be 'pinned', got '%s'", pin.Status)
		}
	}, GetStandardTestOptions()...)
}
