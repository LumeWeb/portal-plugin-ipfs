package tests

import (
	"testing"

	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/upload"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/queryutil"
	"go.lumeweb.com/queryutil/filter"
	coreTesting "go.lumeweb.com/portal/core/testing"
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

// TestTUSUploadOperation_PinStatus verifies that pin status transitions from queued to pinned
func TestTUSUploadOperation_PinStatus(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		fileContent := "Test content for TUS pin status verification"

		// Run the TUS upload workflow using the internal function
		runTUSFileUploadInternal(t, ctx, fileContent)

		// Retrieve the pin service
		pinSvc := core.GetService[pluginCore.IPFSPinService](ctx, pluginCore.PIN_SERVICE)
		if pinSvc == nil {
			t.Skip("Pin service not available for status verification")
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
