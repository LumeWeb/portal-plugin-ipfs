package tests

import (
	"bytes"
	"testing"

	"github.com/ipfs/go-cid"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	pluginUpload "go.lumeweb.com/portal-plugin-ipfs/internal/upload"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/queryutil"
	"go.lumeweb.com/queryutil/filter"
	coreTesting "go.lumeweb.com/portal/core/testing"
	contentArchive "go.lumeweb.com/ipfs-content/archive"
)

func TestPostUploadOperationHandler_Execute_Integration(t *testing.T) {
	t.Run("CAR file upload", func(t *testing.T) {
		testArchiveUpload(t, contentArchive.FormatCAR, pluginUpload.CreateCARArchive, pluginUpload.ArchiveConvert, testPostUploadArchiveWorkflow)
	})

	archiveFormats := []struct {
		name    string
		format  contentArchive.Format
		creator pluginUpload.ArchiveCreator
	}{
		{name: "ZIP", format: contentArchive.FormatZIP, creator: pluginUpload.CreateZIPArchive},
		{name: "TAR", format: contentArchive.FormatTAR, creator: pluginUpload.CreateTARArchive},
		{name: "TAR.GZ", format: contentArchive.FormatTAR_GZ, creator: pluginUpload.CreateTARGZArchive},
		{name: "TAR.BZ2", format: contentArchive.FormatTAR_BZ2, creator: pluginUpload.CreateTARBZ2Archive},
	}

	for _, af := range archiveFormats {
		t.Run(af.name+" archive upload (convert mode)", func(t *testing.T) {
			t.Parallel()
			testArchiveUpload(t, af.format, af.creator, pluginUpload.ArchiveConvert, testPostUploadArchiveWorkflow)
		})
	}

	t.Run("7Z archive upload (convert mode)", func(t *testing.T) {
		t.Parallel()
		testArchiveUpload(t, contentArchive.Format7Z, pluginUpload.Create7ZArchive, pluginUpload.ArchiveConvert, testPostUploadArchiveWorkflow)
	})

	t.Run("RAR archive upload (convert mode)", func(t *testing.T) {
		t.Parallel()
		testArchiveUpload(t, contentArchive.FormatRAR, pluginUpload.CreateRARArchive, pluginUpload.ArchiveConvert, testPostUploadArchiveWorkflow)
	})

	for _, af := range archiveFormats {
		t.Run(af.name+" archive upload (preserve mode)", func(t *testing.T) {
			t.Parallel()
			testArchiveUpload(t, af.format, af.creator, pluginUpload.ArchivePreserve, testPostUploadArchiveWorkflow)
		})
	}

	t.Run("7Z archive upload (preserve mode)", func(t *testing.T) {
		t.Parallel()
		testArchiveUpload(t, contentArchive.Format7Z, pluginUpload.Create7ZArchive, pluginUpload.ArchivePreserve, testPostUploadArchiveWorkflow)
	})

	t.Run("RAR archive upload (preserve mode)", func(t *testing.T) {
		t.Parallel()
		testArchiveUpload(t, contentArchive.FormatRAR, pluginUpload.CreateRARArchive, pluginUpload.ArchivePreserve, testPostUploadArchiveWorkflow)
	})

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

func testPostUploadArchiveWorkflow(t *testing.T, ctx coreTesting.TestContext, universalReader *pluginUpload.UniversalReader, format contentArchive.Format, mode pluginUpload.ArchiveMode) {
	root, _ := testPostUploadWorkflow(t, ctx, universalReader, format, mode)
	assertRootBlockFetchable(t, ctx, root)
}

// testPostUploadWorkflow is a helper function that runs the complete upload workflow test for POST uploads
func testPostUploadWorkflow(t *testing.T, ctx coreTesting.TestContext, universalReader *pluginUpload.UniversalReader, format contentArchive.Format, mode pluginUpload.ArchiveMode, opts ...uploadWorkflowOption) (cid.Cid, *coreTesting.WorkflowTest) {
	root, wfTest := testUploadWorkflow(
		t,
		ctx,
		universalReader,
		format,
		mode,
		"ipfs.post.upload",
		assertWorkflowSuccess,
		func(uploadId string) interface{} {
			return core.WithWorkflowStructData(protocol.PostUploadWorkflowData{
				UploadID: uploadId,
			}, "json")
		},
		opts...,
	)
	return root, wfTest
}

// testFileUpload tests plain file uploads (FormatFile) through the POST upload workflow
func testFileUpload(t *testing.T, fileContent string, filename string) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		fileReader := bytes.NewReader([]byte(fileContent))
		universalReader := pluginUpload.NewUniversalReader(fileReader)

		root, _ := testPostUploadWorkflow(t, ctx, universalReader, contentArchive.FormatFile, pluginUpload.ArchiveConvert)
		assertRootBlockFetchable(t, ctx, root)
	}, GetStandardTestOptions()...)
}

// TestPostUploadOperation_PinStatus verifies that pin status transitions from queued to pinned
func TestPostUploadOperation_PinStatus(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		fileContent := "Test content for pin status verification"
		fileReader := bytes.NewReader([]byte(fileContent))
		universalReader := pluginUpload.NewUniversalReader(fileReader)

		testPostUploadWorkflow(t, ctx, universalReader, contentArchive.FormatFile, pluginUpload.ArchiveConvert)

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
