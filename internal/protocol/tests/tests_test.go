package tests

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"runtime"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	format "github.com/ipfs/go-ipld-format"
	"github.com/stretchr/testify/require"
	"github.com/tus/tusd/v2/pkg/handler"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/upload"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/portal/db/models"
	"gorm.io/gorm"
)

var (
	_, currentFile, _, _ = runtime.Caller(0)
	carFileName          = "../../testing/fixtures/cars/bbb.car"
)

// createCARArchive creates a CAR archive from test files by reading the fixture CAR file
func createCARArchive(testFiles []upload.TestFile) []byte {
	carFile, err := os.OpenFile(path.Join(path.Dir(currentFile), carFileName), os.O_RDONLY, fs.ModePerm)
	if err != nil {
		panic("Failed to open CAR fixture file: " + err.Error())
	}
	defer carFile.Close()

	carData, err := os.ReadFile(path.Join(path.Dir(currentFile), carFileName))
	if err != nil {
		panic("Failed to read CAR fixture file: " + err.Error())
	}

	return carData
}

func createTestRequest(cid cid.Cid, userID *uint) *models.Request {
	req := &models.Request{
		Model:  gorm.Model{ID: 1},
		Status: models.RequestStatusProcessing,
	}

	req.Hash = cid.Hash()

	if userID != nil {
		req.UserID = userID
	}

	return req
}

func uintPtr(i uint) *uint {
	return &i
}

// createTestArchiveBlockstore creates a test DefaultStreamingBlockstore with provided DoneTracker
func createTestArchiveBlockstore(t *testing.T, doneTracker protocol.DoneTracker, hasPassthrough bool) (*protocol.DefaultStreamingBlockstore, ds.Datastore, coreTesting.TestContext) {
	ctx, err := coreTesting.NewTestContext(t)
	require.NoError(t, err)

	var passthroughDatastore ds.Datastore
	if hasPassthrough {
		passthroughDatastore = ds.NewLogDatastore(ds.NewMapDatastore(), "ArchiveBlockstore_Logstore")
	}

	// Create passthrough blockstore if we have passthrough datastore
	var passthroughBlockstore blockstore.Blockstore
	if passthroughDatastore != nil {
		passthroughBlockstore = blockstore.NewBlockstore(dssync.MutexWrap(passthroughDatastore))
	}

	archiveBlockstore := protocol.NewStreamingBlockstoreWithDefaults(ctx.Logger(), passthroughBlockstore, doneTracker, 5) // Small queue for backpressure testing

	return archiveBlockstore, passthroughDatastore, ctx
}

// startProcessingAndCountBlocks starts processing in a background goroutine and returns a channel for the block count
func startProcessingAndCountBlocks(t *testing.T, processor protocol.BlockProcessor, dagService format.DAGService) <-chan int {
	blockCountChan := make(chan int, 1)
	go func() {
		blockCountChan <- countBlocks(t, processor, dagService)
	}()
	return blockCountChan
}

// mustMarshal is a helper function that marshals data to JSON and fails the test if there's an error
func mustMarshal(tb coreTesting.TB, v interface{}) []byte {
	data, err := json.Marshal(v)
	require.NoError(tb, err)
	return data
}

// setupTestUser creates a test user with a unique email based on format and mode
func setupTestUser(t testing.TB, ctx coreTesting.TestContext, format upload.Format, mode upload.ArchiveMode) *models.User {
	userSvc := core.GetService[core.UserService](ctx, core.USER_SERVICE)
	email := strings.ToLower(format.String()) + "_" + mode.String() + "@test.com"
	testUser, err := userSvc.CreateAccount(email, "testpassword123", false)
	require.NoError(t, err)
	return testUser
}

// setupTestServices initializes and returns the workflow test and upload service
func setupTestServices(ctx coreTesting.TestContext) (*coreTesting.WorkflowTest, pluginCore.UploadService) {
	wfTest := coreTesting.NewWorkflowTest(ctx)
	uploadService := core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
	return wfTest, uploadService
}

// handleUploadWithMode handles the upload based on the specified mode
func handleUploadWithMode(uploadService pluginCore.UploadService, ctx coreTesting.TestContext, reader *upload.UniversalReader, userID uint, mode upload.ArchiveMode) (cid.Cid, string, error) {
	if mode == upload.ArchiveConvert {
		return uploadService.HandleUpload(ctx, reader, userID)
	}
	return uploadService.HandleUploadWithMode(ctx, reader, userID, mode)
}

// assertWorkflowSuccess performs common workflow assertions
func assertWorkflowSuccess(wfTest *coreTesting.WorkflowTest, req *models.Request) {
	wfTest.AssertOperationSuccess(req)
	wfTest.AssertOperationStatusMessageContains(req, "Upload processed successfully")
	wfTest.AssertOperationStatusProgress(req, 100)
}

// assertTUSWorkflowSuccess performs TUS-specific workflow assertions with expected message
func assertTUSWorkflowSuccess(wfTest *coreTesting.WorkflowTest, req *models.Request) {
	wfTest.AssertOperationSuccess(req)
	wfTest.AssertOperationStatusMessageContains(req, "Successfully completed")
	wfTest.AssertOperationStatusProgress(req, 100)
}

// testArchiveUpload is a helper function that tests archive uploads for a given format and mode
// The specific workflow function should be provided as a parameter, along with optional test options
func testArchiveUpload(t *testing.T, format upload.Format, creator func(files []upload.TestFile) []byte, mode upload.ArchiveMode, workflowFunc func(*testing.T, coreTesting.TestContext, *upload.UniversalReader, upload.Format, upload.ArchiveMode), testOptions ...coreTesting.TestContextBuilderOption) {
	// Skip 7Z and RAR tests if the required tools are not available
	handleToolUnavailable(t, format)

	// Use provided options if available, otherwise use defaults
	var finalOptions []coreTesting.TestContextBuilderOption
	if len(testOptions) > 0 {
		finalOptions = testOptions
	} else {
		finalOptions = []coreTesting.TestContextBuilderOption{
			coreTesting.CombineOptions(GetCommonTestOptions(), GetDbTestOptions()),
		}
	}

	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange - Create test archive
		testFiles := upload.GetDefaultTestFiles()
		archiveData := creator(testFiles)

		// Create a reader from the archive data using UniversalReader
		archiveReader := bytes.NewReader(archiveData)
		universalReader := upload.NewUniversalReader(archiveReader)

		// Run the upload workflow test using the provided workflow function
		workflowFunc(t, ctx, universalReader, format, mode)
	}, finalOptions...)
}

// handleToolUnavailable checks if required external tools are available for 7Z and RAR formats
func handleToolUnavailable(t *testing.T, format upload.Format) {
	if format == upload.Format7Z || format == upload.FormatRAR {
		// These formats require external tools that might not be available in CI
		// The test will be skipped if the tools are not found during archive creation
		defer func() {
			if r := recover(); r != nil {
				if msg, ok := r.(string); ok && (strings.Contains(msg, "command not found") || strings.Contains(msg, "not found - install")) {
					t.Skipf("Skipping %s test: %s", format.String(), msg)
				} else {
					t.Errorf("Unexpected error during %s archive creation: %v", format.String(), r)
				}
			}
		}()
	}
}

// setupTUSUpload creates a TUS upload with optional hash and returns protocol and request ID
// hash can be nil for files where hash is not yet known (e.g., non-CAR files)
func setupTUSUpload(t *testing.T, ctx coreTesting.TestContext, uploadFile *os.File, hash core.StorageHash) (core.StorageProtocol, uint) {
	tusService := core.GetService[core.TUSService](ctx, core.TUS_SERVICE)
	storageSvc := core.GetService[core.StorageService](ctx, core.STORAGE_SERVICE)
	proto := core.GetProtocol(internal.ProtocolName)

	// TUS Upload Setup
	objectId := uuid.New().String()
	uploadId := uuid.New().String()
	fullId := fmt.Sprintf("%s+%s", objectId, uploadId)

	// Create TUS upload - this is TUS-specific logic
	testUser, err := core.GetService[core.UserService](ctx, core.USER_SERVICE).CreateAccount("test@example.com", "testpassword123", false)
	require.NoError(t, err)

	tusUpload, err := tusService.CreateUpload(
		ctx,
		hash,
		fullId,
		testUser.ID,
		"127.0.0.1",
		proto.(core.StorageProtocol),
	)
	require.NoError(t, err)

	err = tusService.UploadProcessing(ctx, proto.(core.StorageProtocol), tusUpload.TUSUploadID)
	require.NoError(t, err)

	// Get file stats for S3 upload
	fileSize, err := uploadFile.Stat()
	require.NoError(t, err)

	// S3 Upload - TUS-specific logic
	fileInfo := handler.FileInfo{ID: objectId, Size: fileSize.Size()}
	infoData := io.NopCloser(bytes.NewReader(mustMarshal(t, fileInfo)))
	err = storageSvc.S3MultipartUpload(
		ctx,
		infoData,
		ctx.Config().Config().Core.Storage.S3.BufferBucket,
		storageSvc.GetTemporaryUploadPath(proto.(core.StorageProtocol), fmt.Sprintf("%s.info", objectId)),
		uint64(len(mustMarshal(t, fileInfo))),
	)
	require.NoError(t, err)

	// Upload file to S3
	err = storageSvc.S3MultipartUpload(
		ctx,
		uploadFile,
		ctx.Config().Config().Core.Storage.S3.BufferBucket,
		storageSvc.GetTemporaryUploadPath(proto.(core.StorageProtocol), objectId),
		uint64(fileSize.Size()),
	)
	require.NoError(t, err)

	return proto.(core.StorageProtocol), tusUpload.RequestID
}

// testTUSArchiveUpload is a TUS-specific wrapper for testArchiveUpload
// It handles TUS-specific upload logic while using the generic archive upload pattern
func testTUSArchiveUpload(t *testing.T, format upload.Format, creator func(files []upload.TestFile) []byte, mode upload.ArchiveMode, testOptions ...coreTesting.TestContextBuilderOption) {
	// Since TUS doesn't support archive preserve mode yet, only test convert mode
	if mode != upload.ArchiveConvert {
		t.Skip("TUS doesn't support archive preserve mode yet")
	}

	// Create a TUS-specific workflow function that handles the TUS upload logic
	tusWorkflowFunc := func(t *testing.T, ctx coreTesting.TestContext, universalReader *upload.UniversalReader, format upload.Format, mode upload.ArchiveMode) {
		// For TUS, we need to convert the UniversalReader back to a file for TUS processing
		// Read the data from UniversalReader
		archiveData, err := io.ReadAll(universalReader)
		require.NoError(t, err)

		// Create a temporary file from the archive data for TUS processing
		tempFile, err := os.CreateTemp("", "tus-test-*.tmp")
		require.NoError(t, err)
		defer func() {
			tempFile.Close()
			os.Remove(tempFile.Name())
		}()

		_, err = tempFile.Write(archiveData)
		require.NoError(t, err)

		// Seek back to beginning for TUS processing
		_, err = tempFile.Seek(0, 0)
		require.NoError(t, err)

		// Use appropriate setup based on format
		var requestID uint
		if format == upload.FormatCAR {
			// For CAR files, we can pre-compute the hash
			roots, err := upload.GetCarRoots(tempFile, false)
			require.NoError(t, err)
			_, requestID = setupTUSUpload(t, ctx, tempFile, internal.NewIPFSHash(roots[0]))
		} else {
			// For non-CAR files, hash is not known yet
			_, requestID = setupTUSUpload(t, ctx, tempFile, nil)
		}

		// Workflow Execution
		wfTest := coreTesting.NewWorkflowTest(ctx)
		wf := wfTest.NewOperationWorkflow(core.TUSUploadOperationName(internal.ProtocolName))

		// Build workflow options - for CAR files we have the hash, for others we don't
		var workflowOptions []core.WorkflowOption
		if format == upload.FormatCAR {
			// For CAR files, we can pre-compute the hash
			workflowOptions = append(workflowOptions, core.WithWorkflowStorageHash(internal.NewIPFSHash(getCARRootsFromFile(t, tempFile))))
		}
		workflowOptions = append(workflowOptions,
			core.WithWorkflowUserID(1), // User created in setupTUSUpload
			core.WithWorkflowSourceIP("127.0.0.1"),
		)

		req := wfTest.GetRequest(requestID)
		wfTest.MustConvertRequestToWorkflow(
			requestID,
			wf,
			0,
			workflowOptions...,
		)
		wfTest.ExecuteWorkflowStep(req)
		wfTest.CompleteWorkflowStep(req)

		// Assertions
		assertTUSWorkflowSuccess(wfTest, req)
	}

	// Use the generic testArchiveUpload with TUS-specific workflow function
	testArchiveUpload(t, format, creator, mode, tusWorkflowFunc, testOptions...)
}

// testUploadWorkflow is a generic helper function that runs the complete upload workflow test
// It supports both POST and TUS uploads based on the operationName and workflowDataBuilder parameters
func testUploadWorkflow(t *testing.T, ctx coreTesting.TestContext, universalReader *upload.UniversalReader, format upload.Format, mode upload.ArchiveMode, operationName string, assertionFunc func(*coreTesting.WorkflowTest, *models.Request), workflowDataBuilder func(string) interface{}) {
	// Arrange - Setup test user and services
	testUser := setupTestUser(t, ctx, format, mode)
	wfTest, uploadService := setupTestServices(ctx)

	// Handle upload with specified mode
	root, uploadId, err := handleUploadWithMode(uploadService, ctx, universalReader, testUser.ID, mode)
	require.NoError(t, err)

	// Start the workflow with the upload hash using the specified operation name
	// Build workflow options with the provided builder function if available
	var workflowOptions []core.WorkflowOption
	workflowOptions = append(workflowOptions,
		core.WithWorkflowStorageHash(internal.NewIPFSHash(root)),
		core.WithWorkflowUserID(testUser.ID),
		core.WithWorkflowSourceIP("127.0.0.1"),
	)

	if workflowDataBuilder != nil {
		workflowData := workflowDataBuilder(uploadId)
		if workflowData != nil {
			workflowOptions = append(workflowOptions, workflowData.(core.WorkflowOption))
		}
	}

	req := wfTest.StartOperationWorkflow(operationName, workflowOptions...)

	// Act
	wfTest.ExecuteWorkflowStep(req)
	wfTest.CompleteWorkflowStep(req)

	// Assert using the provided assertion function
	assertionFunc(wfTest, req)
}

// getCARRootsFromFile gets CAR roots from a file (helper for TUS)
func getCARRootsFromFile(t *testing.T, file *os.File) cid.Cid {
	roots, err := upload.GetCarRoots(file, false)
	require.NoError(t, err)
	if len(roots) == 0 {
		t.Fatal("No CAR roots found")
	}
	return roots[0]
}
