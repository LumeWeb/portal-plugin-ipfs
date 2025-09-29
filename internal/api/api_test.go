package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/samber/lo"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	"go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/block"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/file_manager"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/pin"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/upload"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	"go.lumeweb.com/portal/db/models"
	"go.lumeweb.com/portal/service"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

var TestOptions = coreTesting.CombineOptions(
	coreTesting.WithStatefulMockRenterService(),
	coreTesting.WithServiceFactory(core.CRON_SERVICE, service.NewCronService),
	coreTesting.WithServiceFactory(core.UPLOAD_SERVICE, service.NewMetadataService),
	coreTesting.WithServiceFactory(core.PIN_SERVICE, service.NewPinService),
	coreTesting.WithServiceFactory(core.REQUEST_SERVICE, service.NewRequestService),
	coreTesting.WithServiceFactory(core.WORKFLOW_SERVICE, service.NewWorkflowCoordinator),
	coreTesting.WithServiceFactory(core.USER_SERVICE, service.NewUserService),
	coreTesting.WithServiceFactory(core.AUTH_SERVICE, service.NewAuthService),
	coreTesting.WithServiceFactory(core.STORAGE_SERVICE, service.NewStorageService),
	coreTesting.WithServiceFactory(pluginCore.FILE_MANAGER_SERVICE, filemanager.NewFileManagerService),
	coreTesting.WithServiceFactory(pluginCore.PIN_SERVICE, pin.NewPinService),
	coreTesting.WithServiceFactory(pluginCore.BLOCK_SERVICE, block.NewBlockService),
	coreTesting.WithServiceFactory(pluginCore.UPLOAD_SERVICE, upload.NewUploadService),
	coreTesting.WithAPI(internal.ProtocolName, NewAPI),
	coreTesting.WithAPIID(internal.ProtocolName),
	coreTesting.WithProtocol(internal.ProtocolName, protocol.NewProtocol),
	coreTesting.WithProtocolConfig(internal.ProtocolName, config.ProtocolConfig{}),
	coreTesting.WithSQLitePluginMigrations(
		internal.ProtocolName, migrations.GetSQLite(),
	),
	coreTesting.WithCron(),
)

func createTestUserAndLogin(ctx coreTesting.TestContext) (string, uint) {
	userSvc := core.GetService[core.UserService](ctx, core.USER_SERVICE)
	authSvc := core.GetService[core.AuthService](ctx, core.AUTH_SERVICE)

	user, err := userSvc.CreateAccount("test@example.com", "example", false)
	if err != nil {
		ctx.T().Fatalf("failed to create test user: %v", err)
	}

	token, _, err := authSvc.LoginPassword("test@example.com", "example", "127.0.0.1", false)
	if err != nil {
		ctx.T().Fatalf("failed to login test user: %v", err)
	}

	return token, user.ID
}

func setAuthHeader(req *http.Request, token string) {
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
}

func createTestFilePath(t *testing.T, ctx coreTesting.TestContext, userID uint, testCID cid.Cid, path, name string, isDirectory bool) *pluginDb.FilePath {
	// Calculate parent path
	parentPath := pluginDb.RootPath
	if path != pluginDb.RootPath && path != "" {
		// Find the last slash to get the parent directory
		lastSlash := strings.LastIndex(path, "/")
		if lastSlash > 0 {
			parentPath = path[:lastSlash]
		} else if lastSlash == 0 {
			parentPath = pluginDb.RootPath
		}
	}

	filePath := &pluginDb.FilePath{
		UserID:      userID,
		CID:         testCID.Bytes(),
		Path:        path,
		Name:        name,
		Type:        0,
		Size:        1024,
		IsDirectory: isDirectory,
		IsOrphan:    false,
		ParentPath:  parentPath,
		Depth:       0,
	}

	err := ctx.DB().Create(filePath).Error
	require.NoError(t, err)
	return filePath
}

type pinTestHelper struct {
	token string
	pinID string
	cid   cid.Cid
}

func setupPinTest(t *testing.T, ctx coreTesting.TestContext) *pinTestHelper {
	token, _ := createTestUserAndLogin(ctx)

	// Create Pin
	reqBody := `{"cid":"bafybeieffnocaq7t4w4daagvydl32igft5oziyyaebqr6vx6rb3fwh2ab4","name":"test"}`
	req := ctx.NewAPIRequest(http.MethodPost, "/pins", []byte(reqBody))
	setAuthHeader(req, token)
	rec := httptest.NewRecorder()
	ctx.Router().ServeHTTP(rec, req)

	var pinResp dto.PinStatusResponse
	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	err = json.Unmarshal(body, &pinResp)
	if err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	// Wait for workflow completion
	wfTest := coreTesting.NewWorkflowTest(ctx)
	_cid := cid.MustParse("bafybeieffnocaq7t4w4daagvydl32igft5oziyyaebqr6vx6rb3fwh2ab4")
	wfTest.WaitForWorkflowInstance(protocol.PIN_WORKFLOW, core.RequestFilter{Hash: _cid.Hash(), Status: lo.ToPtr(models.RequestStatusCompleted)}, 1*time.Hour)

	return &pinTestHelper{
		token: token,
		pinID: pinResp.RequestID,
		cid:   _cid,
	}
}

func TestAPI_listPins(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := setupPinTest(t, ctx)

		// Make HTTP request
		req := ctx.NewAPIRequest(http.MethodGet, "/api/pins", nil)
		setAuthHeader(req, helper.token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusOK, rec.Code)
		var response dto.PinResultsResponse
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.NotEmpty(t, response.Results)
	}, TestOptions)
}

func TestAPI_addPin(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		token, _ := createTestUserAndLogin(ctx)

		// Make HTTP request
		reqBody := `{"cid":"bafybeieffnocaq7t4w4daagvydl32igft5oziyyaebqr6vx6rb3fwh2ab4","name":"test"}`
		req := ctx.NewAPIRequest(http.MethodPost, "/api/pins", []byte(reqBody))
		setAuthHeader(req, token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusAccepted, rec.Code)
		var response dto.PinStatusResponse
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.NotPanics(t, func() {
			uuid.MustParse(response.RequestID)
		})
	}, TestOptions)
}

func TestAPI_getPin(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := setupPinTest(t, ctx)

		// Make HTTP request
		req := ctx.NewAPIRequest(http.MethodGet, fmt.Sprintf("/api/pins/%s", helper.pinID), nil)
		setAuthHeader(req, helper.token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusOK, rec.Code)
		var response dto.PinStatusResponse
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.NotPanics(t, func() {
			uuid.MustParse(response.RequestID)
		})
	}, TestOptions)
}

func TestAPI_replacePin(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := setupPinTest(t, ctx)

		// Make HTTP request
		reqBody := `{"cid":"bafybeieffnocaq7t4w4daagvydl32igft5oziyyaebqr6vx6rb3fwh2ab4","name":"test"}`
		req := ctx.NewAPIRequest(http.MethodPost, fmt.Sprintf("/api/pins/%s", helper.pinID), []byte(reqBody))
		setAuthHeader(req, helper.token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusAccepted, rec.Code)
		var response dto.PinStatusResponse
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.NotPanics(t, func() {
			uuid.MustParse(response.RequestID)
		})
	}, TestOptions)
}

func TestAPI_deletePin(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := setupPinTest(t, ctx)

		// Make HTTP request
		req := ctx.NewAPIRequest(http.MethodDelete, fmt.Sprintf("/api/pins/%s", helper.pinID), nil)
		setAuthHeader(req, helper.token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusAccepted, rec.Code)
	}, TestOptions)
}

func TestAPI_listFiles(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		token, userID := createTestUserAndLogin(ctx)

		// Create test file paths
		testCID1 := util.GenerateTestCID(t, "test data 1")
		testCID2 := util.GenerateTestCID(t, "test data 2")

		createTestFilePath(t, ctx, userID, testCID1, "/file1.txt", "file1.txt", false)
		createTestFilePath(t, ctx, userID, testCID2, "/file2.txt", "file2.txt", false)

		// Make HTTP request
		req := ctx.NewAPIRequest(http.MethodGet, "/api/files", nil)
		setAuthHeader(req, token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusOK, rec.Code)
		var response dto.FileManagerResponse
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), response.Count)
		assert.Len(t, response.Results, 2)

		// Verify response structure
		for _, item := range response.Results {
			assert.NotEmpty(t, item.Path)
			assert.NotEmpty(t, item.Name)
			assert.IsType(t, uint64(0), item.Size)
			assert.IsType(t, false, item.IsDirectory)
			assert.IsType(t, 0, item.Depth)
			assert.IsType(t, time.Time{}, item.Created)
			assert.IsType(t, time.Time{}, item.Updated)
		}
	}, TestOptions)
}

func TestAPI_listFiles_Unauthorized(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Make HTTP request without auth
		req := ctx.NewAPIRequest(http.MethodGet, "/api/files", nil)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	}, TestOptions)
}

func TestAPI_listFiles_EmptyResults(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		token, _ := createTestUserAndLogin(ctx)

		// Make HTTP request with no data
		req := ctx.NewAPIRequest(http.MethodGet, "/api/files", nil)
		setAuthHeader(req, token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusOK, rec.Code)
		var response dto.FileManagerResponse
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), response.Count)
		assert.Empty(t, response.Results)
	}, TestOptions)
}

func TestAPI_listFiles_WithFilters(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		token, userID := createTestUserAndLogin(ctx)

		// Create test file paths
		testCID1 := util.GenerateTestCID(t, "test data 1")
		testCID2 := util.GenerateTestCID(t, "test data 2")
		testCID3 := util.GenerateTestCID(t, "test data 3")

		createTestFilePath(t, ctx, userID, testCID1, "/file1.txt", "file1.txt", false)
		createTestFilePath(t, ctx, userID, testCID2, "/test_dir", "test_dir", true)
		createTestFilePath(t, ctx, userID, testCID3, "/file2.txt", "file2.txt", false)

		// Test name filter with proper bracket notation
		req := ctx.NewAPIRequest(http.MethodGet, "/api/files?filters[name][eq]=file1.txt", nil)
		setAuthHeader(req, token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var response dto.FileManagerResponse
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), response.Count)
		assert.Len(t, response.Results, 1)
		assert.Equal(t, "file1.txt", response.Results[0].Name)

		// Test is_directory filter with proper bracket notation
		req = ctx.NewAPIRequest(http.MethodGet, "/api/files?filters[is_directory][eq]=true", nil)
		setAuthHeader(req, token)
		rec = httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		err = json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), response.Count)
		assert.Len(t, response.Results, 1)
		assert.True(t, response.Results[0].IsDirectory)
		assert.Equal(t, "test_dir", response.Results[0].Name)

		// Test contains operator for name
		req = ctx.NewAPIRequest(http.MethodGet, "/api/files?filters[name][contains]=file", nil)
		setAuthHeader(req, token)
		rec = httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		err = json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), response.Count)
		assert.Len(t, response.Results, 2)

		// Test OR operator with nested filters
		req = ctx.NewAPIRequest(http.MethodGet, "/api/files?filters[or][0][name][eq]=file1.txt&filters[or][1][is_directory][eq]=true", nil)
		setAuthHeader(req, token)
		rec = httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		err = json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), response.Count)
		assert.Len(t, response.Results, 2)

		// Test AND operator with multiple conditions
		req = ctx.NewAPIRequest(http.MethodGet, "/api/files?filters[and][0][name][contains]=file&filters[and][1][is_directory][eq]=false", nil)
		setAuthHeader(req, token)
		rec = httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		err = json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), response.Count)
		assert.Len(t, response.Results, 2)
		for _, result := range response.Results {
			assert.Contains(t, result.Name, "file")
			assert.False(t, result.IsDirectory)
		}
	}, TestOptions)
}

func TestAPI_listFiles_Pagination(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		token, userID := createTestUserAndLogin(ctx)

		// Create test file paths
		testCID1 := util.GenerateTestCID(t, "test data 1")
		testCID2 := util.GenerateTestCID(t, "test data 2")
		testCID3 := util.GenerateTestCID(t, "test data 3")

		createTestFilePath(t, ctx, userID, testCID1, "/file1.txt", "file1.txt", false)
		createTestFilePath(t, ctx, userID, testCID2, "/file2.txt", "file2.txt", false)
		createTestFilePath(t, ctx, userID, testCID3, "/file3.txt", "file3.txt", false)

		// Test pagination with _start and _end parameters (first 2 items)
		req := ctx.NewAPIRequest(http.MethodGet, "/api/files?_start=0&_end=2", nil)
		setAuthHeader(req, token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		var response dto.FileManagerResponse
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), response.Count)
		assert.Len(t, response.Results, 2)

		// Test pagination with offset (items 1-2, skipping first item)
		req = ctx.NewAPIRequest(http.MethodGet, "/api/files?_start=1&_end=3", nil)
		setAuthHeader(req, token)
		rec = httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		err = json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), response.Count)
		assert.Len(t, response.Results, 2)

		// Test single item pagination
		req = ctx.NewAPIRequest(http.MethodGet, "/api/files?_start=0&_end=1", nil)
		setAuthHeader(req, token)
		rec = httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		err = json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), response.Count)
		assert.Len(t, response.Results, 1)
	}, TestOptions)
}

func TestAPI_listDirectoryContents(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		token, userID := createTestUserAndLogin(ctx)

		// Create test file paths
		testCID1 := util.GenerateTestCID(t, "test data 1")
		testCID2 := util.GenerateTestCID(t, "test data 2")
		testCID3 := util.GenerateTestCID(t, "test data 3")

		createTestFilePath(t, ctx, userID, testCID1, "/file1.txt", "file1.txt", false)
		createTestFilePath(t, ctx, userID, testCID2, "/test_dir", "test_dir", true)
		createTestFilePath(t, ctx, userID, testCID3, "/test_dir/file2.txt", "file2.txt", false)

		// Make HTTP request to list root directory
		req := ctx.NewAPIRequest(http.MethodGet, "/api/files/directory?parent_path=/", nil)
		setAuthHeader(req, token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusOK, rec.Code)
		var response dto.FileManagerResponse
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), response.Count)
		assert.Len(t, response.Results, 2)

		// Verify directories come first
		assert.True(t, response.Results[0].IsDirectory)
		assert.False(t, response.Results[1].IsDirectory)
		assert.Equal(t, "test_dir", response.Results[0].Name)
		assert.Equal(t, "file1.txt", response.Results[1].Name)
	}, TestOptions)
}

func TestAPI_listDirectoryContents_Unauthorized(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Make HTTP request without auth
		req := ctx.NewAPIRequest(http.MethodGet, "/api/files/directory?parent_path=/", nil)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	}, TestOptions)
}

func TestAPI_listDirectoryContents_EmptyDirectory(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		token, _ := createTestUserAndLogin(ctx)

		// Make HTTP request to empty directory
		req := ctx.NewAPIRequest(http.MethodGet, "/api/files/directory?parent_path=/empty", nil)
		setAuthHeader(req, token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusOK, rec.Code)
		var response dto.FileManagerResponse
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), response.Count)
		assert.Empty(t, response.Results)
	}, TestOptions)
}

func TestAPI_listDirectoryContents_NonExistentUser(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		token, _ := createTestUserAndLogin(ctx)

		// Make HTTP request with different user ID
		req := ctx.NewAPIRequest(http.MethodGet, "/api/files/directory?parent_path=/", nil)
		setAuthHeader(req, token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response (should be empty but not error)
		assert.Equal(t, http.StatusOK, rec.Code)
		var response dto.FileManagerResponse
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), response.Count)
	}, TestOptions)
}

func TestAPI_getBreadcrumbs(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		token, userID := createTestUserAndLogin(ctx)

		// Create test file paths for breadcrumb hierarchy
		testCID1 := util.GenerateTestCID(t, "test data 1")
		testCID2 := util.GenerateTestCID(t, "test data 2")
		testCID3 := util.GenerateTestCID(t, "test data 3")

		createTestFilePath(t, ctx, userID, testCID1, "/test_dir", "test_dir", true)
		createTestFilePath(t, ctx, userID, testCID2, "/test_dir/subdir", "subdir", true)
		createTestFilePath(t, ctx, userID, testCID3, "/test_dir/subdir/file.txt", "file.txt", false)

		// Make HTTP request
		req := ctx.NewAPIRequest(http.MethodGet, "/api/files/breadcrumbs?path=/test_dir/subdir/file.txt", nil)
		setAuthHeader(req, token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusOK, rec.Code)
		var response dto.FileManagerResponse
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.Equal(t, uint64(3), response.Count)
		assert.Len(t, response.Results, 3)

		// Verify breadcrumb order (should be ordered by depth)
		assert.Equal(t, "/test_dir", response.Results[0].Path)
		assert.Equal(t, "/test_dir/subdir", response.Results[1].Path)
		assert.Equal(t, "/test_dir/subdir/file.txt", response.Results[2].Path)
	}, TestOptions)
}

func TestAPI_getBreadcrumbs_Unauthorized(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Make HTTP request without auth
		req := ctx.NewAPIRequest(http.MethodGet, "/api/files/breadcrumbs?path=/test", nil)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	}, TestOptions)
}

func TestAPI_getBreadcrumbs_InvalidPath(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		token, _ := createTestUserAndLogin(ctx)

		// Make HTTP request with empty path
		req := ctx.NewAPIRequest(http.MethodGet, "/api/files/breadcrumbs?path=", nil)
		setAuthHeader(req, token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusBadRequest, rec.Code)

		// Make HTTP request with path without leading slash
		req = ctx.NewAPIRequest(http.MethodGet, "/api/files/breadcrumbs?path=test/file.txt", nil)
		setAuthHeader(req, token)
		rec = httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	}, TestOptions)
}

func TestAPI_getBreadcrumbs_PathNotFound(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		token, _ := createTestUserAndLogin(ctx)

		// Make HTTP request with non-existent path
		req := ctx.NewAPIRequest(http.MethodGet, "/api/files/breadcrumbs?path=/nonexistent/file.txt", nil)
		setAuthHeader(req, token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusNotFound, rec.Code)
	}, TestOptions)
}

func createTestCAR(t *testing.T) ([]byte, cid.Cid) {
	// Create a simple CAR file with one block containing "test file content"
	content := []byte("test file content")

	// Create a CID for our content
	pref := cid.Prefix{
		Version:  1,
		Codec:    uint64(multicodec.Raw),
		MhType:   multihash.SHA2_256,
		MhLength: -1, // default length
	}

	c, err := pref.Sum(content)
	if err != nil {
		t.Fatalf("failed to create CID: %v", err)
	}

	// Create temp file for CAR
	tmpFile, err := os.CreateTemp("", "test-car-*.car")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Create CAR writer with our actual root CID
	cdest, err := blockstore.OpenReadWrite(tmpFile.Name(), []cid.Cid{c}, car.WriteAsCarV1(true))
	if err != nil {
		t.Fatalf("failed to create CAR writer: %v", err)
	}

	// Write our block
	blk, err := blocks.NewBlockWithCid(content, c)
	if err != nil {
		t.Fatalf("failed to create block: %v", err)
	}
	err = cdest.Put(context.Background(), blk)
	if err != nil {
		t.Fatalf("failed to write block: %v", err)
	}

	// Finalize the CAR
	if err := cdest.Finalize(); err != nil {
		t.Fatalf("failed to finalize CAR: %v", err)
	}

	// Read the temp file back into memory for the test
	carData, err := os.ReadFile(tmpFile.Name())
	if err != nil {
		t.Fatalf("failed to read temp CAR file: %v", err)
	}

	return carData, c
}

func TestAPI_handleUpload(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		token, _ := createTestUserAndLogin(ctx)

		// Create test CAR file
		carData, expectedCID := createTestCAR(t)

		// Make HTTP request with CAR file upload
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)
		part, _ := writer.CreateFormFile("file", "test.car")
		part.Write(carData)
		writer.Close()

		req := ctx.NewAPIRequest(http.MethodPost, "/api/upload", body.Bytes())
		setAuthHeader(req, token)
		req.Header.Set("Content-Type", writer.FormDataContentType())
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusOK, rec.Code)

		// Parse response to get CID
		var resp dto.PostUploadResponse
		err := json.Unmarshal(rec.Body.Bytes(), &resp)
		assert.NoError(t, err)
		assert.Equal(t, expectedCID.String(), resp.CID)
	}, TestOptions, coreTesting.WithMockS3())
}

func TestAPI_handleGetBlockMeta(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := setupPinTest(t, ctx)

		// Make HTTP request
		req := ctx.NewAPIRequest(http.MethodGet, fmt.Sprintf("/api/block/meta/%s", helper.cid.String()), nil)
		setAuthHeader(req, helper.token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusOK, rec.Code)
		var response dto.BlockMetaResponse
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)

		// Verify response structure
		assert.NotNil(t, response)
		assert.IsType(t, "", response.Name)
		assert.IsType(t, uint8(0), response.Type)
		assert.IsType(t, int64(0), response.BlockSize)
		assert.IsType(t, []string{}, response.ChildCID)
		assert.True(t, len(response.ChildCID) > 0, "ChildCID should not be empty")
	}, TestOptions)
}

func TestAPI_handleGetBlockMetaBatch(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := setupPinTest(t, ctx)

		// Make HTTP request
		reqBody := fmt.Sprintf(`{"cid":["%s"]}`, helper.cid.String())
		req := ctx.NewAPIRequest(http.MethodPost, "/api/block/meta/batch", []byte(reqBody))
		setAuthHeader(req, helper.token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusOK, rec.Code)
		var response map[string]*dto.BlockMetaResponse
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)

		// Verify response structure
		assert.NotEmpty(t, response)
		for cid, meta := range response {
			assert.NotEmpty(t, cid)
			assert.NotNil(t, meta)
			assert.IsType(t, "", meta.Name)
			assert.IsType(t, uint8(0), meta.Type)
			assert.IsType(t, int64(0), meta.BlockSize)
			assert.IsType(t, []string{}, meta.ChildCID)
			assert.True(t, len(meta.ChildCID) > 0, "ChildCID should not be empty")
		}
	}, TestOptions)
}

func TestAPI_handleGetInfo(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		token, _ := createTestUserAndLogin(ctx)

		// Make HTTP request
		req := ctx.NewAPIRequest(http.MethodGet, "/api/info", nil)
		setAuthHeader(req, token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusOK, rec.Code)
		var response dto.InfoResponse
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)

		// Verify response structure
		assert.NotEmpty(t, response.PeerID)
		assert.NotEmpty(t, response.AnnouncementAddresses)
		assert.NotEmpty(t, response.ConnectionAddresses)

		// Verify announcement addresses format
		for _, addr := range response.AnnouncementAddresses {
			assert.Contains(t, addr, "/ip6/")
			assert.Contains(t, addr, "/tcp/4001")
		}

		// Verify connection addresses format
		for _, addr := range response.ConnectionAddresses {
			assert.Contains(t, addr, "/ip6/")
			assert.Contains(t, addr, "/tcp/4001/p2p/")
			assert.Contains(t, addr, response.PeerID)
		}
	}, TestOptions)
}

func TestAPI_handleIPFSGet(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := setupPinTest(t, ctx)

		req := ctx.NewAPIRequest(http.MethodGet, fmt.Sprintf("/ipfs/%s", helper.cid.String()), nil)
		setAuthHeader(req, helper.token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, "tornadocash", rec.Body.String())
	}, TestOptions)
}
