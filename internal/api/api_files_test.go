package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	"go.lumeweb.com/queryutil"
	"go.lumeweb.com/queryutil/filter"
	"gorm.io/gorm"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

// File API Tests

func TestAPI_listFiles(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		token, userID := helper.SetupAuthenticatedTestWithCID(util.GenerateTestCID(t, "test data"))

		testCID1 := util.GenerateTestCID(t, "test data 1")
		testCID2 := util.GenerateTestCID(t, "test data 2")

		// Create mock file paths using helper
		mockFilePaths := []*pluginDb.FilePath{
			createMockFilePath(userID, testCID1, "/file1.txt", "file1.txt", false),
			createMockFilePath(userID, testCID2, "/file2.txt", "file2.txt", false),
		}

		// Setup file manager service mock to return our test data
		mockFileManagerService := helper.SetupFileManagerServiceMocks()
		mockFileManagerService.EXPECT().ListFiles(mock.Anything, userID, mock.Anything, mock.Anything, mock.Anything).Return(mockFilePaths, int64(2), nil)

		// Setup pin service mock for multiple CIDs
		helper.setupMultiplePinServiceMocks(userID, []cid.Cid{testCID1, testCID2})

		rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/files", token, nil)

		var response queryutil.Response[[]dto.FileManagerItem]
		helper.assertJSONResponse(t, rec, http.StatusOK, &response)

		assert.Equal(t, int64(2), response.Total)
		assert.Len(t, response.Data, 2)

		for _, item := range response.Data {
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
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		req := ctx.NewAPIRequest(http.MethodGet, "/api/files", nil)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	}, TestOptions)
}

func TestAPI_listFiles_EmptyResults(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		token, userID := helper.SetupAuthenticatedTestWithCID(util.GenerateTestCID(t, "test data"))

		// Setup file manager service mock to return empty results
		mockFileManagerService := helper.SetupFileManagerServiceMocks()
		mockFileManagerService.EXPECT().ListFiles(mock.Anything, userID, mock.Anything, mock.Anything, mock.Anything).Return([]*pluginDb.FilePath{}, int64(0), nil)

		rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/files", token, nil)

		var response queryutil.Response[[]dto.FileManagerItem]
		helper.assertJSONResponse(t, rec, http.StatusOK, &response)
		assert.Equal(t, int64(0), response.Total)
		assert.Empty(t, response.Data)
	}, TestOptions)
}

func TestAPI_listFiles_WithFilters(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		token, userID := helper.SetupAuthenticatedTestWithCID(util.GenerateTestCID(t, "test data"))

		testCID1 := util.GenerateTestCID(t, "test data 1")
		testCID2 := util.GenerateTestCID(t, "test data 2")
		testCID3 := util.GenerateTestCID(t, "test data 3")

		// Create mock file paths using helper
		mockFilePaths := createTestDirectoryDataSet(userID, testCID1, testCID2, testCID3)

		// Setup file manager service mock to return filtered results for each request
		mockFileManagerService := helper.SetupFileManagerServiceMocks()

		// Setup pin service mock for multiple CIDs
		helper.setupMultiplePinServiceMocks(userID, []cid.Cid{testCID1, testCID2, testCID3})

		// Setup expectations for each filtered request
		mockFileManagerService.EXPECT().ListFiles(mock.Anything, userID, mock.Anything, mock.Anything, mock.Anything).Return([]*pluginDb.FilePath{mockFilePaths[0]}, int64(1), nil).Times(1)
		mockFileManagerService.EXPECT().ListFiles(mock.Anything, userID, mock.Anything, mock.Anything, mock.Anything).Return([]*pluginDb.FilePath{mockFilePaths[1]}, int64(1), nil).Times(1)
		mockFileManagerService.EXPECT().ListFiles(mock.Anything, userID, mock.Anything, mock.Anything, mock.Anything).Return([]*pluginDb.FilePath{mockFilePaths[0], mockFilePaths[2]}, int64(2), nil).Times(1)
		mockFileManagerService.EXPECT().ListFiles(mock.Anything, userID, mock.Anything, mock.Anything, mock.Anything).Return([]*pluginDb.FilePath{mockFilePaths[0], mockFilePaths[1]}, int64(2), nil).Times(1)
		mockFileManagerService.EXPECT().ListFiles(mock.Anything, userID, mock.Anything, mock.Anything, mock.Anything).Return([]*pluginDb.FilePath{mockFilePaths[0], mockFilePaths[2]}, int64(2), nil).Times(1)

		// Test filter by name = file1.txt
		url, err := queryutil.BuildURL("/api/files", nil, nil, filter.Equal("name", "file1.txt"))
		assert.NoError(t, err)
		rec := helper.makeAuthenticatedRequest(http.MethodGet, url, token, nil)
		var response queryutil.Response[[]dto.FileManagerItem]
		helper.assertJSONResponse(t, rec, http.StatusOK, &response)
		assert.Equal(t, int64(1), response.Total)
		assert.Len(t, response.Data, 1)
		assert.Equal(t, "file1.txt", response.Data[0].Name)

		// Test filter by is_directory = true
		url, err = queryutil.BuildURL("/api/files", nil, nil, filter.Equal("is_directory", true))
		assert.NoError(t, err)
		rec = helper.makeAuthenticatedRequest(http.MethodGet, url, token, nil)
		helper.assertJSONResponse(t, rec, http.StatusOK, &response)
		assert.Equal(t, int64(1), response.Total)
		assert.Len(t, response.Data, 1)
		assert.True(t, response.Data[0].IsDirectory)
		assert.Equal(t, "test_dir", response.Data[0].Name)

		// Test filter by name contains "file"
		url, err = queryutil.BuildURL("/api/files", nil, nil, filter.Contains("name", "file"))
		assert.NoError(t, err)
		rec = helper.makeAuthenticatedRequest(http.MethodGet, url, token, nil)
		helper.assertJSONResponse(t, rec, http.StatusOK, &response)
		assert.Equal(t, int64(2), response.Total)
		assert.Len(t, response.Data, 2)

		// Test OR filter
		url, err = queryutil.BuildURL("/api/files", nil, nil, filter.Or(
			filter.Equal("name", "file1.txt"),
			filter.Equal("is_directory", true),
		))
		assert.NoError(t, err)
		rec = helper.makeAuthenticatedRequest(http.MethodGet, url, token, nil)
		helper.assertJSONResponse(t, rec, http.StatusOK, &response)
		assert.Equal(t, int64(2), response.Total)
		assert.Len(t, response.Data, 2)

		// Test AND filter
		url, err = queryutil.BuildURL("/api/files", nil, nil, filter.And(
			filter.Contains("name", "file"),
			filter.Equal("is_directory", false),
		))
		assert.NoError(t, err)
		rec = helper.makeAuthenticatedRequest(http.MethodGet, url, token, nil)
		helper.assertJSONResponse(t, rec, http.StatusOK, &response)
		assert.Equal(t, int64(2), response.Total)
		assert.Len(t, response.Data, 2)
		for _, result := range response.Data {
			assert.Contains(t, result.Name, "file")
			assert.False(t, result.IsDirectory)
		}
	}, TestOptions)
}

func TestAPI_listFiles_Pagination(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		token, userID := helper.SetupAuthenticatedTestWithCID(util.GenerateTestCID(t, "test data"))

		testCID1 := util.GenerateTestCID(t, "test data 1")
		testCID2 := util.GenerateTestCID(t, "test data 2")
		testCID3 := util.GenerateTestCID(t, "test data 3")

		// Create mock file paths using helper
		mockFilePaths := createTestFileDataSet(userID, testCID1, testCID2, testCID3)

		// Setup file manager service mock to return paginated results for each request
		mockFileManagerService := helper.SetupFileManagerServiceMocks()

		// Setup pin service mock for multiple CIDs
		helper.setupMultiplePinServiceMocks(userID, []cid.Cid{testCID1, testCID2, testCID3})

		// Setup expectations for each paginated request
		mockFileManagerService.EXPECT().ListFiles(mock.Anything, userID, mock.Anything, mock.Anything, mock.Anything).Return([]*pluginDb.FilePath{mockFilePaths[0], mockFilePaths[1]}, int64(3), nil).Times(1)
		mockFileManagerService.EXPECT().ListFiles(mock.Anything, userID, mock.Anything, mock.Anything, mock.Anything).Return([]*pluginDb.FilePath{mockFilePaths[1], mockFilePaths[2]}, int64(3), nil).Times(1)
		mockFileManagerService.EXPECT().ListFiles(mock.Anything, userID, mock.Anything, mock.Anything, mock.Anything).Return([]*pluginDb.FilePath{mockFilePaths[0]}, int64(3), nil).Times(1)

		// Test first page: _start=0&_end=2
		pagination, err := filter.NewPagination(0, 2)
		assert.NoError(t, err)
		url, err := queryutil.BuildURL("/api/files", nil, &pagination)
		assert.NoError(t, err)
		rec := helper.makeAuthenticatedRequest(http.MethodGet, url, token, nil)
		var response queryutil.Response[[]dto.FileManagerItem]
		helper.assertJSONResponse(t, rec, http.StatusOK, &response)
		assert.Equal(t, int64(3), response.Total)
		assert.Len(t, response.Data, 2)

		// Test second page: _start=1&_end=3
		pagination, err = filter.NewPagination(1, 2)
		assert.NoError(t, err)
		url, err = queryutil.BuildURL("/api/files", nil, &pagination)
		assert.NoError(t, err)
		rec = helper.makeAuthenticatedRequest(http.MethodGet, url, token, nil)
		helper.assertJSONResponse(t, rec, http.StatusOK, &response)
		assert.Equal(t, int64(3), response.Total)
		assert.Len(t, response.Data, 2)

		// Test single item: _start=0&_end=1
		pagination, err = filter.NewPagination(0, 1)
		assert.NoError(t, err)
		url, err = queryutil.BuildURL("/api/files", nil, &pagination)
		assert.NoError(t, err)
		rec = helper.makeAuthenticatedRequest(http.MethodGet, url, token, nil)
		helper.assertJSONResponse(t, rec, http.StatusOK, &response)
		assert.Equal(t, int64(3), response.Total)
		assert.Len(t, response.Data, 1)
	}, TestOptions)
}

func TestAPI_listDirectoryContents(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		token, userID := helper.SetupAuthenticatedTestWithCID(util.GenerateTestCID(t, "test data"))

		testCID1 := util.GenerateTestCID(t, "test data 1")
		testCID2 := util.GenerateTestCID(t, "test data 2")
		testCID3 := util.GenerateTestCID(t, "test data 3")

		// Create mock file paths using helper
		mockFilePaths := createTestDirectoryDataSet(userID, testCID1, testCID2, testCID3)

		// Setup file manager service mock to return only root directory contents
		mockFileManagerService := helper.SetupFileManagerServiceMocks()
		mockFileManagerService.EXPECT().ListDirectoryContents(mock.Anything, userID, mock.Anything).Return([]*pluginDb.FilePath{mockFilePaths[1], mockFilePaths[0]}, nil)

		url, err := queryutil.BuildURL("/api/files/directory", nil, nil, filter.Equal("parent_path", "/"))
		assert.NoError(t, err)
		rec := helper.makeAuthenticatedRequest(http.MethodGet, url, token, nil)

		var response queryutil.Response[[]dto.FileManagerItem]
		helper.assertJSONResponse(t, rec, http.StatusOK, &response)

		assert.Equal(t, int64(2), response.Total)
		assert.Len(t, response.Data, 2)

		assert.True(t, response.Data[0].IsDirectory)
		assert.False(t, response.Data[1].IsDirectory)
		assert.Equal(t, "test_dir", response.Data[0].Name)
		assert.Equal(t, "file1.txt", response.Data[1].Name)
	}, TestOptions)
}

func TestAPI_listDirectoryContents_Unauthorized(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		url, err := queryutil.BuildURL("/api/files/directory", nil, nil, filter.Equal("parent_path", "/"))
		assert.NoError(t, err)
		req := ctx.NewAPIRequest(http.MethodGet, url, nil)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	}, TestOptions)
}

func TestAPI_listDirectoryContents_EmptyDirectory(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		token, userID := helper.SetupAuthenticatedTestWithCID(util.GenerateTestCID(t, "test data"))

		// Setup file manager service mock to return empty results
		mockFileManagerService := helper.SetupFileManagerServiceMocks()
		mockFileManagerService.EXPECT().ListDirectoryContents(mock.Anything, userID, mock.Anything).Return([]*pluginDb.FilePath{}, nil)

		url, err := queryutil.BuildURL("/api/files/directory", nil, nil, filter.Equal("parent_path", "/empty"))
		assert.NoError(t, err)
		rec := helper.makeAuthenticatedRequest(http.MethodGet, url, token, nil)

		var response queryutil.Response[[]dto.FileManagerItem]
		helper.assertJSONResponse(t, rec, http.StatusOK, &response)
		assert.Equal(t, int64(0), response.Total)
		assert.Empty(t, response.Data)
	}, TestOptions)
}

func TestAPI_listDirectoryContents_NonExistentUser(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		token, userID := helper.SetupAuthenticatedTestWithCID(util.GenerateTestCID(t, "test data"))

		// Setup file manager service mock to return empty results
		mockFileManagerService := helper.SetupFileManagerServiceMocks()
		mockFileManagerService.EXPECT().ListDirectoryContents(mock.Anything, userID, mock.Anything).Return([]*pluginDb.FilePath{}, nil)

		url, err := queryutil.BuildURL("/api/files/directory", nil, nil, filter.Equal("parent_path", "/"))
		assert.NoError(t, err)
		rec := helper.makeAuthenticatedRequest(http.MethodGet, url, token, nil)

		var response queryutil.Response[[]dto.FileManagerItem]
		helper.assertJSONResponse(t, rec, http.StatusOK, &response)
		assert.Equal(t, int64(0), response.Total)
	}, TestOptions)
}

func TestAPI_getBreadcrumbs(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		token, userID := helper.SetupAuthenticatedTestWithCID(util.GenerateTestCID(t, "test data"))

		testCID1 := util.GenerateTestCID(t, "test data 1")
		testCID2 := util.GenerateTestCID(t, "test data 2")
		testCID3 := util.GenerateTestCID(t, "test data 3")

		// Create mock file paths using helper
		mockFilePaths := createBreadcrumbsDataSet(userID, testCID1, testCID2, testCID3)

		// Setup file manager service mock to return our test data
		mockFileManagerService := helper.SetupFileManagerServiceMocks()
		mockFileManagerService.EXPECT().GetBreadcrumbs(mock.Anything, userID, mock.Anything).Return(mockFilePaths, nil)

		url, err := queryutil.BuildURL("/api/files/breadcrumbs", nil, nil, filter.Equal("path", "/test_dir/subdir/file.txt"))
		assert.NoError(t, err)
		rec := helper.makeAuthenticatedRequest(http.MethodGet, url, token, nil)

		var response queryutil.Response[[]dto.FileManagerItem]
		helper.assertJSONResponse(t, rec, http.StatusOK, &response)

		assert.Equal(t, int64(3), response.Total)
		assert.Len(t, response.Data, 3)

		assert.Equal(t, "/test_dir", response.Data[0].Path)
		assert.Equal(t, "/test_dir/subdir", response.Data[1].Path)
		assert.Equal(t, "/test_dir/subdir/file.txt", response.Data[2].Path)
	}, TestOptions)
}

func TestAPI_getBreadcrumbs_Unauthorized(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		url, err := queryutil.BuildURL("/api/files/breadcrumbs", nil, nil, filter.Equal("path", "/test"))
		assert.NoError(t, err)
		req := ctx.NewAPIRequest(http.MethodGet, url, nil)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	}, TestOptions)
}

func TestAPI_getBreadcrumbs_InvalidPath(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		token, _ := helper.SetupAuthenticatedTestWithCID(util.GenerateTestCID(t, "test data"))

		url, err := queryutil.BuildURL("/api/files/breadcrumbs", nil, nil, filter.Equal("path", ""))
		assert.NoError(t, err)
		rec := helper.makeAuthenticatedRequest(http.MethodGet, url, token, nil)
		assert.Equal(t, http.StatusBadRequest, rec.Code)

		url, err = queryutil.BuildURL("/api/files/breadcrumbs", nil, nil, filter.Equal("path", "test/file.txt"))
		assert.NoError(t, err)
		rec = helper.makeAuthenticatedRequest(http.MethodGet, url, token, nil)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	}, TestOptions)
}

func TestAPI_getBreadcrumbs_PathNotFound(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		token, userID := helper.SetupAuthenticatedTestWithCID(util.GenerateTestCID(t, "test data"))

		// Setup file manager service mock to return error for non-existent path
		mockFileManagerService := helper.SetupFileManagerServiceMocks()
		mockFileManagerService.EXPECT().GetBreadcrumbs(mock.Anything, userID, "/nonexistent/file.txt").Return([]*pluginDb.FilePath{}, gorm.ErrRecordNotFound)

		url, err := queryutil.BuildURL("/api/files/breadcrumbs", nil, nil, filter.Equal("path", "/nonexistent/file.txt"))
		assert.NoError(t, err)
		rec := helper.makeAuthenticatedRequest(http.MethodGet, url, token, nil)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	}, TestOptions)
}
