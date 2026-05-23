package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	"go.lumeweb.com/portal/core"
	coreMocks "go.lumeweb.com/portal/core/testing/mocks"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/portal/db/models"
)

func TestAPI_handleUploadResult_CompletedUpload(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		testCID := util.GenerateTestCID(t, "test data")
		token, _ := helper.SetupAuthenticatedTestWithCID(testCID)

		mockTUSService := core.GetService[*coreMocks.MockTUSService](helper.ctx, core.TUS_SERVICE)
		mockRequestService := core.GetService[*coreMocks.MockRequestService](helper.ctx, core.REQUEST_SERVICE)

		mockTUSService.EXPECT().UploadExists(mock.Anything, mock.Anything, "test-upload-id").
			Return(true, &models.TUSRequest{RequestID: 1, TUSUploadID: "test-upload-id"}).Maybe()

		mockRequestService.EXPECT().GetRequest(mock.Anything, uint(1)).
			Return(&models.Request{
				Status:  models.RequestStatusCompleted,
				UserID:  new(uint(1)),
				Hash:    testCID.Hash(),
				CIDType: testCID.Type(),
			}, nil).Maybe()

		req := ctx.NewAPIRequest(http.MethodGet, "/api/upload/result/test-upload-id", nil)
		setAuthHeader(req, token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var resp dto.UploadResultResponse
		err := json.Unmarshal(rec.Body.Bytes(), &resp)
		assert.NoError(t, err)
		assert.Equal(t, testCID.String(), resp.CID)
		assert.Equal(t, models.RequestStatusCompleted, resp.Status)
	}, TestOptions)
}

func TestAPI_handleUploadResult_ProcessingUpload(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		testCID := util.GenerateTestCID(t, "test data")
		token, _ := helper.SetupAuthenticatedTestWithCID(testCID)

		mockTUSService := core.GetService[*coreMocks.MockTUSService](helper.ctx, core.TUS_SERVICE)
		mockRequestService := core.GetService[*coreMocks.MockRequestService](helper.ctx, core.REQUEST_SERVICE)

		mockTUSService.EXPECT().UploadExists(mock.Anything, mock.Anything, "processing-upload-id").
			Return(true, &models.TUSRequest{RequestID: 2, TUSUploadID: "processing-upload-id"}).Maybe()

		mockRequestService.EXPECT().GetRequest(mock.Anything, uint(2)).
			Return(&models.Request{
				Status: models.RequestStatusProcessing,
				UserID: new(uint(1)),
			}, nil).Maybe()

		req := ctx.NewAPIRequest(http.MethodGet, "/api/upload/result/processing-upload-id", nil)
		setAuthHeader(req, token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		assert.Equal(t, http.StatusAccepted, rec.Code)

		var resp dto.UploadResultResponse
		err := json.Unmarshal(rec.Body.Bytes(), &resp)
		assert.NoError(t, err)
		assert.Equal(t, models.RequestStatusProcessing, resp.Status)
		assert.Empty(t, resp.CID)
	}, TestOptions)
}

func TestAPI_handleUploadResult_FailedUpload(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		testCID := util.GenerateTestCID(t, "test data")
		token, _ := helper.SetupAuthenticatedTestWithCID(testCID)

		mockTUSService := core.GetService[*coreMocks.MockTUSService](helper.ctx, core.TUS_SERVICE)
		mockRequestService := core.GetService[*coreMocks.MockRequestService](helper.ctx, core.REQUEST_SERVICE)

		mockTUSService.EXPECT().UploadExists(mock.Anything, mock.Anything, "failed-upload-id").
			Return(true, &models.TUSRequest{RequestID: 3, TUSUploadID: "failed-upload-id"}).Maybe()

		mockRequestService.EXPECT().GetRequest(mock.Anything, uint(3)).
			Return(&models.Request{
				Status:        models.RequestStatusFailed,
				StatusMessage: "conversion failed",
				UserID:        new(uint(1)),
			}, nil).Maybe()

		req := ctx.NewAPIRequest(http.MethodGet, "/api/upload/result/failed-upload-id", nil)
		setAuthHeader(req, token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)

		var resp dto.UploadResultResponse
		err := json.Unmarshal(rec.Body.Bytes(), &resp)
		assert.NoError(t, err)
		assert.Equal(t, models.RequestStatusFailed, resp.Status)
		assert.Equal(t, "conversion failed", resp.Error)
	}, TestOptions)
}

func TestAPI_handleUploadResult_NotFound(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		testCID := util.GenerateTestCID(t, "test data")
		token, _ := helper.SetupAuthenticatedTestWithCID(testCID)

		mockTUSService := core.GetService[*coreMocks.MockTUSService](helper.ctx, core.TUS_SERVICE)

		mockTUSService.EXPECT().UploadExists(mock.Anything, mock.Anything, "nonexistent-id").
			Return(false, nil).Maybe()

		req := ctx.NewAPIRequest(http.MethodGet, "/api/upload/result/nonexistent-id", nil)
		setAuthHeader(req, token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		assert.Equal(t, http.StatusNotFound, rec.Code)
	}, TestOptions)
}

func TestAPI_handleUploadResult_RequestIDLookup(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		testCID := util.GenerateTestCID(t, "test data")
		token, _ := helper.SetupAuthenticatedTestWithCID(testCID)

		mockTUSService := core.GetService[*coreMocks.MockTUSService](helper.ctx, core.TUS_SERVICE)
		mockRequestService := core.GetService[*coreMocks.MockRequestService](helper.ctx, core.REQUEST_SERVICE)

		mockTUSService.EXPECT().UploadExists(mock.Anything, mock.Anything, "42").
			Return(false, nil).Maybe()

		mockRequestService.EXPECT().GetRequest(mock.Anything, uint(42)).
			Return(&models.Request{
				Status:  models.RequestStatusCompleted,
				UserID:  new(uint(1)),
				Hash:    testCID.Hash(),
				CIDType: testCID.Type(),
			}, nil).Maybe()

		req := ctx.NewAPIRequest(http.MethodGet, "/api/upload/result/42", nil)
		setAuthHeader(req, token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var resp dto.UploadResultResponse
		err := json.Unmarshal(rec.Body.Bytes(), &resp)
		assert.NoError(t, err)
		assert.Equal(t, testCID.String(), resp.CID)
		assert.Equal(t, models.RequestStatusCompleted, resp.Status)
	}, TestOptions)
}

func TestAPI_handleUploadResult_Unauthenticated(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		req := ctx.NewAPIRequest(http.MethodGet, "/api/upload/result/test-upload-id", nil)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	}, TestOptions)
}

func TestAPI_handleUploadResult_IDOR_DifferentUser(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		testCID := util.GenerateTestCID(t, "test data")
		token, _ := helper.SetupAuthenticatedTestWithCID(testCID)

		mockTUSService := core.GetService[*coreMocks.MockTUSService](helper.ctx, core.TUS_SERVICE)
		mockRequestService := core.GetService[*coreMocks.MockRequestService](helper.ctx, core.REQUEST_SERVICE)

		mockTUSService.EXPECT().UploadExists(mock.Anything, mock.Anything, "idor-upload-id").
			Return(true, &models.TUSRequest{RequestID: 6, TUSUploadID: "idor-upload-id"}).Maybe()

		mockRequestService.EXPECT().GetRequest(mock.Anything, uint(6)).
			Return(&models.Request{
				Status: models.RequestStatusCompleted,
				UserID: new(uint(999)),
				Hash:   testCID.Hash(),
			}, nil).Maybe()

		req := ctx.NewAPIRequest(http.MethodGet, "/api/upload/result/idor-upload-id", nil)
		setAuthHeader(req, token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		assert.Equal(t, http.StatusNotFound, rec.Code)
	}, TestOptions)
}

func TestAPI_handleUploadResult_DuplicateStatus(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		testCID := util.GenerateTestCID(t, "test data")
		token, _ := helper.SetupAuthenticatedTestWithCID(testCID)

		mockTUSService := core.GetService[*coreMocks.MockTUSService](helper.ctx, core.TUS_SERVICE)
		mockRequestService := core.GetService[*coreMocks.MockRequestService](helper.ctx, core.REQUEST_SERVICE)

		mockTUSService.EXPECT().UploadExists(mock.Anything, mock.Anything, "dup-upload-id").
			Return(true, &models.TUSRequest{RequestID: 5, TUSUploadID: "dup-upload-id"}).Maybe()

		mockRequestService.EXPECT().GetRequest(mock.Anything, uint(5)).
			Return(&models.Request{
				Status:  models.RequestStatusDuplicate,
				UserID:  new(uint(1)),
				Hash:    testCID.Hash(),
				CIDType: testCID.Type(),
			}, nil).Maybe()

		req := ctx.NewAPIRequest(http.MethodGet, "/api/upload/result/dup-upload-id", nil)
		setAuthHeader(req, token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)

		var resp dto.UploadResultResponse
		err := json.Unmarshal(rec.Body.Bytes(), &resp)
		assert.NoError(t, err)
		assert.Equal(t, testCID.String(), resp.CID)
		assert.Equal(t, models.RequestStatusCompleted, resp.Status)
	}, TestOptions)
}
