package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

func TestAPI_handleUpload(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		token, _ := helper.SetupAuthenticatedTestWithCID(util.GenerateTestCID(t, "test data"))
		carData, expectedCID := createTestCAR(t)
		helper.SetupUploadServiceMocks(expectedCID)
		helper.SetupUploadWorkflowServiceMock()

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

		assert.Equal(t, http.StatusOK, rec.Code)

		var resp dto.PostUploadResponse
		err := json.Unmarshal(rec.Body.Bytes(), &resp)
		assert.NoError(t, err)
		assert.Equal(t, expectedCID.String(), resp.CID)
	}, TestOptions)
}

func TestAPI_handleUpload_UploadQuotaExceeded(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		token, _ := helper.SetupAuthenticatedTestWithCID(util.GenerateTestCID(t, "test data"))

		// Get the upload service mock and setup quota error expectation
		mockUploadService := core.GetService[*mocks.MockUploadService](helper.ctx, pluginCore.UPLOAD_SERVICE)

		mockUploadService.EXPECT().HandleUploadWithMode(mock.Anything, mock.Anything, mock.AnythingOfType("uint"), mock.AnythingOfType("upload.ArchiveMode")).
			Return(cid.Undef, "", core.ErrUploadQuotaExceeded)

		carData, _ := createTestCAR(t)
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

		assert.Equal(t, http.StatusTooManyRequests, rec.Code)

		var errResp map[string]interface{}
		err := json.Unmarshal(rec.Body.Bytes(), &errResp)
		assert.NoError(t, err)

		// Check that error structure contains quota exceeded information
		if errData, ok := errResp["error"].(map[string]interface{}); ok {
			assert.Contains(t, errData["reason"], "UploadQuotaExceeded")
		}
	}, TestOptions)
}

func TestAPI_handleUpload_StorageQuotaExceeded(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		token, _ := helper.SetupAuthenticatedTestWithCID(util.GenerateTestCID(t, "test data"))

		// Get the upload service mock and setup quota error expectation
		mockUploadService := core.GetService[*mocks.MockUploadService](helper.ctx, pluginCore.UPLOAD_SERVICE)

		mockUploadService.EXPECT().HandleUploadWithMode(mock.Anything, mock.Anything, mock.AnythingOfType("uint"), mock.AnythingOfType("upload.ArchiveMode")).
			Return(cid.Undef, "", core.ErrStorageQuotaExceeded)

		carData, _ := createTestCAR(t)
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

		assert.Equal(t, http.StatusTooManyRequests, rec.Code)

		var errResp map[string]interface{}
		err := json.Unmarshal(rec.Body.Bytes(), &errResp)
		assert.NoError(t, err)

		// Check that error structure contains storage quota exceeded information
		if errData, ok := errResp["error"].(map[string]interface{}); ok {
			assert.Contains(t, errData["reason"], "StorageQuotaExceeded")
		}
	}, TestOptions)
}

func TestAPI_handleUpload_WrappedQuotaError(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		token, _ := helper.SetupAuthenticatedTestWithCID(util.GenerateTestCID(t, "test data"))

		// Get the upload service mock and setup wrapped quota error expectation
		wrappedError := fmt.Errorf("processing failed: %w", core.ErrUploadQuotaExceeded)
		mockUploadService := core.GetService[*mocks.MockUploadService](helper.ctx, pluginCore.UPLOAD_SERVICE)

		mockUploadService.EXPECT().HandleUploadWithMode(mock.Anything, mock.Anything, mock.AnythingOfType("uint"), mock.AnythingOfType("upload.ArchiveMode")).
			Return(cid.Undef, "", wrappedError)

		carData, _ := createTestCAR(t)
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

		assert.Equal(t, http.StatusTooManyRequests, rec.Code)

		var errResp map[string]interface{}
		err := json.Unmarshal(rec.Body.Bytes(), &errResp)
		assert.NoError(t, err)

		// Check that error structure contains quota exceeded information
		if errData, ok := errResp["error"].(map[string]interface{}); ok {
			assert.Contains(t, errData["reason"], "UploadQuotaExceeded")
		}
	}, TestOptions)
}

// TODO: Implement these tests - currently incomplete due to missing dependencies
// See api_test.go for commented implementations of:
// - TestAPI_handleUpload_ZIPConvert
// - TestAPI_handleUpload_ZIPPreserve
// - TestAPI_handleUpload_InvalidZipMode
