package api

import (
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
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

// TODO: Implement these tests - currently incomplete due to missing dependencies
// See api_test.go for commented implementations of:
// - TestAPI_handleUpload_ZIPConvert
// - TestAPI_handleUpload_ZIPPreserve
// - TestAPI_handleUpload_InvalidZipMode
