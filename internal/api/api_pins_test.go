package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

func TestAPI_listPins(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		token, _, _, _ := helper.SetupAuthenticatedTest()

		rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/pins", token, nil)

		assert.Equal(t, http.StatusOK, rec.Code)
		var response dto.PinResultsResponse
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.NotEmpty(t, response.Results)
	}, TestOptions)
}

func TestAPI_addPin(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		helper.SetupWorkflowServiceMock()
		token, _, _, _ := helper.SetupAuthenticatedTest()

		reqBody := fmt.Sprintf(`{"cid":"%s","name":"test"}`, TestCID)
		rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/pins", token, []byte(reqBody))

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
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		token, _, _, pinID := helper.SetupAuthenticatedTest()

		rec := helper.makeAuthenticatedRequest(http.MethodGet, fmt.Sprintf("/api/pins/%s", pinID.String()), token, nil)

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
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		helper.SetupWorkflowServiceMock()
		token, _, _, pinID := helper.SetupAuthenticatedTest()

		reqBody := fmt.Sprintf(`{"cid":"%s","name":"test"}`, TestCID)
		rec := helper.makeAuthenticatedRequest(http.MethodPost, fmt.Sprintf("/api/pins/%s", pinID.String()), token, []byte(reqBody))

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
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		token, _, _, pinID := helper.SetupAuthenticatedTest()

		rec := helper.makeAuthenticatedRequest(http.MethodDelete, fmt.Sprintf("/api/pins/%s", pinID.String()), token, nil)

		assert.Equal(t, http.StatusAccepted, rec.Code)
	}, TestOptions)
}
