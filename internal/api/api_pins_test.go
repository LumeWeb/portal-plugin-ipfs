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

		rec := helper.makeAuthenticatedRequest(http.MethodGet, "/pins", token, nil)

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
		rec := helper.makeAuthenticatedRequest(http.MethodPost, "/pins", token, []byte(reqBody))

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

		rec := helper.makeAuthenticatedRequest(http.MethodGet, fmt.Sprintf("/pins/%s", pinID.String()), token, nil)

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
		rec := helper.makeAuthenticatedRequest(http.MethodPost, fmt.Sprintf("/pins/%s", pinID.String()), token, []byte(reqBody))

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

		rec := helper.makeAuthenticatedRequest(http.MethodDelete, fmt.Sprintf("/pins/%s", pinID.String()), token, nil)

		assert.Equal(t, http.StatusAccepted, rec.Code)
	}, TestOptions)
}

func TestAPI_listPins_requiresAuthentication(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)

		rec := helper.makeRequest(http.MethodGet, "/pins", nil)

		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	}, TestOptions)
}

func TestAPI_listPins_pathIsolation(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)
		token, _, _, _ := helper.SetupAuthenticatedTest()

		// Verify root-level path works
		rec := helper.makeAuthenticatedRequest(http.MethodGet, "/pins", token, nil)
		assert.Equal(t, http.StatusOK, rec.Code)

		// Verify API group path does NOT work (ensures route isolation)
		rec2 := helper.makeAuthenticatedRequest(http.MethodGet, "/api/pins", token, nil)
		assert.Equal(t, http.StatusNotFound, rec2.Code)
	}, TestOptions)
}
