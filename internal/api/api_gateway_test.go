package api

// Gateway API Tests
// These tests verify gateway endpoint behavior with proper authentication
// The routes are registered at /internal/websites/:domain with GatewayAuth middleware

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"gorm.io/gorm"
)

// Helper function to create a test website with IPFS target
func createTestIPFSGatewayWebsite(id, userID uint, domain string, testCID cid.Cid, status pluginDb.WebsiteStatus) *pluginDb.Website {
	version := uint8(testCID.Version())
	return &pluginDb.Website{
		ID:              id,
		UserID:          userID,
		Domain:          domain,
		TargetType:      string(pluginDb.WebsiteTargetTypeIPFS),
		TargetMultihash: testCID.Hash(),
		CIDVersion:      &version,
		Status:          string(status),
		CreatedAt:       time.Now(),
	}
}

// Helper function to create a deleted test website
func createTestDeletedIPFSGatewayWebsite(id, userID uint, domain string, testCID cid.Cid, status pluginDb.WebsiteStatus, deletedTime time.Time) *pluginDb.Website {
	version := uint8(testCID.Version())
	return &pluginDb.Website{
		ID:              id,
		UserID:          userID,
		Domain:          domain,
		TargetType:      string(pluginDb.WebsiteTargetTypeIPFS),
		TargetMultihash: testCID.Hash(),
		CIDVersion:      &version,
		Status:          string(status),
		CreatedAt:       time.Now(),
		DeletedAt:       gorm.DeletedAt{Time: deletedTime, Valid: true},
	}
}

func TestAPI_GetGatewayWebsite(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		helper := newMockHelper(t, ctx)
		domain := TestDomain
		testCID := cid.MustParse(TestCID)

		website := createTestIPFSGatewayWebsite(1, 1, domain, testCID, pluginDb.WebsiteStatusActive)

		helper.SetupWebsiteServiceMocks(domain, website)

		req := ctx.NewAPIRequest(http.MethodGet, "/internal/websites/"+domain, nil)
		req.Header.Set("X-Gateway-Secret", "test-secret")

		// Act
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Assert
		assert.Equal(t, http.StatusOK, rec.Code, "Should return 200 OK for valid website")
		
		var response map[string]interface{}
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		require.NoError(t, err)
		assert.Equal(t, domain, response["domain"])
		assert.Equal(t, "ipfs", response["target_type"])
	}, TestOptions)
}

func TestAPI_GetGatewayWebsite_MissingSecret(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		domain := TestDomain
		req := ctx.NewAPIRequest(http.MethodGet, "/internal/websites/"+domain, nil)
		// Missing X-Gateway-Secret header

		// Act
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Assert
		assert.Equal(t, http.StatusUnauthorized, rec.Code, "Should return 401 Unauthorized without secret")
		
		var response map[string]interface{}
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		require.NoError(t, err)
		assert.Contains(t, response["error"], "Unauthorized")
	}, TestOptions)
}

func TestAPI_GetGatewayWebsite_InvalidSecret(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		domain := TestDomain
		req := ctx.NewAPIRequest(http.MethodGet, "/internal/websites/"+domain, nil)
		req.Header.Set("X-Gateway-Secret", "invalid-secret")

		// Act
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Assert
		assert.Equal(t, http.StatusUnauthorized, rec.Code, "Should return 401 Unauthorized with invalid secret")
		
		var response map[string]interface{}
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		require.NoError(t, err)
		assert.Contains(t, response["error"], "Unauthorized")
	}, TestOptions)
}

func TestAPI_GetGatewayWebsite_BrokenSite(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		helper := newMockHelper(t, ctx)
		domain := "broken.example.com"
		testCID := cid.MustParse(TestCID)

		website := createTestIPFSGatewayWebsite(2, 1, domain, testCID, pluginDb.WebsiteStatusBroken)

		helper.SetupWebsiteServiceMocks(domain, website)
		
		req := ctx.NewAPIRequest(http.MethodGet, "/internal/websites/"+domain, nil)
		req.Header.Set("X-Gateway-Secret", "test-secret")

		// Act
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Assert
		assert.Equal(t, http.StatusGone, rec.Code, "Should return 410 Gone for broken website")
		
		var response map[string]interface{}
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		require.NoError(t, err)
		assert.Equal(t, domain, response["domain"])
		assert.Equal(t, "broken", response["status"])
	}, TestOptions)
}

func TestAPI_GetGatewayWebsite_DeletedSite(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		helper := newMockHelper(t, ctx)
		domain := "deleted.example.com"
		testCID := cid.MustParse(TestCID)

		deletedTime := time.Now()
		website := createTestDeletedIPFSGatewayWebsite(3, 1, domain, testCID, pluginDb.WebsiteStatusActive, deletedTime)

		helper.SetupWebsiteServiceMocks(domain, website)
		req := ctx.NewAPIRequest(http.MethodGet, "/internal/websites/"+domain, nil)
		req.Header.Set("X-Gateway-Secret", "test-secret")

		// Act
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Assert
		assert.Equal(t, http.StatusGone, rec.Code, "Should return 410 Gone for deleted website")
		
		var response map[string]interface{}
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		require.NoError(t, err)
		assert.Equal(t, domain, response["domain"])
	}, TestOptions)
}

func TestAPI_GetGatewayWebsite_NotFound(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		helper := newMockHelper(t, ctx)
		domain := "nonexistent.example.com"
		
		helper.SetupWebsiteServiceMocks(domain, nil)
		
		req := ctx.NewAPIRequest(http.MethodGet, "/internal/websites/"+domain, nil)
		req.Header.Set("X-Gateway-Secret", "test-secret")

		// Act
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Assert
		assert.Equal(t, http.StatusNotFound, rec.Code, "Should return 404 Not Found for nonexistent website")
	}, TestOptions)
}

func TestAPI_GetGatewayWebsiteStatus(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		helper := newMockHelper(t, ctx)
		domain := TestDomain
		testCID := cid.MustParse(TestCID)

		website := createTestIPFSGatewayWebsite(1, 1, domain, testCID, pluginDb.WebsiteStatusActive)

		helper.SetupWebsiteServiceMocks(domain, website)
		
		req := ctx.NewAPIRequest(http.MethodGet, fmt.Sprintf("/internal/websites/%s/status", domain), nil)
		req.Header.Set("X-Gateway-Secret", "test-secret")

		// Act
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Assert
		assert.Equal(t, http.StatusOK, rec.Code, "Should return 200 OK for status check")
		
		var response map[string]interface{}
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		require.NoError(t, err)
		assert.Equal(t, domain, response["domain"])
		assert.Equal(t, "active", response["status"])
	}, TestOptions)
}

func TestAPI_GetGatewayWebsiteStatus_BrokenSite(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		helper := newMockHelper(t, ctx)
		domain := "broken.example.com"
		testCID := cid.MustParse(TestCID)

		website := createTestIPFSGatewayWebsite(2, 1, domain, testCID, pluginDb.WebsiteStatusBroken)

		helper.SetupWebsiteServiceMocks(domain, website)
		helper.SetupWebsiteServiceMocks(domain, website)
		
		req := ctx.NewAPIRequest(http.MethodGet, fmt.Sprintf("/internal/websites/%s/status", domain), nil)
		req.Header.Set("X-Gateway-Secret", "test-secret")

		// Act
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Assert
		assert.Equal(t, http.StatusGone, rec.Code, "Should return 410 Gone for broken website status")
		
		var response map[string]interface{}
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		require.NoError(t, err)
		assert.Equal(t, domain, response["domain"])
		assert.Equal(t, "broken", response["status"])
	}, TestOptions)
}

func TestAPI_GetGatewayWebsiteStatus_NotFound(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		helper := newMockHelper(t, ctx)
		domain := "nonexistent.example.com"
		
		helper.SetupWebsiteServiceMocks(domain, nil)
		
		req := ctx.NewAPIRequest(http.MethodGet, fmt.Sprintf("/internal/websites/%s/status", domain), nil)
		req.Header.Set("X-Gateway-Secret", "test-secret")

		// Act
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Assert
		assert.Equal(t, http.StatusNotFound, rec.Code, "Should return 404 Not Found for nonexistent website")
	}, TestOptions)
}

func TestAPI_GetGatewayWebsite_ServiceError(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		helper := newMockHelper(t, ctx)
		domain := "error.example.com"
		
		mockWebsiteService := core.GetService[*mocks.MockWebsiteService](helper.ctx, pluginCore.WEBSITE_SERVICE)
		mockWebsiteService.EXPECT().GetWebsiteByDomain(mock.Anything, domain).Return(nil, errors.New("database error"))
		
		req := ctx.NewAPIRequest(http.MethodGet, "/internal/websites/"+domain, nil)
		req.Header.Set("X-Gateway-Secret", "test-secret")

		// Act
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Assert
		assert.Equal(t, http.StatusInternalServerError, rec.Code, "Should return 500 Internal Server Error on service error")
	}, TestOptions)
}
