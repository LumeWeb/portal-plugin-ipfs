package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	pluginCoreCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

// AdminTestOptions provides test configuration for admin extension tests
var AdminTestOptions = coreTesting.CombineOptions(
	testPluginOptions,
	coreTesting.WithAPIExtension(NewAdminExtension()),
	coreTesting.WithAPIID("admin"),
)

func TestAdminExtension_TargetAPI(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		extFactory := NewAdminExtension()
		ext, _, err := extFactory()
		assert.NoError(t, err)

		adminExt := ext.(*AdminExtension)

		// Assert
		assert.Equal(t, "admin", adminExt.TargetAPI())
	}, AdminTestOptions)
}

func TestAdminExtension_BlockWebsite_Success(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCoreCore.WEBSITE_SERVICE)
		websiteID := uint(123)

		// Mock the BlockWebsite call
		websiteService.EXPECT().BlockWebsite(mock.Anything, websiteID).Return(nil)

		// Create test request using context helper
		req := ctx.NewAPIRequest(http.MethodPost, "/api/ipfs/websites/123/block", nil)
		rec := httptest.NewRecorder()

		// Act
		ctx.Router().ServeHTTP(rec, req)

		// Assert
		assert.Equal(t, http.StatusOK, rec.Code)
	}, AdminTestOptions)
}

func TestAdminExtension_BlockWebsite_InvalidID(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		// Create test request with invalid ID
		req := ctx.NewAPIRequest(http.MethodPost, "/api/ipfs/websites/invalid/block", nil)
		rec := httptest.NewRecorder()

		// Act
		ctx.Router().ServeHTTP(rec, req)

		// Assert
		assert.NotEqual(t, http.StatusOK, rec.Code)
	}, AdminTestOptions)
}

func TestAdminExtension_BlockWebsite_ServiceError(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCoreCore.WEBSITE_SERVICE)
		websiteID := uint(123)

		// Mock the BlockWebsite call to return an error
		websiteService.EXPECT().BlockWebsite(mock.Anything, websiteID).Return(assert.AnError)

		// Create test request
		req := ctx.NewAPIRequest(http.MethodPost, "/api/ipfs/websites/123/block", nil)
		rec := httptest.NewRecorder()

		// Act
		ctx.Router().ServeHTTP(rec, req)

		// Assert
		assert.NotEqual(t, http.StatusOK, rec.Code)
	}, AdminTestOptions)
}

func TestAdminExtension_UnblockWebsite_Success(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCoreCore.WEBSITE_SERVICE)
		websiteID := uint(123)

		// Mock the UnblockWebsite call
		websiteService.EXPECT().UnblockWebsite(mock.Anything, websiteID).Return(nil)

		// Create test request using context helper
		req := ctx.NewAPIRequest(http.MethodPost, "/api/ipfs/websites/123/unblock", nil)
		rec := httptest.NewRecorder()

		// Act
		ctx.Router().ServeHTTP(rec, req)

		// Assert
		assert.Equal(t, http.StatusOK, rec.Code)
	}, AdminTestOptions)
}

func TestAdminExtension_UnblockWebsite_InvalidID(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		// Create test request with invalid ID
		req := ctx.NewAPIRequest(http.MethodPost, "/api/ipfs/websites/invalid/unblock", nil)
		rec := httptest.NewRecorder()

		// Act
		ctx.Router().ServeHTTP(rec, req)

		// Assert
		assert.NotEqual(t, http.StatusOK, rec.Code)
	}, AdminTestOptions)
}

func TestAdminExtension_UnblockWebsite_ServiceError(t *testing.T) {
	coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCoreCore.WEBSITE_SERVICE)
		websiteID := uint(123)

		// Mock the UnblockWebsite call to return an error
		websiteService.EXPECT().UnblockWebsite(mock.Anything, websiteID).Return(assert.AnError)

		// Create test request
		req := ctx.NewAPIRequest(http.MethodPost, "/api/ipfs/websites/123/unblock", nil)
		rec := httptest.NewRecorder()

		// Act
		ctx.Router().ServeHTTP(rec, req)

		// Assert
		assert.NotEqual(t, http.StatusOK, rec.Code)
	}, AdminTestOptions)
}
