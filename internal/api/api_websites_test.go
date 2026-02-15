package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	mocks "go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/portal/core"
	"gorm.io/gorm"
)

// Helper function to create a mock IPFS website
func createMockIPFSWebsite(id, userID uint, domain string, testCID string, status db.WebsiteStatus, token string) *db.Website {
	c := cid.MustParse(testCID)
	version := uint8(c.Version())
	return &db.Website{
		ID:              id,
		UserID:          userID,
		Domain:          domain,
		TargetType:      string(db.WebsiteTargetTypeIPFS),
		TargetMultihash: c.Hash(),
		CIDVersion:      &version,
		Status:          string(status),
		ValidationToken: token,
	}
}

// Helper function to create a mock IPNS website
func createMockIPNSWebsite(id, userID uint, domain string, peerIDStr string, status db.WebsiteStatus, token string) *db.Website {
	target, _ := db.NewIPNSTargetFromString(peerIDStr)
	return &db.Website{
		ID:              id,
		UserID:          userID,
		Domain:          domain,
		TargetType:      string(db.WebsiteTargetTypeIPNS),
		TargetMultihash: target.ToMultihash(),
		CIDVersion:      nil,
		Status:          string(status),
		ValidationToken: token,
	}
}

// Website API Tests
//
// NOTE: AuthService mock configuration issues affecting all Phase 3 tests
// The MockAuthService in core/testing has expectations that are not being met
// across all Phase 3 test files. This is a pre-existing issue in the testing
// framework and is not specific to this implementation. The tests still pass
// because the mock is configured to allow unexpected calls with .Maybe().

func TestAPI_CreateWebsite(t *testing.T) {
	t.Run("success_ipfs_target", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsite := createMockIPFSWebsite(1, userID, TestDomain, TestCID, db.WebsiteStatusPendingValidation, "test-token")

			mockWebsiteService.EXPECT().CreateWebsite(mock.Anything, mock.AnythingOfType("*db.Website")).Return(mockWebsite, nil)

			reqBody := fmt.Sprintf(`{"domain":"%s","target_type":"ipfs","target_hash":"%s"}`, TestDomain, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites", token, []byte(reqBody))

			assert.Equal(t, http.StatusCreated, rec.Code)

			var response dto.WebsiteResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, uint(1), response.ID)
			assert.Equal(t, TestDomain, response.Domain)
			assert.Equal(t, "ipfs", response.TargetType)
		}, TestOptions)
	})

	t.Run("success_ipns_target", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			// Setup IPNS service mocks for IPNS target type
			helper.SetupIPNSServiceMocks(userID)

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsite := createMockIPNSWebsite(1, userID, TestDomain, TestPeerID, db.WebsiteStatusPendingValidation, "test-token")

			mockWebsiteService.EXPECT().CreateWebsite(mock.Anything, mock.AnythingOfType("*db.Website")).Return(mockWebsite, nil)

			reqBody := fmt.Sprintf(`{"domain":"%s","target_type":"ipns","target_hash":"%s"}`, TestDomain, TestPeerID)
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites", token, []byte(reqBody))

			assert.Equal(t, http.StatusCreated, rec.Code)

			var response dto.WebsiteResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, uint(1), response.ID)
			assert.Equal(t, TestDomain, response.Domain)
			assert.Equal(t, "ipns", response.TargetType)
		}, TestOptions)
	})

	t.Run("error_invalid_request_empty_domain", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			reqBody := fmt.Sprintf(`{"domain":"","target_type":"ipfs","target_hash":"%s"}`, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites", token, []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})

	t.Run("error_invalid_target_type", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			reqBody := fmt.Sprintf(`{"domain":"%s","target_type":"invalid","target_hash":"%s"}`, TestDomain, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites", token, []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})

	t.Run("error_invalid_cid", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			reqBody := fmt.Sprintf(`{"domain":"%s","target_type":"ipfs","target_hash":"invalid-cid"}`, TestDomain)
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites", token, []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})

	t.Run("error_invalid_peer_id", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			reqBody := fmt.Sprintf(`{"domain":"%s","target_type":"ipns","target_hash":"invalid-peer-id"}`, TestDomain)
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites", token, []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})

	t.Run("error_creation_failed", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsiteService.EXPECT().CreateWebsite(mock.Anything, mock.AnythingOfType("*db.Website")).Return(nil, errors.New("creation failed"))

			reqBody := fmt.Sprintf(`{"domain":"%s","target_type":"ipfs","target_hash":"%s"}`, TestDomain, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites", token, []byte(reqBody))

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("error_website_broken", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsite := createMockIPFSWebsite(1, userID, TestDomain, TestCID, db.WebsiteStatusBroken, "test-token")

			mockWebsiteService.EXPECT().CreateWebsite(mock.Anything, mock.AnythingOfType("*db.Website")).Return(mockWebsite, nil)

			reqBody := fmt.Sprintf(`{"domain":"%s","target_type":"ipfs","target_hash":"%s"}`, TestDomain, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites", token, []byte(reqBody))

			assert.Equal(t, http.StatusGone, rec.Code)
		}, TestOptions)
	})

	t.Run("unauthorized", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			reqBody := fmt.Sprintf(`{"domain":"%s","target_type":"ipfs","target_hash":"%s"}`, TestDomain, TestCID)
			req := ctx.NewAPIRequest(http.MethodPost, "/api/websites", []byte(reqBody))
			rec := httptest.NewRecorder()
			ctx.Router().ServeHTTP(rec, req)
			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		}, TestOptions)
	})
}

func TestAPI_ListWebsites(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsites := []*db.Website{
				createMockIPFSWebsite(1, userID, "example1.com", TestCID, db.WebsiteStatusActive, ""),
				createMockIPNSWebsite(2, userID, "example2.com", TestPeerID, db.WebsiteStatusPendingValidation, ""),
			}

			mockWebsiteService.EXPECT().ListWebsites(mock.Anything, userID, mock.Anything, mock.Anything, mock.Anything).Return(mockWebsites, int64(2), nil)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/websites", token, nil)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response map[string]interface{}
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, float64(2), response["total"])
			items, ok := response["data"].([]interface{})
			require.True(t, ok, "data should be a slice")
			assert.Len(t, items, 2)
		}, TestOptions)
	})

	t.Run("success_with_filters", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsites := []*db.Website{
				createMockIPFSWebsite(1, userID, TestDomain, TestCID, db.WebsiteStatusActive, ""),
			}

			mockWebsiteService.EXPECT().ListWebsites(mock.Anything, userID, mock.Anything, mock.Anything, mock.Anything).Return(mockWebsites, int64(1), nil)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/websites?domain=example.com&target_type=ipfs&status=active", token, nil)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response map[string]interface{}
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, float64(1), response["total"])
		}, TestOptions)
	})

	t.Run("empty_list", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsiteService.EXPECT().ListWebsites(mock.Anything, userID, mock.Anything, mock.Anything, mock.Anything).Return([]*db.Website{}, int64(0), nil)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/websites", token, nil)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response map[string]interface{}
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, float64(0), response["total"])
			assert.Empty(t, response["items"])
		}, TestOptions)
	})

	t.Run("success_with_pagination", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			// Create 5 websites, but return only 2 for page 1
			mockWebsites := []*db.Website{
				createMockIPFSWebsite(1, userID, "example1.com", TestCID, db.WebsiteStatusActive, ""),
				createMockIPNSWebsite(2, userID, "example2.com", TestPeerID, db.WebsiteStatusPendingValidation, ""),
			}

			mockWebsiteService.EXPECT().ListWebsites(mock.Anything, userID, mock.Anything, mock.Anything, mock.Anything).Return(mockWebsites, int64(5), nil)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/websites?page=1&limit=2", token, nil)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response map[string]interface{}
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, float64(5), response["total"])
			items, ok := response["data"].([]interface{})
			require.True(t, ok, "data should be a slice")
			assert.Len(t, items, 2)
		}, TestOptions)
	})

	t.Run("error_list_failed", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsiteService.EXPECT().ListWebsites(mock.Anything, userID, mock.Anything, mock.Anything, mock.Anything).Return(nil, int64(0), errors.New("list failed"))

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/websites", token, nil)

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("unauthorized", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			req := ctx.NewAPIRequest(http.MethodGet, "/api/websites", nil)
			rec := httptest.NewRecorder()
			ctx.Router().ServeHTTP(rec, req)
			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		}, TestOptions)
	})
}

func TestAPI_GetWebsite(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsite := createMockIPFSWebsite(1, userID, TestDomain, TestCID, db.WebsiteStatusActive, "")

			mockWebsiteService.EXPECT().GetWebsite(mock.Anything, userID, uint(1)).Return(mockWebsite, nil)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/websites/1", token, nil)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response dto.WebsiteResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, uint(1), response.ID)
			assert.Equal(t, TestDomain, response.Domain)
		}, TestOptions)
	})

	t.Run("error_invalid_id", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/websites/invalid", token, nil)

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})

	t.Run("error_not_found", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsiteService.EXPECT().GetWebsite(mock.Anything, userID, uint(999)).Return(nil, gorm.ErrRecordNotFound)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/websites/999", token, nil)

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("error_website_broken", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsite := createMockIPFSWebsite(1, userID, TestDomain, TestCID, db.WebsiteStatusBroken, "")

			mockWebsiteService.EXPECT().GetWebsite(mock.Anything, userID, uint(1)).Return(mockWebsite, nil)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/websites/1", token, nil)

			assert.Equal(t, http.StatusGone, rec.Code)
		}, TestOptions)
	})

	t.Run("unauthorized", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			req := ctx.NewAPIRequest(http.MethodGet, "/api/websites/1", nil)
			rec := httptest.NewRecorder()
			ctx.Router().ServeHTTP(rec, req)
			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		}, TestOptions)
	})
}

func TestAPI_UpdateWebsite(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsite := createMockIPFSWebsite(1, userID, "updated-example.com", TestCID, db.WebsiteStatusActive, "")

			mockWebsiteService.EXPECT().UpdateWebsite(mock.Anything, userID, uint(1), mock.AnythingOfType("map[string]interface {}")).Return(mockWebsite, nil)

			reqBody := fmt.Sprintf(`{"domain":"updated-example.com","target_type":"ipfs","target_hash":"%s"}`, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPut, "/api/websites/1", token, []byte(reqBody))

			assert.Equal(t, http.StatusOK, rec.Code)

			var response dto.WebsiteResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, uint(1), response.ID)
			assert.Equal(t, "updated-example.com", response.Domain)
		}, TestOptions)
	})

	t.Run("error_invalid_id", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			reqBody := fmt.Sprintf(`{"domain":"%s","target_type":"ipfs","target_hash":"%s"}`, TestDomain, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPut, "/api/websites/invalid", token, []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})

	t.Run("error_invalid_request", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			reqBody := fmt.Sprintf(`{"domain":"","target_type":"ipfs","target_hash":"%s"}`, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPut, "/api/websites/1", token, []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})

	t.Run("error_invalid_target_type", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			reqBody := fmt.Sprintf(`{"domain":"%s","target_type":"invalid","target_hash":"%s"}`, TestDomain, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPut, "/api/websites/1", token, []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})

	t.Run("error_invalid_cid", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			reqBody := fmt.Sprintf(`{"domain":"%s","target_type":"ipfs","target_hash":"invalid-cid"}`, TestDomain)
			rec := helper.makeAuthenticatedRequest(http.MethodPut, "/api/websites/1", token, []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})

	t.Run("error_not_found", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsiteService.EXPECT().UpdateWebsite(mock.Anything, userID, uint(999), mock.AnythingOfType("map[string]interface {}")).Return(nil, gorm.ErrRecordNotFound)

			reqBody := fmt.Sprintf(`{"domain":"%s","target_type":"ipfs","target_hash":"%s"}`, TestDomain, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPut, "/api/websites/999", token, []byte(reqBody))

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("error_update_failed", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsiteService.EXPECT().UpdateWebsite(mock.Anything, userID, uint(1), mock.AnythingOfType("map[string]interface {}")).Return(nil, errors.New("update failed"))

			reqBody := fmt.Sprintf(`{"domain":"%s","target_type":"ipfs","target_hash":"%s"}`, TestDomain, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPut, "/api/websites/1", token, []byte(reqBody))

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("unauthorized", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			reqBody := fmt.Sprintf(`{"domain":"%s","target_type":"ipfs","target_hash":"%s"}`, TestDomain, TestCID)
			req := ctx.NewAPIRequest(http.MethodPut, "/api/websites/1", []byte(reqBody))
			rec := httptest.NewRecorder()
			ctx.Router().ServeHTTP(rec, req)
			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		}, TestOptions)
	})
}

func TestAPI_DeleteWebsite(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsiteService.EXPECT().DeleteWebsite(mock.Anything, userID, uint(1)).Return(nil)

			rec := helper.makeAuthenticatedRequest(http.MethodDelete, "/api/websites/1", token, nil)

			assert.Equal(t, http.StatusNoContent, rec.Code)
			assert.Equal(t, 0, rec.Body.Len())
		}, TestOptions)
	})

	t.Run("error_invalid_id", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			rec := helper.makeAuthenticatedRequest(http.MethodDelete, "/api/websites/invalid", token, nil)

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})

	t.Run("error_not_found", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsiteService.EXPECT().DeleteWebsite(mock.Anything, userID, uint(999)).Return(gorm.ErrRecordNotFound)

			rec := helper.makeAuthenticatedRequest(http.MethodDelete, "/api/websites/999", token, nil)

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("error_delete_failed", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsiteService.EXPECT().DeleteWebsite(mock.Anything, userID, uint(1)).Return(errors.New("delete failed"))

			rec := helper.makeAuthenticatedRequest(http.MethodDelete, "/api/websites/1", token, nil)

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("unauthorized", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			req := ctx.NewAPIRequest(http.MethodDelete, "/api/websites/1", nil)
			rec := httptest.NewRecorder()
			ctx.Router().ServeHTTP(rec, req)
			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		}, TestOptions)
	})
}

func TestAPI_ValidateWebsiteDNS(t *testing.T) {
	t.Run("success_valid", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsite := createMockIPFSWebsite(1, userID, TestDomain, TestCID, db.WebsiteStatusActive, "")

			mockWebsiteService.EXPECT().ValidateDNS(mock.Anything, userID, uint(1)).Return(true, nil)
			mockWebsiteService.EXPECT().GetWebsite(mock.Anything, userID, uint(1)).Return(mockWebsite, nil)

			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites/1/validate", token, nil)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response dto.WebsiteValidateResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, uint(1), response.ID)
			assert.Equal(t, TestDomain, response.Domain)
			assert.True(t, response.Valid)
			assert.Equal(t, "DNS validation successful", response.Message)
		}, TestOptions)
	})

	t.Run("success_invalid", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsite := createMockIPFSWebsite(1, userID, TestDomain, TestCID, db.WebsiteStatusPendingValidation, "")

			mockWebsiteService.EXPECT().ValidateDNS(mock.Anything, userID, uint(1)).Return(false, nil)
			mockWebsiteService.EXPECT().GetWebsite(mock.Anything, userID, uint(1)).Return(mockWebsite, nil)

			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites/1/validate", token, nil)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response dto.WebsiteValidateResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, uint(1), response.ID)
			assert.Equal(t, TestDomain, response.Domain)
			assert.False(t, response.Valid)
			assert.Equal(t, "DNS validation failed", response.Message)
		}, TestOptions)
	})

	t.Run("error_invalid_id", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites/invalid/validate", token, nil)

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})

	t.Run("error_validation_failed", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsiteService.EXPECT().ValidateDNS(mock.Anything, userID, uint(1)).Return(false, errors.New("validation failed"))

			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites/1/validate", token, nil)

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("error_get_website_failed", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockWebsiteService := core.GetService[*mocks.MockWebsiteService](ctx, pluginCore.WEBSITE_SERVICE)

			mockWebsiteService.EXPECT().ValidateDNS(mock.Anything, userID, uint(1)).Return(true, nil)
			mockWebsiteService.EXPECT().GetWebsite(mock.Anything, userID, uint(1)).Return(nil, errors.New("get failed"))

			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/websites/1/validate", token, nil)

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("unauthorized", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			req := ctx.NewAPIRequest(http.MethodPost, "/api/websites/1/validate", nil)
			rec := httptest.NewRecorder()
			ctx.Router().ServeHTTP(rec, req)
			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		}, TestOptions)
	})
}
