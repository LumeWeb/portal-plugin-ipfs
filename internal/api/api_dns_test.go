package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	mocks "go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"gorm.io/gorm"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/portal/core"
)

const (
	// TestZoneDomain is a domain name for testing DNS zones
	TestZoneDomain = "example.com"

	// TestZoneID is a zone ID for testing
	TestZoneID = uint(1)

	// TestZoneID2 is a second zone ID for testing
	TestZoneID2 = uint(2)
)

// createMockDNSZone creates a standardized DNSZone mock object
func createMockDNSZone(id, userID uint, domain string, status db.DNSZoneStatus, powerDNSZoneID string) *db.DNSZone {
	now := time.Now()
	return &db.DNSZone{
		Model:                  gorm.Model{ID: id, CreatedAt: now, UpdatedAt: now},
		UserID:                 userID,
		Domain:                 domain,
		Status:                 string(status),
		PowerDNSZoneID:         powerDNSZoneID,
		NameserversVerifiedAt:  &now,
	}
}

// DNS Zone API Tests

func TestAPI_CreateZone(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockDNSService := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

			mockZone := createMockDNSZone(TestZoneID, userID, TestZoneDomain, db.DNSZoneStatusPendingNameserver, "pdns-123")

			mockDNSService.EXPECT().CreateZone(mock.Anything, TestZoneDomain, userID).Return(mockZone, nil)

			reqBody := fmt.Sprintf(`{"domain":"%s"}`, TestZoneDomain)
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/dns/zones", token, []byte(reqBody))

			assert.Equal(t, http.StatusCreated, rec.Code)

			var response dto.ZoneResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, TestZoneID, response.ID)
			assert.Equal(t, TestZoneDomain, response.Domain)
			assert.Equal(t, userID, response.UserID)
		}, TestOptions)
	})

	t.Run("success_with_nameservers", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockDNSService := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

			mockZone := createMockDNSZone(TestZoneID, userID, TestZoneDomain, db.DNSZoneStatusPendingNameserver, "pdns-123")

			mockDNSService.EXPECT().CreateZone(mock.Anything, TestZoneDomain, userID).Return(mockZone, nil)

			reqBody := fmt.Sprintf(`{"domain":"%s","nameservers":["ns1.example.com","ns2.example.com"]}`, TestZoneDomain)
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/dns/zones", token, []byte(reqBody))

			assert.Equal(t, http.StatusCreated, rec.Code)

			var response dto.ZoneResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, TestZoneID, response.ID)
		}, TestOptions)
	})

	t.Run("error_unauthenticated", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			reqBody := fmt.Sprintf(`{"domain":"%s"}`, TestZoneDomain)
			rec := helper.makeRequest(http.MethodPost, "/api/dns/zones", []byte(reqBody))

			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		}, TestOptions)
	})

	t.Run("error_invalid_request_empty_domain", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			reqBody := `{"domain":""}`
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/dns/zones", token, []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})

	t.Run("error_invalid_domain_format", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockDNSService := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

			mockDNSService.EXPECT().CreateZone(mock.Anything, "invalid..domain", userID).Return(nil, errors.New("invalid domain format"))

			reqBody := `{"domain":"invalid..domain"}`
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/dns/zones", token, []byte(reqBody))

			assert.Equal(t, http.StatusBadRequest, rec.Code)
		}, TestOptions)
	})

	t.Run("error_creation_failed", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockDNSService := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

			mockDNSService.EXPECT().CreateZone(mock.Anything, mock.AnythingOfType("string"), mock.AnythingOfType("uint")).Return(nil, errors.New("creation failed"))

			reqBody := fmt.Sprintf(`{"domain":"%s"}`, TestZoneDomain)
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/dns/zones", token, []byte(reqBody))

			assert.Equal(t, http.StatusBadRequest, rec.Code)
		}, TestOptions)
	})

	t.Run("error_domain_too_long", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			longDomain := "a." + strings.Repeat("a", 300) + ".com"
			reqBody := fmt.Sprintf(`{"domain":"%s"}`, longDomain)
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/dns/zones", token, []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})
}

func TestAPI_ListZones(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockDNSService := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

			mockZone1 := createMockDNSZone(TestZoneID, userID, TestZoneDomain, db.DNSZoneStatusActive, "pdns-123")
			mockZone2 := createMockDNSZone(TestZoneID2, userID, "test.com", db.DNSZoneStatusPendingNameserver, "pdns-456")

			mockDNSService.EXPECT().ListZones(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*db.DNSZone{mockZone1, mockZone2}, int64(2), nil)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/dns/zones", token, nil)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response struct {
				Data []dto.ZoneListResponse `json:"data"`
				Total int64 `json:"total"`
			}
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Len(t, response.Data, 2)
			assert.Equal(t, int64(2), response.Total)
		}, TestOptions)
	})

	t.Run("success_with_pagination", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockDNSService := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

			mockZone := createMockDNSZone(TestZoneID, userID, TestZoneDomain, db.DNSZoneStatusActive, "pdns-123")

			mockDNSService.EXPECT().ListZones(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*db.DNSZone{mockZone}, int64(10), nil)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/dns/zones?page=1&limit=1", token, nil)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response struct {
				Data []dto.ZoneListResponse `json:"data"`
				Total int64 `json:"total"`
			}
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Len(t, response.Data, 1)
			assert.Equal(t, int64(10), response.Total)
		}, TestOptions)
	})

	t.Run("success_empty_list", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockDNSService := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

			mockDNSService.EXPECT().ListZones(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*db.DNSZone{}, int64(0), nil)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/dns/zones", token, nil)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response struct {
				Data []dto.ZoneListResponse `json:"data"`
				Total int64 `json:"total"`
			}
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Len(t, response.Data, 0)
			assert.Equal(t, int64(0), response.Total)
		}, TestOptions)
	})

	t.Run("error_unauthenticated", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			rec := helper.makeRequest(http.MethodGet, "/api/dns/zones", nil)

			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		}, TestOptions)
	})

	t.Run("error_list_failed", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockDNSService := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

			mockDNSService.EXPECT().ListZones(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, int64(0), errors.New("list failed"))

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/dns/zones", token, nil)

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("success_with_filter_status", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockDNSService := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

			mockZone := createMockDNSZone(TestZoneID, userID, TestZoneDomain, db.DNSZoneStatusActive, "pdns-123")

			mockDNSService.EXPECT().ListZones(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*db.DNSZone{mockZone}, int64(1), nil)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/dns/zones?filter[status]=active", token, nil)

			assert.Equal(t, http.StatusOK, rec.Code)
		}, TestOptions)
	})
}

func TestAPI_GetZone(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockDNSService := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

			mockZone := createMockDNSZone(TestZoneID, userID, TestZoneDomain, db.DNSZoneStatusActive, "pdns-123")

			mockDNSService.EXPECT().GetZone(mock.Anything, TestZoneID).Return(mockZone, nil)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, fmt.Sprintf("/api/dns/zones/%d", TestZoneID), token, nil)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response dto.ZoneResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, TestZoneID, response.ID)
			assert.Equal(t, TestZoneDomain, response.Domain)
		}, TestOptions)
	})

	t.Run("error_unauthenticated", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			rec := helper.makeRequest(http.MethodGet, fmt.Sprintf("/api/dns/zones/%d", TestZoneID), nil)

			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		}, TestOptions)
	})

	t.Run("error_not_found", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockDNSService := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

			mockDNSService.EXPECT().GetZone(mock.Anything, TestZoneID).Return(nil, errors.New("not found"))

			rec := helper.makeAuthenticatedRequest(http.MethodGet, fmt.Sprintf("/api/dns/zones/%d", TestZoneID), token, nil)

			assert.Equal(t, http.StatusNotFound, rec.Code)
		}, TestOptions)
	})

	t.Run("error_permission_denied_different_user", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockDNSService := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

			otherUserID := userID + 1
			mockZone := createMockDNSZone(TestZoneID, otherUserID, TestZoneDomain, db.DNSZoneStatusActive, "pdns-123")

			mockDNSService.EXPECT().GetZone(mock.Anything, TestZoneID).Return(mockZone, nil)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, fmt.Sprintf("/api/dns/zones/%d", TestZoneID), token, nil)

			assert.Equal(t, http.StatusForbidden, rec.Code)
		}, TestOptions)
	})

	t.Run("error_invalid_zone_id", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/dns/zones/invalid", token, nil)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
		}, TestOptions)
	})
}

func TestAPI_UpdateZone(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockDNSService := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

			mockZone := createMockDNSZone(TestZoneID, userID, TestZoneDomain, db.DNSZoneStatusActive, "pdns-123")

			mockDNSService.EXPECT().GetZone(mock.Anything, TestZoneID).Return(mockZone, nil)

			reqBody := fmt.Sprintf(`{"domain":"%s"}`, TestZoneDomain)
			rec := helper.makeAuthenticatedRequest(http.MethodPut, fmt.Sprintf("/api/dns/zones/%d", TestZoneID), token, []byte(reqBody))

			assert.Equal(t, http.StatusOK, rec.Code)

			var response dto.ZoneResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, TestZoneID, response.ID)
			assert.Equal(t, TestZoneDomain, response.Domain)
		}, TestOptions)
	})

	t.Run("error_unauthenticated", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			reqBody := fmt.Sprintf(`{"domain":"%s"}`, TestZoneDomain)
			rec := helper.makeRequest(http.MethodPut, fmt.Sprintf("/api/dns/zones/%d", TestZoneID), []byte(reqBody))

			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		}, TestOptions)
	})

	t.Run("error_not_found", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockDNSService := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

			mockDNSService.EXPECT().GetZone(mock.Anything, TestZoneID).Return(nil, errors.New("not found"))

			reqBody := fmt.Sprintf(`{"domain":"%s"}`, TestZoneDomain)
			rec := helper.makeAuthenticatedRequest(http.MethodPut, fmt.Sprintf("/api/dns/zones/%d", TestZoneID), token, []byte(reqBody))

			assert.Equal(t, http.StatusNotFound, rec.Code)
		}, TestOptions)
	})

	t.Run("error_permission_denied_different_user", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockDNSService := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

			otherUserID := userID + 1
			mockZone := createMockDNSZone(TestZoneID, otherUserID, TestZoneDomain, db.DNSZoneStatusActive, "pdns-123")

			mockDNSService.EXPECT().GetZone(mock.Anything, TestZoneID).Return(mockZone, nil)

			reqBody := fmt.Sprintf(`{"domain":"%s"}`, TestZoneDomain)
			rec := helper.makeAuthenticatedRequest(http.MethodPut, fmt.Sprintf("/api/dns/zones/%d", TestZoneID), token, []byte(reqBody))

			assert.Equal(t, http.StatusForbidden, rec.Code)
		}, TestOptions)
	})

	t.Run("error_invalid_request_empty_domain", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			reqBody := `{"domain":""}`
			rec := helper.makeAuthenticatedRequest(http.MethodPut, fmt.Sprintf("/api/dns/zones/%d", TestZoneID), token, []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})

	t.Run("error_invalid_zone_id", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			reqBody := fmt.Sprintf(`{"domain":"%s"}`, TestZoneDomain)
			rec := helper.makeAuthenticatedRequest(http.MethodPut, "/api/dns/zones/invalid", token, []byte(reqBody))

			assert.Equal(t, http.StatusBadRequest, rec.Code)
		}, TestOptions)
	})
}

func TestAPI_DeleteZone(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockDNSService := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

			mockZone := createMockDNSZone(TestZoneID, userID, TestZoneDomain, db.DNSZoneStatusActive, "pdns-123")

			mockDNSService.EXPECT().GetZone(mock.Anything, TestZoneID).Return(mockZone, nil)
			mockDNSService.EXPECT().DeleteZone(mock.Anything, TestZoneID).Return(nil)

			rec := helper.makeAuthenticatedRequest(http.MethodDelete, fmt.Sprintf("/api/dns/zones/%d", TestZoneID), token, nil)

			assert.Equal(t, http.StatusNoContent, rec.Code)
		}, TestOptions)
	})

	t.Run("error_unauthenticated", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			rec := helper.makeRequest(http.MethodDelete, fmt.Sprintf("/api/dns/zones/%d", TestZoneID), nil)

			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		}, TestOptions)
	})

	t.Run("error_not_found", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockDNSService := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

			mockDNSService.EXPECT().GetZone(mock.Anything, TestZoneID).Return(nil, errors.New("not found"))

			rec := helper.makeAuthenticatedRequest(http.MethodDelete, fmt.Sprintf("/api/dns/zones/%d", TestZoneID), token, nil)

			assert.Equal(t, http.StatusNotFound, rec.Code)
		}, TestOptions)
	})

	t.Run("error_permission_denied_different_user", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockDNSService := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

			otherUserID := userID + 1
			mockZone := createMockDNSZone(TestZoneID, otherUserID, TestZoneDomain, db.DNSZoneStatusActive, "pdns-123")

			mockDNSService.EXPECT().GetZone(mock.Anything, TestZoneID).Return(mockZone, nil)

			rec := helper.makeAuthenticatedRequest(http.MethodDelete, fmt.Sprintf("/api/dns/zones/%d", TestZoneID), token, nil)

			assert.Equal(t, http.StatusForbidden, rec.Code)
		}, TestOptions)
	})

	t.Run("error_delete_failed", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockDNSService := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

			mockZone := createMockDNSZone(TestZoneID, userID, TestZoneDomain, db.DNSZoneStatusActive, "pdns-123")

			mockDNSService.EXPECT().GetZone(mock.Anything, TestZoneID).Return(mockZone, nil)
			mockDNSService.EXPECT().DeleteZone(mock.Anything, TestZoneID).Return(errors.New("delete failed"))

			rec := helper.makeAuthenticatedRequest(http.MethodDelete, fmt.Sprintf("/api/dns/zones/%d", TestZoneID), token, nil)

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("error_invalid_zone_id", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			rec := helper.makeAuthenticatedRequest(http.MethodDelete, "/api/dns/zones/invalid", token, nil)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
		}, TestOptions)
	})
}

func TestAPI_ValidateZone(t *testing.T) {
	t.Run("success_validation_passed", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockDNSService := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

			mockZone := createMockDNSZone(TestZoneID, userID, TestZoneDomain, db.DNSZoneStatusActive, "pdns-123")

			mockDNSService.EXPECT().GetZone(mock.Anything, TestZoneID).Return(mockZone, nil)
			mockDNSService.EXPECT().ValidateNameservers(mock.Anything, TestZoneID).Return(true, nil)

			rec := helper.makeAuthenticatedRequest(http.MethodPost, fmt.Sprintf("/api/dns/zones/%d/validate", TestZoneID), token, nil)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response dto.ValidationResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.True(t, response.Valid)
			assert.Equal(t, "Validation successful", response.Message)
		}, TestOptions)
	})

	t.Run("success_validation_failed", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockDNSService := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

			mockZone := createMockDNSZone(TestZoneID, userID, TestZoneDomain, db.DNSZoneStatusPendingNameserver, "pdns-123")

			mockDNSService.EXPECT().GetZone(mock.Anything, TestZoneID).Return(mockZone, nil)
			mockDNSService.EXPECT().ValidateNameservers(mock.Anything, TestZoneID).Return(false, nil)

			rec := helper.makeAuthenticatedRequest(http.MethodPost, fmt.Sprintf("/api/dns/zones/%d/validate", TestZoneID), token, nil)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response dto.ValidationResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.False(t, response.Valid)
			assert.Equal(t, "Validation failed", response.Message)
		}, TestOptions)
	})

	t.Run("error_unauthenticated", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			rec := helper.makeRequest(http.MethodPost, fmt.Sprintf("/api/dns/zones/%d/validate", TestZoneID), nil)

			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		}, TestOptions)
	})

	t.Run("error_not_found", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockDNSService := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

			mockDNSService.EXPECT().GetZone(mock.Anything, TestZoneID).Return(nil, errors.New("not found"))

			rec := helper.makeAuthenticatedRequest(http.MethodPost, fmt.Sprintf("/api/dns/zones/%d/validate", TestZoneID), token, nil)

			assert.Equal(t, http.StatusNotFound, rec.Code)
		}, TestOptions)
	})

	t.Run("error_permission_denied_different_user", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockDNSService := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

			otherUserID := userID + 1
			mockZone := createMockDNSZone(TestZoneID, otherUserID, TestZoneDomain, db.DNSZoneStatusActive, "pdns-123")

			mockDNSService.EXPECT().GetZone(mock.Anything, TestZoneID).Return(mockZone, nil)

			rec := helper.makeAuthenticatedRequest(http.MethodPost, fmt.Sprintf("/api/dns/zones/%d/validate", TestZoneID), token, nil)

			assert.Equal(t, http.StatusForbidden, rec.Code)
		}, TestOptions)
	})

	t.Run("error_validation_service_failed", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockDNSService := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

			mockZone := createMockDNSZone(TestZoneID, userID, TestZoneDomain, db.DNSZoneStatusActive, "pdns-123")

			mockDNSService.EXPECT().GetZone(mock.Anything, TestZoneID).Return(mockZone, nil)
			mockDNSService.EXPECT().ValidateNameservers(mock.Anything, TestZoneID).Return(false, errors.New("validation failed"))

			rec := helper.makeAuthenticatedRequest(http.MethodPost, fmt.Sprintf("/api/dns/zones/%d/validate", TestZoneID), token, nil)

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("error_invalid_zone_id", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/dns/zones/invalid/validate", token, nil)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
		}, TestOptions)
	})
}

func TestAPI_GetZoneStatus(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockDNSService := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

			mockZone := createMockDNSZone(TestZoneID, userID, TestZoneDomain, db.DNSZoneStatusActive, "pdns-123")

			mockDNSService.EXPECT().GetZone(mock.Anything, TestZoneID).Return(mockZone, nil)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, fmt.Sprintf("/api/dns/zones/%d/status", TestZoneID), token, nil)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response dto.ZoneResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, TestZoneID, response.ID)
			assert.Equal(t, TestZoneDomain, response.Domain)
			assert.Equal(t, string(db.DNSZoneStatusActive), response.Status)
		}, TestOptions)
	})

	t.Run("error_unauthenticated", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			rec := helper.makeRequest(http.MethodGet, fmt.Sprintf("/api/dns/zones/%d/status", TestZoneID), nil)

			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		}, TestOptions)
	})

	t.Run("error_not_found", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockDNSService := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

			mockDNSService.EXPECT().GetZone(mock.Anything, TestZoneID).Return(nil, errors.New("not found"))

			rec := helper.makeAuthenticatedRequest(http.MethodGet, fmt.Sprintf("/api/dns/zones/%d/status", TestZoneID), token, nil)

			assert.Equal(t, http.StatusNotFound, rec.Code)
		}, TestOptions)
	})

	t.Run("error_permission_denied_different_user", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockDNSService := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)

			otherUserID := userID + 1
			mockZone := createMockDNSZone(TestZoneID, otherUserID, TestZoneDomain, db.DNSZoneStatusActive, "pdns-123")

			mockDNSService.EXPECT().GetZone(mock.Anything, TestZoneID).Return(mockZone, nil)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, fmt.Sprintf("/api/dns/zones/%d/status", TestZoneID), token, nil)

			assert.Equal(t, http.StatusForbidden, rec.Code)
		}, TestOptions)
	})

	t.Run("error_invalid_zone_id", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/dns/zones/invalid/status", token, nil)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
		}, TestOptions)
	})
}
