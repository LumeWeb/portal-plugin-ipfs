package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"gorm.io/gorm"
)

// IPNS API Tests

func TestAPI_CreateIPNSKey(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockIPNSKeyService, _ := helper.SetupIPNSServiceMocks(userID)

			mockKey := &db.IPFSIPNSKey{
				ID:       1,
				UserID:   userID,
				Name:     "test-key",
				IPNSName: TestIPNSName,
				PeerID:   TestPeerID,
			}

			mockIPNSKeyService.EXPECT().CreateKey(mock.Anything, userID, "test-key", KeyTypeEd25519).Return(mockKey, nil)

			reqBody := `{"name":"test-key"}`
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/ipns/keys", token, []byte(reqBody))

			assert.Equal(t, http.StatusOK, rec.Code)

			var response dto.IPNSKeyResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, uint(1), response.ID)
			assert.Equal(t, "test-key", response.Name)
			assert.NotEmpty(t, response.PeerID)
		}, TestOptions)
	})

	t.Run("import_key_success", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockIPNSKeyService, _ := helper.SetupIPNSServiceMocksNoDefaults(userID)

			mockKey := &db.IPFSIPNSKey{
				ID:       1,
				UserID:   userID,
				Name:     "imported-key",
				IPNSName: TestIPNSName,
				PeerID:   TestPeerID,
			}

			mockIPNSKeyService.EXPECT().ImportKey(mock.Anything, userID, "imported-key", mock.AnythingOfType("string")).Return(mockKey, nil)

			reqBody := `{"name":"imported-key","key":"CAESQAoY8f9K8u0p9c0f1e2d3c4b5a6987654321fedcba"}`
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/ipns/keys", token, []byte(reqBody))

			assert.Equal(t, http.StatusOK, rec.Code)

			var response dto.IPNSKeyResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, uint(1), response.ID)
			assert.Equal(t, "imported-key", response.Name)
		}, TestOptions)
	})

	t.Run("error_invalid_request", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			reqBody := `{"name":""}`
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/ipns/keys", token, []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})

	t.Run("error_creation_failed", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockIPNSKeyService, _ := helper.SetupIPNSServiceMocksNoDefaults(userID)

			mockIPNSKeyService.EXPECT().CreateKey(mock.Anything, userID, "test-key", KeyTypeEd25519).Return(nil, errors.New("key creation failed"))

			reqBody := `{"name":"test-key"}`
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/ipns/keys", token, []byte(reqBody))

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("unauthorized", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			reqBody := `{"name":"test-key"}`
			req := ctx.NewAPIRequest(http.MethodPost, "/api/ipns/keys", []byte(reqBody))
			rec := httptest.NewRecorder()
			ctx.Router().ServeHTTP(rec, req)
			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		}, TestOptions)
	})
}

func TestAPI_ListIPNSKeys(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockIPNSKeyService, _ := helper.SetupIPNSServiceMocks(userID)

			mockKeys := []db.IPFSIPNSKey{
				{
					ID:       1,
					UserID:   userID,
					Name:     "key1",
					IPNSName: TestIPNSName,
					PeerID:   TestPeerID,
				},
				{
					ID:       2,
					UserID:   userID,
					Name:     "key2",
					IPNSName: "k51qzi5uqu5dljj4y7g7lq43z7z8p9c0f1e2d3c4b5a6987654321fedcba",
					PeerID:   TestPeerID,
				},
			}

			mockIPNSKeyService.EXPECT().ListKeys(mock.Anything, userID).Return(mockKeys, nil)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/ipns/keys", token, nil)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response []dto.IPNSKeyResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Len(t, response, 2)
		}, TestOptions)
	})

	t.Run("empty_list", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockIPNSKeyService, _ := helper.SetupIPNSServiceMocks(userID)

			mockIPNSKeyService.EXPECT().ListKeys(mock.Anything, userID).Return([]db.IPFSIPNSKey{}, nil)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/ipns/keys", token, nil)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response []dto.IPNSKeyResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Empty(t, response)
		}, TestOptions)
	})

	t.Run("error_list_failed", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockIPNSKeyService, _ := helper.SetupIPNSServiceMocks(userID)

			mockIPNSKeyService.EXPECT().ListKeys(mock.Anything, userID).Return(nil, errors.New("list failed"))

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/ipns/keys", token, nil)

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("unauthorized", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			req := ctx.NewAPIRequest(http.MethodGet, "/api/ipns/keys", nil)
			rec := httptest.NewRecorder()
			ctx.Router().ServeHTTP(rec, req)
			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		}, TestOptions)
	})
}

func TestAPI_GetIPNSKey(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockIPNSKeyService, _ := helper.SetupIPNSServiceMocks(userID)

			mockKey := &db.IPFSIPNSKey{
				ID:       1,
				UserID:   userID,
				Name:     "test-key",
				IPNSName: TestIPNSName,
				PeerID:   TestPeerID,
			}

			mockIPNSKeyService.EXPECT().GetKeyByID(mock.Anything, userID, uint(1)).Return(mockKey, nil)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/ipns/keys/1", token, nil)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response dto.IPNSKeyResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, uint(1), response.ID)
			assert.Equal(t, "test-key", response.Name)
		}, TestOptions)
	})

	t.Run("error_invalid_key_id", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/ipns/keys/invalid", token, nil)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
		}, TestOptions)
	})

	t.Run("error_key_not_found", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockIPNSKeyService, mockIPNSPublisherService := helper.SetupIPNSServiceMocksNoDefaults(userID)

			mockIPNSKeyService.EXPECT().GetKeyByID(mock.Anything, userID, uint(999)).Return(nil, gorm.ErrRecordNotFound)
			mockIPNSPublisherService.EXPECT().GetPublished(mock.Anything, TestPeerID, false).Return(nil, errors.New("key not found"))

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/ipns/keys/999", token, nil)

			assert.Equal(t, http.StatusNotFound, rec.Code)
		}, TestOptions)
	})

	t.Run("unauthorized", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			req := ctx.NewAPIRequest(http.MethodGet, "/api/ipns/keys/1", nil)
			rec := httptest.NewRecorder()
			ctx.Router().ServeHTTP(rec, req)
			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		}, TestOptions)
	})
}

func TestAPI_DeleteIPNSKey(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockIPNSKeyService, _ := helper.SetupIPNSServiceMocks(userID)

			mockIPNSKeyService.EXPECT().DeleteKey(mock.Anything, userID, uint(1)).Return(nil)

			rec := helper.makeAuthenticatedRequest(http.MethodDelete, "/api/ipns/keys/1", token, nil)

			assert.Equal(t, http.StatusNoContent, rec.Code)
		}, TestOptions)
	})

	t.Run("error_invalid_key_id", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			rec := helper.makeAuthenticatedRequest(http.MethodDelete, "/api/ipns/keys/invalid", token, nil)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
		}, TestOptions)
	})

	t.Run("error_delete_failed", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockIPNSKeyService, _ := helper.SetupIPNSServiceMocksNoDefaults(userID)

			mockIPNSKeyService.EXPECT().DeleteKey(mock.Anything, userID, uint(1)).Return(errors.New("delete failed"))

			rec := helper.makeAuthenticatedRequest(http.MethodDelete, "/api/ipns/keys/1", token, nil)

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("unauthorized", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			req := ctx.NewAPIRequest(http.MethodDelete, "/api/ipns/keys/1", nil)
			rec := httptest.NewRecorder()
			ctx.Router().ServeHTTP(rec, req)
			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		}, TestOptions)
	})
}

func TestAPI_PublishIPNS(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockIPNSKeyService, mockIPNSPublisherService := helper.SetupIPNSServiceMocks(userID)

			mockKey := &db.IPFSIPNSKey{
				ID:       1,
				UserID:   userID,
				Name:     "test-key",
				IPNSName: TestIPNSName,
				PeerID:   TestPeerID,
			}

			targetCID := cid.MustParse(TestCID)

			mockIPNSKeyService.EXPECT().GetKeyByID(mock.Anything, userID, uint(1)).Return(mockKey, nil)
			mockIPNSPublisherService.EXPECT().PublishCID(mock.Anything, mockKey.PeerID, targetCID.String(), mock.AnythingOfType("time.Duration")).Return(nil)
			mockIPNSPublisherService.EXPECT().GetPublished(mock.Anything, mockKey.PeerID, false).Return(nil, errors.New("not found"))

			reqBody := fmt.Sprintf(`{"key_id":1,"cid":"%s"}`, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/ipns/publish", token, []byte(reqBody))

			assert.Equal(t, http.StatusOK, rec.Code)
		}, TestOptions)
	})

	t.Run("success_with_ttl", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockIPNSKeyService, mockIPNSPublisherService := helper.SetupIPNSServiceMocks(userID)

			mockKey := &db.IPFSIPNSKey{
				ID:       1,
				UserID:   userID,
				Name:     "test-key",
				IPNSName: TestIPNSName,
				PeerID:   TestPeerID,
			}

			targetCID := cid.MustParse(TestCID)

			mockIPNSKeyService.EXPECT().GetKeyByID(mock.Anything, userID, uint(1)).Return(mockKey, nil)
			mockIPNSPublisherService.EXPECT().PublishCID(mock.Anything, mockKey.PeerID, targetCID.String(), 24*time.Hour).Return(nil)
			mockIPNSPublisherService.EXPECT().GetPublished(mock.Anything, mockKey.PeerID, false).Return(nil, errors.New("not found"))

			reqBody := fmt.Sprintf(`{"key_id":1,"cid":"%s","ttl":"24h"}`, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/ipns/publish", token, []byte(reqBody))

			assert.Equal(t, http.StatusOK, rec.Code)
		}, TestOptions)
	})

	t.Run("error_invalid_request", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			reqBody := `{"key_id":1}`
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/ipns/publish", token, []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})

	t.Run("error_invalid_cid", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			reqBody := `{"key_id":1,"cid":"invalid-cid"}`
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/ipns/publish", token, []byte(reqBody))

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("error_key_not_found", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockIPNSKeyService, _ := helper.SetupIPNSServiceMocksNoDefaults(userID)

			mockIPNSKeyService.EXPECT().GetKeyByID(mock.Anything, userID, uint(999)).Return(nil, gorm.ErrRecordNotFound)

			reqBody := fmt.Sprintf(`{"key_id":999,"cid":"%s"}`, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/ipns/publish", token, []byte(reqBody))

			assert.Equal(t, http.StatusNotFound, rec.Code)
		}, TestOptions)
	})

	t.Run("error_publish_failed", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockIPNSKeyService, mockIPNSPublisherService := helper.SetupIPNSServiceMocksNoDefaults(userID)

			mockKey := &db.IPFSIPNSKey{
				ID:       1,
				UserID:   userID,
				Name:     "test-key",
				IPNSName: TestIPNSName,
				PeerID:   TestPeerID,
			}

			targetCID := cid.MustParse(TestCID)

			mockIPNSKeyService.EXPECT().GetKeyByID(mock.Anything, userID, uint(1)).Return(mockKey, nil)
			mockIPNSPublisherService.EXPECT().PublishCID(mock.Anything, mockKey.PeerID, targetCID.String(), mock.AnythingOfType("time.Duration")).Return(errors.New("publish failed"))

			reqBody := fmt.Sprintf(`{"key_id":1,"cid":"%s"}`, TestCID)
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/ipns/publish", token, []byte(reqBody))

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("unauthorized", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			reqBody := fmt.Sprintf(`{"key_id":1,"cid":"%s"}`, TestCID)
			req := ctx.NewAPIRequest(http.MethodPost, "/api/ipns/publish", []byte(reqBody))
			rec := httptest.NewRecorder()
			ctx.Router().ServeHTTP(rec, req)
			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		}, TestOptions)
	})
}

func TestAPI_ResolveIPNS(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			_, mockIPNSPublisherService := helper.SetupIPNSServiceMocks(1)

			// Create a valid IPNS record with a value
			mockRecord := createMockIPNSRecord(t, TestCID)
			mockIPNSPublisherService.EXPECT().GetPublished(mock.Anything, TestIPNSName, false).Return(mockRecord, nil)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, fmt.Sprintf("/api/ipns/resolve/%s", TestIPNSName), token, nil)

			assert.Equal(t, http.StatusOK, rec.Code)
		}, TestOptions)
	})

	t.Run("success_with_routing", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			_, mockIPNSPublisherService := helper.SetupIPNSServiceMocks(1)

			// Create a valid IPNS record with a value
			mockRecord := createMockIPNSRecord(t, TestCID)
			mockIPNSPublisherService.EXPECT().GetPublished(mock.Anything, TestIPNSName, true).Return(mockRecord, nil)

			rec := helper.makeAuthenticatedRequest(http.MethodGet, fmt.Sprintf("/api/ipns/resolve/%s?check_routing=true", TestIPNSName), token, nil)

			assert.Equal(t, http.StatusOK, rec.Code)
		}, TestOptions)
	})

	t.Run("error_missing_name", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			rec := helper.makeAuthenticatedRequest(http.MethodGet, "/api/ipns/resolve/", token, nil)

			assert.Equal(t, http.StatusNotFound, rec.Code)
		}, TestOptions)
	})

	t.Run("error_resolve_failed", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, _ := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			_, mockIPNSPublisherService := helper.SetupIPNSServiceMocks(1)

			mockIPNSPublisherService.EXPECT().GetPublished(mock.Anything, TestIPNSName, false).Return(nil, errors.New("resolve failed"))

			rec := helper.makeAuthenticatedRequest(http.MethodGet, fmt.Sprintf("/api/ipns/resolve/%s", TestIPNSName), token, nil)

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("unauthorized", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			req := ctx.NewAPIRequest(http.MethodGet, "/api/ipns/resolve/"+TestIPNSName, nil)
			rec := httptest.NewRecorder()
			ctx.Router().ServeHTTP(rec, req)
			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		}, TestOptions)
	})
}

func TestAPI_RepublishIPNS(t *testing.T) {
	t.Run("success_all_keys", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockIPNSKeyService, mockIPNSPublisherService := helper.SetupIPNSServiceMocksNoDefaults(userID)

			targetCID := cid.MustParse(TestCID)
			ipnsPath := path.FromCid(targetCID)

			// Create a valid IPNS record with a value
			mockRecord := createMockIPNSRecord(t, TestCID)
			ipnsName, _ := ipns.NameFromString(TestIPNSName)
			records := map[ipns.Name]*ipns.Record{
				ipnsName: mockRecord,
			}

			mockIPNSPublisherService.EXPECT().ListPublished(mock.Anything).Return(records, nil)
			mockIPNSKeyService.EXPECT().GetPrivateKeyByPeerID(mock.Anything, TestPeerID).Return(nil, userID, nil).Times(1)
			mockIPNSPublisherService.EXPECT().PublishWithKey(mock.Anything, nil, ipnsPath.String(), mock.AnythingOfType("time.Duration")).Return(nil)

			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/ipns/republish", token, nil)

			assert.Equal(t, http.StatusOK, rec.Code)

			var response dto.IPNSRepublishResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, 1, response.Count)
		}, TestOptions)
	})

	t.Run("success_specific_key", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockIPNSKeyService, mockIPNSPublisherService := helper.SetupIPNSServiceMocks(userID)

			mockKey := &db.IPFSIPNSKey{
				ID:       1,
				UserID:   userID,
				Name:     "test-key",
				IPNSName: TestIPNSName,
				PeerID:   TestPeerID,
			}

			targetCID := cid.MustParse(TestCID)
			ipnsPath := path.FromCid(targetCID)

			// Create a valid IPNS record with a value
			mockRecord := createMockIPNSRecord(t, TestCID)

			mockIPNSKeyService.EXPECT().GetKeyByID(mock.Anything, userID, uint(1)).Return(mockKey, nil)
			mockIPNSPublisherService.EXPECT().GetPublished(mock.Anything, mockKey.PeerID, false).Return(mockRecord, nil)
			mockIPNSKeyService.EXPECT().GetPrivateKeyByPeerID(mock.Anything, mockKey.PeerID).Return(nil, userID, nil)
			mockIPNSPublisherService.EXPECT().PublishWithKey(mock.Anything, nil, ipnsPath.String(), mock.AnythingOfType("time.Duration")).Return(nil)

			reqBody := `{"key_id":1}`
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/ipns/republish", token, []byte(reqBody))

			assert.Equal(t, http.StatusOK, rec.Code)

			var response dto.IPNSRepublishResponse
			err := json.Unmarshal(rec.Body.Bytes(), &response)
			require.NoError(t, err)
			assert.Equal(t, 1, response.Count)
		}, TestOptions)
	})

	t.Run("error_key_not_found", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			mockIPNSKeyService, _ := helper.SetupIPNSServiceMocksNoDefaults(userID)

			mockIPNSKeyService.EXPECT().GetKeyByID(mock.Anything, userID, uint(999)).Return(nil, gorm.ErrRecordNotFound)

			reqBody := `{"key_id":999}`
			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/ipns/republish", token, []byte(reqBody))

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("error_republish_failed", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)
			token, userID := helper.SetupAuthenticatedTestWithCID(cid.MustParse(TestCID))

			_, mockIPNSPublisherService := helper.SetupIPNSServiceMocks(userID)

			mockIPNSPublisherService.EXPECT().ListPublished(mock.Anything).Return(nil, errors.New("republish failed"))

			rec := helper.makeAuthenticatedRequest(http.MethodPost, "/api/ipns/republish", token, nil)

			assert.Equal(t, http.StatusInternalServerError, rec.Code)
		}, TestOptions)
	})

	t.Run("unauthorized", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			req := ctx.NewAPIRequest(http.MethodPost, "/api/ipns/republish", nil)
			rec := httptest.NewRecorder()
			ctx.Router().ServeHTTP(rec, req)
			assert.Equal(t, http.StatusUnauthorized, rec.Code)
		}, TestOptions)
	})
}
