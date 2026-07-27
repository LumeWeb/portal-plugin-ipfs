package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/dane"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

func TestAPI_PushCert(t *testing.T) {
	t.Run("valid_hns_cert_computes_tlsa", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			// Pre-create a website domain record so the push stores TLSA (DANE flow)
			domainRec := &pluginDb.WebsiteDomain{
				Domain:    "example",
				Namespace: pluginDb.DomainNamespaceHNS,
						}
			require.NoError(t, ctx.DB().Create(domainRec).Error)

			// Generate a real self-signed cert
			certPEM, _, err := dane.GenerateSelfSignedECDSA([]string{"example"}, time.Now().AddDate(1, 0, 0))
			require.NoError(t, err)

			reqBody := fmt.Sprintf(`{"domain":"example","namespace":"hns","cert_pem":%q}`, certPEM)
			rec := helper.makeGatewayAuthenticatedRequest(http.MethodPost, "/internal/dns/cert", testGatewaySecret(), []byte(reqBody))

			assert.Equal(t, http.StatusOK, rec.Code)

			var resp dto.CertPushResponse
			err = json.Unmarshal(rec.Body.Bytes(), &resp)
			require.NoError(t, err)
			assert.True(t, resp.OK)
			assert.NotEmpty(t, resp.TLSA)
			assert.Contains(t, resp.TLSA, "3 1 1")
			assert.NotEmpty(t, resp.OwnerName)

			// Verify it was stored in the domain record (ProtocolData)
			var stored pluginDb.WebsiteDomain
			require.NoError(t, ctx.DB().Where("domain = ? AND namespace = ?", "example", pluginDb.DomainNamespaceHNS).First(&stored).Error)
			assert.Equal(t, resp.TLSA, stored.ProtocolData["tlsa"])
			assert.Equal(t, resp.OwnerName, stored.ProtocolData["owner_name"])
		}, TestOptions)
	})

	t.Run("invalid_cert_pem", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			reqBody := `{"domain":"example","namespace":"hns","cert_pem":"not-a-valid-cert"}`
			rec := helper.makeGatewayAuthenticatedRequest(http.MethodPost, "/internal/dns/cert", testGatewaySecret(), []byte(reqBody))

			assert.Equal(t, http.StatusBadRequest, rec.Code)
		}, TestOptions)
	})

	t.Run("unbound_domain_returns_computed_tlsa", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			// Do NOT pre-create a domain record — the handler should
			// fall through to best-effort TLSA computation and return 200,
			// not 500 (regression: previously fell through to 500).
			certPEM, _, err := dane.GenerateSelfSignedECDSA([]string{"unbound"}, time.Now().AddDate(1, 0, 0))
			require.NoError(t, err)

			reqBody := fmt.Sprintf(`{"domain":"unbound","namespace":"hns","cert_pem":%q}`, certPEM)
			rec := helper.makeGatewayAuthenticatedRequest(http.MethodPost, "/internal/dns/cert", testGatewaySecret(), []byte(reqBody))

			assert.Equal(t, http.StatusOK, rec.Code)

			var resp dto.CertPushResponse
			err = json.Unmarshal(rec.Body.Bytes(), &resp)
			require.NoError(t, err)
			assert.True(t, resp.OK)
			assert.NotEmpty(t, resp.TLSA)
			assert.Contains(t, resp.TLSA, "3 1 1")
		}, TestOptions)
	})

	t.Run("missing_fields", func(t *testing.T) {
		coreTesting.RunTestCase(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			// Missing cert_pem — DTO validation rejects with 422
			reqBody := `{"domain":"example","namespace":"hns"}`
			rec := helper.makeGatewayAuthenticatedRequest(http.MethodPost, "/internal/dns/cert", testGatewaySecret(), []byte(reqBody))

			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
		}, TestOptions)
	})
}

func TestAPI_UpdateTLSA(t *testing.T) {
	t.Run("valid_hns_cert_updates_tlsa", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			// Pre-create a website domain record so the update stores TLSA
			domainRec := &pluginDb.WebsiteDomain{
				Domain:    "example",
				Namespace: pluginDb.DomainNamespaceHNS,
			}
			require.NoError(t, ctx.DB().Create(domainRec).Error)

			// Generate a real self-signed cert
			certPEM, _, err := dane.GenerateSelfSignedECDSA([]string{"example"}, time.Now().AddDate(1, 0, 0))
			require.NoError(t, err)

			reqBody := fmt.Sprintf(`{"domain":"example","namespace":"hns","tlsa":"3 1 1 abc123","cert_pem":%q}`, certPEM)
			rec := helper.makeGatewayAuthenticatedRequest(http.MethodPost, "/internal/dns/tlsa", testGatewaySecret(), []byte(reqBody))

			assert.Equal(t, http.StatusOK, rec.Code)

			var resp dto.CertPushResponse
			err = json.Unmarshal(rec.Body.Bytes(), &resp)
			require.NoError(t, err)
			assert.True(t, resp.OK)
			assert.NotEmpty(t, resp.TLSA)
			assert.Contains(t, resp.TLSA, "3 1 1")
			assert.NotEmpty(t, resp.OwnerName)

			// Verify it was stored in the domain record (ProtocolData)
			var stored pluginDb.WebsiteDomain
			require.NoError(t, ctx.DB().Where("domain = ? AND namespace = ?", "example", pluginDb.DomainNamespaceHNS).First(&stored).Error)
			assert.Equal(t, resp.TLSA, stored.ProtocolData["tlsa"])
			assert.Equal(t, resp.OwnerName, stored.ProtocolData["owner_name"])
		}, TestOptions)
	})

	t.Run("invalid_cert_pem", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			helper := newMockHelper(t, ctx)

			reqBody := `{"domain":"example","namespace":"hns","tlsa":"3 1 1 abc123","cert_pem":"not-a-valid-cert"}`
			rec := helper.makeGatewayAuthenticatedRequest(http.MethodPost, "/internal/dns/tlsa", testGatewaySecret(), []byte(reqBody))

			assert.Equal(t, http.StatusBadRequest, rec.Code)
		}, TestOptions)
	})
}
