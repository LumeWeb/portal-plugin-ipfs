package domain

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"go.lumeweb.com/dane"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

// testDANEKey is a fixed 32-byte AES-256 key (base64) used for at-rest key tests.
const testDANEKey = "IUf7FMs69krvqJGFn7y8U2jfurNf8bxynXFQBGnP7cI="

// keyTestOptions wires the DnsConfig with a DANE key-encryption key so the
// service will actually encrypt/persist TLS keys. TestOptions already brings
// the SQLite migrations (including the new TLSPrivateKey columns).
var keyTestOptions = coreTesting.CombineOptions(
	TestOptions,
	coreTesting.WithConfig("plugin.ipfs.service.dns.dane_key_encryption_key", testDANEKey),
)

func TestEncryptDecryptPrivateKey_RoundTrip(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		// The service reads DNS_SERVICE config; keyTestOptions sets the encryption key.
		enc, err := svc.encryptPrivateKey(ctx, "-----BEGIN PRIVATE KEY-----fake-----END PRIVATE KEY-----")
		require.NoError(tb, err)
		assert.NotContains(tb, enc, "BEGIN PRIVATE KEY")

		dec, err := svc.decryptPrivateKey(ctx, enc)
		require.NoError(tb, err)
		assert.Equal(tb, "-----BEGIN PRIVATE KEY-----fake-----END PRIVATE KEY-----", dec)
	}, keyTestOptions)
}

func TestUpdateTLSAFromCert_PersistsAndReusesKey(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		require.NoError(tb, db.Create(&pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "example", Namespace: pluginDb.DomainNamespaceHNS,
		}).Error)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		// First push: a cert + a real private key.
		keyPEM := mustGenerateKey(t)
		certPEM, _ := issueCertFromKey(t, keyPEM, "example")

		_, _, err := svc.UpdateTLSAFromCert(ctx, "hns", "example", certPEM, keyPEM)
		require.NoError(tb, err)

		// The domain row should now hold an encrypted key (not plaintext).
		var stored pluginDb.WebsiteDomain
		require.NoError(tb, db.Where("domain = ? AND namespace = ?", "example", pluginDb.DomainNamespaceHNS).First(&stored).Error)
		require.NotEmpty(tb, stored.TLSPrivateKey)
		assert.NotContains(tb, stored.TLSPrivateKey, "BEGIN PRIVATE KEY")

		// GetCertificateKey decrypts and round-trips the SAME key.
		got, err := svc.GetCertificateKey(ctx, "hns", "example")
		require.NoError(tb, err)
		assert.Equal(tb, keyPEM, got.PrivateKeyPEM)
		assert.Equal(tb, certPEM, got.CertPEM)
		assert.NotEmpty(tb, got.TLSA)
		assert.NotEmpty(tb, got.OwnerName)

		// Second push with a DIFFERENT key must NOT clobber the persisted key.
		key2 := mustGenerateKey(t)
		cert2, _ := issueCertFromKey(t, key2, "example")
		_, _, err = svc.UpdateTLSAFromCert(ctx, "hns", "example", cert2, key2)
		require.NoError(tb, err)

		got2, err := svc.GetCertificateKey(ctx, "hns", "example")
		require.NoError(tb, err)
		assert.Equal(tb, keyPEM, got2.PrivateKeyPEM, "existing key must not be overwritten")
	}, keyTestOptions)
}

func TestGetCertificateKey_NotFound(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		// No domain row -> ErrRecordNotFound.
		_, err := svc.GetCertificateKey(ctx, "hns", "nonexistent")
		require.Error(tb, err)
		assert.ErrorIs(tb, err, gorm.ErrRecordNotFound)

		// Domain row exists but no key persisted -> ErrRecordNotFound.
		require.NoError(tb, ctx.DB().Create(&pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "nokey", Namespace: pluginDb.DomainNamespaceHNS,
		}).Error)
		_, err = svc.GetCertificateKey(ctx, "hns", "nokey")
		require.Error(tb, err)
		assert.ErrorIs(tb, err, gorm.ErrRecordNotFound)
	}, keyTestOptions)
}

// --- helpers ---

func mustGenerateKey(t testing.TB) string {
	t.Helper()
	_, keyPEM, err := dane.GenerateSelfSignedECDSA([]string{"example"}, time.Now().AddDate(1, 0, 0))
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return keyPEM
}

func issueCertFromKey(t testing.TB, keyPEM, domain string) (string, string) {
	t.Helper()
	// dane lib doesn't yet expose IssueCertFromKey; re-use GenerateSelfSignedECDSA
	// which mints a fresh key — for these tests we only assert that whatever key
	// we handed in is what's persisted, so a fresh key is fine for cert gen as
	// long as the push carries the intended key. The SPKI-stability guarantee is
	// exercised at the Caddy layer (Repo C/D).
	certPEM, _, err := dane.GenerateSelfSignedECDSA([]string{domain}, time.Now().AddDate(1, 0, 0))
	if err != nil {
		t.Fatalf("issue cert: %v", err)
	}
	return certPEM, keyPEM
}
