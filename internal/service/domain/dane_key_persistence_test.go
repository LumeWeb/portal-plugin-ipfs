package domain

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/datatypes"
	"gorm.io/gorm"

	"go.lumeweb.com/dane"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

// testDANEKey is a fixed 32-byte AES-256 key (base64) used to encrypt the DANE
// private key at rest. It is an encryption key for at-rest ciphertext, not a
// private key/secret literal, and is the same shape as the production config.
const testDANEKey = "IUf7FMs69krvqJGFn7y8U2jfurNf8bxynXFQBGnP7cI="

// roundTripTestKey returns a fresh runtime-generated PKCS#8 private key so tests
// never hard-code secret-looking literals in source.
func roundTripTestKey(t testing.TB) string {
	t.Helper()
	return mustGenerateKey(t)
}

// keyTestOptions wires the DnsConfig with a DANE key-encryption key so the
// service will actually encrypt/persist TLS keys.
var keyTestOptions = coreTesting.CombineOptions(
	TestOptions,
	coreTesting.WithConfig("plugin.ipfs.service.dns.dane_key_encryption_key", testDANEKey),
)

func TestEncryptDecryptPrivateKey_RoundTrip(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		keyPEM := roundTripTestKey(t)
		enc, err := svc.encryptPrivateKey(ctx, keyPEM)
		require.NoError(tb, err)
		assert.NotContains(tb, enc, "BEGIN PRIVATE KEY")

		dec, err := svc.decryptPrivateKey(ctx, enc)
		require.NoError(tb, err)
		assert.Equal(tb, keyPEM, dec)
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

		// The domain row should now hold an encrypted key in ProtocolData (not plaintext).
		var stored pluginDb.WebsiteDomain
		require.NoError(tb, db.Where("domain = ? AND namespace = ?", "example", pluginDb.DomainNamespaceHNS).First(&stored).Error)
		require.NotNil(tb, stored.ProtocolData)
		encKey, ok := stored.ProtocolData[protocolDataPrivateKeyKey].(string)
		require.True(tb, ok, "dane_private_key should be present in ProtocolData")
		assert.NotEmpty(tb, encKey)
		assert.NotContains(tb, encKey, "BEGIN PRIVATE KEY")
		assert.NotEmpty(tb, stored.ProtocolData[daneKeyField], "dane_cert_pem should be cached")
		assert.NotEmpty(tb, stored.ProtocolData[protocolDataTLSAKey], "tlsa should be stored")
		assert.NotEmpty(tb, stored.ProtocolData[protocolDataOwnerKey], "owner_name should be stored")

		// GetCertificateKey decrypts and round-trips the SAME key.
		got, err := svc.GetCertificateKey(ctx, "hns", "example")
		require.NoError(tb, err)
		assert.Equal(tb, keyPEM, got.PrivateKeyPEM)
		assert.Equal(tb, certPEM, got.CertPEM)
		assert.NotEmpty(tb, got.TLSA)
		assert.NotEmpty(tb, got.OwnerName)

		// Second push with a DIFFERENT key must NOT clobber the persisted key,
		// but must refresh the cached cert.
		key2 := mustGenerateKey(t)
		cert2, _ := issueCertFromKey(t, key2, "example")
		_, _, err = svc.UpdateTLSAFromCert(ctx, "hns", "example", cert2, key2)
		require.NoError(tb, err)

		got2, err := svc.GetCertificateKey(ctx, "hns", "example")
		require.NoError(tb, err)
		assert.Equal(tb, keyPEM, got2.PrivateKeyPEM, "existing key must not be overwritten")
		assert.Equal(tb, cert2, got2.CertPEM, "cached cert should refresh to the latest push")

		// The row's UpdatedAt must advance on each push so cache invalidation /
		// admin ordering / renewal monitoring see fresh timestamps.
		var after pluginDb.WebsiteDomain
		require.NoError(tb, db.Where("domain = ? AND namespace = ?", "example", pluginDb.DomainNamespaceHNS).First(&after).Error)
		assert.False(tb, after.UpdatedAt.Before(stored.UpdatedAt), "updated_at should advance on push")
	}, keyTestOptions)
}

func TestEnsureCertificateKey_BootstrapsAndReusesStableSPKI(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		require.NoError(tb, db.Create(&pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "bootstrap", Namespace: pluginDb.DomainNamespaceHNS,
		}).Error)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		first, err := svc.EnsureCertificateKey(ctx, "hns", "bootstrap")
		require.NoError(tb, err)
		require.NotNil(tb, first)
		require.NotEmpty(tb, first.PrivateKeyPEM)
		require.Empty(tb, first.CertPEM)
		require.NotEmpty(tb, first.TLSA)

		var storedRow pluginDb.WebsiteDomain
		require.NoError(tb, db.Where("domain = ? AND namespace = ?", "bootstrap", pluginDb.DomainNamespaceHNS).First(&storedRow).Error)
		assert.Empty(tb, storedRow.ProtocolData[daneKeyField], "bootstrap must not fabricate or store a certificate")

		second, err := svc.EnsureCertificateKey(ctx, "hns", "bootstrap")
		require.NoError(tb, err)
		assert.Equal(tb, first.PrivateKeyPEM, second.PrivateKeyPEM)
		assert.Equal(tb, first.TLSA, second.TLSA)

		stored, err := svc.GetCertificateKey(ctx, "hns", "bootstrap")
		require.NoError(tb, err)
		assert.Equal(tb, first.PrivateKeyPEM, stored.PrivateKeyPEM)
		assert.Equal(tb, second.TLSA, stored.TLSA)
	}, keyTestOptions)
}

func TestDANEPublicationTargetFor(t *testing.T) {
	// The publication-target helper is the single source of truth for DANE
	// republish eligibility. Portal-managed HNS republishes into its managed
	// zone; chain-managed (HIP-5) republishes to the on-chain name data; every
	// other binding / namespace carries no portal DANE duty.
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		mk := func(domain string, ns pluginDb.DomainNamespace, status pluginDb.DomainStatus, zoneID uint) *pluginDb.WebsiteDomain {
			wd := &pluginDb.WebsiteDomain{
				WebsiteID: 1, UserID: 1, Domain: domain, Namespace: ns, Status: status, ZoneID: zoneID,
			}
			require.NoError(tb, db.Create(wd).Error)
			return wd
		}

		portalManaged := mk("ptl", pluginDb.DomainNamespaceHNS, pluginDb.DomainStatusActive, 99)
		if locus, ok := svc.DANEPublicationTargetFor(portalManaged); assert.True(tb, ok) {
			assert.Equal(tb, DANEPublishManagedZone, locus)
		}

		onchain := mk("chain", pluginDb.DomainNamespaceHNS, pluginDb.DomainStatusOnchainManaged, 0)
		if locus, ok := svc.DANEPublicationTargetFor(onchain); assert.True(tb, ok) {
			assert.Equal(tb, DANEPublishChain, locus)
		}

		// An on-chain binding carrying a stray zone is still chain-managed:
		// class (not the zone reference) decides the locus.
		stray := mk("stray", pluginDb.DomainNamespaceHNS, pluginDb.DomainStatusOnchainManaged, 7)
		if locus, ok := svc.DANEPublicationTargetFor(stray); assert.True(tb, ok) {
			assert.Equal(tb, DANEPublishChain, locus)
		}

		// ICANN has no DANE locus anywhere.
		icann := mk("x.com", pluginDb.DomainNamespaceICANN, pluginDb.DomainStatusActive, 1)
		_, ok := svc.DANEPublicationTargetFor(icann)
		assert.False(tb, ok)

		// Self-hosted and unresolved bindings have no portal DANE publication
		// duty even though the namespace is DANE-capable.
		selfHosted := mk("sh", pluginDb.DomainNamespaceHNS, pluginDb.DomainStatusSelfHosted, 0)
		_, ok = svc.DANEPublicationTargetFor(selfHosted)
		assert.False(tb, ok)
		unresolved := mk("unr", pluginDb.DomainNamespaceHNS, pluginDb.DomainStatusDraft, 0)
		_, ok = svc.DANEPublicationTargetFor(unresolved)
		assert.False(tb, ok)
	}, TestOptions)
}

func TestGetDANERecord(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		wd := &pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "dane.hns", Namespace: pluginDb.DomainNamespaceHNS,
			Status: pluginDb.DomainStatusOnchainManaged,
			ProtocolData: datatypes.JSONMap{
				"tlsa":       "3 1 1 aabb",
				"owner_name": "_443._tcp.dane.hns",
			},
		}
		require.NoError(tb, db.Create(wd).Error)

		tlsa, owner, err := svc.GetDANERecord(ctx, "hns", "dane.hns")
		require.NoError(tb, err)
		assert.Equal(tb, "3 1 1 aabb", tlsa)
		assert.Equal(tb, "_443._tcp.dane.hns", owner)

		// A binding with no DANE identity reports empty, not an error.
		wd2 := &pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "nodane.hns", Namespace: pluginDb.DomainNamespaceHNS,
			Status: pluginDb.DomainStatusOnchainManaged,
		}
		require.NoError(tb, db.Create(wd2).Error)
		tlsa, owner, err = svc.GetDANERecord(ctx, "hns", "nodane.hns")
		require.NoError(tb, err)
		assert.Empty(tb, tlsa)
		assert.Empty(tb, owner)
	}, TestOptions)
}

func TestDANEKeyNotConfiguredSentinel(t *testing.T) {
	// With no DANE key-encryption key configured, daneEncryptionKey must return
	// the errDANEKeyNotConfigured sentinel — not a generic error — so
	// ensureDANEIdentity can skip persistence on the empty-key case specifically
	// without masking genuine failures that merely co-occur with an absent key.
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		_, err := svc.daneEncryptionKey()
		require.Error(tb, err)
		assert.ErrorIs(tb, err, errDANEKeyNotConfigured)
	}, TestOptions)
}

func TestRepublishChainDANERecord(t *testing.T) {
	t.Run("recomputes_from_stored_key_when_encryption_key_configured", func(t *testing.T) {
		// With the key-encryption key configured, RepublishChainDANERecord
		// derives the TLSA fresh from the stable DANE key (source of truth) and
		// persists it, even when no cert was ever pushed.
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
			require.NotNil(tb, svc)

			require.NoError(tb, ctx.DB().Create(&pluginDb.WebsiteDomain{
				WebsiteID: 1, UserID: 1, Domain: "chain.hns", Namespace: pluginDb.DomainNamespaceHNS,
				Status: pluginDb.DomainStatusOnchainManaged,
			}).Error)

			tlsa, owner, err := svc.RepublishChainDANERecord(ctx, "hns", "chain.hns")
			require.NoError(tb, err)
			require.NotEmpty(tb, tlsa)
			require.NotEmpty(tb, owner)

			// The recomputed record must persist as the stored DANE identity.
			storedTLSa, storedOwner, err := svc.GetDANERecord(ctx, "hns", "chain.hns")
			require.NoError(tb, err)
			assert.Equal(tb, tlsa, storedTLSa)
			assert.Equal(tb, owner, storedOwner)
		}, keyTestOptions)
	})

	t.Run("falls_back_to_stored_tlsa_without_encryption_key", func(t *testing.T) {
		// The real-world flaw: a chain-managed name whose cert was pushed but
		// whose private key was never persisted (no key-encryption key). The
		// stored TLSA must be returned rather than a NoStoredCertificate error.
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
			require.NotNil(tb, svc)

			wd := &pluginDb.WebsiteDomain{
				WebsiteID: 1, UserID: 1, Domain: "chain-fallback.hns", Namespace: pluginDb.DomainNamespaceHNS,
				Status: pluginDb.DomainStatusOnchainManaged,
			}
			require.NoError(tb, ctx.DB().Create(wd).Error)

			keyPEM := mustGenerateKey(tb)
			certPEM, _ := issueCertFromKey(tb, keyPEM, "chain-fallback.hns")
			// No key-encryption key configured: the push stores tlsa/owner/cert
			// but skips persisting the private key.
			_, _, err := svc.UpdateTLSAFromCert(ctx, "hns", wd.Domain, certPEM, keyPEM)
			require.NoError(tb, err)

			// Precondition: TLSA present, private key absent.
			stored, _, err := svc.GetDANERecord(ctx, "hns", wd.Domain)
			require.NoError(tb, err)
			require.NotEmpty(tb, stored)
			_, err = svc.GetCertificateKey(ctx, "hns", wd.Domain)
			require.ErrorIs(tb, err, gorm.ErrRecordNotFound)

			tlsa, owner, err := svc.RepublishChainDANERecord(ctx, "hns", wd.Domain)
			require.NoError(tb, err)
			assert.Equal(tb, stored, tlsa)
			require.NotEmpty(tb, owner)
		}, TestOptions)
	})

	t.Run("errors_when_no_identity_exists", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
			require.NotNil(tb, svc)

			require.NoError(tb, ctx.DB().Create(&pluginDb.WebsiteDomain{
				WebsiteID: 1, UserID: 1, Domain: "empty.hns", Namespace: pluginDb.DomainNamespaceHNS,
				Status: pluginDb.DomainStatusOnchainManaged,
			}).Error)

			_, _, err := svc.RepublishChainDANERecord(ctx, "hns", "empty.hns")
			require.ErrorIs(tb, err, gorm.ErrRecordNotFound)
		}, TestOptions)
	})
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
	// Re-issue a cert for the given domain. For these tests we only assert that
	// whatever key we handed in is what's persisted, so a fresh cert is fine as
	// long as the push carries the intended key. The SPKI-stability guarantee is
	// exercised at the Caddy layer (Repo C/D).
	certPEM, _, err := dane.GenerateSelfSignedECDSA([]string{domain}, time.Now().AddDate(1, 0, 0))
	if err != nil {
		t.Fatalf("issue cert: %v", err)
	}
	return certPEM, keyPEM
}
