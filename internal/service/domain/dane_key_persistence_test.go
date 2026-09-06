package domain

import (
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"gorm.io/datatypes"
	"gorm.io/gorm"

	"go.lumeweb.com/dane"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"

	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
)

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

		_, _, err := svc.UpdateTLSAFromCert(ctx, string(pluginDb.DomainNamespaceHNS), "example", certPEM, keyPEM)
		require.NoError(tb, err)

		// The domain row should now hold the private key in ProtocolData.
		var stored pluginDb.WebsiteDomain
		require.NoError(tb, db.Where("domain = ? AND namespace = ?", "example", pluginDb.DomainNamespaceHNS).First(&stored).Error)
		require.NotNil(tb, stored.ProtocolData)
		encKey, ok := stored.ProtocolData[pluginDb.ProtocolDataDANEPrivKey].(string)
		require.True(tb, ok, "dane_private_key should be present in ProtocolData")
		assert.NotEmpty(tb, encKey)
		assert.Equal(tb, keyPEM, encKey)
		assert.NotEmpty(tb, stored.ProtocolData[pluginDb.ProtocolDataDANECertPEM], "dane_cert_pem should be cached")
		assert.NotEmpty(tb, stored.ProtocolData[pluginDb.ProtocolDataTLSA], "tlsa should be stored")
		assert.NotEmpty(tb, stored.ProtocolData[pluginDb.ProtocolDataTLSAOwner], "owner_name should be stored")

		// GetCertificateKey round-trips the SAME key.
		got, err := svc.GetCertificateKey(ctx, string(pluginDb.DomainNamespaceHNS), "example")
		require.NoError(tb, err)
		assert.Equal(tb, keyPEM, got.PrivateKeyPEM)
		assert.Equal(tb, certPEM, got.CertPEM)
		assert.NotEmpty(tb, got.TLSA)
		assert.NotEmpty(tb, got.OwnerName)

		// Second push with a DIFFERENT key must be REJECTED (SPKI-drift guard):
		// publishing a TLSA for the pushed key while the persisted stable key
		// stays K1 would create an internally inconsistent identity.
		key2 := mustGenerateKey(t)
		cert2, _ := issueCertFromKey(t, key2, "example")
		_, _, err = svc.UpdateTLSAFromCert(ctx, string(pluginDb.DomainNamespaceHNS), "example", cert2, key2)
		require.Error(tb, err)
		assert.Contains(tb, err.Error(), "DANE key mismatch")

		// The stored identity is untouched by the rejected push.
		got2, err := svc.GetCertificateKey(ctx, string(pluginDb.DomainNamespaceHNS), "example")
		require.NoError(tb, err)
		assert.Equal(tb, keyPEM, got2.PrivateKeyPEM, "existing key must not be overwritten")
		assert.Equal(tb, certPEM, got2.CertPEM, "cached cert must not refresh from a rejected push")
		tlsaAfter, _, dErr := svc.GetDANERecord(ctx, string(pluginDb.DomainNamespaceHNS), "example")
		require.NoError(tb, dErr)
		assert.Equal(tb, got.TLSA, tlsaAfter, "stored TLSA must not rotate from a rejected push")

		// The row's UpdatedAt must advance on each push so cache invalidation /
		// admin ordering / renewal monitoring see fresh timestamps.
		var after pluginDb.WebsiteDomain
		require.NoError(tb, db.Where("domain = ? AND namespace = ?", "example", pluginDb.DomainNamespaceHNS).First(&after).Error)
		assert.False(tb, after.UpdatedAt.Before(stored.UpdatedAt), "updated_at should advance on push")
	}, TestOptions)
}

func TestEnsureCertificateKey_BootstrapsAndReusesStableSPKI(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		require.NoError(tb, db.Create(&pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "bootstrap", Namespace: pluginDb.DomainNamespaceHNS,
		}).Error)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		first, err := svc.EnsureCertificateKey(ctx, string(pluginDb.DomainNamespaceHNS), "bootstrap")
		require.NoError(tb, err)
		require.NotNil(tb, first)
		require.NotEmpty(tb, first.PrivateKeyPEM)
		require.Empty(tb, first.CertPEM)
		require.NotEmpty(tb, first.TLSA)

		var storedRow pluginDb.WebsiteDomain
		require.NoError(tb, db.Where("domain = ? AND namespace = ?", "bootstrap", pluginDb.DomainNamespaceHNS).First(&storedRow).Error)
		assert.Empty(tb, storedRow.ProtocolData[pluginDb.ProtocolDataDANECertPEM], "bootstrap must not fabricate or store a certificate")

		second, err := svc.EnsureCertificateKey(ctx, string(pluginDb.DomainNamespaceHNS), "bootstrap")
		require.NoError(tb, err)
		assert.Equal(tb, first.PrivateKeyPEM, second.PrivateKeyPEM)
		assert.Equal(tb, first.TLSA, second.TLSA)

		stored, err := svc.GetCertificateKey(ctx, string(pluginDb.DomainNamespaceHNS), "bootstrap")
		require.NoError(tb, err)
		assert.Equal(tb, first.PrivateKeyPEM, stored.PrivateKeyPEM)
		assert.Equal(tb, second.TLSA, stored.TLSA)
	}, TestOptions)
}

func TestUpdateTLSAFromCert_SemanticallyIdenticalReencodedKeyAccepted(t *testing.T) {
	// The identity check compares SPKI imprints, never raw PEM bytes: a
	// re-serialized / line-normalized encoding of the same key is accepted,
	// and the persisted (canonical) key is not overwritten.
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		require.NoError(tb, db.Create(&pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "reenc.hns", Namespace: pluginDb.DomainNamespaceHNS,
			ZoneID: 42,
		}).Error)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockDNS.EXPECT().SetTLSARecord(mock.Anything, uint(42), mock.Anything, mock.Anything).Return(nil).Once()

		stored, err := svc.EnsureCertificateKey(ctx, string(pluginDb.DomainNamespaceHNS), "reenc.hns")
		require.NoError(tb, err)
		canonicalKey := stored.PrivateKeyPEM

		certPEM, err := dane.IssueCertFromKey(canonicalKey, []string{"reenc.hns"}, time.Now().AddDate(1, 0, 0))
		require.NoError(tb, err)

		// Re-encode the SAME key as EC PRIVATE KEY (PKCS#1) — different PEM
		// bytes, identical public key / SPKI.
		reencoded := reencodeKeyAsPKCS1(t, canonicalKey)
		assert.NotEqual(t, canonicalKey, reencoded)

		_, _, err = svc.UpdateTLSAFromCert(ctx, string(pluginDb.DomainNamespaceHNS), "reenc.hns", certPEM, reencoded)
		require.NoError(tb, err)

		// The canonical persisted key is untouched.
		got, gErr := svc.GetCertificateKey(ctx, string(pluginDb.DomainNamespaceHNS), "reenc.hns")
		require.NoError(tb, gErr)
		assert.Equal(t, canonicalKey, got.PrivateKeyPEM)
	}, TestOptions)
}

func TestUpdateTLSAFromCert_RejectedPushDoesNotMutateProviderCache(t *testing.T) {
	// The provider's in-memory cert cache (BuildDelegation reads it to derive
	// TLSA) must only reflect ACCEPTED pushes: a push rejected on SPKI drift
	// must leave the previously published certificate cached. OnCertAvailable
	// therefore fires only after the row-locked identity check passes.
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		require.NoError(tb, db.Create(&pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "match-cache.hns", Namespace: pluginDb.DomainNamespaceHNS,
			ZoneID: 42,
		}).Error)

		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)
		prov := svc.registry.Get(string(pluginDb.DomainNamespaceHNS)).(*HNSProvider)

		mockDNS := core.GetService[*mocks.MockDNSService](ctx, pluginCore.DNS_SERVICE)
		mockDNS.EXPECT().SetTLSARecord(mock.Anything, uint(42), mock.Anything, mock.Anything).Return(nil).Once()

		stored, err := svc.EnsureCertificateKey(ctx, string(pluginDb.DomainNamespaceHNS), "match-cache.hns")
		require.NoError(tb, err)
		goodKey := stored.PrivateKeyPEM
		goodCert, _ := issueCertFromKey(t, goodKey, "match-cache.hns")
		_, _, err = svc.UpdateTLSAFromCert(ctx, string(pluginDb.DomainNamespaceHNS), "match-cache.hns", goodCert, goodKey)
		require.NoError(tb, err)
		require.Equal(t, goodCert, prov.tlsaSource.Certs["match-cache.hns"], "accepted push must populate the provider cache")

		// A cert issued from a DIFFERENT key + that key: rejected on SPKI drift.
		otherKey := mustGenerateKey(t)
		otherCert, _ := issueCertFromKey(t, otherKey, "match-cache.hns")
		_, _, err = svc.UpdateTLSAFromCert(ctx, string(pluginDb.DomainNamespaceHNS), "match-cache.hns", otherCert, otherKey)
		require.Error(tb, err)
		require.Equal(t, goodCert, prov.tlsaSource.Certs["match-cache.hns"],
			"rejected push must not replace the provider cache")
		mockDNS.AssertNumberOfCalls(tb, "SetTLSARecord", 1)
	}, TestOptions)
}

func TestEnsureCertificateKey_LegacyUnreadableKey(t *testing.T) {
	t.Run("unreadable_key_with_installed_tlsa_refuses_rotation", func(t *testing.T) {
		// A legacy row whose dane_private_key is pre-plaintext AES ciphertext
		// (the at-rest encryption and its config were removed) must fail
		// LOUDLY when a TLSA identity is installed: bootstrapping a fresh key
		// would silently rotate the live SPKI pin.
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			require.NoError(tb, ctx.DB().Create(&pluginDb.WebsiteDomain{
				WebsiteID: 1, UserID: 1, Domain: "legacy.hns", Namespace: pluginDb.DomainNamespaceHNS,
				Status: pluginDb.DomainStatusOnchainManaged,
				ProtocolData: datatypes.JSONMap{
					"dane_private_key": "c3VwZXJzZWNyZXQ=\\nnot-a-pem",
					"tlsa":             "3 1 1 aabbcc",
					"owner_name":       "_443._tcp.legacy.hns",
				},
			}).Error)

			svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
			require.NotNil(tb, svc)

			_, err := svc.EnsureCertificateKey(ctx, string(pluginDb.DomainNamespaceHNS), "legacy.hns")
			require.Error(tb, err)
			assert.Contains(tb, err.Error(), "unreadable")
			assert.Contains(tb, err.Error(), "rotate")

			// The identity must be preserved verbatim.
			tlsa, owner, dErr := svc.GetDANERecord(ctx, string(pluginDb.DomainNamespaceHNS), "legacy.hns")
			require.NoError(tb, dErr)
			assert.Equal(tb, "3 1 1 aabbcc", tlsa)
			assert.Equal(tb, "_443._tcp.legacy.hns", owner)
		}, TestOptions)
	})

	t.Run("unreadable_key_without_tlsa_bootstraps_fresh", func(t *testing.T) {
		// No installed identity, so a fresh stable key is safe to bootstrap;
		// the garbage value must not be handed back out as PEM.
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			require.NoError(tb, ctx.DB().Create(&pluginDb.WebsiteDomain{
				WebsiteID: 1, UserID: 1, Domain: "legacy-fresh.hns", Namespace: pluginDb.DomainNamespaceHNS,
				Status: pluginDb.DomainStatusOnchainManaged,
				ProtocolData: datatypes.JSONMap{
					"dane_private_key": "c3VwZXJzZWNyZXQ=",
				},
			}).Error)

			svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
			require.NotNil(tb, svc)

			sc, err := svc.EnsureCertificateKey(ctx, string(pluginDb.DomainNamespaceHNS), "legacy-fresh.hns")
			require.NoError(tb, err)
			assert.Contains(tb, sc.PrivateKeyPEM, "BEGIN ")
			assert.NotEmpty(tb, sc.TLSA)

			got, gErr := svc.GetCertificateKey(ctx, string(pluginDb.DomainNamespaceHNS), "legacy-fresh.hns")
			require.NoError(tb, gErr)
			assert.Equal(tb, sc.PrivateKeyPEM, got.PrivateKeyPEM)
		}, TestOptions)
	})
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

		tlsa, owner, err := svc.GetDANERecord(ctx, string(pluginDb.DomainNamespaceHNS), "dane.hns")
		require.NoError(tb, err)
		assert.Equal(tb, "3 1 1 aabb", tlsa)
		assert.Equal(tb, "_443._tcp.dane.hns", owner)

		// A binding with no DANE identity reports empty, not an error.
		wd2 := &pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "nodane.hns", Namespace: pluginDb.DomainNamespaceHNS,
			Status: pluginDb.DomainStatusOnchainManaged,
		}
		require.NoError(tb, db.Create(wd2).Error)
		tlsa, owner, err = svc.GetDANERecord(ctx, string(pluginDb.DomainNamespaceHNS), "nodane.hns")
		require.NoError(tb, err)
		assert.Empty(tb, tlsa)
		assert.Empty(tb, owner)
	}, TestOptions)
}

func TestRepublishChainDANERecord(t *testing.T) {
	t.Run("bootstraps_identity_when_none_stored", func(t *testing.T) {
		// A chain-managed binding with no DANE identity is bootstrapped on
		// republish: a stable DANE key is generated, its SPKI-derived TLSA is
		// computed, and it persists as the stored identity — no configuration
		// or prior cert push required.
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
			require.NotNil(tb, svc)

			require.NoError(tb, ctx.DB().Create(&pluginDb.WebsiteDomain{
				WebsiteID: 1, UserID: 1, Domain: "chain.hns", Namespace: pluginDb.DomainNamespaceHNS,
				Status: pluginDb.DomainStatusOnchainManaged,
			}).Error)

			tlsa, owner, err := svc.RepublishChainDANERecord(ctx, string(pluginDb.DomainNamespaceHNS), "chain.hns")
			require.NoError(tb, err)
			require.NotEmpty(tb, tlsa)
			require.NotEmpty(tb, owner)

			// The recomputed record must persist as the stored DANE identity.
			storedTLSa, storedOwner, err := svc.GetDANERecord(ctx, string(pluginDb.DomainNamespaceHNS), "chain.hns")
			require.NoError(tb, err)
			assert.Equal(tb, tlsa, storedTLSa)
			assert.Equal(tb, owner, storedOwner)
		}, TestOptions)
	})

	t.Run("preserves_installed_tlsa_without_rotating_key", func(t *testing.T) {
		// Regression: republish must NOT rotate an installed on-chain identity.
		// A binding with an already-installed TLSA and no stored private key
		// must return that exact TLSA and persist no new key — otherwise the
		// SPKI pin rotates and the live DANE record breaks.
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
			require.NotNil(tb, svc)

			wd := &pluginDb.WebsiteDomain{
				WebsiteID: 1, UserID: 1, Domain: "chain-installed.hns", Namespace: pluginDb.DomainNamespaceHNS,
				Status: pluginDb.DomainStatusOnchainManaged,
				ProtocolData: datatypes.JSONMap{
					"tlsa":       "3 1 1 aabbcc",
					"owner_name": "_443._tcp.chain-installed.hns",
				},
			}
			require.NoError(tb, ctx.DB().Create(wd).Error)

			tlsa, owner, err := svc.RepublishChainDANERecord(ctx, string(pluginDb.DomainNamespaceHNS), "chain-installed.hns")
			require.NoError(tb, err)
			assert.Equal(tb, "3 1 1 aabbcc", tlsa)
			assert.Equal(tb, "_443._tcp.chain-installed.hns", owner)

			// The installed identity must be preserved verbatim: no private key
			// is persisted and the TLSA is unchanged, so no rotation occurred.
			_, gErr := svc.GetCertificateKey(ctx, string(pluginDb.DomainNamespaceHNS), "chain-installed.hns")
			assert.ErrorIs(tb, gErr, gorm.ErrRecordNotFound, "republish must not persist a new key for an installed identity")
			stored, _, _ := svc.GetDANERecord(ctx, string(pluginDb.DomainNamespaceHNS), "chain-installed.hns")
			assert.Equal(tb, "3 1 1 aabbcc", stored)
		}, TestOptions)
	})

	t.Run("recomputes_owner_when_stored_record_has_none", func(t *testing.T) {
		// A stored TLSA with a missing/corrupt owner_name must still yield a
		// usable owner — the owner name is deterministic from the domain, so
		// republish recomputes it instead of returning a bare TLSA.
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
			require.NotNil(tb, svc)

			wd := &pluginDb.WebsiteDomain{
				WebsiteID: 1, UserID: 1, Domain: "chain-noowner.hns", Namespace: pluginDb.DomainNamespaceHNS,
				Status: pluginDb.DomainStatusOnchainManaged,
				ProtocolData: datatypes.JSONMap{
					"tlsa": "3 1 1 aabbcc",
					// no owner_name
				},
			}
			require.NoError(tb, ctx.DB().Create(wd).Error)

			tlsa, owner, err := svc.RepublishChainDANERecord(ctx, string(pluginDb.DomainNamespaceHNS), "chain-noowner.hns")
			require.NoError(tb, err)
			assert.Equal(tb, "3 1 1 aabbcc", tlsa)
			assert.Equal(tb, "_443._tcp.chain-noowner.hns", owner)
		}, TestOptions)
	})

	t.Run("errors_when_binding_missing", func(t *testing.T) {
		// No binding row: the missing-row sentinel propagates (wrapped by the
		// API layer into a client-visible republish error), not a silent
		// bootstrap of nothing.
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
			require.NotNil(tb, svc)

			_, _, err := svc.RepublishChainDANERecord(ctx, string(pluginDb.DomainNamespaceHNS), "ghost.hns")
			require.ErrorIs(tb, err, gorm.ErrRecordNotFound)
		}, TestOptions)
	})
}

func TestGetCertificateKey_NotFound(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
		require.NotNil(tb, svc)

		// No domain row -> ErrRecordNotFound.
		_, err := svc.GetCertificateKey(ctx, string(pluginDb.DomainNamespaceHNS), "nonexistent")
		require.Error(tb, err)
		assert.ErrorIs(tb, err, gorm.ErrRecordNotFound)

		// Domain row exists but no key persisted -> ErrRecordNotFound.
		require.NoError(tb, ctx.DB().Create(&pluginDb.WebsiteDomain{
			WebsiteID: 1, UserID: 1, Domain: "nokey", Namespace: pluginDb.DomainNamespaceHNS,
		}).Error)
		_, err = svc.GetCertificateKey(ctx, string(pluginDb.DomainNamespaceHNS), "nokey")
		require.Error(tb, err)
		assert.ErrorIs(tb, err, gorm.ErrRecordNotFound)
	}, TestOptions)
}

// --- helpers ---

// reencodeKeyAsPKCS1 parses a PKCS#8/EC PEM private key and re-encodes the
// same ECDSA private key as an "EC PRIVATE KEY" PEM block — different PEM
// bytes with an identical public key / SPKI.
func reencodeKeyAsPKCS1(t testing.TB, keyPEM string) string {
	t.Helper()
	block, _ := pem.Decode([]byte(keyPEM))
	require.NotNil(t, block, "input must be a PEM key")
	var ecKey *ecdsa.PrivateKey
	switch block.Type {
	case "EC PRIVATE KEY":
		pk, err := x509.ParseECPrivateKey(block.Bytes)
		require.NoError(t, err)
		ecKey = pk
	case "PRIVATE KEY":
		pk, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		require.NoError(t, err)
		var ok bool
		ecKey, ok = pk.(*ecdsa.PrivateKey)
		require.True(t, ok, "input must be an EC key")
	default:
		t.Fatalf("unsupported key PEM block type %q", block.Type)
	}
	der, err := x509.MarshalECPrivateKey(ecKey)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der}))
}

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
	// Issue the cert from the SAME key the push carries: the portal stores the
	// key and the cert's SPKI must agree with it (issue-around-stable-key).
	certPEM, err := dane.IssueCertFromKey(keyPEM, []string{domain}, time.Now().AddDate(1, 0, 0))
	if err != nil {
		t.Fatalf("issue cert: %v", err)
	}
	return certPEM, keyPEM
}
