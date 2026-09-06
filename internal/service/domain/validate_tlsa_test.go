package domain

import (
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

// startTLSADNSServer serves a single TLSA record for the given owner (a
// fully-qualified name). An empty rdata answers NODATA (no answer section).
func startTLSADNSServer(t *testing.T, owner, rdata string) string {
	t.Helper()

	handler := dns.HandlerFunc(func(w dns.ResponseWriter, req *dns.Msg) {
		m := new(dns.Msg)
		m.SetReply(req)
		if len(req.Question) > 0 && req.Question[0].Qtype == dns.TypeTLSA && rdata != "" {
			parts := strings.Fields(rdata)
			usage, _ := strconv.Atoi(parts[0])
			selector, _ := strconv.Atoi(parts[1])
			matching, _ := strconv.Atoi(parts[2])
			m.Answer = append(m.Answer, &dns.TLSA{
				Hdr:          dns.RR_Header{Name: owner, Rrtype: dns.TypeTLSA, Class: dns.ClassINET, Ttl: 300},
				Usage:        uint8(usage),
				Selector:     uint8(selector),
				MatchingType: uint8(matching),
				Certificate:  parts[3],
			})
		}
		_ = w.WriteMsg(m)
	})

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := pc.LocalAddr().String()
	udpSrv := &dns.Server{PacketConn: pc, Handler: handler}
	go func() { _ = udpSrv.ActivateAndServe() }()
	t.Cleanup(func() { _ = udpSrv.Shutdown() })
	return addr
}

// --- QueryTLSARdata ---

func TestHNSProvider_QueryTLSARdata(t *testing.T) {
	t.Run("returns_normalized_rdata", func(t *testing.T) {
		provider := NewHNSProvider("127.0.0.1:53", nil, TLSASource{})

		// Uppercase hex in the served record must normalize to lowercase.
		addr := startTLSADNSServer(t, "_443._tcp.dane.hns.", "3 1 1 AABBCC")
		provider = NewHNSProvider(addr, nil, TLSASource{})

		got, err := provider.QueryTLSARdata(t.Context(), "dane.hns")
		require.NoError(t, err)
		assert.Equal(t, "3 1 1 aabbcc", got)
	})

	t.Run("not_published_reports_empty", func(t *testing.T) {
		addr := startTLSADNSServer(t, "_443._tcp.dane.hns.", "")
		provider := NewHNSProvider(addr, nil, TLSASource{})

		got, err := provider.QueryTLSARdata(t.Context(), "dane.hns")
		require.NoError(t, err)
		assert.Empty(t, got)
	})

	t.Run("no_resolver_configured_errors", func(t *testing.T) {
		provider := NewHNSProvider("", nil, TLSASource{})

		_, err := provider.QueryTLSARdata(t.Context(), "dane.hns")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "HNS resolver not configured")
	})
}

// --- ValidateOnChainTLSA ---

func TestValidateOnChainTLSA(t *testing.T) {
	const liveHash = "aabbccdd"

	// run builds a chain-managed (or other-class) binding and runs the gate.
	run := func(t *testing.T, addr string, mkBinding func(svc *DelegatedDomainService) *pluginDb.WebsiteDomain,
		fn func(t *testing.T, svc *DelegatedDomainService, wd *pluginDb.WebsiteDomain)) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
			require.NotNil(tb, svc)
			hnsProv := svc.registry.Get(string(pluginDb.DomainNamespaceHNS)).(*HNSProvider)
			hnsProv.resolverAddr = addr

			wd := mkBinding(svc)
			fn(t, svc, wd)
		}, testOptionsWithHNSResolver("127.0.0.1:53"))
	}

	mkChain := func(domain string) func(*DelegatedDomainService) *pluginDb.WebsiteDomain {
		return func(svc *DelegatedDomainService) *pluginDb.WebsiteDomain {
			wd := &pluginDb.WebsiteDomain{
				WebsiteID: 1, UserID: 1, Domain: domain, Namespace: pluginDb.DomainNamespaceHNS,
				Status: pluginDb.DomainStatusOnchainManaged,
			}
			require.NoError(t, svc.DB().Create(wd).Error)
			return wd
		}
	}

	storeTLSA := func(t *testing.T, svc *DelegatedDomainService, domain, tlsa string) {
		require.NoError(t, svc.persistTLSAKeyMetadata(t.Context(), string(pluginDb.DomainNamespaceHNS), domain, tlsa, ""))
	}

	t.Run("no_stored_identity_fails_with_bootstrap_guidance", func(t *testing.T) {
		run(t, "127.0.0.1:53", mkChain("nodane.hns"), func(t *testing.T, svc *DelegatedDomainService, wd *pluginDb.WebsiteDomain) {
			ok, detail, expected, found, err := svc.ValidateOnChainTLSA(t.Context(), wd)
			require.NoError(t, err)
			assert.False(t, ok)
			assert.Contains(t, detail, "no DANE identity stored")
			assert.Contains(t, detail, "republish")
			assert.Empty(t, expected)
			assert.Empty(t, found)
		})
	})

	t.Run("matching_live_record_passes", func(t *testing.T) {
		addr := startTLSADNSServer(t, "_443._tcp.match.hns.", "3 1 1 "+liveHash)
		run(t, addr, mkChain("match.hns"), func(t *testing.T, svc *DelegatedDomainService, wd *pluginDb.WebsiteDomain) {
			storeTLSA(t, svc, "match.hns", "3 1 1 "+liveHash)

			ok, detail, expected, found, err := svc.ValidateOnChainTLSA(t.Context(), wd)
			require.NoError(t, err)
			assert.True(t, ok)
			assert.NotEmpty(t, detail)
			assert.Equal(t, "3 1 1 "+liveHash, expected)
			assert.Equal(t, "3 1 1 "+liveHash, found)
		})
	})

	t.Run("uppercase_stored_hash_normalizes", func(t *testing.T) {
		addr := startTLSADNSServer(t, "_443._tcp.case.hns.", "3 1 1 "+liveHash)
		run(t, addr, mkChain("case.hns"), func(t *testing.T, svc *DelegatedDomainService, wd *pluginDb.WebsiteDomain) {
			storeTLSA(t, svc, "case.hns", "3 1 1 AABBCCDD")

			ok, _, expected, _, err := svc.ValidateOnChainTLSA(t.Context(), wd)
			require.NoError(t, err)
			assert.True(t, ok)
			assert.Equal(t, "3 1 1 aabbccdd", expected)
		})
	})

	t.Run("not_published_fails_with_missing", func(t *testing.T) {
		// NODATA: the on-chain zone does not serve the TLSA yet.
		addr := startTLSADNSServer(t, "_443._tcp.missing.hns.", "")
		run(t, addr, mkChain("missing.hns"), func(t *testing.T, svc *DelegatedDomainService, wd *pluginDb.WebsiteDomain) {
			storeTLSA(t, svc, "missing.hns", "3 1 1 "+liveHash)

			ok, detail, expected, found, err := svc.ValidateOnChainTLSA(t.Context(), wd)
			require.NoError(t, err)
			assert.False(t, ok)
			assert.Contains(t, detail, "not published")
			assert.Equal(t, "3 1 1 "+liveHash, expected)
			assert.Empty(t, found)
		})
	})

	t.Run("mismatched_live_record_fails_with_expected_found", func(t *testing.T) {
		addr := startTLSADNSServer(t, "_443._tcp.mismatch.hns.", "3 1 1 deadbeef")
		run(t, addr, mkChain("mismatch.hns"), func(t *testing.T, svc *DelegatedDomainService, wd *pluginDb.WebsiteDomain) {
			storeTLSA(t, svc, "mismatch.hns", "3 1 1 "+liveHash)

			ok, detail, expected, found, err := svc.ValidateOnChainTLSA(t.Context(), wd)
			require.NoError(t, err)
			assert.False(t, ok)
			assert.Contains(t, detail, "does not match")
			assert.Equal(t, "3 1 1 aabbccdd", expected)
			assert.Equal(t, "3 1 1 deadbeef", found)
		})
	})

	t.Run("non_chain_loci_are_not_applicable", func(t *testing.T) {
		// Self-hosted/ICANN carry no portal DANE duty; the gate must be a
		// no-op — no DNS query, and OK regardless of stored state.
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)
			require.NotNil(tb, svc)
			hnsProv := svc.registry.Get(string(pluginDb.DomainNamespaceHNS)).(*HNSProvider)
			hnsProv.resolverAddr = "127.0.0.1:1" // unreachable: must never be queried

			db := ctx.DB()
			selfHosted := &pluginDb.WebsiteDomain{
				WebsiteID: 1, UserID: 1, Domain: "sh.hns", Namespace: pluginDb.DomainNamespaceHNS,
				Status: pluginDb.DomainStatusSelfHosted,
			}
			require.NoError(t, db.Create(selfHosted).Error)
			icann := &pluginDb.WebsiteDomain{
				WebsiteID: 1, UserID: 1, Domain: "x.com", Namespace: pluginDb.DomainNamespaceICANN,
				Status: pluginDb.DomainStatusActive, ZoneID: 1,
			}
			require.NoError(t, db.Create(icann).Error)

			for _, wd := range []*pluginDb.WebsiteDomain{selfHosted, icann} {
				ok, detail, _, _, err := svc.ValidateOnChainTLSA(t.Context(), wd)
				require.NoError(t, err)
				assert.True(t, ok, "locus %s", wd.Status)
				assert.Empty(t, detail)
			}
		}, TestOptions)
	})
}
