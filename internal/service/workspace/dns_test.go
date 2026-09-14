// This file tests the per-workspace DNS reconciliation phase (ReconcileDNS)
// and teardown (DeleteDNSRecords): record target resolution from the Coolify
// placement server (address-family-driven A/AAAA), zone resolution from the
// workspace's platform root, the relative record owner naming, idempotent
// record writes, and idempotent two-family teardown.
package workspace

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	apiDTO "go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/coolify"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"gorm.io/gorm"
)

// fakeWorkspaceProvider gains the ResolveServerIP override here (methods may be
// declared in any file of the package): the placement server IP the workspace
// DNS reconciler points records at.
type serverIPFakeProvider struct {
	*fakeWorkspaceProvider

	serverIP    string
	serverIPErr error
	ipCalls     int
}

func (f *serverIPFakeProvider) ResolveServerIP(_ context.Context, _ string) (string, error) {
	f.ipCalls++
	if f.serverIPErr != nil {
		return "", f.serverIPErr
	}
	return f.serverIP, nil
}

// fakeDNSService is a test fake for pluginCore.DNSService. It embeds the
// interface (nil) and overrides only the platform-zone lookup and address
// record write/delete paths used by the DNS reconciliation phase; calling any
// other method panics, which fails the test loudly.
type fakeDNSService struct {
	pluginCore.DNSService

	zones     map[string]*pluginDb.DNSZone
	zoneErr   error
	creates   []dnsRecordRequest
	deletes   []dnsDeleteRequest
	createErr error
	deleteErr error
}

type dnsRecordRequest struct {
	ZoneID  uint
	Name    string
	Type    string
	Content string
	TTL     uint
}

type dnsDeleteRequest struct {
	ZoneID uint
	Name   string
	Type   string
}

func (f *fakeDNSService) GetZoneByDomain(_ context.Context, domain string) (*pluginDb.DNSZone, error) {
	if f.zoneErr != nil {
		return nil, f.zoneErr
	}
	return f.zones[domain], nil
}

func (f *fakeDNSService) CreateRecord(_ context.Context, zoneID uint, name string, recordType string, content string, ttl uint) (*apiDTO.DNSRecord, error) {
	if f.createErr != nil {
		return nil, f.createErr
	}
	f.creates = append(f.creates, dnsRecordRequest{ZoneID: zoneID, Name: name, Type: recordType, Content: content, TTL: ttl})
	return &apiDTO.DNSRecord{ZoneID: zoneID, Name: name, Type: recordType, Content: content, TTL: ttl}, nil
}

func (f *fakeDNSService) DeleteRecord(_ context.Context, zoneID uint, name string, recordType string, _ ...string) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	f.deletes = append(f.deletes, dnsDeleteRequest{ZoneID: zoneID, Name: name, Type: recordType})
	return nil
}

// newFakeDNSService returns the fake DNS service wired by the lifecycle test
// harness: the platform domain the lifecycle workspace rows are bound to
// (example.com, platform domain 10) has a provisioned zone, so Delete's
// teardown deletes the hostname records from it.
func newFakeDNSService() *fakeDNSService {
	return &fakeDNSService{zones: map[string]*pluginDb.DNSZone{"example.com": platformZone()}}
}

// platformZone returns a PowerDNS zone for the workspace test platform root.
func platformZone() *pluginDb.DNSZone {
	return &pluginDb.DNSZone{Model: gorm.Model{ID: 7}, Domain: "example.com"}
}

func newDNSTestService(tb coreTesting.TB, db *gorm.DB, provider coolify.WorkspaceProvider, dnsSvc pluginCore.DNSService) *WorkspaceService {
	tb.Helper()
	bc := &core.BaseComponent{}
	bc.SetDB(db)
	return &WorkspaceService{
		BaseComponent: bc,
		config: &pluginConfig.WorkspaceConfig{
			Enabled: true,
			Coolify: pluginConfig.WorkspaceCoolifyConfig{
				ServerUUID: "srv-1",
			},
		},
		provider:    provider,
		dnsSvc:      dnsSvc,
		mysqlProv:   &fakeEngineer{},
		identityKey: testIdentityPrivateKey(),
	}
}

func TestReconcileDNS_PublishesARecordFromPlacementServer(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertWebsite(tb, db, 1, 1)
		insertPlatformDomain(tb, db, 10, "example.com", "icann", true)
		ws := insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning)

		provider := &serverIPFakeProvider{fakeWorkspaceProvider: &fakeWorkspaceProvider{}, serverIP: "203.0.113.7"}
		dns := &fakeDNSService{zones: map[string]*pluginDb.DNSZone{"example.com": platformZone()}}
		svc := newDNSTestService(tb, db, provider, dns)

		require.NoError(tb, svc.ReconcileDNS(context.Background(), ws))

		require.Len(tb, dns.creates, 1)
		rec := dns.creates[0]
		assert.Equal(t, uint(7), rec.ZoneID)
		assert.Equal(t, "ws-test.build.example.com", ws.Hostname())
		assert.Equal(t, "ws-test.build", rec.Name)
		assert.Equal(t, "A", rec.Type)
		assert.Equal(t, "203.0.113.7", rec.Content)
		assert.Equal(t, uint(workspaceDNSRecordTTL), rec.TTL)
		assert.Equal(t, 1, provider.ipCalls)
	}, workspaceTestOptions)
}

func TestReconcileDNS_IPv6TargetWritesAAAA(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertPlatformDomain(tb, db, 10, "example.com", "icann", true)
		ws := insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning)

		provider := &serverIPFakeProvider{fakeWorkspaceProvider: &fakeWorkspaceProvider{}, serverIP: "2001:db8::1"}
		dns := &fakeDNSService{zones: map[string]*pluginDb.DNSZone{"example.com": platformZone()}}
		svc := newDNSTestService(tb, db, provider, dns)

		require.NoError(tb, svc.ReconcileDNS(context.Background(), ws))
		require.Len(t, dns.creates, 1)
		assert.Equal(t, "AAAA", dns.creates[0].Type)
	}, workspaceTestOptions)
}

func TestReconcileDNS_ZoneMissingFailsRetryably(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertPlatformDomain(tb, db, 10, "example.com", "icann", true)
		ws := insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning)

		provider := &serverIPFakeProvider{fakeWorkspaceProvider: &fakeWorkspaceProvider{}, serverIP: "203.0.113.7"}
		// No zone registered: the platform flow has not provisioned one.
		dns := &fakeDNSService{zones: map[string]*pluginDb.DNSZone{}}
		svc := newDNSTestService(tb, db, provider, dns)

		err := svc.ReconcileDNS(context.Background(), ws)
		require.ErrorIs(t, err, ErrWorkspaceZoneUnavailable)
		assert.Empty(t, dns.creates)
	}, workspaceTestOptions)
}

func TestReconcileDNS_InvalidServerIPFailsClosed(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertPlatformDomain(tb, db, 10, "example.com", "icann", true)
		ws := insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning)

		provider := &serverIPFakeProvider{fakeWorkspaceProvider: &fakeWorkspaceProvider{}, serverIP: "not-an-ip"}
		dns := &fakeDNSService{zones: map[string]*pluginDb.DNSZone{"example.com": platformZone()}}
		svc := newDNSTestService(tb, db, provider, dns)

		// The target is provider-owned data: an unparsable IP is reported
		// verbatim (fail closed) without a sentinel, so the reconciler still
		// retries it as a transient class.
		err := svc.ReconcileDNS(context.Background(), ws)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid ip")
		assert.Empty(t, dns.creates)
	}, workspaceTestOptions)
}

func TestDeleteDNSRecords_RemovesBothAddressFamilies(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertPlatformDomain(tb, db, 10, "example.com", "icann", true)
		ws := insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusDeleting)

		dns := &fakeDNSService{zones: map[string]*pluginDb.DNSZone{"example.com": platformZone()}}
		svc := newDNSTestService(tb, db, &serverIPFakeProvider{fakeWorkspaceProvider: &fakeWorkspaceProvider{}}, dns)

		require.NoError(tb, svc.DeleteDNSRecords(context.Background(), ws))
		require.Len(t, dns.deletes, 2)
		types := []string{dns.deletes[0].Type, dns.deletes[1].Type}
		assert.Contains(t, types, "A")
		assert.Contains(t, types, "AAAA")
		for _, d := range dns.deletes {
			assert.Equal(t, uint(7), d.ZoneID)
			assert.Equal(t, "ws-test.build", d.Name)
		}
	}, workspaceTestOptions)
}

func TestDeleteDNSRecords_MissingZoneIsNoOp(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertPlatformDomain(tb, db, 10, "example.com", "icann", true)
		ws := insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusDeleting)

		dns := &fakeDNSService{zones: map[string]*pluginDb.DNSZone{}}
		svc := newDNSTestService(tb, db, &serverIPFakeProvider{fakeWorkspaceProvider: &fakeWorkspaceProvider{}}, dns)

		require.NoError(tb, svc.DeleteDNSRecords(context.Background(), ws))
		assert.Empty(t, dns.deletes)
	}, workspaceTestOptions)
}

func TestReconcileDNS_RecordWriteFailureWrapsSentinel(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		db := ctx.DB()
		insertPlatformDomain(tb, db, 10, "example.com", "icann", true)
		ws := insertWorkspace(tb, db, 0, 1, 10, pluginDb.WorkspaceStatusProvisioning)

		provider := &serverIPFakeProvider{fakeWorkspaceProvider: &fakeWorkspaceProvider{}, serverIP: "203.0.113.7"}
		dns := &fakeDNSService{
			zones:     map[string]*pluginDb.DNSZone{"example.com": platformZone()},
			createErr: assert.AnError,
		}
		svc := newDNSTestService(tb, db, provider, dns)

		err := svc.ReconcileDNS(context.Background(), ws)
		require.ErrorIs(t, err, ErrWorkspaceDNSRecordFailed)
	}, workspaceTestOptions)
}
