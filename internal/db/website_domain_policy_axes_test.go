package db

// Policy-axis accessor tests: the persisted axis columns must validate values against the
// internal/domainpolicy enums and fail closed on unknown values, and the
// migration must apply cleanly to both a fresh schema and a legacy
// (pre-axes-column) schema with existing rows.

import (
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/domainpolicy"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

func TestWebsiteDomain_SetLifecycleStatus_FailClosed(t *testing.T) {
	wd := &WebsiteDomain{}
	require.NoError(t, wd.SetLifecycleStatus(domainpolicy.LifecycleActive))
	assert.Equal(t, "active", *wd.LifecycleStatus)
	require.Error(t, wd.SetLifecycleStatus(domainpolicy.LifecycleUnknown))
	require.Error(t, wd.SetLifecycleStatus(domainpolicy.Lifecycle(99)))

	wd = &WebsiteDomain{}
	require.NoError(t, wd.SetAuthorityLocus(domainpolicy.AuthorityLocusPortalZone))
	require.Error(t, wd.SetAuthorityLocus(domainpolicy.AuthorityLocusUnknown))

	wd = &WebsiteDomain{}
	require.NoError(t, wd.SetResolutionRoute(domainpolicy.ResolutionRouteCrossChain))
	require.Error(t, wd.SetResolutionRoute(domainpolicy.ResolutionRouteUnknown))

	wd = &WebsiteDomain{}
	require.NoError(t, wd.SetResolutionBackend(domainpolicy.BackendEthereum))
	require.Error(t, wd.SetResolutionBackend(domainpolicy.EmptyBackendID))

	wd = &WebsiteDomain{}
	require.NoError(t, wd.SetHostingRequest(domainpolicy.HostingRequestPortal))
	require.Error(t, wd.SetHostingRequest(domainpolicy.HostingRequestUnknown))
}

func TestWebsiteDomain_SetPolicy_FailClosed(t *testing.T) {
	wd := &WebsiteDomain{}
	require.NoError(t, wd.SetPolicy(domainpolicy.ProfileIDICANNPortal, domainpolicy.ProfileVersion1))
	require.Error(t, wd.SetPolicy(domainpolicy.EmptyProfileID, domainpolicy.ProfileVersion1))
	require.Error(t, wd.SetPolicy(domainpolicy.ProfileIDICANNPortal, domainpolicy.ProfileVersion(0)))
}

func TestWebsiteDomain_Getters_UnsetColumnsAreTypedErrors(t *testing.T) {
	wd := &WebsiteDomain{}
	_, err := wd.GetLifecycleStatus()
	require.ErrorIs(t, err, ErrPolicyAxisUnset)
	_, err = wd.GetAuthorityLocus()
	require.ErrorIs(t, err, ErrPolicyAxisUnset)
	_, err = wd.GetResolutionRoute()
	require.ErrorIs(t, err, ErrPolicyAxisUnset)
	_, err = wd.GetResolutionBackend()
	require.ErrorIs(t, err, ErrPolicyAxisUnset)
	_, err = wd.GetHostingRequest()
	require.ErrorIs(t, err, ErrPolicyAxisUnset)
	_, _, err = wd.GetPolicy()
	require.ErrorIs(t, err, ErrPolicyAxisUnset)
	_, err = wd.GetReconciliationStatus()
	require.ErrorIs(t, err, ErrPolicyAxisUnset)
}

func TestWebsiteDomain_Getters_RejectUnknownPersistedValues(t *testing.T) {
	garbage := "definitely-not-an-enum"
	wd := &WebsiteDomain{LifecycleStatus: &garbage}
	_, err := wd.GetLifecycleStatus()
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrPolicyAxisUnset)

	versionZero := 0
	wd = &WebsiteDomain{
		PolicyID:      policyIDPtr("icann.portal.current-v1"),
		PolicyVersion: &versionZero,
	}
	_, _, err = wd.GetPolicy()
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrPolicyAxisUnset)
}

func TestWebsiteDomain_PolicyAxesMapped_And_PersistedClass(t *testing.T) {
	wd := &WebsiteDomain{}
	assert.False(t, wd.PolicyAxesMapped())
	wd.SetReconciliationStatus(PolicyReconciliationError)
	assert.False(t, wd.PolicyAxesMapped())

	wd.SetReconciliationStatus(PolicyReconciliationMapped)
	assert.True(t, wd.PolicyAxesMapped())

	require.NoError(t, wd.SetAuthorityLocus(domainpolicy.AuthorityLocusChain))
	cls, err := wd.PersistedDomainClass()
	require.NoError(t, err)
	assert.Equal(t, ClassOnChainManaged, cls)

	require.NoError(t, wd.SetAuthorityLocus(domainpolicy.AuthorityLocusOperatorZone))
	cls, err = wd.PersistedDomainClass()
	require.NoError(t, err)
	assert.Equal(t, ClassPortalManaged, cls)

	// An invalid persisted authority fails closed rather than guessing a class.
	bad := "corrupt"
	wd.AuthorityLocus = &bad
	_, err = wd.PersistedDomainClass()
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrPolicyAxisUnset)
}

func axesFixture() (DomainPolicyAxes, *WebsiteDomain) {
	axes := DomainPolicyAxes{
		Lifecycle: domainpolicy.LifecycleActive,
		Authority: domainpolicy.AuthorityLocusPortalZone,
		Route:     domainpolicy.ResolutionRouteStandardDNS,
		Backend:   domainpolicy.BackendSystemDNS,
		Hosting:   domainpolicy.HostingRequestPortal,
		PolicyID:  domainpolicy.ProfileIDICANNPortal,
		PolicyVer: domainpolicy.ProfileVersion1,
	}
	return axes, &WebsiteDomain{}
}

func policyIDPtr(v string) *string {
	copied := v
	return &copied
}

func TestWebsiteDomain_ApplyAxes_AndColumns(t *testing.T) {
	axes, _ := axesFixture()
	cols := axes.Columns()
	assert.Equal(t, PolicyReconciliationMapped, cols["reconciliation_status"])
	assert.Equal(t, "active", cols["lifecycle_status"])
	assert.Equal(t, "portal-zone", cols["authority_locus"])
	assert.Equal(t, "standard-dns", cols["resolution_route"])
	assert.Equal(t, "system-dns", cols["resolution_backend"])
	assert.Equal(t, "portal", cols["hosting_request"])
	assert.Equal(t, "icann.portal.current-v1", cols["policy_id"])
	assert.Equal(t, 1, cols["policy_version"])

	_, wd := axesFixture()
	require.NoError(t, wd.ApplyAxes(axes))
	assert.True(t, wd.PolicyAxesMapped())
	got, err := wd.GetLifecycleStatus()
	require.NoError(t, err)
	assert.Equal(t, domainpolicy.LifecycleActive, got)
	gotRoute, err := wd.GetResolutionRoute()
	require.NoError(t, err)
	assert.Equal(t, domainpolicy.ResolutionRouteStandardDNS, gotRoute)
	id, ver, err := wd.GetPolicy()
	require.NoError(t, err)
	assert.Equal(t, domainpolicy.ProfileIDICANNPortal, id)
	assert.Equal(t, domainpolicy.ProfileVersion1, ver)
}

func TestWebsiteDomain_AxesPersist_RoundTripThroughDB(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		gormDB := ctx.DB()
		axes, _ := axesFixture()
		wd := &WebsiteDomain{
			WebsiteID: 1,
			UserID:    1,
			Domain:    "axes-roundtrip.com",
			Namespace: DomainNamespaceICANN,
			Status:    DomainStatusActive,
		}
		require.NoError(tb, wd.ApplyAxes(axes))
		require.NoError(tb, gormDB.Create(wd).Error)

		var reloaded WebsiteDomain
		require.NoError(tb, gormDB.Where("domain = ?", "axes-roundtrip.com").First(&reloaded).Error)
		assert.True(tb, reloaded.PolicyAxesMapped())
		status, err := reloaded.GetReconciliationStatus()
		require.NoError(tb, err)
		assert.Equal(tb, PolicyReconciliationMapped, status)
		id, ver, err := reloaded.GetPolicy()
		require.NoError(tb, err)
		assert.Equal(tb, axes.PolicyID, id)
		assert.Equal(tb, axes.PolicyVer, ver)

		// Simulate legacy-only drift: mutate ONLY the legacy status column;
		// the axes stay as stored (staleness detection is the reader's job,
		// not the storage's).
		require.NoError(tb, gormDB.Model(&reloaded).Update("status", DomainStatusOnchainManaged).Error)
		var drifted WebsiteDomain
		require.NoError(tb, gormDB.Where("domain = ?", "axes-roundtrip.com").Where("deleted_at IS NULL").First(&drifted).Error)
		assert.True(tb, drifted.PolicyAxesMapped())
		assert.Equal(tb, DomainStatusOnchainManaged, drifted.Status)
		assert.Equal(tb, axes.Lifecycle, mustLifecycle(tb, &drifted))
	}, dbTestOptions)
}

func mustLifecycle(tb coreTesting.TB, wd *WebsiteDomain) domainpolicy.Lifecycle {
	tb.Helper()
	v, err := wd.GetLifecycleStatus()
	require.NoError(tb, err)
	return v
}

// axesColumns are the columns this PR adds to website_domains.
var axesColumns = []string{
	"lifecycle_status", "authority_locus", "resolution_route", "resolution_backend",
	"hosting_request", "policy_id", "policy_version", "reconciliation_status",
}

// axesMigrationFile is the sqlite migration file for the persisted policy
// axes. It is
// excluded from the legacy-fixture FS so the legacy test can re-apply its Up
// statements by hand against the pre-axes schema.
const axesMigrationFile = "20260907090000_add_domain_policy_axes.sql"

func TestWebsiteDomain_AxesMigration_FreshDB(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		gormDB := ctx.DB()
		notNullByName := map[string]int64{}
		rows, err := gormDB.Raw("PRAGMA table_info(website_domains)").Rows()
		require.NoError(tb, err)
		for rows.Next() {
			var cid int64
			var name, colType string
			var notNull, pk int64
			var dflt any
			require.NoError(tb, rows.Scan(&cid, &name, &colType, &notNull, &dflt, &pk))
			notNullByName[name] = notNull
		}
		require.NoError(tb, rows.Err())
		for _, col := range axesColumns {
			notNull, ok := notNullByName[col]
			require.True(tb, ok, "axis column %s missing from website_domains", col)
			assert.Equal(tb, int64(0), notNull, "axis column %s must be nullable", col)
		}
	}, dbTestOptions)
}

func TestWebsiteDomain_AxesMigration_LegacyFixtureDB(t *testing.T) {
	legacyFS := mapFSWithoutFile(t, migrations.GetSQLite(), axesMigrationFile)
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		gormDB := ctx.DB()

		// Seed a legacy row exactly as the pre-axes schema would, using raw
		// SQL with ONLY the legacy columns (the struct now carries axis fields
		// the legacy schema has no columns for). Then apply the new
		// migration's Up statements by hand and prove the migration is a
		// clean additive NULLable change over a populated legacy table.
		require.NoError(tb, gormDB.Exec(
			"INSERT INTO website_domains (website_id, user_id, domain, namespace, zone_id, status, dns_hosting_enabled) VALUES (7, 7, 'legacy.example.com', 'icann', 3, 'active', 1)",
		).Error)

		for _, stmt := range sqliteUpStatements(t, migrations.GetSQLite(), axesMigrationFile) {
			require.NoError(tb, gormDB.Exec(stmt).Error, "statement: %s", stmt)
		}

		var wd WebsiteDomain
		require.NoError(tb, gormDB.Where("domain = ?", "legacy.example.com").First(&wd).Error)
		// Legacy data untouched.
		assert.Equal(tb, uint(3), wd.ZoneID)
		assert.True(tb, wd.DNSHostingEnabled)
		assert.Equal(tb, DomainStatusActive, wd.Status)
		// Axes columns exist and are NULL (no SQL backfill of legacy rows).
		assert.False(tb, wd.PolicyAxesMapped())
		_, err := wd.GetLifecycleStatus()
		require.ErrorIs(tb, err, ErrPolicyAxisUnset)

		// The backfill filter runs against the real index.
		var count int64
		require.NoError(tb, gormDB.Model(&WebsiteDomain{}).Where("reconciliation_status IS NULL").Count(&count).Error)
		assert.Equal(tb, int64(1), count)
	}, coreTesting.WithSQLitePluginMigrations(internal.ProtocolName, legacyFS))
}

// mapFSWithoutFile copies a migrations fs into an fstest.MapFS minus one file,
// producing a "legacy" pre-axes migration set for fixture tests.
func mapFSWithoutFile(t testing.TB, base fs.FS, name string) fstest.MapFS {
	t.Helper()
	out := fstest.MapFS{}
	require.NoError(t, fs.WalkDir(base, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if path == name {
			return nil
		}
		data, rerr := fs.ReadFile(base, path)
		require.NoError(t, rerr)
		out[path] = &fstest.MapFile{Data: data}
		return nil
	}))
	require.NotContains(t, out, name)
	return out
}

// sqliteUpStatements extracts the executable statements of a goose Up section.
func sqliteUpStatements(tb coreTesting.TB, base fs.FS, name string) []string {
	tb.Helper()
	data, err := fs.ReadFile(base, name)
	require.NoError(tb, err)
	sql := string(data)
	up := strings.SplitN(sql, "-- +goose Up", 2)[1]
	up = strings.SplitN(up, "-- +goose Down", 2)[0]
	// Strip comment lines BEFORE splitting on ";" so comment prose containing
	// semicolons cannot split a statement.
	var body []string
	for _, line := range strings.Split(up, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}
		body = append(body, trimmed)
	}
	require.NotEmpty(tb, body)
	return strings.Split(strings.Join(body, "\n"), ";")
}
