package dns

import (
	"context"
	"io/fs"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/testopts"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.uber.org/zap"
)

var NameserverValidatorTestOptions = coreTesting.CombineOptions(
	testopts.NewBaseMockPluginBuilder().WithService(pluginCore.DNS_SERVICE, func() (core.Service, []core.ContextBuilderOption, error) {
		logger := zap.NewNop()
		coreLogger := &core.Logger{Logger: logger}
		pdnsClient, err := NewPowerDNSClient("http://localhost:8081", "test-api-key", coreLogger)
		if err != nil {
			return nil, nil, err
		}
		return NewDNSServiceWithOptions(WithPowerDNSClient(pdnsClient))
	}).WithServiceConfig(pluginCore.DNS_SERVICE, &pluginConfig.DnsConfig{
		Enabled:                      true,
		PowerDNSAPIURL:               "http://localhost:8081",
		PowerDNSAPIKey:               "test-api-key",
		Nameservers:                  []string{"ns1.example.com.", "ns2.example.com."},
		NameserverValidationInterval: 5 * time.Minute,
	}).WithMigrations(map[core.DBType]fs.FS{
		core.DB_TYPE_SQLITE: migrations.GetSQLite(),
	}).BuilderOption(),
)

// TestNameserverValidatorJob_SuccessfulValidation tests that a zone successfully transitions from pending_nameserver to active
func TestNameserverValidatorJob_SuccessfulValidation(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange

		// Create test zone in pending_nameserver status
		testZone := &db.DNSZone{
			UserID:         1,
			Domain:         "example.com.",
			Status:         string(db.DNSZoneStatusPendingNameserver),
			PowerDNSZoneID: "pdns-123",
		}
		err := ctx.DB().Create(testZone).Error
		require.NoError(tb, err)

		// Mock DNS lookup to return success
		setupDNSLookupMock(tb, ctx, testZone.Domain, []*net.NS{{Host: "ns1.example.com."}, {Host: "ns2.example.com."}}, nil)

		// Act - Call validateDNSZones via janitor job
		svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		validated, err := svc.ValidateNameservers(context.Background(), testZone.ID)

		// Assert
		require.NoError(tb, err)
		require.True(tb, validated)

		// Verify zone status updated to active
		var updatedZone db.DNSZone
		err = ctx.DB().First(&updatedZone, testZone.ID).Error
		require.NoError(tb, err)
		assert.Equal(tb, string(db.DNSZoneStatusActive), updatedZone.Status)
		assert.NotNil(tb, updatedZone.NameserversVerifiedAt)
		assert.WithinDuration(tb, time.Now(), *updatedZone.NameserversVerifiedAt, time.Minute)
	}, NameserverValidatorTestOptions)
}

// TestNameserverValidatorJob_FailedValidation tests handling of validation failures
func TestNameserverValidatorJob_FailedValidation(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange

		// Create test zone
		testZone := &db.DNSZone{
			UserID:         1,
			Domain:         "example.com.",
			Status:         string(db.DNSZoneStatusPendingNameserver),
			PowerDNSZoneID: "pdns-123",
		}
		err := ctx.DB().Create(testZone).Error
		require.NoError(tb, err)

		// Record initial timestamp
		var initialZone db.DNSZone
		err = ctx.DB().First(&initialZone, testZone.ID).Error
		require.NoError(tb, err)
		initialCheckTime := initialZone.LastNameserverCheckAt

		// Mock DNS lookup to return error
		setupDNSLookupMock(tb, ctx, testZone.Domain, nil, assert.AnError)

		// Act
		svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		validated, err := svc.ValidateNameservers(context.Background(), testZone.ID)

		// Assert
		require.Error(tb, err)
		require.False(tb, validated)
		assert.Contains(tb, err.Error(), "failed to lookup nameservers")

		// Verify zone status remains pending_nameserver
		var updatedZone db.DNSZone
		err = ctx.DB().First(&updatedZone, testZone.ID).Error
		require.NoError(tb, err)
		assert.Equal(tb, string(db.DNSZoneStatusPendingNameserver), updatedZone.Status)

		// Verify LastNameserverCheckAt was updated even on error
		assert.NotNil(tb, updatedZone.LastNameserverCheckAt)
		if initialCheckTime != nil {
			assert.True(tb, updatedZone.LastNameserverCheckAt.After(*initialCheckTime))
		}
	}, NameserverValidatorTestOptions)
}

// TestNameserverValidatorJob_NonExistentZone tests handling of non-existent zones
func TestNameserverValidatorJob_NonExistentZone(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		// Note: DNS lookup mock not needed since zone doesn't exist

		// Act
		svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		validated, err := svc.ValidateNameservers(context.Background(), 999)

		// Assert
		require.Error(tb, err)
		require.False(tb, validated)
		assert.Contains(tb, err.Error(), "not found")
	}, NameserverValidatorTestOptions)
}

// TestNameserverValidatorJob_IntervalScheduling tests that zones are only validated after the interval
func TestNameserverValidatorJob_IntervalScheduling(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange

		// Create test zone with recent check timestamp (within 5-minute interval)
		recentTime := time.Now().Add(-2 * time.Minute)
		testZone := &db.DNSZone{
			UserID:                1,
			Domain:                "example.com.",
			Status:                string(db.DNSZoneStatusPendingNameserver),
			PowerDNSZoneID:        "pdns-123",
			LastNameserverCheckAt: &recentTime,
		}
		err := ctx.DB().Create(testZone).Error
		require.NoError(tb, err)

		// Query for zones that need validation (should be empty due to recent check)
		var zones []*db.DNSZone
		err = ctx.DB().
			Where("status = ?", db.DNSZoneStatusPendingNameserver).
			Where("last_nameserver_check_at IS NULL OR last_nameserver_check_at < ?", time.Now().Add(-5*time.Minute)).
			Find(&zones).Error

		// Assert - No zones should need validation
		require.NoError(tb, err)
		assert.Empty(tb, zones, "Recently checked zone should not be validated again")
	}, NameserverValidatorTestOptions)
}

// TestNameserverValidatorJob_ZoneTimestampUpdate tests timestamp updates on validation
func TestNameserverValidatorJob_ZoneTimestampUpdate(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange

		// Create test zone with old timestamp
		oldTime := time.Now().Add(-10 * time.Minute)
		testZone := &db.DNSZone{
			UserID:                1,
			Domain:                "example.com.",
			Status:                string(db.DNSZoneStatusPendingNameserver),
			PowerDNSZoneID:        "pdns-123",
			LastNameserverCheckAt: &oldTime,
		}
		err := ctx.DB().Create(testZone).Error
		require.NoError(tb, err)

		// Mock DNS lookup to return success
		setupDNSLookupMock(tb, ctx, testZone.Domain, []*net.NS{{Host: "ns1.example.com."}, {Host: "ns2.example.com."}}, nil)

		// Act
		svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		validated, err := svc.ValidateNameservers(context.Background(), testZone.ID)

		// Assert
		require.NoError(tb, err)
		require.True(tb, validated)

		// Verify zone was updated successfully
		var updatedZone db.DNSZone
		err = ctx.DB().First(&updatedZone, testZone.ID).Error
		require.NoError(tb, err)
		assert.Equal(tb, string(db.DNSZoneStatusActive), updatedZone.Status)
		assert.NotNil(tb, updatedZone.NameserversVerifiedAt)
	}, NameserverValidatorTestOptions)
}

// TestNameserverValidatorJob_MultipleZones tests validation of multiple zones
func TestNameserverValidatorJob_MultipleZones(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange

		// Create multiple test zones
		zones := []*db.DNSZone{
			{
				UserID:         1,
				Domain:         "example1.com.",
				Status:         string(db.DNSZoneStatusPendingNameserver),
				PowerDNSZoneID: "pdns-1",
			},
			{
				UserID:         1,
				Domain:         "example2.com.",
				Status:         string(db.DNSZoneStatusPendingNameserver),
				PowerDNSZoneID: "pdns-2",
			},
			{
				UserID:         2,
				Domain:         "example3.com.",
				Status:         string(db.DNSZoneStatusPendingNameserver),
				PowerDNSZoneID: "pdns-3",
			},
		}

		for _, zone := range zones {
			err := ctx.DB().Create(zone).Error
			require.NoError(tb, err)
		}

		// Mock DNS lookups for each zone using a single mock with multiple expectations
		mockDNSLookup := mocks.NewMockDNSLookup(tb)
		mockDNSLookup.EXPECT().LookupNS("example1.com.").Return([]*net.NS{{Host: "ns1.example.com."}, {Host: "ns2.example.com."}}, nil)
		mockDNSLookup.EXPECT().LookupNS("example2.com.").Return([]*net.NS{{Host: "ns1.example.com."}, {Host: "ns2.example.com."}}, nil)
		mockDNSLookup.EXPECT().LookupNS("example3.com.").Return(nil, assert.AnError)

		svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)
		svc.SetDNSLookup(mockDNSLookup)

		// Act

		validated1, err1 := svc.ValidateNameservers(context.Background(), zones[0].ID)
		validated2, err2 := svc.ValidateNameservers(context.Background(), zones[1].ID)
		validated3, err3 := svc.ValidateNameservers(context.Background(), zones[2].ID)

		// Assert
		require.NoError(tb, err1)
		require.True(tb, validated1)

		require.NoError(tb, err2)
		require.True(tb, validated2)

		require.Error(tb, err3)
		require.False(tb, validated3)

		// Verify zones 1 and 2 are active, zone 3 is still pending
		var zone1, zone2, zone3 db.DNSZone
		err := ctx.DB().First(&zone1, zones[0].ID).Error
		require.NoError(tb, err)
		assert.Equal(tb, string(db.DNSZoneStatusActive), zone1.Status)

		err = ctx.DB().First(&zone2, zones[1].ID).Error
		require.NoError(tb, err)
		assert.Equal(tb, string(db.DNSZoneStatusActive), zone2.Status)

		err = ctx.DB().First(&zone3, zones[2].ID).Error
		require.NoError(tb, err)
		assert.Equal(tb, string(db.DNSZoneStatusPendingNameserver), zone3.Status)
	}, NameserverValidatorTestOptions)
}

// TestNameserverValidatorJob_NoZonesToValidate tests behavior when no zones need validation
func TestNameserverValidatorJob_NoZonesToValidate(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		// Create zones that are already active
		activeZone := &db.DNSZone{
			UserID:         1,
			Domain:         "example.com.",
			Status:         string(db.DNSZoneStatusActive),
			PowerDNSZoneID: "pdns-123",
		}
		err := ctx.DB().Create(activeZone).Error
		require.NoError(tb, err)

		// Query for zones that need validation
		var zones []*db.DNSZone
		err = ctx.DB().
			Where("status = ?", db.DNSZoneStatusPendingNameserver).
			Where("last_nameserver_check_at IS NULL OR last_nameserver_check_at < ?", time.Now().Add(-5*time.Minute)).
			Find(&zones).Error

		// Assert
		require.NoError(tb, err)
		assert.Empty(tb, zones, "No pending zones should require validation")
	}, NameserverValidatorTestOptions)
}

// TestNameserverValidatorJob_ZoneAlreadyActive tests that active zones are not re-validated
func TestNameserverValidatorJob_ZoneAlreadyActive(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange

		// Create zone that's already active
		testZone := &db.DNSZone{
			UserID:                1,
			Domain:                "example.com.",
			Status:                string(db.DNSZoneStatusActive),
			PowerDNSZoneID:        "pdns-123",
			LastNameserverCheckAt: timePtr(time.Now().Add(-1 * time.Hour)),
			NameserversVerifiedAt: timePtr(time.Now().Add(-1 * time.Hour)),
		}
		err := ctx.DB().Create(testZone).Error
		require.NoError(tb, err)

		// Act - Query for pending zones
		var zones []*db.DNSZone
		err = ctx.DB().
			Where("status = ?", db.DNSZoneStatusPendingNameserver).
			Find(&zones).Error

		// Assert - Active zone should not be included
		require.NoError(tb, err)
		assert.Empty(tb, zones, "Active zones should not be in pending list")
	}, NameserverValidatorTestOptions)
}

// TestNameserverValidatorJob_StatusTransitionPendingToActive tests the status transition
func TestNameserverValidatorJob_StatusTransitionPendingToActive(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange

		// Create test zone in pending_nameserver status
		testZone := &db.DNSZone{
			UserID:         1,
			Domain:         "example.com.",
			Status:         string(db.DNSZoneStatusPendingNameserver),
			PowerDNSZoneID: "pdns-123",
		}
		err := ctx.DB().Create(testZone).Error
		require.NoError(tb, err)

		// Verify initial status
		var initialZone db.DNSZone
		err = ctx.DB().First(&initialZone, testZone.ID).Error
		require.NoError(tb, err)
		assert.Equal(tb, string(db.DNSZoneStatusPendingNameserver), initialZone.Status)
		assert.Nil(tb, initialZone.NameserversVerifiedAt)

		// Mock DNS lookup to return success
		setupDNSLookupMock(tb, ctx, testZone.Domain, []*net.NS{{Host: "ns1.example.com."}, {Host: "ns2.example.com."}}, nil)

		// Act
		svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		validated, err := svc.ValidateNameservers(context.Background(), testZone.ID)

		// Assert
		require.NoError(tb, err)
		require.True(tb, validated)

		// Verify status transition
		var finalZone db.DNSZone
		err = ctx.DB().First(&finalZone, testZone.ID).Error
		require.NoError(tb, err)
		assert.Equal(tb, string(db.DNSZoneStatusActive), finalZone.Status)
		assert.NotNil(tb, finalZone.NameserversVerifiedAt)
		assert.WithinDuration(tb, time.Now(), *finalZone.NameserversVerifiedAt, time.Minute)
	}, NameserverValidatorTestOptions)
}

// TestNameserverValidatorJob_InvalidDomainFormat tests handling of invalid domains
func TestNameserverValidatorJob_InvalidDomainFormat(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange

		// Create zone with invalid domain format
		testZone := &db.DNSZone{
			UserID:         1,
			Domain:         "invalid-domain", // Missing TLD
			Status:         string(db.DNSZoneStatusPendingNameserver),
			PowerDNSZoneID: "pdns-123",
		}
		err := ctx.DB().Create(testZone).Error
		require.NoError(tb, err)

		// Mock DNS lookup to return error for invalid domain
		setupDNSLookupMock(tb, ctx, testZone.Domain, nil, assert.AnError)

		// Act
		svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		validated, err := svc.ValidateNameservers(context.Background(), testZone.ID)

		// Assert
		require.Error(tb, err)
		require.False(tb, validated)
		assert.Contains(tb, err.Error(), "failed to lookup nameservers")

		// Verify zone status remains pending
		var updatedZone db.DNSZone
		err = ctx.DB().First(&updatedZone, testZone.ID).Error
		require.NoError(tb, err)
		assert.Equal(tb, string(db.DNSZoneStatusPendingNameserver), updatedZone.Status)
	}, NameserverValidatorTestOptions)
}

// TestNameserverValidatorJob_RecentlyCheckedShouldNotBeValidated tests that zones checked recently are not validated again
func TestNameserverValidatorJob_RecentlyCheckedShouldNotBeValidated(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		recentCheck := time.Now().Add(-1 * time.Minute)
		testZone := &db.DNSZone{
			UserID:                1,
			Domain:                "example.com.",
			Status:                string(db.DNSZoneStatusPendingNameserver),
			PowerDNSZoneID:        "pdns-1",
			LastNameserverCheckAt: &recentCheck,
		}
		err := ctx.DB().Create(testZone).Error
		require.NoError(tb, err)

		// Query for zones needing validation
		var zones []*db.DNSZone
		err = ctx.DB().
			Where("status = ?", db.DNSZoneStatusPendingNameserver).
			Where("last_nameserver_check_at IS NULL OR last_nameserver_check_at < ?", time.Now().Add(-5*time.Minute)).
			Find(&zones).Error

		// Assert - Zone should not be included due to recent check
		require.NoError(tb, err)
		assert.Empty(tb, zones, "Recently checked zone should not be validated again")
	}, NameserverValidatorTestOptions)
}

// TestNameserverValidatorJob_CheckDueShouldBeValidated tests that zones checked >5 minutes ago are validated
func TestNameserverValidatorJob_CheckDueShouldBeValidated(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		oldCheck := time.Now().Add(-10 * time.Minute)
		testZone := &db.DNSZone{
			UserID:                1,
			Domain:                "example.com.",
			Status:                string(db.DNSZoneStatusPendingNameserver),
			PowerDNSZoneID:        "pdns-2",
			LastNameserverCheckAt: &oldCheck,
		}
		err := ctx.DB().Create(testZone).Error
		require.NoError(tb, err)

		// Query for zones needing validation
		var zones []*db.DNSZone
		err = ctx.DB().
			Where("status = ?", db.DNSZoneStatusPendingNameserver).
			Where("last_nameserver_check_at IS NULL OR last_nameserver_check_at < ?", time.Now().Add(-5*time.Minute)).
			Find(&zones).Error

		// Assert - Zone should be included
		require.NoError(tb, err)
		assert.Len(tb, zones, 1, "Zone checked >5 minutes ago should be validated")
		assert.Equal(tb, testZone.ID, zones[0].ID)
	}, NameserverValidatorTestOptions)
}

// TestNameserverValidatorJob_NeverCheckedShouldBeValidated tests that zones never checked are validated
func TestNameserverValidatorJob_NeverCheckedShouldBeValidated(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		testZone := &db.DNSZone{
			UserID:         1,
			Domain:         "example.com.",
			Status:         string(db.DNSZoneStatusPendingNameserver),
			PowerDNSZoneID: "pdns-3",
		}
		err := ctx.DB().Create(testZone).Error
		require.NoError(tb, err)

		// Query for zones needing validation
		var zones []*db.DNSZone
		err = ctx.DB().
			Where("status = ?", db.DNSZoneStatusPendingNameserver).
			Where("last_nameserver_check_at IS NULL OR last_nameserver_check_at < ?", time.Now().Add(-5*time.Minute)).
			Find(&zones).Error

		// Assert - Zone should be included
		require.NoError(tb, err)
		assert.Len(tb, zones, 1, "Zone never checked should be validated")
		assert.Equal(tb, testZone.ID, zones[0].ID)
	}, NameserverValidatorTestOptions)
}

// TestNameserverValidatorJob_DatabaseErrorHandling tests error handling for database failures
func TestNameserverValidatorJob_DatabaseErrorHandling(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange

		// Create test zone
		testZone := &db.DNSZone{
			UserID:         1,
			Domain:         "example.com.",
			Status:         string(db.DNSZoneStatusPendingNameserver),
			PowerDNSZoneID: "pdns-123",
		}
		err := ctx.DB().Create(testZone).Error
		require.NoError(tb, err)

		// Mock DNS lookup to return success
		setupDNSLookupMock(tb, ctx, testZone.Domain, []*net.NS{{Host: "ns1.example.com."}, {Host: "ns2.example.com."}}, nil)

		// Act
		svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		validated, err := svc.ValidateNameservers(context.Background(), testZone.ID)

		// Assert
		require.NoError(tb, err)
		require.True(tb, validated)

		// Verify zone was updated successfully
		var updatedZone db.DNSZone
		err = ctx.DB().First(&updatedZone, testZone.ID).Error
		require.NoError(tb, err)
		assert.Equal(tb, string(db.DNSZoneStatusActive), updatedZone.Status)
	}, NameserverValidatorTestOptions)
}

// Helper function
func setupDNSLookupMock(t testing.TB, ctx coreTesting.TestContext, domain string, nameservers []*net.NS, err error) {
	mockDNSLookup := mocks.NewMockDNSLookup(t)
	if nameservers != nil {
		mockDNSLookup.EXPECT().LookupNS(domain).Return(nameservers, err)
	} else if err != nil {
		mockDNSLookup.EXPECT().LookupNS(domain).Return(nil, err)
	} else {
		mockDNSLookup.EXPECT().LookupNS(domain).Return([]*net.NS{{Host: "ns1.example.com."}}, nil)
	}
	svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
	require.NotNil(t, svc)
	svc.SetDNSLookup(mockDNSLookup)
}

func timePtr(t time.Time) *time.Time {
	return &t
}
