package dns

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/dns/powerdns"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.uber.org/zap"
)

// Helper function to get protocol config pointer
func getTestProtocolConfig() *pluginConfig.ProtocolConfig {
	return &pluginConfig.ProtocolConfig{
		DnsHosting: pluginConfig.Config{
			Enabled: true,
			PowerDNSAPIURL:    "http://localhost:8081",
			PowerDNSAPIKey:    "test-api-key",
			Nameservers:       []string{"ns1.example.com.", "ns2.example.com."},
		},
	}
}

// mockPowerDNSServer is a shared mock PowerDNS server for all tests
var mockPowerDNSServer *httptest.Server

func TestMain(m *testing.M) {
	// Start mock PowerDNS server
	mockPowerDNSServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Default handler - return 404 for unhandled requests
		w.WriteHeader(http.StatusNotFound)
	}))

	code := m.Run()

	mockPowerDNSServer.Close()

	os.Exit(code)
}

var TestOptions = coreTesting.CombineOptions(
	coreTesting.WithServiceFactory(pluginCore.DNS_SERVICE, func() (core.Service, []core.ContextBuilderOption, error) {
		logger := zap.NewNop()
		coreLogger := &core.Logger{Logger: logger}
		pdnsClient, err := NewPowerDNSClient(mockPowerDNSServer.URL, "test-api-key", coreLogger)
		if err != nil {
			return nil, nil, err
		}
		return NewDNSServiceWithOptions(WithPowerDNSClient(pdnsClient))
	}),
	coreTesting.WithProtocolConfig(internal.ProtocolName, getTestProtocolConfig()),
	coreTesting.WithConfig("plugin.ipfs.protocol.dns_hosting.dns_hosting_enabled", true),
	coreTesting.WithConfig("plugin.ipfs.protocol.dns_hosting.powerdns_api_url", "http://localhost:8081"),
	coreTesting.WithConfig("plugin.ipfs.protocol.dns_hosting.powerdns_api_key", "test-api-key"),
	coreTesting.WithConfig("plugin.ipfs.protocol.dns_hosting.nameservers", []string{"ns1.example.com.", "ns2.example.com."}),
	coreTesting.WithSQLitePluginMigrations(
		internal.ProtocolName, migrations.GetSQLite(),
	),
)

func TestDNSServiceCreateZone(t *testing.T) {
	// Create a mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request method and path
		if r.Method != http.MethodPost {
			t.Errorf("expected POST request, got %s", r.Method)
		}

		if r.URL.Path != "/servers/localhost/zones" {
			t.Errorf("expected path /servers/localhost/zones, got %s", r.URL.Path)
		}

		// Verify API key header
		apiKey := r.Header.Get("X-API-Key")
		if apiKey != "test-api-key" {
			t.Errorf("expected API key 'test-api-key', got '%s'", apiKey)
		}

		// Return success response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(powerdns.Zone{
			Id:   new("example.com."),
			Name: new("example.com."),
			Kind: (*powerdns.ZoneKind)(new("Native")),
		})
	}))
	defer server.Close()

	// Create test options with mocked PowerDNS client
	testOptions := coreTesting.CombineOptions(
		coreTesting.WithServiceFactory(pluginCore.DNS_SERVICE, func() (core.Service, []core.ContextBuilderOption, error) {
			logger := zap.NewNop()
			coreLogger := &core.Logger{Logger: logger}
			pdnsClient, err := NewPowerDNSClient(server.URL, "test-api-key", coreLogger)
			if err != nil {
				return nil, nil, err
			}
			return NewDNSServiceWithOptions(WithPowerDNSClient(pdnsClient))
		}),
		coreTesting.WithProtocolConfig(internal.ProtocolName, getTestProtocolConfig()),
		coreTesting.WithConfig("plugin.ipfs.protocol.dns_hosting.dns_hosting_enabled", true),
		coreTesting.WithConfig("plugin.ipfs.protocol.dns_hosting.powerdns_api_url", "http://localhost:8081"),
		coreTesting.WithConfig("plugin.ipfs.protocol.dns_hosting.powerdns_api_key", "test-api-key"),
		coreTesting.WithConfig("plugin.ipfs.protocol.dns_hosting.nameservers", []string{"ns1.example.com.", "ns2.example.com."}),
		coreTesting.WithSQLitePluginMigrations(
			internal.ProtocolName, migrations.GetSQLite(),
		),
	)

	
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		// Test successful zone creation
		zone, err := svc.CreateZone(ctx, "example.com.", 1)
		require.NoError(tb, err)
		require.NotNil(tb, zone)
		require.Equal(tb, "example.com.", zone.Domain)
		require.Equal(tb, uint(1), zone.UserID)
		require.Equal(tb, string(pluginDb.DNSZoneStatusPendingNameserver), zone.Status)
	}, testOptions)
}

func TestDNSServiceCreateZoneInvalidDomain(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		// Test invalid domain format
		_, err := svc.CreateZone(ctx, "invalid-domain", 1)
		require.Error(tb, err)
	}, TestOptions)
}

func TestDNSServiceCreateZoneDuplicate(t *testing.T) {
	// Create a mock HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request method and path
		if r.Method != http.MethodPost {
			t.Errorf("expected POST request, got %s", r.Method)
		}

		if r.URL.Path != "/servers/localhost/zones" {
			t.Errorf("expected path /servers/localhost/zones, got %s", r.URL.Path)
		}

		// Return success response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(powerdns.Zone{
			Id:   new("example.com."),
			Name: new("example.com."),
			Kind: (*powerdns.ZoneKind)(new("Native")),
		})
	}))
	defer server.Close()

	// Create test options with mocked PowerDNS client
	testOptions := coreTesting.CombineOptions(
		coreTesting.WithServiceFactory(pluginCore.DNS_SERVICE, func() (core.Service, []core.ContextBuilderOption, error) {
			logger := zap.NewNop()
			coreLogger := &core.Logger{Logger: logger}
			pdnsClient, err := NewPowerDNSClient(server.URL, "test-api-key", coreLogger)
			if err != nil {
				return nil, nil, err
			}
			return NewDNSServiceWithOptions(WithPowerDNSClient(pdnsClient))
		}),
		coreTesting.WithProtocolConfig(internal.ProtocolName, getTestProtocolConfig()),
		coreTesting.WithConfig("plugin.ipfs.protocol.dns_hosting.dns_hosting_enabled", true),
		coreTesting.WithConfig("plugin.ipfs.protocol.dns_hosting.powerdns_api_url", "http://localhost:8081"),
		coreTesting.WithConfig("plugin.ipfs.protocol.dns_hosting.powerdns_api_key", "test-api-key"),
		coreTesting.WithConfig("plugin.ipfs.protocol.dns_hosting.nameservers", []string{"ns1.example.com.", "ns2.example.com."}),
		coreTesting.WithSQLitePluginMigrations(
			internal.ProtocolName, migrations.GetSQLite(),
		),
	)

	
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		// Create first zone
		zone1, err := svc.CreateZone(ctx, "example.com.", 1)
		require.NoError(tb, err)

		// Try to create duplicate zone
		zone2, err := svc.CreateZone(ctx, "example.com.", 1)
		require.Error(tb, err)
		require.Nil(tb, zone2)

		// Verify first zone still exists
		retrievedZone, err := svc.GetZone(ctx, zone1.ID)
		require.NoError(tb, err)
		require.Equal(tb, zone1.ID, retrievedZone.ID)
	}, testOptions)
}

func TestDNSServiceGetZone(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		// Create test zone
		testZone := &pluginDb.DNSZone{
			UserID:         1,
			Domain:         "example.com.",
			Status:         string(pluginDb.DNSZoneStatusPendingNameserver),
			PowerDNSZoneID: "pdns-123",
		}
		err := ctx.DB().Create(testZone).Error
		require.NoError(tb, err)

		// Test getting existing zone
		zone, err := svc.GetZone(ctx, testZone.ID)
		require.NoError(tb, err)
		require.NotNil(tb, zone)
		require.Equal(tb, testZone.ID, zone.ID)

		// Test getting non-existent zone
		zone, err = svc.GetZone(ctx, 999)
		require.NoError(tb, err)
		require.Nil(tb, zone)
	}, TestOptions)
}

func TestDNSServiceGetZoneByDomain(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		// Create test zone
		testZone := &pluginDb.DNSZone{
			UserID:         1,
			Domain:         "example.com.",
			Status:         string(pluginDb.DNSZoneStatusPendingNameserver),
			PowerDNSZoneID: "pdns-123",
		}
		err := ctx.DB().Create(testZone).Error
		require.NoError(tb, err)

		// Test getting existing zone by domain
		zone, err := svc.GetZoneByDomain(ctx, "example.com.")
		require.NoError(tb, err)
		require.NotNil(tb, zone)
		require.Equal(tb, "example.com.", zone.Domain)

		// Test getting non-existent zone by domain
		zone, err = svc.GetZoneByDomain(ctx, "nonexistent.com.")
		require.NoError(tb, err)
		require.Nil(tb, zone)
	}, TestOptions)
}

func TestDNSServiceListZones(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		// Create test zones
		zones := []*pluginDb.DNSZone{
			{
				UserID:         1,
				Domain:         "example1.com.",
				Status:         string(pluginDb.DNSZoneStatusPendingNameserver),
				PowerDNSZoneID: "pdns-1",
			},
			{
				UserID:         1,
				Domain:         "example2.com.",
				Status:         string(pluginDb.DNSZoneStatusActive),
				PowerDNSZoneID: "pdns-2",
			},
			{
				UserID:         2,
				Domain:         "example3.com.",
				Status:         string(pluginDb.DNSZoneStatusPendingNameserver),
				PowerDNSZoneID: "pdns-3",
			},
		}

		for _, zone := range zones {
			err := ctx.DB().Create(zone).Error
			require.NoError(tb, err)
		}

		// Test listing zones for user 1
		userZones, err := svc.ListZones(ctx, 1)
		require.NoError(tb, err)
		require.Len(tb, userZones, 2)

		// Verify zones belong to user 1
		for _, zone := range userZones {
			require.Equal(tb, uint(1), zone.UserID)
		}
	}, TestOptions)
}

func TestDNSServiceUpdateZone(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		// Create test zone
		testZone := &pluginDb.DNSZone{
			UserID:         1,
			Domain:         "example.com.",
			Status:         string(pluginDb.DNSZoneStatusPendingNameserver),
			PowerDNSZoneID: "pdns-123",
		}
		err := ctx.DB().Create(testZone).Error
		require.NoError(tb, err)

		// Test updating zone status
		err = svc.UpdateZone(ctx, testZone.ID, pluginDb.DNSZoneStatusActive)
		require.NoError(tb, err)

		// Verify update
		var updatedZone pluginDb.DNSZone
		err = ctx.DB().First(&updatedZone, testZone.ID).Error
		require.NoError(tb, err)
		require.Equal(tb, string(pluginDb.DNSZoneStatusActive), updatedZone.Status)
	}, TestOptions)
}

func TestDNSServiceDeleteZone(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		// Create test zone
		testZone := &pluginDb.DNSZone{
			UserID:         1,
			Domain:         "example.com.",
			Status:         string(pluginDb.DNSZoneStatusPendingNameserver),
			PowerDNSZoneID: "pdns-123",
		}
		err := ctx.DB().Create(testZone).Error
		require.NoError(tb, err)

		// Test deleting zone
		err = svc.DeleteZone(ctx, testZone.ID)
		require.NoError(tb, err)

		// Verify deletion
		var deletedZone pluginDb.DNSZone
		result := ctx.DB().First(&deletedZone, testZone.ID)
		require.Error(tb, result.Error)
	}, TestOptions)
}

func TestDNSServiceValidateNameservers(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		// Create test zone
		testZone := &pluginDb.DNSZone{
			UserID:         1,
			Domain:         "example.com.",
			Status:         string(pluginDb.DNSZoneStatusPendingNameserver),
			PowerDNSZoneID: "pdns-123",
		}
		err := ctx.DB().Create(testZone).Error
		require.NoError(tb, err)

		// Test validating nameservers
		validated, err := svc.ValidateNameservers(ctx, testZone.ID)
		require.NoError(tb, err)
		require.True(tb, validated)

		// Verify zone status updated
		var updatedZone pluginDb.DNSZone
		err = ctx.DB().First(&updatedZone, testZone.ID).Error
		require.NoError(tb, err)
		require.Equal(tb, string(pluginDb.DNSZoneStatusActive), updatedZone.Status)
		require.NotNil(tb, updatedZone.NameserversVerifiedAt)
	}, TestOptions)
}

func TestDNSServiceValidateNameserversNonExistentZone(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		// Test validating non-existent zone
		validated, err := svc.ValidateNameservers(ctx, 999)
		require.Error(tb, err)
		require.False(tb, validated)
	}, TestOptions)
}
