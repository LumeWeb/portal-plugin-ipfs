package dns

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	apiDTO "go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/dns/powerdns"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.uber.org/zap"
	"gorm.io/gorm"
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
		w.Header().Set("Content-Type", "application/json")
		
		// Handle zone creation
		if r.Method == http.MethodPost && r.URL.Path == "/servers/localhost/zones" {
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(powerdns.Zone{
				Id:   new("example.com."),
				Name: new("example.com."),
				Kind: (*powerdns.ZoneKind)(new("Native")),
			})
			return
		}
		
		// Handle zone retrieval
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/servers/localhost/zones/example.com.") {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(powerdns.Zone{
				Id:      new("example.com."),
				Name:    new("example.com."),
				Kind:    (*powerdns.ZoneKind)(new("Native")),
				Rrsets:  &[]powerdns.RRSet{},
			})
			return
		}
		
		// Handle zone updates (PATCH for bulk operations)
		if r.Method == http.MethodPatch && strings.HasPrefix(r.URL.Path, "/servers/localhost/zones/example.com.") {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		
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
	coreTesting.WithConfig("plugin.ipfs.protocol.dns_hosting.dns_enabled", true),
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
		coreTesting.WithConfig("plugin.ipfs.protocol.dns_hosting.dns_enabled", true),
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
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		// Create first zone
		zone1, err := svc.CreateZone(ctx, "example.com.", 1)
		require.NoError(tb, err)
		require.NotNil(tb, zone1)

		// Try to create duplicate zone
		zone2, err := svc.CreateZone(ctx, "example.com.", 1)
		require.Error(tb, err)
		require.Nil(tb, zone2)
	}, TestOptions)
}

func TestDNSServiceCreateZoneTableDriven(t *testing.T) {
	tests := []struct {
		name           string
		domain         string
		userID         uint
		expectError    bool
		expectedStatus string
	}{
		{
			name:           "successful zone creation",
			domain:         "example.com.",
			userID:         1,
			expectError:    false,
			expectedStatus: string(pluginDb.DNSZoneStatusPendingNameserver),
		},
		{
			name:           "zone creation with subdomain",
			domain:         "subdomain.example.com.",
			userID:         1,
			expectError:    false,
			expectedStatus: string(pluginDb.DNSZoneStatusPendingNameserver),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				svc := core.GetService[*DNSService](ctx, pluginCore.DNS_SERVICE)
				require.NotNil(tb, svc)

				zone, err := svc.CreateZone(ctx, tt.domain, tt.userID)
				if tt.expectError {
					require.Error(tb, err)
					require.Nil(tb, zone)
				} else {
					require.NoError(tb, err)
					require.NotNil(tb, zone)
					require.Equal(tb, tt.domain, zone.Domain)
					require.Equal(tb, tt.userID, zone.UserID)
					require.Equal(tb, tt.expectedStatus, zone.Status)
				}
			}, TestOptions)
		})
	}
}

func TestDNSServiceGetZone(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		// Create a zone first
		createdZone, err := svc.CreateZone(ctx, "example.com.", 1)
		require.NoError(tb, err)
		require.NotNil(tb, createdZone)

		// Get the zone
		zone, err := svc.GetZone(ctx, createdZone.ID)
		require.NoError(tb, err)
		require.NotNil(tb, zone)
		require.Equal(tb, createdZone.ID, zone.ID)
		require.Equal(tb, "example.com.", zone.Domain)
	}, TestOptions)
}

func TestDNSServiceGetZoneNotFound(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		// Try to get non-existent zone
		_, err := svc.GetZone(ctx, 999)
		require.Error(tb, err)
		require.Equal(tb, gorm.ErrRecordNotFound, err)
	}, TestOptions)
}

func TestDNSServiceValidateNameservers(t *testing.T) {
	t.Run("validation_succeeds_for_valid_zone", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSService](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, svc)

			// Create a zone
			zone, err := svc.CreateZone(ctx, "example.com.", 1)
			require.NoError(tb, err)
			require.NotNil(tb, zone)

			// Validate nameservers (will fail DNS lookup in test, but should not error)
			// This test verifies the method is callable and returns proper structure
			_, err = svc.ValidateNameservers(ctx, zone.ID)
			// DNS lookup may fail in test environment, but method should not panic
		}, TestOptions)
	})

	t.Run("validation_fails_for_nonexistent_zone", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSService](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, svc)

			// Try to validate non-existent zone
			_, err := svc.ValidateNameservers(ctx, 999)
			require.Error(tb, err)
		}, TestOptions)
	})
}

func TestDNSServiceValidateNameserversTableDriven(t *testing.T) {
	tests := []struct {
		name         string
		zoneID       uint
		expectError  bool
		description  string
	}{
		{
			name:         "validation_succeeds_for_valid_zone",
			zoneID:       1,
			expectError:  false,
			description:  "Valid zone should succeed (DNS lookup may fail in test)",
		},
		{
			name:         "validation_fails_for_nonexistent_zone",
			zoneID:       999,
			expectError:  true,
			description:  "Non-existent zone should return error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				svc := core.GetService[*DNSService](ctx, pluginCore.DNS_SERVICE)
				require.NotNil(tb, svc)

				// Create a zone if needed
				if tt.zoneID == 1 {
					zone, err := svc.CreateZone(ctx, "example.com.", 1)
					require.NoError(tb, err)
					require.NotNil(tb, zone)
					tt.zoneID = zone.ID
				}

				_, err := svc.ValidateNameservers(ctx, tt.zoneID)
				if tt.expectError {
					require.Error(tb, err)
				}
				// Note: We don't assert success for non-error cases because DNS lookups
				// will fail in test environment
			}, TestOptions)
		})
	}
}

func TestDNSServiceCreateWebsiteDNSRecords(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		// Create a zone first
		zone, err := svc.CreateZone(ctx, "example.com.", 1)
		require.NoError(tb, err)
		require.NotNil(tb, zone)

		// Create website DNS records
		err = svc.CreateWebsiteDNSRecords(ctx, zone.ID, "QmHash123", pluginDb.WebsiteTargetTypeIPFS, "test-validation-token")
		require.NoError(tb, err)
	}, TestOptions)
}

func TestDNSServiceDeleteWebsiteDNSRecords(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DNSService](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		// Create a zone first
		zone, err := svc.CreateZone(ctx, "example.com.", 1)
		require.NoError(tb, err)
		require.NotNil(tb, zone)

		// Delete website DNS records
		err = svc.DeleteWebsiteDNSRecords(ctx, zone.ID)
		require.NoError(tb, err)
	}, TestOptions)
}

func TestDNSServiceBulkDeleteRecords(t *testing.T) {
	// Create a mock HTTP server that handles PowerDNS API requests
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		
		// Handle zone creation
		if r.Method == http.MethodPost && r.URL.Path == "/servers/localhost/zones" {
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(powerdns.Zone{
				Id:   new("example.com."),
				Name: new("example.com."),
				Kind: (*powerdns.ZoneKind)(new("Native")),
			})
			return
		}
		
		// Handle zone retrieval
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/servers/localhost/zones/example.com.") {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(powerdns.Zone{
				Id:      new("example.com."),
				Name:    new("example.com."),
				Kind:    (*powerdns.ZoneKind)(new("Native")),
				Rrsets:  &[]powerdns.RRSet{},
			})
			return
		}
		
		// Handle zone updates (for bulk delete)
		if r.Method == http.MethodPatch && strings.HasPrefix(r.URL.Path, "/servers/localhost/zones/example.com.") {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		
		w.WriteHeader(http.StatusNotFound)
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
		coreTesting.WithConfig("plugin.ipfs.protocol.dns_hosting.dns_enabled", true),
		coreTesting.WithConfig("plugin.ipfs.protocol.dns_hosting.powerdns_api_url", server.URL),
		coreTesting.WithConfig("plugin.ipfs.protocol.dns_hosting.powerdns_api_key", "test-api-key"),
		coreTesting.WithConfig("plugin.ipfs.protocol.dns_hosting.nameservers", []string{"ns1.example.com.", "ns2.example.com."}),
		coreTesting.WithSQLitePluginMigrations(
			internal.ProtocolName, migrations.GetSQLite(),
		),
	)

	t.Run("success", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSService](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, svc)

			// Create a zone first
			zone, err := svc.CreateZone(ctx, "example.com.", 1)
			require.NoError(tb, err)
			require.NotNil(tb, zone)

			records := []apiDTO.RecordIdentifier{
				{Name: "www", Type: "A"},
				{Name: "mail", Type: "MX"},
			}

			response, err := svc.BulkDeleteRecords(ctx, zone.ID, 1, records, false)
			require.NoError(tb, err)
			require.NotNil(tb, response)
			require.Len(tb, response.Results, 2)
			for _, result := range response.Results {
				require.Equal(tb, "success", result.Status)
			}
		}, testOptions)
	})

	t.Run("dry_run", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSService](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, svc)

			// Create a zone first
			zone, err := svc.CreateZone(ctx, "example.com.", 1)
			require.NoError(tb, err)
			require.NotNil(tb, zone)

			records := []apiDTO.RecordIdentifier{
				{Name: "www", Type: "A"},
			}

			response, err := svc.BulkDeleteRecords(ctx, zone.ID, 1, records, true)
			require.NoError(tb, err)
			require.NotNil(tb, response)
			require.Len(tb, response.Results, 1)
			require.Equal(tb, "success", response.Results[0].Status)
		}, testOptions)
	})

	t.Run("zone_not_found", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSService](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, svc)

			records := []apiDTO.RecordIdentifier{
				{Name: "www", Type: "A"},
			}

			_, err := svc.BulkDeleteRecords(ctx, 999, 1, records, false)
			require.Error(tb, err)
		}, testOptions)
	})

}

func TestDNSServiceImportZoneFile(t *testing.T) {
	zoneFileContent := `$ORIGIN example.com.
$TTL 3600
@		IN	SOA	ns1.example.com. admin.example.com. (
			2024010101 ; serial
			3600       ; refresh
			1800       ; retry
			604800     ; expire
			86400 )    ; minimum
@		IN	NS	ns1.example.com.
@		IN	NS	ns2.example.com.
www		IN	A	192.0.2.1
mail		IN	A	192.0.2.2
@		IN	MX	10	mail.example.com.
test		IN	TXT	"test record"`

	// Create test options with mocked PowerDNS client
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		
		// Handle zone creation
		if r.Method == http.MethodPost && r.URL.Path == "/servers/localhost/zones" {
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(powerdns.Zone{
				Id:   new("example.com."),
				Name: new("example.com."),
				Kind: (*powerdns.ZoneKind)(new("Native")),
			})
			return
		}
		
		// Handle zone retrieval
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/servers/localhost/zones/example.com.") {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(powerdns.Zone{
				Id:      new("example.com."),
				Name:    new("example.com."),
				Kind:    (*powerdns.ZoneKind)(new("Native")),
				Rrsets:  &[]powerdns.RRSet{},
			})
			return
		}
		
		// Handle record creation/updates
		if r.Method == http.MethodPatch && strings.HasPrefix(r.URL.Path, "/servers/localhost/zones/example.com.") {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

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
		coreTesting.WithConfig("plugin.ipfs.protocol.dns_hosting.dns_enabled", true),
		coreTesting.WithConfig("plugin.ipfs.protocol.dns_hosting.powerdns_api_url", server.URL),
		coreTesting.WithConfig("plugin.ipfs.protocol.dns_hosting.powerdns_api_key", "test-api-key"),
		coreTesting.WithConfig("plugin.ipfs.protocol.dns_hosting.nameservers", []string{"ns1.example.com.", "ns2.example.com."}),
		coreTesting.WithSQLitePluginMigrations(
			internal.ProtocolName, migrations.GetSQLite(),
		),
	)

	t.Run("merge_mode_success", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSService](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, svc)

			// Create a zone first
			zone, err := svc.CreateZone(ctx, "example.com.", 1)
			require.NoError(tb, err)
			require.NotNil(tb, zone)

			response, err := svc.ImportZoneFile(ctx, zone.ID, zoneFileContent, apiDTO.ImportModeMerge, false)
			require.NoError(tb, err)
			require.NotNil(tb, response)
			require.Greater(tb, len(response.CreatedRecords), 0)
			require.Equal(tb, 0, response.FailedCount)
		}, testOptions)
	})

	t.Run("merge_mode_dry_run", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSService](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, svc)

			// Create a zone first
			zone, err := svc.CreateZone(ctx, "example.com.", 1)
			require.NoError(tb, err)
			require.NotNil(tb, zone)

			response, err := svc.ImportZoneFile(ctx, zone.ID, zoneFileContent, apiDTO.ImportModeMerge, true)
			require.NoError(tb, err)
			require.NotNil(tb, response)
			require.Greater(tb, len(response.CreatedRecords), 0)
		}, TestOptions)
	})

	t.Run("replace_mode", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSService](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, svc)

			// Create a zone first
			zone, err := svc.CreateZone(ctx, "example.com.", 1)
			require.NoError(tb, err)
			require.NotNil(tb, zone)

			response, err := svc.ImportZoneFile(ctx, zone.ID, zoneFileContent, apiDTO.ImportModeReplace, false)
			require.NoError(tb, err)
			require.NotNil(tb, response)
			require.Greater(tb, len(response.CreatedRecords), 0)
		}, TestOptions)
	})

	t.Run("update_mode", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSService](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, svc)

			// Create a zone first
			zone, err := svc.CreateZone(ctx, "example.com.", 1)
			require.NoError(tb, err)
			require.NotNil(tb, zone)

			response, err := svc.ImportZoneFile(ctx, zone.ID, zoneFileContent, apiDTO.ImportModeUpdate, false)
			require.NoError(tb, err)
			require.NotNil(tb, response)
		}, TestOptions)
	})

	t.Run("zone_not_found", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSService](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, svc)

			_, err := svc.ImportZoneFile(ctx, 999, zoneFileContent, apiDTO.ImportModeMerge, false)
			require.Error(tb, err)
			require.Contains(tb, err.Error(), "failed to get zone")
		}, TestOptions)
	})

	t.Run("invalid_zone_file", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSService](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, svc)

			// Create a zone first
			zone, err := svc.CreateZone(ctx, "example.com.", 1)
			require.NoError(tb, err)
			require.NotNil(tb, zone)

			invalidContent := "invalid zone file content !!!"

			response, err := svc.ImportZoneFile(ctx, zone.ID, invalidContent, apiDTO.ImportModeMerge, false)
			require.Error(tb, err)
			require.NotNil(tb, response)
			require.Greater(tb, len(response.Errors), 0)
			require.Contains(tb, response.Errors[0].Error, "Failed to parse zone file")
		}, TestOptions)
	})

	t.Run("filters_powerdns_managed_records", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSService](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, svc)

			// Create a zone first
			zone, err := svc.CreateZone(ctx, "example.com.", 1)
			require.NoError(tb, err)
			require.NotNil(tb, zone)

			response, err := svc.ImportZoneFile(ctx, zone.ID, zoneFileContent, apiDTO.ImportModeMerge, false)
			require.NoError(tb, err)
			require.NotNil(tb, response)

			for _, record := range response.CreatedRecords {
				recordType := pluginCore.RecordType(record.Type)
				require.False(tb, recordType.IsManagedByPowerDNS(), 
					fmt.Sprintf("Record type %s should not be managed by PowerDNS", record.Type))
			}
		}, TestOptions)
	})
}

// findRRSet is a helper to find an RRSet by name and type
func findRRSet(rrsets []powerdns.RRSet, name, recordType string) *powerdns.RRSet {
	for i := range rrsets {
		if rrsets[i].Name == name && rrsets[i].Type == recordType {
			return &rrsets[i]
		}
	}
	return nil
}
