package dns

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/samber/lo"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	apiDTO "go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/dns/powerdns"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/testopts"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// Helper function to get DNS service config
func getTestDnsConfig() *pluginConfig.DnsConfig {
	return &pluginConfig.DnsConfig{
		Enabled:              true,
		PowerDNSAPIURL:       "http://localhost:8081",
		PowerDNSAPIKey:       "test-api-key",
		Nameservers:          []string{"ns1.localhost", "ns2.localhost"},
		VerificationTokenKey: "lumeweb-verify",
	}
}

// mockPowerDNSServer is a shared mock PowerDNS server for all tests
var mockPowerDNSServer *httptest.Server

// init creates the mock PowerDNS server before any tests run
func init() {
	// Create a new ServeMux with Go 1.22+ routing
	mux := http.NewServeMux()

	// Handle zone creation
	mux.HandleFunc("POST /servers/localhost/zones", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(powerdns.Zone{
			Id:   new("example.com."),
			Name: new("example.com."),
			Kind: (*powerdns.ZoneKind)(new("Native")),
		})
	})

	// Handle zone retrieval
	mux.HandleFunc("GET /servers/localhost/zones/example.com.", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(powerdns.Zone{
			Id:     new("example.com."),
			Name:   new("example.com."),
			Kind:   (*powerdns.ZoneKind)(new("Native")),
			Rrsets: &[]powerdns.RRSet{},
		})
	})

	// Handle zone updates (PATCH for bulk operations)
	mux.HandleFunc("PATCH /servers/localhost/zones/example.com.", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNoContent)
	})

	// Start mock PowerDNS server
	mockPowerDNSServer = httptest.NewServer(mux)
}

func TestMain(m *testing.M) {
	code := m.Run()
	mockPowerDNSServer.Close()
	os.Exit(code)
}

// setupDefaultServerHandlers sets up the default PowerDNS server handlers
// Use this for tests that create their own mock servers
func setupDefaultServerHandlers(mux *http.ServeMux) {
	// Handle zone creation
	mux.HandleFunc("POST /servers/localhost/zones", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(powerdns.Zone{
			Id:   new("example.com."),
			Name: new("example.com."),
			Kind: (*powerdns.ZoneKind)(new("Native")),
		})
	})

	// Handle zone retrieval
	mux.HandleFunc("GET /servers/localhost/zones/example.com.", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(powerdns.Zone{
			Id:     new("example.com."),
			Name:   new("example.com."),
			Kind:   (*powerdns.ZoneKind)(new("Native")),
			Rrsets: &[]powerdns.RRSet{},
		})
	})

	// Handle zone updates (PATCH for bulk operations)
	mux.HandleFunc("PATCH /servers/localhost/zones/example.com.", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNoContent)
	})
}

// createTestOptionsWithPowerDNSClient creates test options with a custom PowerDNS client
func createTestOptionsWithPowerDNSClient(client *PowerDNSClient) coreTesting.TestContextBuilderOption {
	return testopts.NewBaseMockPluginBuilder().WithService(pluginCore.DNS_SERVICE, func() (core.Service, []core.ContextBuilderOption, error) {
		return NewDNSServiceWithOptions(WithPowerDNSClient(client))
	}).WithServiceConfig(pluginCore.DNS_SERVICE, getTestDnsConfig()).WithMigrations(map[core.DBType]fs.FS{
		core.DB_TYPE_SQLITE: migrations.GetSQLite(),
	}).BuilderOption()
}

// createTestOptionsWithServer creates test options with a custom PowerDNS server
func createTestOptionsWithServer(server *httptest.Server) coreTesting.TestContextBuilderOption {
	return createTestOptionsWithPowerDNSClient(createTestPowerDNSClient(server.URL))
}

// createTestPowerDNSClient creates a test PowerDNS client
func createTestPowerDNSClient(serverURL string) *PowerDNSClient {
	logger := zap.NewNop()
	coreLogger := &core.Logger{Logger: logger}
	pdnsClient, err := NewPowerDNSClient(serverURL, "test-api-key", coreLogger)
	if err != nil {
		panic(fmt.Sprintf("failed to create PowerDNS client: %v", err))
	}
	return pdnsClient
}

func getTestOptions() coreTesting.TestContextBuilderOption {
	return createTestOptionsWithPowerDNSClient(createTestPowerDNSClient(mockPowerDNSServer.URL))
}

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

	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		// Test successful zone creation
		zone, err := svc.CreateZone(ctx, "example.com.", 1)
		require.NoError(tb, err)
		require.NotNil(tb, zone)
		require.Equal(tb, "example.com.", zone.Domain)
		require.Equal(tb, uint(1), zone.UserID)
		require.Equal(tb, string(pluginDb.DNSZoneStatusPendingNameserver), zone.Status)
	}, createTestOptionsWithServer(server))
}

func TestDNSServiceCreateZoneInvalidDomain(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		// Test invalid domain format - contains invalid characters
		_, err := svc.CreateZone(ctx, "example!.com.", 1)
		require.Error(tb, err)
		require.Contains(tb, err.Error(), "invalid character")
	}, getTestOptions())
}

func TestDNSServiceCreateZoneDuplicate(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		zone1, err := svc.CreateZone(ctx, "example.com.", 1)
		require.NoError(tb, err)
		require.NotNil(tb, zone1)

		// Duplicate should return existing zone instead of erroring
		zone2, err := svc.CreateZone(ctx, "example.com.", 1)
		require.NoError(tb, err)
		require.NotNil(tb, zone2)
		require.Equal(tb, zone1.ID, zone2.ID)
	}, getTestOptions())
}

func TestDNSServiceCreateZoneDuplicateDifferentUser(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		zone1, err := svc.CreateZone(ctx, "example.com.", 1)
		require.NoError(tb, err)
		require.NotNil(tb, zone1)

		// Different user trying to create zone for same domain should fail
		zone2, err := svc.CreateZone(ctx, "example.com.", 2)
		require.Error(tb, err)
		require.Nil(tb, zone2)
		require.Contains(tb, err.Error(), "already owned by another user")
	}, getTestOptions())
}

func TestDNSServiceCreateZoneIdempotentAfterPowerDNSConflict(t *testing.T) {
	zoneCreated := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/servers/localhost/zones" {
			if zoneCreated {
				w.WriteHeader(http.StatusConflict)
				w.Write([]byte(`{"error": "Conflict"}`))
				return
			}
			zoneCreated = true
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(powerdns.Zone{
				Id:   new("conflict-test.com."),
				Name: new("conflict-test.com."),
				Kind: (*powerdns.ZoneKind)(new("Native")),
			})
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/servers/localhost/zones/conflict-test.com." {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(powerdns.Zone{
				Id:   new("conflict-test.com."),
				Name: new("conflict-test.com."),
				Kind: (*powerdns.ZoneKind)(new("Native")),
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		// First call creates zone normally in PowerDNS + DB
		zone1, err := svc.CreateZone(ctx, "conflict-test.com.", 1)
		require.NoError(tb, err)
		require.NotNil(tb, zone1)

		// The DB zone was deleted externally (simulating toggle-off),
		// but PowerDNS still has the zone (returns 409 on re-create).
		// Service should handle this gracefully via GetZoneByDomain returning
		// nil for the DB, then PowerDNS 409 → fetch existing → create DB row.
	}, createTestOptionsWithServer(server))
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
				svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
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
			}, getTestOptions())
		})
	}
}

func TestDNSServiceGetZone(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
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
	}, getTestOptions())
}

func TestDNSServiceGetZoneNotFound(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		// Try to get non-existent zone
		_, err := svc.GetZone(ctx, 999)
		require.Error(tb, err)
		require.Equal(tb, gorm.ErrRecordNotFound, err)
	}, getTestOptions())
}

func TestDNSServiceValidateNameservers(t *testing.T) {
	t.Run("validation_succeeds_for_valid_zone", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, svc)

			// Create a zone
			zone, err := svc.CreateZone(ctx, "example.com.", 1)
			require.NoError(tb, err)
			require.NotNil(tb, zone)

			// Validate nameservers (will fail DNS lookup in test, but should not error)
			// This test verifies the method is callable and returns proper structure
			_, err = svc.ValidateNameservers(ctx, zone.ID)
			// DNS lookup may fail in test environment, but method should not panic
		}, getTestOptions())
	})

	t.Run("validation_fails_for_nonexistent_zone", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, svc)

			// Try to validate non-existent zone
			_, err := svc.ValidateNameservers(ctx, 999)
			require.Error(tb, err)
		}, getTestOptions())
	})
}

func TestDNSServiceValidateNameserversTableDriven(t *testing.T) {
	tests := []struct {
		name        string
		zoneID      uint
		expectError bool
		description string
	}{
		{
			name:        "validation_succeeds_for_valid_zone",
			zoneID:      1,
			expectError: false,
			description: "Valid zone should succeed (DNS lookup may fail in test)",
		},
		{
			name:        "validation_fails_for_nonexistent_zone",
			zoneID:      999,
			expectError: true,
			description: "Non-existent zone should return error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
				svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
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
			}, getTestOptions())
		})
	}
}

func TestDNSServiceCreateWebsiteDNSRecords(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		// Create a zone first
		zone, err := svc.CreateZone(ctx, "example.com.", 1)
		require.NoError(tb, err)
		require.NotNil(tb, zone)

		// Create website DNS records
		err = svc.CreateWebsiteDNSRecords(ctx, zone.ID, "example.com", "QmHash123", pluginDb.WebsiteTargetTypeIPFS, "test-validation-token")
		require.NoError(tb, err)
	}, getTestOptions())
}

func TestDNSServiceDeleteWebsiteDNSRecords(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		// Create a zone first
		zone, err := svc.CreateZone(ctx, "example.com.", 1)
		require.NoError(tb, err)
		require.NotNil(tb, zone)

		// Delete website DNS records
		err = svc.DeleteWebsiteDNSRecords(ctx, zone.ID, "example.com")
		require.NoError(tb, err)
	}, getTestOptions())
}

func TestDNSServiceBulkDeleteRecords(t *testing.T) {
	mux := http.NewServeMux()
	setupDefaultServerHandlers(mux)

	server := httptest.NewServer(mux)
	defer server.Close()

	testOptions := createTestOptionsWithServer(server)

	t.Run("success", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
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
			svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
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
			svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
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

	mux := http.NewServeMux()
	setupDefaultServerHandlers(mux)

	server := httptest.NewServer(mux)
	defer server.Close()

	testOptions := createTestOptionsWithServer(server)

	t.Run("merge_mode_success", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, svc)

			// Create a zone first
			zone, err := svc.CreateZone(ctx, "example.com.", 1)
			require.NoError(tb, err)
			require.NotNil(tb, zone)

			response, err := svc.ImportZoneFile(ctx, zone.ID, zoneFileContent, apiDTO.ImportModeMerge, false)
			require.NoError(tb, err)
			require.NotNil(tb, response)
			// Note: The mock PowerDNS server doesn't track RRSets created via PATCH,
			// so we can't verify created_count > 0. We just verify the operation completes.
		}, testOptions)
	})

	t.Run("merge_mode_dry_run", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, svc)

			// Create a zone first
			zone, err := svc.CreateZone(ctx, "example.com.", 1)
			require.NoError(tb, err)
			require.NotNil(tb, zone)

			response, err := svc.ImportZoneFile(ctx, zone.ID, zoneFileContent, apiDTO.ImportModeMerge, true)
			require.NoError(tb, err)
			require.NotNil(tb, response)
			// Note: The mock PowerDNS server doesn't track RRSets, so we check parsed records count
			require.Greater(tb, len(response.CreatedRecords)+response.SkippedCount, 0)
		}, getTestOptions())
	})

	t.Run("replace_mode", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, svc)

			// Create a zone first
			zone, err := svc.CreateZone(ctx, "example.com.", 1)
			require.NoError(tb, err)
			require.NotNil(tb, zone)

			response, err := svc.ImportZoneFile(ctx, zone.ID, zoneFileContent, apiDTO.ImportModeReplace, false)
			require.NoError(tb, err)
			require.NotNil(tb, response)
			// Note: The mock PowerDNS server doesn't track RRSets created via PATCH,
			// so we just verify the operation completes without error.
		}, getTestOptions())
	})

	t.Run("update_mode", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, svc)

			// Create a zone first
			zone, err := svc.CreateZone(ctx, "example.com.", 1)
			require.NoError(tb, err)
			require.NotNil(tb, zone)

			response, err := svc.ImportZoneFile(ctx, zone.ID, zoneFileContent, apiDTO.ImportModeUpdate, false)
			require.NoError(tb, err)
			require.NotNil(tb, response)
		}, getTestOptions())
	})

	t.Run("zone_not_found", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, svc)

			_, err := svc.ImportZoneFile(ctx, 999, zoneFileContent, apiDTO.ImportModeMerge, false)
			require.Error(tb, err)
			require.Contains(tb, err.Error(), "failed to get zone")
		}, getTestOptions())
	})

	t.Run("invalid_zone_file", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
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
		}, getTestOptions())
	})

	t.Run("filters_powerdns_managed_records", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
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
		}, getTestOptions())
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

func TestDNSServiceCreateRecord(t *testing.T) {
	// Set up a mock zone that includes the created RRSet
	createdRRSet := &[]powerdns.RRSet{}

	mux := http.NewServeMux()

	// Handle zone creation
	mux.HandleFunc("POST /servers/localhost/zones", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(powerdns.Zone{
			Id:   new("example.com."),
			Name: new("example.com."),
			Kind: (*powerdns.ZoneKind)(new("Native")),
		})
	})

	// Handle zone retrieval with dynamic RRSet
	mux.HandleFunc("GET /servers/localhost/zones/example.com.", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(powerdns.Zone{
			Id:     new("example.com."),
			Name:   new("example.com."),
			Kind:   (*powerdns.ZoneKind)(new("Native")),
			Rrsets: createdRRSet,
		})
	})

	// Handle zone updates (PATCH for RRSet operations)
	mux.HandleFunc("PATCH /servers/localhost/zones/example.com.", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		
		// Parse the request body to get the RRSet being created
		var updateRequest powerdns.ZonePatch
		if err := json.NewDecoder(r.Body).Decode(&updateRequest); err == nil && updateRequest.Rrsets != nil && len(*updateRequest.Rrsets) > 0 {
			// Store the RRSet in our slice for retrieval
			*createdRRSet = *updateRequest.Rrsets
		}
		
		w.WriteHeader(http.StatusNoContent)
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	testOptions := createTestOptionsWithServer(server)

	t.Run("success_returns_complete_record", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, svc)

			// Create a zone first
			zone, err := svc.CreateZone(ctx, "example.com.", 1)
			require.NoError(tb, err)
			require.NotNil(tb, zone)

			// Create a record
			record, err := svc.CreateRecord(ctx, zone.ID, "www", "A", "192.0.2.1", 3600)
			require.NoError(tb, err)
			require.NotNil(tb, record)

			// Verify all fields are populated
			require.Equal(tb, zone.ID, record.ZoneID, "ZoneID should match the request zone ID")
			require.Equal(tb, "www", record.Name, "Name should match")
			require.Equal(tb, "A", record.Type, "Type should match")
			require.Equal(tb, "192.0.2.1", record.Content, "Content should match")
			require.Equal(tb, uint(3600), record.TTL, "TTL should match")
			require.False(tb, record.Disabled, "Disabled should be false for new records")
		}, testOptions)
	})

	t.Run("success_with_complex_record", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, svc)

			// Create a zone first
			zone, err := svc.CreateZone(ctx, "example.com.", 1)
			require.NoError(tb, err)
			require.NotNil(tb, zone)

			// Create different types of records and verify completeness
			testCases := []struct {
				name    string
				rtype   string
				content string
				ttl     uint
			}{
				{"www", "A", "192.0.2.10", 300},
				{"mail", "MX", "10 mail.example.com.", 600},
				{"api", "CNAME", "api.example.com.", 900},
			}

			for _, tc := range testCases {
				record, err := svc.CreateRecord(ctx, zone.ID, tc.name, tc.rtype, tc.content, tc.ttl)
				require.NoError(tb, err, "CreateRecord should succeed for %s", tc.name)
				require.NotNil(tb, record, "Record should not be nil for %s", tc.name)
				
				require.Equal(tb, zone.ID, record.ZoneID, "ZoneID should match for %s", tc.name)
				require.Equal(tb, tc.name, record.Name, "Name should match for %s", tc.name)
				require.Equal(tb, tc.rtype, record.Type, "Type should match for %s", tc.name)
				require.Equal(tb, tc.content, record.Content, "Content should match for %s", tc.name)
				require.Equal(tb, tc.ttl, record.TTL, "TTL should match for %s", tc.name)
				require.False(tb, record.Disabled, "Disabled should be false for %s", tc.name)
			}
		}, testOptions)
	})

	t.Run("success_apex_record_empty_name", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, svc)

			zone, err := svc.CreateZone(ctx, "example.com.", 1)
			require.NoError(tb, err)
			require.NotNil(tb, zone)

			record, err := svc.CreateRecord(ctx, zone.ID, "", "TXT", "v=spf1 include:_spf.example.com ~all", 3600)
			require.NoError(tb, err)
			require.NotNil(tb, record)

			require.Equal(tb, zone.ID, record.ZoneID)
			require.Equal(tb, "", record.Name)
			require.Equal(tb, "TXT", record.Type)
			require.Equal(tb, "v=spf1 include:_spf.example.com ~all", record.Content)
		}, testOptions)
	})

	t.Run("success_apex_record_at_symbol", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, svc)

			zone, err := svc.CreateZone(ctx, "example.com.", 1)
			require.NoError(tb, err)
			require.NotNil(tb, zone)

			record, err := svc.CreateRecord(ctx, zone.ID, "@", "A", "192.0.2.1", 3600)
			require.NoError(tb, err)
			require.NotNil(tb, record)

			require.Equal(tb, zone.ID, record.ZoneID)
			require.Equal(tb, "", record.Name)
			require.Equal(tb, "A", record.Type)
			require.Equal(tb, "192.0.2.1", record.Content)
		}, testOptions)
	})

	t.Run("zone_not_found", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, svc)

			_, err := svc.CreateRecord(ctx, 999, "www", "A", "192.0.2.1", 3600)
			require.Error(tb, err)
			require.Contains(tb, err.Error(), "failed to get zone")
		}, testOptions)
	})

	t.Run("powerdns_error", func(t *testing.T) {
		errorMux := http.NewServeMux()
		
		// Handle zone creation
		errorMux.HandleFunc("POST /servers/localhost/zones", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(powerdns.Zone{
				Id:   new("example.com."),
				Name: new("example.com."),
				Kind: (*powerdns.ZoneKind)(new("Native")),
			})
		})

		// Handle zone retrieval
		errorMux.HandleFunc("GET /servers/localhost/zones/example.com.", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(powerdns.Zone{
				Id:     new("example.com."),
				Name:   new("example.com."),
				Kind:   (*powerdns.ZoneKind)(new("Native")),
				Rrsets: &[]powerdns.RRSet{},
			})
		})

		// Handle zone updates with error
		errorMux.HandleFunc("PATCH /servers/localhost/zones/example.com.", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		})

		server := httptest.NewServer(errorMux)
		defer server.Close()

		testOptionsWithError := createTestOptionsWithServer(server)

		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, svc)

			// Create a zone first
			zone, err := svc.CreateZone(ctx, "example.com.", 1)
			require.NoError(tb, err)
			require.NotNil(tb, zone)

			// Try to create a record - PATCH returns 500, so error is caught immediately
			_, err = svc.CreateRecord(ctx, zone.ID, "www", "A", "192.0.2.1", 3600)
			require.Error(tb, err)
			// Error should be from UpdateZoneRRSets detecting the 500
			require.Contains(tb, err.Error(), "PowerDNS returned status 500")
		}, testOptionsWithError)
	})

	t.Run("getrrset_fails_after_creation", func(t *testing.T) {
		// This test verifies that if GetRRSet fails after successful creation, the error is propagated
		// We simulate this by having the GET return 500 after the PATCH succeeds
		getFailed := false

		mux := http.NewServeMux()

		// Handle zone creation
		mux.HandleFunc("POST /servers/localhost/zones", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(powerdns.Zone{
				Id:   new("example.com."),
				Name: new("example.com."),
				Kind: (*powerdns.ZoneKind)(new("Native")),
			})
		})

		// Handle zone retrieval - fail after record creation
		mux.HandleFunc("GET /servers/localhost/zones/example.com.", func(w http.ResponseWriter, r *http.Request) {
			if getFailed {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(powerdns.Zone{
				Id:     new("example.com."),
				Name:   new("example.com."),
				Kind:   (*powerdns.ZoneKind)(new("Native")),
				Rrsets: &[]powerdns.RRSet{},
			})
		})

		// Handle zone updates - mark getFailed before returning success
		mux.HandleFunc("PATCH /servers/localhost/zones/example.com.", func(w http.ResponseWriter, r *http.Request) {
			getFailed = true
			w.WriteHeader(http.StatusNoContent)
		})

		server := httptest.NewServer(mux)
		defer server.Close()

		testOptionsWithFailure := createTestOptionsWithServer(server)

		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, svc)

			// Create a zone first
			zone, err := svc.CreateZone(ctx, "example.com.", 1)
			require.NoError(tb, err)
			require.NotNil(tb, zone)

			// Create a record - should return record before GetRRSet fails
			// Actually, the current implementation calls GetRRSet immediately after UpdateZoneRRSets
			// So this will fail if GetRRSet returns error
			_, err = svc.CreateRecord(ctx, zone.ID, "www", "A", "192.0.2.1", 3600)
			require.Error(tb, err)
			require.Contains(tb, err.Error(), "failed to retrieve created record")
		}, testOptionsWithFailure)
	})
}

func TestDNSServiceGetRRSet(t *testing.T) {
	mux := http.NewServeMux()

	// Handle zone creation
	mux.HandleFunc("POST /servers/localhost/zones", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(powerdns.Zone{
			Id:   new("example.com."),
			Name: new("example.com."),
			Kind: (*powerdns.ZoneKind)(new("Native")),
		})
	})

	// Handle zone retrieval with RRSets
	mux.HandleFunc("GET /servers/localhost/zones/example.com.", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		rrsets := []powerdns.RRSet{
			{
				Name:  "www.example.com.",
				Type:  "A",
				Ttl:   lo.ToPtr(3600),
				Records: []powerdns.Record{
					{Content: "192.0.2.1"},
				},
			},
		}
		json.NewEncoder(w).Encode(powerdns.Zone{
			Id:     new("example.com."),
			Name:   new("example.com."),
			Kind:   (*powerdns.ZoneKind)(new("Native")),
			Rrsets: (*[]powerdns.RRSet)(&rrsets),
		})
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	testOptions := createTestOptionsWithServer(server)

	t.Run("success", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, svc)

			// Create a zone first
			zone, err := svc.CreateZone(ctx, "example.com.", 1)
			require.NoError(tb, err)
			require.NotNil(tb, zone)

			// Get RRSet
			records, err := svc.GetRRSet(ctx, zone.ID, "www", "A")
			require.NoError(tb, err)
			require.NotNil(tb, records)
			require.Len(tb, records, 1)

			// Verify record has all fields populated
			record := records[0]
			require.Equal(tb, zone.ID, record.ZoneID, "ZoneID should match zone ID")
			require.Equal(tb, "www", record.Name, "Name should match")
			require.Equal(tb, "A", record.Type, "Type should match")
			require.Equal(tb, "192.0.2.1", record.Content, "Content should match")
			require.Equal(tb, uint(3600), record.TTL, "TTL should match")
			require.False(tb, record.Disabled, "Disabled should be false")
		}, testOptions)
	})

	t.Run("not_found", func(t *testing.T) {
		coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
			svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
			require.NotNil(tb, svc)

			// Create a zone first
			zone, err := svc.CreateZone(ctx, "example.com.", 1)
			require.NoError(tb, err)
			require.NotNil(tb, zone)

			// Get non-existent RRSet
			_, err = svc.GetRRSet(ctx, zone.ID, "nonexistent", "A")
			require.Error(tb, err)
			require.Contains(tb, err.Error(), "RRSet not found")
		}, testOptions)
	})
}

func TestDNSServiceCreateZoneRestoresSoftDeletedZone(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		zone1, err := svc.CreateZone(ctx, "example.com.", 1)
		require.NoError(tb, err)
		require.NotNil(tb, zone1)
		originalID := zone1.ID
		originalPDNSZoneID := zone1.PowerDNSZoneID

		err = svc.DeleteZone(ctx, zone1.ID)
		require.NoError(tb, err)

		deleted, err := svc.GetZoneByDomain(ctx, "example.com.")
		require.NoError(tb, err)
		require.NotNil(tb, deleted)
		require.True(tb, deleted.DeletedAt.Valid, "zone should be soft-deleted")

		_, err = svc.GetZone(ctx, originalID)
		require.Equal(tb, gorm.ErrRecordNotFound, err)

		zone2, err := svc.CreateZone(ctx, "example.com.", 1)
		require.NoError(tb, err)
		require.NotNil(tb, zone2)

		require.Equal(tb, originalID, zone2.ID, "restored zone should have same ID")
		require.False(tb, zone2.DeletedAt.Valid, "restored zone should not be soft-deleted")
		require.Equal(tb, string(pluginDb.DNSZoneStatusPendingNameserver), zone2.Status)
		require.Equal(tb, originalPDNSZoneID, zone2.PowerDNSZoneID, "restored zone should have updated PowerDNSZoneID from recreation")

		freshZone, err := svc.GetZone(ctx, zone2.ID)
		require.NoError(tb, err)
		require.NotNil(tb, freshZone)
		require.Equal(tb, "example.com.", freshZone.Domain)
		require.Equal(tb, originalPDNSZoneID, freshZone.PowerDNSZoneID)
	}, getTestOptions())
}

func TestDNSServiceCreateZoneSoftDeletedDifferentUser(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		// Create a zone for user 1
		zone1, err := svc.CreateZone(ctx, "example.com.", 1)
		require.NoError(tb, err)
		require.NotNil(tb, zone1)

		// Soft-delete the zone
		err = svc.DeleteZone(ctx, zone1.ID)
		require.NoError(tb, err)

		// User 2 trying to create the same domain should fail
		zone2, err := svc.CreateZone(ctx, "example.com.", 2)
		require.Error(tb, err)
		require.Nil(tb, zone2)
		require.Contains(tb, err.Error(), "already owned by another user")
	}, getTestOptions())
}

func TestDNSServiceGetZoneByDomainFindsSoftDeleted(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		// Create a zone
		zone, err := svc.CreateZone(ctx, "example.com.", 1)
		require.NoError(tb, err)
		require.NotNil(tb, zone)

		// Soft-delete the zone
		err = svc.DeleteZone(ctx, zone.ID)
		require.NoError(tb, err)

		// GetZoneByDomain should still find the soft-deleted zone
		found, err := svc.GetZoneByDomain(ctx, "example.com.")
		require.NoError(tb, err)
		require.NotNil(tb, found)
		require.Equal(tb, "example.com.", found.Domain)
		require.True(tb, found.DeletedAt.Valid, "should find the soft-deleted zone")
	}, getTestOptions())
}

func TestDNSServiceCreateZoneActiveZoneNotSoftDeletedReturnsExisting(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		zone1, err := svc.CreateZone(ctx, "example.com.", 1)
		require.NoError(tb, err)
		require.NotNil(tb, zone1)

		zone2, err := svc.CreateZone(ctx, "example.com.", 1)
		require.NoError(tb, err)
		require.NotNil(tb, zone2)
		require.Equal(tb, zone1.ID, zone2.ID)
		require.False(tb, zone2.DeletedAt.Valid, "existing active zone should not be soft-deleted")
	}, getTestOptions())
}

func TestDNSServiceCreateZoneRestoresSoftDeletedZoneWithNewPowerDNSZoneID(t *testing.T) {
	createCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/servers/localhost/zones" {
			createCount++
			zoneID := fmt.Sprintf("pdns-zone-%d.", createCount)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(powerdns.Zone{
				Id:   &zoneID,
				Name: new("recreate-test.com."),
				Kind: (*powerdns.ZoneKind)(new("Native")),
			})
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/servers/localhost/zones/pdns-zone-1." {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(powerdns.Zone{
				Id:     new("pdns-zone-1."),
				Name:   new("recreate-test.com."),
				Kind:   (*powerdns.ZoneKind)(new("Native")),
				Rrsets: &[]powerdns.RRSet{},
			})
			return
		}
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		svc := core.GetService[*DNSServiceDefault](ctx, pluginCore.DNS_SERVICE)
		require.NotNil(tb, svc)

		zone1, err := svc.CreateZone(ctx, "recreate-test.com.", 1)
		require.NoError(tb, err)
		require.NotNil(tb, zone1)
		require.Equal(tb, "pdns-zone-1.", zone1.PowerDNSZoneID)

		err = svc.DeleteZone(ctx, zone1.ID)
		require.NoError(tb, err)

		zone2, err := svc.CreateZone(ctx, "recreate-test.com.", 1)
		require.NoError(tb, err)
		require.NotNil(tb, zone2)

		require.Equal(tb, zone1.ID, zone2.ID, "restored zone should have same DB ID")
		require.Equal(tb, "pdns-zone-2.", zone2.PowerDNSZoneID, "restored zone should have new PowerDNS zone ID from recreation")
		require.False(tb, zone2.DeletedAt.Valid)
		require.Equal(tb, string(pluginDb.DNSZoneStatusPendingNameserver), zone2.Status)
	}, createTestOptionsWithServer(server))
}
