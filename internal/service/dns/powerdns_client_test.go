package dns

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"go.lumeweb.com/portal-plugin-ipfs/internal/dns/powerdns"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
)

// mockHTTPClient is a mock HTTP client for testing
type mockHTTPClient struct {
	response *http.Response
	err      error
	requests []*http.Request
	// bodyBytes caches the configured response body so it can be replayed on
	// every request (CreateZone now issues POST + GET + PATCH).
	bodyBytes []byte
}

// Do records the request and returns a fresh copy of the configured response
// so the same response can serve multiple requests. The body is cached on first
// use and re-buffered on every call.
func (m *mockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	m.requests = append(m.requests, req)
	if m.err != nil || m.response == nil {
		return m.response, m.err
	}
	if m.bodyBytes == nil && m.response.Body != nil {
		m.bodyBytes, _ = io.ReadAll(m.response.Body)
		m.response.Body.Close()
	}
	clone := *m.response
	if m.bodyBytes != nil {
		clone.Body = io.NopCloser(bytes.NewBuffer(m.bodyBytes))
	}
	return &clone, nil
}

func TestNewPowerDNSClient(t *testing.T) {
	logger := zap.NewNop()
	coreLogger := &core.Logger{Logger: logger}
	client, err := NewPowerDNSClient("http://localhost:8081", testAPIKey(), coreLogger)

	if client == nil {
		t.Fatal("NewPowerDNSClient returned nil")
	}

	if client.client == nil {
		t.Fatal("PowerDNSClient.client is nil")
	}

	if client.logger == nil {
		t.Fatal("PowerDNSClient.logger is nil")
	}

	if err != nil {
		t.Fatalf("NewPowerDNSClient returned error: %v", err)
	}
}

func TestCreateZone(t *testing.T) {
	tests := []struct {
		name           string
		domain         string
		nameservers    []string
		mockResponse   *http.Response
		mockError      error
		expectedStatus int
		expectError    bool
	}{
		{
			name:        "successful zone creation",
			domain:      "example.com.",
			nameservers: []string{"ns1.example.com.", "ns2.example.com."},
			mockResponse: &http.Response{
				StatusCode: http.StatusCreated,
				Body: io.NopCloser(bytes.NewBufferString(`{
					"id": "example.com.",
					"name": "example.com.",
					"kind": "Native"
				}`)),
			},
			expectedStatus: http.StatusCreated,
			expectError:    false,
		},
		{
			name:        "zone creation with API error",
			domain:      "example.com.",
			nameservers: []string{"ns1.example.com."},
			mockError:   errors.New("network error"),
			expectError: true,
		},
		{
			name:        "zone creation with server error",
			domain:      "example.com.",
			nameservers: []string{"ns1.example.com."},
			mockResponse: &http.Response{
				StatusCode: http.StatusInternalServerError,
				Body:       io.NopCloser(bytes.NewBufferString("Internal Server Error")),
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := zap.NewNop()
			coreLogger := &core.Logger{Logger: logger}
			mockClient := &mockHTTPClient{
				response: tt.mockResponse,
				err:      tt.mockError,
			}

			pdnsClient := &PowerDNSClient{
				client: &powerdns.Client{
					Client: mockClient,
					Server: "http://localhost:8081",
				},
				logger: coreLogger,
			}

			ctx := context.Background()
			zone, err := pdnsClient.CreateZone(ctx, tt.domain, tt.nameservers)

			if tt.expectError && err == nil {
				t.Error("expected error but got nil")
			}

			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if !tt.expectError && zone != nil {
				if zone.Name == nil || *zone.Name != tt.domain {
					t.Errorf("expected zone name %s, got %v", tt.domain, zone.Name)
				}
			}
		})
	}
}

func TestGetZone(t *testing.T) {
	tests := []struct {
		name         string
		zoneID       string
		mockResponse *http.Response
		mockError    error
		expectError  bool
	}{
		{
			name:   "successful zone retrieval",
			zoneID: "example.com.",
			mockResponse: &http.Response{
				StatusCode: http.StatusOK,
				Body: io.NopCloser(bytes.NewBufferString(`{
					"id": "example.com.",
					"name": "example.com.",
					"kind": "Native",
					"serial": 2026022601
				}`)),
			},
			expectError: false,
		},
		{
			name:        "zone not found",
			zoneID:      "nonexistent.com.",
			mockError:   errors.New("network error"),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := zap.NewNop()
			coreLogger := &core.Logger{Logger: logger}
			mockClient := &mockHTTPClient{
				response: tt.mockResponse,
				err:      tt.mockError,
			}

			pdnsClient := &PowerDNSClient{
				client: &powerdns.Client{
					Client: mockClient,
					Server: "http://localhost:8081",
				},
				logger: coreLogger,
			}

			ctx := context.Background()
			zone, err := pdnsClient.GetZone(ctx, tt.zoneID)

			if tt.expectError && err == nil {
				t.Error("expected error but got nil")
			}

			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if !tt.expectError && zone != nil {
				if zone.Id == nil || *zone.Id != tt.zoneID {
					t.Errorf("expected zone ID %s, got %v", tt.zoneID, zone.Id)
				}
			}
		})
	}
}

// TestGetZoneRRSetsQuery distinguishes the two accessors: GetZone stays on the
// light payload (no rrsets query), while GetZoneWithRRSets requests
// ?rrsets=true so callers that need the SOA (e.g. fixSOAMNAME) actually receive
// the zone's rrsets instead of a silent nil.
func TestGetZoneRRSetsQuery(t *testing.T) {
	getParams := make(chan string, 4)
	returnedRRSets := []powerdns.RRSet{{Name: "example.com.", Type: "SOA"}}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		getParams <- r.URL.Query().Get("rrsets")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(powerdns.Zone{
			Id:     strPtr("example.com."),
			Name:   strPtr("example.com."),
			Rrsets: &returnedRRSets,
		})
	}))
	defer server.Close()

	logger := zap.NewNop()
	coreLogger := &core.Logger{Logger: logger}
	client, err := NewPowerDNSClient(server.URL, testAPIKey(), coreLogger)
	if err != nil {
		t.Fatalf("NewPowerDNSClient failed: %v", err)
	}

	// Plain GetZone must NOT request rrsets (light payload).
	if _, err := client.GetZone(context.Background(), "example.com."); err != nil {
		t.Fatalf("GetZone returned error: %v", err)
	}
	if p := <-getParams; p != "" {
		t.Errorf("expected GetZone to omit rrsets query, got %q", p)
	}

	// GetZoneWithRRSets must request rrsets=true and surface Rrsets.
	zone, err := client.GetZoneWithRRSets(context.Background(), "example.com.")
	if err != nil {
		t.Fatalf("GetZoneWithRRSets returned error: %v", err)
	}
	if p := <-getParams; p != "true" {
		t.Errorf("expected GetZoneWithRRSets to send rrsets=true, got %q", p)
	}
	if zone == nil || zone.Rrsets == nil {
		t.Fatal("expected Rrsets in GetZoneWithRRSets response")
	}
}

func TestUpdateZoneRRSets(t *testing.T) {
	tests := []struct {
		name         string
		zoneID       string
		rrsets       []powerdns.RRSet
		mockResponse *http.Response
		mockError    error
		expectError  bool
	}{
		{
			name:   "successful RRSet update",
			zoneID: "example.com.",
			rrsets: []powerdns.RRSet{
				{
					Changetype: powerdns.REPLACE,
					Name:       "_dnslink.example.com.",
					Type:       "TXT",
					Ttl:        intPtr(300),
					Records: []powerdns.Record{
						{
							Content: `"dnslink=/ipfs/QmTest"`,
						},
					},
				},
			},
			mockResponse: &http.Response{
				StatusCode: http.StatusNoContent,
				Body:       io.NopCloser(bytes.NewBuffer(nil)),
			},
			expectError: false,
		},
		{
			name:        "RRSet update with API error",
			zoneID:      "example.com.",
			rrsets:      []powerdns.RRSet{},
			mockError:   errors.New("network error"),
			expectError: true,
		},
		{
			name:   "RRSet update with 422 from PowerDNS",
			zoneID: "example.com.",
			rrsets: []powerdns.RRSet{
				{
					Changetype: powerdns.REPLACE,
					Name:       "test.example.com.",
					Type:       "CNAME",
					Ttl:        intPtr(300),
					Records: []powerdns.Record{
						{Content: "target.example.com."},
					},
				},
			},
			mockResponse: &http.Response{
				StatusCode: http.StatusUnprocessableEntity,
				Body:       io.NopCloser(bytes.NewBufferString(`{"error": "CNAME conflict"}`)),
			},
			expectError: true,
		},
		{
			name:         "RRSet update with nil response",
			zoneID:       "example.com.",
			rrsets:       []powerdns.RRSet{},
			mockResponse: nil,
			mockError:    nil,
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := zap.NewNop()
			coreLogger := &core.Logger{Logger: logger}
			mockClient := &mockHTTPClient{
				response: tt.mockResponse,
				err:      tt.mockError,
			}

			pdnsClient := &PowerDNSClient{
				client: &powerdns.Client{
					Client: mockClient,
					Server: "http://localhost:8081",
				},
				logger: coreLogger,
			}

			ctx := context.Background()
			err := pdnsClient.UpdateZoneRRSets(ctx, tt.zoneID, tt.rrsets)

			if tt.expectError && err == nil {
				t.Error("expected error but got nil")
			}

			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestDeleteZone(t *testing.T) {
	tests := []struct {
		name         string
		zoneID       string
		mockResponse *http.Response
		mockError    error
		expectError  bool
	}{
		{
			name:   "successful zone deletion",
			zoneID: "example.com.",
			mockResponse: &http.Response{
				StatusCode: http.StatusNoContent,
				Body:       io.NopCloser(bytes.NewBuffer(nil)),
			},
			expectError: false,
		},
		{
			name:        "zone deletion with API error",
			zoneID:      "example.com.",
			mockError:   errors.New("network error"),
			expectError: true,
		},
		{
			name:   "zone deletion with 404 from PowerDNS",
			zoneID: "example.com.",
			mockResponse: &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(bytes.NewBufferString(`{"error": "Zone not found"}`)),
			},
			expectError: true,
		},
		{
			name:         "zone deletion with nil response",
			zoneID:       "example.com.",
			mockResponse: nil,
			mockError:    nil,
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := zap.NewNop()
			coreLogger := &core.Logger{Logger: logger}
			mockClient := &mockHTTPClient{
				response: tt.mockResponse,
				err:      tt.mockError,
			}

			pdnsClient := &PowerDNSClient{
				client: &powerdns.Client{
					Client: mockClient,
					Server: "http://localhost:8081",
				},
				logger: coreLogger,
			}

			ctx := context.Background()
			err := pdnsClient.DeleteZone(ctx, tt.zoneID)

			if tt.expectError && err == nil {
				t.Error("expected error but got nil")
			}

			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestCreateZoneIntegration(t *testing.T) {
	// Integration test with actual HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodPost:
			// Verify request method and path
			if r.URL.Path != "/servers/localhost/zones" {
				t.Errorf("expected path /servers/localhost/zones, got %s", r.URL.Path)
			}

			// Verify API key header
			apiKey := r.Header.Get("X-API-Key")
			if apiKey != testAPIKey() {
				t.Errorf("expected API key %q, got %q", testAPIKey(), apiKey)
			}

			// Verify request body
			var body powerdns.ZoneCreate
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("failed to decode request body: %v", err)
			}

			if body.Name != "example.com." {
				t.Errorf("expected domain 'example.com.', got '%s'", body.Name)
			}

			// Return success response
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(powerdns.Zone{
				Id:   strPtr("example.com."),
				Name: strPtr("example.com."),
				Kind: (*powerdns.ZoneKind)(strPtr("Native")),
			})
		case http.MethodGet:
			// GetZone (called by the MNAME fix): return the zone with the SOA
			// RRset PowerDNS generated.
			ttl := 3600
			json.NewEncoder(w).Encode(powerdns.Zone{
				Id:   strPtr("example.com."),
				Name: strPtr("example.com."),
				Rrsets: &[]powerdns.RRSet{
					{Name: "example.com.", Type: "SOA", Ttl: &ttl,
						Records: []powerdns.Record{{Content: "a.misconfigured.dns.server.invalid. hostmaster.example.com. 2024052601 10800 3600 604800 3600"}}},
				},
			})
		case http.MethodPatch:
			// UpdateZoneRRSets (MNAME fix): 204 No Content.
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected method %s", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	logger := zap.NewNop()
	coreLogger := &core.Logger{Logger: logger}
	client, err := NewPowerDNSClient(server.URL, testAPIKey(), coreLogger)
	if err != nil {
		t.Fatalf("NewPowerDNSClient failed: %v", err)
	}

	ctx := context.Background()
	zone, err := client.CreateZone(ctx, "example.com.", []string{"ns1.example.com."})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if zone == nil {
		t.Fatal("expected zone, got nil")
	}

	if zone.Name == nil || *zone.Name != "example.com." {
		t.Errorf("expected zone name 'example.com.', got %v", zone.Name)
	}
}

func TestGetZoneIntegration(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET request, got %s", r.Method)
		}

		if r.URL.Path != "/servers/localhost/zones/example.com." {
			t.Errorf("expected path /servers/localhost/zones/example.com., got %s", r.URL.Path)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(powerdns.Zone{
			Id:     strPtr("example.com."),
			Name:   strPtr("example.com."),
			Kind:   (*powerdns.ZoneKind)(strPtr("Native")),
			Serial: intPtr(2026022601),
		})
	}))
	defer server.Close()

	logger := zap.NewNop()
	coreLogger := &core.Logger{Logger: logger}
	client, err := NewPowerDNSClient(server.URL, testAPIKey(), coreLogger)
	if err != nil {
		t.Fatalf("NewPowerDNSClient failed: %v", err)
	}

	ctx := context.Background()
	zone, err := client.GetZone(ctx, "example.com.")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if zone == nil {
		t.Fatal("expected zone, got nil")
	}

	if zone.Id == nil || *zone.Id != "example.com." {
		t.Errorf("expected zone ID 'example.com.', got %v", zone.Id)
	}
}

func TestUpdateZoneRRSetsIntegration(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			t.Errorf("expected PATCH request, got %s", r.Method)
		}

		if r.URL.Path != "/servers/localhost/zones/example.com." {
			t.Errorf("expected path /servers/localhost/zones/example.com., got %s", r.URL.Path)
		}

		// Verify request body
		var body powerdns.ZonePatch
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("failed to decode request body: %v", err)
		}

		if body.Rrsets == nil || len(*body.Rrsets) != 1 {
			t.Errorf("expected 1 RRSet, got %d", len(*body.Rrsets))
		}

		rrset := (*body.Rrsets)[0]
		if rrset.Name != "_dnslink.example.com." {
			t.Errorf("expected RRSet name '_dnslink.example.com.', got '%s'", rrset.Name)
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	logger := zap.NewNop()
	coreLogger := &core.Logger{Logger: logger}
	client, err := NewPowerDNSClient(server.URL, testAPIKey(), coreLogger)
	if err != nil {
		t.Fatalf("NewPowerDNSClient failed: %v", err)
	}

	ctx := context.Background()
	rrsets := []powerdns.RRSet{
		{
			Changetype: powerdns.REPLACE,
			Name:       "_dnslink.example.com.",
			Type:       "TXT",
			Ttl:        intPtr(300),
			Records: []powerdns.Record{
				{
					Content: `"dnslink=/ipfs/QmTest"`,
				},
			},
		},
	}

	err = client.UpdateZoneRRSets(ctx, "example.com.", rrsets)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDeleteZoneIntegration(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE request, got %s", r.Method)
		}

		if r.URL.Path != "/servers/localhost/zones/example.com." {
			t.Errorf("expected path /servers/localhost/zones/example.com., got %s", r.URL.Path)
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	logger := zap.NewNop()
	coreLogger := &core.Logger{Logger: logger}
	client, err := NewPowerDNSClient(server.URL, testAPIKey(), coreLogger)
	if err != nil {
		t.Fatalf("NewPowerDNSClient failed: %v", err)
	}

	ctx := context.Background()
	err = client.DeleteZone(ctx, "example.com.")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCreateZoneCanonicalDomain(t *testing.T) {
	tests := []struct {
		name         string
		inputDomain  string
		expectedBody string
		expectError  bool
	}{
		{
			name:         "canonicalize domain without trailing dot",
			inputDomain:  "example.com",
			expectedBody: `{"kind":"Native","name":"example.com.","nameservers":[]}`,
			expectError:  false,
		},
		{
			name:         "preserve existing trailing dot",
			inputDomain:  "example.com.",
			expectedBody: `{"kind":"Native","name":"example.com.","nameservers":[]}`,
			expectError:  false,
		},
		{
			name:         "canonicalize subdomain without trailing dot",
			inputDomain:  "test.example.com",
			expectedBody: `{"kind":"Native","name":"test.example.com.","nameservers":[]}`,
			expectError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Read raw request body to compare with expectedBody
				bodyBytes, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("failed to read request body: %v", err)
				}

				// Compare actual body with expected body
				actualBody := string(bodyBytes)
				expectedBody := tt.expectedBody

				// Normalize whitespace for comparison
				actualNormalized := strings.ReplaceAll(actualBody, " ", "")
				expectedNormalized := strings.ReplaceAll(expectedBody, " ", "")

				if !tt.expectError && actualNormalized != expectedNormalized {
					t.Errorf("expected request body %q, got %q", expectedBody, actualBody)
				}

				var body powerdns.ZoneCreate
				if err := json.Unmarshal(bodyBytes, &body); err != nil {
					t.Errorf("failed to decode request body: %v", err)
				}

				if body.Name != "example.com." && body.Name != "test.example.com." {
					t.Errorf("expected canonical domain, got '%s'", body.Name)
				}

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				json.NewEncoder(w).Encode(powerdns.Zone{
					Id:   strPtr(body.Name),
					Name: strPtr(body.Name),
					Kind: (*powerdns.ZoneKind)(strPtr("Native")),
				})
			}))
			defer server.Close()

			logger := zap.NewNop()
			coreLogger := &core.Logger{Logger: logger}
			client, err := NewPowerDNSClient(server.URL, testAPIKey(), coreLogger)
			if err != nil {
				t.Fatalf("NewPowerDNSClient failed: %v", err)
			}

			ctx := context.Background()
			zone, err := client.CreateZone(ctx, tt.inputDomain, []string{})

			if tt.expectError && err == nil {
				t.Error("expected error but got nil")
			}

			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if !tt.expectError && zone != nil {
				if zone.Name == nil {
					t.Errorf("expected zone name, got nil")
				} else if !strings.HasSuffix(*zone.Name, ".") {
					t.Errorf("expected zone name to have trailing dot, got '%s'", *zone.Name)
				}
			}
		})
	}
}
func TestCreateZoneCanonicalNameservers(t *testing.T) {
	tests := []struct {
		name        string
		domain      string
		nameservers []string
		expectedNS  []string
		expectError bool
	}{
		{
			name:        "normalize nameservers without trailing dots",
			domain:      "example.com",
			nameservers: []string{"ns1.example.com", "ns2.example.com"},
			expectedNS:  []string{"ns1.example.com.", "ns2.example.com."},
			expectError: false,
		},
		{
			name:        "preserve nameservers with trailing dots",
			domain:      "example.com",
			nameservers: []string{"ns1.example.com.", "ns2.example.com."},
			expectedNS:  []string{"ns1.example.com.", "ns2.example.com."},
			expectError: false,
		},
		{
			name:        "mix of normalized and non-normalized nameservers",
			domain:      "example.com",
			nameservers: []string{"ns1.example.com", "ns2.example.com.", "ns3.example.com"},
			expectedNS:  []string{"ns1.example.com.", "ns2.example.com.", "ns3.example.com."},
			expectError: false,
		},
		{
			name:        "single nameserver without trailing dot",
			domain:      "example.com",
			nameservers: []string{"ns1.example.com"},
			expectedNS:  []string{"ns1.example.com."},
			expectError: false,
		},
		{
			name:        "empty nameserver list",
			domain:      "example.com",
			nameservers: []string{},
			expectedNS:  []string{},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var capturedBody powerdns.ZoneCreate

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.Method {
				case http.MethodPost:
					// CreateZone: capture and validate the create body.
					bodyBytes, err := io.ReadAll(r.Body)
					if err != nil {
						t.Errorf("failed to read request body: %v", err)
					}

					if err := json.Unmarshal(bodyBytes, &capturedBody); err != nil {
						t.Errorf("failed to decode request body: %v", err)
					}

					// Verify nameservers are normalized
					if capturedBody.Nameservers == nil {
						t.Fatalf("expected nameservers to be set, got nil")
					}

					actualNameservers := *capturedBody.Nameservers

					if len(actualNameservers) != len(tt.expectedNS) {
						t.Errorf("expected %d nameservers, got %d", len(tt.expectedNS), len(actualNameservers))
					}

					for i, expected := range tt.expectedNS {
						if i >= len(actualNameservers) {
							t.Errorf("missing nameserver at index %d", i)
							break
						}
						if actualNameservers[i] != expected {
							t.Errorf("expected nameserver[%d] to be %q, got %q", i, expected, actualNameservers[i])
						}
					}

					// Verify all nameservers have trailing dots
					for _, ns := range actualNameservers {
						if !strings.HasSuffix(ns, ".") {
							t.Errorf("expected nameserver '%s' to have trailing dot", ns)
						}
					}

					w.WriteHeader(http.StatusCreated)
					json.NewEncoder(w).Encode(powerdns.Zone{
						Id:   strPtr(capturedBody.Name),
						Name: strPtr(capturedBody.Name),
						Kind: (*powerdns.ZoneKind)(strPtr("Native")),
					})
				case http.MethodGet:
					// GetZone (called by the MNAME fix): return the zone with the
					// SOA RRset PowerDNS generated.
					ttl := 3600
					json.NewEncoder(w).Encode(powerdns.Zone{
						Id:   strPtr(tt.domain + "."),
						Name: strPtr(tt.domain + "."),
						Rrsets: &[]powerdns.RRSet{
							{Name: tt.domain + ".", Type: "SOA", Ttl: &ttl,
								Records: []powerdns.Record{{Content: "a.misconfigured.dns.server.invalid. hostmaster." + tt.domain + ". 2024052601 10800 3600 604800 3600"}}},
						},
					})
				case http.MethodPatch:
					// UpdateZoneRRSets (MNAME fix): 204 No Content.
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Errorf("unexpected method %s", r.Method)
					w.WriteHeader(http.StatusMethodNotAllowed)
				}
			}))
			defer server.Close()

			logger := zap.NewNop()
			coreLogger := &core.Logger{Logger: logger}
			client, err := NewPowerDNSClient(server.URL, testAPIKey(), coreLogger)
			if err != nil {
				t.Fatalf("NewPowerDNSClient failed: %v", err)
			}

			ctx := context.Background()
			zone, err := client.CreateZone(ctx, tt.domain, tt.nameservers)

			if tt.expectError && err == nil {
				t.Error("expected error but got nil")
			}

			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if !tt.expectError && zone != nil {
				if zone.Name == nil {
					t.Errorf("expected zone name, got nil")
				} else if !strings.HasSuffix(*zone.Name, ".") {
					t.Errorf("expected zone name to have trailing dot, got '%s'", *zone.Name)
				}
			}
		})
	}
}

func TestCreateZoneSOAMNAME(t *testing.T) {
	// PowerDNS seeds a new zone's SOA with this placeholder MNAME; the fix
	// must replace only field[0] and preserve the rest (especially the serial).
	const placeholder = "a.misconfigured.dns.server.invalid."
	const serverSerial = "2024052601"

	tests := []struct {
		name           string
		domain         string
		nameservers    []string
		expectSOAMNAME string
		// When true, no nameservers means the MNAME fix is skipped entirely and
		// no PATCH is issued.
		expectNoFix bool
	}{
		{
			name:           "primary nameserver becomes SOA MNAME",
			domain:         "example.com",
			nameservers:    []string{"ns1.example.com.", "ns2.example.com."},
			expectSOAMNAME: "ns1.example.com.",
		},
		{
			name:           "MNAME normalized to FQDN",
			domain:         "example.com",
			nameservers:    []string{"ns1.example.com", "ns2.example.com"},
			expectSOAMNAME: "ns1.example.com.",
		},
		{
			name:        "no nameservers means no MNAME fix (no PATCH)",
			domain:      "example.com",
			nameservers: []string{},
			expectNoFix: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var createdBody powerdns.ZoneCreate
			var patchedBody powerdns.ZonePatch
			patchCount := 0

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.Method {
				case http.MethodPost:
					// CreateZone: capture body, echo a minimal zone back.
					bodyBytes, _ := io.ReadAll(r.Body)
					if err := json.Unmarshal(bodyBytes, &createdBody); err != nil {
						t.Errorf("failed to decode create body: %v", err)
					}
					w.WriteHeader(http.StatusCreated)
					json.NewEncoder(w).Encode(powerdns.Zone{
						Id:   strPtr(createdBody.Name),
						Name: strPtr(createdBody.Name),
						Kind: (*powerdns.ZoneKind)(strPtr("Native")),
					})
				case http.MethodGet:
					// GetZone (called by fixSOAMNAME): must request rrsets=true
					// or PowerDNS returns Rrsets as nil and the MNAME fix is a
					// silent no-op. Assert the query param is present.
					if q := r.URL.Query().Get("rrsets"); q != "true" {
						t.Errorf("expected GET /zones/{id}?rrsets=true, got rrsets=%q", q)
					}
					// Return the zone with the SOA RRset PowerDNS generated.
					ttl := 3600
					soaContent := fmt.Sprintf("%s hostmaster.%s %s 10800 3600 604800 3600", placeholder, tt.domain, serverSerial)
					json.NewEncoder(w).Encode(powerdns.Zone{
						Id:   strPtr(tt.domain + "."),
						Name: strPtr(tt.domain + "."),
						Rrsets: &[]powerdns.RRSet{
							{Name: tt.domain + ".", Type: "SOA", Ttl: &ttl, Records: []powerdns.Record{{Content: soaContent}}},
						},
					})
				case http.MethodPatch:
					// UpdateZoneRRSets (fixSOAMNAME PATCH): capture body.
					patchCount++
					bodyBytes, _ := io.ReadAll(r.Body)
					if err := json.Unmarshal(bodyBytes, &patchedBody); err != nil {
						t.Errorf("failed to decode patch body: %v", err)
					}
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Errorf("unexpected method %s", r.Method)
					w.WriteHeader(http.StatusMethodNotAllowed)
				}
			}))
			defer server.Close()

			logger := zap.NewNop()
			coreLogger := &core.Logger{Logger: logger}
			client, err := NewPowerDNSClient(server.URL, testAPIKey(), coreLogger)
			if err != nil {
				t.Fatalf("NewPowerDNSClient failed: %v", err)
			}

			_, err = client.CreateZone(context.Background(), tt.domain, tt.nameservers)
			if err != nil {
				t.Fatalf("CreateZone returned error: %v", err)
			}

			// 1) We must NOT inject an explicit SOA RRSet at creation: with only
			// `nameservers` set, PowerDNS auto-generates SOA + NS (so delegation
			// is never broken) and owns the serial. Supplying our own SOA here
			// would fight PowerDNS's serial management.
			if createdBody.Rrsets != nil && len(*createdBody.Rrsets) != 0 {
				t.Fatalf("expected no explicit rrsets in create request (PowerDNS should auto-generate SOA+NS), got %+v", createdBody.Rrsets)
			}

			if tt.expectNoFix {
				if patchCount != 0 {
					t.Fatalf("expected no MNAME fix PATCH when no nameservers, got %d", patchCount)
				}
				return
			}

			// 2) A single MNAME-fix PATCH must have been issued.
			if patchCount != 1 {
				t.Fatalf("expected exactly 1 MNAME-fix PATCH, got %d", patchCount)
			}
			if patchedBody.Rrsets == nil || len(*patchedBody.Rrsets) != 1 {
				t.Fatalf("expected exactly one RRSet in MNAME-fix PATCH, got %+v", patchedBody.Rrsets)
			}
			soa := (*patchedBody.Rrsets)[0]
			if soa.Type != "SOA" || soa.Changetype != powerdns.REPLACE {
				t.Fatalf("expected SOA REPLACE RRSet, got type=%s changetype=%s", soa.Type, soa.Changetype)
			}
			if len(soa.Records) != 1 {
				t.Fatalf("expected one SOA record in patch, got %d", len(soa.Records))
			}

			fields := strings.Fields(soa.Records[0].Content)
			if len(fields) != 7 {
				t.Fatalf("expected 7 SOA fields (MNAME RNAME SERIAL REFRESH RETRY EXPIRE MINIMUM), got %d: %q", len(fields), soa.Records[0].Content)
			}
			if fields[0] != tt.expectSOAMNAME {
				t.Errorf("expected SOA MNAME %q, got %q", tt.expectSOAMNAME, fields[0])
			}
			if fields[0] == placeholder {
				t.Error("SOA MNAME must not be the PowerDNS placeholder 'a.misconfigured.dns.server.invalid.'")
			}

			// 3) The serial must be preserved verbatim from what PowerDNS
			// generated — we only swap the MNAME, never fabricate a serial.
			if fields[2] != serverSerial {
				t.Errorf("expected serial %q preserved, got %q (we must not fabricate a serial)", serverSerial, fields[2])
			}

			// 4) Timing fields must remain present.
			for _, f := range fields[3:] {
				if f == "" {
					t.Errorf("SOA timing fields must be present: %q", soa.Records[0].Content)
				}
			}
		})
	}
}

// TestCreateZoneBestEffortSoaMNameFixReturnsCreatedZone verifies the
// fresh-create path treats the SOA MNAME correction as strictly best-effort.
// Even when the follow-up GET returns a malformed SOA (forcing fixSOAMNAME to
// fail), CreateZone must still succeed and return the created zone — a
// transient fetch/PATCH failure must never fail the create nor destroy the
// live zone — and must not issue a compensating DeleteZone.
func TestCreateZoneBestEffortSoaMNameFixReturnsCreatedZone(t *testing.T) {
	var (
		mu          sync.Mutex
		deleteCalls int
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(powerdns.Zone{
				Id:   strPtr("example.com."),
				Name: strPtr("example.com."),
				Kind: (*powerdns.ZoneKind)(strPtr("Native")),
			})
		case http.MethodGet:
			// Return a zone whose SOA record content is empty -> malformed SOA
			// -> fixSOAMNAMEOnZone returns an error.
			ttl := 3600
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(powerdns.Zone{
				Id:   strPtr("example.com."),
				Name: strPtr("example.com."),
				Rrsets: &[]powerdns.RRSet{
					{Name: "example.com.", Type: "SOA", Ttl: &ttl, Records: []powerdns.Record{{Content: ""}}},
				},
			})
		case http.MethodDelete:
			mu.Lock()
			deleteCalls++
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()

	logger := zap.NewNop()
	coreLogger := &core.Logger{Logger: logger}
	client, err := NewPowerDNSClient(server.URL, testAPIKey(), coreLogger)
	if err != nil {
		t.Fatalf("NewPowerDNSClient failed: %v", err)
	}

	zone, err := client.CreateZone(context.Background(), "example.com", []string{"ns1.example.com.", "ns2.example.com."})
	if err != nil {
		t.Fatalf("expected success despite best-effort SOA MNAME fix failure, got error: %v", err)
	}
	if zone == nil || zone.Id == nil || *zone.Id != "example.com." {
		t.Fatalf("expected the created zone to be returned, got %v", zone)
	}
	mu.Lock()
	defer mu.Unlock()
	if deleteCalls != 0 {
		t.Errorf("expected no compensating DeleteZone on best-effort path, got %d", deleteCalls)
	}
}

func TestCreateZoneHTTPErrorHandling(t *testing.T) {
	tests := []struct {
		name          string
		statusCode    int
		errorBody     string
		expectedError string
	}{
		{
			name:          "HTTP 422 unprocessable entity with error message",
			statusCode:    http.StatusUnprocessableEntity,
			errorBody:     `{"error": "DNS Name 'test.example.com' is not canonical"}`,
			expectedError: "PowerDNS API returned status 422",
		},
		{
			name:          "HTTP 400 bad request",
			statusCode:    http.StatusBadRequest,
			errorBody:     `{"error": "Invalid request"}`,
			expectedError: "PowerDNS API returned status 400",
		},
		{
			name:          "HTTP 500 internal server error",
			statusCode:    http.StatusInternalServerError,
			errorBody:     `Internal Server Error`,
			expectedError: "PowerDNS API returned status 500",
		},
		{
			name:          "HTTP 404 not found",
			statusCode:    http.StatusNotFound,
			errorBody:     `{"error": "Not found"}`,
			expectedError: "PowerDNS API returned status 404",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)
				w.Write([]byte(tt.errorBody))
			}))
			defer server.Close()

			logger := zap.NewNop()
			coreLogger := &core.Logger{Logger: logger}
			client, err := NewPowerDNSClient(server.URL, testAPIKey(), coreLogger)
			if err != nil {
				t.Fatalf("NewPowerDNSClient failed: %v", err)
			}

			ctx := context.Background()
			zone, err := client.CreateZone(ctx, "example.com", []string{})

			if err == nil {
				t.Error("expected error but got nil")
			}

			if !strings.Contains(err.Error(), tt.expectedError) {
				t.Errorf("expected error to contain %q, got %v", tt.expectedError, err)
			}

			if zone != nil {
				t.Errorf("expected nil zone on error, got %v", zone)
			}

			if strings.Contains(err.Error(), tt.errorBody) {
				t.Logf("✓ Error body included in error message: %v", err)
			}
		})
	}
}

func TestCreateZoneErrorResponseBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		w.Write([]byte(`{"error": "DNS Name is not canonical", "details": "must end with a dot"}`))
	}))
	defer server.Close()

	logger := zap.NewNop()
	coreLogger := &core.Logger{Logger: logger}
	client, err := NewPowerDNSClient(server.URL, testAPIKey(), coreLogger)
	if err != nil {
		t.Fatalf("NewPowerDNSClient failed: %v", err)
	}

	ctx := context.Background()
	zone, err := client.CreateZone(ctx, "invalid-domain", []string{})

	if err == nil {
		t.Fatal("expected error but got nil")
	}

	if zone != nil {
		t.Errorf("expected nil zone on error, got %v", zone)
	}

	// Verify the error message includes the HTTP status and body
	if !strings.Contains(err.Error(), "422") {
		t.Errorf("expected error to contain status code 422, got %v", err)
	}

	if !strings.Contains(err.Error(), "DNS Name is not canonical") {
		t.Errorf("expected error to contain PowerDNS error message, got %v", err)
	}
}

func TestCreateZoneConflictFetchesExisting(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if r.Method == http.MethodPost && r.URL.Path == "/servers/localhost/zones" {
			w.WriteHeader(http.StatusConflict)
			w.Write([]byte(`{"error": "Conflict"}`))
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/servers/localhost/zones/example.com." {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(powerdns.Zone{
				Id:   strPtr("example.com."),
				Name: strPtr("example.com."),
				Kind: (*powerdns.ZoneKind)(strPtr("Native")),
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	logger := zap.NewNop()
	coreLogger := &core.Logger{Logger: logger}
	client, err := NewPowerDNSClient(server.URL, testAPIKey(), coreLogger)
	if err != nil {
		t.Fatalf("NewPowerDNSClient failed: %v", err)
	}

	ctx := context.Background()
	zone, err := client.CreateZone(ctx, "example.com", []string{"ns1.example.com."})

	if err != nil {
		t.Fatalf("expected no error on 409 conflict, got: %v", err)
	}
	if zone == nil {
		t.Fatal("expected zone, got nil")
	}
	if zone.Id == nil || *zone.Id != "example.com." {
		t.Errorf("expected zone ID 'example.com.', got %v", zone.Id)
	}
	if requestCount != 2 {
		t.Errorf("expected 2 requests (POST + GET), got %d", requestCount)
	}
}

func TestCreateZoneConflictGetAlsoFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/servers/localhost/zones" {
			w.WriteHeader(http.StatusConflict)
			w.Write([]byte(`{"error": "Conflict"}`))
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	logger := zap.NewNop()
	coreLogger := &core.Logger{Logger: logger}
	client, err := NewPowerDNSClient(server.URL, testAPIKey(), coreLogger)
	if err != nil {
		t.Fatalf("NewPowerDNSClient failed: %v", err)
	}

	ctx := context.Background()
	zone, err := client.CreateZone(ctx, "example.com", []string{"ns1.example.com."})

	if err == nil {
		t.Error("expected error when GET also fails after 409")
	}
	if !strings.Contains(err.Error(), "zone already exists") {
		t.Errorf("expected error to mention 'zone already exists', got: %v", err)
	}
	if zone != nil {
		t.Errorf("expected nil zone on error, got %v", zone)
	}
}

func TestCreateZoneConflictExistingZoneNoID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/servers/localhost/zones" {
			w.WriteHeader(http.StatusConflict)
			w.Write([]byte(`{"error": "Conflict"}`))
			return
		}
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(powerdns.Zone{
				Name: strPtr("example.com."),
				Kind: (*powerdns.ZoneKind)(strPtr("Native")),
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	logger := zap.NewNop()
	coreLogger := &core.Logger{Logger: logger}
	client, err := NewPowerDNSClient(server.URL, testAPIKey(), coreLogger)
	if err != nil {
		t.Fatalf("NewPowerDNSClient failed: %v", err)
	}

	ctx := context.Background()
	zone, err := client.CreateZone(ctx, "example.com", []string{"ns1.example.com."})

	if err == nil {
		t.Error("expected error when existing zone has no ID")
	}
	if !strings.Contains(err.Error(), "existing zone has no ID") {
		t.Errorf("expected error about missing ID, got: %v", err)
	}
	if zone != nil {
		t.Errorf("expected nil zone on error, got %v", zone)
	}
}

func TestIsDuplicateKeyError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"sqlite unique", fmt.Errorf("UNIQUE constraint failed: ipfs_dns_zones.domain"), true},
		{"mysql duplicate", fmt.Errorf("Duplicate entry 'example.com' for key 'idx_dns_zones_domain'"), true},
		{"postgres duplicate", fmt.Errorf("duplicate key value violates unique constraint"), true},
		{"other error", fmt.Errorf("some other error"), false},
		{"nil error", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isDuplicateKeyError(tt.err)
			if result != tt.expected {
				t.Errorf("isDuplicateKeyError(%v) = %v, want %v", tt.err, result, tt.expected)
			}
		})
	}
}

// Helper functions

func TestEnableDNSSEC(t *testing.T) {
	t.Run("reuses existing active KSK (no POST, idempotent)", func(t *testing.T) {
		var gotPOST bool
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				gotPOST = true
				t.Error("expected no POST when an active KSK exists")
			}
			if r.URL.Path != "/servers/localhost/zones/lumeweb./cryptokeys" {
				t.Errorf("unexpected path: %s", r.URL.Path)
			}
			// The idempotent reuse depends on details=true returning the dnskey
			// content; missing it means DNSKey is empty and the guard never fires.
			if r.URL.Query().Get("details") != "true" {
				t.Errorf("expected ?details=true on cryptokeys GET, got query %q", r.URL.RawQuery)
			}
			// PowerDNS lists cryptokeys as an array.
			body := `[{"id":"1","keytype":"ksk","active":true,"dnskey":"257 3 13 AwEAAaX9pZzY3eiw=="}]`
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(body))
		}))
		defer server.Close()

		logger := zap.NewNop()
		coreLogger := &core.Logger{Logger: logger}
		client, err := NewPowerDNSClient(server.URL, testAPIKey(), coreLogger)
		if err != nil {
			t.Fatalf("NewPowerDNSClient failed: %v", err)
		}

		dnskey, err := client.EnableDNSSEC(context.Background(), "lumeweb.")
		if err != nil {
			t.Fatalf("EnableDNSSEC returned error: %v", err)
		}
		if dnskey != "257 3 13 AwEAAaX9pZzY3eiw==" {
			t.Errorf("unexpected dnskey: %q", dnskey)
		}
		if gotPOST {
			t.Fatal("EnableDNSSEC created a new KSK despite an active KSK existing")
		}
	})

	t.Run("errors when multiple active keys exist (no silent guess)", func(t *testing.T) {
		var gotPOST bool
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost {
				gotPOST = true
				t.Error("expected no POST when an active KSK exists")
			}
			if r.URL.Path != "/servers/localhost/zones/lumeweb./cryptokeys" {
				t.Errorf("unexpected path: %s", r.URL.Path)
			}
			if r.Method == http.MethodGet && r.URL.Query().Get("details") != "true" {
				t.Errorf("expected ?details=true on cryptokeys GET, got query %q", r.URL.RawQuery)
			}
			// Two active signing keys: the published one cannot be identified, so
			// EnableDNSSEC must error rather than guess.
			body := `[{"id":"1","keytype":"ksk","active":true,"dnskey":"257 3 13 AwEAAaX9pZzY3eiw=="},{"id":"2","keytype":"ksk","active":true,"dnskey":"257 3 13 AwEAAbB0yYx4fZQ=="}]`
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(body))
		}))
		defer server.Close()

		logger := zap.NewNop()
		coreLogger := &core.Logger{Logger: logger}
		client, err := NewPowerDNSClient(server.URL, testAPIKey(), coreLogger)
		if err != nil {
			t.Fatalf("NewPowerDNSClient failed: %v", err)
		}

		_, err = client.EnableDNSSEC(context.Background(), "lumeweb.")
		if err == nil {
			t.Fatal("expected an error when multiple active signing keys exist and the published DS cannot be identified")
		}
		if !strings.Contains(err.Error(), "reconcile manually") {
			t.Errorf("expected a reconcile-manually error, got: %v", err)
		}
		if gotPOST {
			t.Fatal("EnableDNSSEC created a new KSK despite multiple active KSKs existing")
		}
	})

	t.Run("creates KSK only when none exists (404 list -> POST)", func(t *testing.T) {
		methodLog := []string{}
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			methodLog = append(methodLog, r.Method)
			if r.URL.Path != "/servers/localhost/zones/lumeweb./cryptokeys" {
				t.Errorf("unexpected path: %s", r.URL.Path)
			}
			// The idempotent reuse depends on details=true returning the dnskey
			// content; missing it means DNSKey is empty and the guard never fires.
			// Only the GET (list) carries the query; the POST (create) does not.
			if r.Method == http.MethodGet && r.URL.Query().Get("details") != "true" {
				t.Errorf("expected ?details=true on cryptokeys GET, got query %q", r.URL.RawQuery)
			}
			if r.Method == http.MethodGet {
				// No cryptokeys yet -> 404.
				w.WriteHeader(http.StatusNotFound)
				return
			}
			// POST returns the freshly created KSK.
			body := `{"dnskey":"257 3 13 AwEAAaX9pZzY3eiw==","ds":["45688 13 2 1F287B0F9E0C1A2B3C4D5E6F7A8B9C0D1E2F3A4B5C6D7E8F9A0B1C2D3E4F5"]}`
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(body))
		}))
		defer server.Close()

		logger := zap.NewNop()
		coreLogger := &core.Logger{Logger: logger}
		client, err := NewPowerDNSClient(server.URL, testAPIKey(), coreLogger)
		if err != nil {
			t.Fatalf("NewPowerDNSClient failed: %v", err)
		}

		dnskey, err := client.EnableDNSSEC(context.Background(), "lumeweb.")
		if err != nil {
			t.Fatalf("EnableDNSSEC returned error: %v", err)
		}
		if dnskey != "257 3 13 AwEAAaX9pZzY3eiw==" {
			t.Errorf("unexpected dnskey: %q", dnskey)
		}
		// Must have been GET (list) then POST (create) — and only one POST.
		if len(methodLog) != 2 || methodLog[0] != http.MethodGet || methodLog[1] != http.MethodPost {
			t.Fatalf("expected GET then POST, got: %v", methodLog)
		}
	})

	t.Run("decodes cryptokey response when creating from empty list", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet {
				// Empty list (200 with []) also means no KSK -> create.
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`[]`))
				return
			}
			if r.Method != http.MethodPost {
				t.Errorf("expected POST after empty list, got %s", r.Method)
			}
			body := `{"dnskey":"257 3 13 AwEAAaX9pZzY3eiw==","ds":["45688 13 2 1F287B0F9E0C1A2B3C4D5E6F7A8B9C0D1E2F3A4B5C6D7E8F9A0B1C2D3E4F5"]}`
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(body))
		}))
		defer server.Close()

		logger := zap.NewNop()
		coreLogger := &core.Logger{Logger: logger}
		client, err := NewPowerDNSClient(server.URL, testAPIKey(), coreLogger)
		if err != nil {
			t.Fatalf("NewPowerDNSClient failed: %v", err)
		}

		dnskey, err := client.EnableDNSSEC(context.Background(), "lumeweb.")
		if err != nil {
			t.Fatalf("EnableDNSSEC returned error: %v", err)
		}
		if dnskey != "257 3 13 AwEAAaX9pZzY3eiw==" {
			t.Errorf("unexpected dnskey: %q", dnskey)
		}
	})

	t.Run("serializes per-zone create under concurrency (only one POST)", func(t *testing.T) {
		// Two concurrent delegation builds for the same zone must not both mint a
		// KSK. The server's list returns no active KSK while a create is in
		// flight, so without the per-zone mutex both goroutines would POST.
		var (
			mu         sync.Mutex
			postCount  int
			createPath string
		)
		createPath = "/servers/localhost/zones/lumeweb./cryptokeys"
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			isPost := r.Method == http.MethodPost
			c := postCount
			if isPost {
				postCount++
			}
			mu.Unlock()

			if r.URL.Path != createPath {
				t.Errorf("unexpected path: %s", r.URL.Path)
			}
			if isPost {
				// Before we mint, list must already see the new key. Simulate
				// PowerDNS returning the created KSK on the next list.
				body := `{"dnskey":"257 3 13 AwEAAaX9pZzY3eiw==","ds":["45688 13 2 1F287B0F9E0C1A2B3C4D5E6F7A8B9C0D1E2F3A4B5C6D7E8F9A0B1C2D3E4F5"]}`
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				_, _ = w.Write([]byte(body))
				return
			}
			// GET list: no active KSK on the very first request (c==0); once a
			// POST has happened (c>0) report the KSK as present.
			if c > 0 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`[{"id":"1","keytype":"ksk","active":true,"dnskey":"257 3 13 AwEAAaX9pZzY3eiw=="}]`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[]`))
		}))
		defer server.Close()

		logger := zap.NewNop()
		coreLogger := &core.Logger{Logger: logger}
		client, err := NewPowerDNSClient(server.URL, testAPIKey(), coreLogger)
		if err != nil {
			t.Fatalf("NewPowerDNSClient failed: %v", err)
		}

		const goroutines = 8
		var wg sync.WaitGroup
		errs := make([]error, goroutines)
		for i := 0; i < goroutines; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				_, e := client.EnableDNSSEC(context.Background(), "lumeweb.")
				errs[idx] = e
			}(i)
		}
		wg.Wait()
		for i, e := range errs {
			if e != nil {
				t.Fatalf("goroutine %d EnableDNSSEC error: %v", i, e)
			}
		}

		mu.Lock()
		defer mu.Unlock()
		if postCount != 1 {
			t.Fatalf("expected exactly one POST (KSK create) under concurrency, got %d", postCount)
		}
	})

	t.Run("propagates non-2xx response", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error": "zone not found"}`))
		}))
		defer server.Close()

		logger := zap.NewNop()
		coreLogger := &core.Logger{Logger: logger}
		client, err := NewPowerDNSClient(server.URL, testAPIKey(), coreLogger)
		if err != nil {
			t.Fatalf("NewPowerDNSClient failed: %v", err)
		}

		_, err = client.EnableDNSSEC(context.Background(), "lumeweb.")
		if err == nil {
			t.Fatal("expected error for non-2xx response, got nil")
		}
		if !strings.Contains(err.Error(), "status 400") {
			t.Errorf("expected status 400 in error, got: %v", err)
		}
	})
}

// TestFixSOAMNAMEOnZoneCorrectsMNAME verifies the core correction helper on the
// fresh-create path, where the zone was just created by the caller and is
// provably portal-owned. The placeholder MNAME is corrected, and an already-
// correct MNAME is left alone (idempotent).
func TestFixSOAMNAMEOnZoneCorrectsMNAME(t *testing.T) {
	ttl := 3600
	makeZone := func(soaContent string) *powerdns.Zone {
		return &powerdns.Zone{
			Id:   strPtr("example.com."),
			Name: strPtr("example.com."),
			Rrsets: &[]powerdns.RRSet{
				{Name: "example.com.", Type: "SOA", Ttl: &ttl, Records: []powerdns.Record{{Content: soaContent}}},
			},
		}
	}

	t.Run("placeholder MNAME is corrected", func(t *testing.T) {
		patchCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPatch {
				patchCount++
			}
			w.WriteHeader(http.StatusNoContent)
		}))
		defer server.Close()

		logger := zap.NewNop()
		coreLogger := &core.Logger{Logger: logger}
		client, err := NewPowerDNSClient(server.URL, testAPIKey(), coreLogger)
		if err != nil {
			t.Fatalf("NewPowerDNSClient failed: %v", err)
		}

		zone := makeZone("a.misconfigured.dns.server.invalid. hostmaster.example.com. 2024052601 10800 3600 604800 3600")
		err = client.fixSOAMNAMEOnZone(context.Background(), "example.com.", "example.com", "ns1.example.com.", zone)
		if err != nil {
			t.Fatalf("fixSOAMNAMEOnZone returned error: %v", err)
		}
		if patchCount != 1 {
			t.Errorf("expected 1 PATCH request, got %d", patchCount)
		}
	})

	t.Run("already-correct MNAME is left alone", func(t *testing.T) {
		patchCount := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPatch {
				patchCount++
			}
			w.WriteHeader(http.StatusNoContent)
		}))
		defer server.Close()

		logger := zap.NewNop()
		coreLogger := &core.Logger{Logger: logger}
		client, err := NewPowerDNSClient(server.URL, testAPIKey(), coreLogger)
		if err != nil {
			t.Fatalf("NewPowerDNSClient failed: %v", err)
		}

		zone := makeZone("ns1.example.com. hostmaster.example.com. 2024052601 10800 3600 604800 3600")
		err = client.fixSOAMNAMEOnZone(context.Background(), "example.com.", "example.com", "ns1.example.com.", zone)
		if err != nil {
			t.Fatalf("fixSOAMNAMEOnZone returned error: %v", err)
		}
		if patchCount != 0 {
			t.Errorf("expected 0 PATCH requests for already-correct MNAME, got %d", patchCount)
		}
	})
}

// TestCreateZone409DoesNotMutateExistingZone verifies the 409 already-exists
// path never issues a write. The existing zone may be a foreign or operator-
// managed zone the portal does not own, so CreateZone must return it untouched
// (POST + GET only, no PATCH) even when nameservers are supplied and the zone
// carries a placeholder SOA MNAME.
func TestCreateZone409DoesNotMutateExistingZone(t *testing.T) {
	patchCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ttl := 3600
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/servers/localhost/zones":
			w.WriteHeader(http.StatusConflict)
			w.Write([]byte(`{"error": "Conflict"}`))
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/servers/localhost/zones/"):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(powerdns.Zone{
				Id:   strPtr("example.com."),
				Name: strPtr("example.com."),
				Rrsets: &[]powerdns.RRSet{
					{Name: "example.com.", Type: "SOA", Ttl: &ttl, Records: []powerdns.Record{{Content: "a.misconfigured.dns.server.invalid. hostmaster.example.com. 2024052601 10800 3600 604800 3600"}}},
				},
			})
		case r.Method == http.MethodPatch:
			patchCount++
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	logger := zap.NewNop()
	coreLogger := &core.Logger{Logger: logger}
	client, err := NewPowerDNSClient(server.URL, testAPIKey(), coreLogger)
	if err != nil {
		t.Fatalf("NewPowerDNSClient failed: %v", err)
	}

	zone, err := client.CreateZone(context.Background(), "example.com", []string{"ns1.example.com.", "ns2.example.com."})
	if err != nil {
		t.Fatalf("expected no error on 409, got: %v", err)
	}
	if zone == nil || zone.Id == nil || *zone.Id != "example.com." {
		t.Fatalf("expected recovered zone ID 'example.com.', got %v", zone.Id)
	}
	if patchCount != 0 {
		t.Errorf("expected 0 PATCH requests on 409 (must not mutate a non-owned zone), got %d", patchCount)
	}
}

func intPtr(i int) *int {
	return &i
}

// testAPIKey returns the PowerDNS API key used by these tests. It comes from
// the POWERDNS_TEST_API_KEY environment variable when set; both the client
// under test and the mock-server header assertion use this same helper, so the
// tests remain self-consistent. When unset it falls back to an explicitly
// non-secret placeholder so the X-API-Key header assertions compare against a
// real value instead of "" (which would make them tautological no-ops).
// The placeholder is not a credential — it never leaves the test process and
// is never used against a real PowerDNS server.
func testAPIKey() string {
	// Prefer the environment; only fall back to a non-secret placeholder so
	// the X-API-Key header assertions compare against a real value.
	if k := os.Getenv("POWERDNS_TEST_API_KEY"); k != "" {
		return k
	}
	return "test-only-non-secret-placeholder"
}

func strPtr(s string) *string {
	return &s
}

func TestSha256DSPresentation(t *testing.T) {
	tests := []struct {
		name    string
		in      []string
		want    string
		wantErr bool
	}{
		{
			name: "selects SHA-256 (type 2) among multiple digest types",
			in: []string{
				"60776 13 4 870ecd07a97de1bad8b771699b8cd59f385437be4dec76698bcc049857cb67f68790e0a705909579570987844a0d8a61",
				"60776 13 2 3b35deed97def5fbb5ce939cd5b9036f12db0ccc2e1cb40bb4c565c168c66116",
			},
			want: "60776 13 2 3b35deed97def5fbb5ce939cd5b9036f12db0ccc2e1cb40bb4c565c168c66116",
		},
		{name: "only SHA-256 present", in: []string{"44451 13 2 bdb0d7c0"}, want: "44451 13 2 bdb0d7c0"},
		{name: "no DS entries", in: nil, wantErr: true},
		{name: "only SHA-512 present", in: []string{"44451 13 4 beef"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := sha256DSPresentation(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("expected %q, got %q", tt.want, got)
			}
		})
	}
}

func TestGetActiveDNSKEYDS(t *testing.T) {
	t.Run("returns SHA-256 DS for single active CSK", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("details") != "true" {
				t.Errorf("expected ?details=true, got %q", r.URL.RawQuery)
			}
			body := `[{"id":"5","keytype":"csk","active":true,"dnskey":"257 3 13 evH3XP==","ds":["60776 13 2 3b35deed97def5fbb5ce939cd5b9036f12db0ccc2e1cb40bb4c565c168c66116","60776 13 4 870ecd07"]}]`
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(body))
		}))
		defer server.Close()
		client, err := NewPowerDNSClient(server.URL, testAPIKey(), &core.Logger{Logger: zap.NewNop()})
		if err != nil {
			t.Fatalf("NewPowerDNSClient: %v", err)
		}
		ds, err := client.GetActiveDNSKEYDS(context.Background(), "lumeweb.")
		if err != nil {
			t.Fatalf("GetActiveDNSKEYDS: %v", err)
		}
		want := "60776 13 2 3b35deed97def5fbb5ce939cd5b9036f12db0ccc2e1cb40bb4c565c168c66116"
		if ds != want {
			t.Errorf("expected %q, got %q", want, ds)
		}
	})

	t.Run("parses numeric id as PowerDNS actually returns", func(t *testing.T) {
		// Regression: PowerDNS sends cryptokey id as a JSON number (not a
		// quoted string). A struct whose ID field was typed string failed to
		// unmarshal, breaking every cryptokey read (EnableDNSSEC, DS
		// derivation) so the DS never rendered and verify 500'd.
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body := `[{"id":5,"keytype":"csk","active":true,"dnskey":"257 3 13 evH3XP==","ds":["60776 13 2 3b35deed97def5fbb5ce939cd5b9036f12db0ccc2e1cb40bb4c565c168c66116"]}]`
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(body))
		}))
		defer server.Close()
		client, err := NewPowerDNSClient(server.URL, testAPIKey(), &core.Logger{Logger: zap.NewNop()})
		if err != nil {
			t.Fatalf("NewPowerDNSClient: %v", err)
		}
		ds, err := client.GetActiveDNSKEYDS(context.Background(), "lumeweb.")
		if err != nil {
			t.Fatalf("GetActiveDNSKEYDS with numeric id: %v", err)
		}
		want := "60776 13 2 3b35deed97def5fbb5ce939cd5b9036f12db0ccc2e1cb40bb4c565c168c66116"
		if ds != want {
			t.Errorf("expected %q, got %q", want, ds)
		}
	})

	t.Run("no active signing key returns empty", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"id":"1","keytype":"csk","active":false,"dnskey":"257 3 13 x=","ds":["60776 13 2 abc"]}]`))
		}))
		defer server.Close()
		client, err := NewPowerDNSClient(server.URL, testAPIKey(), &core.Logger{Logger: zap.NewNop()})
		if err != nil {
			t.Fatalf("NewPowerDNSClient: %v", err)
		}
		ds, err := client.GetActiveDNSKEYDS(context.Background(), "lumeweb.")
		if err != nil {
			t.Fatalf("GetActiveDNSKEYDS: %v", err)
		}
		if ds != "" {
			t.Errorf("expected empty DS, got %q", ds)
		}
	})

	t.Run("multiple active keys errors (no guess)", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body := `[{"id":"4","keytype":"csk","active":true,"dnskey":"257 3 13 a=","ds":["44451 13 2 bdb0d7c0"]},{"id":"5","keytype":"csk","active":true,"dnskey":"257 3 13 b=","ds":["60776 13 2 3b35deed"]}]`
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(body))
		}))
		defer server.Close()
		client, err := NewPowerDNSClient(server.URL, testAPIKey(), &core.Logger{Logger: zap.NewNop()})
		if err != nil {
			t.Fatalf("NewPowerDNSClient: %v", err)
		}
		if _, err := client.GetActiveDNSKEYDS(context.Background(), "lumeweb."); err == nil {
			t.Fatal("expected error with multiple active signing keys")
		}
	})
}
