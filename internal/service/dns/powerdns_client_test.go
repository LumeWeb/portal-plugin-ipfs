package dns

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/dns/powerdns"
	"go.uber.org/zap"
)

// mockHTTPClient is a mock HTTP client for testing
type mockHTTPClient struct {
	response  *http.Response
	err       error
	requests  []*http.Request
}

func (m *mockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	m.requests = append(m.requests, req)
	return m.response, m.err
}

func TestNewPowerDNSClient(t *testing.T) {
	logger := zap.NewNop()
	coreLogger := &core.Logger{Logger: logger}
	client, err := NewPowerDNSClient("http://localhost:8081", "test-api-key", coreLogger)

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
				Body:       io.NopCloser(bytes.NewBufferString(`{
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
			name:      "zone not found",
			zoneID:    "nonexistent.com.",
			mockError: errors.New("network error"),
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
			name:         "RRSet update with API error",
			zoneID:       "example.com.",
			rrsets:       []powerdns.RRSet{},
			mockError:    errors.New("network error"),
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

		// Verify request body
		var body powerdns.ZoneCreate
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("failed to decode request body: %v", err)
		}

		if body.Name != "example.com." {
			t.Errorf("expected domain 'example.com.', got '%s'", body.Name)
		}

		// Return success response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(powerdns.Zone{
			Id:   strPtr("example.com."),
			Name: strPtr("example.com."),
			Kind: (*powerdns.ZoneKind)(strPtr("Native")),
		})
	}))
	defer server.Close()

	logger := zap.NewNop()
	coreLogger := &core.Logger{Logger: logger}
	client, err := NewPowerDNSClient(server.URL, "test-api-key", coreLogger)
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
	client, err := NewPowerDNSClient(server.URL, "test-api-key", coreLogger)
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
	client, err := NewPowerDNSClient(server.URL, "test-api-key", coreLogger)
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
	client, err := NewPowerDNSClient(server.URL, "test-api-key", coreLogger)
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
			client, err := NewPowerDNSClient(server.URL, "test-api-key", coreLogger)
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
			client, err := NewPowerDNSClient(server.URL, "test-api-key", coreLogger)
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
	client, err := NewPowerDNSClient(server.URL, "test-api-key", coreLogger)
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

// Helper functions

func intPtr(i int) *int {
	return &i
}

func strPtr(s string) *string {
	return &s
}
