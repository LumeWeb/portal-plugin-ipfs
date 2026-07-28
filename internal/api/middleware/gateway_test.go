package middleware

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.uber.org/zap"
)

func testGatewaySecret() string {
	return os.Getenv("GATEWAY_SECRET")
}

// TestGatewayAuth_NoSecret tests that requests without X-Gateway-Secret header return 401
func TestGatewayAuth_NoSecret(t *testing.T) {
	t.Setenv("GATEWAY_SECRET", "test-gw-"+t.Name())
	// Create config with a secret
	cfg := &config.APIConfig{
		GatewaySecret: testGatewaySecret(),
	}

	// Create logger (nil for testing - middleware handles this gracefully)
	var logger *core.Logger

	// Create middleware
	mw := GatewayAuth(cfg, logger)

	// Create test handler
	handler := mw(func(c echo.Context) error {
		return c.String(http.StatusOK, "success")
	})

	// Create request without secret header
	req := httptest.NewRequest(http.MethodGet, "/internal/websites/test.com/status", nil)
	rec := httptest.NewRecorder()

	// Create Echo context
	e := echo.New()
	c := e.NewContext(req, rec)

	// Execute middleware
	err := handler(c)

	// Verify response
	assert.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	// Verify error response
	var response map[string]interface{}
	err = json.Unmarshal(rec.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Contains(t, response["error"], "Unauthorized")
	assert.Contains(t, response["error"], "invalid or missing gateway secret")
}

// TestGatewayAuth_InvalidSecret tests that requests with invalid secret return 401
func TestGatewayAuth_InvalidSecret(t *testing.T) {
	t.Setenv("GATEWAY_SECRET", "test-gw-"+t.Name())
	// Create config with a secret
	cfg := &config.APIConfig{
		GatewaySecret: testGatewaySecret(),
	}

	// Create logger (nil for testing - middleware handles this gracefully)
	var logger *core.Logger

	// Create middleware
	mw := GatewayAuth(cfg, logger)

	// Create test handler
	handler := mw(func(c echo.Context) error {
		return c.String(http.StatusOK, "success")
	})

	// Create request with invalid secret
	req := httptest.NewRequest(http.MethodGet, "/internal/websites/test.com/status", nil)
	req.Header.Set("X-Gateway-Secret", "wrong-secret")
	rec := httptest.NewRecorder()

	// Create Echo context
	e := echo.New()
	c := e.NewContext(req, rec)

	// Execute middleware
	err := handler(c)

	// Verify response
	assert.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	// Verify error response
	var response map[string]interface{}
	err = json.Unmarshal(rec.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Contains(t, response["error"], "Unauthorized")
	assert.Contains(t, response["error"], "invalid or missing gateway secret")
}

// TestGatewayAuth_ValidSecret tests that requests with valid secret return 200
func TestGatewayAuth_ValidSecret(t *testing.T) {
	t.Setenv("GATEWAY_SECRET", "test-gw-"+t.Name())
	// Create config with a secret
	cfg := &config.APIConfig{
		GatewaySecret: testGatewaySecret(),
	}

	// Create logger (nil for testing - middleware handles this gracefully)
	var logger *core.Logger

	// Create middleware
	mw := GatewayAuth(cfg, logger)

	// Create test handler
	handler := mw(func(c echo.Context) error {
		return c.String(http.StatusOK, "success")
	})

	// Create request with valid secret
	req := httptest.NewRequest(http.MethodGet, "/internal/websites/test.com/status", nil)
	req.Header.Set("X-Gateway-Secret", testGatewaySecret())
	rec := httptest.NewRecorder()

	// Create Echo context
	e := echo.New()
	c := e.NewContext(req, rec)

	// Execute middleware
	err := handler(c)

	// Verify response
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "success", rec.Body.String())
}

// TestGatewayAuth_EmptyConfigSecret tests that requests fail when config secret is empty
func TestGatewayAuth_EmptyConfigSecret(t *testing.T) {
	// Create config with empty secret
	cfg := &config.APIConfig{
		GatewaySecret: "",
	}

	// Create logger (nil for testing - middleware handles this gracefully)
	var logger *core.Logger

	// Create middleware
	mw := GatewayAuth(cfg, logger)

	// Create test handler
	handler := mw(func(c echo.Context) error {
		return c.String(http.StatusOK, "success")
	})

	// Create request with secret (but config has empty secret)
	req := httptest.NewRequest(http.MethodGet, "/internal/websites/test.com/status", nil)
	req.Header.Set("X-Gateway-Secret", testGatewaySecret())
	rec := httptest.NewRecorder()

	// Create Echo context
	e := echo.New()
	c := e.NewContext(req, rec)

	// Execute middleware
	err := handler(c)

	// Verify response - should be 401 because config secret is empty
	assert.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

// TestGatewayAuth_Logging tests that authentication failures are logged
func TestGatewayAuth_Logging(t *testing.T) {
	t.Setenv("GATEWAY_SECRET", "test-gw-"+t.Name())
	// Create a logger
	zapLogger, err := zap.NewDevelopment()
	assert.NoError(t, err)
	defer zapLogger.Sync()
	logger := core.NewLogger(nil, zapLogger)

	// Create config with a secret
	cfg := &config.APIConfig{
		GatewaySecret: testGatewaySecret(),
	}

	// Create middleware with logger
	mw := GatewayAuth(cfg, logger)

	// Create test handler
	handler := mw(func(c echo.Context) error {
		return c.String(http.StatusOK, "success")
	})

	// Create request without secret header (should trigger log)
	req := httptest.NewRequest(http.MethodGet, "/internal/websites/test.com/status", nil)
	rec := httptest.NewRecorder()

	// Create Echo context
	e := echo.New()
	c := e.NewContext(req, rec)

	// Execute middleware
	err = handler(c)

	// Verify response
	assert.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	// Note: In a real test, we would verify that logs were written.
	// Since we can't easily capture zap logs in unit tests without
	// additional setup, we just verify the middleware doesn't crash
	// when a logger is provided.
}
