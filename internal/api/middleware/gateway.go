package middleware

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.uber.org/zap"
)

const GatewaySecretHeader = "X-Gateway-Secret"

// GatewayAuth creates middleware that validates the gateway shared secret header
func GatewayAuth(cfg *config.APIConfig, logger *core.Logger) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			// Get the secret from the request header
			providedSecret := c.Request().Header.Get(GatewaySecretHeader)

			// Get the configured secret
			configuredSecret := cfg.GatewaySecret

			// Validate the secret
			if providedSecret == "" || configuredSecret == "" || providedSecret != configuredSecret {
				// Log authentication failure for security auditing
				if logger != nil {
					logger.Warn("Gateway authentication failed",
						zap.String("client_ip", c.RealIP()),
						zap.String("path", c.Request().URL.Path),
					)
				}
				return c.JSON(http.StatusUnauthorized, map[string]interface{}{
					"error": "Unauthorized: invalid or missing gateway secret",
				})
			}

			// Secret is valid, proceed to next handler
			return next(c)
		}
	}
}
