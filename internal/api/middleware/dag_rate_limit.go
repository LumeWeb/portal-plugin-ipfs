package middleware

import (
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/labstack/echo/v4"
	echoMiddleware "github.com/labstack/echo/v4/middleware"
	"github.com/multiformats/go-multiaddr"
	"golang.org/x/time/rate"
)

// DefaultDAGRateLimit is the per-IP rate (req/s) for the public DAG endpoint.
const DefaultDAGRateLimit = 5

// DAGRateLimiterConfig configures the DAG rate limiter middleware.
type DAGRateLimiterConfig struct {
	// Rate is the per-IP request rate in req/s.
	Rate rate.Limit
	// Burst is the maximum burst size.
	Burst int
	// AllowNets is the set of CIDR ranges that bypass rate limiting
	// (gateway IPs, private networks, loopback).
	AllowNets []*net.IPNet
	// IPExtractor is used to determine the client IP for rate limiting.
	// If nil, c.RealIP() is used.
	IPExtractor echo.IPExtractor
}

// NewDAGRateLimiterStore creates a RateLimiterStore for the public DAG
// endpoint. Requests from IPs in AllowNets always pass.
func NewDAGRateLimiterStore(cfg DAGRateLimiterConfig) echoMiddleware.RateLimiterStore {
	if cfg.Rate <= 0 {
		cfg.Rate = rate.Limit(DefaultDAGRateLimit)
	}
	if cfg.Burst <= 0 {
		cfg.Burst = int(cfg.Rate) * 2
		if cfg.Burst < 1 {
			cfg.Burst = 1
		}
	}

	store := echoMiddleware.NewRateLimiterMemoryStoreWithConfig(
		echoMiddleware.RateLimiterMemoryStoreConfig{
			Rate:      cfg.Rate,
			Burst:     cfg.Burst,
			ExpiresIn: 3 * time.Minute,
		},
	)

	return &dagRateLimiterStore{
		inner: store,
		nets:  cfg.AllowNets,
	}
}

type dagRateLimiterStore struct {
	inner echoMiddleware.RateLimiterStore
	nets  []*net.IPNet
}

func (s *dagRateLimiterStore) Allow(identifier string) (bool, error) {
	if ip := net.ParseIP(identifier); ip != nil {
		for _, n := range s.nets {
			if n.Contains(ip) {
				return true, nil
			}
		}
	}
	return s.inner.Allow(identifier)
}

// NewDAGRateLimiterMiddleware builds the echo rate limiter middleware for the
// public DAG endpoint using the provided config. If IPExtractor is set, it is
// used for both the identifier and the skipper; otherwise c.RealIP() is used.
func NewDAGRateLimiterMiddleware(cfg DAGRateLimiterConfig) echo.MiddlewareFunc {
	store := NewDAGRateLimiterStore(cfg)

	extractIP := cfg.IPExtractor
	if extractIP == nil {
		extractIP = func(req *http.Request) string {
			// Fallback: extract direct IP if no extractor configured.
			host, _, err := net.SplitHostPort(req.RemoteAddr)
			if err != nil {
				return req.RemoteAddr
			}
			return host
		}
	}

	return echoMiddleware.RateLimiterWithConfig(echoMiddleware.RateLimiterConfig{
		Store: store,
		IdentifierExtractor: func(c echo.Context) (string, error) {
			return extractIP(c.Request()), nil
		},
		Skipper: func(c echo.Context) bool {
			ip := net.ParseIP(extractIP(c.Request()))
			if ip == nil {
				return false
			}
			for _, n := range cfg.AllowNets {
				if n.Contains(ip) {
					return true
				}
			}
			return false
		},
	})
}

// DefaultDAGAllowNets returns the default CIDR ranges that bypass DAG rate
// limiting: loopback and private networks.
func DefaultDAGAllowNets() []*net.IPNet {
	return []*net.IPNet{
		mustParseCIDR("127.0.0.0/8"),
		mustParseCIDR("::1/128"),
		mustParseCIDR("172.16.0.0/12"),
		mustParseCIDR("10.0.0.0/8"),
		mustParseCIDR("192.168.0.0/16"),
		mustParseCIDR("fc00::/7"),
	}
}

// AppendGatewayNets parses gateway multiaddrs (e.g. /ip4/10.0.0.1/tcp/4001)
// and appends their IP nets to the provided slice.
func AppendGatewayNets(nets []*net.IPNet, gateways []string) []*net.IPNet {
	for _, gw := range gateways {
		ma, err := multiaddr.NewMultiaddr(gw)
		if err != nil {
			continue
		}
		for _, c := range ma {
			if c.Protocol().Code == multiaddr.P_IP4 || c.Protocol().Code == multiaddr.P_IP6 {
				ip := net.ParseIP(c.Value())
				if ip == nil {
					continue
				}
				bits := 32
				if ip.To4() == nil {
					bits = 128
				}
				if _, ipNet, err := net.ParseCIDR(ip.String() + "/" + strconv.Itoa(bits)); err == nil {
					nets = append(nets, ipNet)
				}
			}
		}
	}
	return nets
}

func mustParseCIDR(s string) *net.IPNet {
	_, n, err := net.ParseCIDR(s)
	if err != nil {
		panic(err)
	}
	return n
}
