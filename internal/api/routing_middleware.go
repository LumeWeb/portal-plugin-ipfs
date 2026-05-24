package api

import (
	"net/http"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/labstack/echo/v4"
	"github.com/libp2p/go-libp2p/core/peer"
)

// normalizeIPNSNameMiddleware converts a raw peer ID (e.g. 12D3KooW...)
// in the :name path param to its CIDv1 form (e.g. bafzaajaiaejc...)
// before the request reaches boxo's routing server. Boxo's GetIPNS
// calls cid.Decode() which rejects raw peer IDs; this makes the
// endpoint accept both, consistent with boxo's parsePeerID on /peers/.
func normalizeIPNSNameMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		nameStr := c.Param("name")
		if nameStr == "" {
			return next(c)
		}

		if _, err := cid.Decode(nameStr); err != nil {
			pid, pidErr := peer.Decode(nameStr)
			if pidErr == nil {
				cidForm := peer.ToCid(pid)
				c.SetParamValues(cidForm.String())
				rewriteRequestPath(c.Request(), nameStr, cidForm.String())
			}
		}

		return next(c)
	}
}

func rewriteRequestPath(r *http.Request, old, new string) {
	r.URL.Path = strings.Replace(r.URL.Path, old, new, 1)
	r.URL.RawPath = strings.Replace(r.URL.RawPath, old, new, 1)
}
