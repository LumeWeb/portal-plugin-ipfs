package api

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
	"go.lumeweb.com/httputil"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.uber.org/zap"
)

// getWebsiteChanges implements the gateway change-reconciliation endpoint.
//
// The gateway calls this after an SSE reconnect with its last fully processed
// event ID (the `after` cursor) to discover any published/removed domains it
// missed during the gap — including brand-new domains that never reached it —
// without needing visitor traffic. Events are returned in ascending durable-ID
// order together with the current high-water mark so the gateway knows when
// catch-up is complete.
func (a *API) getWebsiteChanges(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	var after uint64
	if raw := c.QueryParam("after"); raw != "" {
		v, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return ctx.Error(errors.New("after must be a non-negative integer cursor"), http.StatusBadRequest)
		}
		after = v
	}

	if a.sse == nil || a.sse.log == nil {
		return ctx.Error(errors.New("website event log unavailable"), http.StatusInternalServerError)
	}

	limit := a.changesMaxEvents()
	events, hwm, err := a.sse.log.ListSince(reqCtx, after, limit)
	if err != nil {
		a.Logger().Error("failed to list website changes from event log",
			zap.Uint64("after", after), zap.Error(err))
		return ctx.Error(err, http.StatusInternalServerError)
	}

	resp := &dto.WebsiteChangesResponse{
		Events:        make([]dto.WebsiteChangeEvent, 0, len(events)),
		HighWaterMark: hwm,
		Truncated:     len(events) >= limit && after+uint64(len(events)) < hwm,
	}
	for _, ev := range events {
		resp.Events = append(resp.Events, dto.WebsiteChangeEvent{
			ID:        ev.ID,
			EventType: ev.EventType,
			Domain:    ev.Domain,
			CID:       ev.CID,
			WebsiteID: ev.WebsiteID,
			UserID:    ev.UserID,
			CreatedAt: ev.CreatedAt,
		})
	}

	return c.JSON(http.StatusOK, resp)
}

// changesMaxEvents returns the configured per-request cap for the change
// reconciliation endpoint, falling back to the default (1000) on any error.
func (a *API) changesMaxEvents() int {
	if apiConfig, ok := a.Config().GetAPI(internal.ProtocolName).(*pluginConfig.APIConfig); ok && apiConfig.ChangesMaxEvents > 0 {
		return apiConfig.ChangesMaxEvents
	}
	return 1000
}
