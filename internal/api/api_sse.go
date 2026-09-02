package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	sseServer "github.com/apt304/sse-go/server"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	pluginEvent "go.lumeweb.com/portal-plugin-ipfs/internal/event"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
)

// SSE event types emitted on the "gateway" topic. The live feed and the
// Last-Event-ID replay path emit the same types and payload shapes so the
// gateway can treat them identically.
const (
	sseEventSitePublished = "site_published"
	sseEventSiteRemoved   = "site_removed"
	sseEventHighWaterMark = "high_water_mark"
	sseTopicGateway       = "gateway"
)

// sseState holds the SSE server and the durable event log that backs replay
// and reconciliation for website event streaming.
type sseState struct {
	server *sseServer.Server
	log    *pluginEvent.Store
}

// initSSEServer initializes the SSE server, the durable website event log, and
// the retention purger, then registers event listeners for website lifecycle
// events.
func (a *API) initSSEServer(ctx core.Context) error {
	subscriber := sseServer.NewDropOldestSubscriber(sseServer.Options{
		Buffer:            100,              // Store up to 100 events per subscriber
		HeartbeatInterval: 15 * time.Second, // Send heartbeat every 15s to keep connections alive
	})

	a.sse = &sseState{
		server: sseServer.NewServer(sseServer.Config{}, subscriber),
		log:    pluginEvent.NewStore(a.DB()),
	}

	// Register event listeners for website events
	a.registerWebsiteEventListeners(ctx)

	// Start the retention purger so the durable log never grows without bound.
	a.startEventLogPurger(ctx)

	ctx.Logger().Info("SSE server initialized for website event streaming")
	return nil
}

// startEventLogPurger periodically removes events older than the configured
// retention window, which is the documented period the gateway has to consume
// the log before it can no longer replay a missed event.
func (a *API) startEventLogPurger(ctx core.Context) {
	retention := a.eventLogRetention()
	if retention <= 0 {
		retention = 24 * time.Hour
	}

	done := ctx.GetContext().Done()
	go func() {
		ticker := time.NewTicker(retention / 2)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				before := time.Now().Add(-retention)
				removed, err := a.sse.log.PurgeBefore(context.Background(), before)
				if err != nil {
					a.Logger().Error("failed to purge expired website events",
						zap.Error(err))
					continue
				}
				if removed > 0 {
					a.Logger().Debug("purged expired website events from durable log",
						zap.Int64("removed", removed))
				}
			}
		}
	}()
}

// eventLogRetention returns the configured durable event log retention window,
// falling back to the default (24h) on any error.
func (a *API) eventLogRetention() time.Duration {
	if apiConfig, ok := a.Config().GetAPI(internal.ProtocolName).(*pluginConfig.APIConfig); ok && apiConfig.EventLogRetention > 0 {
		return apiConfig.EventLogRetention
	}
	return 24 * time.Hour
}

// registerWebsiteEventListeners registers the SSE server as a listener to
// website lifecycle events. Each event is appended to the durable log (whose
// auto-increment ID becomes the SSE event id) and then published on the
// "gateway" topic.
func (a *API) registerWebsiteEventListeners(coreCtx core.Context) {
	pluginEvent.OnWebsitePublished(coreCtx, func(ctx context.Context, ev pluginEvent.WebsitePublishedEvent) error {
		rec := pluginDb.WebsiteEvent{
			EventType: string(pluginDb.WebsiteEventPublished),
			Domain:    ev.Domain,
			CID:       ev.CID,
			WebsiteID: ev.WebsiteID,
			UserID:    ev.UserID,
			CreatedAt: ev.PublishedAt,
		}
		return a.publishWebsiteEvent(ctx, sseEventSitePublished, ev, rec)
	})

	pluginEvent.OnWebsiteRemoved(coreCtx, func(ctx context.Context, ev pluginEvent.WebsiteRemovedEvent) error {
		rec := pluginDb.WebsiteEvent{
			EventType: string(pluginDb.WebsiteEventRemoved),
			Domain:    ev.Domain,
			WebsiteID: ev.WebsiteID,
			UserID:    ev.UserID,
			CreatedAt: ev.RemovedAt,
		}
		return a.publishWebsiteEvent(ctx, sseEventSiteRemoved, ev, rec)
	})

	coreCtx.Logger().Info("SSE server registered as listener to website events")
}

// publishWebsiteEvent durably records a website event and publishes it to the
// "gateway" SSE topic using the durable event ID as the SSE id.
func (a *API) publishWebsiteEvent(ctx context.Context, eventType string, eventData any, rec pluginDb.WebsiteEvent) error {
	if a.sse == nil || a.sse.log == nil {
		return nil
	}

	// Persist first so the event survives a gateway gap and the returned
	// durable ID is available as the SSE id (making Last-Event-ID meaningful).
	// A durable-write failure must not suppress live delivery: log it, continue,
	// and fall back to a non-durable id so the gateway's authoritative
	// /internal/websites/changes reconciliation can close any gap.
	eventID, err := a.sse.log.Append(ctx, rec)
	idStr := ""
	if err != nil {
		a.Logger().Error("failed to append website event to durable log; continuing live SSE delivery",
			zap.String("event_type", eventType),
			zap.Error(err))
		idStr = uuid.New().String()
	} else {
		idStr = strconv.FormatUint(eventID, 10)
	}

	sseEvent := pluginEvent.NewSSEEvent(eventType, eventData)
	eventJSON, err := json.Marshal(sseEvent)
	if err != nil {
		a.Logger().Error("failed to marshal website event for SSE",
			zap.String("event_type", eventType),
			zap.Error(err))
		return err
	}

	event := sseServer.Event{
		ID:    idStr,
		Type:  eventType,
		Data:  eventJSON,
		Retry: 3000, // Recommended retry interval in milliseconds
	}

	if err := a.sse.server.Publish(event, sseTopicGateway); err != nil {
		a.Logger().Error("failed to publish website event to SSE",
			zap.String("topic", sseTopicGateway),
			zap.Error(err))
		return err
	}

	a.Logger().Debug("published website event to SSE",
		zap.String("event_type", eventType),
		zap.Uint64("event_id", eventID),
		zap.String("topic", sseTopicGateway))
	return nil
}

// handleWebsiteSSE establishes a Server-Sent Events connection for website
// lifecycle events. The gateway connects to this endpoint to receive real-time
// notifications when websites are published, updated, or removed.
//
// When the client supplies a Last-Event-ID (its persisted cursor), the durable
// events after that cursor are replayed first so nothing is lost across an SSE
// gap or gateway restart; a high_water_mark event signals the replay target.
// The live in-memory feed then takes over.
func (a *API) handleWebsiteSSE(c echo.Context) error {
	lastEventID := c.Request().Header.Get("Last-Event-ID")
	if lastEventID != "" && a.sse != nil && a.sse.log != nil {
		a.replayWebsiteEvents(c, lastEventID)
	}

	hooks := sseServer.LifecycleHooks{
		OnConnect: func(sub sseServer.Subscription) {
			a.Logger().Debug("gateway SSE client connected",
				zap.Strings("topics", sub.Topics))
		},
		OnDisconnect: func(sub sseServer.Subscription) {
			a.Logger().Debug("gateway SSE client disconnected",
				zap.Strings("topics", sub.Topics))
		},
	}

	// Single "gateway" topic — one gateway consumer per portal instance
	a.sse.server.ServeHTTP(c.Response(), c.Request(), []string{sseTopicGateway}, hooks)
	return nil
}

// replayWebsiteEvents writes the durable website events after lastEventID to
// the connection (mirroring the live feed's event type and data shape), then a
// high_water_mark event. The in-memory live feed takes over afterwards.
func (a *API) replayWebsiteEvents(c echo.Context, lastEventID string) {
	after, err := strconv.ParseUint(lastEventID, 10, 64)
	if err != nil {
		a.Logger().Warn("ignoring non-numeric SSE Last-Event-ID", zap.String("last_event_id", lastEventID))
		return
	}

	reqCtx := c.Request().Context()
	events, hwm, err := a.sse.log.ListSince(reqCtx, after, a.changesMaxEvents())
	if err != nil {
		a.Logger().Error("failed to load events for SSE replay",
			zap.Uint64("after", after), zap.Error(err))
		return
	}

	w := c.Response()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)

	for _, ev := range events {
		frame := a.sseReplayFrame(ev)
		if frame == nil {
			continue
		}
		if err := frame.Write(w); err != nil {
			a.Logger().Warn("SSE replay write failed", zap.Error(err))
			return
		}
	}

	// Re-read the high-water mark after draining the replay so the terminal
	// event reflects the freshest known id, shrinking the window in which an
	// event published between replay and live-subscribe would be missed. SSE
	// replay is best-effort: for authoritative catch-up the gateway compares
	// this high-water mark against its cursor and reconciles via
	// /internal/websites/changes, which is what actually closes the gap.
	if fresh, err := a.sse.log.HighWaterMark(reqCtx); err == nil {
		hwm = fresh
	}

	hwmData, _ := json.Marshal(dto.SSEHighWaterMark{HighWaterMark: hwm})
	if err := (sseServer.Event{Type: sseEventHighWaterMark, Data: hwmData}).Write(w); err != nil {
		a.Logger().Warn("SSE high-water mark write failed", zap.Error(err))
		return
	}
	w.Flush()
}

// sseReplayFrame builds an SSE frame for a durable website event that matches
// the shape the live feed emits, so the gateway can process replays and live
// events identically. Returns nil for unknown event types.
func (a *API) sseReplayFrame(ev pluginDb.WebsiteEvent) *sseServer.Event {
	var sseType string
	var payload any
	switch pluginDb.WebsiteEventType(ev.EventType) {
	case pluginDb.WebsiteEventPublished:
		sseType = sseEventSitePublished
		payload = map[string]any{
			"domain":       ev.Domain,
			"cid":          ev.CID,
			"published_at": ev.CreatedAt,
		}
	case pluginDb.WebsiteEventRemoved:
		sseType = sseEventSiteRemoved
		payload = map[string]any{
			"domain":     ev.Domain,
			"removed_at": ev.CreatedAt,
		}
	default:
		a.Logger().Warn("skipping unknown website event type during SSE replay",
			zap.String("event_type", ev.EventType))
		return nil
	}

	data, err := json.Marshal(pluginEvent.NewSSEEvent(sseType, payload))
	if err != nil {
		a.Logger().Error("failed to marshal replay event", zap.Error(err))
		return nil
	}

	return &sseServer.Event{
		ID:   strconv.FormatUint(ev.ID, 10),
		Type: sseType,
		Data: data,
	}
}
