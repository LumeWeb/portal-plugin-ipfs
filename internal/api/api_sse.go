package api

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	sseServer "github.com/apt304/sse-go/server"
	"go.lumeweb.com/portal/core"
	pluginEvent "go.lumeweb.com/portal-plugin-ipfs/internal/event"
	"go.uber.org/zap"
)

// sseState holds the SSE server for website event streaming.
type sseState struct {
	server *sseServer.Server
}

// initSSEServer initializes the SSE server and registers event listeners
// for website lifecycle events.
func (a *API) initSSEServer(ctx core.Context) error {
	subscriber := sseServer.NewDropOldestSubscriber(sseServer.Options{
		Buffer:            100,              // Store up to 100 events per subscriber
		HeartbeatInterval: 15 * time.Second, // Send heartbeat every 15s to keep connections alive
	})

	a.sse = &sseState{
		server: sseServer.NewServer(sseServer.Config{}, subscriber),
	}

	// Register event listeners for website events
	a.registerWebsiteEventListeners(ctx)

	ctx.Logger().Info("SSE server initialized for website event streaming")

	return nil
}

// registerWebsiteEventListeners registers the SSE server as a listener to
// website lifecycle events. When website events occur, they are published
// to the "gateway" SSE topic.
func (a *API) registerWebsiteEventListeners(coreCtx core.Context) {
	// Listen for website published events (create + update with target hash change)
	pluginEvent.OnWebsitePublished(coreCtx, func(ctx context.Context, ev pluginEvent.WebsitePublishedEvent) error {
		return a.publishWebsiteEvent("site_published", ev)
	})

	// Listen for website removed events (delete)
	pluginEvent.OnWebsiteRemoved(coreCtx, func(ctx context.Context, ev pluginEvent.WebsiteRemovedEvent) error {
		return a.publishWebsiteEvent("site_removed", ev)
	})

	coreCtx.Logger().Info("SSE server registered as listener to website events")
}

// publishWebsiteEvent publishes a website event to the "gateway" SSE topic.
func (a *API) publishWebsiteEvent(eventType string, eventData any) error {
	// Create SSE event wrapper for client consumption
	sseEvent := pluginEvent.NewSSEEvent(eventType, eventData)

	eventJSON, err := json.Marshal(sseEvent)
	if err != nil {
		a.Logger().Error("failed to marshal website event for SSE",
			zap.String("event_type", eventType),
			zap.Error(err))
		return err
	}

	// Create SSE event
	topic := "gateway"
	event := sseServer.Event{
		ID:    uuid.New().String(),
		Type:  eventType,
		Data:  eventJSON,
		Retry: 3000, // Recommended retry interval in milliseconds
	}

	if err := a.sse.server.Publish(event, topic); err != nil {
		a.Logger().Error("failed to publish website event to SSE",
			zap.String("topic", topic),
			zap.String("event_type", eventType),
			zap.Error(err))
		return err
	}

	a.Logger().Debug("published website event to SSE",
		zap.String("event_type", eventType),
		zap.String("topic", topic))
	return nil
}

// handleWebsiteSSE establishes a Server-Sent Events connection for website
// lifecycle events. The gateway connects to this endpoint to receive
// real-time notifications when websites are published, updated, or removed.
func (a *API) handleWebsiteSSE(c echo.Context) error {
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
	a.sse.server.ServeHTTP(c.Response(), c.Request(), []string{"gateway"}, hooks)

	return nil
}
