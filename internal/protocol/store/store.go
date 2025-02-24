package store

import (
	"context"
	"github.com/ipfs/go-cid"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
)

type (
	// ContextKey is a type for context keys
	ContextKey string
)

const (
	// VirtualReadKey is the context key for the virtual read option
	VirtualReadKey   ContextKey = "virtualRead"
	DisableMetaCheck ContextKey = "disableMetaCheck"
)

// VirtualReadOption sets the virtual read option in the context
func VirtualReadOption(ctx context.Context, enabled bool) context.Context {
	return context.WithValue(ctx, VirtualReadKey, enabled)
}

// isVirtualReadEnabled checks if virtual read is enabled in the context
func isVirtualReadEnabled(ctx context.Context) bool {
	value, ok := ctx.Value(VirtualReadKey).(bool)
	return ok && value
}

// DisableMetaCheckOption sets the disable metadata check option in the context
func DisableMetaCheckOption(ctx context.Context, enabled bool) context.Context {
	return context.WithValue(ctx, DisableMetaCheck, enabled)
}

// isMetaCheckDisabled checks if metadata check is disabled in the context
func isMetaCheckDisabled(ctx context.Context) bool {
	value, ok := ctx.Value(DisableMetaCheck).(bool)
	return ok && value
}

func cidKey(c cid.Cid) string {
	return encoding.ToV1(c).String()
}
