package store

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
)

type (
	// ContextKey is a type for context keys
	ContextKey string
)

const (
	// VirtualReadKey is the context key for the virtual read option
	VirtualReadKey   ContextKey = "virtualRead"
	DisableMetaCheck ContextKey = "disableMetaCheck"
	ClientIPKey      ContextKey = "clientIP"
	SkipQuotaCheck   ContextKey = "skipQuotaCheck"
	UserIDKey        ContextKey = "userID"
)

// VirtualReadOption sets the virtual read option in the context
func VirtualReadOption(ctx context.Context, enabled bool) context.Context {
	ctx, span := core.TraceMethod(ctx, "VirtualReadOption")
	defer span.End()

	return context.WithValue(ctx, VirtualReadKey, enabled)
}

// isVirtualReadEnabled checks if virtual read is enabled in the context
func isVirtualReadEnabled(ctx context.Context) bool {
	ctx, span := core.TraceMethod(ctx, "isVirtualReadEnabled")
	defer span.End()

	value, ok := ctx.Value(VirtualReadKey).(bool)
	return ok && value
}

// DisableMetaCheckOption sets the disable metadata check option in the context
func DisableMetaCheckOption(ctx context.Context, enabled bool) context.Context {
	ctx, span := core.TraceMethod(ctx, "DisableMetaCheckOption")
	defer span.End()

	return context.WithValue(ctx, DisableMetaCheck, enabled)
}

// isMetaCheckDisabled checks if metadata check is disabled in the context
func isMetaCheckDisabled(ctx context.Context) bool {
	ctx, span := core.TraceMethod(ctx, "isMetaCheckDisabled")
	defer span.End()

	value, ok := ctx.Value(DisableMetaCheck).(bool)
	return ok && value
}

// ClientIPOption sets client IP in the context
func ClientIPOption(ctx context.Context, clientIP string) context.Context {
	ctx, span := core.TraceMethod(ctx, "ClientIPOption")
	defer span.End()

	return context.WithValue(ctx, ClientIPKey, clientIP)
}

// GetClientIP retrieves the client IP from the context
func GetClientIP(ctx context.Context) string {
	ctx, span := core.TraceMethod(ctx, "GetClientIP")
	defer span.End()

	value, ok := ctx.Value(ClientIPKey).(string)
	if !ok {
		return ""
	}
	return value
}

// SkipQuotaCheckOption sets the skip quota check option in the context
func SkipQuotaCheckOption(ctx context.Context, enabled bool) context.Context {
	ctx, span := core.TraceMethod(ctx, "SkipQuotaCheckOption")
	defer span.End()

	return context.WithValue(ctx, SkipQuotaCheck, enabled)
}

// IsQuotaCheckSkipped checks if quota check is skipped in the context
func IsQuotaCheckSkipped(ctx context.Context) bool {
	ctx, span := core.TraceMethod(ctx, "IsQuotaCheckSkipped")
	defer span.End()

	value, ok := ctx.Value(SkipQuotaCheck).(bool)
	return ok && value
}

func cidKey(c cid.Cid) string {
	return encoding.ToV1(c).String()
}

// UserOption sets user ID in the context
func UserOption(ctx context.Context, userID uint) context.Context {
	ctx, span := core.TraceMethod(ctx, "UserOption")
	defer span.End()

	return context.WithValue(ctx, UserIDKey, userID)
}

// GetUserID retrieves the user ID from the context
func GetUserID(ctx context.Context) uint {
	ctx, span := core.TraceMethod(ctx, "GetUserID")
	defer span.End()

	value, ok := ctx.Value(UserIDKey).(uint)
	if !ok {
		return 0
	}
	return value
}

// IsValidUserID checks if the user ID is valid (greater than 0)
func IsValidUserID(userID uint) bool {
	return userID > 0
}

// LogIfClientIPMissing logs a debug warning if client IP is not set in context
func LogIfClientIPMissing(ctx context.Context, log *core.Logger, cid fmt.Stringer) {
	if GetClientIP(ctx) == "" {
		log.Debug("Client IP not set in context for quota tracking", zap.Stringer("cid", cid))
	}
}
