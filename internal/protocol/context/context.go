package context

import (
	"context"

	"go.lumeweb.com/portal/core"
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
)

// VirtualReadOption sets the virtual read option in the context
func VirtualReadOption(ctx context.Context, enabled bool) context.Context {
	ctx, span := core.TraceMethod(ctx, "VirtualReadOption")
	defer span.End()

	return context.WithValue(ctx, VirtualReadKey, enabled)
}

// IsVirtualReadEnabled checks if virtual read is enabled in the context
func IsVirtualReadEnabled(ctx context.Context) bool {
	ctx, span := core.TraceMethod(ctx, "IsVirtualReadEnabled")
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

// IsMetaCheckDisabled checks if metadata check is disabled in the context
func IsMetaCheckDisabled(ctx context.Context) bool {
	ctx, span := core.TraceMethod(ctx, "IsMetaCheckDisabled")
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
