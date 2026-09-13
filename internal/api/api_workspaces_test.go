package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestToUintPtr covers the Create-path normalization of the wire WebsiteID: nil
// OR <= 0 must resolve to nil (unattached / omit semantics) so an explicitly
// constructed request with WebsiteID: ptr(0) can never reach required-ID
// validation or become an attached ID 0; only a positive ID is attached.
func TestToUintPtr(t *testing.T) {
	t.Run("nil means unattached", func(t *testing.T) {
		assert.Nil(t, toUintPtr(nil))
	})

	t.Run("zero means unattached", func(t *testing.T) {
		assert.Nil(t, toUintPtr(new(int(0))))
	})

	t.Run("negative means unattached", func(t *testing.T) {
		assert.Nil(t, toUintPtr(new(int(-1))))
	})

	t.Run("positive means attached", func(t *testing.T) {
		got := toUintPtr(new(int(42)))
		require.NotNil(t, got)
		assert.Equal(t, uint(42), *got)
	})
}

// TestAttachedWebsiteID covers the attach-path conversion of the wire
// WebsiteID. Unlike Create (which normalizes nil/<=0 to "unattached"), attach
// REQUIRES a positive website to link, so nil and non-positive values must be
// rejected (ok=false) before conversion: a negative int must never wrap to a
// huge uint, and zero must not become an attached ID 0. Only a positive ID
// passes through unchanged.
func TestAttachedWebsiteID(t *testing.T) {
	t.Run("nil is rejected", func(t *testing.T) {
		id, ok := attachedWebsiteID(nil)
		assert.False(t, ok)
		assert.Equal(t, uint(0), id)
	})

	t.Run("zero is rejected", func(t *testing.T) {
		id, ok := attachedWebsiteID(new(int(0)))
		assert.False(t, ok)
		assert.Equal(t, uint(0), id)
	})

	t.Run("negative is rejected (no wrap)", func(t *testing.T) {
		id, ok := attachedWebsiteID(new(int(-1)))
		assert.False(t, ok)
		assert.Equal(t, uint(0), id)
	})

	t.Run("positive is accepted unchanged", func(t *testing.T) {
		id, ok := attachedWebsiteID(new(int(42)))
		require.True(t, ok)
		assert.Equal(t, uint(42), id)
	})
}
