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
