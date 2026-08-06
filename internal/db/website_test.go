package db

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWebsiteDomainSSLFields(t *testing.T) {
	t.Run("WebsiteDomain struct has SSL status fields", func(t *testing.T) {
		w := WebsiteDomain{}
		assert.IsType(t, "", w.SSLStatus)
		assert.IsType(t, "", w.SSLError)
		assert.IsType(t, (*time.Time)(nil), w.SSLIssuedAt)
		assert.IsType(t, (*time.Time)(nil), w.SSLLastUpdatedAt)
	})

	t.Run("SSLStatus field can be set to valid values", func(t *testing.T) {
		w := WebsiteDomain{
			SSLStatus: string(SSLStatusPending),
		}
		assert.Equal(t, string(SSLStatusPending), w.SSLStatus)

		w.SSLStatus = string(SSLStatusIssuing)
		assert.Equal(t, string(SSLStatusIssuing), w.SSLStatus)

		w.SSLStatus = string(SSLStatusReady)
		assert.Equal(t, string(SSLStatusReady), w.SSLStatus)

		w.SSLStatus = string(SSLStatusFailed)
		assert.Equal(t, string(SSLStatusFailed), w.SSLStatus)
	})

	t.Run("SSLError field can store error messages", func(t *testing.T) {
		w := WebsiteDomain{
			SSLError: "certificate validation failed",
		}
		assert.Equal(t, "certificate validation failed", w.SSLError)

		w.SSLError = ""
		assert.Equal(t, "", w.SSLError)
	})

	t.Run("SSLIssuedAt field can store timestamp", func(t *testing.T) {
		now := time.Now()
		w := WebsiteDomain{
			SSLIssuedAt: &now,
		}
		assert.NotNil(t, w.SSLIssuedAt)
		assert.Equal(t, now, *w.SSLIssuedAt)

		w.SSLIssuedAt = nil
		assert.Nil(t, w.SSLIssuedAt)
	})

	t.Run("SSLLastUpdatedAt field can store timestamp", func(t *testing.T) {
		now := time.Now()
		w := WebsiteDomain{
			SSLLastUpdatedAt: &now,
		}
		assert.NotNil(t, w.SSLLastUpdatedAt)
		assert.Equal(t, now, *w.SSLLastUpdatedAt)

		w.SSLLastUpdatedAt = nil
		assert.Nil(t, w.SSLLastUpdatedAt)
	})

	t.Run("SSL fields work together", func(t *testing.T) {
		issuedAt := time.Now().Add(-24 * time.Hour)
		updatedAt := time.Now()
		w := WebsiteDomain{
			SSLStatus:        string(SSLStatusReady),
			SSLError:         "",
			SSLIssuedAt:      &issuedAt,
			SSLLastUpdatedAt: &updatedAt,
		}
		assert.Equal(t, string(SSLStatusReady), w.SSLStatus)
		assert.Equal(t, "", w.SSLError)
		assert.Equal(t, issuedAt, *w.SSLIssuedAt)
		assert.Equal(t, updatedAt, *w.SSLLastUpdatedAt)
	})
}

func TestSSLStatus(t *testing.T) {
	t.Run("SSLStatus values match PRD specification", func(t *testing.T) {
		assert.Equal(t, "pending", string(SSLStatusPending))
		assert.Equal(t, "issuing", string(SSLStatusIssuing))
		assert.Equal(t, "ready", string(SSLStatusReady))
		assert.Equal(t, "failed", string(SSLStatusFailed))
	})

	t.Run("SSLStatus is a string type", func(t *testing.T) {
		var status SSLStatus = SSLStatusReady
		assert.IsType(t, SSLStatus(""), status)
		assert.IsType(t, "", string(status))
	})

	t.Run("validSSLStatuses contains all SSL status values", func(t *testing.T) {
		// Test that all defined SSL status constants are in the valid map
		_, ok := validSSLStatuses[SSLStatusPending]
		require.True(t, ok, "SSLStatusPending should be in validSSLStatuses")

		_, ok = validSSLStatuses[SSLStatusIssuing]
		require.True(t, ok, "SSLStatusIssuing should be in validSSLStatuses")

		_, ok = validSSLStatuses[SSLStatusReady]
		require.True(t, ok, "SSLStatusReady should be in validSSLStatuses")

		_, ok = validSSLStatuses[SSLStatusFailed]
		require.True(t, ok, "SSLStatusFailed should be in validSSLStatuses")
	})

	t.Run("validSSLStatuses only contains expected values", func(t *testing.T) {
		require.Equal(t, 4, len(validSSLStatuses), "validSSLStatuses should contain exactly 4 statuses")

		expectedStatuses := map[SSLStatus]bool{
			SSLStatusPending: true,
			SSLStatusIssuing: true,
			SSLStatusReady:   true,
			SSLStatusFailed:  true,
		}

		for status := range validSSLStatuses {
			assert.True(t, expectedStatuses[status], "validSSLStatuses should only contain defined SSL status constants")
		}
	})

	t.Run("SSLStatus can be used in map lookups", func(t *testing.T) {
		statuses := []SSLStatus{
			SSLStatusPending,
			SSLStatusIssuing,
			SSLStatusReady,
			SSLStatusFailed,
		}

		for _, status := range statuses {
			_, ok := validSSLStatuses[status]
			assert.True(t, ok, "SSLStatus %s should be valid", status)
		}
	})

	t.Run("invalid SSLStatus values are not in validSSLStatuses", func(t *testing.T) {
		invalidStatus := SSLStatus("invalid_status")
		_, ok := validSSLStatuses[invalidStatus]
		assert.False(t, ok, "invalid SSLStatus values should not be in validSSLStatuses")
	})
}

func TestWebsiteDomainBeforeSaveSSLValidation(t *testing.T) {
	t.Run("BeforeSave accepts empty SSL status", func(t *testing.T) {
		w := &WebsiteDomain{
			Domain:    "lumeweb",
			Namespace: DomainNamespaceHNS,
			SSLStatus: "",
		}

		err := w.BeforeSave(nil)
		assert.NoError(t, err)
		// Verify it was set to default pending
		assert.Equal(t, string(SSLStatusPending), w.SSLStatus)
	})

	t.Run("BeforeSave preserves an explicit non-pending SSL status", func(t *testing.T) {
		w := &WebsiteDomain{
			Domain:    "lumeweb",
			Namespace: DomainNamespaceHNS,
			SSLStatus: string(SSLStatusReady),
		}

		err := w.BeforeSave(nil)
		assert.NoError(t, err)
		assert.Equal(t, string(SSLStatusReady), w.SSLStatus)
	})
}
