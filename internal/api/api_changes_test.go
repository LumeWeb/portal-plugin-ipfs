package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

func TestAPI_GetWebsiteChanges(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := newMockHelper(t, ctx)

		// Seed the durable event log directly.
		ev1 := pluginDb.WebsiteEvent{EventType: string(pluginDb.WebsiteEventPublished), Domain: "alpha.example", CID: "bafy1"}
		ev2 := pluginDb.WebsiteEvent{EventType: string(pluginDb.WebsiteEventRemoved), Domain: "beta.example"}
		ev3 := pluginDb.WebsiteEvent{EventType: string(pluginDb.WebsiteEventPublished), Domain: "gamma.example", CID: "bafy2"}
		require.NoError(t, ctx.DB().Create(&ev1).Error)
		require.NoError(t, ctx.DB().Create(&ev2).Error)
		require.NoError(t, ctx.DB().Create(&ev3).Error)

		t.Run("returns_all_changes_and_high_water_mark", func(t *testing.T) {
			rec := helper.makeGatewayAuthenticatedRequest(http.MethodGet, "/internal/websites/changes", testGatewaySecret(), nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp dto.WebsiteChangesResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, ev3.ID, resp.HighWaterMark)
			require.Len(t, resp.Events, 3)
			assert.Equal(t, ev1.ID, resp.Events[0].ID)
			assert.Equal(t, string(pluginDb.WebsiteEventPublished), resp.Events[0].EventType)
			assert.Equal(t, "alpha.example", resp.Events[0].Domain)
			assert.Equal(t, "bafy1", resp.Events[0].CID)
			assert.Equal(t, ev2.ID, resp.Events[1].ID)
			assert.Equal(t, ev3.ID, resp.Events[2].ID)
			assert.False(t, resp.Truncated)
		})

		t.Run("resumes_after_cursor", func(t *testing.T) {
			rec := helper.makeGatewayAuthenticatedRequest(http.MethodGet, "/internal/websites/changes?after="+strconv.FormatUint(ev1.ID, 10), testGatewaySecret(), nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp dto.WebsiteChangesResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.Len(t, resp.Events, 2)
			assert.Equal(t, ev2.ID, resp.Events[0].ID)
			assert.Equal(t, ev3.ID, resp.Events[1].ID)
			assert.Equal(t, ev3.ID, resp.HighWaterMark)
		})

		t.Run("cursor_at_high_water_mark_returns_empty", func(t *testing.T) {
			rec := helper.makeGatewayAuthenticatedRequest(http.MethodGet, "/internal/websites/changes?after="+strconv.FormatUint(ev3.ID, 10), testGatewaySecret(), nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp dto.WebsiteChangesResponse
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Empty(t, resp.Events)
			assert.Equal(t, ev3.ID, resp.HighWaterMark)
		})

		t.Run("invalid_cursor_rejected", func(t *testing.T) {
			rec := helper.makeGatewayAuthenticatedRequest(http.MethodGet, "/internal/websites/changes?after=not-a-number", testGatewaySecret(), nil)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}, TestOptions)
}
