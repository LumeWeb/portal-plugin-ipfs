package dns

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.lumeweb.com/queryutil"
)

func TestRecordListRejectsGroupsBeforeFetchingZone(t *testing.T) {
	for _, node := range []queryutil.CrudFilter{
		queryutil.Or(queryutil.Equal("name", "one")),
		queryutil.And(queryutil.Equal("type", "TXT"), queryutil.Equal("name", "one")),
		queryutil.Not(queryutil.Equal("name", "one")),
	} {
		svc := &DNSServiceDefault{}
		require.NotPanics(t, func() {
			rows, total, err := svc.GetZoneRecords(context.Background(), 1, []queryutil.CrudFilter{node}, nil, queryutil.Pagination{})
			require.ErrorIs(t, err, queryutil.ErrInvalidQuery)
			require.Empty(t, rows)
			require.Zero(t, total)
		})
	}
}
