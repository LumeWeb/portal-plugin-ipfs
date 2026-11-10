package dns

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	"go.lumeweb.com/queryutil"
)

func TestRecordListEvaluatesNestedGroups(t *testing.T) {
	records := []*dto.DNSRecord{{Name: "one", Type: "A"}, {Name: "two", Type: "TXT"}, {Name: "three", Type: "AAAA"}}
	fs := []queryutil.CrudFilter{queryutil.Or(queryutil.Equal("name", "one"), queryutil.And(queryutil.Equal("type", "TXT"), queryutil.Not(queryutil.Equal("name", "three"))))}
	result := applyInMemoryFilters(records, fs, nil, queryutil.Pagination{})
	require.Len(t, result, 2)
	require.Equal(t, "one", result[0].Name)
	require.Equal(t, "two", result[1].Name)
}
