package dto

import (
	"errors"
	"net/http/httptest"
	"net/url"
	"testing"

	"go.lumeweb.com/queryutil/filter"
)

func TestListQueryReturnsErrorsForMalformedOperands(t *testing.T) {
	for _, q := range []url.Values{
		{"filters[size][gt]": {"not-a-number"}},
		{"filters[created_at][gt]": {"not-a-date"}},
		{"filters[type][contains]": {"7"}},
	} {
		t.Run(q.Encode(), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("parser panicked instead of returning invalid query: %v", r)
				}
			}()
			_, _, _, err := ParseListQuery(QueryFiles, httptest.NewRequest("GET", "/?"+q.Encode(), nil))
			if !errors.Is(err, filter.ErrInvalidQuery) {
				t.Fatalf("malformed operand: %v", err)
			}
		})
	}
}
