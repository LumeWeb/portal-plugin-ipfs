package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	"go.lumeweb.com/queryutil"
	"go.lumeweb.com/queryutil/filter"
)

func TestListQueryRejectsFieldsBeforeServiceDispatch(t *testing.T) {
	for _, q := range []url.Values{
		{"filters[id) OR 1=1 --][eq]": {"1"}},
		{"filters[or][0][user_id][eq]": {"2"}},
		{"_sort": {"(SELECT 1)"}},
		{"filters[size][gt]": {"not-a-number"}},
		{"filters[type][contains]": {"7"}},
	} {
		w := httptest.NewRecorder()
		called := false
		err := processListRequest(dto.QueryFiles, w, httptest.NewRequest(http.MethodGet, "/?"+q.Encode(), nil), "files",
			func([]queryutil.CrudFilter, []queryutil.Sort, queryutil.Pagination) ([]int, int64, error) {
				called = true
				return nil, 0, nil
			},
			func(v int) int { return v })
		if err == nil || w.Code != http.StatusBadRequest || called {
			t.Fatalf("status=%d called=%v err=%v", w.Code, called, err)
		}
	}
}

func TestListQueryMapsServiceValidationErrorsToBadRequest(t *testing.T) {
	w := httptest.NewRecorder()
	err := processListRequest(dto.QueryFiles, w, httptest.NewRequest(http.MethodGet, "/", nil), "files",
		func([]queryutil.CrudFilter, []queryutil.Sort, queryutil.Pagination) ([]int, int64, error) {
			return nil, 0, filter.ErrInvalidQuery
		},
		func(v int) int { return v })
	if err == nil || w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d err=%v", w.Code, err)
	}
}

func TestListQueryPreservesListResponse(t *testing.T) {
	w := httptest.NewRecorder()
	err := processListRequest(dto.QueryFiles, w, httptest.NewRequest(http.MethodGet, "/", nil), "files",
		func([]queryutil.CrudFilter, []queryutil.Sort, queryutil.Pagination) ([]int, int64, error) {
			return []int{7}, 1, nil
		},
		func(v int) int { return v })
	if err != nil || w.Code != http.StatusOK || w.Header().Get("Content-Range") != "files 0-0/1" {
		t.Fatalf("status=%d range=%q err=%v", w.Code, w.Header().Get("Content-Range"), err)
	}
	var body struct {
		Data  []int `json:"data"`
		Total int64 `json:"total"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || len(body.Data) != 1 || body.Data[0] != 7 || body.Total != 1 {
		t.Fatalf("response %q: %v", w.Body.String(), err)
	}
}
