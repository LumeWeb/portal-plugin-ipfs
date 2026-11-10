package api

import (
	"errors"
	"net/http"

	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	"go.lumeweb.com/queryutil"
	"go.lumeweb.com/queryutil/filter"
	queryHTTP "go.lumeweb.com/queryutil/http"
	"gorm.io/gorm"
)

func processListRequest[T any, D any](resource dto.ListQueryResource, w http.ResponseWriter, r *http.Request, entity string, list queryutil.EntityFunc[T], convert func(T) D) error {
	filters, sorts, pagination, err := dto.ParseListQuery(resource, r)
	if err != nil {
		http.Error(w, "Invalid query parameters", http.StatusBadRequest)
		return err
	}
	items, total, err := list(filters, sorts, pagination)
	if err != nil {
		switch {
		case errors.Is(err, filter.ErrInvalidQuery):
			http.Error(w, "Invalid query parameters", http.StatusBadRequest)
		case errors.Is(err, gorm.ErrRecordNotFound):
			http.Error(w, "Entity "+entity+" not found", http.StatusNotFound)
		default:
			http.Error(w, "Failed to list "+entity, http.StatusInternalServerError)
		}
		return err
	}
	responses := make([]D, len(items))
	for i, item := range items {
		responses[i] = convert(item)
	}
	queryHTTP.SetContentRangeHeader(w, entity, pagination, responses, total)
	return queryHTTP.EncodeJSON(w, queryutil.BuildResponse(responses, total))
}
