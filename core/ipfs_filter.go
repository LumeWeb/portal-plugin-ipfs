package core

import (
	"context"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/samber/lo"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	"go.lumeweb.com/queryutil"
	"go.lumeweb.com/queryutil/filter"
)

type IPFSPinParser struct {
	ctx    context.Context
	filter dto.IPFSPinFilter
}

func NewIPFSPinParser(ctx context.Context, filter dto.IPFSPinFilter) *IPFSPinParser {
	return &IPFSPinParser{ctx: ctx, filter: filter}
}

func (p *IPFSPinParser) ParseFilters() ([]filter.CrudFilter, error) {
	var crudFilters []filter.CrudFilter

	if len(p.filter.CIDs) > 0 {
		// First validate all CIDs can be parsed
		for _, cidStr := range p.filter.CIDs {
			if _, err := cid.Parse(cidStr); err != nil {
				return nil, fmt.Errorf("failed to parse CID %s: %w", cidStr, err)
			}
		}

		// Parse CID strings to CID objects and convert to bytes for database comparison
		cidBytes := lo.Map(p.filter.CIDs, func(cidStr string, _ int) any {
			cidObj, _ := cid.Parse(cidStr)
			return cidObj.Bytes()
		})
		crudFilters = append(crudFilters, filter.FieldIn("cid", cidBytes...))
	}

	if p.filter.Name != "" {
		switch p.filter.Match {
		case dto.TextMatchingStrategyExact:
			crudFilters = append(crudFilters, filter.StringField("name").Eq(p.filter.Name))
		case dto.TextMatchingStrategyIExact:
			// For exact matching, we need both starts with and ends with the same value
			startFilter := filter.NewLogicalFilter("name", queryutil.OpStartswith, p.filter.Name)
			endFilter := filter.NewLogicalFilter("name", queryutil.OpEndswith, p.filter.Name)
			crudFilters = append(crudFilters, filter.AndF(startFilter, endFilter))
		case dto.TextMatchingStrategyIPartial:
			crudFilters = append(crudFilters, filter.StringField("name").Contains(p.filter.Name))
		case dto.TextMatchingStrategyPartial:
			crudFilters = append(crudFilters, filter.NewLogicalFilter("name", queryutil.OpContainss, p.filter.Name))
		}
	}

	if len(p.filter.Status) > 0 {
		var statusStrings []string
		for _, status := range p.filter.Status {
			statusStrings = append(statusStrings, string(status))
		}
		crudFilters = append(crudFilters, filter.FieldIn("status", lo.Map(statusStrings, func(item string, _ int) any {
			return any(item)
		})...))
	}

	if p.filter.Before != nil {
		crudFilters = append(crudFilters, filter.FieldLt("created_at", *p.filter.Before))
	}

	if p.filter.After != nil {
		crudFilters = append(crudFilters, filter.FieldGt("created_at", *p.filter.After))
	}

	if len(p.filter.Meta) > 0 {
		for key, value := range p.filter.Meta {
			crudFilters = append(crudFilters, filter.FieldEqual(fmt.Sprintf("meta.%s", key), value))
		}
	}

	return crudFilters, nil
}

func (p *IPFSPinParser) ParseSorts(_ *filter.SortConfig) ([]filter.Sort, error) {
	return []filter.Sort{
		{
			Field: "created_at",
			Order: filter.OrderDesc,
		},
	}, nil
}

func (p *IPFSPinParser) ParsePagination() (filter.Pagination, error) {
	return filter.NewPagination(0, p.filter.Limit)
}
