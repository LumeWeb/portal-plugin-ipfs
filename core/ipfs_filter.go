package core

import (
	"context"
	"fmt"
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
		crudFilters = append(crudFilters, filter.FieldIn("cid", lo.Map(p.filter.CIDs, func(item string, _ int) any {
			return any(item)
		})...))
	}

	if p.filter.Name != "" {
		op := queryutil.OpEq
		switch p.filter.Match {
		case dto.TextMatchingStrategyIExact, dto.TextMatchingStrategyExact:
			op = queryutil.OpEq
		case dto.TextMatchingStrategyIPartial:
			op = queryutil.OpContains
		case dto.TextMatchingStrategyPartial:
			op = queryutil.OpContainss
		}

		crudFilters = append(crudFilters, filter.NewLogicalFilter("name", op, p.filter.Name))
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
	return []filter.Sort{}, nil
}

func (p *IPFSPinParser) ParsePagination() (filter.Pagination, error) {
	return filter.NewPagination(0, p.filter.Limit)
}

// ParseIPFSPinFilter converts an IPFSPinFilter DTO to filter, sort, and pagination AST structs.
func ParseIPFSPinFilter(ctx context.Context, input dto.IPFSPinFilter) ([]filter.CrudFilter, []filter.Sort, filter.Pagination, error) { //Removed sortConfig
	parser := NewIPFSPinParser(ctx, input)

	crudFilters, err := parser.ParseFilters()
	if err != nil {
		return nil, nil, filter.Pagination{}, err
	}

	pagination, err := parser.ParsePagination()
	if err != nil {
		return nil, nil, filter.Pagination{}, err
	}

	return crudFilters, []filter.Sort{}, pagination, nil
}
