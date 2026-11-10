package website

import (
	"go.lumeweb.com/queryutil"
	"go.lumeweb.com/queryutil/filter"
	"gorm.io/gorm"
)

// applyWebsiteFilters translates the virtual domain field without flattening
// boolean groups or changing the caller's tree across transaction retries.
func applyWebsiteFilters(query *gorm.DB, filters []queryutil.CrudFilter) *gorm.DB {
	for _, node := range filters {
		query = query.Where(websiteFilterCondition(query.Session(&gorm.Session{NewDB: true}), node))
	}
	return query
}

func websiteFilterCondition(query *gorm.DB, node queryutil.CrudFilter) *gorm.DB {
	switch f := node.(type) {
	case *filter.LogicalFilter:
		if f.Field() == "domain" {
			return query.Where("EXISTS (SELECT 1 FROM website_domains wd WHERE wd.website_id = ipfs_websites.id AND wd.domain = ? AND wd.deleted_at IS NULL)", f.Value())
		}
		return queryutil.ApplyFilters(query, []queryutil.CrudFilter{f}, nil)
	case *filter.ConditionalFilter:
		group := query.Session(&gorm.Session{NewDB: true})
		for _, child := range f.Filters {
			condition := websiteFilterCondition(query.Session(&gorm.Session{NewDB: true}), child)
			if f.Operator == filter.LogicalOr {
				group = group.Or(condition)
			} else {
				group = group.Where(condition)
			}
		}
		if f.Operator == filter.LogicalNot {
			return query.Not(group)
		}
		return group
	}
	return query
}
