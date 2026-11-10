package api

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	dnsService "go.lumeweb.com/portal-plugin-ipfs/internal/service/dns"
	domainService "go.lumeweb.com/portal-plugin-ipfs/internal/service/domain"
	fileService "go.lumeweb.com/portal-plugin-ipfs/internal/service/file_manager"
	keyService "go.lumeweb.com/portal-plugin-ipfs/internal/service/ipns_key"
	pinService "go.lumeweb.com/portal-plugin-ipfs/internal/service/pin"
	websiteService "go.lumeweb.com/portal-plugin-ipfs/internal/service/website"
	workspaceService "go.lumeweb.com/portal-plugin-ipfs/internal/service/workspace"
	"go.lumeweb.com/queryutil"
	"go.lumeweb.com/queryutil/filter"
)

func TestListingServicesRejectQueriesBeforeDatabaseAccess(t *testing.T) {
	ctx := context.Background()
	for _, bad := range []struct {
		name    string
		filters []queryutil.CrudFilter
		sorts   []queryutil.Sort
		page    queryutil.Pagination
	}{
		{name: "field", filters: []queryutil.CrudFilter{queryutil.Equal("id) OR 1=1 --", 1)}},
		{name: "nested_owner", filters: []queryutil.CrudFilter{queryutil.Or(queryutil.Equal("user_id", 2))}},
		{name: "custom_direction", sorts: []queryutil.Sort{{Field: "id", Order: filter.SortOrder("asc; SELECT 1")}}},
		{name: "negative_limit", page: queryutil.Pagination{PageSize: -1}},
		{name: "nil_node", filters: []queryutil.CrudFilter{nil}},
	} {
		t.Run(bad.name, func(t *testing.T) {
			fs, sorts, p := bad.filters, bad.sorts, bad.page
			for _, test := range []struct {
				name string
				call func() error
			}{
				{"pins", func() error { _, _, err := (&pinService.PinServiceDefault{}).ListPins(ctx, fs, sorts, p); return err }},
				{"user_pins", func() error {
					_, _, err := (&pinService.PinServiceDefault{}).ListPinsForUser(ctx, 1, fs, sorts, p)
					return err
				}},
				{"keys", func() error {
					_, _, err := (&keyService.IPNSKeyServiceDefault{}).ListKeysWithFilters(ctx, 1, fs, sorts, p)
					return err
				}},
				{"files", func() error {
					_, _, err := (&fileService.FileManagerServiceDefault{}).ListFiles(ctx, 1, fs, sorts, p)
					return err
				}},
				{"websites", func() error {
					_, _, err := (&websiteService.WebsiteServiceDefault{}).ListWebsites(ctx, 1, fs, sorts, p)
					return err
				}},
				{"workspaces", func() error {
					_, _, err := (&workspaceService.WorkspaceService{}).List(ctx, 1, fs, sorts, p)
					return err
				}},
				{"zones", func() error { _, _, err := (&dnsService.DNSServiceDefault{}).ListZones(ctx, fs, sorts, p); return err }},
				{"user_zones", func() error {
					_, _, err := (&dnsService.DNSServiceDefault{}).ListZonesForUser(ctx, 1, fs, sorts, p)
					return err
				}},
				{"records", func() error {
					_, _, err := (&dnsService.DNSServiceDefault{}).GetZoneRecords(ctx, 1, fs, sorts, p)
					return err
				}},
				{"platform_domains", func() error {
					_, _, err := (&domainService.DelegatedDomainService{}).ListPlatformDomains(ctx, fs, sorts, p)
					return err
				}},
			} {
				t.Run(test.name, func(t *testing.T) {
					require.NotPanics(t, func() { require.ErrorIs(t, test.call(), filter.ErrInvalidQuery) })
				})
			}
		})
	}
}
