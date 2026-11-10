package website

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	"go.lumeweb.com/queryutil"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestWebsiteListPreservesNestedDomainScopeAndInputs(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	require.NoError(t, err)
	conn, err := db.DB()
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, db.Exec("CREATE TABLE ipfs_websites (id INTEGER PRIMARY KEY, user_id INTEGER, status TEXT)").Error)
	require.NoError(t, db.Exec("CREATE TABLE website_domains (website_id INTEGER, domain TEXT, deleted_at DATETIME)").Error)
	require.NoError(t, db.Exec("INSERT INTO ipfs_websites VALUES (1,1,'active'),(2,1,'pending_validation'),(3,2,'active')").Error)
	require.NoError(t, db.Exec("INSERT INTO website_domains VALUES (1,'one.site',NULL),(2,'two.site',NULL),(3,'other.site',NULL)").Error)
	original := []queryutil.CrudFilter{queryutil.Or(queryutil.Equal("domain", "one.site"), queryutil.Equal("domain", "other.site"))}
	fs, err := dto.NormalizeListQuery(dto.QueryWebsites, original, nil, queryutil.Pagination{})
	require.NoError(t, err)
	for attempt := 0; attempt < 2; attempt++ {
		var ids []uint
		query := db.Table("ipfs_websites").Where("user_id = ?", 1)
		require.NoError(t, applyWebsiteFilters(query, fs).Pluck("id", &ids).Error)
		require.Equal(t, []uint{1}, ids)
	}
	require.Equal(t, "domain", original[0].GetFilters()[0].GetField())
	require.Equal(t, "one.site", original[0].GetFilters()[0].GetValue())
	not := []queryutil.CrudFilter{queryutil.Not(queryutil.Equal("domain", "one.site"))}
	fs, err = dto.NormalizeListQuery(dto.QueryWebsites, not, nil, queryutil.Pagination{})
	require.NoError(t, err)
	var ids []uint
	require.NoError(t, applyWebsiteFilters(db.Table("ipfs_websites").Where("user_id = ?", 1), fs).Pluck("id", &ids).Error)
	require.Equal(t, []uint{2}, ids)
}
