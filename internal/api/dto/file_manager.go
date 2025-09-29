package dto

import (
	"context"
	"time"

	"github.com/Oudwins/zog"
	"github.com/ipfs/go-cid"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
)

type BlockService interface {
	GetBlockMeta(ctx context.Context, c cid.Cid) (*pluginDb.UnixFSNode, error)
}

// FileManagerFilter represents the filtering options for file listings
type FileManagerFilter struct {
	Name        string     `json:"name,omitempty" query:"name"`
	Type        *uint8     `json:"type,omitempty" query:"type"`
	IsDirectory *bool      `json:"is_directory,omitempty" query:"is_directory"`
	MinSize     *uint64    `json:"min_size,omitempty" query:"min_size"`
	MaxSize     *uint64    `json:"max_size,omitempty" query:"max_size"`
	FromDate    *time.Time `json:"from_date,omitempty" query:"from_date"`
	ToDate      *time.Time `json:"to_date,omitempty" query:"to_date"`
	PathPattern string     `json:"path_pattern,omitempty" query:"path_pattern"`
	ParentPath  string     `json:"parent_path,omitempty" query:"parent_path"`
}

func (f FileManagerFilter) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"Name":        zog.String(),
		"Type":        zog.Ptr(zog.UintLike[uint8]().Optional()),
		"IsDirectory": zog.Ptr(zog.Bool().Optional()),
		"MinSize":     zog.Ptr(zog.UintLike[uint64]().Optional()),
		"MaxSize":     zog.Ptr(zog.UintLike[uint64]().Optional()),
		"FromDate":    zog.Ptr(zog.Time().Optional()),
		"ToDate":      zog.Ptr(zog.Time().Optional()),
		"PathPattern": zog.String(),
		"ParentPath":  zog.String(),
	})
}

func (f FileManagerFilter) ToModel() (FileManagerFilter, error) {
	return f, nil
}

// FileManagerItem represents a unified file view for the file manager UI
type FileManagerItem struct {
	Path        string    `json:"path"`
	Name        string    `json:"name"`
	Type        uint8     `json:"type"`
	Size        uint64    `json:"size"`
	IsDirectory bool      `json:"is_directory"`
	Depth       int       `json:"depth"`
	Created     time.Time `json:"created"`
	Updated     time.Time `json:"updated"`
}

// FileManagerResponse represents paginated file manager results
type FileManagerResponse struct {
	Count   uint64            `json:"count"`
	Results []FileManagerItem `json:"results"`
}

func (f *FileManagerResponse) FromModel(model []*pluginDb.FilePath) error {
	f.Results = make([]FileManagerItem, len(model))

	for i, path := range model {
		item := FileManagerItem{
			Path:        path.Path,
			Name:        path.Name,
			Type:        path.Type,
			Size:        uint64(path.Size),
			IsDirectory: path.IsDirectory,
			Depth:       path.Depth,
			Created:     path.CreatedAt,
			Updated:     path.UpdatedAt,
		}

		f.Results[i] = item
	}

	return nil
}

func (f *FileManagerResponse) FromModelSingle(model *pluginDb.FilePath) error {
	f.Results = make([]FileManagerItem, 1)

	item := FileManagerItem{
		Path:        model.Path,
		Name:        model.Name,
		Type:        model.Type,
		Size:        uint64(model.Size),
		IsDirectory: model.IsDirectory,
		Depth:       model.Depth,
		Created:     model.CreatedAt,
		Updated:     model.UpdatedAt,
	}

	f.Results[0] = item

	return nil
}

// FileManagerListRequest represents the request for listing files
type FileManagerListRequest struct {
	Filters []interface{} `json:"filters,omitempty"`
	Sort    []interface{} `json:"sort,omitempty"`
	Limit   *int          `json:"limit,omitempty"`
	Offset  *int          `json:"offset,omitempty"`
}

// FileManagerDirectoryRequest represents the request for listing directory contents
type FileManagerDirectoryRequest struct {
	ParentPath string `json:"parent_path" query:"parent_path"`
	Limit      *int   `json:"limit,omitempty" query:"limit"`
	Offset     *int   `json:"offset,omitempty" query:"offset"`
}

// FileManagerBreadcrumbRequest represents the request for getting breadcrumbs
type FileManagerBreadcrumbRequest struct {
	Path string `json:"path" query:"path"`
}

func (f FileManagerListRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"Limit":  zog.Int().Optional(),
		"Offset": zog.Int().Optional(),
	})
}

func (f FileManagerDirectoryRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"ParentPath": zog.String(),
		"Limit":      zog.Ptr(zog.Int().Optional()),
		"Offset":     zog.Ptr(zog.Int().Optional()),
	})
}

func (f FileManagerBreadcrumbRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"Path": zog.String(),
	})
}

func (f FileManagerListRequest) ToModel() (FileManagerListRequest, error) {
	return f, nil
}

func (f FileManagerDirectoryRequest) ToModel() (FileManagerDirectoryRequest, error) {
	return f, nil
}

func (f FileManagerBreadcrumbRequest) ToModel() (FileManagerBreadcrumbRequest, error) {
	return f, nil
}
