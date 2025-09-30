package dto

import (
	"fmt"
	"strings"
	"time"

	"github.com/Oudwins/zog"
)

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

// ValidateFileManagerPath validates that a path is properly formatted for file operations
func ValidateFileManagerPath(path string) (string, error) {
	if path == "" {
		return "", fmt.Errorf("path is required")
	}

	if !strings.HasPrefix(path, "/") {
		return "", fmt.Errorf("path must start with '/'")
	}

	return path, nil
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
	CID         string    `json:"cid"`
}

// FileManagerListRequest represents the request for listing files
type FileManagerListRequest struct {
	Filters []interface{} `json:"filters,omitempty"`
	Sort    []interface{} `json:"sort,omitempty"`
	Limit   *int          `json:"limit,omitempty"`
	Offset  *int          `json:"offset,omitempty"`
}

func (f FileManagerListRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"Limit":  zog.Int().Optional(),
		"Offset": zog.Int().Optional(),
	})
}

func (f FileManagerListRequest) ToModel() (FileManagerListRequest, error) {
	return f, nil
}
