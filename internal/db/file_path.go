package db

import (
	"fmt"
	"gorm.io/gorm"
)

const RootPath = "/"

// Validation constants for FilePath fields
const (
	MaxPathLength        = 1000
	MaxNameLength        = 255
	MaxParentPathLength  = 1000
)

type FilePath struct {
	gorm.Model
	UserID      uint   `gorm:"column:user_id;not null"`
	CID         []byte `gorm:"column:cid;not null"`
	Path        string `gorm:"column:path;not null"`
	Name        string `gorm:"column:name;not null"`
	Type        uint8  `gorm:"column:type;not null"`
	Size        int64  `gorm:"column:size"`
	IsDirectory bool   `gorm:"column:is_directory;default:false"`
	IsOrphan    bool   `gorm:"column:is_orphan;default:false"`
	ParentPath  string `gorm:"column:parent_path"`
	Depth       int    `gorm:"column:depth;default:0"`
}

func (FilePath) TableName() string {
	return "ipfs_file_paths"
}

// BeforeCreate validates the FilePath fields before creation
func (fp *FilePath) BeforeCreate(tx *gorm.DB) error {
	return fp.validateFields()
}

// BeforeUpdate validates the FilePath fields before update
func (fp *FilePath) BeforeUpdate(tx *gorm.DB) error {
	return fp.validateFields()
}

// validateFields validates the FilePath fields against database constraints
func (fp *FilePath) validateFields() error {
	if len(fp.Path) > MaxPathLength {
		return fmt.Errorf("path length exceeds maximum allowed length of %d characters", MaxPathLength)
	}
	
	if len(fp.Name) > MaxNameLength {
		return fmt.Errorf("name length exceeds maximum allowed length of %d characters", MaxNameLength)
	}
	
	if len(fp.ParentPath) > MaxParentPathLength {
		return fmt.Errorf("parent_path length exceeds maximum allowed length of %d characters", MaxParentPathLength)
	}
	
	return nil
}
