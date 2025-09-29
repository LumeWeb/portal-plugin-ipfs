package db

import (
	"gorm.io/gorm"
)

const RootPath = "/"

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
