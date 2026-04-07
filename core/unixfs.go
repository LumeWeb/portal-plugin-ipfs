package core

import "go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"

// Re-export UnixFSType from dto package for use in internal packages
type UnixFSType = dto.UnixFSType

// Re-export UnixFSType constants from dto package
const (
	UnixFSTypeDirectory = dto.UnixFSTypeDirectory
	UnixFSTypeFile      = dto.UnixFSTypeFile
	UnixFSTypeSymlink   = dto.UnixFSTypeSymlink
	UnixFSTypeHAMTShard = dto.UnixFSTypeHAMTShard
)
