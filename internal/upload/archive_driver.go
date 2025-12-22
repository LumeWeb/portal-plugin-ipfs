package upload

import (
	"context"
	"fmt"
	"io/fs"

	"github.com/mholt/archives"
)

// ArchivesDriver provides a unified interface using mholt/archives as the backend
// This acts as a middle layer that abstracts the archives library functionality
type ArchivesDriver struct {
	format Format
	reader archives.ReaderAtSeeker
}

// NewArchivesDriver creates a new driver for the specified format
func NewArchivesDriver(format Format, reader archives.ReaderAtSeeker) *ArchivesDriver {
	return &ArchivesDriver{
		format: format,
		reader: reader,
	}
}

// addFormatAttributes adds format-specific attributes to the file entry
func (d *ArchivesDriver) addFormatAttributes(entry *ArchiveFileEntry, f archives.FileInfo) {
	switch d.format {
	case FormatZIP:
		if f.Sys() != nil {
			if header, ok := f.Sys().(interface{ Method() uint16 }); ok {
				entry.SetAttribute("method", fmt.Sprintf("%d", header.Method()))
			}
		}
	case FormatRAR:
		if f.Sys() != nil {
			if header, ok := f.Sys().(interface{ GetCRC32() uint32 }); ok {
				entry.SetAttribute("crc", fmt.Sprintf("%08x", header.GetCRC32()))
			}
		}
	}
}

// IsFormatSupported checks if the format is supported by the driver
func (d *ArchivesDriver) IsFormatSupported() bool {
	switch d.format {
	case FormatZIP, FormatTAR, FormatTAR_GZ, FormatTAR_BZ2, FormatRAR, Format7Z:
		return true
	default:
		return false
	}
}

// GetFormat returns the archive format
func (d *ArchivesDriver) GetFormat() Format {
	return d.format
}

// Filesystem returns a filesystem interface for the archive
func (d *ArchivesDriver) Filesystem(ctx context.Context) (fs.FS, error) {

	switch d.format {
	case FormatZIP, FormatTAR, FormatTAR_GZ, FormatTAR_BZ2, FormatRAR, Format7Z:
	default:
		return nil, fmt.Errorf("unsupported archive format: %s", d.format.String())
	}

	// Create filesystem from archive
	fsys, err := archives.FileSystem(ctx, "", d.reader)
	if err != nil {
		return nil, fmt.Errorf("failed to create filesystem from %s archive: %w", d.format.String(), err)
	}

	return fsys, nil
}
