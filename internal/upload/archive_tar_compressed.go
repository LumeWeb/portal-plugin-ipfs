package upload

import (
	"context"
	"fmt"
	"io"
	"io/fs"

	"github.com/mholt/archives"
)

// TarGzArchiveExtractor implements ArchiveExtractor for TAR.GZ files using the unified driver
type TarGzArchiveExtractor struct {
	driver *ArchivesDriver
}

// NewTarGzArchiveExtractor creates a new TAR.GZ archive extractor using the unified driver
func NewTarGzArchiveExtractor(r archives.ReaderAtSeeker) (*TarGzArchiveExtractor, error) {
	driver := NewArchivesDriver(FormatTAR_GZ, r)

	if !driver.IsFormatSupported() {
		return nil, fmt.Errorf("TAR.GZ format not supported by driver")
	}

	return &TarGzArchiveExtractor{
		driver: driver,
	}, nil
}

// Format returns the archive format this extractor handles
func (tgz *TarGzArchiveExtractor) Format() Format {
	return FormatTAR_GZ
}

// Filesystem returns a filesystem interface for the archive
func (tgz *TarGzArchiveExtractor) Filesystem(ctx context.Context) (fs.FS, error) {
	return tgz.driver.Filesystem(ctx)
}

// Close closes the extractor and releases any resources
func (tgz *TarGzArchiveExtractor) Close() error {
	// The driver doesn't need explicit closing
	// Individual file readers are closed by the caller
	return nil
}

// TarBz2ArchiveExtractor implements ArchiveExtractor for TAR.BZ2 files using the unified driver
type TarBz2ArchiveExtractor struct {
	driver *ArchivesDriver
}

// NewTarBz2ArchiveExtractor creates a new TAR.BZ2 archive extractor using the unified driver
func NewTarBz2ArchiveExtractor(r archives.ReaderAtSeeker) (*TarBz2ArchiveExtractor, error) {
	driver := NewArchivesDriver(FormatTAR_BZ2, r)

	if !driver.IsFormatSupported() {
		return nil, fmt.Errorf("TAR.BZ2 format not supported by driver")
	}

	return &TarBz2ArchiveExtractor{
		driver: driver,
	}, nil
}

// Format returns the archive format this extractor handles
func (tbz2 *TarBz2ArchiveExtractor) Format() Format {
	return FormatTAR_BZ2
}

// Filesystem returns a filesystem interface for the archive
func (tbz2 *TarBz2ArchiveExtractor) Filesystem(ctx context.Context) (fs.FS, error) {
	return tbz2.driver.Filesystem(ctx)
}

// Close closes the extractor and releases any resources
func (tbz2 *TarBz2ArchiveExtractor) Close() error {
	// The driver doesn't need explicit closing
	// Individual file readers are closed by the caller
	return nil
}

// RegisterTarGzExtractor registers the TAR.GZ extractor with the default registry
func RegisterTarGzExtractor() {
	RegisterExtractor(FormatTAR_GZ, func(reader archives.ReaderAtSeeker) (ArchiveExtractor, error) {
		return NewTarGzArchiveExtractor(reader)
	})
}

// RegisterTarBz2Extractor registers the TAR.BZ2 extractor with the default registry
func RegisterTarBz2Extractor() {
	RegisterExtractor(FormatTAR_BZ2, func(reader archives.ReaderAtSeeker) (ArchiveExtractor, error) {
		return NewTarBz2ArchiveExtractor(reader)
	})
}

// detectCompressedTar contains the core logic for detecting compressed TAR archives
func detectCompressedTar(data []byte, n int) (Format, bool) {
	if len(data) < 10 {
		return FormatUnknown, false
	}

	// Check for gzip magic number
	if data[0] == 0x1f && data[1] == 0x8b {
		// This is gzip, assume it's tar.gz for our use case
		return FormatTAR_GZ, true
	}

	// Check for bzip2 magic number
	if n >= 3 && data[0] == 'B' && data[1] == 'Z' && data[2] == 'h' {
		// This is bzip2, assume it's tar.bz2 for our use case
		return FormatTAR_BZ2, true
	}

	return FormatUnknown, false
}

// detectCompressedTarFromReader detects if compressed data contains a TAR archive
func detectCompressedTarFromReader(reader io.Reader) (Format, bool) {
	// Read a small buffer for detection
	buf := make([]byte, 10)
	n, err := io.ReadFull(reader, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return FormatUnknown, false
	}

	return detectCompressedTar(buf, n)
}

// detectCompressedTarFromBytes detects if compressed data contains a TAR archive (byte slice version)
func detectCompressedTarFromBytes(data []byte) (Format, bool) {
	return detectCompressedTar(data, len(data))
}
