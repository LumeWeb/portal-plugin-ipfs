package upload

// Format represents a unified file format type that covers both upload and archive formats
type Format int

const (
	// Basic formats supported for upload
	FormatUnknown Format = iota
	FormatCAR     Format = iota
	FormatZIP     Format = iota
	FormatFile    Format = iota

	// Extended archive formats
	FormatRAR     Format = iota
	FormatTAR     Format = iota
	FormatTAR_GZ  Format = iota
	FormatTAR_BZ2 Format = iota
	Format7Z      Format = iota
)

// IsUploadFormat returns true if this format is supported for direct upload
func (f Format) IsUploadFormat() bool {
	return f == FormatCAR
}

// IsArchiveFormat returns true if this format is an archive type
func (f Format) IsArchiveFormat() bool {
	// FormatFile is a single file, not an archive container
	// Archives are containers that can be extracted (ZIP, TAR, RAR, 7Z)
	// FormatCAR is a CAR file that doesn't need extraction
	return f != FormatUnknown && f != FormatFile && f != FormatCAR
}

// String returns the string representation of Format
func (f Format) String() string {
	switch f {
	case FormatCAR:
		return "car"
	case FormatFile:
		return "file"
	case FormatZIP:
		return "zip"
	case FormatRAR:
		return "rar"
	case FormatTAR:
		return "tar"
	case FormatTAR_GZ:
		return "tar.gz"
	case FormatTAR_BZ2:
		return "tar.bz2"
	case Format7Z:
		return "7z"
	default:
		return "unknown"
	}
}

// ParseFormat parses a string into Format
func ParseFormat(s string) Format {
	switch s {
	case "car":
		return FormatCAR
	case "file":
		return FormatFile
	case "zip":
		return FormatZIP
	case "rar":
		return FormatRAR
	case "tar":
		return FormatTAR
	case "tar.gz":
		return FormatTAR_GZ
	case "tar.bz2":
		return FormatTAR_BZ2
	case "7z":
		return Format7Z
	default:
		return FormatUnknown
	}
}
