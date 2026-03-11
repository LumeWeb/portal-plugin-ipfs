package upload

import (
	"fmt"
	"io"
	"sync"

	"github.com/h2non/filetype"
	"github.com/mholt/archives"
	"github.com/samber/lo"
	"go.lumeweb.com/portal-plugin-ipfs/internal/upload/common"
)

const (
	MinDetectionBytes = 20 // Minimum bytes needed for reliable format detection
	TAR_GZIP_MIME     = "application/gzip"
	TAR_BZIP2_MIME    = "application/x-bzip2"
)

var (
	// Map MIME types to archive formats
	mimeToFormat = map[string]Format{
		"application/zip":              FormatZIP,
		"application/x-tar":            FormatTAR,
		"application/vnd.rar":          FormatRAR,
		"application/x-rar-compressed": FormatRAR,
		"application/x-7z-compressed":  Format7Z,
	}
)

// Global registry instance
var defaultRegistry *ArchiveRegistry
var initOnce sync.Once

// MaybeInit ensures the default registry is initialized if nil
func MaybeInit() {
	initOnce.Do(func() {
		defaultRegistry = NewArchiveRegistry()
		RegisterDefaultDetectors(defaultRegistry)
	})
}

// DefaultRegistry returns the default global archive registry
func DefaultRegistry() *ArchiveRegistry {
	MaybeInit()
	return defaultRegistry
}

// RegisterExtractor registers an extractor with the default registry
func RegisterExtractor(format Format, creator ExtractorCreator) {
	MaybeInit()
	defaultRegistry.RegisterExtractor(format, creator)
}

// DetectFormat detects format using the default registry
func DetectFormat(reader io.Reader) (Format, error) {
	MaybeInit()
	return defaultRegistry.DetectFormat(reader)
}

// CreateExtractor creates an extractor using the default registry
func CreateExtractor(reader archives.ReaderAtSeeker) (ArchiveExtractor, error) {
	MaybeInit()
	return defaultRegistry.CreateExtractor(reader)
}

// NewArchiveExtractor creates an archive extractor for a specific format
func NewArchiveExtractor(reader archives.ReaderAtSeeker, format Format) (ArchiveExtractor, error) {
	MaybeInit()
	return defaultRegistry.CreateExtractorForFormat(format, reader)
}

// SupportedFormats returns supported formats from the default registry
func SupportedFormats() []Format {
	MaybeInit()
	return defaultRegistry.SupportedFormats()
}

func init() {
	MaybeInit()
}

// ArchiveRegistry manages registration and creation of archive extractors
// It acts as both a registry and factory, combining the responsibilities
// of format detection and extractor creation
type ArchiveRegistry struct {
	mu         sync.RWMutex
	extractors map[Format]ExtractorCreator
	detectors  []FormatDetector
}

// ExtractorCreator defines how to create an extractor for a specific format
type ExtractorCreator func(reader archives.ReaderAtSeeker) (ArchiveExtractor, error)

// FormatDetector defines a function to detect archive format from data
type FormatDetector func(reader io.Reader) (Format, bool)

// NewArchiveRegistry creates a new empty archive registry
func NewArchiveRegistry() *ArchiveRegistry {
	return &ArchiveRegistry{
		extractors: make(map[Format]ExtractorCreator),
		detectors:  make([]FormatDetector, 0),
	}
}

// RegisterDefaultDetectors registers the default format detectors to a registry
func RegisterDefaultDetectors(registry *ArchiveRegistry) {
	// Register CAR detector
	registry.RegisterDetector(detectCAR)

	// Register compressed TAR detector
	registry.RegisterDetector(detectCompressedTarFromReader)
}

// RegisterExtractor registers a creator function for a specific archive format
func (r *ArchiveRegistry) RegisterExtractor(format Format, creator ExtractorCreator) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.extractors[format] = creator
}

// RegisterDetector adds a new format detector to the registry
func (r *ArchiveRegistry) RegisterDetector(detector FormatDetector) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.detectors = append(r.detectors, detector)
}

// DetectFormat detects the archive format from the reader
// It uses filetype library as primary detection and custom detectors for edge cases
func (r *ArchiveRegistry) DetectFormat(reader io.Reader) (Format, error) {
	r.mu.RLock()
	detectors := make([]FormatDetector, len(r.detectors))
	copy(detectors, r.detectors)
	extractors := make(map[Format]ExtractorCreator)
	for k, v := range r.extractors {
		extractors[k] = v
	}
	r.mu.RUnlock()

	seeker, ok := reader.(io.ReadSeeker)
	if !ok {
		return FormatUnknown, fmt.Errorf("format detection requires a reader that supports seeking")
	}

	// Early validation: if no detectors and no extractors, we can't determine anything
	if len(detectors) == 0 && len(extractors) == 0 {
		return FormatUnknown, fmt.Errorf("no detectors or extractors registered for format detection")
	}

	// Prepare reader for format detection while preserving current position
	currentPos, err := common.PrepareReaderPreservePos(seeker)
	if err != nil {
		return FormatUnknown, err
	}

	// Read buffer for format detection
	buf := make([]byte, 1024)
	n, err := io.ReadFull(reader, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		// Restore original position on error
		if restoreErr := common.RestoreReaderPos(seeker, currentPos); restoreErr != nil {
			return FormatUnknown, fmt.Errorf("failed to read header for format detection: %w (and failed to restore position: %v)", err, restoreErr)
		}
		return FormatUnknown, fmt.Errorf("failed to read header for format detection: %w", err)
	}

	var detectedFormat Format

	if n > 0 {
		// Use filetype library for primary detection
		filetypeType, err := filetype.Match(buf[:n])
		if err == nil {
			// Require minimum data length for reliable detection
			if n < MinDetectionBytes {
				// Too short for reliable detection, keep as unknown for now
				detectedFormat = FormatUnknown
			} else {
				if format, exists := mimeToFormat[filetypeType.MIME.Value]; exists {
					detectedFormat = format
				} else if filetypeType.MIME.Value == TAR_GZIP_MIME || filetypeType.MIME.Value == TAR_BZIP2_MIME {
					// Handle compressed formats that need subdetection
					if format, detected := detectCompressedTarFromBytes(buf[:n]); detected {
						detectedFormat = format
					} else {
						detectedFormat = FormatFile
					}
				} else {
					detectedFormat = FormatFile
				}
			}
		}
		// If filetype.Match fails, we keep detectedFormat as FormatUnknown
		// and will try custom detectors next
	}

	// Try custom detectors for any non-archive formats or unknown formats
	// Custom detectors should run unless we've already detected a known archive format
	if detectedFormat == FormatUnknown || detectedFormat == FormatFile {
		// Reset reader position for custom detectors
		_, err = seeker.Seek(0, io.SeekStart)
		if err != nil {
			// Restore original position on error
			if seekErr := common.RestoreReaderPos(seeker, currentPos); seekErr != nil {
				return FormatUnknown, fmt.Errorf("failed to seek to beginning for custom detection: %w (and failed to restore position: %v)", err, seekErr)
			}
			return FormatUnknown, fmt.Errorf("failed to seek to beginning for custom detection: %w", err)
		}

		for _, detector := range detectors {
			if format, detected := detector(seeker); detected {
				// Only override if we detected an archive format (not FormatUnknown or FormatFile)
				if format != FormatUnknown && format != FormatFile {
					detectedFormat = format
					break
				}
			}
		}
	}

	// Restore original position
	if err := common.RestoreReaderPos(seeker, currentPos); err != nil {
		return FormatUnknown, fmt.Errorf("failed to restore original reader position: %w", err)
	}

	// After all detection attempts, if still unknown and we have data to analyze,
	// then it's a regular file (not an archive)
	if detectedFormat == FormatUnknown {
		if n > 0 {
			// We have data but couldn't detect any archive format,
			// so it's a regular file
			detectedFormat = FormatFile
		} else {
			// No data available to make determination
			return FormatUnknown, fmt.Errorf("no data available for format detection")
		}
	}

	// Check if there's an extractor registered for the detected format
	// FormatFile is a special case - regular files don't need extractors
	if detectedFormat != FormatFile && detectedFormat != FormatCAR {
		_, hasExtractor := extractors[detectedFormat]
		if !hasExtractor {
			return FormatUnknown, fmt.Errorf("no extractor registered for detected format %s", detectedFormat.String())
		}
	}

	return detectedFormat, nil
}

// CreateExtractor creates an extractor for the detected format
func (r *ArchiveRegistry) CreateExtractor(reader archives.ReaderAtSeeker) (ArchiveExtractor, error) {
	// Check if reader supports seeking
	seeker, ok := reader.(io.Seeker)
	if !ok {
		return nil, fmt.Errorf("archive reader must support seeking for format detection")
	}

	// Prepare reader for format detection while preserving current position
	currentPos, err := common.PrepareReaderPreservePos(seeker)
	if err != nil {
		return nil, err
	}

	// Detect format
	detectedFormat, err := r.DetectFormat(reader)
	if err != nil {
		// Restore original position on error
		if seekErr := common.RestoreReaderPos(seeker, currentPos); seekErr != nil {
			return nil, fmt.Errorf("format detection failed: %w (and failed to restore position: %v)", err, seekErr)
		}
		return nil, err
	}

	// Seek back to beginning for extractor creation
	_, err = seeker.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to beginning for extractor creation: %w", err)
	}

	return r.CreateExtractorForFormat(detectedFormat, reader)
}

// CreateExtractorForFormat creates an extractor for a specific format
func (r *ArchiveRegistry) CreateExtractorForFormat(format Format, reader archives.ReaderAtSeeker) (ArchiveExtractor, error) {
	r.mu.RLock()
	creator, exists := r.extractors[format]
	r.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no extractor registered for format: %s", format.String())
	}

	return creator(reader)
}

// SupportedFormats returns a list of all registered archive formats
func (r *ArchiveRegistry) SupportedFormats() []Format {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return lo.Keys(r.extractors)
}

// IsFormatSupported checks if a format is supported
func (r *ArchiveRegistry) IsFormatSupported(format Format) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, exists := r.extractors[format]
	return exists
}
