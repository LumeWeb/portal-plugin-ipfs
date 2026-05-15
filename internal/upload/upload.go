package upload

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/mholt/archives"
	"go.lumeweb.com/portal/core"
	contentArchive "go.lumeweb.com/ipfs-content/archive"
)

var cidUndefSlice = []cid.Cid{cid.Undef}

// ArchiveMode represents the ZIP processing mode
type ArchiveMode int

const (
	ArchiveConvert  ArchiveMode = iota // Extract zip → IPFS structure
	ArchivePreserve                    // Keep zip intact → wrap in CAR
)

// String returns the string representation of ArchiveMode
func (m ArchiveMode) String() string {
	switch m {
	case ArchiveConvert:
		return "convert"
	case ArchivePreserve:
		return "preserve"
	default:
		return "unknown"
	}
}

// ParseArchiveMode parses a string into ArchiveMode
func ParseArchiveMode(s string) ArchiveMode {
	switch s {
	case "convert":
		return ArchiveConvert
	case "preserve":
		return ArchivePreserve
	default:
		return ArchiveConvert // default to convert
	}
}

func GetCarRoots(reader io.Reader, inspect bool) ([]cid.Cid, error) {
	readerAt, ok := reader.(io.ReaderAt)
	if !ok {
		return cidUndefSlice, fmt.Errorf("reader does not implement io.ReaderAt")
	}
	carReader, err := car.NewReader(readerAt)
	if err != nil {
		return cidUndefSlice, err
	}

	if inspect {
		_, err = carReader.Inspect(true)
		if err != nil {
			return cidUndefSlice, err
		}

	}

	roots, err := carReader.Roots()
	if err != nil {
		return cidUndefSlice, err
	}
	if len(roots) == 0 {
		return cidUndefSlice, fmt.Errorf("no roots found in CAR file")
	}

	// Reset reader position if it's seekable, so caller can read full CAR content
	if seeker, ok := reader.(io.Seeker); ok {
		_, err = seeker.Seek(0, io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("failed to reset reader position: %w", err)
		}
	}

	return roots, nil
}

// UploadProcessor handles processing of uploaded files based on format
type UploadProcessor interface {
	// Process handles the uploaded file based on its format
	Process(ctx context.Context, reader io.ReadSeekCloser) (cid.Cid, string, error)
}

// UploadProcessorFactory creates processors based on format and mode
type UploadProcessorFactory interface {
	// CreateProcessor returns a processor suitable for the specified format
	CreateProcessor(format contentArchive.Format, mode ArchiveMode, portalCtx core.Context, userID uint) (UploadProcessor, error)
}

// UniversalReader wraps an io.Reader to make it seekable by buffering its content.
// If the underlying reader already implements io.ReadSeekCloser, it is used directly
// without buffering — this avoids copying the entire file into memory for readers
// that are already seekable (e.g., TUSUploadReader, *os.File).
type UniversalReader struct {
	reader io.Reader
	buf    *bytes.Reader
	pos    int64
	closed bool
}

var _ archives.ReaderAtSeeker = (*UniversalReader)(nil)
var _ io.ReaderFrom = (*UniversalReader)(nil)

// NewUniversalReader creates a new UniversalReader instance.
// If the reader already implements io.ReadSeekCloser, it is wrapped directly
// without buffering. Otherwise, the content is buffered on first access.
func NewUniversalReader(reader io.Reader) *UniversalReader {
	return &UniversalReader{reader: reader}
}

// NewSeekableReader returns an io.ReadSeekCloser from the given reader.
// If the reader already implements io.ReadSeekCloser, it is returned directly.
// Otherwise, it is wrapped in a UniversalReader for seekability.
// Use this when you need a seekable reader but want to avoid unnecessary buffering.
func NewSeekableReader(reader io.Reader) io.ReadSeekCloser {
	if rsc, ok := reader.(io.ReadSeekCloser); ok {
		return rsc
	}
	return NewUniversalReader(reader)
}

// ensureBuffer ensures that all data from the original reader is loaded into the buffer.
// If the underlying reader is already seekable, it is used directly without buffering.
func (s *UniversalReader) ensureBuffer() error {
	if s.buf != nil {
		return nil
	}

	// If the underlying reader is already a ReadSeekCloser, wrap it directly
	// without buffering the entire content into memory.
	if rsc, ok := s.reader.(io.ReadSeekCloser); ok {
		s.buf = bytes.NewReader(nil) // sentinel: non-nil means initialized
		s.reader = rsc
		return nil
	}

	// For non-seekable readers, buffer the entire content.
	// Pre-allocate if we can determine the size via seeking.
	size := -1
	if seeker, ok := s.reader.(io.Seeker); ok {
		end, err := seeker.Seek(0, io.SeekEnd)
		if err == nil {
			size = int(end)
			_, _ = seeker.Seek(0, io.SeekStart)
		}
	}

	var data bytes.Buffer
	if size > 0 {
		data.Grow(size)
	}
	if _, err := io.Copy(&data, s.reader); err != nil {
		return err
	}
	s.buf = bytes.NewReader(data.Bytes())
	return nil
}

// isPassthrough returns true if the underlying reader is seekable and we're
// delegating reads/seeks to it directly instead of using the buffer.
func (s *UniversalReader) isPassthrough() bool {
	_, ok := s.reader.(io.ReadSeekCloser)
	return ok && s.buf != nil
}

// Read implements io.Reader
func (s *UniversalReader) Read(p []byte) (n int, err error) {
	if s.closed {
		return 0, io.ErrClosedPipe
	}

	if err = s.ensureBuffer(); err != nil {
		return 0, err
	}

	// Passthrough to the underlying seekable reader
	if s.isPassthrough() {
		rsc := s.reader.(io.ReadSeekCloser)
		return rsc.Read(p)
	}

	n, err = s.buf.ReadAt(p, s.pos)
	s.pos += int64(n)
	return n, err
}

// Seek implements io.Seeker
func (s *UniversalReader) Seek(offset int64, whence int) (int64, error) {
	if s.closed {
		return 0, io.ErrClosedPipe
	}

	if err := s.ensureBuffer(); err != nil {
		return 0, err
	}

	// Passthrough to the underlying seekable reader
	if s.isPassthrough() {
		rsc := s.reader.(io.ReadSeekCloser)
		return rsc.Seek(offset, whence)
	}

	var newPos int64
	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = s.pos + offset
	case io.SeekEnd:
		newPos = int64(s.buf.Len()) + offset
	default:
		return 0, fmt.Errorf("invalid whence value: %d", whence)
	}

	if newPos < 0 {
		return 0, fmt.Errorf("negative position")
	}

	s.pos = newPos
	return newPos, nil
}

// ReadAt implements io.ReaderAt
func (s *UniversalReader) ReadAt(p []byte, off int64) (n int, err error) {
	if s.closed {
		return 0, io.ErrClosedPipe
	}

	if err := s.ensureBuffer(); err != nil {
		return 0, err
	}

	// Passthrough: seek to offset, then read
	if s.isPassthrough() {
		rsc := s.reader.(io.ReadSeekCloser)
		if _, err := rsc.Seek(off, io.SeekStart); err != nil {
			return 0, err
		}
		return rsc.Read(p)
	}

	return s.buf.ReadAt(p, off)
}

// Close implements io.Closer
func (s *UniversalReader) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true

	// If the underlying reader is also a Closer, close it
	if closer, ok := s.reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// ReadFrom implements io.ReaderFrom
// Reads data from the given reader and appends it to the UniversalReader's buffer
func (s *UniversalReader) ReadFrom(r io.Reader) (int64, error) {
	if s.closed {
		return 0, io.ErrClosedPipe
	}

	if err := s.ensureBuffer(); err != nil {
		return 0, err
	}

	// Passthrough readers don't support appending
	if s.isPassthrough() {
		return 0, fmt.Errorf("ReadFrom not supported for passthrough readers")
	}

	// Read all data from the new reader
	var newData bytes.Buffer
	n, err := io.Copy(&newData, r)
	if err != nil {
		return n, err
	}

	// Combine existing data with new data
	existingData := make([]byte, s.buf.Len())
	s.buf.ReadAt(existingData, 0)

	combinedData := append(existingData, newData.Bytes()...)
	s.buf = bytes.NewReader(combinedData)

	// Reset position to beginning if we were at the end
	if s.pos >= int64(len(existingData)) {
		s.pos = int64(len(existingData))
	}

	return n, nil
}
