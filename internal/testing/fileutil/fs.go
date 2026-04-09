package fileutil

import (
	"fmt"
	"io"
	"io/fs"
	"time"
)

// TestMemFile implements fs.File for in-memory test files.
// This is a shared test utility for creating mock files in tests.
type TestMemFile struct {
	name    string
	content []byte
	pos     int64
	isDir   bool
	mode    fs.FileMode
}

// NewTestMemFile creates a new in-memory test file with the given name and content.
func NewTestMemFile(name string, content []byte) *TestMemFile {
	return &TestMemFile{
		name:    name,
		content: content,
		isDir:   false,
		mode:    0644,
	}
}

// NewTestMemFileWithMode creates a new in-memory test file with the given name, content, and mode.
func NewTestMemFileWithMode(name string, content []byte, mode fs.FileMode) *TestMemFile {
	return &TestMemFile{
		name:    name,
		content: content,
		isDir:   false,
		mode:    mode,
	}
}

// NewTestMemDir creates a new in-memory directory.
func NewTestMemDir(name string) *TestMemFile {
	return &TestMemFile{
		name:  name,
		isDir: true,
		mode:  fs.ModeDir | 0755,
	}
}

// NewTestMemFileInfo creates a new TestMemFileInfo with all parameters.
func NewTestMemFileInfo(name string, size int, isDir bool, mode fs.FileMode, modified time.Time) *TestMemFileInfo {
	return &TestMemFileInfo{
		name:     name,
		size:     size,
		isDir:    isDir,
		mode:     mode,
		modified: modified,
	}
}

// Read implements io.Reader.
func (f *TestMemFile) Read(p []byte) (n int, err error) {
	if f.isDir {
		return 0, fmt.Errorf("cannot read from a directory")
	}
	if f.pos >= int64(len(f.content)) {
		return 0, io.EOF
	}
	n = copy(p, f.content[f.pos:])
	f.pos += int64(n)
	return n, nil
}

// Seek implements io.Seeker.
func (f *TestMemFile) Seek(offset int64, whence int) (int64, error) {
	var newPos int64
	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = f.pos + offset
	case io.SeekEnd:
		newPos = int64(len(f.content)) + offset
	default:
		return 0, fmt.Errorf("invalid whence")
	}
	if newPos < 0 {
		return 0, fmt.Errorf("negative position")
	}
	f.pos = newPos
	return newPos, nil
}

// Close implements io.Closer.
func (f *TestMemFile) Close() error {
	return nil
}

// Stat implements fs.File.
func (f *TestMemFile) Stat() (fs.FileInfo, error) {
	return &TestMemFileInfo{
		name:  f.name,
		size:  len(f.content),
		isDir: f.isDir,
		mode:  f.mode,
	}, nil
}

// TestMemFileInfo implements fs.FileInfo for test files.
type TestMemFileInfo struct {
	name     string
	size     int
	isDir    bool
	mode     fs.FileMode
	modified time.Time
}

// Name returns the file name.
func (fi *TestMemFileInfo) Name() string {
	return fi.name
}

// Size returns the file size in bytes.
func (fi *TestMemFileInfo) Size() int64 {
	return int64(fi.size)
}

// Mode returns the file mode bits.
func (fi *TestMemFileInfo) Mode() fs.FileMode {
	return fi.mode
}

// ModTime returns the modification time.
func (fi *TestMemFileInfo) ModTime() time.Time {
	if !fi.modified.IsZero() {
		return fi.modified
	}
	return time.Now()
}

// IsDir returns whether this is a directory.
func (fi *TestMemFileInfo) IsDir() bool {
	return fi.isDir
}

// Sys returns the underlying data source (always nil for test files).
func (fi *TestMemFileInfo) Sys() any {
	return nil
}
