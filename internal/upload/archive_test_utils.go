package upload

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"strings"
	"testing"
	"time"

	"github.com/mholt/archives"
	"go.lumeweb.com/portal/core"
	contentArchive "go.lumeweb.com/ipfs-content/archive"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/fileutil"
)

// testMemFileInfo implements fs.FileInfo for archive test files with custom modes
type testMemFileInfo struct {
	name     string
	size     int
	isDir    bool
	mode     fs.FileMode
	modified time.Time
}

func (fi *testMemFileInfo) Name() string     { return fi.name }
func (fi *testMemFileInfo) Size() int64      { return int64(fi.size) }
func (fi *testMemFileInfo) Mode() fs.FileMode { return fi.mode }
func (fi *testMemFileInfo) ModTime() time.Time {
	if !fi.modified.IsZero() {
		return fi.modified
	}
	return time.Now()
}
func (fi *testMemFileInfo) IsDir() bool      { return fi.isDir }
func (fi *testMemFileInfo) Sys() any        { return nil }

// TestArchiveCreator provides a unified interface for creating archives
// from different data structures (map[string]string or []TestFile)
type TestArchiveCreator struct {
	t   *testing.T
	ctx core.Context
}

// NewTestArchiveCreator creates a new unified archive creator
func NewTestArchiveCreator(t *testing.T, ctx core.Context) *TestArchiveCreator {
	return &TestArchiveCreator{
		t:   t,
		ctx: ctx,
	}
}

// CreateArchiveFromMap creates an archive from a map[string]string
func (u *TestArchiveCreator) CreateArchiveFromMap(ctx context.Context, format contentArchive.Format, files map[string]string) (*bytes.Buffer, error) {
	ctx, span := core.TraceMethod(ctx, "TestArchiveCreator.CreateArchiveFromMap")
	defer span.End()

	// Convert map to TestFile slice
	testFiles := u.mapToTestFiles(files)
	return u.CreateArchiveFromTestFiles(ctx, format, testFiles)
}

// CreateArchiveFromTestFiles creates an archive from a []TestFile
func (u *TestArchiveCreator) CreateArchiveFromTestFiles(ctx context.Context, format contentArchive.Format, files []TestFile) (*bytes.Buffer, error) {
	ctx, span := core.TraceMethod(ctx, "TestArchiveCreator.CreateArchiveFromTestFiles")
	defer span.End()

	var buf bytes.Buffer
	fileList := u.createFileListFromTestFiles(files)

	switch format {
	case contentArchive.FormatZIP:
		zipFormat := archives.Zip{}
		err := zipFormat.Archive(ctx, &buf, fileList)
		return &buf, err
	case contentArchive.FormatTAR:
		tarFormat := archives.Tar{}
		err := tarFormat.Archive(ctx, &buf, fileList)
		return &buf, err
	case contentArchive.FormatTAR_GZ:
		tarGzFormat := archives.CompressedArchive{
			Compression: archives.Gz{},
			Archival:    archives.Tar{},
		}
		err := tarGzFormat.Archive(ctx, &buf, fileList)
		return &buf, err
	case contentArchive.FormatTAR_BZ2:
		tarBz2Format := archives.CompressedArchive{
			Compression: archives.Bz2{},
			Archival:    archives.Tar{},
		}
		err := tarBz2Format.Archive(ctx, &buf, fileList)
		return &buf, err
	case contentArchive.Format7Z:
		// For 7Z format, use the external tool approach from archive_test_common.go
		archiveData := Create7ZArchive(u.t, u.ctx, files)
		return bytes.NewBuffer(archiveData), nil
	default:
		return nil, fmt.Errorf("unsupported archive format: %v", format)
	}
}

// mapToTestFiles converts map[string]string to []TestFile
func (u *TestArchiveCreator) mapToTestFiles(files map[string]string) []TestFile {
	testFiles := make([]TestFile, 0, len(files))
	now := time.Now()

	for path, content := range files {
		isDir := strings.HasSuffix(path, "/")

		testFile := TestFile{
			Name:     path,
			Content:  content,
			IsDir:    isDir,
			Mode:     0644,
			Modified: now,
		}
		if isDir {
			testFile.Mode = 0755
		}

		testFiles = append(testFiles, testFile)
	}

	return testFiles
}

// createFileListFromTestFiles converts TestFile slice to archives.FileInfo slice
func (u *TestArchiveCreator) createFileListFromTestFiles(files []TestFile) []archives.FileInfo {
	fileList := make([]archives.FileInfo, 0, len(files))

	for _, file := range files {
		file := file // Create new variable to avoid closure capture issue
		var content []byte
		if !file.IsDir {
			content = []byte(file.Content)
		}

		openFunc := func() (fs.File, error) {
			if file.IsDir {
				return fileutil.NewTestMemDir(file.Name), nil
			}

			return fileutil.NewTestMemFile(file.Name, content), nil
		}

		// For directories, use a simple mode to avoid archive entry issues
		// For files, use the full mode from the test data
		var memFileInfo *fileutil.TestMemFileInfo
		if file.IsDir {
			memFileInfo = fileutil.NewTestMemFileInfo(file.Name, len(content), file.IsDir, fs.ModeDir|0755, file.Modified)
		} else {
			memFileInfo = fileutil.NewTestMemFileInfo(file.Name, len(content), file.IsDir, fs.FileMode(file.Mode), file.Modified)
		}

		fileInfo := archives.FileInfo{
			NameInArchive: file.Name,
			FileInfo:      memFileInfo,
			Open:          openFunc,
		}

		fileList = append(fileList, fileInfo)
	}

	return fileList
}
