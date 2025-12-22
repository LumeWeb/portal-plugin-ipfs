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
)

// TestArchiveCreator provides a unified interface for creating archives
// from different data structures (map[string]string or []TestFile)
type TestArchiveCreator struct {
	t  *testing.T
	ctx core.Context
}

// NewTestArchiveCreator creates a new unified archive creator
func NewTestArchiveCreator(t *testing.T, ctx core.Context) *TestArchiveCreator {
	return &TestArchiveCreator{
		t:  t,
		ctx: ctx,
	}
}

// CreateArchiveFromMap creates an archive from a map[string]string
func (u *TestArchiveCreator) CreateArchiveFromMap(ctx context.Context, format Format, files map[string]string) (*bytes.Buffer, error) {
	// Convert map to TestFile slice
	testFiles := u.mapToTestFiles(files)
	return u.CreateArchiveFromTestFiles(ctx, format, testFiles)
}

// CreateArchiveFromTestFiles creates an archive from a []TestFile
func (u *TestArchiveCreator) CreateArchiveFromTestFiles(ctx context.Context, format Format, files []TestFile) (*bytes.Buffer, error) {
	var buf bytes.Buffer
	fileList := u.createFileListFromTestFiles(files)

	switch format {
	case FormatZIP:
		zipFormat := archives.Zip{}
		err := zipFormat.Archive(ctx, &buf, fileList)
		return &buf, err
	case FormatTAR:
		tarFormat := archives.Tar{}
		err := tarFormat.Archive(ctx, &buf, fileList)
		return &buf, err
	case FormatTAR_GZ:
		tarGzFormat := archives.CompressedArchive{
			Compression: archives.Gz{},
			Archival:    archives.Tar{},
		}
		err := tarGzFormat.Archive(ctx, &buf, fileList)
		return &buf, err
	case FormatTAR_BZ2:
		tarBz2Format := archives.CompressedArchive{
			Compression: archives.Bz2{},
			Archival:    archives.Tar{},
		}
		err := tarBz2Format.Archive(ctx, &buf, fileList)
		return &buf, err
	case Format7Z:
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
		openFunc := func() (fs.File, error) {
			if file.IsDir {
				return &testDirFile{
					name:    strings.TrimSuffix(file.Name, "/"),
					modTime: file.Modified,
				}, nil
			}

			return &testMemFile{
				name:    file.Name,
				content: []byte(file.Content),
				modTime: file.Modified,
			}, nil
		}

		fileInfo := archives.FileInfo{
			NameInArchive: file.Name,
			FileInfo: &testFileInfo{
				name:    file.Name,
				size:    int64(len(file.Content)),
				modTime: file.Modified,
				isDir:   file.IsDir,
			},
			Open: openFunc,
		}

		fileList = append(fileList, fileInfo)
	}

	return fileList
}
