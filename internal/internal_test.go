package internal

import (
	"archive/zip"
	"bytes"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-varint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDetectUploadFormat_CAR(t *testing.T) {
	// Create a simple CAR file for testing
	testCID := cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
	
	// Create a CAR buffer
	var buf bytes.Buffer
	carWriter, err := car.NewSelectiveCar(&storage.ReadableCar{},
		[]cid.Cid{testCID},
		car.WriteAsCarV1(true),
	)
	require.NoError(t, err)
	
	_, err = carWriter.WriteTo(&buf)
	require.NoError(t, err)
	
	// Test detection
	format, err := DetectUploadFormat(bytes.NewReader(buf.Bytes()))
	assert.NoError(t, err)
	assert.Equal(t, FormatCAR, format)
}

func TestDetectUploadFormat_ZIP(t *testing.T) {
	// Create a simple ZIP file for testing
	var buf bytes.Buffer
	zipWriter := zip.NewWriter(&buf)
	
	// Add a file to the ZIP
	writer, err := zipWriter.Create("test.txt")
	require.NoError(t, err)
	
	_, err = writer.Write([]byte("Hello, World!"))
	require.NoError(t, err)
	
	err = zipWriter.Close()
	require.NoError(t, err)
	
	// Test detection
	format, err := DetectUploadFormat(bytes.NewReader(buf.Bytes()))
	assert.NoError(t, err)
	assert.Equal(t, FormatZIP, format)
}

func TestDetectUploadFormat_Unknown(t *testing.T) {
	// Test with random data
	data := []byte("This is not a CAR or ZIP file")
	
	format, err := DetectUploadFormat(bytes.NewReader(data))
	assert.Error(t, err)
	assert.Equal(t, FormatUnknown, format)
	assert.Contains(t, err.Error(), "unsupported file format")
}

func TestDetectUploadFormat_Empty(t *testing.T) {
	// Test with empty data
	data := []byte{}
	
	format, err := DetectUploadFormat(bytes.NewReader(data))
	assert.Error(t, err)
	assert.Equal(t, FormatUnknown, format)
}

func TestDetectUploadFormat_ReadError(t *testing.T) {
	// Test with a reader that returns an error
	reader := &errorReader{err: strings.NewReader("test")}
	
	format, err := DetectUploadFormat(reader)
	assert.Error(t, err)
	assert.Equal(t, FormatUnknown, format)
}

func TestIsCARFormat(t *testing.T) {
	// Test with valid CAR data
	testCID := cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
	
	var buf bytes.Buffer
	carWriter, err := car.NewSelectiveCar(&storage.ReadableCar{},
		[]cid.Cid{testCID},
		car.WriteAsCarV1(true),
	)
	require.NoError(t, err)
	
	_, err = carWriter.WriteTo(&buf)
	require.NoError(t, err)
	
	assert.True(t, isCARFormat(buf.Bytes()))
}

func TestIsCARFormat_Invalid(t *testing.T) {
	// Test with invalid CAR data
	data := []byte("This is not a CAR file")
	assert.False(t, isCARFormat(data))
}

func TestIsZIPFormat(t *testing.T) {
	// Test with valid ZIP data
	var buf bytes.Buffer
	zipWriter := zip.NewWriter(&buf)
	
	writer, err := zipWriter.Create("test.txt")
	require.NoError(t, err)
	
	_, err = writer.Write([]byte("Hello, World!"))
	require.NoError(t, err)
	
	err = zipWriter.Close()
	require.NoError(t, err)
	
	assert.True(t, isZIPFormat(buf.Bytes()))
}

func TestIsZIPFormat_Invalid(t *testing.T) {
	// Test with invalid ZIP data
	data := []byte("This is not a ZIP file")
	assert.False(t, isZIPFormat(data))
}

func TestFormat_String(t *testing.T) {
	assert.Equal(t, "car", FormatCAR.String())
	assert.Equal(t, "zip", FormatZIP.String())
	assert.Equal(t, "unknown", FormatUnknown.String())
	assert.Equal(t, "unknown", Format(999).String())
}

func TestZipMode_String(t *testing.T) {
	assert.Equal(t, "convert", ZipConvert.String())
	assert.Equal(t, "preserve", ZipPreserve.String())
	assert.Equal(t, "unknown", ZipMode(999).String())
}

func TestParseZipMode(t *testing.T) {
	assert.Equal(t, ZipConvert, ParseZipMode("convert"))
	assert.Equal(t, ZipPreserve, ParseZipMode("preserve"))
	assert.Equal(t, ZipConvert, ParseZipMode("invalid")) // default
	assert.Equal(t, ZipConvert, ParseZipMode("")) // default
}

// errorReader is a helper for testing error conditions
type errorReader struct {
	err *strings.Reader
}

func (r *errorReader) Read(p []byte) (n int, error) {
	return 0, assert.AnError
}