package upload

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

// mockReadSeekCloser implements io.ReadSeekCloser for testing passthrough behavior
type mockReadSeekCloser struct {
	reader *bytes.Reader
	closed bool
}

func newMockReadSeekCloser(data []byte) *mockReadSeekCloser {
	return &mockReadSeekCloser{
		reader: bytes.NewReader(data),
	}
}

func (m *mockReadSeekCloser) Read(p []byte) (n int, err error) {
	if m.closed {
		return 0, io.ErrClosedPipe
	}
	return m.reader.Read(p)
}

func (m *mockReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	if m.closed {
		return 0, io.ErrClosedPipe
	}
	return m.reader.Seek(offset, whence)
}

func (m *mockReadSeekCloser) Close() error {
	m.closed = true
	return nil
}

// --- NewSeekableReader tests ---

func TestNewSeekableReader_AlreadySeekable(t *testing.T) {
	data := []byte("hello world")
	rsc := newMockReadSeekCloser(data)

	result := NewSeekableReader(rsc)

	// Should return the same reader, not a UniversalReader wrapper
	if result != rsc {
		t.Error("NewSeekableReader should return the same ReadSeekCloser when reader already implements it")
	}
}

func TestNewSeekableReader_NotSeekable(t *testing.T) {
	data := "hello world"
	reader := strings.NewReader(data)

	result := NewSeekableReader(reader)

	// Should return a UniversalReader wrapper
	if _, ok := result.(*UniversalReader); !ok {
		t.Error("NewSeekableReader should return a UniversalReader when reader is not seekable")
	}
}

// --- UniversalReader passthrough tests (seekable underlying reader) ---

func TestUniversalReader_Passthrough_Read(t *testing.T) {
	data := []byte("hello world")
	rsc := newMockReadSeekCloser(data)
	ur := NewUniversalReader(rsc)

	buf := make([]byte, 5)
	n, err := ur.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(buf[:n]) != "hello" {
		t.Errorf("Read got %q, want %q", string(buf[:n]), "hello")
	}
}

func TestUniversalReader_Passthrough_Seek(t *testing.T) {
	data := []byte("hello world")
	rsc := newMockReadSeekCloser(data)
	ur := NewUniversalReader(rsc)

	// Seek to offset 6
	pos, err := ur.Seek(6, io.SeekStart)
	if err != nil {
		t.Fatalf("Seek failed: %v", err)
	}
	if pos != 6 {
		t.Errorf("Seek position = %d, want 6", pos)
	}

	// Read from the new position
	buf := make([]byte, 5)
	n, err := ur.Read(buf)
	if err != nil {
		t.Fatalf("Read after seek failed: %v", err)
	}
	if string(buf[:n]) != "world" {
		t.Errorf("Read after seek got %q, want %q", string(buf[:n]), "world")
	}
}

func TestUniversalReader_Passthrough_ReadAt(t *testing.T) {
	data := []byte("hello world")
	rsc := newMockReadSeekCloser(data)
	ur := NewUniversalReader(rsc)

	buf := make([]byte, 5)
	n, err := ur.ReadAt(buf, 6)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if string(buf[:n]) != "world" {
		t.Errorf("ReadAt got %q, want %q", string(buf[:n]), "world")
	}
}

func TestUniversalReader_Passthrough_Close(t *testing.T) {
	data := []byte("hello world")
	rsc := newMockReadSeekCloser(data)
	ur := NewUniversalReader(rsc)

	if err := ur.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if !rsc.closed {
		t.Error("Close should close the underlying reader")
	}

	// Second close should not error
	if err := ur.Close(); err != nil {
		t.Fatalf("Second Close failed: %v", err)
	}
}

func TestUniversalReader_Passthrough_SeekFromEnd(t *testing.T) {
	data := []byte("hello world")
	rsc := newMockReadSeekCloser(data)
	ur := NewUniversalReader(rsc)

	pos, err := ur.Seek(-5, io.SeekEnd)
	if err != nil {
		t.Fatalf("Seek from end failed: %v", err)
	}
	if pos != 6 {
		t.Errorf("Seek from end position = %d, want 6", pos)
	}
}

// --- UniversalReader buffer tests (non-seekable underlying reader) ---

func TestUniversalReader_Buffered_Read(t *testing.T) {
	data := "hello world"
	ur := NewUniversalReader(strings.NewReader(data))

	buf := make([]byte, 5)
	n, err := ur.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(buf[:n]) != "hello" {
		t.Errorf("Read got %q, want %q", string(buf[:n]), "hello")
	}
}

func TestUniversalReader_Buffered_SeekAndRead(t *testing.T) {
	data := "hello world"
	ur := NewUniversalReader(strings.NewReader(data))

	// Seek to offset 6
	pos, err := ur.Seek(6, io.SeekStart)
	if err != nil {
		t.Fatalf("Seek failed: %v", err)
	}
	if pos != 6 {
		t.Errorf("Seek position = %d, want 6", pos)
	}

	// Read from the new position
	buf := make([]byte, 5)
	n, err := ur.Read(buf)
	if err != nil {
		t.Fatalf("Read after seek failed: %v", err)
	}
	if string(buf[:n]) != "world" {
		t.Errorf("Read after seek got %q, want %q", string(buf[:n]), "world")
	}
}

func TestUniversalReader_Buffered_ReadAt(t *testing.T) {
	data := "hello world"
	ur := NewUniversalReader(strings.NewReader(data))

	buf := make([]byte, 5)
	n, err := ur.ReadAt(buf, 6)
	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}
	if string(buf[:n]) != "world" {
		t.Errorf("ReadAt got %q, want %q", string(buf[:n]), "world")
	}
}

func TestUniversalReader_Buffered_Close(t *testing.T) {
	data := "hello world"
	ur := NewUniversalReader(strings.NewReader(data))

	if err := ur.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Read after close should error
	buf := make([]byte, 5)
	_, err := ur.Read(buf)
	if err == nil {
		t.Error("Read after close should return an error")
	}
}

func TestUniversalReader_Buffered_SeekFromEnd(t *testing.T) {
	data := "hello world"
	ur := NewUniversalReader(strings.NewReader(data))

	pos, err := ur.Seek(-5, io.SeekEnd)
	if err != nil {
		t.Fatalf("Seek from end failed: %v", err)
	}
	if pos != 6 {
		t.Errorf("Seek from end position = %d, want 6", pos)
	}
}

// --- Large data tests ---

func TestUniversalReader_Passthrough_LargeData(t *testing.T) {
	// Create 1MB of data
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	rsc := newMockReadSeekCloser(data)
	ur := NewUniversalReader(rsc)

	// Read first 100 bytes
	buf := make([]byte, 100)
	n, err := ur.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if n != 100 {
		t.Errorf("Read returned %d bytes, want 100", n)
	}

	// Seek to middle
	pos, err := ur.Seek(512*1024, io.SeekStart)
	if err != nil {
		t.Fatalf("Seek failed: %v", err)
	}
	if pos != 512*1024 {
		t.Errorf("Seek position = %d, want %d", pos, 512*1024)
	}

	// Read from middle
	n, err = ur.Read(buf)
	if err != nil {
		t.Fatalf("Read after seek failed: %v", err)
	}
	if n != 100 {
		t.Errorf("Read after seek returned %d bytes, want 100", n)
	}
}

func TestUniversalReader_Buffered_LargeData(t *testing.T) {
	// Create 1MB of data using a non-seekable reader
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	ur := NewUniversalReader(bytes.NewReader(data))

	// Read first 100 bytes
	buf := make([]byte, 100)
	n, err := ur.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if n != 100 {
		t.Errorf("Read returned %d bytes, want 100", n)
	}

	// Seek to middle
	pos, err := ur.Seek(512*1024, io.SeekStart)
	if err != nil {
		t.Fatalf("Seek failed: %v", err)
	}
	if pos != 512*1024 {
		t.Errorf("Seek position = %d, want %d", pos, 512*1024)
	}

	// Read from middle
	n, err = ur.Read(buf)
	if err != nil {
		t.Fatalf("Read after seek failed: %v", err)
	}
	if n != 100 {
		t.Errorf("Read after seek returned %d bytes, want 100", n)
	}
}

// --- ReadFrom tests ---

func TestUniversalReader_Passthrough_ReadFrom_Error(t *testing.T) {
	data := []byte("hello")
	rsc := newMockReadSeekCloser(data)
	ur := NewUniversalReader(rsc)

	// Trigger ensureBuffer by reading
	buf := make([]byte, 1)
	_, _ = ur.Read(buf)

	// ReadFrom should fail for passthrough readers
	_, err := ur.ReadFrom(strings.NewReader("more data"))
	if err == nil {
		t.Error("ReadFrom should return an error for passthrough readers")
	}
}

func TestUniversalReader_Buffered_ReadFrom(t *testing.T) {
	data := "hello"
	ur := NewUniversalReader(strings.NewReader(data))

	// Trigger ensureBuffer by reading
	buf := make([]byte, 5)
	n, err := ur.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(buf[:n]) != "hello" {
		t.Errorf("Read got %q, want %q", string(buf[:n]), "hello")
	}

	// Append more data via ReadFrom
	n2, err := ur.ReadFrom(strings.NewReader(" world"))
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if n2 != 6 {
		t.Errorf("ReadFrom returned %d, want 6", n2)
	}

	// Seek to start and read all
	_, _ = ur.Seek(0, io.SeekStart)
	allBuf := make([]byte, 20)
	n3, err := ur.Read(allBuf)
	if err != nil && err != io.EOF {
		t.Fatalf("Read after ReadFrom failed: %v", err)
	}
	if string(allBuf[:n3]) != "hello world" {
		t.Errorf("Read after ReadFrom got %q, want %q", string(allBuf[:n3]), "hello world")
	}
}

// --- Edge case tests ---

func TestUniversalReader_Passthrough_ReadAfterClose(t *testing.T) {
	data := []byte("hello")
	rsc := newMockReadSeekCloser(data)
	ur := NewUniversalReader(rsc)

	_ = ur.Close()

	buf := make([]byte, 5)
	_, err := ur.Read(buf)
	if err == nil {
		t.Error("Read after close should return an error")
	}
}

func TestUniversalReader_Passthrough_SeekAfterClose(t *testing.T) {
	data := []byte("hello")
	rsc := newMockReadSeekCloser(data)
	ur := NewUniversalReader(rsc)

	_ = ur.Close()

	_, err := ur.Seek(0, io.SeekStart)
	if err == nil {
		t.Error("Seek after close should return an error")
	}
}

func TestUniversalReader_Buffered_SeekCurrent(t *testing.T) {
	data := "hello world"
	ur := NewUniversalReader(strings.NewReader(data))

	// Read 5 bytes
	buf := make([]byte, 5)
	_, _ = ur.Read(buf)

	// Seek relative to current position (which should be 5)
	pos, err := ur.Seek(1, io.SeekCurrent)
	if err != nil {
		t.Fatalf("SeekCurrent failed: %v", err)
	}
	if pos != 6 {
		t.Errorf("SeekCurrent position = %d, want 6", pos)
	}

	// Read from position 6
	n, err := ur.Read(buf)
	if err != nil {
		t.Fatalf("Read after SeekCurrent failed: %v", err)
	}
	if string(buf[:n]) != "world" {
		t.Errorf("Read after SeekCurrent got %q, want %q", string(buf[:n]), "world")
	}
}

func TestUniversalReader_Buffered_InvalidWhence(t *testing.T) {
	data := "hello"
	ur := NewUniversalReader(strings.NewReader(data))

	// Trigger ensureBuffer
	buf := make([]byte, 1)
	_, _ = ur.Read(buf)

	_, err := ur.Seek(0, 99) // invalid whence
	if err == nil {
		t.Error("Seek with invalid whence should return an error")
	}
}

func TestUniversalReader_Buffered_NegativePosition(t *testing.T) {
	data := "hello"
	ur := NewUniversalReader(strings.NewReader(data))

	// Trigger ensureBuffer
	buf := make([]byte, 1)
	_, _ = ur.Read(buf)

	_, err := ur.Seek(-1, io.SeekStart)
	if err == nil {
		t.Error("Seek to negative position should return an error")
	}
}
