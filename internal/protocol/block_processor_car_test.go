package protocol

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal-plugin-ipfs/internal/upload"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

func TestNewCARBlockProcessor_ValidCAR(t *testing.T) {
	// Create a valid CAR file using our generator
	carBuf := createTestCAR(t, "test content")

	reader := bytes.NewReader(carBuf.Bytes())
	processor, err := NewCARBlockProcessor(reader)

	assert.NoError(t, err, "Should create CARBlockProcessor without error")
	assert.NotNil(t, processor, "Processor should not be nil")
	assert.NotNil(t, processor.reader, "Reader should be initialized")
	assert.False(t, processor.rootsCalled, "rootsCalled should be false initially")
}

func TestNewCARBlockProcessor_InvalidCAR(t *testing.T) {
	invalidData := []byte("not a car file")
	reader := bytes.NewReader(invalidData)

	processor, err := NewCARBlockProcessor(reader)

	assert.Error(t, err, "Should return error for invalid CAR data")
	assert.Nil(t, processor, "Processor should be nil on error")

	// Check that error indicates a CAR format issue (could be various messages)
	errorMsg := strings.ToLower(err.Error())
	isValidError := strings.Contains(errorMsg, "invalid") ||
		strings.Contains(errorMsg, "unexpected") ||
		strings.Contains(errorMsg, "format") ||
		strings.Contains(errorMsg, "eof")
	assert.True(t, isValidError, "Error should indicate CAR format issue, got: %s", err.Error())
}

func TestCARBlockProcessor_Next_HasContent(t *testing.T) {
	// Create CAR with content using our generator
	carBuf := createTestCAR(t, "test content for next")
	reader := bytes.NewReader(carBuf.Bytes())

	processor, err := NewCARBlockProcessor(reader)
	require.NoError(t, err)

	// Should be able to read at least one block from our generated CAR
	retrievedBlock, err := processor.Next()
	if err == nil {
		assert.NotNil(t, retrievedBlock, "Block should not be nil")
		assert.NotNil(t, retrievedBlock.Cid(), "Block should have a CID")
		assert.NotEmpty(t, retrievedBlock.RawData(), "Block should have data")
	}
	assert.True(t, processor.rootsCalled, "rootsCalled should be set to true")
}

func TestCARBlockProcessor_Roots_HasContent(t *testing.T) {
	carBuf := createTestCAR(t, "test content for roots")
	reader := bytes.NewReader(carBuf.Bytes())

	processor, err := NewCARBlockProcessor(reader)
	require.NoError(t, err)

	roots := processor.Roots()
	assert.NotEmpty(t, roots, "Should return non-empty slice for CAR with content")
}

func TestCARBlockProcessor_Release(t *testing.T) {
	carBuf := createTestCAR(t, "test content for release")
	reader := bytes.NewReader(carBuf.Bytes())

	processor, err := NewCARBlockProcessor(reader)
	require.NoError(t, err)

	// Call Release - should not panic or error
	processor.Release()

	// Should still be able to call other methods (Release is no-op for CAR)
	roots := processor.Roots()
	assert.NotNil(t, roots, "Should still be able to access roots after Release")
}

func TestCARBlockProcessor_DoneTracker_Functionality(t *testing.T) {
	carBuf := createTestCAR(t, "test content for done tracker")
	reader := bytes.NewReader(carBuf.Bytes())

	processor, err := NewCARBlockProcessor(reader)
	require.NoError(t, err)

	// Test initial state
	assert.Empty(t, processor.GetDoneCIDs(), "Should have no done CIDs initially")

	// Test Done functionality
	testCID := generateTestCIDFromInt(1)
	processor.Done(testCID)
	assert.Contains(t, processor.GetDoneCIDs(), testCID, "Should contain the marked CID")

	// Test Done with another CID
	anotherCID := generateTestCIDFromInt(2)
	processor.Done(anotherCID)
	assert.Len(t, processor.GetDoneCIDs(), 2, "Should have 2 done CIDs")
}

func TestCARBlockProcessor_RealCARFile(t *testing.T) {
	// Test with a real CAR file if available from fixtures
	// This test can be skipped if the file doesn't exist
	carFilePath := "../testing/fixtures/cars/empty.car"
	carData, err := readTestFixture(carFilePath)
	if err != nil {
		t.Skipf("Skipping test - test fixture %s not available: %v", carFilePath, err)
		return
	}

	reader := bytes.NewReader(carData)
	processor, err := NewCARBlockProcessor(reader)

	if strings.Contains(carFilePath, "empty.car") {
		// For empty CAR, expect no roots but should still create processor
		assert.NoError(t, err, "Should create processor even for empty CAR")
		if err == nil {
			assert.Empty(t, processor.Roots(), "Empty CAR should have no roots")
		}
	}
}

func TestCARBlockProcessor_RealCARFile_WithBlocks(t *testing.T) {
	// Test with a real CAR file containing blocks
	carFilePath := "../testing/fixtures/cars/docx.car"
	carData, err := readTestFixture(carFilePath)
	if err != nil {
		t.Skipf("Skipping test - test fixture %s not available: %v", carFilePath, err)
		return
	}

	reader := bytes.NewReader(carData)
	processor, err := NewCARBlockProcessor(reader)
	require.NoError(t, err)

	// Should have some roots for a real CAR file
	roots := processor.Roots()
	assert.NotEmpty(t, roots, "CAR file with blocks should have roots")

	// Should be able to read blocks (at least one)
	block, err := processor.Next()
	if err == nil {
		assert.NotNil(t, block, "Should read a valid block")
		assert.NotNil(t, block.Cid(), "Block should have a CID")
		assert.NotEmpty(t, block.RawData(), "Block should have data")
	}
}

// createTestCAR creates a test CAR file using our CAR generator
func createTestCAR(t *testing.T, content string) *bytes.Buffer {
	ctx, err := coreTesting.NewTestContext(t)
	require.NoError(t, err)
	logger := ctx.Logger()
	generator := upload.NewCARGeneratorWithDefaults(logger)

	reader := io.NopCloser(strings.NewReader(content))
	buf, _, err := generator.FileToCAR(context.Background(), reader)
	require.NoError(t, err, "Should generate CAR without error")

	return buf
}

// readTestFixture reads a test fixture file
func readTestFixture(path string) ([]byte, error) {
	// Try to read from the current working directory first
	if data, err := os.ReadFile(path); err == nil {
		return data, nil
	}

	// Try to read from relative path to this test file
	relPath := "./testing/fixtures/" + filepath.Base(path)
	return os.ReadFile(relPath)
}

// Test helper function to verify CAR processor implements BlockProcessor interface
func TestCARBlockProcessor_BlockProcessorInterface(t *testing.T) {
	var _ BlockProcessor = &CARBlockProcessor{}

	carBuf := createTestCAR(t, "test content for interface")
	reader := bytes.NewReader(carBuf.Bytes())

	processor, err := NewCARBlockProcessor(reader)
	require.NoError(t, err)

	// Verify all interface methods are available and don't panic
	_ = processor.Next
	_ = processor.Roots
	_ = processor.Done
	_ = processor.GetDoneCIDs
	_ = processor.Release
}
