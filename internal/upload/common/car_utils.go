package common

import (
	"context"
	"fmt"
	"io"

	contentCar "go.lumeweb.com/ipfs-content/car"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
)

// GetCARBlockDAGSize reads a CAR file and returns the total size of all blocks in the DAG.
// This uses the ipfs-content ReadCAR API to calculate the actual raw block size,
// not the CAR file size (which includes headers and overhead).
//
// The memoryLimit parameter controls the LRU cache size for indexed CAR reading.
// Recommended values:
//   - For small CARs: 10-100 MB
//   - For large CARs: 100-1000 MB
//   - For very large CARs: Minimum to hold frequently-accessed blocks
//
// This function requires a seekable reader (io.ReadSeeker). The reader position
// will be reset to the beginning before returning.
//
// Parameters:
//   - ctx: Context for cancellation
//   - reader: Seekable CAR file reader (must implement io.ReadSeeker)
//   - logger: Logger for error reporting
//   - memoryLimit: Maximum bytes for LRU cache during CAR reading
//
// Returns:
//   - uint64: Total size of all blocks in the DAG (raw block data size)
//   - error: Error if CAR reading fails or reader is not seekable
func GetCARBlockDAGSize(ctx context.Context, reader io.ReadSeeker, logger *core.Logger, memoryLimit uint64) (uint64, error) {
	// Validate reader is seekable
	if _, ok := reader.(io.Seeker); !ok {
		return 0, fmt.Errorf("reader must be seekable (io.ReadSeeker required)")
	}

	// Read CAR and reconstruct tree structure
	// ReadCAR will index blocks and calculate actual DAG block sizes
	summary, err := contentCar.ReadCAR(ctx, reader, memoryLimit)
	if err != nil {
		logger.Error("Failed to read CAR for DAG size calculation",
			zap.Error(err))
		return 0, fmt.Errorf("failed to read CAR: %w", err)
	}

	// Reset reader to beginning for subsequent operations
	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		logger.Error("Failed to reset CAR reader position",
			zap.Error(err))
		return 0, fmt.Errorf("failed to reset reader position: %w", err)
	}

	// Return the total size of all blocks in the DAG (not CAR file size)
	return summary.TotalSize, nil
}

// GetCARBlockDAGSizeWithDefaultLimit reads a CAR file and returns the total size of all blocks in the DAG.
// This is a convenience function that uses the default memory limit from ipfs-content.
//
// Parameters:
//   - ctx: Context for cancellation
//   - reader: Seekable CAR file reader (must implement io.ReadSeeker)
//   - logger: Logger for error reporting
//
// Returns:
//   - uint64: Total size of all blocks in the DAG (raw block data size)
//   - error: Error if CAR reading fails or reader is not seekable
func GetCARBlockDAGSizeWithDefaultLimit(ctx context.Context, reader io.ReadSeeker, logger *core.Logger) (uint64, error) {
	// Use the default memory limit from ipfs-content (100MB)
	// This provides a good balance for most upload scenarios
	return GetCARBlockDAGSize(ctx, reader, logger, contentCar.DefaultMemoryLimit)
}
