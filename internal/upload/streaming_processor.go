package upload

import (
	"context"
	"fmt"
	"io/fs"
	"sync"
	"time"

	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	unixfsio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"go.lumeweb.com/portal-plugin-ipfs/internal/upload/common"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
)

// ErrRootNodeNotAvailable is returned when GetRootNode is called before ProcessArchive
var ErrRootNodeNotAvailable = fmt.Errorf("root node not available - archive may not have been processed yet")

// FileInfo represents file metadata during streaming processing
type FileInfo struct {
	Path       string    // File path within the archive
	Size       int64     // File size in bytes
	ModTime    time.Time // Modification time
	IsDir      bool      // Whether this is a directory
	Mode       int64     // File permissions
	CID        string    // Content identifier (if available)
	Processed  bool      // Whether the file has been processed
	Error      error     // Any error encountered during processing
	ParentPath string    // Parent directory path
	Name       string    // Base name of the file/directory
	// Memory-efficient: no format.Node stored here
}

// StreamingArchiveProcessor defines the interface for streaming archive processing
type StreamingArchiveProcessor interface {
	// ProcessArchive processes an entire archive using streaming approach
	ProcessArchive(ctx context.Context, extractor ArchiveExtractor) error

	// GetRootNode returns the root node of the processed archive
	GetRootNode(ctx context.Context) (format.Node, error)

	// GetProcessedFiles returns information about all processed files
	GetProcessedFiles() []FileInfo

	// Close cleans up resources and finalizes processing
	Close() error
}

// StreamingProcessor implements StreamingArchiveProcessor with streaming capabilities
type StreamingProcessor struct {
	nodeGenerator     UnixFSNodeGenerator
	dagService        format.DAGService
	blockstore        blockstore.Blockstore
	logger            *core.Logger
	processedFiles    []FileInfo
	directoryMetadata map[string]directoryMetadata // Store directory metadata only
	rootCID           string                       // Store only root CID, not node
	maxLinks          int
	mu                sync.RWMutex // Protects access to processedFiles, directoryMetadata, and rootCID
}

// directoryMetadata stores metadata for directories
type directoryMetadata struct {
	Path       string
	ParentPath string
	Name       string
	CID        string             // CID will be set after directory is created
	DirObj     unixfsio.Directory // In-memory directory object used during tree building
}

// StreamingProcessorOptions holds configuration options for StreamingProcessor
type StreamingProcessorOptions struct {
	NodeGenerator UnixFSNodeGenerator
	DAGService    format.DAGService
	Blockstore    blockstore.Blockstore
	Logger        *core.Logger
	MaxLinks      int
}

// StreamingProcessorOption is a function that configures StreamingProcessorOptions
type StreamingProcessorOption func(*StreamingProcessorOptions)

// WithStreamingProcessorNodeGenerator sets the UnixFS node generator for the streaming processor
func WithStreamingProcessorNodeGenerator(nodeGenerator UnixFSNodeGenerator) StreamingProcessorOption {
	return func(opts *StreamingProcessorOptions) {
		opts.NodeGenerator = nodeGenerator
	}
}

// WithStreamingProcessorDAGService sets the DAG service for the streaming processor
func WithStreamingProcessorDAGService(dagService format.DAGService) StreamingProcessorOption {
	return func(opts *StreamingProcessorOptions) {
		opts.DAGService = dagService
	}
}

// WithStreamingProcessorBlockstore sets the blockstore for the streaming processor
func WithStreamingProcessorBlockstore(blockstore blockstore.Blockstore) StreamingProcessorOption {
	return func(opts *StreamingProcessorOptions) {
		opts.Blockstore = blockstore
	}
}

// WithStreamingProcessorLogger sets the logger for the streaming processor
func WithStreamingProcessorLogger(logger *core.Logger) StreamingProcessorOption {
	return func(opts *StreamingProcessorOptions) {
		opts.Logger = logger
	}
}

// WithStreamingProcessorMaxLinks sets the maximum links per block for the streaming processor
func WithStreamingProcessorMaxLinks(maxLinks int) StreamingProcessorOption {
	return func(opts *StreamingProcessorOptions) {
		opts.MaxLinks = maxLinks
	}
}

// NewStreamingProcessor creates a new streaming processor instance with required dependencies and default options
func NewStreamingProcessor(
	nodeGenerator UnixFSNodeGenerator,
	dagService format.DAGService,
	blockstore blockstore.Blockstore,
	logger *core.Logger,
) *StreamingProcessor {
	// Validate required components
	validator := common.NewComponentValidator()
	if err := validator.ValidateRequiredComponents(dagService, blockstore, logger); err != nil {
		panic(fmt.Sprintf("Invalid components for StreamingProcessor: %v", err))
	}
	if nodeGenerator == nil {
		panic("NodeGenerator is required")
	}

	return NewStreamingProcessorWithOptions(
		WithStreamingProcessorNodeGenerator(nodeGenerator),
		WithStreamingProcessorDAGService(dagService),
		WithStreamingProcessorBlockstore(blockstore),
		WithStreamingProcessorLogger(logger),
		WithStreamingProcessorMaxLinks(helpers.DefaultLinksPerBlock),
	)
}

// NewStreamingProcessorWithDefaults creates a new streaming processor instance using default in-memory implementations
func NewStreamingProcessorWithDefaults(logger *core.Logger) *StreamingProcessor {
	// Create default in-memory implementations using shared factory
	dagService, bstore := DefaultInMemoryComponents()

	// Create node generator with the default DAG service and blockstore
	nodeGenerator := NewUnixFSNodeGeneratorWithOptions(
		WithUnixFSNodeGeneratorDAGService(dagService),
		WithUnixFSNodeGeneratorBlockstore(bstore),
		WithUnixFSNodeGeneratorLogger(logger),
	)

	// Create streaming processor with default components
	return NewStreamingProcessor(
		nodeGenerator,
		dagService,
		bstore,
		logger,
	)
}

// NewStreamingProcessorWithOptions creates a new streaming processor instance with configurable options
func NewStreamingProcessorWithOptions(options ...StreamingProcessorOption) *StreamingProcessor {
	// Create default options
	opts := &StreamingProcessorOptions{
		MaxLinks: helpers.DefaultLinksPerBlock,
	}

	// Apply provided options
	for _, option := range options {
		option(opts)
	}

	// Validate required components using ComponentValidator
	validator := common.NewComponentValidator()
	if err := validator.ValidateRequiredComponents(opts.DAGService, opts.Blockstore, opts.Logger); err != nil {
		panic(fmt.Sprintf("Invalid components for StreamingProcessor: %v", err))
	}
	if opts.NodeGenerator == nil {
		panic("NodeGenerator is required - use WithStreamingProcessorNodeGenerator option")
	}

	return &StreamingProcessor{
		nodeGenerator:     opts.NodeGenerator,
		dagService:        opts.DAGService,
		blockstore:        opts.Blockstore,
		logger:            opts.Logger,
		processedFiles:    make([]FileInfo, 0),
		directoryMetadata: make(map[string]directoryMetadata),
		maxLinks:          opts.MaxLinks,
	}
}

// ProcessArchive implements StreamingArchiveProcessor.ProcessArchive
func (sp *StreamingProcessor) ProcessArchive(ctx context.Context, extractor ArchiveExtractor) error {
	// Check for context cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Initialize processor state
	sp.mu.Lock()
	sp.processedFiles = make([]FileInfo, 0)
	sp.directoryMetadata = make(map[string]directoryMetadata)
	sp.rootCID = ""
	sp.mu.Unlock()

	// Get filesystem from extractor
	efs, err := extractor.Filesystem(ctx)
	if err != nil {
		return fmt.Errorf("failed to get filesystem: %w", err)
	}

	// Scan filesystem and collect metadata
	files, err := sp.collectFileMetadata(ctx, efs)
	if err != nil {
		return fmt.Errorf("failed to collect metadata: %w", err)
	}

	// Handle empty archive case
	sp.mu.RLock()
	dirCount := 0
	if sp.directoryMetadata != nil {
		dirCount = len(sp.directoryMetadata)
	}
	sp.mu.RUnlock()
	if len(files) == 0 && dirCount <= 1 {
		// No files and only root directory (or no directories at all)
		return fmt.Errorf("no entries found in archive")
	}

	// Build directory tree in memory
	if err := sp.buildDirectoryTree(ctx); err != nil {
		return fmt.Errorf("failed to build directory tree in memory: %w", err)
	}

	// Store directory tree to blockstore
	if err := sp.persistDirectoryTree(ctx); err != nil {
		return fmt.Errorf("failed to store directory tree to blockstore: %w", err)
	}

	if sp.logger != nil {
		sp.mu.RLock()
		dirCount := 0
		if sp.directoryMetadata != nil {
			dirCount = len(sp.directoryMetadata)
		}
		rootCID := sp.rootCID
		sp.mu.RUnlock()
		sp.logger.Info("Successfully processed archive",
			zap.Int("total_files", len(files)),
			zap.Int("directories", dirCount),
			zap.String("root_cid", rootCID))
	}

	return nil
}

// GetRootNode implements StreamingArchiveProcessor.GetRootNode
func (sp *StreamingProcessor) GetRootNode(ctx context.Context) (format.Node, error) {
	sp.mu.RLock()
	rootCID := sp.rootCID
	sp.mu.RUnlock()

	if rootCID == "" {
		return nil, ErrRootNodeNotAvailable
	}

	// Reconstruct root node from blockstore using stored CID
	decodedCID, err := cid.Decode(rootCID)
	if err != nil {
		return nil, fmt.Errorf("failed to decode root CID: %w", err)
	}

	return sp.dagService.Get(ctx, decodedCID)
}

// GetProcessedFiles implements StreamingArchiveProcessor.GetProcessedFiles
func (sp *StreamingProcessor) GetProcessedFiles() []FileInfo {
	sp.mu.RLock()
	defer sp.mu.RUnlock()

	// Return a copy to prevent external modification
	if sp.processedFiles == nil {
		return []FileInfo{}
	}
	files := make([]FileInfo, len(sp.processedFiles))
	copy(files, sp.processedFiles)
	return files
}

// Close implements StreamingArchiveProcessor.Close
func (sp *StreamingProcessor) Close() error {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	// Clear processed files
	sp.processedFiles = nil
	sp.directoryMetadata = nil
	sp.rootCID = ""
	return nil
}

// addProcessedFile adds a file to the processed files list
func (sp *StreamingProcessor) addProcessedFile(fileInfo FileInfo) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.processedFiles = append(sp.processedFiles, fileInfo)
}

// getMaxLinks returns the maximum links per block setting
func (sp *StreamingProcessor) getMaxLinks() int {
	return sp.maxLinks
}

// collectFileMetadata walks the filesystem and collects file and directory metadata
func (sp *StreamingProcessor) collectFileMetadata(ctx context.Context, efs fs.FS) ([]FileInfo, error) {
	var files []FileInfo

	// Walk the filesystem and collect metadata only
	err := fs.WalkDir(efs, common.ROOT, func(currentPath string, d fs.DirEntry, err error) error {
		// Handle context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err != nil {
			// Check for empty archive conditions
			if currentPath == common.ROOT && (common.IsNoSuchFileError(err) || common.IsPathError(err)) {
				return nil // Empty archive, let caller handle
			}
			return fmt.Errorf("error accessing path %s: %w", currentPath, err)
		}

		// Validate path using common utility
		if !common.IsValidPath(currentPath) {
			return fmt.Errorf("invalid path detected: %s", currentPath)
		}

		// Skip root path from file list but track it as directory
		if currentPath == common.ROOT {
			sp.mu.Lock()
			if sp.directoryMetadata != nil {
				sp.directoryMetadata[currentPath] = directoryMetadata{
					Path:       currentPath,
					ParentPath: "", // Root has no parent
					Name:       common.ROOT,
				}
			}
			sp.mu.Unlock()
			return nil
		}

		// Get file info
		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("failed to get info for %s: %w", currentPath, err)
		}

		// Determine parent path
		parentPath := common.GetParentPath(currentPath)

		if d.IsDir() {
			// Only record directory metadata, don't create UnixFS directory objects yet
			sp.mu.Lock()
			if sp.directoryMetadata != nil {
				sp.directoryMetadata[currentPath] = directoryMetadata{
					Path:       currentPath,
					ParentPath: parentPath,
					Name:       d.Name(),
				}
			}
			sp.mu.Unlock()
			return nil
		}

		// Process file immediately - create UnixFS node and store to blockstore
		fileInfo := FileInfo{
			Path:       currentPath,
			Name:       d.Name(),
			Size:       info.Size(),
			ModTime:    info.ModTime(),
			IsDir:      false,
			Mode:       int64(info.Mode()),
			ParentPath: parentPath,
			Processed:  false,
			Error:      nil,
		}

		if err := sp.processFile(ctx, &fileInfo, efs); err != nil {
			fileInfo.Error = err
			if sp.logger != nil {
				sp.logger.Warn("Failed to process file",
					zap.String("path", currentPath),
					zap.Error(err))
			}
		}

		// Add to both local files and processor's processedFiles
		files = append(files, fileInfo)
		sp.addProcessedFile(fileInfo)
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("filesystem walk failed: %w", err)
	}

	return files, nil
}

// buildDirectoryTree constructs the directory tree in memory
func (sp *StreamingProcessor) buildDirectoryTree(ctx context.Context) error {
	// Check if directoryMetadata map is nil (race condition protection)
	sp.mu.RLock()
	if sp.directoryMetadata == nil {
		sp.mu.RUnlock()
		return nil
	}

	// Copy directoryMetadata to work with it without holding the lock
	directoryMetadataCopy := make(map[string]directoryMetadata)
	for k, v := range sp.directoryMetadata {
		directoryMetadataCopy[k] = v
	}
	sp.mu.RUnlock()

	// Sort directory paths from deepest to shallowest
	var dirPaths []string
	for _path := range directoryMetadataCopy {
		dirPaths = append(dirPaths, _path)
	}

	dirPaths = common.SortDirectoriesByDepth(dirPaths)

	// Create a snapshot of processedFiles once to avoid repeated copies
	sp.mu.RLock()
	processedFilesCopy := make([]FileInfo, len(sp.processedFiles))
	copy(processedFilesCopy, sp.processedFiles)
	sp.mu.RUnlock()

	// Create UnixFS directory objects in memory only
	for _, _path := range dirPaths {
		metadata := directoryMetadataCopy[_path]

		// Create UnixFS directory in memory
		dir, err := sp.nodeGenerator.CreateDirectory()
		if err != nil {
			return fmt.Errorf("failed to create directory for %s: %w", _path, err)
		}

		// Store the directory object in metadata for later use
		metadata.DirObj = dir
		directoryMetadataCopy[_path] = metadata
	}

	// Build tree from child to parent using stored metadata only
	for _, _path := range dirPaths {
		metadata := directoryMetadataCopy[_path]

		if _path != common.ROOT {
			// Get parent directory object from metadata (not blockstore)
			parentMetadata := directoryMetadataCopy[metadata.ParentPath]
			parentDir := parentMetadata.DirObj
			if parentDir == nil {
				return fmt.Errorf("parent directory object not available for %s", metadata.ParentPath)
			}

			dirNode, err := metadata.DirObj.GetNode()
			if err != nil {
				return fmt.Errorf("failed to get node for directory %s: %w", _path, err)
			}

			// Add child directories to parent
			if err = parentDir.AddChild(ctx, metadata.Name, dirNode); err != nil {
				return fmt.Errorf("failed to add directory %s to parent %s: %w", _path, metadata.ParentPath, err)
			}
		}

		// Add files to this directory using stored metadata

		for _, file := range processedFilesCopy {
			if file.ParentPath == _path && file.Processed && file.Error == nil {
				// Get file node from blockstore using stored CID
				fileCID, err := cid.Decode(file.CID)
				if err != nil {
					return fmt.Errorf("failed to decode file CID %s: %w", file.CID, err)
				}

				fileNode, err := sp.dagService.Get(ctx, fileCID)
				if err != nil {
					return fmt.Errorf("failed to get file node %s: %w", file.CID, err)
				}

				// Add file to directory object in memory
				if err = metadata.DirObj.AddChild(ctx, file.Name, fileNode); err != nil {
					return fmt.Errorf("failed to add file %s to directory %s: %w", file.Name, _path, err)
				}
			}
		}
	}

	// Write the modified copy back to the original map
	sp.mu.Lock()
	sp.directoryMetadata = directoryMetadataCopy
	sp.mu.Unlock()

	if sp.logger != nil {
		totalDirs := len(directoryMetadataCopy)
		sp.logger.Debug("Directory tree construction completed",
			zap.Int("total_directories", totalDirs))
	}

	return nil
}

// persistDirectoryTree stores the complete directory tree to blockstore
func (sp *StreamingProcessor) persistDirectoryTree(ctx context.Context) error {
	// Check if directoryMetadata map is nil (race condition protection)
	sp.mu.RLock()
	if sp.directoryMetadata == nil {
		sp.mu.RUnlock()
		return nil
	}

	// Copy directoryMetadata to work with it without holding the lock
	directoryMetadataCopy := make(map[string]directoryMetadata)
	for k, v := range sp.directoryMetadata {
		directoryMetadataCopy[k] = v
	}
	sp.mu.RUnlock()

	// Sort directory paths from deepest to shallowest for storage
	var dirPaths []string
	for _path := range directoryMetadataCopy {
		dirPaths = append(dirPaths, _path)
	}

	dirPaths = common.SortDirectoriesByDepth(dirPaths)

	// Store directories from deepest to shallowest
	for _, _path := range dirPaths {
		metadata := directoryMetadataCopy[_path]
		dir := metadata.DirObj

		// Get the directory node
		dirNode, err := dir.GetNode()
		if err != nil {
			return fmt.Errorf("failed to get directory node for %s: %w", _path, err)
		}

		// Store the directory node in blockstore
		if err = sp.dagService.Add(ctx, dirNode); err != nil {
			return fmt.Errorf("failed to store directory %s: %w", _path, err)
		}

		// Update metadata with the actual CID
		metadata.CID = dirNode.Cid().String()
		directoryMetadataCopy[_path] = metadata

		// Update root CID if this is the root directory
		if _path == common.ROOT {
			sp.mu.Lock()
			sp.rootCID = metadata.CID
			sp.mu.Unlock()
		}
	}

	// Write the modified copy back to the original map
	sp.mu.Lock()
	sp.directoryMetadata = directoryMetadataCopy
	sp.mu.Unlock()

	if sp.logger != nil {
		totalDirs := len(directoryMetadataCopy)
		sp.logger.Debug("Directory tree persisted to blockstore",
			zap.Int("total_directories", totalDirs))
	}

	return nil
}

// processFile processes a single file and stores its CID metadata
func (sp *StreamingProcessor) processFile(ctx context.Context, fileInfo *FileInfo, efs fs.FS) error {
	// Check for context cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Open the file from the filesystem
	file, err := efs.Open(fileInfo.Path)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", fileInfo.Path, err)
	}

	// Ensure file is closed even if processing fails
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			if sp.logger != nil {
				sp.logger.Warn("Failed to close file",
					zap.String("path", fileInfo.Path),
					zap.Error(closeErr))
			}
		}
	}()

	// Create a seekable wrapper for the file
	seekableFile := NewUniversalReader(file)

	// Use the nodeGenerator to create a UnixFS node
	node, err := sp.nodeGenerator.CreateNode(ctx, seekableFile)
	if err != nil {
		return fmt.Errorf("failed to create node for file %s: %w", fileInfo.Path, err)
	}

	// Store the node in the blockstore via dagService immediately
	if err := sp.dagService.Add(ctx, node); err != nil {
		return fmt.Errorf("failed to store node for file %s: %w", fileInfo.Path, err)
	}

	// Update file info with CID and processed status - keep only metadata
	fileInfo.CID = node.Cid().String()
	fileInfo.Processed = true

	if sp.logger != nil {
		sp.logger.Debug("File processed successfully",
			zap.String("path", fileInfo.Path),
			zap.String("cid", node.Cid().String()),
			zap.Int64("size", fileInfo.Size))
	}

	return nil
}
