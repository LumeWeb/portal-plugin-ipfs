package upload

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"strings"

	"github.com/docker/go-units"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/ipld/merkledag"
	unixfsio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-unixfsnode"
	carv2 "github.com/ipld/go-car/v2"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	blockstoreAdapter "github.com/ipld/go-ipld-prime/storage/bsadapter"
	_ "go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/upload/common"
	"go.uber.org/zap"
)

// CARGenerator defines the interface for CAR generation operations
type CARGenerator interface {
	// ArchiveToCAR converts an archive extractor to a CAR file buffer and root CID
	ArchiveToCAR(ctx context.Context, extractor ArchiveExtractor) (*bytes.Buffer, cid.Cid, error)

	// FileToCAR converts a single file reader to a CAR file buffer and root CID
	FileToCAR(ctx context.Context, reader io.ReadCloser) (*bytes.Buffer, cid.Cid, error)
}

// CARGeneratorOptions holds configuration options for CARGenerator
type CARGeneratorOptions struct {
	// NodeGenerator is the UnixFS node generator to use
	NodeGenerator UnixFSNodeGenerator
	// Blockstore is the blockstore to use (optional, will be extracted from NodeGenerator if not provided)
	Blockstore blockstore.Blockstore
	// DAGService is the DAG service to use (optional, will be extracted from NodeGenerator if not provided)
	DAGService format.DAGService
}

// CARGeneratorOption is a function that configures CARGeneratorOptions
type CARGeneratorOption func(*CARGeneratorOptions)

// WithCARGeneratorNodeGenerator sets the UnixFS node generator for the CAR generator
func WithCARGeneratorNodeGenerator(nodeGenerator UnixFSNodeGenerator) CARGeneratorOption {
	return func(opts *CARGeneratorOptions) {
		opts.NodeGenerator = nodeGenerator
	}
}

// WithCARGeneratorBlockstore sets the blockstore for the CAR generator
func WithCARGeneratorBlockstore(blockstore blockstore.Blockstore) CARGeneratorOption {
	return func(opts *CARGeneratorOptions) {
		opts.Blockstore = blockstore
	}
}

// WithCARGeneratorDAGService sets the DAG service for the CAR generator
func WithCARGeneratorDAGService(dagService format.DAGService) CARGeneratorOption {
	return func(opts *CARGeneratorOptions) {
		opts.DAGService = dagService
	}
}

// IPFSCARGenerator implements the CARGenerator interface using IPFS libraries
type IPFSCARGenerator struct {
	blockstore    blockstore.Blockstore
	dagService    format.DAGService
	logger        *core.Logger
	nodeGenerator UnixFSNodeGenerator
}

const (
	// DefaultChunkSize represents the default chunk size for file processing (256 KiB)
	DefaultChunkSize = 256 * units.KB
)

// NewCARGenerator creates a new CARGenerator instance with required nodeGenerator DI
func NewCARGenerator(logger *core.Logger, nodeGenerator UnixFSNodeGenerator) CARGenerator {
	// Validate required components
	validator := common.NewComponentValidator()
	if err := validator.ValidateRequiredComponents(nil, nil, logger); err != nil {
		panic(fmt.Sprintf("Invalid components for CARGenerator: %v", err))
	}
	if nodeGenerator == nil {
		panic("NodeGenerator is required")
	}

	return NewCARGeneratorWithOptions(logger, WithCARGeneratorNodeGenerator(nodeGenerator))
}

// NewCARGeneratorWithDefaults creates a new CARGenerator instance using default in-memory implementations
func NewCARGeneratorWithDefaults(logger *core.Logger) CARGenerator {
	// Create default node generator with in-memory components
	nodeGenerator := NewUnixFSNodeGeneratorWithDefaults(logger)

	return NewCARGeneratorWithOptions(
		logger,
		WithCARGeneratorNodeGenerator(nodeGenerator),
	)
}

// NewCARGeneratorWithOptions creates a new CARGenerator instance with configurable options
func NewCARGeneratorWithOptions(logger *core.Logger, options ...CARGeneratorOption) CARGenerator {
	// Create default options
	opts := &CARGeneratorOptions{}

	// Apply provided options
	for _, option := range options {
		option(opts)
	}

	// Validate required options
	if opts.NodeGenerator == nil {
		panic("NodeGenerator is required - use WithCARGeneratorNodeGenerator option")
	}

	// Use provided components or extract from nodeGenerator
	var dagService format.DAGService
	var bstore blockstore.Blockstore

	if opts.DAGService != nil {
		dagService = opts.DAGService
	}
	if opts.Blockstore != nil {
		bstore = opts.Blockstore
	}

	// If components are not provided, extract from nodeGenerator
	if dagService == nil {
		dagService = opts.NodeGenerator.GetDAGService()
	}
	if bstore == nil {
		bstore = opts.NodeGenerator.GetBlockstore()
	}

	// Validate required components using ComponentValidator
	validator := common.NewComponentValidator()
	if err := validator.ValidateRequiredComponents(dagService, bstore, logger); err != nil {
		panic(fmt.Sprintf("Invalid components for CARGenerator: %v", err))
	}

	return &IPFSCARGenerator{
		blockstore:    bstore,
		dagService:    dagService,
		logger:        logger,
		nodeGenerator: opts.NodeGenerator,
	}
}

// generateCARFromStore generates a CAR file from a blockstore and root CID
func (gen *IPFSCARGenerator) generateCARFromStore(ctx context.Context, node format.Node) (*bytes.Buffer, error) {
	// Set up LinkSystem with the provided blockstore
	ls := cidlink.DefaultLinkSystem()
	ls.SetReadStorage(&blockstoreAdapter.Adapter{
		Wrapped: gen.blockstore,
	})

	unixfsnode.AddUnixFSReificationToLinkSystem(&ls)

	spec := unixfsnode.ExploreAllRecursivelySelector
	sel := spec.Node()

	// Create SelectiveWriter to generate CAR using the complete DAG
	w, err := carv2.NewSelectiveWriter(
		ctx,
		&ls,
		node.Cid(),
		sel,
	)
	if err != nil {
		gen.logger.Error("Failed to create selective writer", zap.Error(err))
		return nil, fmt.Errorf("failed to create selective writer: %w", err)
	}

	// Write CAR to memory buffer
	var buf bytes.Buffer
	if _, err := w.WriteTo(&buf); err != nil {
		gen.logger.Error("Failed to write CAR to buffer", zap.Error(err))
		return nil, fmt.Errorf("failed to write CAR: %w", err)
	}

	return &buf, nil
}

// pruneEmptyDirectories removes empty directories from the directory tree and updates their parents
func (gen *IPFSCARGenerator) pruneEmptyDirectories(ctx context.Context, directories map[string]unixfsio.Directory) error {
	// Track which directories are empty
	emptyDirs := make(map[string]bool)

	emptyDir, err := unixfsio.NewDirectory(gen.dagService)
	if err != nil {
		return fmt.Errorf("failed to create empty directory: %w", err)
	}

	emptyDirNode, err := emptyDir.GetNode()

	if err != nil {
		return fmt.Errorf("failed to get empty directory node: %w", err)
	}

	// First pass: identify empty directories
	for _path, dir := range directories {
		// Get the directory node to check its links
		node, err := dir.GetNode()
		if err != nil {
			gen.logger.Warn("Failed to get directory node for pruning check", zap.String("path", _path), zap.Error(err))
			continue
		}

		// A directory is empty if it has no links or only links to standard empty directories
		links := node.Links()
		isEmpty := len(links) == 0 ||
			(len(links) == 1 && links[0].Cid.Equals(emptyDirNode.Cid()))

		emptyDirs[_path] = isEmpty
	}

	// Second pass: remove empty directories from their parents
	for _path, isEmpty := range emptyDirs {
		if !isEmpty {
			continue // Skip non-empty directories
		}

		// Get parent directory
		parentPath := common.GetParentPath(_path)
		parentDir, parentExists := directories[parentPath]
		if !parentExists {
			continue // Parent not found, skip
		}

		// Remove the child from parent directory
		dirName := _path[strings.LastIndex(_path, "/")+1:]
		if err := parentDir.RemoveChild(ctx, dirName); err != nil {
			// Log the error but continue - this is a best-effort cleanup operation
			continue
		}
	}

	// Third pass: remove empty directories from the map (except root)
	for _path, isEmpty := range emptyDirs {
		if isEmpty && _path != "." {
			delete(directories, _path)
		}
	}

	return nil
}

// collectArchiveEntries extracts archive entries and builds a directory structure
func (gen *IPFSCARGenerator) collectArchiveEntries(ctx context.Context, extractor ArchiveExtractor) (unixfsio.Directory, map[string]unixfsio.Directory, []format.Node, error) {
	efs, err := extractor.Filesystem(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	// Create a mutable root directory
	rootDir, err := unixfsio.NewDirectory(gen.dagService)
	if err != nil {
		return nil, nil, nil, err
	}

	// Track created directories to build the hierarchy
	directories := make(map[string]unixfsio.Directory)
	files := make([]format.Node, 0)
	directories[common.ROOT] = rootDir // root directory

	// Pass 1: Walk the filesystem to create all directory objects and process files.
	// We do NOT link directories to their parents yet.
	err = fs.WalkDir(efs, common.ROOT, func(currentPath string, d fs.DirEntry, err error) error {
		if err != nil {
			// Check if this is an empty archive by looking for filesystem-related errors
			// that indicate no entries exist (e.g., "no such file or directory" for root)
			if currentPath == common.ROOT && (common.IsNoSuchFileError(err) || common.IsPathError(err)) {
				// This appears to be an empty archive - return nil to let the
				// "no entries found" check in ArchiveToCAR handle it
				return nil
			}
			return err
		}

		// Skip root path
		if currentPath == common.ROOT {
			return nil
		}

		if d.IsDir() {
			// Create directory entry and store it. Don't add to parent yet.
			dir, err := unixfsio.NewDirectory(gen.dagService)
			if err != nil {
				return err
			}
			directories[currentPath] = dir
		} else {
			// Handle file
			_, err = d.Info()
			if err != nil {
				return nil // Continue processing
			}

			// Open the file from the archive filesystem
			file, err := efs.Open(currentPath)
			if err != nil {
				return nil // Continue processing
			}

			// Get parent directory
			parentPath := common.GetParentPath(currentPath)
			parentDir, exists := directories[parentPath]
			if !exists {
				if closeErr := file.Close(); closeErr != nil {
					// Silently close file
				}
				return nil // Continue processing
			}

			node, err := gen.createNode(ctx, &UniversalReader{reader: file})
			if err != nil {
				if closeErr := file.Close(); closeErr != nil {
					// Silently close file
				}
				return err
			}

			// Add file to parent directory
			if protoNode, ok := node.(*merkledag.ProtoNode); ok {
				err = parentDir.AddChild(ctx, d.Name(), protoNode)
			} else {
				return fmt.Errorf("expected ProtoNode, got %T", node)
			}
			if err != nil {
				if closeErr := file.Close(); closeErr != nil {
					gen.logger.Warn("Failed to close file", zap.String("path", currentPath), zap.Error(closeErr))
				}
				return err
			}

			files = append(files, node)

			// Close the file when we're done with it
			if closeErr := file.Close(); closeErr != nil {
				// Silently close file
			}
		}

		return nil
	})

	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to walk archive filesystem: %w", err)
	}

	// Pass 2: Now that all directories are created and files are in place,
	// build the directory hierarchy from the bottom up.
	// We sort paths from deepest to shallowest to ensure children are finalized before their parents are linked.
	var dirPaths []string
	for path := range directories {
		if path != common.ROOT {
			dirPaths = append(dirPaths, path)
		}
	}
	dirPaths = common.SortDirectoriesByDepth(dirPaths)

	for _, path := range dirPaths {
		parentPath := common.GetParentPath(path)
		parentDir, exists := directories[parentPath]
		if !exists {
			return nil, nil, nil, fmt.Errorf("parent directory not found during linking: %s", parentPath)
		}

		dir := directories[path]
		node, err := dir.GetNode()
		if err != nil {
			return nil, nil, nil, fmt.Errorf("failed to get node for directory %s: %w", path, err)
		}

		dirName := path[strings.LastIndex(path, "/")+1:]
		if err := parentDir.AddChild(ctx, dirName, node); err != nil {
			return nil, nil, nil, fmt.Errorf("failed to link directory %s to parent %s: %w", path, parentPath, err)
		}

	}

	return directories[common.ROOT], directories, files, nil
}

// createDAGFromReader creates a DAG from a reader with the given parameters
func (gen *IPFSCARGenerator) createDAGFromReader(ctx context.Context, reader io.Reader, maxlinks int, chunkSize int64, rawLeaves bool) (format.Node, error) {
	return gen.nodeGenerator.CreateDAGFromReader(ctx, reader, maxlinks, chunkSize, rawLeaves)
}

// createUnixFSNode creates a UnixFS node from a reader using specified parameters
// Automatically detects and switches to rawLeaves=true for large content to avoid identity hash limits
func (gen *IPFSCARGenerator) createUnixFSNode(ctx context.Context, r io.ReadSeekCloser, maxlinks int, chunkSize int64) (format.Node, error) {
	return gen.nodeGenerator.CreateUnixFSNode(ctx, r, maxlinks, chunkSize)
}

// createNode creates a UnixFS node from a reader using default parameters
func (gen *IPFSCARGenerator) createNode(ctx context.Context, r io.ReadSeekCloser) (format.Node, error) {
	return gen.nodeGenerator.CreateNode(ctx, r)
}

// processNodeTree manually crawls the node tree and processes all file and folder records
// ensuring all nodes are stored in the blockstore
func (gen *IPFSCARGenerator) processNodeTree(ctx context.Context, directories map[string]unixfsio.Directory, files []format.Node) error {
	// Process all subdirectories
	for _path, dir := range directories {
		if err := gen.processDirectory(ctx, dir, _path); err != nil {
			gen.logger.Warn("Failed to process directory", zap.String("path", _path), zap.Error(err))
			continue
		}
	}

	// Process all files
	for _, fileNode := range files {
		if err := gen.processFile(ctx, fileNode); err != nil {
			gen.logger.Warn("Failed to process file node", zap.String("cid", fileNode.Cid().String()), zap.Error(err))
			continue
		}
	}

	return nil
}

// processDirectory ensures a directory and all its children are stored in the blockstore
func (gen *IPFSCARGenerator) processDirectory(ctx context.Context, dir unixfsio.Directory, path string) error {
	// Get the directory node to ensure it's stored
	node, err := dir.GetNode()
	if err != nil {
		return fmt.Errorf("failed to get directory node: %w", err)
	}

	// Manually store the directory node in the blockstore
	if err := gen.dagService.Add(ctx, node); err != nil {
		return fmt.Errorf("failed to store directory node: %w", err)
	}

	return nil
}

// processFile ensures a file and all its data blocks are stored in the blockstore
func (gen *IPFSCARGenerator) processFile(ctx context.Context, fileNode format.Node) error {
	// Store the file node itself
	if err := gen.dagService.Add(ctx, fileNode); err != nil {
		return fmt.Errorf("failed to store file node: %w", err)
	}

	// Recursively process all child nodes (data blocks) of the file
	return nil
}

// ArchiveToCAR implements CARGenerator.ArchiveToCAR
func (gen *IPFSCARGenerator) ArchiveToCAR(ctx context.Context, extractor ArchiveExtractor) (*bytes.Buffer, cid.Cid, error) {
	// Check for context cancellation
	if err := ctx.Err(); err != nil {
		return nil, cid.Undef, err
	}

	// Collect archive entries and build directory structure
	rootDir, directories, files, err := gen.collectArchiveEntries(ctx, extractor)
	if err != nil {
		return nil, cid.Undef, fmt.Errorf("failed to collect archive entries: %w", err)
	}

	// Check if we have any entries
	if len(directories) == 1 && len(files) == 0 {
		return nil, cid.Undef, fmt.Errorf("no entries found in archive")
	}

	// Prune empty directories BEFORE processing the node tree.
	// This ensures the final state is written to the blockstore.
	if err := gen.pruneEmptyDirectories(ctx, directories); err != nil {
		return nil, cid.Undef, fmt.Errorf("failed to prune empty directories: %w", err)
	}

	// Process the final, pruned node tree to ensure all nodes are stored in blockstore
	if err := gen.processNodeTree(ctx, directories, files); err != nil {
		return nil, cid.Undef, fmt.Errorf("failed to process node tree: %w", err)
	}

	// Get the root directory node
	rootDirNode, err := rootDir.GetNode()
	if err != nil {
		return nil, cid.Undef, fmt.Errorf("failed to get root directory node: %w", err)
	}

	// Generate CAR from the blockstore
	buf, err := gen.generateCARFromStore(ctx, rootDirNode)
	if err != nil {
		return nil, cid.Undef, err
	}
	return buf, rootDirNode.Cid(), nil
}

// FileToCAR implements CARGenerator.FileToCAR
func (gen *IPFSCARGenerator) FileToCAR(ctx context.Context, reader io.ReadCloser) (*bytes.Buffer, cid.Cid, error) {
	if reader == nil {
		return nil, cid.Undef, fmt.Errorf("reader is nil")
	}

	// Check for context cancellation
	if err := ctx.Err(); err != nil {
		return nil, cid.Undef, err
	}

	// Create a UnixFS node from the file reader
	node, err := gen.createNode(ctx, NewUniversalReader(reader))
	if err != nil {
		return nil, cid.Undef, fmt.Errorf("failed to create UnixFS node: %w", err)
	}

	// Store the file node in the blockstore
	if err := gen.dagService.Add(ctx, node); err != nil {
		return nil, cid.Undef, fmt.Errorf("failed to store file node: %w", err)
	}

	// Generate CAR from the blockstore
	buf, err := gen.generateCARFromStore(ctx, node)
	if err != nil {
		return nil, cid.Undef, err
	}
	return buf, node.Cid(), nil
}

// ArchiveExtractorToCAR converts an ArchiveExtractor to a CAR file buffer and root CID.
// This is a backward compatibility wrapper that uses the new interface-based implementation.
func ArchiveExtractorToCAR(ctx context.Context, logger *core.Logger, extractor ArchiveExtractor) (*bytes.Buffer, cid.Cid, error) {
	generator := NewCARGeneratorWithDefaults(logger)

	return generator.ArchiveToCAR(ctx, extractor)
}

// SingleFileToCAR converts a single file reader to a CAR file buffer and root CID.
// This is a backward compatibility wrapper that uses the new interface-based implementation.
func SingleFileToCAR(ctx context.Context, logger *core.Logger, r io.ReadCloser) (*bytes.Buffer, cid.Cid, error) {
	generator := NewCARGeneratorWithDefaults(logger)

	return generator.FileToCAR(ctx, r)
}
