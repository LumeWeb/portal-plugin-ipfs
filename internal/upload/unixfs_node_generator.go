package upload

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/chunker"
	"github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	"github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	unixfsio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/ipfs/boxo/verifcid"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multicodec"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/upload/common"
)

// UnixFSNodeGenerator defines the interface for creating UnixFS nodes from readers
type UnixFSNodeGenerator interface {
	// CreateNode creates a UnixFS node from a reader using default parameters
	CreateNode(ctx context.Context, reader io.ReadSeekCloser) (format.Node, error)

	// CreateUnixFSNode creates a UnixFS node from a reader with specified parameters
	CreateUnixFSNode(ctx context.Context, r io.ReadSeekCloser, maxlinks int, chunkSize int64) (format.Node, error)

	// CreateDAGFromReader creates a DAG from a reader with the given parameters
	CreateDAGFromReader(ctx context.Context, reader io.Reader, maxlinks int, chunkSize int64, rawLeaves bool) (format.Node, error)

	// CreateDirectory creates an empty UnixFS directory
	CreateDirectory() (unixfsio.Directory, error)

	// GetDAGService returns the underlying DAG service
	GetDAGService() format.DAGService

	// GetBlockstore returns the underlying blockstore
	GetBlockstore() blockstore.Blockstore
}

// UnixFSNodeGeneratorOptions holds configuration options for UnixFSNodeGenerator
type UnixFSNodeGeneratorOptions struct {
	// DAGService is the DAG service to use
	DAGService format.DAGService
	// Blockstore is the blockstore to use
	Blockstore blockstore.Blockstore
	// Logger is the logger to use
	Logger *core.Logger
}

// UnixFSNodeGeneratorOption is a function that configures UnixFSNodeGeneratorOptions
type UnixFSNodeGeneratorOption func(*UnixFSNodeGeneratorOptions)

// WithUnixFSNodeGeneratorDAGService sets the DAG service for the node generator
func WithUnixFSNodeGeneratorDAGService(dagService format.DAGService) UnixFSNodeGeneratorOption {
	return func(opts *UnixFSNodeGeneratorOptions) {
		opts.DAGService = dagService
	}
}

// WithUnixFSNodeGeneratorBlockstore sets the blockstore for the node generator
func WithUnixFSNodeGeneratorBlockstore(blockstore blockstore.Blockstore) UnixFSNodeGeneratorOption {
	return func(opts *UnixFSNodeGeneratorOptions) {
		opts.Blockstore = blockstore
	}
}

// WithUnixFSNodeGeneratorLogger sets the logger for the node generator
func WithUnixFSNodeGeneratorLogger(logger *core.Logger) UnixFSNodeGeneratorOption {
	return func(opts *UnixFSNodeGeneratorOptions) {
		opts.Logger = logger
	}
}

// IPFSUnixFSNodeGenerator implements the UnixFSNodeGenerator interface using IPFS libraries
type IPFSUnixFSNodeGenerator struct {
	dagService format.DAGService
	blockstore blockstore.Blockstore
	logger     *core.Logger
}

// NewUnixFSNodeGenerator creates a new UnixFSNodeGenerator instance
func NewUnixFSNodeGenerator(dagService format.DAGService, logger *core.Logger) UnixFSNodeGenerator {
	// Validate required components
	validator := common.NewComponentValidator()
	if err := validator.ValidateRequiredComponents(dagService, nil, logger); err != nil {
		panic(fmt.Sprintf("Invalid components for UnixFSNodeGenerator: %v", err))
	}

	return &IPFSUnixFSNodeGenerator{
		dagService: dagService,
		logger:     logger,
	}
}

// NewUnixFSNodeGeneratorWithOptions creates a new UnixFSNodeGenerator instance with configurable options
func NewUnixFSNodeGeneratorWithOptions(options ...UnixFSNodeGeneratorOption) UnixFSNodeGenerator {
	// Create default options
	opts := &UnixFSNodeGeneratorOptions{}

	// Apply provided options
	for _, option := range options {
		option(opts)
	}

	// Validate required components
	validator := common.NewComponentValidator()
	if err := validator.ValidateRequiredComponents(opts.DAGService, opts.Blockstore, opts.Logger); err != nil {
		panic(fmt.Sprintf("Invalid components for UnixFSNodeGenerator: %v", err))
	}

	return &IPFSUnixFSNodeGenerator{
		dagService: opts.DAGService,
		blockstore: opts.Blockstore,
		logger:     opts.Logger,
	}
}

// NewUnixFSNodeGeneratorWithDefaults creates a new UnixFSNodeGenerator instance using default in-memory implementations
func NewUnixFSNodeGeneratorWithDefaults(logger *core.Logger) UnixFSNodeGenerator {
	dagService, bstore := common.DefaultInMemoryComponents()

	return NewUnixFSNodeGeneratorWithOptions(
		WithUnixFSNodeGeneratorDAGService(dagService),
		WithUnixFSNodeGeneratorBlockstore(bstore),
		WithUnixFSNodeGeneratorLogger(logger),
	)
}

// CreateDirectory implements UnixFSNodeGenerator.CreateDirectory
func (gen *IPFSUnixFSNodeGenerator) CreateDirectory() (unixfsio.Directory, error) {
	return unixfsio.NewDirectory(gen.dagService)
}

// CreateNode implements UnixFSNodeGenerator.CreateNode
func (gen *IPFSUnixFSNodeGenerator) CreateNode(ctx context.Context, r io.ReadSeekCloser) (format.Node, error) {
	return gen.CreateUnixFSNode(ctx, r, helpers.DefaultLinksPerBlock, 0)
}

// CreateUnixFSNode implements UnixFSNodeGenerator.CreateUnixFSNode
func (gen *IPFSUnixFSNodeGenerator) CreateUnixFSNode(ctx context.Context, r io.ReadSeekCloser, maxlinks int, chunkSize int64) (format.Node, error) {
	// Check for context cancellation
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// First attempt with rawLeaves=false
	node, err := gen.CreateDAGFromReader(ctx, r, maxlinks, chunkSize, false)
	if err != nil && strings.Contains(err.Error(), verifcid.ErrDigestTooLarge.Error()) {
		// Retry with rawLeaves=true for large content
		// Seek back to start for retry
		_, seekErr := r.Seek(0, io.SeekStart)
		if seekErr != nil {
			return nil, fmt.Errorf("failed to seek to start for retry: %w", seekErr)
		}
		node, err = gen.CreateDAGFromReader(ctx, r, maxlinks, chunkSize, true)
	}

	return node, err
}

// CreateDAGFromReader implements UnixFSNodeGenerator.CreateDAGFromReader
func (gen *IPFSUnixFSNodeGenerator) CreateDAGFromReader(ctx context.Context, reader io.Reader, maxlinks int, chunkSize int64, rawLeaves bool) (format.Node, error) {
	if reader == nil {
		return nil, fmt.Errorf("reader cannot be nil")
	}

	builder := cid.V1Builder{
		Codec:  cid.DagProtobuf,
		MhType: uint64(multicodec.Sha2_256),
	}

	// Prepare UnixFS parameters
	params := helpers.DagBuilderParams{
		Maxlinks:   maxlinks,
		CidBuilder: builder,
		Dagserv:    gen.dagService,
		NoCopy:     false,
	}

	if rawLeaves {
		params.RawLeaves = rawLeaves
		builder.Codec = cid.Raw
		params.CidBuilder = builder
	}

	// Create a chunker (splits data into blocks)
	var chnk chunk.Splitter
	if chunkSize > 0 {
		chnk = chunk.NewSizeSplitter(reader, chunkSize)
	} else {
		chnk = chunk.DefaultSplitter(reader)
	}

	// Build the balanced DAG
	db, err := params.New(chnk)
	if err != nil {
		return nil, err
	}

	// Check for context cancellation before layout
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Layout the DAG using balanced layout
	return balanced.Layout(db)
}

// GetDAGService implements UnixFSNodeGenerator.GetDAGService
func (gen *IPFSUnixFSNodeGenerator) GetDAGService() format.DAGService {
	return gen.dagService
}

// GetBlockstore implements UnixFSNodeGenerator.GetBlockstore
func (gen *IPFSUnixFSNodeGenerator) GetBlockstore() blockstore.Blockstore {
	return gen.blockstore
}
