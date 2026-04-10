package dag

import (
	"context"
	"fmt"

	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
	"go.uber.org/zap"
)

// WalkDAGFunc is called for each node during a DAG walk.
//
// Parameters:
//   - ctx: Context for this specific node visitation
//   - c: The CID of the current node (normalized if NormalizeCID option is enabled)
//   - node: The IPLD node
//
// Returns:
//   - error: Error to abort the walk, or nil to continue to next node
type WalkDAGFunc func(ctx context.Context, c cid.Cid, node *merkledag.ProtoNode) error

// WalkDAGOptions configures the behavior of the DAG walk.
type WalkDAGOptions struct {
	// NormalizeCID determines whether to normalize CIDs to v1 format before processing.
	// When enabled, v0 CIDs are converted to v1 CIDs.
	NormalizeCID bool

	// Concurrent enables concurrent walking of the DAG.
	// This can significantly speed up walks for large DAGs but may increase resource usage.
	Concurrent bool

	// IgnoreErrors determines whether to continue walking on errors.
	// When true, errors returned by the callback are logged but don't stop the walk.
	// When false, the first error stops the walk immediately.
	IgnoreErrors bool

	// Logger is used for error logging when IgnoreErrors is true.
	Logger *core.Logger
}

// DefaultWalkDAGOptions returns sensible defaults for DAG walking.
// Defaults:
//   - NormalizeCID: true (for consistent CID representation)
//   - Concurrent: true (for performance on large DAGs)
//   - IgnoreErrors: false (stop on first error by default)
//   - Logger: nil (no logging unless provided)
func DefaultWalkDAGOptions() *WalkDAGOptions {
	return &WalkDAGOptions{
		NormalizeCID: true,
		Concurrent:   true,
		IgnoreErrors: false,
		Logger:       nil,
	}
}

// WalkDAG traverses a DAG (Directed Acyclic Graph) from the given root CID,
// visiting each node exactly once and calling the provided callback function.
//
// This helper centralizes common boilerplate for DAG traversal including:
//   - Session management
//   - CID deduplication/cycle detection
//   - CID normalization
//   - Concurrent walking support
//   - Error handling with option to continue or stop
//
// The walk performs a depth-first traversal starting from the root CID and follows
// all links found in the DAG nodes.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - dagService: DAG service for retrieving nodes
//   - root: Root CID of the DAG to walk
//   - callback: Function called for each node in the DAG
//   - opts: Configuration options (use DefaultWalkDAGOptions() for defaults)
//
// Returns:
//   - error: Error if the walk fails and IgnoreErrors is false, or if context is cancelled
//
// Example:
//
//	err := WalkDAG(ctx, dagService, root, func(ctx context.Context, c cid.Cid, node *merkledag.ProtoNode) error {
//	    fmt.Printf("Visited node: %s\n", c)
//	    return nil
//	}, DefaultWalkDAGOptions())
func WalkDAG(ctx context.Context, dagService format.DAGService, root cid.Cid, callback WalkDAGFunc, opts *WalkDAGOptions) error {
	if opts == nil {
		opts = DefaultWalkDAGOptions()
	}

	// Create a DAG session for efficient node retrieval
	sess := merkledag.NewSession(ctx, dagService)

	// Track visited CIDs to handle cycles and avoid duplicate processing
	seen := make(map[string]bool)

	// Build walk options
	var walkOpts []merkledag.WalkOption
	if opts.Concurrent {
		walkOpts = append(walkOpts, merkledag.Concurrent())
	}
	if opts.IgnoreErrors {
		walkOpts = append(walkOpts, merkledag.IgnoreErrors())
	}

	// Perform the walk
	err := merkledag.Walk(ctx, merkledag.GetLinksWithDAG(sess), root, func(c cid.Cid) bool {
		// Normalize CID if requested
		if opts.NormalizeCID {
			c = encoding.NormalizeCid(c)
		}

		// Get string key for deduplication
		key := c.String()

		// Skip if already visited
		if seen[key] {
			return false
		}

		// Mark as visited
		seen[key] = true

		// Retrieve the node
		node, err := sess.Get(ctx, c)
		if err != nil {
			if opts.IgnoreErrors && opts.Logger != nil {
				opts.Logger.Error("Failed to retrieve node during DAG walk",
					zap.Stringer("cid", c),
					zap.Error(err))
			}
			return !opts.IgnoreErrors
		}

		// Type assertion - merkledag.ProtoNode implements both ipld.Node and blocks.Block
		protoNode, ok := node.(*merkledag.ProtoNode)
		if !ok {
			if opts.IgnoreErrors && opts.Logger != nil {
				opts.Logger.Error("Node is not a ProtoNode",
					zap.Stringer("cid", c),
					zap.String("type", fmt.Sprintf("%T", node)))
			}
			return !opts.IgnoreErrors
		}

		// Call the callback
		if err := callback(ctx, c, protoNode); err != nil {
			if opts.IgnoreErrors && opts.Logger != nil {
				opts.Logger.Error("Callback error during DAG walk",
					zap.Stringer("cid", c),
					zap.Error(err))
				return true // Continue walking
			}
			return false // Stop walking
		}

		return true // Continue walking
	}, walkOpts...)

	return err
}
