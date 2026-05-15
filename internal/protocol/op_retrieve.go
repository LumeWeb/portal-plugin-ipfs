package protocol

import (
	"context"
	"fmt"
	"strings"

	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/go-cid"
	"github.com/samber/lo"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/dag"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/store"
	pc "go.lumeweb.com/portal-plugin-ipfs/internal/protocol/context"
	"go.lumeweb.com/portal-plugin-ipfs/internal/quota"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	quotaCore "go.lumeweb.com/portal-plugin-quota/core"
	"go.uber.org/zap"
)

// RetrieveOperationHandler handles fetching content from the IPFS network
type RetrieveOperationHandler struct {
	core.OperationHelper
}

func (h *RetrieveOperationHandler) ValidateRequest(_ context.Context, req *models.Request) error {
	if len(req.Hash) == 0 {
		return fmt.Errorf("hash is required")
	}
	return nil
}

func (h *RetrieveOperationHandler) Execute(ctx context.Context, req *models.Request) error {
	ctx, span := core.TraceMethod(ctx, "RetrieveOperationHandler.Execute")
	defer span.End()

	// Initialize progress tracker with manual mode for simple milestones
	tracker, err := InitializeManualProgressTracker(h, req.ID, core.OpTypeRetrieve, 10)
	if err != nil {
		return err
	}

	var workflowData PinWorkflowData
	err = h.StructuredWorkflowData(req.ID, &workflowData)
	if err != nil {
		return err
	}

	c, err := internal.CIDFromHash(req.Hash, req.CIDType)
	if err != nil {
		return fmt.Errorf("failed to create CID: %w", err)
	}

	protoCfg := h.Context().Config().GetProtocol(internal.ProtocolName).(*pluginConfig.ProtocolConfig)
	proto := h.Protocol().(ProtoNode)
	metadataStore := proto.GetMetadataStore()

	// Store quota check results for reservation management
	checkResults := &quota.QuotaCheckResults{}
	reservations := make(map[cid.Cid]*quota.BlockReservations)

	// ==== VIRTUAL DISCOVERY PHASE ====
	// Collect DAG CIDs and sizes virtually (no storage, no quota checking)
	// This provides metadata for quota calculations before committing
	dagResult, err := collectDAGCids(h.Context(), proto, c, true)
	if err != nil {
		return fmt.Errorf("failed to collect DAG CIDs virtually: %w", err)
	}

	h.setProgressOrWarn(tracker, 30)

	// Filter out the root CID to get only child CIDs
	childCids := lo.Filter(dagResult.Cids, func(item cid.Cid, _ int) bool {
		return !item.Equals(c)
	})

	// Check download quota for the root block size
	if req.UserID != nil && *req.UserID > 0 {
		checkResult, err := quota.CheckWithReservation(ctx, h.Context(), quota.CheckTypeDownload, *req.UserID, dagResult.CIDSizes[c], quota.CheckDownloadQuota)
		if err != nil {
			return err
		}
		checkResults.Download = checkResult
	}

	if len(childCids) > 0 {
		// Validate user ID before processing
		if err := h.validateUserID(req.UserID, checkResults.Download); err != nil {
			return err
		}

		// Calculate total child size using DAG result (faster than loop)
		childTotalSize := dagResult.TotalSize - dagResult.CIDSizes[c]

		// Quick non-reservation quota check for the cumulative size
		// This provides an early failure if the user doesn't have enough quota
		if childTotalSize > 0 {
			err = quota.ValidateUploadQuota(ctx, h.Context(), *req.UserID, childTotalSize)
			if err != nil {
				cleanupDownloadReservation(checkResults.Download)
				return err
			}
		}

		// Create per-block reservations for upload and storage quota using cached sizes
		// This ensures each block has its own reservation for accurate tracking
		childCidSet := lo.SliceToMap(childCids, func(c cid.Cid) (cid.Cid, struct{}) {
			return c, struct{}{}
		})
		childSizes := lo.PickBy(dagResult.CIDSizes, func(c cid.Cid, size uint64) bool {
			return size > 0 && lo.HasKey(childCidSet, c)
		})

		reservations, err = CreatePerBlockReservationsWithSizes(ctx, h.Context(), *req.UserID, childSizes)
		if err != nil {
			cleanupDownloadReservation(checkResults.Download)
			return err
		}
	}

	h.setProgressOrWarn(tracker, 50)

	// ==== ACTUAL COLLECTION PHASE (ONLY IF RESERVATIONS PASS) ====
	if len(childCids) > 0 {
		// Collect and store the actual blocks now that reservations are secured
		// This performs the real fetch and storage to blockstore
		// The DAG service automatically stores blocks when not in virtual mode
		dagResult, err = collectDAGCids(h.Context(), proto, c, false)
		if err != nil {
			h.Logger().Error("Failed to collect and store DAG CIDs", zap.Error(err))
			cleanupDownloadReservation(checkResults.Download)
			return fmt.Errorf("failed to collect and store DAG CIDs: %w", err)
		}

		// Flush buffered metadata to the database before any DB lookups.
		// collectDAGCids stores blocks via BlockStore.Put → batcher.Add,
		// but the batcher only auto-flushes at batch-size boundaries.
		// Without an explicit flush, subsequent ProcessMissingUnixFSNames
		// and quota lookups will fail with "record not found".
		if flusher := proto.GetBlockstoreFlusher(); flusher != nil {
			if err := flusher.Flush(ctx); err != nil {
				h.Logger().Error("Failed to flush block metadata", zap.Error(err))
			}
		}

		h.setProgressOrWarn(tracker, 70)

		// UnixFS name processing (NOW AFTER blocks are in blockstore)
		if metadataStore != nil {
			err = metadataStore.ProcessMissingUnixFSNames(ctx, dagResult.Cids)
			if err != nil {
				h.Logger().Warn("Failed to process missing UnixFS names", zap.Error(err))
			}
		}

		// Include both parent and child CIDs in the workflow data
		allCids := append([]cid.Cid{c}, childCids...)
		workflowData.Cids = lo.Map(allCids, func(item cid.Cid, index int) string {
			return item.String()
		})

		err = h.UpdateWorkflowDataStruct(req.ID, &workflowData)
		if err != nil {
			h.Logger().Error("Failed to update workflow data", zap.Error(err))
			quota.ReleaseBlockReservationsMap(reservations)
			cleanupDownloadReservation(checkResults.Download)
			return err
		}

		h.setProgressOrWarn(tracker, 90)
	}

	// ==== UPLOAD PROCESSING ====
	if len(childCids) > 0 && reservations != nil {
		uploadSvc := core.GetService[pluginCore.UploadService](h.Context(), pluginCore.UPLOAD_SERVICE)
		if uploadSvc == nil {
			h.Logger().Error("Upload service not available")
			quota.ReleaseBlockReservationsMap(reservations)
			cleanupDownloadReservation(checkResults.Download)
			return fmt.Errorf("upload service not available")
		}

		// Set client IP in context for quota tracking
		ctx = pc.ClientIPOption(ctx, req.SourceIP)

		// Fetch and prepare all child blocks metadata before processing
		// Blocks are already in blockstore from actual collection phase
		var validChildCids []cid.Cid
		getCtx, cancel := context.WithTimeout(ctx, protoCfg.BlockStore.Timeout)

		// Skip quota check for internal retrieve operations
		getCtx = pc.SkipQuotaCheckOption(getCtx, true)

		for _, childCid := range childCids {
			if _, ok := reservations[childCid]; !ok {
				// Skip CIDs without reservations
				continue
			}

			if h.fetchAndPrepareChildBlock(getCtx, proto, metadataStore, childCid) {
				validChildCids = append(validChildCids, childCid)
			}
		}
		cancel()

		err = uploadSvc.ProcessUpload(ctx, validChildCids, *req.UserID, reservations)
		if err != nil {
			h.Logger().Error("Failed to batch process child blocks", zap.Error(err))
			quota.ReleaseBlockReservationsMap(reservations)
			cleanupDownloadReservation(checkResults.Download)
			return err
		}
	}

	h.setProgressOrWarn(tracker, 100)

	return nil
}

func (h *RetrieveOperationHandler) GetStatus(_ context.Context, req *models.Request) (*core.RequestStatus, error) {
	return h.GetStatusFromWorkflowData(req.ID, req)
}

func (h *RetrieveOperationHandler) Cleanup(_ context.Context, _ *models.Request) error {
	// No cleanup needed for retrieve operation
	return nil
}

func NewRetrieveOperation(ctx core.Context) core.Operation {
	return core.NewRetrieveOperation(internal.ProtocolName, &RetrieveOperationHandler{
		OperationHelper: core.NewProtocolOperationHelper(ctx, internal.ProtocolName),
	})
}

// setProgressOrWarn is a helper to set progress and log warnings
func (h *RetrieveOperationHandler) setProgressOrWarn(tracker *core.ProgressTracker, progress float64) {
	if err := tracker.SetProgress(progress); err != nil {
		h.Logger().Warn("Failed to update progress", zap.Error(err))
	}
}

// cleanupDownloadReservation releases download reservation if it exists
func cleanupDownloadReservation(downloadReservation *quotaCore.QuotaCheckResult) {
	if downloadReservation != nil {
		downloadReservation.ReleaseReservation()
	}
}

// cleanupReservations releases the download reservation
func cleanupReservations(downloadReservation *quotaCore.QuotaCheckResult) {
	cleanupDownloadReservation(downloadReservation)
}

// fetchAndPrepareChildBlock fetches a child block and updates UnixFS metadata
// Returns true if successful, false if the block should be skipped
func (h *RetrieveOperationHandler) fetchAndPrepareChildBlock(
	ctx context.Context,
	proto ProtoNode,
	metadataStore pluginCore.MetadataStore,
	childCid cid.Cid,
) bool {
	block, err := proto.GetNode().GetBlock(ctx, childCid)
	if err != nil {
		h.Logger().Error("Failed to fetch child block", zap.Stringer("cid", childCid), zap.Error(err))
		return false
	}

	// Update UnixFS metadata
	if metadataStore != nil {
		pinnedBlock := pluginCore.PinnedBlock{
			Cid:  childCid,
			Node: block,
			Size: uint64(len(block.RawData())),
		}
		unixFSNode, err := store.ExtractNodeMetadata(h.Logger(), pinnedBlock)
		if err == nil {
			if err := metadataStore.UpdateUnixFSMetadata(childCid, unixFSNode); err != nil {
				h.Logger().Warn("Failed to update UnixFS metadata", zap.Stringer("cid", childCid), zap.Error(err))
			}
		}
	}

	return true
}

// validateUserID validates that a user ID is provided and releases download reservation on failure
func (h *RetrieveOperationHandler) validateUserID(userID *uint, downloadReservation *quotaCore.QuotaCheckResult) error {
	if userID == nil || *userID == 0 {
		cleanupDownloadReservation(downloadReservation)
		return fmt.Errorf("user ID is required")
	}
	return nil
}

func isRecoverableNodeError(err error) bool {
	return !strings.Contains(err.Error(), "protobuf:")
}

// DAGCollectResult holds the results of a DAG collection operation
type DAGCollectResult struct {
	Cids          []cid.Cid           // All CIDs in the DAG (normalized)
	TotalSize     uint64              // Total size of all blocks in bytes
	CIDSizes      map[cid.Cid]uint64  // Size of each individual block
}

// collectDAGCids walks the DAG to collect all CIDs and their sizes
// If virtualOnly is true, performs a virtual read (no quota checking, no storage)
// If virtualOnly is false, fetches and stores blocks automatically via DAG service
func collectDAGCids(
	ctx core.Context,
	ipfs ProtoNode,
	c cid.Cid,
	virtualOnly bool,
) (*DAGCollectResult, error) {
	// Set virtual mode based on parameter
	var getCtx context.Context
	getCtx = ctx
	if virtualOnly {
		getCtx = pc.VirtualReadOption(ctx, true)
	}
	
	// Skip quota check for internal retrieve operations
	getCtx = pc.SkipQuotaCheckOption(getCtx, true)
	var cancel context.CancelFunc
	getCtx, cancel = context.WithTimeout(getCtx, ctx.Config().GetProtocol(internal.ProtocolName).(*pluginConfig.ProtocolConfig).BlockStore.Timeout)

	var cids []cid.Cid
	cidSizes := make(map[cid.Cid]uint64)
	var totalSize uint64

	opts := &dag.WalkDAGOptions{
		NormalizeCID: true,
		Concurrent:   true,
		IgnoreErrors: true,
		Logger:       ipfs.Logger(),
	}

	err := dag.WalkDAG(getCtx, ipfs.GetNode().DagService(), c, func(_ context.Context, cid cid.Cid, node *merkledag.ProtoNode) error {
		// Get block size from the node
		blockSize := uint64(len(node.RawData()))
		cidSizes[cid] = blockSize
		totalSize += blockSize
		
		cids = append(cids, cid)
		
		return nil
	}, opts)

	cancel()

	return &DAGCollectResult{
		Cids:      cids,
		TotalSize: totalSize,
		CIDSizes:  cidSizes,
	}, err
}
