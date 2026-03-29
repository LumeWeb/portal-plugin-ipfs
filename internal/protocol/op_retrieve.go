package protocol

import (
	"context"
	"fmt"
	"strings"

	"github.com/ipfs/boxo/ipld/merkledag"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/labstack/gommon/log"
	"github.com/samber/lo"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/store"
	pc "go.lumeweb.com/portal-plugin-ipfs/internal/protocol/context"
	"go.lumeweb.com/portal-plugin-ipfs/internal/quota"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
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

	// Get block for quota validation and reuse for downstream processing
	var block blocks.Block
	var blockSize uint64

	protoCfg := h.Context().Config().GetProtocol(internal.ProtocolName).(*pluginConfig.ProtocolConfig)
	getCtx, cancel := context.WithTimeout(ctx, protoCfg.BlockStore.Timeout)
	defer cancel()

	// Skip quota check for internal retrieve operations
	getCtx = pc.SkipQuotaCheckOption(getCtx, true)

	proto := h.Protocol().(*Protocol)
	block, err = proto.GetNode().GetBlock(getCtx, c)
	if err != nil {
		return fmt.Errorf("failed to get block: %w", err)
	}

	blockSize = uint64(len(block.RawData()))

	// Check download quota if user ID is available
	if req.UserID != nil && *req.UserID > 0 {
		// Validate download quota using the already fetched block
		err = quota.ValidateDownloadQuota(ctx, h.Context(), *req.UserID, blockSize)
		if err != nil {
			h.Logger().Warn("Download quota exceeded", zap.Uint("user_id", *req.UserID), zap.Uint64("block_size", blockSize), zap.Error(err))
			return err
		}
	}

	cids, err := collectDAGCids(h.Context(), proto, c)
	if err != nil {
		return fmt.Errorf("failed to collect cids: %w", err)
	}

	// Set progress - collecting DAG CIDs
	if err := tracker.SetProgress(30); err != nil {
		h.Logger().Warn("Failed to update progress", zap.Error(err))
	}

	childCids := lo.Filter(cids, func(item cid.Cid, _ int) bool {
		return !item.Equals(c)
	})

	// Fix any UnixFS metadata gaps before proceeding with child block processing
	_store := proto.GetMetadataStore()
	if _store != nil {
		err = _store.ProcessMissingUnixFSNames(ctx, cids)
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
		return err
	}

	// Set progress - updating workflow data
	if err := tracker.SetProgress(50); err != nil {
		h.Logger().Warn("Failed to update progress", zap.Error(err))
	}

	if len(childCids) > 0 {
		// Set progress - processing child blocks
		if err := tracker.SetProgress(70); err != nil {
			h.Logger().Warn("Failed to update progress", zap.Error(err))
		}

		uploadSvc := core.GetService[pluginCore.UploadService](h.Context(), pluginCore.UPLOAD_SERVICE)
		if uploadSvc == nil {
			h.Logger().Error("Upload service not available")
			return fmt.Errorf("upload service not available")
		}

		// Prepare all child blocks and metadata first
		var validChildCids []cid.Cid
		for _, childCid := range childCids {
			// Skip quota check for internal retrieve operations
			childGetCtx := pc.SkipQuotaCheckOption(getCtx, true)
			block, err := proto.GetNode().GetBlock(childGetCtx, childCid)
			if err != nil {
				h.Logger().Error("Failed to fetch child block", zap.Stringer("cid", childCid), zap.Error(err))
				continue
			}

			// Update UnixFS metadata
			if _store != nil {
				pinnedBlock := pluginCore.PinnedBlock{
					Cid:  childCid,
					Node: block,
					Size: uint64(len(block.RawData())),
				}
				unixFSNode, err := store.ExtractNodeMetadata(h.Logger(), pinnedBlock)
				if err == nil {
					if err := _store.UpdateUnixFSMetadata(childCid, unixFSNode); err != nil {
						h.Logger().Warn("Failed to update UnixFS metadata", zap.Stringer("cid", childCid), zap.Error(err))
					}
				}
			}

			validChildCids = append(validChildCids, childCid)
		}

		// Batch process all valid child CIDs
		if len(validChildCids) > 0 {
			// Validate user ID before processing
			if req.UserID == nil || *req.UserID == 0 {
				return fmt.Errorf("user ID is required")
			}

			// Set client IP in context for quota tracking
			ctx = pc.ClientIPOption(ctx, req.SourceIP)

			err = uploadSvc.ProcessUpload(ctx, validChildCids, *req.UserID)
			if err != nil {
				h.Logger().Error("Failed to batch process child blocks", zap.Error(err))
			}
		}
	}

	// Complete
	if err := tracker.SetProgress(100); err != nil {
		h.Logger().Warn("Failed to update progress", zap.Error(err))
	}

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

func isRecoverableNodeError(err error) bool {
	return !strings.Contains(err.Error(), "protobuf:")
}

func collectDAGCids(ctx core.Context, ipfs *Protocol, c cid.Cid) ([]cid.Cid, error) {

	getCtx := pc.VirtualReadOption(ctx, true)
	// Skip quota check for internal retrieve operations
	getCtx = pc.SkipQuotaCheckOption(getCtx, true)
	getCtx, cancel := context.WithTimeout(getCtx, ctx.Config().GetProtocol(internal.ProtocolName).(*pluginConfig.ProtocolConfig).BlockStore.Timeout)

	sess := merkledag.NewSession(getCtx, ipfs.GetNode().DagService())
	seen := make(map[string]bool)
	var cids []cid.Cid
	err := merkledag.Walk(getCtx, merkledag.GetLinksWithDAG(sess), c, func(c cid.Cid) bool {
		c = encoding.NormalizeCid(c)
		key := c.String()
		if seen[key] {
			return false
		}
		_, err := sess.Get(getCtx, c)
		if err != nil {
			log.Error("failed to get node", zap.Error(err))
			return false
		}
		if !seen[key] {
			cids = append(cids, c)
		}
		seen[key] = true
		return true
	}, merkledag.Concurrent(), merkledag.IgnoreErrors())

	cancel()

	return cids, err
}
