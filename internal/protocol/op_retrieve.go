package protocol

import (
	"context"
	"fmt"
	"strings"

	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/go-cid"
	"github.com/labstack/gommon/log"
	"github.com/samber/lo"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/store"
	"go.lumeweb.com/portal-plugin-ipfs/internal/quota"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	"go.lumeweb.com/portal/db/types"
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
	var workflowData PinWorkflowData
	err := h.StructuredWorkflowData(req.ID, &workflowData)
	if err != nil {
		return err
	}

	c, err := internal.CIDFromHash(req.Hash, req.CIDType)
	if err != nil {
		return fmt.Errorf("failed to create CID: %w", err)
	}

	// Check download quota if user ID is available
	if req.UserID != nil && *req.UserID > 0 {
		// Get block size for quota validation
		protoCfg := h.Context().Config().GetProtocol(internal.ProtocolName).(*pluginConfig.ProtocolConfig)
		getCtx, cancel := context.WithTimeout(ctx, protoCfg.BlockStore.Timeout)
		defer cancel()
		
		proto := h.Protocol().(*Protocol)
		block, err := proto.GetNode().GetBlock(getCtx, c)
		if err != nil {
			return fmt.Errorf("failed to get block for quota validation: %w", err)
		}
		
		blockSize := uint64(len(block.RawData()))
		
		// Validate download quota
		err = quota.ValidateDownloadQuota(h.Context(), *req.UserID, blockSize)
		if err != nil {
			h.Logger().Warn("Download quota exceeded", zap.Uint("user_id", *req.UserID), zap.Uint64("block_size", blockSize), zap.Error(err))
			return err
		}
	}

	protoCfg := h.Context().Config().GetProtocol(internal.ProtocolName).(*pluginConfig.ProtocolConfig)

	getCtx, cancel := context.WithTimeout(ctx, protoCfg.BlockStore.Timeout)
	proto := h.Protocol().(*Protocol)
	_, err = proto.GetNode().GetBlock(getCtx, c)
	cancel()
	if err != nil {
		h.Logger().Error("Failed to get node", zap.Error(err))
		pinSvc := core.GetService[pluginCore.IPFSPinService](h.Context(), pluginCore.PIN_SERVICE)
		updateErr := pinSvc.UpdatePinStatus(ctx, types.FromUUID(workflowData.PinRequestID), db.PinningStatusFailed, nil)
		if updateErr != nil {
			return updateErr
		}
		if isRecoverableNodeError(err) {
			return fmt.Errorf("failed to store block: %w", err)
		}

		return nil
	}

	cids, err := collectDAGCids(h.Context(), proto, c)
	if err != nil {
		return fmt.Errorf("failed to collect cids: %w", err)
	}

	// Fetch the block from the network
	block, err := h.Protocol().(*Protocol).GetNode().GetBlock(ctx, c)
	if err != nil {
		return fmt.Errorf("failed to get block: %w", err)
	}

	// Emit download completed event for quota tracking
	if req.UserID != nil && *req.UserID > 0 {
		blockSize := uint64(len(block.RawData()))
		quota.EmitDownloadCompleted(h.Context(), req.ID, blockSize, "")
	}

	childCids := lo.Filter(cids, func(item cid.Cid, _ int) bool {
		return !item.Equals(c)
	})

	// Fix any UnixFS metadata gaps before proceeding with child block processing
	_store := proto.GetMetadataStore()
	if _store != nil {
		err = _store.ProcessMissingUnixFSNames(cids)
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

	if len(childCids) > 0 {
		uploadSvc := core.GetService[pluginCore.UploadService](h.Context(), pluginCore.UPLOAD_SERVICE)
		if uploadSvc == nil {
			h.Logger().Error("Upload service not available")
			return fmt.Errorf("upload service not available")
		}

		// Prepare all child blocks and metadata first
		var validChildCids []cid.Cid
		for _, childCid := range childCids {
			block, err := proto.GetNode().GetBlock(ctx, childCid)
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

			err = uploadSvc.ProcessUpload(ctx, validChildCids, *req.UserID)
			if err != nil {
				h.Logger().Error("Failed to batch process child blocks", zap.Error(err))
			}
		}
	}

	return nil
}

func (h *RetrieveOperationHandler) GetStatus(ctx context.Context, req *models.Request) (*core.RequestStatus, error) {
	// For now just return a simple status since retrieval is synchronous
	status := &core.RequestStatus{
		ProgressPercent: 100,
	}

	if req.Status == models.RequestStatusCompleted {
		status.Message = "Content retrieved from network"
		status.ProgressPercent = 100
	}

	return status, nil
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

	getCtx := store.VirtualReadOption(ctx, true)
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
