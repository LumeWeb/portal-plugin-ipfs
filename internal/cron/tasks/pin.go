package tasks

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/go-cid"
	billingPluginService "go.lumeweb.com/portal-plugin-billing/service"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/cron/define"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/store"
	pluginService "go.lumeweb.com/portal-plugin-ipfs/internal/service"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
	"strings"
)

func CronTaskPin(args *define.CronTaskPinArgs, ctx core.Context) error {
	logger := ctx.Logger()
	logger.Info("Starting pin import task")

	ipfs := core.GetProtocol(internal.ProtocolName).(*protocol.Protocol)
	uploadService := ctx.Service(pluginService.UPLOAD_SERVICE).(*pluginService.UploadService)

	pin, err := uploadService.GetPinByIdentifier(ctx, args.RequestID, 0)
	if err != nil || pin == nil {
		logger.Error("Failed to get pin", zap.Error(err))
		if pin == nil {
			err = fmt.Errorf("pin not found")
		}
		return err
	}

	if core.ServiceExists(ctx, billingPluginService.QUOTA_SERVICE) {
		// Check if this is a root pin (no parent)
		if pin.ParentPinRequestID == nil {
			// Calculate total file size
			totalSize, err := calculateTotalFileSize(ctx, ipfs, pin.Hash, pin.CIDType)
			if err != nil {
				logger.Error("Failed to calculate total file size", zap.Error(err))
				return err
			}

			quotaService := core.GetService[billingPluginService.QuotaService](ctx, billingPluginService.QUOTA_SERVICE)
			allowed, err := quotaService.CheckDownloadQuota(pin.UserID, totalSize)
			if err != nil {
				return err
			}

			if !allowed {
				err := uploadService.PinRequestStatusFailed(ctx, args.RequestID)
				if err != nil {
					return err
				}
			}
		}
	}

	// Update status to Pinning
	if err := uploadService.PinRequestStatusPinning(ctx, args.RequestID); err != nil {
		logger.Error("Failed to update import status to Pinning", zap.Error(err))
		return err
	}

	// Get the node
	c, err := internal.CIDFromHash(pin.Hash, pin.CIDType)
	if err != nil {
		logger.Error("Failed to cast hash to CID", zap.Error(err))
		return err
	}

	logger.Debug("Trying to import block", zap.String("CID", c.String()))
	getCtx, cancel := context.WithTimeout(ctx, ctx.Config().GetProtocol(internal.ProtocolName).(*pluginConfig.Config).BlockStore.Timeout)
	node, err := ipfs.GetNode().GetBlock(getCtx, c)
	cancel()
	if err != nil {
		logger.Error("Failed to get node", zap.Error(err))
		err2 := uploadService.PinRequestStatusFailed(ctx, args.RequestID)
		if err2 != nil {
			return err2
		}
		if isRecoverableNodeError(err) {
			return fmt.Errorf("failed to store block: %w", err)
		}

		return nil
	}

	err = uploadService.CompletePin(ctx, pin, node)
	if err != nil {
		logger.Error("Failed to complete pin", zap.Error(err))
		return err
	}

	// If this is a ProtoNode, create child imports for its children
	if protoNode, ok := node.(*merkledag.ProtoNode); ok {
		for _, link := range protoNode.Links() {
			childCID := link.Cid
			logger.Debug("Creating child import", zap.String("childCID", childCID.String()))

			_, err = uploadService.CreateQueuedPin(ctx, childCID, pin.UserID, pin.UploaderIP, pin.Name, true, &args.RequestID, true)
			if err != nil {
				logger.Error("Failed to create child import", zap.Error(err), zap.String("childCID", childCID.String()))
				return err
			}
		}
	}

	// Detect and update partial status
	if err = uploadService.DetectUpdatePartialStatus(ctx, node); err != nil {
		logger.Error("Failed to detect and/or update partial status", zap.Error(err))
		return err
	}

	// Update status to Pinned for this node
	if err = uploadService.PinRequestStatusPinned(ctx, args.RequestID); err != nil {
		logger.Error("Failed to update import status to Pinned", zap.Error(err))
		return err
	}

	// Update progress for all ancestors
	if err = updateAncestorProgress(ctx, uploadService, args.RequestID); err != nil {
		logger.Error("Failed to update ancestor progress", zap.Error(err))
		return err
	}

	logger.Info("Pin import task completed successfully")

	return nil
}

func calculateTotalFileSize(ctx core.Context, ipfs *protocol.Protocol, hash []byte, cidType uint64) (uint64, error) {
	c, err := internal.CIDFromHash(hash, cidType)
	if err != nil {
		return 0, fmt.Errorf("failed to cast hash to CID: %w", err)
	}

	var totalSize uint64
	visited := make(map[string]bool)

	err = traverseDAG(ctx, ipfs, c, visited, &totalSize)
	if err != nil {
		return 0, fmt.Errorf("failed to traverse DAG: %w", err)
	}

	return totalSize, nil
}

func traverseDAG(ctx core.Context, ipfs *protocol.Protocol, c cid.Cid, visited map[string]bool, totalSize *uint64) error {
	if visited[c.String()] {
		return nil
	}
	visited[c.String()] = true

	virtualCtx := store.VirtualReadOption(ctx, true)
	getCtx, cancel := context.WithTimeout(virtualCtx, ctx.Config().GetProtocol(internal.ProtocolName).(*pluginConfig.Config).BlockStore.Timeout)
	block, err := ipfs.GetNode().GetBlock(getCtx, c)
	cancel()
	if err != nil {
		return fmt.Errorf("failed to get block: %w", err)
	}

	size := uint64(len(block.RawData()))
	*totalSize += size

	// Check if it's a ProtoNode and traverse its links
	node, err := encoding.DecodeBlock(ctx, block)
	if err != nil {
		return fmt.Errorf("failed to decode block: %w", err)
	}

	if protoNode, ok := node.(*merkledag.ProtoNode); ok {
		for _, link := range protoNode.Links() {
			err = traverseDAG(ctx, ipfs, link.Cid, visited, totalSize)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func updateAncestorProgress(ctx context.Context, uploadService *pluginService.UploadService, requestID uuid.UUID) error {
	for {
		pin, err := uploadService.GetPinByIdentifier(ctx, requestID, 0)
		if err != nil || pin == nil {
			if pin == nil {
				err = fmt.Errorf("pin not found")
			}
			return err
		}

		if pin.ParentPinRequestID == nil {
			// This is the root node, we're done
			return nil
		}

		// Check if all siblings are complete
		siblings, err := uploadService.GetChildPins(ctx, uuid.UUID(*pin.ParentPinRequestID))
		if err != nil {
			return err
		}

		allComplete := true
		for _, sibling := range siblings {
			if internal.RequestStatusToPinStatus(sibling.Status) != db.PinningStatusPinned {
				allComplete = false
				break
			}
		}

		if allComplete {
			// Update parent status to Pinned
			if err = uploadService.PinRequestStatusPinned(ctx, uuid.UUID(*pin.ParentPinRequestID)); err != nil {
				return err
			}
			// Continue with the parent
			requestID = uuid.UUID(*pin.ParentPinRequestID)
		} else {
			// Not all siblings are complete, stop here
			return nil
		}
	}
}

func isRecoverableNodeError(err error) bool {
	return !strings.Contains(err.Error(), "protobuf:")
}
