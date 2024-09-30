package tasks

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/samber/lo"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
	"gorm.io/gorm"
	"io"
	"sync"
	"time"

	"github.com/golang-queue/queue"
	queueCore "github.com/golang-queue/queue/core"
	"github.com/google/uuid"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	pluginService "go.lumeweb.com/portal-plugin-ipfs/internal/service"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/models"
	"go.uber.org/zap"
)

type blockJob struct {
	Block     blocks.Block
	ParentCID cid.Cid
}

func (j *blockJob) Bytes() []byte {
	data, err := json.Marshal(j)
	if err != nil {
		// In a real-world scenario, you might want to handle this error more gracefully
		panic(fmt.Sprintf("Failed to marshal blockJob: %v", err))
	}
	return data
}

type blockJobJSON struct {
	BlockData []byte `json:"block_data"`
	BlockCid  string `json:"block_cid"`
	ParentCID string `json:"parent_cid"`
}

func (j *blockJob) MarshalJSON() ([]byte, error) {
	var parentCID string

	if j.ParentCID.Defined() {
		parentCID = j.ParentCID.String()
	}

	return json.Marshal(blockJobJSON{
		BlockData: j.Block.RawData(),
		BlockCid:  j.Block.Cid().String(),
		ParentCID: parentCID,
	})
}

func (j *blockJob) UnmarshalJSON(data []byte) error {
	var bjson blockJobJSON
	if err := json.Unmarshal(data, &bjson); err != nil {
		return err
	}

	blockCid, err := cid.Decode(bjson.BlockCid)
	if err != nil {
		return fmt.Errorf("invalid block CID: %w", err)
	}

	block, err := blocks.NewBlockWithCid(bjson.BlockData, blockCid)
	if err != nil {
		return fmt.Errorf("failed to create block: %w", err)
	}

	parentCid := cid.Undef

	if bjson.ParentCID != "" {
		parentCid, err = cid.Decode(bjson.ParentCID)
		if err != nil {
			return fmt.Errorf("invalid parent CID: %w", err)
		}
	}

	j.Block = block
	j.ParentCID = parentCid

	return nil
}

type blockProcessor struct {
	rootCIDs           map[string]bool
	processedPins      map[string]uuid.UUID
	pinExisted         map[string]bool
	pinLinks           map[string]string
	processedNodes     map[string]*internal.NodeInfo
	uploadService      *pluginService.UploadService
	requestService     core.RequestService
	pinService         core.PinService
	proto              *protocol.Protocol
	request            *models.Request
	logger             *core.Logger
	queue              *queue.Queue
	mu                 sync.RWMutex
	errChan            chan error
	nonCanceledErrChan chan error
	ctx                context.Context
	cancel             context.CancelFunc
	done               chan struct{}
}

func newBlockProcessor(coreCtx core.Context, pinService *pluginService.UploadService, proto *protocol.Protocol, request *models.Request, logger *core.Logger, rootCIDs []cid.Cid) *blockProcessor {
	ctx, cancel := context.WithCancel(coreCtx.GetContext())
	bp := &blockProcessor{
		rootCIDs:           lo.SliceToMap(rootCIDs, func(cid cid.Cid) (string, bool) { return string(cid.Bytes()), true }),
		processedPins:      make(map[string]uuid.UUID),
		pinExisted:         make(map[string]bool),
		pinLinks:           make(map[string]string),
		processedNodes:     make(map[string]*internal.NodeInfo),
		uploadService:      pinService,
		requestService:     core.GetService[core.RequestService](coreCtx, core.REQUEST_SERVICE),
		pinService:         core.GetService[core.PinService](coreCtx, core.PIN_SERVICE),
		proto:              proto,
		request:            request,
		logger:             logger,
		errChan:            make(chan error, 1),
		nonCanceledErrChan: make(chan error, 1),
		ctx:                ctx,
		cancel:             cancel,
		done:               make(chan struct{}),
	}

	q, err := queue.NewQueue(
		queue.WithWorker(queue.NewRing(queue.WithFn(bp.processBlock))),
		queue.WithWorkerCount(10),
		queue.WithLogger(NewZapLogAdapter(logger)),
	)
	if err != nil {
		logger.Error("Failed to create queue", zap.Error(err))
		return nil
	}

	bp.queue = q
	return bp
}

func (bp *blockProcessor) processBlock(ctx context.Context, msg queueCore.QueuedMessage) error {
	if err := bp.ctx.Err(); err != nil {
		return err
	}

	var job blockJob
	if err := json.Unmarshal(msg.Bytes(), &job); err != nil {
		return bp.handleError(fmt.Errorf("failed to deserialize blockJob: %w", err))
	}

	bp.logger.Debug("Processing block", zap.String("CID", job.Block.Cid().String()))

	cidStr := string(job.Block.Cid().Bytes())

	// Import the block
	err := bp.proto.GetNode().AddBlock(bp.ctx, job.Block)
	if err != nil {
		return bp.handleError(fmt.Errorf("failed to add block: %w", err))
	}

	// Analyze the node
	nodeInfo, err := internal.AnalyzeNode(bp.ctx, job.Block)
	if err != nil {
		return bp.handleError(fmt.Errorf("failed to analyze node: %w", err))
	}

	bp.processedNodes[cidStr] = nodeInfo

	// Check if this is a root block
	_, isRoot := bp.rootCIDs[cidStr]

	// Create pin
	var requestID *uuid.UUID
	if isRoot {
		data, err := bp.requestService.GetProtocolData(bp.ctx, bp.request.ID)
		if err != nil {
			return bp.handleError(err)
		}
		ipfsData := data.(*pluginDb.IPFSRequest)
		requestID = (*uuid.UUID)(&ipfsData.PinRequestID)
	}

	pin, existing, err := bp.uploadService.CreatePinnedPin(bp.ctx, job.Block.Cid(), bp.request.Operation, nodeInfo.Size, bp.request.UserID, bp.request.SourceIP, "", !isRoot, isRoot, requestID, nil)
	if err != nil {
		return bp.handleError(fmt.Errorf("failed to pin block: %w", err))
	}

	cidBytes := job.Block.Cid().Bytes()

	bp.mu.Lock()
	bp.processedPins[string(cidBytes)] = uuid.UUID(pin.PinRequestID)
	bp.pinExisted[string(cidBytes)] = existing

	node, err := encoding.DecodeBlock(ctx, job.Block)
	if err != nil {
		return fmt.Errorf("failed to decode block: %w", err)
	}

	for _, link := range node.Links() {
		bp.pinLinks[string(link.Cid.Bytes())] = string(cidBytes)
	}

	if isRoot {
		bp.pinLinks[string(job.Block.Cid().Bytes())] = string(cid.Undef.Bytes())
	}

	bp.mu.Unlock()

	return nil
}

func (bp *blockProcessor) linkPins() error {
	bp.mu.RLock()
	defer bp.mu.RUnlock()

	rootPinID := uuid.Nil

	for child, parent := range bp.pinLinks {
		childPinID, ok := bp.processedPins[child]
		if !ok {
			return fmt.Errorf("child pin not found for block %s", child)
		}

		cast, err := cid.Cast([]byte(child))
		if err != nil {
			return err
		}

		if bytes.Equal(cast.Hash(), bp.request.Hash) {
			rootPinID = childPinID
			continue
		}

		parentPinID, ok := bp.processedPins[parent]
		if !ok {
			return fmt.Errorf("parent pin not found for block %s", parent)
		}

		if bp.pinExisted[child] || parentPinID == uuid.Nil {
			continue
		}

		err = bp.uploadService.UpdatePinnedPinParent(bp.ctx, childPinID, parentPinID)
		if err != nil {
			bp.logger.Error("Failed to update child pin parent", zap.Error(err))
			return fmt.Errorf("failed to update child pin parent: %w", err)
		}
	}

	if rootPinID != uuid.Nil {
		err := bp.uploadService.PinRequestStatusPinned(bp.ctx, rootPinID)
		if err != nil {
			bp.logger.Error("Failed to update child pin status", zap.Error(err))
			return fmt.Errorf("failed to update child pin status: %w", err)
		}
	}

	return nil
}

func (bp *blockProcessor) handleError(err error) error {
	bp.logger.Error("Critical error occurred", zap.Error(err))
	if !isContextCanceled(err) {
		select {
		case bp.nonCanceledErrChan <- err:
		default:
			bp.logger.Error("Non-canceled error channel full, logging additional error", zap.Error(err))
		}
	}
	select {
	case bp.errChan <- err:
	default:
		bp.logger.Error("Error channel full, logging additional error", zap.Error(err))
	}
	bp.cancel()
	return err
}

func (bp *blockProcessor) queueBlock(job *blockJob) error {
	select {
	case <-bp.ctx.Done():
		return bp.ctx.Err()
	default:
		return bp.queue.Queue(job)
	}
}

func (bp *blockProcessor) start() {
	bp.queue.Start()
}

func (bp *blockProcessor) wait() error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-bp.done:
			bp.queue.Shutdown()
			return nil
		case err := <-bp.nonCanceledErrChan:
			bp.queue.Shutdown()
			return err
		case <-bp.ctx.Done():
			bp.queue.Shutdown()
			if !isContextCanceled(bp.ctx.Err()) {
				return bp.ctx.Err()
			}
			return nil
		case <-ticker.C:
			if bp.queue.SubmittedTasks() == bp.queue.SuccessTasks()+bp.queue.FailureTasks() {
				close(bp.done)
			}
		}
	}
}

func (bp *blockProcessor) release() {
	bp.cancel()
	bp.queue.Release()
}

func (bp *blockProcessor) detectPartialBlocks() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	for cidStr, _ := range bp.rootCIDs {
		if _, ok := bp.processedNodes[cidStr]; !ok {
			bp.logger.Error("Root node not found", zap.String("CID", cidStr))
			continue
		}

		nodeInfo := bp.processedNodes[cidStr]

		if internal.IsPartialFile(nodeInfo) {
			continue
		}

		hash := internal.NewIPFSHash(nodeInfo.CID)

		pin, err := bp.pinService.QueryProtocolPin(bp.ctx, internal.ProtocolName, nil, core.PinFilter{Protocol: internal.ProtocolName, Hash: hash})
		if err != nil {
			return err
		}

		if pin == nil {
			bp.logger.Error("Pin not found", zap.String("CID", cidStr))
		}

		pinData := pin.(*pluginDb.IPFSPin)

		if err = bp.uploadService.UpdatePartialStatus(bp.ctx, pinData.PinID, false); err != nil {
			return err
		}

		bp.logger.Debug("Updated root node to complete", zap.String("CID", cidStr))
	}

	for cidStr, nodeInfo := range bp.processedNodes {
		if _, isRoot := bp.rootCIDs[cidStr]; !isRoot {
			hash := internal.NewIPFSHash(nodeInfo.CID)

			pin, err := bp.pinService.QueryProtocolPin(bp.ctx, internal.ProtocolName, nil, core.PinFilter{Protocol: internal.ProtocolName, Hash: hash})
			if err != nil {
				return err
			}

			if pin == nil {
				return errors.New("pin not found")
			}

			pinData := pin.(*pluginDb.IPFSPin)

			// Determine if it's a partial file
			isPartial := internal.IsPartialFile(nodeInfo)

			if nodeInfo.Type == internal.NodeTypeRaw && bp.pinLinks[cidStr] != "" {
				isPartial = true
			}

			if err = bp.uploadService.UpdatePartialStatus(bp.ctx, pinData.PinID, isPartial); err != nil {
				return err
			}

			bp.logger.Debug("Updated root node to complete", zap.String("CID", cidStr))
		}
	}

	return nil
}

func processCar(ctx core.Context, r io.Reader, request *models.Request) ([]cid.Cid, error) {
	pinService := core.GetService[*pluginService.UploadService](ctx, pluginService.UPLOAD_SERVICE)
	requestService := core.GetService[core.RequestService](ctx, core.REQUEST_SERVICE)
	proto := core.GetProtocol(internal.ProtocolName).(*protocol.Protocol)
	logger := ctx.Logger()

	processedCIDs := make([]cid.Cid, 0)

	cr, err := car.NewBlockReader(r)
	if err != nil {
		logger.Error("Failed to create block reader", zap.Error(err))
		return nil, fmt.Errorf("failed to create block reader: %w", err)
	}

	rootCIDs := cr.Roots

	duplicates := false

	for _, rootCid := range rootCIDs {
		hash := internal.NewIPFSHash(rootCid)
		req, err := requestService.QueryRequest(ctx, &models.Request{
			Hash: hash.Multihash(), HashType: hash.Type(),
			Status: models.RequestStatusCompleted,
		}, core.RequestFilter{
			Protocol:  internal.ProtocolName,
			Operation: models.RequestOperationUpload,
			UserID:    request.UserID,
		})
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				continue
			}

			return nil, fmt.Errorf("failed to query request: %w", err)
		}

		err = requestService.UpdateRequestStatus(ctx, req.ID, models.RequestStatusDuplicate)
		if err != nil {
			return nil, err
		}

		duplicates = true
	}

	if duplicates {
		return nil, core.ErrDuplicateRequest
	}

	bp := newBlockProcessor(ctx, pinService, proto, request, logger, rootCIDs)
	if bp == nil {
		return nil, fmt.Errorf("failed to create block processor")
	}

	bp.start()
	defer bp.release()

	err = pinService.PinRequestStatusPinning(ctx, request.ID)
	if err != nil {
		return nil, err
	}

	// Read all blocks
	for {
		block, err := cr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read block: %w", err)
		}

		job := &blockJob{
			Block: block,
		}

		if err := bp.queueBlock(job); err != nil {
			if !isContextCanceled(err) {
				return nil, fmt.Errorf("failed to queue block: %w", err)
			}
			go bp.handleError(err)
			break
		}

		processedCIDs = append(processedCIDs, block.Cid())
	}

	// Wait for all blocks to be processed
	if err := bp.wait(); err != nil {
		return nil, fmt.Errorf("block processing failed: %w", err)
	}

	// Link pins
	if err := bp.linkPins(); err != nil {
		return nil, fmt.Errorf("failed to link pins: %w", err)
	}

	// Finalize processing
	if err := bp.detectPartialBlocks(); err != nil {
		return nil, fmt.Errorf("failed to detect partial blocks: %w", err)
	}

	return processedCIDs, nil
}
func isContextCanceled(err error) bool {
	return err == context.Canceled || err == context.DeadlineExceeded
}
