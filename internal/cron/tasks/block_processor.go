package tasks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
	"io"
	"sync"
	"time"

	"github.com/golang-queue/queue"
	queueCore "github.com/golang-queue/queue/core"
	"github.com/google/uuid"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/samber/lo"
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
	IsRoot    bool
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
	IsRoot    bool   `json:"is_root"`
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
		IsRoot:    j.IsRoot,
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
	j.IsRoot = bjson.IsRoot
	j.ParentCID = parentCid

	return nil
}

type blockProcessor struct {
	processedPins      map[string]uuid.UUID
	pinExisted         map[string]bool
	pinLinks           map[string]string
	uploadService      *pluginService.UploadService
	requestService     core.RequestService
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

func newBlockProcessor(coreCtx core.Context, pinService *pluginService.UploadService, proto *protocol.Protocol, request *models.Request, logger *core.Logger) *blockProcessor {
	ctx, cancel := context.WithCancel(coreCtx.GetContext())
	bp := &blockProcessor{
		processedPins:      make(map[string]uuid.UUID),
		pinExisted:         make(map[string]bool),
		pinLinks:           make(map[string]string),
		uploadService:      pinService,
		requestService:     core.GetService[core.RequestService](coreCtx, core.REQUEST_SERVICE),
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

	// Import the block
	err := bp.proto.GetNode().AddBlock(bp.ctx, job.Block)
	if err != nil {
		return bp.handleError(fmt.Errorf("failed to add block: %w", err))
	}

	// Create pin
	var requestID *uuid.UUID
	if job.IsRoot {
		data, err := bp.requestService.GetProtocolData(bp.ctx, bp.request.ID)
		if err != nil {
			return bp.handleError(err)
		}
		ipfsData := data.(*pluginDb.IPFSRequest)
		requestID = (*uuid.UUID)(&ipfsData.PinRequestID)
	}

	pin, existing, err := bp.uploadService.CreatePinnedPin(bp.ctx, job.Block.Cid(), bp.request.Operation, uint64(len(job.Block.RawData())), bp.request.UserID, bp.request.SourceIP, "", !job.IsRoot, job.IsRoot, requestID, nil)
	if err != nil {
		return bp.handleError(fmt.Errorf("failed to pin block: %w", err))
	}

	if err = bp.uploadService.DetectUpdatePartialStatus(ctx, job.Block); err != nil {
		return bp.handleError(fmt.Errorf("failed to detect and/or update partial status: %w", err))
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

	if job.IsRoot {
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

func processCar(ctx core.Context, r io.Reader, request *models.Request) ([]cid.Cid, error) {
	pinService := core.GetService[*pluginService.UploadService](ctx, pluginService.UPLOAD_SERVICE)
	proto := core.GetProtocol(internal.ProtocolName).(*protocol.Protocol)
	logger := ctx.Logger()

	processedCIDs := make([]cid.Cid, 0)

	cr, err := car.NewBlockReader(r)
	if err != nil {
		logger.Error("Failed to create block reader", zap.Error(err))
		return nil, fmt.Errorf("failed to create block reader: %w", err)
	}

	rootCIDs := cr.Roots
	bp := newBlockProcessor(ctx, pinService, proto, request, logger)
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

		isRoot := lo.Contains(rootCIDs, block.Cid())

		job := &blockJob{
			Block:  block,
			IsRoot: isRoot,
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

	return processedCIDs, nil
}
func isContextCanceled(err error) bool {
	return err == context.Canceled || err == context.DeadlineExceeded
}
