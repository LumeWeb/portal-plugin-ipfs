// package downloader contains a cache for downloading blocks from a renterd node.
// A cache limits the number of in-flight requests to avoid overloading the
// node and caches blocks to avoid redundant downloads.
package downloader

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"github.com/samber/lo"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/store"
	"go.lumeweb.com/portal-plugin-ipfs/internal/quota"
	"go.lumeweb.com/portal/core"
	"io"
	"sync"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"go.uber.org/zap"
)

const (
	downloadPriorityLow downloadPriority = iota + 1
	downloadPriorityMedium
	downloadPriorityHigh
	downloadPriorityMax
)

type (
	downloadPriority int8

	blockResponse struct {
		ch  chan struct{}
		b   []byte
		err error

		cid       cid.Cid
		priority  downloadPriority
		index     int
		timestamp time.Time
		log       *core.Logger
		clientIP  string
	}

	priorityQueue []*blockResponse

	// BlockDownloaderDefault is a cache for downloading blocks from a renterd node.
	// It limits the number of in-flight requests to avoid overloading the node
	// and caches blocks to avoid redundant downloads.
	//
	// For UnixFS nodes, it also prefetches linked blocks.
	BlockDownloaderDefault struct {
		ctx     core.Context
		store   pluginCore.MetadataStore
		proto   core.StorageProtocol
		storage core.StorageService
		log     *core.Logger

		mu       sync.Mutex // protects the fields below
		cond     sync.Cond
		inflight map[string]*blockResponse
		queue    *priorityQueue
	}
)

func (dp downloadPriority) String() string {
	switch dp {
	case downloadPriorityLow:
		return "low"
	case downloadPriorityMedium:
		return "medium"
	case downloadPriorityHigh:
		return "high"
	case downloadPriorityMax:
		return "max"
	default:
		panic("invalid download priority")
	}
}

func (h priorityQueue) Len() int { return len(h) }

func (h priorityQueue) Less(i, j int) bool {
	if h[i].priority != h[j].priority {
		return h[i].priority > h[j].priority
	}
	return h[i].timestamp.Before(h[j].timestamp)
}

func (h priorityQueue) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *priorityQueue) Push(t any) {
	n := len(*h)
	task := t.(*blockResponse)
	task.index = n
	*h = append(*h, task)
}

func (h *priorityQueue) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*h = old[0 : n-1]
	return item
}

var _ heap.Interface = &priorityQueue{}

func (br *blockResponse) block(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-br.ch:
	}
	if br.err != nil {
		return nil, br.err
	}

	br.log.Debug("resolved block", zap.String("CID", c.String()))

	return blocks.NewBlockWithCid(br.b, c)
}

func (bd *BlockDownloaderDefault) downloadBlockData(ctx context.Context, c cid.Cid, clientIP string) ([]byte, error) {
	blockBuf := bytes.NewBuffer(make([]byte, 0, 2<<20))

	bd.log.Debug("Trying to download block", zap.String("CID", c.String()))
	object, err := bd.storage.DownloadObjectWithOptions(ctx, bd.proto, internal.NewIPFSHash(c), core.StorageDownloadWithSkipMetadataCheck(true))
	if err != nil {
		return nil, fmt.Errorf("failed to download block: %w", err)
	}

	// Ensure the object is always closed, even on later errors.
	defer func() {
		if cerr := object.Close(); cerr != nil {
			bd.log.Error("failed to close object", zap.Error(cerr))
		}
	}()

	_, err = io.Copy(blockBuf, object)
	if err != nil {
		return nil, fmt.Errorf("failed to read block: %w", err)
	}

	// Check if the hash function is supported before verifying
	mhType := c.Prefix().MhType
	// TODO: Maybe allow other hash types?
	if mhType != mh.SHA2_256 {
		return nil, fmt.Errorf("unsupported hash function: %d", mhType)
	}

	h, err := mh.Sum(blockBuf.Bytes(), mhType, -1)
	if err != nil {
		return nil, fmt.Errorf("failed to verify block: %w", err)
	} else if c.Hash().HexString() != h.HexString() {
		return nil, fmt.Errorf("block hash mismatch: expected %s, actual %s", c.Hash().HexString(), h.HexString())
	}

	// Emit download completion event for block retrieval
	// uploadID=0 indicates this download is not associated with a specific upload record
	// The clientIP is still tracked for quota purposes when available
	quota.EmitDownloadCompleted(bd.ctx, 0, uint64(blockBuf.Len()), clientIP, nil)

	return blockBuf.Bytes(), nil
}

func (bd *BlockDownloaderDefault) queueRelated(c cid.Cid) {
	log := bd.log.Named("queueRelated").With(zap.Stringer("cid", c))
	siblings, err := bd.store.BlockSiblings(c, 64)
	if err != nil {
		log.Error("failed to get block siblings", zap.Error(err))
		return
	}

	children, err := bd.store.BlockChildren(c, lo.ToPtr(64))
	if err != nil {
		log.Error("failed to get block children", zap.Error(err))
		return
	}

	bd.mu.Lock()
	defer bd.mu.Unlock()

	for _, sibling := range siblings {
		// check if the block exists in the store
		err = bd.store.BlockExists(sibling)
		if err != nil {
			continue
		}

		// Prefetch downloads use empty client IP to distinguish from user-initiated downloads
		// This allows quota tracking to differentiate between foreground and background traffic
		if _, ok := bd.queueBlock(sibling, downloadPriorityMedium, ""); ok {
			log.Debug("queued sibling", zap.Stringer("sibling", sibling))
		}
	}

	for _, child := range children {
		// check if the block exists in the store
		err = bd.store.BlockExists(child)
		if err != nil {
			continue
		}

		// Prefetch downloads use empty client IP to distinguish from user-initiated downloads
		// This allows quota tracking to differentiate between foreground and background traffic
		if _, ok := bd.queueBlock(child, downloadPriorityLow, ""); ok {
			log.Debug("queued child", zap.Stringer("child", child))
		}
	}
}

func (bd *BlockDownloaderDefault) doDownloadTask(task *blockResponse, log *zap.Logger) {
	start := time.Now()
	log = log.Named("doDownloadTask").With(zap.Stringer("cid", task.cid), zap.Stringer("priority", task.priority))

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	buf, err := bd.downloadBlockData(ctx, task.cid, task.clientIP)
	if err != nil {
		log.Error("failed to download block", zap.Error(err))
		task.err = err
	} else {
		log.Debug("block downloaded", zap.Int("size", len(buf)), zap.Duration("elapsed", time.Since(start)))
		task.b = buf
	}
	close(task.ch)

	if task.err == nil && task.priority >= downloadPriorityHigh {
		go bd.queueRelated(task.cid)
	}
}

func (bd *BlockDownloaderDefault) downloadWorker(n int) {
	log := bd.log.Named("worker").With(zap.Int("id", n))
	for {
		bd.mu.Lock()
		for bd.queue.Len() == 0 {
			bd.cond.Wait()
		}

		// pop the highest priority task from the queue
		task := heap.Pop(bd.queue).(*blockResponse)
		bd.mu.Unlock() // unlock the mutex before doing the download

		// download the block
		log := log.With(zap.Stringer("cid", task.cid), zap.Stringer("priority", task.priority))
		log.Debug("popped task from queue")
		bd.doDownloadTask(task, log)

		// delete the task from the inflight map after it's done
		bd.mu.Lock()
		delete(bd.inflight, cidKey(task.cid))
		bd.mu.Unlock()
	}
}

func (bd *BlockDownloaderDefault) queueBlock(c cid.Cid, priority downloadPriority, clientIP string) (*blockResponse, bool) {
	resp, ok := bd.inflight[cidKey(c)]
	if ok {
		if resp.priority < priority {
			resp.priority = priority
			// Only call heap.Fix if the task is still in the queue (index >= 0)
			if resp.index >= 0 {
				heap.Fix(bd.queue, resp.index)
			}
		}
		// Upgrade from anonymous/prefetch to user-initiated download when possible.
		if resp.clientIP == "" && clientIP != "" {
			resp.clientIP = clientIP
		}
		return resp, false
	}

	resp = &blockResponse{
		cid: c,

		priority:  priority,
		timestamp: time.Now(),

		ch:       make(chan struct{}),
		log:      bd.log,
		clientIP: clientIP,
	}
	bd.inflight[cidKey(c)] = resp
	heap.Push(bd.queue, resp)
	bd.cond.Signal()
	return resp, true
}

// Get returns a block by CID.
func (bd *BlockDownloaderDefault) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	// Check context before doing any work
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// check if the block exists in the store
	err := bd.store.BlockExists(c)
	if err != nil {
		return nil, err
	}

	// Get client IP from context before locking
	clientIP := store.GetClientIP(ctx)

	bd.mu.Lock()

	bd.log.Debug("queuing block", zap.String("CID", c.String()))
	resp, _ := bd.queueBlock(c, downloadPriorityMax, clientIP)
	bd.mu.Unlock()
	bd.log.Debug("waiting on queued block", zap.String("CID", c.String()))
	return resp.block(ctx, c)
}

func cidKey(c cid.Cid) string {
	return cid.NewCidV1(c.Type(), c.Hash()).String()
}

var _ pluginCore.BlockDownloader = (*BlockDownloaderDefault)(nil)

// NewBlockDownloader creates a new BlockDownloaderDefault.
func NewBlockDownloader(ctx core.Context, store pluginCore.MetadataStore, workers int) (*BlockDownloaderDefault, error) {
	log := ctx.Logger()

	proto, ok := core.GetProtocol(internal.ProtocolName).(core.StorageProtocol)
	if !ok {
		return nil, fmt.Errorf("protocol not found: %s", internal.ProtocolName)
	}

	storage := core.GetService[core.StorageService](ctx, core.STORAGE_SERVICE)
	if storage == nil {
		return nil, fmt.Errorf("storage service not found: %s", core.STORAGE_SERVICE)
	}

	bd := &BlockDownloaderDefault{
		ctx:     ctx,
		store:   store,
		proto:   proto,
		log:     log,
		storage: storage,

		inflight: make(map[string]*blockResponse),
		queue:    &priorityQueue{},
	}
	bd.cond.L = &bd.mu
	heap.Init(bd.queue)
	for i := 0; i < workers; i++ {
		go bd.downloadWorker(i)
	}
	return bd, nil
}
