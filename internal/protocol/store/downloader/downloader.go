// package downloader contains a cache for downloading blocks from a renterd node.
// A cache optimizes the number of in-flight requests to avoid overloading the
// node and caches blocks to avoid redundant downloads.
package downloader

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"github.com/icza/gox/gox"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
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
	}

	priorityQueue []*blockResponse

	// A MetadataStore is a store for IPFS block metadata. It is used to
	// optimize block downloads by prefetching linked blocks.
	MetadataStore interface {
		BlockExists(c cid.Cid) (err error)
		BlockSiblings(c cid.Cid, max int) (siblings []cid.Cid, err error)
		BlockChildren(c cid.Cid, max *int) (siblings []cid.Cid, err error)
	}

	// BlockDownloader is a cache for downloading blocks from a renterd node.
	// It limits the number of in-flight requests to avoid overloading the node
	// and caches blocks to avoid redundant downloads.
	//
	// For UnixFS nodes, it also prefetches linked blocks.
	BlockDownloader struct {
		ctx     core.Context
		store   MetadataStore
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
	return blocks.NewBlockWithCid(br.b, c)
}

func (bd *BlockDownloader) downloadBlockData(ctx context.Context, c cid.Cid) ([]byte, error) {
	blockBuf := bytes.NewBuffer(make([]byte, 0, 2<<20))

	bd.log.Debug("Trying to download block", zap.String("CID", c.String()))
	object, err := bd.storage.DownloadObject(ctx, bd.proto, internal.NewIPFSHash(c), 0)
	if err != nil {
		return nil, fmt.Errorf("failed to download block: %w", err)
	}

	_, err = io.Copy(blockBuf, object)
	if err != nil {
		return nil, fmt.Errorf("failed to read block: %w", err)
	}

	defer func(object io.ReadCloser) {
		err := object.Close()
		if err != nil {
			bd.log.Error("failed to close object", zap.Error(err))
		}
	}(object)

	h, err := mh.Sum(blockBuf.Bytes(), c.Prefix().MhType, -1)
	if err != nil {
		return nil, fmt.Errorf("failed to verify block: %w", err)
	} else if c.Hash().HexString() != h.HexString() {
		return nil, fmt.Errorf("block hash mismatch: expected %s, actual %s", c.Hash().HexString(), h.HexString())
	}

	return blockBuf.Bytes(), nil
}

func (bd *BlockDownloader) queueRelated(c cid.Cid) {
	log := bd.log.Named("queueRelated").With(zap.Stringer("cid", c))
	siblings, err := bd.store.BlockSiblings(c, 64)
	if err != nil {
		log.Error("failed to get block siblings", zap.Error(err))
		return
	}

	children, err := bd.store.BlockChildren(c, gox.NewInt(64))
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

		if _, ok := bd.queueBlock(sibling, downloadPriorityMedium); ok {
			log.Debug("queued sibling", zap.Stringer("sibling", sibling))
		}
	}

	for _, child := range children {
		// check if the block exists in the store
		err = bd.store.BlockExists(child)
		if err != nil {
			continue
		}

		if _, ok := bd.queueBlock(child, downloadPriorityLow); ok {
			log.Debug("queued child", zap.Stringer("child", child))
		}
	}
}

func (bd *BlockDownloader) doDownloadTask(task *blockResponse, log *zap.Logger) {
	start := time.Now()
	log = log.Named("doDownloadTask").With(zap.Stringer("cid", task.cid), zap.Stringer("priority", task.priority))

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	buf, err := bd.downloadBlockData(ctx, task.cid)
	if err != nil {
		log.Error("failed to download block", zap.Error(err))
		task.err = err
	} else {
		log.Debug("block downloaded", zap.Int("size", len(buf)), zap.Duration("elapsed", time.Since(start)))
		task.b = buf
	}
	close(task.ch)

	if task.priority >= downloadPriorityHigh {
		go bd.queueRelated(task.cid)
	}
}

func (bd *BlockDownloader) downloadWorker(n int) {
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

func (bd *BlockDownloader) queueBlock(c cid.Cid, priority downloadPriority) (*blockResponse, bool) {
	resp, ok := bd.inflight[cidKey(c)]
	if ok {
		if resp.priority < priority {
			resp.priority = priority
			heap.Fix(bd.queue, resp.index)
		}
		return resp, false
	}

	resp = &blockResponse{
		cid: c,

		priority:  priority,
		timestamp: time.Now(),

		ch: make(chan struct{}),
	}
	bd.inflight[cidKey(c)] = resp
	heap.Push(bd.queue, resp)
	bd.cond.Signal()
	return resp, true
}

// Get returns a block by CID.
func (bd *BlockDownloader) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	// check if the block exists in the store
	err := bd.store.BlockExists(c)
	if err != nil {
		return nil, err
	}

	bd.mu.Lock()

	bd.log.Debug("queuing block", zap.String("CID", c.String()))
	resp, _ := bd.queueBlock(c, downloadPriorityMax)
	bd.mu.Unlock()
	bd.log.Debug("waiting on queued block", zap.String("CID", c.String()))
	return resp.block(ctx, c)
}

func cidKey(c cid.Cid) string {
	return cid.NewCidV1(c.Type(), c.Hash()).String()
}

// NewBlockDownloader creates a new BlockDownloader.
func NewBlockDownloader(ctx core.Context, store MetadataStore, workers int) (*BlockDownloader, error) {
	log := ctx.Logger()

	proto, ok := core.GetProtocol(internal.ProtocolName).(core.StorageProtocol)
	if !ok {
		return nil, fmt.Errorf("protocol not found: %s", internal.ProtocolName)
	}

	bd := &BlockDownloader{
		store:   store,
		proto:   proto,
		log:     log,
		storage: ctx.Service(core.STORAGE_SERVICE).(core.StorageService),

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
