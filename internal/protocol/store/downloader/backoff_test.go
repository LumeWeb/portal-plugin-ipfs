package downloader

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multihash"
)

func TestPruneExpiredFailures(t *testing.T) {
	now := time.Unix(100, 0)
	failedUntil := map[string]time.Time{
		"expired": now,
		"stale":   now.Add(-time.Second),
		"active":  now.Add(time.Second),
	}

	pruneExpiredFailures(failedUntil, now)

	if len(failedUntil) != 1 {
		t.Fatalf("expected one active failure, got %d", len(failedUntil))
	}
	if _, ok := failedUntil["active"]; !ok {
		t.Fatal("active failure was pruned")
	}
}

func TestPrefetchMissingBlockArmsBackoff(t *testing.T) {
	testCID := testCidForBackoff(t)
	key := cidKey(testCID)
	now := time.Now()
	failedUntil := make(map[string]time.Time)
	err := fmt.Errorf("failed to download block: %w", format.ErrNotFound{Cid: testCID})

	recordDownloadFailure(failedUntil, key, err, now)
	if _, ok := failedUntil[key]; !ok {
		t.Fatal("wrapped not-found error did not activate backoff")
	}

	bd := &BlockDownloaderDefault{failedUntil: failedUntil, queue: &priorityQueue{}}
	resp, queued := bd.queueBlock(context.Background(), testCID, downloadPriorityMedium, "")
	if queued {
		t.Fatal("missing prefetch was re-queued during backoff")
	}
	if resp == nil || resp.err == nil {
		t.Fatal("missing prefetch did not return a backoff error")
	}

	// A transient failure must not clear an existing missing-CID cooldown.
	recordDownloadFailure(failedUntil, key, errors.New("temporary storage failure"), now)
	if _, ok := failedUntil[key]; !ok {
		t.Fatal("transient error cleared an existing backoff")
	}
}

// TestPrefetchOriginUpgradeKeepsBackoff guards the anti-amplification
// guarantee: a prefetch task whose priority is upgraded by a concurrent
// foreground Get must still arm the missing-CID cooldown, so a genuine
// ErrNotFound is not silently dropped.
func TestPrefetchOriginUpgradeKeepsBackoff(t *testing.T) {
	testCID := testCidForBackoff(t)
	bd := &BlockDownloaderDefault{failedUntil: make(map[string]time.Time), inflight: make(map[string]*blockResponse), queue: &priorityQueue{}}

	// A prefetch initiates the task, then a foreground Get bonds to it.
	bd.queueBlock(context.Background(), testCID, downloadPriorityMedium, "")
	resp, _ := bd.queueBlock(context.Background(), testCID, downloadPriorityMax, "client-ip")
	if !resp.prefetchOrigin {
		t.Fatal("prefetch task lost its prefetch-origin marker")
	}

	key := cidKey(testCID)
	err := fmt.Errorf("failed to download block: %w", format.ErrNotFound{Cid: testCID})
	recordDownloadFailure(bd.failedUntil, key, err, time.Now())
	if _, ok := bd.failedUntil[key]; !ok {
		t.Fatal("prefetch-origin missing block did not arm a backoff despite upgrade")
	}
}

// TestForegroundOriginDoesNotArmBackoff preserves the separation that a
// foreground-initiated request is not marked as prefetch-origin, so a missing
// failure on it never arms a prefetch cooldown.
func TestForegroundOriginDoesNotArmBackoff(t *testing.T) {
	testCID := testCidForBackoff(t)
	bd := &BlockDownloaderDefault{failedUntil: make(map[string]time.Time), inflight: make(map[string]*blockResponse), queue: &priorityQueue{}}

	resp, _ := bd.queueBlock(context.Background(), testCID, downloadPriorityMax, "client-ip")
	if resp.prefetchOrigin {
		t.Fatal("foreground-created task was marked prefetch-origin")
	}
}

func TestWrappedNotFoundIsClassified(t *testing.T) {
	var notFound format.ErrNotFound
	err := fmt.Errorf("failed to download block: %w", format.ErrNotFound{})
	if !errors.As(err, &notFound) {
		t.Fatal("wrapped not-found error was not classified")
	}
}

// TestSuccessfulFetchClearsBackoff verifies a block that becomes available again
// within the cooldown window is no longer suppressed after a successful fetch.
func TestSuccessfulFetchClearsBackoff(t *testing.T) {
	testCID := testCidForBackoff(t)
	key := cidKey(testCID)
	bd := &BlockDownloaderDefault{failedUntil: make(map[string]time.Time)}
	bd.failedUntil[key] = time.Now().Add(failedDownloadBackoff)

	task := &blockResponse{cid: testCID, priority: downloadPriorityMedium}
	task.prefetchOrigin = true

	bd.finalizeDownload(task, key, time.Now())
	if _, ok := bd.failedUntil[key]; ok {
		t.Fatal("successful fetch did not clear the stale backoff")
	}
}

// TestPrefetchOriginFailureArmsBackoff verifies a prefetch-origin missing block
// arms the cooldown even after the retry is attempted.
func TestPrefetchOriginFailureArmsBackoff(t *testing.T) {
	testCID := testCidForBackoff(t)
	key := cidKey(testCID)
	bd := &BlockDownloaderDefault{failedUntil: make(map[string]time.Time)}

	task := &blockResponse{cid: testCID, priority: downloadPriorityMedium}
	task.prefetchOrigin = true
	task.err = fmt.Errorf("failed to download block: %w", format.ErrNotFound{Cid: testCID})

	bd.finalizeDownload(task, key, time.Now())
	if _, ok := bd.failedUntil[key]; !ok {
		t.Fatal("prefetch-origin missing block did not arm a backoff")
	}
}

// TestForegroundOriginFailureDoesNotArmBackoff verifies a foreground-origin
// missing block never arms a prefetch cooldown.
func TestForegroundOriginFailureDoesNotArmBackoff(t *testing.T) {
	testCID := testCidForBackoff(t)
	key := cidKey(testCID)
	bd := &BlockDownloaderDefault{failedUntil: make(map[string]time.Time)}

	task := &blockResponse{cid: testCID, priority: downloadPriorityMax}
	task.err = fmt.Errorf("failed to download block: %w", format.ErrNotFound{Cid: testCID})

	bd.finalizeDownload(task, key, time.Now())
	if _, ok := bd.failedUntil[key]; ok {
		t.Fatal("foreground-origin missing block armed a prefetch backoff")
	}
}

func testCidForBackoff(t *testing.T) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum([]byte("missing-cid"), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	return cid.NewCidV1(cid.Raw, mh)
}
