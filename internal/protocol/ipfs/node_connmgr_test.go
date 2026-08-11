package ipfs

import (
	"fmt"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
)

// newTestLimiter builds a fixed limiter whose System connection limits are set
// explicitly, independent of machine-specific autoscaling. This lets the
// watermark derivation be tested deterministically.
func newTestLimiter(t *testing.T, connsTotal, connsInbound, connsOutbound int) rcmgr.Limiter {
	t.Helper()

	limits := rcmgr.PartialLimitConfig{
		System: rcmgr.ResourceLimits{
			Conns:         rcmgr.LimitVal(connsTotal),
			ConnsInbound:  rcmgr.LimitVal(connsInbound),
			ConnsOutbound: rcmgr.LimitVal(connsOutbound),
		},
	}.Build(rcmgr.DefaultLimits.AutoScale())

	return rcmgr.NewFixedLimiter(limits)
}

func TestDeriveConnLimits_InvariantHolds(t *testing.T) {
	// Simulates the autoscaled default that bit production: total hard ceiling
	// ~2x the inbound ceiling (the classic 128 total / 64 inbound default).
	limiter := newTestLimiter(t, 377, 188, 377)

	low, high := deriveConnLimits(limiter)

	// The binding direction is inbound (188) but the derivation must act on
	// min(total, inbound), so high-water must be driven by 188, not 377.
	if high >= 188 {
		t.Fatalf("high-water %d must be < inbound hard limit 188 (was driven by total)", high)
	}
	if !(low < high && high < 188) {
		t.Fatalf("invariant low(%d) < high(%d) < hard(188) violated", low, high)
	}

	// high = 70% of 188 = 131; low = 75% of 131 = 98
	if high != 131 {
		t.Errorf("expected high-water 131, got %d", high)
	}
	if low != 98 {
		t.Errorf("expected low-water 98, got %d", low)
	}
}

func TestDeriveConnLimits_UsesMinOfTotalAndInbound(t *testing.T) {
	cases := []struct {
		name           string
		total          int
		inbound        int
		wantHardDrives int // the hard limit that should drive the result
	}{
		// Inbound is the binding direction (classic default: inbound ~half).
		{"inbound_binding", 377, 188, 188},
		// Inbound equals total (public server override in Task 1.2).
		{"inbound_equals_total", 377, 377, 377},
		// With equal inbound/total, derivation is driven by the shared ceiling.
		{"shared_ceiling", 100, 100, 100},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			limiter := newTestLimiter(t, tc.total, tc.inbound, tc.total)
			low, high := deriveConnLimits(limiter)

			wantHigh := int(float64(tc.wantHardDrives) * 0.70)
			if high != wantHigh {
				t.Errorf("high-water = %d, want %d (driven by hard limit %d)",
					high, wantHigh, tc.wantHardDrives)
			}
			if low >= high {
				t.Errorf("low-water %d >= high-water %d", low, high)
			}
			// low = 75% of high
			wantLow := int(float64(wantHigh) * 0.75)
			if low != wantLow {
				t.Errorf("low-water = %d, want %d", low, wantLow)
			}
		})
	}
}

func TestDeriveConnLimits_TinyHardLimitStillOrdered(t *testing.T) {
	// A tiny ceiling must always yield strictly low < high and keep both >= 1.
	// Regression guard: hard limits of 1 and 2 previously collapsed to
	// low==high==1, violating the ordering BasicConnMgr requires.
	//
	// For hard limits >= 2 the pair must also stay within the ceiling (a
	// valid low<high pair exists there). For a hard limit of exactly 1 no
	// ordered pair fits inside the ceiling, so the function returns (1,2):
	// connmgr high=2 merely means "never prune below 2", while the rcmgr still
	// enforces the real 1-connection cap, so nothing exceeds the hard fence.
	for n := 1; n <= 5; n++ {
		n := n
		t.Run(fmt.Sprintf("hard_%d", n), func(t *testing.T) {
			limiter := newTestLimiter(t, n, n, n)

			low, high := deriveConnLimits(limiter)

			if low < 1 || high < 1 {
				t.Fatalf("watermarks must be >= 1, got low=%d high=%d", low, high)
			}
			if low >= high {
				t.Fatalf("low(%d) >= high(%d) for hard limit %d", low, high, n)
			}
			if n == 1 {
				if low != 1 || high != 2 {
					t.Fatalf("hard 1 must escape to (1,2), got (%d,%d)", low, high)
				}
				return
			}
			if low > n || high > n {
				t.Fatalf("watermarks exceed hard limit %d: low=%d high=%d", n, low, high)
			}
		})
	}
}

func TestDeriveConnLimits_InBoundDirectionRead(t *testing.T) {
	// Regression guard: GetConnLimit must be queried with the inbound
	// direction. If the code accidentally used the total limit for both, this
	// test (inbound < total) would produce a high-water that matches the total
	// ceiling instead of the inbound one.
	limiter := newTestLimiter(t, 200, 80, 200)

	// Sanity-check the limiter itself reports what we configured.
	sys := limiter.GetSystemLimits()
	if got := sys.GetConnLimit(network.DirInbound); got != 80 {
		t.Fatalf("test setup: GetConnLimit(inbound) = %d, want 80", got)
	}
	if got := sys.GetConnTotalLimit(); got != 200 {
		t.Fatalf("test setup: GetConnTotalLimit() = %d, want 200", got)
	}

	low, high := deriveConnLimits(limiter)

	wantHigh := int(float64(80) * 0.70) // 56
	if high != wantHigh {
		t.Errorf("high-water = %d, want %d (must be inbound-driven, not 200)", high, wantHigh)
	}
	if low != int(float64(wantHigh)*0.75) { // 42
		t.Errorf("low-water = %d, want 42", low)
	}
}

func TestDeriveConnLimits_UnlimitedDoesNotCollapse(t *testing.T) {
	// Regression guard for the disable_resource_limits / debug-unlimited path.
	// InfiniteLimits resolves its connection limits to math.MaxInt (not a real
	// ceiling). Deriving proportions from it must neither collapse the pool to
	// a tiny floor (e.g. low=1/high=1) nor produce an inverted pair; it should
	// return effectively-never-prune watermarks so the unlimited mode behaves
	// as "no connection manager interference".
	limiter := rcmgr.NewFixedLimiter(rcmgr.InfiniteLimits)

	low, high := deriveConnLimits(limiter)

	if low == 1 && high == 1 {
		t.Fatalf("unlimited hard limit collapsed watermarks to 1/1; must stay effectively-never-prune")
	}
	if low >= high {
		t.Fatalf("unlimited path must keep low(%d) < high(%d)", low, high)
	}
	// They must be at a scale that never triggers ordinary pruning on a real
	// host (way above any realistic connection count).
	if low < 1<<20 || high < 1<<20 {
		t.Fatalf("unlimited watermarks must be large (no pruning), got low=%d high=%d", low, high)
	}
}

func TestDeriveConnLimits_DegenerateHardLimitGuarded(t *testing.T) {
	// BlockAll ("0") or a negative/degenerate hard limit must be treated as
	// "no meaningful ceiling" and yield never-prune watermarks, never an
	// inverted or collapsed pair.
	//
	// NOTE: LimitVal(0) is DefaultLimit (falls back to the default autoscaled
	// value), so a genuinely-zero ceiling requires BlockAllLimit (-2), which
	// resolves to 0 in Build.
	for name, limiter := range map[string]rcmgr.Limiter{
		"block_all_inbound": newTestLimiter(t, 100, int(rcmgr.BlockAllLimit), 100), // inbound blocked -> 0
		"block_all_total":   newTestLimiter(t, int(rcmgr.BlockAllLimit), int(rcmgr.BlockAllLimit), int(rcmgr.BlockAllLimit)),
	} {
		t.Run(name, func(t *testing.T) {
			low, high := deriveConnLimits(limiter)
			if low >= high {
				t.Fatalf("low(%d) >= high(%d) for %s", low, high, name)
			}
			if low < 1<<20 || high < 1<<20 {
				t.Fatalf("degenerate %s must yield never-prune watermarks, got low=%d high=%d", name, low, high)
			}
		})
	}
}
