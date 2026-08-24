package config

import (
	"fmt"
	"math/rand"
)

// Generated label wordlists for platform subdomains. GenerateDNSSlug composes
// an adjective-noun-number triple (e.g. "swift-river-42") — pronounceable,
// opaque, and non-sequential, so platform usage is hard to enumerate. Wordlists
// live at the application layer (not in a table) until a per-root variance
// requirement exists.
var (
	labelAdjectives = []string{
		"swift", "calm", "bold", "mellow", "bright", "quiet", "rapid", "gentle",
		"lively", "steady", "nimble", "sunny", "frosty", "crimson", "amber",
		"navy", "ivory", "sage", "mossy", "misty",
	}
	labelNouns = []string{
		"river", "forest", "mountain", "meadow", "harbor", "ridge", "stone",
		"willow", "falcon", "otter", "breeze", "summit", "canyon", "glacier",
		"pine", "lark", "dune", "fern", "brook", "prairie",
	}
)

// GenerateDNSSlug returns a pronounceable, DNS-safe label for a platform
// subdomain. Callers may retry on collision; the random numeric suffix makes
// collisions rare.
//
// It relies on math/rand's process-wide (auto, crypto-seeded since Go 1.20)
// source, whose package-level functions are goroutine-safe — no manual Seed and
// no shared sync flags, so concurrent claims cannot race on generator state.
func GenerateDNSSlug() string {
	adj := labelAdjectives[rand.Intn(len(labelAdjectives))]
	noun := labelNouns[rand.Intn(len(labelNouns))]
	suffix := rand.Intn(1000)
	return SanitizeDNSLabel(fmt.Sprintf("%s-%s-%d", adj, noun, suffix))
}
