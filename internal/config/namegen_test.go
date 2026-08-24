package config

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var slugPattern = regexp.MustCompile(`^[a-z0-9]+-[a-z0-9]+-[0-9]+$`)

func TestGenerateDNSSlug_Format(t *testing.T) {
	// Generated labels are adjective-noun-number, DNS-safe (lowercase,
	// hyphen-separated, <= 63 labels), and pronounceable/opaque — never a bare
	// sequential counter that reveals usage volume.
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		slug := GenerateDNSSlug()
		require.LessOrEqual(t, len(slug), 63)
		assert.Regexp(t, slugPattern, slug, "slug %q should match adjective-noun-number", slug)
		assert.False(t, seen[slug], "slug %q should vary across calls", slug)
		seen[slug] = true

		// Must pass a strict DNS-label validation (no underscores, no leading
		// digits beyond allowed, no empty labels).
		assert.NotContains(t, slug, "_")
		assert.NotContains(t, slug, "..")
	}
}

func TestGenerateDNSSlug_DNSValid(t *testing.T) {
	// Every generated slug must be a valid DNS label as used by the providers
	// (single label, letters/digits/hyphens only).
	r := regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?$`)
	for i := 0; i < 100; i++ {
		assert.True(t, r.MatchString(GenerateDNSSlug()))
	}
}

// TestGenerateDNSSlug_Concurrent guards the generator against data races /
// shared mutable state (the prior labelSeeded seed flag). Run under -race.
func TestGenerateDNSSlug_Concurrent(t *testing.T) {
	done := make(chan struct{})
	const workers = 32
	for i := 0; i < workers; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 200; j++ {
				slug := GenerateDNSSlug()
				assert.LessOrEqual(t, len(slug), 63)
				assert.Regexp(t, slugPattern, slug)
			}
		}()
	}
	for i := 0; i < workers; i++ {
		<-done
	}
}
