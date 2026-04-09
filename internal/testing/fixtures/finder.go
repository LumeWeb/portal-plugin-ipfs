// Package fixtures provides test fixture discovery utilities.
// This is a Go version of the bash fixture discovery logic.
package fixtures

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// FindFixturesDir uses Go tooling and directory scanning to find ipfs-content fixtures.
// Similar to Node.js module resolution - tries go list first, then scans up for vendor.
func FindFixturesDir(baseDir string) string {
	const maxDepth = 10
	const testDataDir = "internal/testing/fixtures"
	const ipfsContentModule = "go.lumeweb.com/ipfs-content"

	// Approach 1: Try go list first (fastest method)
	if modDir, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", ipfsContentModule).Output(); err == nil {
		modPath := strings.TrimSpace(string(modDir))
		if modPath != "" {
			fixturesDir := filepath.Join(modPath, testDataDir)
			if _, err := os.Stat(fixturesDir); err == nil {
				return fixturesDir
			}
		}
	}

	// Approach 2: Fall back to vendor directory scanning (like Node.js)
	checkDir := baseDir
	depth := 0

	for depth < maxDepth {
		// Check for vendor directory at current level
		vendorDir := filepath.Join(checkDir, "vendor", filepath.ToSlash(ipfsContentModule), testDataDir)
		if _, err := os.Stat(vendorDir); err == nil {
			return vendorDir
		}

		// Move up one directory
		parentDir := filepath.Dir(checkDir)
		if parentDir == checkDir || parentDir == "/" {
			break
		}
		checkDir = parentDir
		depth++
	}

	// Approach 3: Try relative path (assuming sibling repos)
	relPath := filepath.Join(baseDir, "..", "..", filepath.ToSlash(ipfsContentModule), testDataDir)
	if _, err := os.Stat(relPath); err == nil {
		return relPath
	}

	// Approach 4: Last resort - try from current working directory
	if cwd, err := os.Getwd(); err == nil {
		cwdPath := filepath.Join(cwd, "vendor", filepath.ToSlash(ipfsContentModule), testDataDir)
		if _, err := os.Stat(cwdPath); err == nil {
			return cwdPath
		}
	}

	// All approaches failed
	return ""
}

// FindFixturesFile finds a specific file in the fixtures directory
func FindFixturesFile(baseDir, filename string) string {
	fixturesDir := FindFixturesDir(baseDir)
	if fixturesDir == "" {
		return ""
	}
	return filepath.Join(fixturesDir, filename)
}

// EnsureFixturesAvailable checks if fixtures are available and returns an error if not
func EnsureFixturesAvailable(baseDir string) error {
	fixturesDir := FindFixturesDir(baseDir)
	if fixturesDir == "" {
		return os.ErrNotExist
	}
	
	// Check that at least lib.sh exists as a sanity check
	libSh := filepath.Join(fixturesDir, "lib.sh")
	if _, err := os.Stat(libSh); err != nil {
		return err
	}
	
	return nil
}
