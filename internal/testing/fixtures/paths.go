package fixtures

import (
	"fmt"

	// Centralized fixture discovery from ipfs-content
	contentfixtures "go.lumeweb.com/ipfs-content/testing/fixtures"
)

var (
	// FixturesDir is the path to the ipfs-content fixtures directory
	// This is automatically discovered using the ipfs-content package
	FixturesDir string
)

func init() {
	// Use centralized fixture discovery from ipfs-content
	var err error
	FixturesDir, err = contentfixtures.FindFixturesDir()
	if err != nil {
		panic(fmt.Sprintf("Failed to find ipfs-content fixtures: %v", err))
	}
}
