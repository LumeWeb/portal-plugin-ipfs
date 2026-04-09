package internal_test

import (
	"fmt"
	"testing"

	// Import fixtures to ensure side-effect import works
	_ "go.lumeweb.com/ipfs-content/testing/fixtures"

	contentfixtures "go.lumeweb.com/ipfs-content/testing/fixtures"
)

func TestFixtureDiscovery(t *testing.T) {
	// Test that we can find fixtures directory
	fixturesDir, err := contentfixtures.FindFixturesDir()
	if err != nil {
		t.Fatalf("Failed to find fixtures directory: %v", err)
	}

	fmt.Printf("✓ Found fixtures directory: %s\n", fixturesDir)

	// Test that we can get specific paths
	libSh, err := contentfixtures.GetLibSh()
	if err != nil {
		t.Fatalf("Failed to get lib.sh path: %v", err)
	}
	fmt.Printf("✓ lib.sh path: %s\n", libSh)

	dataDir, err := contentfixtures.GetDataDir()
	if err != nil {
		t.Fatalf("Failed to get data directory: %v", err)
	}
	fmt.Printf("✓ data directory: %s\n", dataDir)

	fmt.Println("✓ All fixture discovery tests passed!")
}
