//go:build ignore

package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
)

func main() {
	// Get current working directory
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Println("Error getting current working directory:", err)
		return
	}

	// Define the output CAR file path in the current directory
	carFilePath := filepath.Join(cwd, "empty.car")

	// Create a new CAR file with no roots
	roots := []cid.Cid{} // Empty roots slice
	opts := []carv2.Option{}

	// Create a new CARv2 blockstore writer
	bs, err := blockstore.OpenReadWrite(carFilePath, roots, opts...)
	if err != nil {
		fmt.Println("Error creating blockstore writer:", err)
		return
	}

	// Finalize the CAR file
	if err := bs.Finalize(); err != nil {
		fmt.Println("Error finalizing CAR file:", err)
		return
	}

	fmt.Println("Successfully created empty CAR file:", carFilePath)

	// Verification remains the same
	f, err := os.Open(carFilePath)
	if err != nil {
		fmt.Println("Error opening CAR file for verification:", err)
		return
	}
	defer func(f *os.File) {
		err = f.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(f)

	r, err := carv2.NewReader(f)
	if err != nil {
		fmt.Println("Error creating CAR reader:", err)
		return
	}

	roots, err = r.Roots()
	if err != nil {
		fmt.Println("Error getting roots from CAR:", err)
		return
	}

	if len(roots) != 0 {
		fmt.Println("CAR file is not empty, roots found:", roots)
	} else {
		fmt.Println("CAR file is empty, no roots found.")
	}

	if err := r.Close(); err != nil {
		fmt.Println("Error closing CAR reader:", err)
		return
	}
}
