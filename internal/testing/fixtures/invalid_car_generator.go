//go:build ignore

package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"

	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
)

func main() {
	// Get source file location using runtime
	_, filename, _, _ := runtime.Caller(0)
	cwd := filepath.Dir(filename)

	// Define the output CAR file path in the cars/ subdirectory
	carsDir := filepath.Join(cwd, "cars")
	if err := os.MkdirAll(carsDir, 0755); err != nil {
		fmt.Println("Error creating cars directory:", err)
		return
	}
	carFilePath := filepath.Join(carsDir, "invalid.car")

	// Create a new CAR file with no roots (valid initially)
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

	// Now intentionally corrupt the file by truncating it
	file, err := os.OpenFile(carFilePath, os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening file for corruption:", err)
		return
	}
	defer file.Close()

	// Get file info to determine size
	info, err := file.Stat()
	if err != nil {
		fmt.Println("Error getting file info:", err)
		return
	}

	// Truncate to half its size to make it invalid
	err = file.Truncate(info.Size() / 2)
	if err != nil {
		fmt.Println("Error truncating file:", err)
		return
	}

	fmt.Println("Successfully created invalid CAR file:", carFilePath)

	// Verification - this should fail
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

	_, err = carv2.NewReader(f)
	if err != nil {
		fmt.Println("Successfully verified CAR file is invalid:", err)
	} else {
		fmt.Println("Warning: CAR file appears to be valid when it shouldn't be")
	}
}
