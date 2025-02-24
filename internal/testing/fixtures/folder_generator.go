package main

import (
	"crypto/rand"
	"flag"
	"fmt"
	"math/big"
	mrand "math/rand"
	"os"
	"path/filepath"
)

func generateRandomContent(size int) (string, error) {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, size)
	for i := range result {
		num, err := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		if err != nil {
			return "", err
		}
		result[i] = charset[num.Int64()]
	}
	return string(result), nil
}

func createFile(path string, size int) error {
	content, err := generateRandomContent(size)
	if err != nil {
		return err
	}
	return os.WriteFile(path, []byte(content), 0644)
}

func createNestedStructure(basePath string, depth, filesPerFolder, maxFileSize, subdirsPerFolder int) error {
	if depth == 0 {
		return nil
	}

	// Create files
	for i := 0; i < filesPerFolder; i++ {
		filePath := filepath.Join(basePath, fmt.Sprintf("file_%d.txt", i))
		fileSize := 1 + mrand.Intn(maxFileSize)
		if err := createFile(filePath, fileSize); err != nil {
			return err
		}
	}

	// Create subdirectories (skip if in HAMT mode)
	if subdirsPerFolder > 0 {
		for i := 0; i < subdirsPerFolder; i++ {
			dirPath := filepath.Join(basePath, fmt.Sprintf("subfolder_%d", i))
			if err := os.MkdirAll(dirPath, 0755); err != nil {
				return err
			}
			if err := createNestedStructure(dirPath, depth-1, filesPerFolder, maxFileSize, subdirsPerFolder); err != nil {
				return err
			}
		}
	}

	return nil
}

func main() {
	var (
		basePath         string
		depth            int
		filesPerFolder   int
		maxFileSize      int
		subdirsPerFolder int
		hamtMode         bool
	)

	flag.StringVar(&basePath, "path", "ipfs_test_data", "Base directory path for test data")
	flag.IntVar(&depth, "depth", 1, "Directory nesting depth")
	flag.IntVar(&filesPerFolder, "files", 10, "Number of files per directory")
	flag.IntVar(&maxFileSize, "size", 1024, "Maximum file size in bytes")
	flag.IntVar(&subdirsPerFolder, "subdirs", 3, "Number of subdirectories per folder")
	flag.BoolVar(&hamtMode, "hamt", false, "HAMT mode: creates single level with 262,145 files")
	flag.Parse()

	// Override settings if HAMT mode is enabled
	if hamtMode {
		depth = 1
		filesPerFolder = 262145
		subdirsPerFolder = 0
	}

	if err := os.MkdirAll(basePath, 0755); err != nil {
		fmt.Printf("Error creating base directory: %v\n", err)
		return
	}

	if err := createNestedStructure(basePath, depth, filesPerFolder, maxFileSize, subdirsPerFolder); err != nil {
		fmt.Printf("Error creating test data: %v\n", err)
		return
	}

	fmt.Printf("Test data generated in '%s' directory.\n", basePath)
	fmt.Printf("Configuration: depth=%d, filesPerFolder=%d, maxFileSize=%d, subdirsPerFolder=%d, hamtMode=%v\n",
		depth, filesPerFolder, maxFileSize, subdirsPerFolder, hamtMode)
}
