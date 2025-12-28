//go:build ignore

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/golang/protobuf/proto"
)

type Test struct {
	Value []byte `protobuf:"bytes,1,opt,name=value,proto3"`
}

func (m *Test) Reset()         { *m = Test{} }
func (m *Test) String() string { return proto.CompactTextString(m) }
func (m *Test) ProtoMessage()  {}

func main() {
	size := flag.Int("size", 1024, "Size of protobuf data in bytes")
	partial := flag.Int("partial", 0, "Generate partial/corrupted protobuf data")
	flag.Parse()

	// Generate random data of specified size
	data := make([]byte, *size)
	rand.Read(data)

	// Create protobuf message with the random data
	test := &Test{Value: data}

	// Serialize to protobuf
	protoData, err := proto.Marshal(test)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to marshal protobuf: %v\n", err)
		os.Exit(1)
	}

	// Corrupt data if partial flag is set
	if *partial == 1 {
		if len(protoData) > 10 {
			// Corrupt some bytes in the middle
			for i := len(protoData)/2 - 5; i < len(protoData)/2+5; i++ {
				protoData[i] = 0xFF
			}
		}
	}

	// Put the block into IPFS (allow blocks >1MB for testing)
	cmd := exec.Command("ipfs", "dag", "put", "--input-codec=dag-pb", "--store-codec=dag-pb", "--allow-big-block")
	cmd.Stdin = bytes.NewReader(protoData)
	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ipfs dag put failed: %v\nOutput: %s\n", err, out)
		os.Exit(1)
	}

	// Print just the CID (removing any trailing newline)
	cidStr := string(bytes.TrimSpace(out))
	fmt.Print(cidStr)

	type ProtobufInfo struct {
		Size         int    `json:"size"`
		MessageSize  int    `json:"message_size"`
		Missing      int    `json:"missing"`
		CID          string `json:"cid"`
		IsPartial    bool   `json:"is_partial"`
		RawBlockSize int    `json:"raw_block_size"`
	}

	// Create JSON info file
	outputDir := os.Getenv("OUTPUT_DIR")
	if outputDir == "" {
		outputDir = "."
	}
	// Use 1/0 instead of true/false for consistent naming
	missingFlag := 0
	if *partial == 1 {
		missingFlag = 1
	}

	info := ProtobufInfo{
		Size:         *size,
		MessageSize:  len(data), // Size of the protobuf message data
		Missing:      missingFlag,
		CID:          cidStr,
		IsPartial:    *partial == 1,
		RawBlockSize: len(protoData), // Size of the serialized protobuf block
	}
	infoFile := filepath.Join(outputDir, fmt.Sprintf("protobuf_%d_%d.info.json", *size, missingFlag))
	file, err := os.Create(infoFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create info file: %v\n", err)
		os.Exit(1)
	}
	defer func(file *os.File) {
		err = file.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(file)

	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")
	if err := enc.Encode(info); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to write info file: %v\n", err)
		os.Exit(1)
	}
}
