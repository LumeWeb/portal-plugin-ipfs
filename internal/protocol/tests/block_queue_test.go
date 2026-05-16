package tests

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/ipld/go-car/v2"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/fixtures"
	"go.lumeweb.com/portal/core"

	"github.com/stretchr/testify/require"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

func TestProcessCarIntegration(t *testing.T) {
	// Define test cases
	testCases := []struct {
		name         string
		carFileName  string
		expectedSize int64
		rootCIDs     []string
		expectError  bool
		runNode      bool
	}{
		{
			name:         "Valid CAR file - Big Buck Bunny",
			carFileName:  "cars/bbb.car",
			expectedSize: 515027709,
			rootCIDs:     []string{"QmbvEbKtzaZEtyXMvD5HDnQRpcYkg4hAj9xqNLsi7arTq3"},
			expectError:  false,
		},
		{
			name:         "Valid CAR file - DOCX",
			carFileName:  "cars/docx.car",
			expectedSize: 34976,
			rootCIDs:     []string{"QmNdr9DzL38nQyASPbvSuzkTeX7FSjojyW6EMbs81sC4iv"},
			expectError:  false,
		},
		{
			name:         "Valid CAR file - File Tree",
			carFileName:  "cars/filetree.car",
			expectedSize: 70200,
			rootCIDs:     []string{"QmP68x398CKjjKSYHaqJr9iJdksM6TRTckhtcfw6HANt2F"},
			expectError:  false,
			runNode:      true,
		},
		{
			name:         "Valid CAR file - HAMT Tree",
			carFileName:  "cars/hamttree.car",
			expectedSize: 70252,
			rootCIDs:     []string{"bafybeiafoftayzdd4qsi6keef373dinlrzyibxdqnct64ymvjusovgw2yq"},
			expectError:  false,
		},
		{
			name:         "Invalid CAR file",
			carFileName:  "cars/invalid.car",
			expectedSize: 37,
			rootCIDs:     []string{},
			expectError:  true,
		},
		{
			name:         "Empty CAR file",
			carFileName:  "cars/empty.car",
			expectedSize: 75,
			rootCIDs:     []string{},
			expectError:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(t coreTesting.TB, ctx coreTesting.TestContext) {
				if !tc.runNode {
					err := core.GetProtocol(internal.ProtocolName).(*protocol.Protocol).GetNode().Close()
					require.NoError(t, err)
				}
				// Open the CAR file directly from ipfs-content fixtures
				carFile, err := os.Open(filepath.Join(fixtures.FixturesDir, tc.carFileName))
				require.NoError(t, err)
				defer func(carFile *os.File) {
					err := carFile.Close()
					require.NoError(t, err)
				}(carFile)

				// Verify the file size
				fileInfo, err := carFile.Stat()
				require.NoError(t, err)
				require.Equal(t, tc.expectedSize, fileInfo.Size(), "File size does not match expected size")
				// Get the protocol node once
				proto := core.GetProtocol(internal.ProtocolName).(*protocol.Protocol)
				node := proto.GetNode()

				// Create CAR processor
				processor, err := protocol.NewCARBlockProcessor(carFile)

				// For invalid CAR files, expect processor creation to fail
				if tc.name == "Invalid CAR file" {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)

				// Call the ProcessBlocks function
				processedCIDs, _, err := protocol.ProcessBlocks(ctx, processor, proto.GetBlockstoreFlusher())

				// Assert the error
				if tc.expectError {
					require.Error(t, err)
				} else {
					require.NoError(t, err)

					// Rewind the CAR file
					_, err = carFile.Seek(0, io.SeekStart)
					require.NoError(t, err)

					// Create new block reader
					cr, err := car.NewBlockReader(carFile)
					require.NoError(t, err)

					// Create map for faster lookups
					processedMap := make(map[string]struct{})
					for _, c := range processedCIDs {
						processedMap[c.String()] = struct{}{}
					}

					// Walk through all blocks
					for {
						block, err := cr.Next()
						if err == io.EOF {
							break
						}
						require.NoError(t, err)

						// Verify block is in processed list
						_, exists := processedMap[block.Cid().String()]
						require.True(t, exists, "block %s not found in processed list", block.Cid())

						// Mark block as ready
						err = proto.GetMetadataStore().MarkBlockReady(block.Cid(), true)
						if err != nil {
							require.NoError(t, err)
						}

						// Verify block can be retrieved
						retrieved, err := node.GetBlock(ctx, block.Cid())
						require.NoError(t, err)
						require.Equal(t, block.RawData(), retrieved.RawData(), "retrieved block data doesn't match original")
					}
				}
			},
				GetStandardTestOptions()...,
			)
		})
	}
}
