package tests

import (
	"io"
	"os"
	"path"
	"runtime"
	"testing"

	"github.com/ipld/go-car/v2"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/file_manager"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/pin"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/service"

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
			carFileName:  "../../testing/fixtures/cars/bbb.car",
			expectedSize: 515008217,
			rootCIDs:     []string{"bafybeiehmyjhx3ucuy4gejj5q3nqgrp2uaiqnebqnfvchal63bsnwlxg7y"},
			expectError:  false,
		},
		{
			name:         "Valid CAR file - DOCX",
			carFileName:  "../../testing/fixtures/cars/docx.car", // Ensure this file exists
			expectedSize: 34658,
			rootCIDs:     []string{"bafybeie4meysywjfzp6a6d4jo4t2zz262qduvesub647ov5g2rvc4doas4"},
			expectError:  false,
		},
		{
			name:         "Valid CAR file - File Tree",
			carFileName:  "../../testing/fixtures/cars/filetree.car",
			expectedSize: 497705023,
			rootCIDs:     []string{"bafybeiccfclkdtucu6y4yc5cpr6y3yuinr67svmii46v5cfcrkp47ihehy"},
			expectError:  false,
			runNode:      true,
		},
		// TODO: Create HAMT Tree dataset
		/*		{
				name:         "Valid CAR file - HAMT Tree",
				carFileName:  "testdata/hamttree.car", // Ensure this file exists
				expectedSize: 102400,
				rootCIDs:     []string{"bafybeiaysi4s6lnjev27ln5icwm6tueaw2vdykrtjkwiphwekaywqhcjze"},
				expectError:  false,
			},*/
		{
			name:         "Invalid CAR file",
			carFileName:  "../../testing/fixtures/cars/invalid.car",
			expectedSize: 37,
			rootCIDs:     []string{},
			expectError:  true,
		},
		{
			name:         "Empty CAR file",
			carFileName:  "../../testing/fixtures/cars/empty.car",
			expectedSize: 75,
			rootCIDs:     []string{},
			expectError:  true,
		},
	}

	_, file, _, _ := runtime.Caller(0)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			coreTesting.RunTestCaseWithDB(t, func(t coreTesting.TB, ctx coreTesting.TestContext) {
				if !tc.runNode {
					err := core.GetProtocol(internal.ProtocolName).(*protocol.Protocol).GetNode().Close()
					require.NoError(t, err)
				}
				// Open the CAR file directly
				carFile, err := os.Open(path.Join(path.Dir(file), tc.carFileName))
				require.NoError(t, err)
				defer func(carFile *os.File) {
					err = carFile.Close()
					if err != nil {
						require.NoError(t, err)
					}
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
				processedCIDs, _, err := protocol.ProcessBlocks(ctx, processor)

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
				coreTesting.WithStatefulMockRenterService(),
				coreTesting.WithServiceFactory(core.UPLOAD_SERVICE, service.NewMetadataService),
				coreTesting.WithServiceFactory(core.STORAGE_SERVICE, service.NewStorageService),
				coreTesting.WithServiceFactory(core.CRON_SERVICE, service.NewCronService),
				coreTesting.WithServiceFactory(core.REQUEST_SERVICE, service.NewRequestService),
				coreTesting.WithServiceFactory(core.WORKFLOW_SERVICE, service.NewWorkflowCoordinator),
				coreTesting.WithServiceFactory(pluginCore.FILE_MANAGER_SERVICE, filemanager.NewFileManagerService),
				coreTesting.WithServiceFactory(pluginCore.PIN_SERVICE, pin.NewPinService),
				coreTesting.WithProtocol(internal.ProtocolName, protocol.NewProtocol),
				coreTesting.WithProtocolConfig(internal.ProtocolName, &pluginConfig.ProtocolConfig{}),
				coreTesting.WithSQLitePluginMigrations(
					internal.ProtocolName, migrations.GetSQLite(),
				),
			)
		})
	}
}
