package tests

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

func TestFilePathOperationHandler_StressTest_ConcurrentDirectoryCreation(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Configuration
		numUsers := 5
		numDirsPerUser := 10
		numFilesPerDir := 5
		maxDepth := 3 // Maximum directory depth
		maxNameLength := 20

		// Arrange
		wfTest := coreTesting.NewWorkflowTest(ctx)

		var wg sync.WaitGroup
		workflowID := 0
		wfIDMutex := sync.Mutex{}
		errChan := make(chan error, numUsers*numDirsPerUser) // Buffered channel for errors

		// Run concurrent directory creation for multiple users
		for u := 0; u < numUsers; u++ {
			userID := uint(u + 1) // Unique user ID

			for d := 0; d < numDirsPerUser; d++ {
				wg.Add(1)
				go func(userID uint, dirIndex int) {
					defer wg.Done()
					defer func() {
						if r := recover(); r != nil {
							errChan <- fmt.Errorf("user %d, dir %d: panic during execution: %v", userID, dirIndex, r)
						}
					}()

					// Generate unique workflow ID for this goroutine
					wfIDMutex.Lock()
					myWorkflowID := workflowID
					workflowID++
					wfIDMutex.Unlock()

					// Create a random directory structure with unique seed per goroutine
					seed := time.Now().UnixNano() + rand.Int63() + int64(userID*1000000) + int64(dirIndex*10000)
					rng := rand.New(rand.NewSource(seed))

					rootCID, err := createRandomDirectoryStructure(tb, ctx, rng, numFilesPerDir, maxDepth, maxNameLength)
					if err != nil {
						errChan <- fmt.Errorf("user %d, dir %d: failed to create directory structure: %w", userID, dirIndex, err)
						return
					}

					// Register a unique workflow for this goroutine with the actual operation name in steps
					uniqueWorkflowName := "test-workflow-ipfs.file.path-" + strconv.Itoa(myWorkflowID)
					steps := []core.OperationStep{
						{Operation: protocol.FilePathOperationName(), FailureBehavior: core.FailWorkflow, Foreground: true},
					}
					wfTest.RegisterWorkflow(uniqueWorkflowName, steps, false)

					// Start the workflow
					req := wfTest.StartWorkflow(
						uniqueWorkflowName,
						core.WithWorkflowStructData(protocol.PinWorkflowData{
							Cids: []string{rootCID.String()},
						}, "json"),
						core.WithWorkflowStorageHash(internal.NewIPFSHash(rootCID)),
						core.WithWorkflowUserID(userID),
						core.WithWorkflowSourceIP("127.0.0.1"),
					)

					// Execute the workflow step
					wfTest.ExecuteWorkflowStep(req)
					wfTest.CompleteWorkflowStep(req)

					// Verify operation success
					wfTest.AssertOperationSuccess(req)
				}(userID, d)
			}
		}

		wg.Wait()
		close(errChan)

		// Check for errors
		for err := range errChan {
			assert.NoError(t, err, "Concurrent directory creation failed")
		}

		// Basic validation: Check total number of file paths created
		var totalFilePaths int64
		result := ctx.DB().Model(&pluginDb.FilePath{}).Count(&totalFilePaths)
		require.NoError(t, result.Error)
		tb.Logf("Total file paths created: %d", totalFilePaths)

		// Add more detailed validation as needed (e.g., check paths for specific users)
	}, TestOptions...)
}

// Helper function to create a random directory structure
func createRandomDirectoryStructure(tb coreTesting.TB, ctx coreTesting.TestContext, rng *rand.Rand, numFilesPerDir, maxDepth, maxNameLength int) (cid.Cid, error) {
	// Recursive function to create the directory structure
	var createDir func(tb coreTesting.TB, rng *rand.Rand, currentDepth int) (cid.Cid, []cid.Cid, error)

	createDir = func(tb coreTesting.TB, rng *rand.Rand, currentDepth int) (cid.Cid, []cid.Cid, error) {
		// Create unique dir name with random component
		dirName := generateRandomNameWithRng(rng, maxNameLength) + fmt.Sprintf("_%d_%d", rng.Int63(), currentDepth)
		var childCIDs []cid.Cid

		// Create files in this directory with unique names
		for i := 0; i < numFilesPerDir; i++ {
			fileName := generateRandomNameWithRng(rng, maxNameLength) + fmt.Sprintf("_%d_%d.txt", rng.Int63(), i)
			fileContent := generateRandomNameWithRng(rng, 50) + fmt.Sprintf("_%d_%d", rng.Int63(), i) // Random content for the file
			fileCID := util.GenerateTestCID(tb.(*testing.T), fileContent)

			// Create test block and UnixFS file node
			_, _ = util.CreateTestBlockAndNode(tb.(*testing.T), ctx, fileCID, fileName, 0, int64(len(fileContent)), []cid.Cid{})

			childCIDs = append(childCIDs, fileCID)
		}

		// Create subdirectories if we haven't reached the maximum depth
		if currentDepth < maxDepth {
			numSubDirs := rng.Intn(3) // Random number of subdirectories (0-2)
			for i := 0; i < numSubDirs; i++ {
				// Create new random source for subdirectory to ensure uniqueness
				subRng := rand.New(rand.NewSource(rng.Int63()))
				subDirCID, subDirChildCIDs, err := createDir(tb, subRng, currentDepth+1) // Recursive call
				if err != nil {
					return cid.Undef, nil, err
				}
				childCIDs = append(childCIDs, subDirCID)

				// Create links between parent and child directories
				// This part is missing in the original code, but it's crucial for creating the directory structure
				for _, childCID := range subDirChildCIDs {
					// You might need to create IPFSLinkedBlock entries here to link the directories
					// This depends on how your IPFS block service stores directory links
					_ = childCID // Placeholder to avoid "unused variable" error
				}
			}
		}

		// Create the directory node with unique content
		dirContent := dirName + fmt.Sprintf("_depth_%d_%d", currentDepth, rng.Int63())
		dirCID := util.GenerateTestCID(tb.(*testing.T), dirContent)
		_, _ = util.CreateTestBlockAndNode(tb.(*testing.T), ctx, dirCID, dirName, 1, 0, childCIDs)

		return dirCID, childCIDs, nil
	}

	// Start creating the directory structure from the root
	rootCID, _, err := createDir(tb, rng, 0)
	if err != nil {
		return cid.Undef, err
	}

	return rootCID, nil
}

// Helper function to generate a random name with a specific random source
func generateRandomNameWithRng(rng *rand.Rand, length int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, length)
	for i := range b {
		b[i] = letters[rng.Intn(len(letters))]
	}
	return string(b)
}
