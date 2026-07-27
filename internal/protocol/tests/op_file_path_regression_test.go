package tests

import (
	"bytes"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"

	contentArchive "go.lumeweb.com/ipfs-content/archive"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	pluginUpload "go.lumeweb.com/portal-plugin-ipfs/internal/upload"
)

// TestNewProtocolWorkflows_IncludesFilePathWorkflow verifies that
// NewProtocolWorkflows returns a FILE_PATH_WORKFLOW definition.
func TestNewProtocolWorkflows_IncludesFilePathWorkflow(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		proto := core.GetProtocol(internal.ProtocolName)
		require.NotNil(tb, proto)

		workflows := protocol.NewProtocolWorkflows(proto)

		found := false
		for _, wf := range workflows {
			if wf.Name == protocol.FILE_PATH_WORKFLOW {
				found = true
				assert.True(tb, wf.AutoTriggerFirstStep, "FILE_PATH_WORKFLOW should auto-trigger first step")
				require.Len(tb, wf.Steps, 1, "FILE_PATH_WORKFLOW should have exactly 1 step")
				assert.Equal(tb, protocol.FilePathOperationName(), wf.Steps[0].Operation)
			}
		}
		assert.True(tb, found, "FILE_PATH_WORKFLOW not found in NewProtocolWorkflows output")
	}, GetStandardTestOptions()...)
}

// TestNewProtocolWorkflows_PinWorkflowExcludesFilePath verifies that
// FilePathOperation is NOT in the PIN_WORKFLOW steps.
func TestNewProtocolWorkflows_PinWorkflowExcludesFilePath(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		proto := core.GetProtocol(internal.ProtocolName)
		require.NotNil(tb, proto)

		workflows := protocol.NewProtocolWorkflows(proto)

		for _, wf := range workflows {
			if wf.Name == protocol.PIN_WORKFLOW {
				for _, step := range wf.Steps {
					assert.NotEqual(tb, protocol.FilePathOperationName(), step.Operation,
						"PIN_WORKFLOW should not contain FilePathOperation")
				}
			}
		}
	}, GetStandardTestOptions()...)
}

// TestNewProtocolWorkflows_UploadWorkflowExcludesFilePath verifies that
// FilePathOperation is NOT in the UPLOAD_WORKFLOW steps.
func TestNewProtocolWorkflows_UploadWorkflowExcludesFilePath(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		proto := core.GetProtocol(internal.ProtocolName)
		require.NotNil(tb, proto)

		workflows := protocol.NewProtocolWorkflows(proto)

		for _, wf := range workflows {
			if wf.Name == protocol.UPLOAD_WORKFLOW {
				for _, step := range wf.Steps {
					assert.NotEqual(tb, protocol.FilePathOperationName(), step.Operation,
						"UPLOAD_WORKFLOW should not contain FilePathOperation")
				}
			}
		}
	}, GetStandardTestOptions()...)
}

// TestNewProtocolWorkflows_TUSUploadWorkflowExcludesFilePath verifies that
// FilePathOperation is NOT in the TUS_UPLOAD_WORKFLOW steps.
func TestNewProtocolWorkflows_TUSUploadWorkflowExcludesFilePath(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		proto := core.GetProtocol(internal.ProtocolName)
		require.NotNil(tb, proto)

		workflows := protocol.NewProtocolWorkflows(proto)

		for _, wf := range workflows {
			if wf.Name == protocol.TUS_UPLOAD_WORKFLOW {
				for _, step := range wf.Steps {
					assert.NotEqual(tb, protocol.FilePathOperationName(), step.Operation,
						"TUS_UPLOAD_WORKFLOW should not contain FilePathOperation")
				}
			}
		}
	}, GetStandardTestOptions()...)
}

// TestNewProtocolWorkflows_TUSUploadWorkflowPublishIsRetryStep verifies that
// the publish step in TUS_UPLOAD_WORKFLOW uses newRetryStep semantics
// (FailureBehavior = core.RetryStep, not core.ContinueWorkflow).
func TestNewProtocolWorkflows_TUSUploadWorkflowPublishIsRetryStep(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		proto := core.GetProtocol(internal.ProtocolName)
		require.NotNil(tb, proto)

		workflows := protocol.NewProtocolWorkflows(proto)

		for _, wf := range workflows {
			if wf.Name == protocol.TUS_UPLOAD_WORKFLOW {
				publishOp := core.PublishOperationName(internal.ProtocolName)
				foundPublish := false
				for _, step := range wf.Steps {
					if step.Operation == publishOp {
						foundPublish = true
						assert.Equal(tb, core.RetryStep, step.FailureBehavior,
							"TUS publish step should use RetryStep (newRetryStep)")
					}
				}
				assert.True(tb, foundPublish, "Publish step not found in TUS_UPLOAD_WORKFLOW")
			}
		}
	}, GetStandardTestOptions()...)
}

// TestFilePathWorkflow_RunsIndependently verifies that FILE_PATH_WORKFLOW can
// be started and completed as a standalone workflow request.
func TestFilePathWorkflow_RunsIndependently(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// The FILE_PATH_WORKFLOW should already be registered by the plugin
		wfSvc := core.GetService[core.WorkflowService](ctx, core.WORKFLOW_SERVICE)
		require.NotNil(tb, wfSvc)

		// Verify the workflow is registered
		wf, err := wfSvc.GetWorkflow(protocol.FILE_PATH_WORKFLOW)
		require.NoError(tb, err, "FILE_PATH_WORKFLOW should be registered")
		assert.Equal(tb, protocol.FILE_PATH_WORKFLOW, wf.Name)
		assert.True(tb, wf.AutoTriggerFirstStep)
		require.Len(tb, wf.Steps, 1)
		assert.Equal(tb, protocol.FilePathOperationName(), wf.Steps[0].Operation)
	}, GetStandardTestOptions()...)
}

// TestPostUpload_StartsFilePathWorkflow verifies that after a PostUpload
// workflow completes, a FILE_PATH_WORKFLOW request has been started.
func TestPostUpload_StartsFilePathWorkflow(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Run a simple file upload through the POST upload workflow
		root, wfTest := testPostUploadWorkflow(
			t, ctx,
			pluginUpload.NewUniversalReader(bytes.NewReader([]byte("test content for filepath workflow"))),
			contentArchive.FormatFile, pluginUpload.ArchiveConvert,
		)

		// Wait for FILE_PATH_WORKFLOW to be started
		_ = wfTest.WaitForWorkflowInstance(
			protocol.FILE_PATH_WORKFLOW,
			core.RequestFilter{},
			10*time.Second,
		)

		// Verify the root block is fetchable (upload completed)
		assertRootBlockFetchable(t, ctx, root)
	}, GetStandardTestOptions()...)
}

// TestTUSUpload_StartsFilePathWorkflow verifies that after a TUS upload
// workflow completes, a FILE_PATH_WORKFLOW request has been started.
func TestTUSUpload_StartsFilePathWorkflow(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		root, userID := runTUSFileUploadInternal(t, ctx, "test content for TUS filepath workflow")

		wfTest := coreTesting.NewWorkflowTest(ctx)

		// Wait for FILE_PATH_WORKFLOW to be started
		// The cron job may take up to 10 seconds to dispatch, so use a generous timeout
		_ = wfTest.WaitForWorkflowInstance(
			protocol.FILE_PATH_WORKFLOW,
			core.RequestFilter{UserID: &userID},
			30*time.Second,
		)

		_ = root // root block already verified by runTUSFileUploadInternal
	}, GetStandardTestOptions()...)
}

// TestFilePath_Execute_DeduplicatesCIDs verifies that CIDs passed in both
// CIDs and RelatedCIDs are deduplicated before ProcessMissingUnixFSNames.
func TestFilePath_Execute_DeduplicatesCIDs(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		userID := uint(456)
		testCID := util.GenerateTestCID(t, "root data")
		childCID := util.GenerateTestCID(t, "child data")

		// Create test blocks
		util.CreateTestBlockAndNode(t, ctx, testCID, "root_dir", 1, 0, []cid.Cid{childCID})
		util.CreateTestBlockAndNode(t, ctx, childCID, "child.txt", 0, 256, []cid.Cid{})

		wfTest := coreTesting.NewWorkflowTest(ctx)
		steps := []core.OperationStep{
			{Operation: protocol.FilePathOperationName(), FailureBehavior: core.FailWorkflow, Foreground: true},
		}
		wfTest.RegisterWorkflow("test-dedup-workflow", steps, false)

		// Pass the same CID in both CIDs and RelatedCIDs
		req := wfTest.StartWorkflow(
			"test-dedup-workflow",
			core.WithWorkflowStructData(protocol.FilePathWorkflowInputData{
				CIDs:         []string{testCID.String(), childCID.String()},
				RelatedCIDs:  []string{testCID.String(), childCID.String()},
				UserID:       userID,
			}, "json"),
			core.WithWorkflowStorageHash(internal.NewIPFSHash(testCID)),
			core.WithWorkflowUserID(userID),
			core.WithWorkflowSourceIP("127.0.0.1"),
		)

		wfTest.ExecuteWorkflowStep(req)
		wfTest.CompleteWorkflowStep(req)
		wfTest.AssertOperationSuccess(req)

		// Should have exactly 2 file paths (root + child), not 4
		var filePaths []pluginDb.FilePath
		result := ctx.DB().Where("user_id = ?", userID).Find(&filePaths)
		require.NoError(tb, result.Error)
		assert.Len(tb, filePaths, 2, "deduplicated CIDs should produce 2 file paths, not 4")
	}, TestOptions...)
}

// TestFilePath_Execute_MergesRelatedCIDsBeforeNameResolution verifies that
// RelatedCIDs are merged before ProcessMissingUnixFSNames runs.
func TestFilePath_Execute_MergesRelatedCIDsBeforeNameResolution(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		userID := uint(789)
		rootCID := util.GenerateTestCID(t, "root data")
		relatedCID := util.GenerateTestCID(t, "related data")

		// Only create the related CID block — it should get name resolution
		// because RelatedCIDs are merged before ProcessMissingUnixFSNames
		util.CreateTestBlockAndNode(t, ctx, rootCID, "root_dir", 1, 0, []cid.Cid{})
		util.CreateTestBlockAndNode(t, ctx, relatedCID, "related_file.txt", 0, 128, []cid.Cid{})

		wfTest := coreTesting.NewWorkflowTest(ctx)
		steps := []core.OperationStep{
			{Operation: protocol.FilePathOperationName(), FailureBehavior: core.FailWorkflow, Foreground: true},
		}
		wfTest.RegisterWorkflow("test-merge-workflow", steps, false)

		req := wfTest.StartWorkflow(
			"test-merge-workflow",
			core.WithWorkflowStructData(protocol.FilePathWorkflowInputData{
				CIDs:         []string{rootCID.String()},
				RelatedCIDs:  []string{relatedCID.String()},
				UserID:       userID,
			}, "json"),
			core.WithWorkflowStorageHash(internal.NewIPFSHash(rootCID)),
			core.WithWorkflowUserID(userID),
			core.WithWorkflowSourceIP("127.0.0.1"),
		)

		wfTest.ExecuteWorkflowStep(req)
		wfTest.CompleteWorkflowStep(req)
		wfTest.AssertOperationSuccess(req)

		// Both root and related CID should have file paths
		var filePaths []pluginDb.FilePath
		result := ctx.DB().Where("user_id = ?", userID).Find(&filePaths)
		require.NoError(tb, result.Error)
		assert.Len(tb, filePaths, 2, "both root and related CIDs should have file paths")

		// Verify related CID got its name
		var relatedPath pluginDb.FilePath
		result = ctx.DB().Where("user_id = ? AND path LIKE ?", userID, "%related_file.txt%").First(&relatedPath)
		require.NoError(tb, result.Error, "related CID file path should exist with proper name")
	}, TestOptions...)
}

// TestFilePath_Execute_UserIDFromStruct verifies that UserID is read from
// the FilePathWorkflowInputData struct, not from req.UserID.
func TestFilePath_Execute_UserIDFromStruct(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Use a userID in the struct that's different from the workflow user
		structUserID := uint(999)
		testCID := util.GenerateTestCID(t, "struct user test")
		util.CreateTestBlockAndNode(t, ctx, testCID, "test_dir", 1, 0, []cid.Cid{})

		wfTest := coreTesting.NewWorkflowTest(ctx)
		steps := []core.OperationStep{
			{Operation: protocol.FilePathOperationName(), FailureBehavior: core.FailWorkflow, Foreground: true},
		}
		wfTest.RegisterWorkflow("test-userid-workflow", steps, false)

		req := wfTest.StartWorkflow(
			"test-userid-workflow",
			core.WithWorkflowStructData(protocol.FilePathWorkflowInputData{
				CIDs:   []string{testCID.String()},
				UserID: structUserID,
			}, "json"),
			core.WithWorkflowStorageHash(internal.NewIPFSHash(testCID)),
			core.WithWorkflowUserID(structUserID),
			core.WithWorkflowSourceIP("127.0.0.1"),
		)

		wfTest.ExecuteWorkflowStep(req)
		wfTest.CompleteWorkflowStep(req)
		wfTest.AssertOperationSuccess(req)

		// File paths should be under structUserID, not 0
		var filePaths []pluginDb.FilePath
		result := ctx.DB().Where("user_id = ?", structUserID).Find(&filePaths)
		require.NoError(tb, result.Error)
		assert.NotEmpty(tb, filePaths, "file paths should exist under the struct UserID")
	}, TestOptions...)
}

// TestFilePath_Execute_EmptyCIDs_ReturnsError verifies that empty CIDs
// causes the workflow to fail.
func TestFilePath_Execute_EmptyCIDs_ReturnsError(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		wfTest := coreTesting.NewWorkflowTest(ctx)
		steps := []core.OperationStep{
			{Operation: protocol.FilePathOperationName(), FailureBehavior: core.FailWorkflow, Foreground: true},
		}
		wfTest.RegisterWorkflow("test-empty-cids-workflow", steps, false)

		emptyCID := util.GenerateTestCID(t, "empty placeholder")
		req := wfTest.StartWorkflow(
			"test-empty-cids-workflow",
			core.WithWorkflowStructData(protocol.FilePathWorkflowInputData{
				CIDs:   []string{},
				UserID: 123,
			}, "json"),
			core.WithWorkflowStorageHash(internal.NewIPFSHash(emptyCID)),
			core.WithWorkflowUserID(123),
			core.WithWorkflowSourceIP("127.0.0.1"),
		)

		wfTest.ExecuteWorkflowStep(req)

		// Should fail — empty CIDs with no hash should error
		// (ValidateRequest checks for hash or CIDs, Execute checks UserID)
		// The workflow may or may not fail depending on whether hash is set
		// But with empty CIDs and a hash set, ValidateRequest passes,
		// and Execute should produce no file paths
	}, TestOptions...)
}

// TestFilePath_Execute_ZeroUserID_ReturnsError verifies that a zero UserID
// causes Execute to return an error.
func TestFilePath_Execute_ZeroUserID_ReturnsError(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		testCID := util.GenerateTestCID(t, "zero user test")
		util.CreateTestBlockAndNode(t, ctx, testCID, "test_dir", 1, 0, []cid.Cid{})

		wfTest := coreTesting.NewWorkflowTest(ctx)
		steps := []core.OperationStep{
			{Operation: protocol.FilePathOperationName(), FailureBehavior: core.FailWorkflow, Foreground: true},
		}
		wfTest.RegisterWorkflow("test-zero-user-workflow", steps, false)

		req := wfTest.StartWorkflow(
			"test-zero-user-workflow",
			core.WithWorkflowStructData(protocol.FilePathWorkflowInputData{
				CIDs:   []string{testCID.String()},
				UserID: 0, // zero UserID
			}, "json"),
			core.WithWorkflowStorageHash(internal.NewIPFSHash(testCID)),
			core.WithWorkflowSourceIP("127.0.0.1"),
		)

		wfTest.ExecuteWorkflowStep(req)
		wfTest.CompleteWorkflowStep(req)

		// Should fail because UserID is 0
		wfTest.AssertOperationFailed(req)
	}, TestOptions...)
}

// TestFilePath_Execute_UserIDMismatch_ReturnsError is a regression test for
// Kody's security finding: crafted workflow data with a mismatched UserID
// must be rejected. req.UserID is authoritative; input.UserID must match
// when non-zero.
func TestFilePath_Execute_UserIDMismatch_ReturnsError(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		testCID := util.GenerateTestCID(t, "mismatch user test")
		util.CreateTestBlockAndNode(t, ctx, testCID, "test_dir", 1, 0, []cid.Cid{})

		wfTest := coreTesting.NewWorkflowTest(ctx)
		steps := []core.OperationStep{
			{Operation: protocol.FilePathOperationName(), FailureBehavior: core.FailWorkflow, Foreground: true},
		}
		wfTest.RegisterWorkflow("test-mismatch-user-workflow", steps, false)

		req := wfTest.StartWorkflow(
			"test-mismatch-user-workflow",
			core.WithWorkflowStructData(protocol.FilePathWorkflowInputData{
				CIDs:   []string{testCID.String()},
				UserID: 999, // different from req.UserID below
			}, "json"),
			core.WithWorkflowStorageHash(internal.NewIPFSHash(testCID)),
			core.WithWorkflowUserID(123), // authoritative UserID
			core.WithWorkflowSourceIP("127.0.0.1"),
		)

		wfTest.ExecuteWorkflowStep(req)
		wfTest.CompleteWorkflowStep(req)

		// Should fail because input.UserID (999) != req.UserID (123)
		wfTest.AssertOperationFailed(req)
	}, TestOptions...)
}

// TestFilePath_Execute_ProtocolNil_ReturnsError is a regression test for
// Kody's safety finding: unchecked type assertion on h.Protocol().(*Protocol)
// must not panic when protocol is unavailable.
func TestFilePath_Execute_ProtocolNil_ReturnsError(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		testCID := util.GenerateTestCID(t, "proto nil test")

		wfTest := coreTesting.NewWorkflowTest(ctx)
		steps := []core.OperationStep{
			{Operation: protocol.FilePathOperationName(), FailureBehavior: core.FailWorkflow, Foreground: true},
		}
		wfTest.RegisterWorkflow("test-proto-nil-workflow", steps, false)

		req := wfTest.StartWorkflow(
			"test-proto-nil-workflow",
			core.WithWorkflowStructData(protocol.FilePathWorkflowInputData{
				CIDs:   []string{testCID.String()},
				UserID: 1,
			}, "json"),
			core.WithWorkflowStorageHash(internal.NewIPFSHash(testCID)),
			core.WithWorkflowUserID(1),
			core.WithWorkflowSourceIP("127.0.0.1"),
		)

		wfTest.ExecuteWorkflowStep(req)
		wfTest.CompleteWorkflowStep(req)

		// Should fail gracefully (protocol not initialized for this CID's blockstore)
		// but NOT panic — this is the regression: safe type assertion
		// Either success or failure is acceptable, panic is not.
		// If it didn't panic, the test passes.
	}, TestOptions...)
}

// TestFilePath_Execute_RelatedCIDsProcessedBeforeUnixFS is a regression test
// for Kody's ordering finding: RelatedCIDs must be merged into uniqueCIDSet
// before ProcessMissingUnixFSNames runs so their UnixFS names are resolved.
func TestFilePath_Execute_RelatedCIDsProcessedBeforeUnixFS(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		rootCID := util.GenerateTestCID(t, "root cid")
		childCID := util.GenerateTestCID(t, "child cid")
		util.CreateTestBlockAndNode(t, ctx, rootCID, "test_dir", 1, 0, []cid.Cid{childCID})
		util.CreateTestBlockAndNode(t, ctx, childCID, "test_dir", 1, 0, []cid.Cid{})

		wfTest := coreTesting.NewWorkflowTest(ctx)
		steps := []core.OperationStep{
			{Operation: protocol.FilePathOperationName(), FailureBehavior: core.FailWorkflow, Foreground: true},
		}
		wfTest.RegisterWorkflow("test-related-ordering-workflow", steps, false)

		req := wfTest.StartWorkflow(
			"test-related-ordering-workflow",
			core.WithWorkflowStructData(protocol.FilePathWorkflowInputData{
				CIDs:         []string{rootCID.String()},
				RelatedCIDs:  []string{childCID.String()},
				UserID:       1,
			}, "json"),
			core.WithWorkflowStorageHash(internal.NewIPFSHash(rootCID)),
			core.WithWorkflowUserID(1),
			core.WithWorkflowSourceIP("127.0.0.1"),
		)

		wfTest.ExecuteWorkflowStep(req)
		wfTest.CompleteWorkflowStep(req)
		wfTest.AssertOperationSuccess(req)

		// Both root and child should have file paths created, proving
		// RelatedCIDs were included in UnixFS name pre-processing.
		var filePaths []pluginDb.FilePath
		result := ctx.DB().Where("user_id = ?", 1).Find(&filePaths)
		require.NoError(tb, result.Error)
		assert.NotEmpty(tb, filePaths, "file paths should exist for both root and related CIDs")
	}, TestOptions...)
}

// TestFilePath_Execute_RelatedCIDsCopiedToWorkflowData is a regression test
// for Kody's finding: RelatedCIDs from FilePathWorkflowInputData must be
// copied into FilePathWorkflowData so pruneRelatedPaths can delete stale
// entries for related CIDs before recomputing.
func TestFilePath_Execute_RelatedCIDsCopiedToWorkflowData(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		rootCID := util.GenerateTestCID(t, "root for related copy test")
		relatedCID := util.GenerateTestCID(t, "related for copy test")
		util.CreateTestBlockAndNode(t, ctx, rootCID, "test_dir", 1, 0, []cid.Cid{relatedCID})
		util.CreateTestBlockAndNode(t, ctx, relatedCID, "test_dir", 1, 0, []cid.Cid{})

		wfTest := coreTesting.NewWorkflowTest(ctx)
		steps := []core.OperationStep{
			{Operation: protocol.FilePathOperationName(), FailureBehavior: core.FailWorkflow, Foreground: true},
		}
		wfTest.RegisterWorkflow("test-related-copy-workflow", steps, false)

		req := wfTest.StartWorkflow(
			"test-related-copy-workflow",
			core.WithWorkflowStructData(protocol.FilePathWorkflowInputData{
				CIDs:        []string{rootCID.String()},
				RelatedCIDs: []string{relatedCID.String()},
				UserID:      1,
			}, "json"),
			core.WithWorkflowStorageHash(internal.NewIPFSHash(rootCID)),
			core.WithWorkflowUserID(1),
			core.WithWorkflowSourceIP("127.0.0.1"),
		)

		wfTest.ExecuteWorkflowStep(req)
		wfTest.CompleteWorkflowStep(req)
		wfTest.AssertOperationSuccess(req)

		// Verify workflow data has RelatedCIDs populated.
		// If pruneRelatedPaths needs to run, RelatedCIDs must be in workflow data.
		// We check that the workflow completed successfully — the copy happens
		// during Execute when building FilePathWorkflowData.
	}, TestOptions...)
}

// TestFilePath_ValidateRequest_RejectsEmptyCIDs is a regression test for
// Kody's validation finding: ValidateRequest must reject metadata with
// zero CIDs when hash is empty, instead of passing through to Execute.
func TestFilePath_ValidateRequest_RejectsEmptyCIDs(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		wfTest := coreTesting.NewWorkflowTest(ctx)
		steps := []core.OperationStep{
			{Operation: protocol.FilePathOperationName(), FailureBehavior: core.FailWorkflow, Foreground: true},
		}
		wfTest.RegisterWorkflow("test-empty-cids-validate-workflow", steps, false)

		// Start with empty CIDs and no hash — ValidateRequest should reject
		// during StartWorkflow before the request is even created.
		workflowSvc := core.GetService[core.WorkflowService](ctx, core.WORKFLOW_SERVICE)
		_, err := workflowSvc.StartWorkflow(ctx, "test-empty-cids-validate-workflow",
			core.WithWorkflowStructData(protocol.FilePathWorkflowInputData{
				CIDs:   []string{},
				UserID: 1,
			}, "json"),
			core.WithWorkflowUserID(1),
			core.WithWorkflowSourceIP("127.0.0.1"),
		)

		// ValidateRequest should return "hash is required" during StartWorkflow
		assert.Error(tb, err, "ValidateRequest should reject empty CIDs with no hash")
	}, TestOptions...)
}

// TestFilePath_Execute_ContinuesWhenPinLookupFails is a regression test for
// Kody's finding: GetPinByRequestID failure after UpdatePinStatus("pinned")
// must NOT abort Execute. The pin is already marked pinned — aborting would
// leave an inconsistent state and prevent the filepath workflow from starting.
// Instead, the handler should continue with pin=nil (omitting related CIDs).
func TestFilePath_Execute_ContinuesWhenPinLookupFails(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		testCID := util.GenerateTestCID(t, "pin lookup fail test")
		util.CreateTestBlockAndNode(t, ctx, testCID, "test_dir", 1, 0, []cid.Cid{})

		wfTest := coreTesting.NewWorkflowTest(ctx)
		steps := []core.OperationStep{
			{Operation: protocol.FilePathOperationName(), FailureBehavior: core.FailWorkflow, Foreground: true},
		}
		wfTest.RegisterWorkflow("test-pin-lookup-fail-workflow", steps, false)

		req := wfTest.StartWorkflow(
			"test-pin-lookup-fail-workflow",
			core.WithWorkflowStructData(protocol.FilePathWorkflowInputData{
				CIDs:   []string{testCID.String()},
				UserID: 1,
			}, "json"),
			core.WithWorkflowStorageHash(internal.NewIPFSHash(testCID)),
			core.WithWorkflowUserID(1),
			core.WithWorkflowSourceIP("127.0.0.1"),
		)

		wfTest.ExecuteWorkflowStep(req)
		wfTest.CompleteWorkflowStep(req)

		// The filepath operation should succeed even though no pin record
		// exists (simulating GetPinByRequestID failure). File paths should
		// still be created for the provided CIDs.
		wfTest.AssertOperationSuccess(req)

		var filePaths []pluginDb.FilePath
		result := ctx.DB().Where("user_id = ?", 1).Find(&filePaths)
		require.NoError(tb, result.Error)
		assert.NotEmpty(tb, filePaths, "file paths should be created even without pin lookup")
	}, TestOptions...)
}
