package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
)

// TestConfirm_EmptyCIDs_CompletesInsteadOfRetrying is a regression test for
// the infinite step-executor retry loop. The pin workflow's confirm step is
// configured with FailureBehavior=RetryStep, and workflowData.Cids is only
// ever populated by the prior retrieve step. A request whose workflow data has
// no CIDs (e.g. a stale/orphaned request) can therefore never be fixed by
// retrying: the coordinator would reset the request to pending and re-queue a
// fresh step_executor cron forever.
//
// ConfirmOperationHandler must treat an empty Cids list as a no-op success so
// the workflow completes, instead of returning an error that triggers the
// RetryStep loop.
func TestConfirm_EmptyCIDs_CompletesInsteadOfRetrying(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		wfTest := coreTesting.NewWorkflowTest(ctx)

		// Mirror production: a single confirm step with RetryStep semantics.
		confirmOp := core.OperationName(internal.ProtocolName, "confirm")
		steps := []core.OperationStep{
			{Operation: confirmOp, FailureBehavior: core.RetryStep, Foreground: true},
		}
		wfTest.RegisterWorkflow("test-confirm-empty-cids-workflow", steps, false)

		// Start a workflow whose workflow data has zero CIDs and a valid hash
		// (to satisfy ConfirmOperationHandler.ValidateRequest).
		req := wfTest.StartWorkflow(
			"test-confirm-empty-cids-workflow",
			core.WithWorkflowStructData(protocol.PinWorkflowData{
				Cids: []string{},
			}, "json"),
			core.WithWorkflowStorageHash(internal.NewIPFSHash(util.GenerateTestCID(t, "confirm-empty-cids"))),
			core.WithWorkflowUserID(1),
			core.WithWorkflowSourceIP("127.0.0.1"),
		)

		// Before the fix this returned a retried error (re-queueing a cron
		// job) indefinitely. With the fix it succeeds so the step completes.
		wfTest.ExecuteWorkflowStep(req)
		wfTest.CompleteWorkflowStep(req)
		wfTest.AssertOperationSuccess(req)

		// The confirm step leaves no pin records behind (no CIDs to process),
		// but the workflow itself must be terminal, not stuck pending.
		_, err := core.GetService[core.RequestService](ctx, core.REQUEST_SERVICE).GetRequest(ctx, req.ID)
		require.NoError(tb, err)
	}, GetStandardTestOptions()...)
}
