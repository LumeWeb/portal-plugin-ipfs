package tests

import (
	"fmt"
	"testing"

	"github.com/ipfs/go-cid"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

func TestPinChildBlocksOperationHandler_Execute_Integration(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		wfTest := coreTesting.NewWorkflowTest(ctx)
		wfTest.DisableWorkflow(protocol.PIN_WORKFLOW)
		wfTest.DisableWorkflow(protocol.PIN_CHILD_BLOCK_WORKFLOW)

		// Known CID that has child blocks
		rootCid := "QmSnuWmxptJZdLJpKRarxBMS2Ju2oANVrgbr2xWbie9b2D"
		childCid := "QmSnuWmxptJZdLJpKRarxBMS2Ju2oANVrgbr2xWbie9b2D"

		// Create a workflow data struct
		workflowData := protocol.PinChildBlockWorkflowData{
			Cid: childCid,
		}

		// Get the operation name
		operationName := fmt.Sprintf("%s.pin.children", internal.ProtocolName)

		// Start the workflow with the child CID as workflow data.
		req := wfTest.StartOperationWorkflow(operationName,
			core.WithWorkflowStructData(workflowData, "json"),
			core.WithWorkflowStorageHash(internal.NewIPFSHash(cid.MustParse(rootCid))),
			core.WithWorkflowUserID(0),
			core.WithWorkflowSourceIP("127.0.0.1"))

		// Execute the workflow step.
		wfTest.ExecuteWorkflowStep(req)

		// Assertions
		wfTest.AssertOperationSuccess(req)
		wfTest.AssertOperationStatusMessageContains(req, "Child block pinned successfully")
		wfTest.AssertOperationStatusProgress(req, 100)

	},
		coreTesting.CombineOptions(GetCommonTestOptions(), GetDbTestOptions()),
	)
}
