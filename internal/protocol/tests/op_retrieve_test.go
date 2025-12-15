package tests

import (
	"context"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

const KnownCID = "bafybeieffnocaq7t4w4daagvydl32igft5oziyyaebqr6vx6rb3fwh2ab4"

func TestRetrieveOperationHandler_Execute_Integration(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		_cid, _ := cid.Parse(KnownCID)

		// Create test user
		userSvc := core.GetService[core.UserService](ctx, core.USER_SERVICE)
		testUser, err := userSvc.CreateAccount("test-retrieve@example.com", "testpassword123", false)
		require.NoError(tb, err)

		model, err := dto.PinRequest{CID: KnownCID}.ToModel()
		require.NoError(tb, err)

		pinService := core.GetService[pluginCore.IPFSPinService](ctx, pluginCore.PIN_SERVICE)
		// Create a WorkflowTest instance.
		wfTest := coreTesting.NewWorkflowTest(ctx)
		wfTest.DisableWorkflow(protocol.PIN_WORKFLOW)

		// Act - Add the pin
		_pin, err := pinService.AddPin(context.Background(), model)
		require.NoError(tb, err)

		// Get the operation name
		operationName := core.RetrieveOperationName(internal.ProtocolName)

		// Start the workflow with the root CID as workflow data.
		req := wfTest.StartOperationWorkflow(operationName,
			core.WithWorkflowStructData(protocol.PinWorkflowData{
				PinRequestID: _pin.RequestID.ToUUID(),
			}, "json"),
			core.WithWorkflowStorageHash(internal.NewIPFSHash(_cid)),
			core.WithWorkflowUserID(testUser.ID),
			core.WithWorkflowSourceIP("127.0.0.1"))

		// Execute the workflow step.
		wfTest.ExecuteWorkflowStep(req)
		wfTest.CompleteWorkflowStep(req)

		// Assertions
		wfTest.AssertOperationSuccess(req)
		wfTest.AssertOperationStatusMessageContains(req, "Content retrieved from network")
		wfTest.AssertOperationStatusProgress(req, 100)

	},
		coreTesting.CombineOptions(GetCommonTestOptions(), GetDbTestOptions()),
	)
}
