package tests

import (
	"fmt"
	"github.com/ipfs/go-cid"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/pin"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/portal/service"
	"testing"
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
		coreTesting.WithStatefulMockRenterService(),
		coreTesting.WithServiceFactory(core.UPLOAD_SERVICE, service.NewMetadataService),
		coreTesting.WithServiceFactory(core.STORAGE_SERVICE, service.NewStorageService),
		coreTesting.WithServiceFactory(core.CRON_SERVICE, service.NewCronService),
		coreTesting.WithServiceFactory(core.REQUEST_SERVICE, service.NewRequestService),
		coreTesting.WithServiceFactory(core.WORKFLOW_SERVICE, service.NewWorkflowCoordinator),
		coreTesting.WithServiceFactory(pluginCore.PIN_SERVICE, pin.NewPinService),
		coreTesting.WithProtocol(internal.ProtocolName, protocol.NewProtocol),
		coreTesting.WithProtocolConfig(internal.ProtocolName, &config.ProtocolConfig{}),
		coreTesting.WithSQLitePluginMigrations(
			internal.ProtocolName, migrations.GetSQLite(),
		),
	)
}
