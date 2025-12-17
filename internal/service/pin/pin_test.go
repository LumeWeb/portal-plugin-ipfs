package pin

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	coreMocks "go.lumeweb.com/portal/core/testing/mocks"
	"go.lumeweb.com/portal/db/types"
	"go.lumeweb.com/queryutil"
	"gorm.io/datatypes"
)

var TestOptions = coreTesting.CombineOptions(
	coreTesting.WithServiceFactory(pluginCore.PIN_SERVICE, NewPinService),
	coreTesting.WithMockServiceFactory(pluginCore.FILE_MANAGER_SERVICE, mocks.NewMockFileManagerService),
	util.GetProtocolMock(),
	coreTesting.WithProtocolConfig(internal.ProtocolName, &pluginConfig.ProtocolConfig{}),
	coreTesting.WithSQLitePluginMigrations(
		internal.ProtocolName, migrations.GetSQLite(),
	),
)

func TestPinService_AddPin(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		pinService := core.GetService[pluginCore.IPFSPinService](ctx, pluginCore.PIN_SERVICE)

		// Generate a CID from a test string
		testString := "test data"
		testCID := util.GenerateTestCID(t, testString)

		testPin := &pluginDb.IPFSPin{
			CID:       testCID.Bytes(),
			RequestID: types.NewBinUUID(),
		}

		// Act
		addedPin, err := pinService.AddPin(context.Background(), testPin)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, addedPin)
		assert.Equal(tb, testPin.CID, addedPin.CID)

		// Verify that the pin exists in the database
		var retrievedPin pluginDb.IPFSPin
		result := ctx.DB().Where("request_id = ?", testPin.RequestID).First(&retrievedPin)
		require.NoError(tb, result.Error)
		assert.Equal(tb, testPin.CID, retrievedPin.CID)
	}, TestOptions)
}

func TestPinService_GetPinByRequestID(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		pinService := core.GetService[pluginCore.IPFSPinService](ctx, pluginCore.PIN_SERVICE)

		// Generate a CID from a test string
		testString := "test data"
		testCID := util.GenerateTestCID(t, testString)

		testPin := &pluginDb.IPFSPin{
			CID:       testCID.Bytes(),
			RequestID: types.NewBinUUID(),
		}

		// Add the pin to the database
		result := ctx.DB().Create(testPin)
		require.NoError(tb, result.Error)

		// Act
		retrievedPin, err := pinService.GetPinByRequestID(context.Background(), testPin.RequestID)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, retrievedPin)
		assert.Equal(tb, testPin.CID, retrievedPin.CID)
	}, TestOptions)
}

func TestPinService_GetPinByRequestID_NotFound(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		pinService := core.GetService[pluginCore.IPFSPinService](ctx, pluginCore.PIN_SERVICE)

		nonExistentRequestID := types.NewBinUUID()

		// Act
		retrievedPin, err := pinService.GetPinByRequestID(context.Background(), nonExistentRequestID)

		// Assert
		require.NoError(tb, err)
		assert.Nil(tb, retrievedPin)
	}, TestOptions)
}

func TestPinService_ListPins(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		pinService := core.GetService[pluginCore.IPFSPinService](ctx, pluginCore.PIN_SERVICE)

		// Generate a CID from a test string
		testString1 := "test data 1"
		testCID1 := util.GenerateTestCID(t, testString1)

		// Generate a CID from a test string
		testString2 := "test data 2"
		testCID2 := util.GenerateTestCID(t, testString2)

		pin1 := &pluginDb.IPFSPin{
			CID:       testCID1.Bytes(),
			RequestID: types.NewBinUUID(),
		}
		pin2 := &pluginDb.IPFSPin{
			CID:       testCID2.Bytes(),
			RequestID: types.NewBinUUID(),
		}

		// Add the pins to the database
		result := ctx.DB().Create(pin1)
		require.NoError(tb, result.Error)
		result = ctx.DB().Create(pin2)
		require.NoError(tb, result.Error)

		// Act
		pins, total, err := pinService.ListPins(context.Background(), nil, nil, queryutil.DefaultPagination)

		// Assert
		require.NoError(tb, err)
		assert.Len(tb, pins, 2)
		assert.Equal(tb, int64(2), total)
	}, TestOptions)
}

func TestPinService_ReplacePin(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		pinService := core.GetService[pluginCore.IPFSPinService](ctx, pluginCore.PIN_SERVICE)

		// Generate a CID from a test string
		testString1 := "test data 1"
		testCID1 := util.GenerateTestCID(t, testString1)

		// Generate a CID from a test string
		testString2 := "test data 2"
		testCID2 := util.GenerateTestCID(t, testString2)

		oldPin := &pluginDb.IPFSPin{
			CID:       testCID1.Bytes(),
			RequestID: types.NewBinUUID(),
		}

		// Add the old pin to the database
		result := ctx.DB().Create(oldPin)
		require.NoError(tb, result.Error)

		newPin := &pluginDb.IPFSPin{
			CID:       testCID2.Bytes(),
			RequestID: types.NewBinUUID(),
		}

		userID := uint(123)
		userIP := "192.168.1.1"

		// Act
		replacedPin, err := pinService.ReplacePin(context.Background(), userID, userIP, oldPin.RequestID, newPin)

		// Assert
		require.NoError(tb, err)
		assert.NotNil(tb, replacedPin)
		assert.Equal(tb, newPin.CID, replacedPin.CID)

		// Verify that the old pin is deleted and the new pin exists in the database
		var retrievedOldPin pluginDb.IPFSPin
		result = ctx.DB().Where("request_id = ?", oldPin.RequestID).First(&retrievedOldPin)
		assert.Error(tb, result.Error) // Expecting record not found error

		var retrievedNewPin pluginDb.IPFSPin
		result = ctx.DB().Where("request_id = ?", newPin.RequestID).First(&retrievedNewPin)
		require.NoError(tb, result.Error)
		assert.Equal(tb, newPin.CID, retrievedNewPin.CID)
	}, TestOptions)
}

func TestPinService_DeletePin(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		pinService := core.GetService[pluginCore.IPFSPinService](ctx, pluginCore.PIN_SERVICE)
		corePinService := core.GetService[*coreMocks.MockPinService](ctx, core.PIN_SERVICE)
		fileManagerService := core.GetService[*mocks.MockFileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)

		// Generate a CID from a test string
		testString := "test data"
		testCID := util.GenerateTestCID(t, testString)

		testPin := &pluginDb.IPFSPin{
			CID:       testCID.Bytes(),
			RequestID: types.NewBinUUID(),
		}

		// Setup mock expectations
		hash := internal.NewIPFSHash(testCID)
		corePinService.EXPECT().GetPinByHash(hash, uint(0)).Return(nil, nil).Maybe()
		corePinService.EXPECT().DeletePinByHash(hash, uint(0)).Return(nil).Maybe()
		fileManagerService.EXPECT().DeleteFilePathSmart(mock.Anything, uint(0), testCID.Bytes()).Return(nil).Maybe()

		// Add the pin to the database
		result := ctx.DB().Create(testPin)
		require.NoError(tb, result.Error)

		// Act
		err := pinService.DeletePin(context.Background(), testPin.RequestID)

		// Assert
		require.NoError(tb, err)

		// Verify that the pin is deleted from the database
		var retrievedPin pluginDb.IPFSPin
		result = ctx.DB().Where("request_id = ?", testPin.RequestID).First(&retrievedPin)
		assert.Error(tb, result.Error) // Expecting record not found error
	}, TestOptions)
}

func TestPinService_UpdatePinStatus(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		pinService := core.GetService[pluginCore.IPFSPinService](ctx, pluginCore.PIN_SERVICE)

		// Generate a CID from a test string
		testString := "test data"
		testCID := util.GenerateTestCID(t, testString)

		testPin := &pluginDb.IPFSPin{
			CID:       testCID.Bytes(),
			RequestID: types.NewBinUUID(),
			Status:    pluginDb.PinningStatusQueued, // Initial status
		}

		// Add the pin to the database
		result := ctx.DB().Create(testPin)
		require.NoError(tb, result.Error)

		newStatus := pluginDb.PinningStatusPinning
		info := datatypes.JSON([]byte(`{"message": "Pinning in progress"}`))

		// Act
		err := pinService.UpdatePinStatus(context.Background(), testPin.RequestID, newStatus, info)

		// Assert
		require.NoError(tb, err)

		// Verify that the pin status is updated in the database
		var retrievedPin pluginDb.IPFSPin
		result = ctx.DB().Where("request_id = ?", testPin.RequestID).First(&retrievedPin)
		require.NoError(tb, result.Error)
		assert.Equal(tb, newStatus, retrievedPin.Status)
		assert.Equal(tb, info, retrievedPin.Info)
	}, TestOptions)
}

func TestPinService_UpdatePinStatus_NotFound(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		pinService := core.GetService[pluginCore.IPFSPinService](ctx, pluginCore.PIN_SERVICE)
		nonExistentRequestID := types.NewBinUUID()
		newStatus := pluginDb.PinningStatusFailed
		info := datatypes.JSON([]byte(`{"message": "Pinning failed"}`))

		// Act
		err := pinService.UpdatePinStatus(context.Background(), nonExistentRequestID, newStatus, info)

		// Assert
		require.Error(tb, err)
		assert.Contains(tb, err.Error(), fmt.Sprintf("no pin found with request ID: %s", nonExistentRequestID.String()))
	}, TestOptions)
}
