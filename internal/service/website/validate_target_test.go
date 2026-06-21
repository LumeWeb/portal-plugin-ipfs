package website

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal/core"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"gorm.io/gorm"
)

const (
	// TestPeerID1 is a valid libp2p ed25519 peer ID
	TestPeerID1 = "12D3KooWRhWS6DXi1U1YnJ5r9E6KpSDHGbZAznXif4T9qDjHeEfE"
	// TestPeerID2 is another valid libp2p ed25519 peer ID
	TestPeerID2 = "12D3KooWR4Mq4DEB9Nhz41sDDRKtqnWHjB9qzTmnPogUJLjxTD8z"
	// TestCIDv1Libp2pKey is a valid CIDv1 with libp2p-key codec
	TestCIDv1Libp2pKey = "k51qzi5uqu5dlts3p5vfpw8kneqp5ye1ttb2jlt8qkt5mq9f2gvgmet6sec29r"
)

func TestValidateTarget_IPNS_PeerIDFirst(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		// Use a valid peer ID (base36 encoding - the format from IPNS publish)
		peerIDString := TestPeerID1

		// Act & Assert
		err := websiteService.(*WebsiteServiceDefault).validateTarget(string(pluginDb.WebsiteTargetTypeIPNS), peerIDString)
		require.NoError(tb, err, "Valid peer ID should pass validation")
	}, TestOptions)
}

func TestValidateTarget_IPNS_CIDv1Libp2pKey(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		// Use a valid CIDv1 with libp2p-key codec
		cidv1Key := TestCIDv1Libp2pKey

		// Act & Assert
		err := websiteService.(*WebsiteServiceDefault).validateTarget(string(pluginDb.WebsiteTargetTypeIPNS), cidv1Key)
		require.NoError(tb, err, "Valid CIDv1 with libp2p-key codec should pass validation")
	}, TestOptions)
}

func TestValidateTarget_IPNS_InvalidPeerID(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		// Invalid peer ID string
		invalidPeerID := "not-valid-peer-id"

		// Act & Assert
		err := websiteService.(*WebsiteServiceDefault).validateTarget(string(pluginDb.WebsiteTargetTypeIPNS), invalidPeerID)
		require.Error(tb, err, "Invalid peer ID should fail validation")
		require.Contains(tb, err.Error(), "invalid IPNS", "Error should mention IPNS validation failure")
	}, TestOptions)
}

func TestValidateTarget_IPNS_InvalidCID(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		// Invalid CID that looks like CID but has unsupported encoding
		invalidCID := "invalid-cid-format"

		// Act & Assert
		err := websiteService.(*WebsiteServiceDefault).validateTarget(string(pluginDb.WebsiteTargetTypeIPNS), invalidCID)
		require.Error(tb, err, "Invalid CID should fail validation")
		require.Contains(tb, err.Error(), "invalid IPNS", "Error should mention IPNS validation failure")
	}, TestOptions)
}

func TestValidateTarget_IPNS_CIDNotLibp2pKey(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		// Valid CID but not libp2p-key codec (e.g., DagProtobuf CID)
		// Qm... is CIDv0, bafkrei... is CIDv1 with raw codec
		nonLibp2pKeyCID := "bafkreiem6tcea4zz7g2z4w4ocjp7t3ve5s7uoixxxdh2ikvmq3retdhsy"

		// Act & Assert
		err := websiteService.(*WebsiteServiceDefault).validateTarget(string(pluginDb.WebsiteTargetTypeIPNS), nonLibp2pKeyCID)
		require.Error(tb, err, "CID without libp2p-key codec should fail validation")
		require.Contains(tb, err.Error(), "invalid IPNS", "Error should mention IPNS validation failure")
	}, TestOptions)
}

func TestValidateTarget_IPFS_ValidCID(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		// Valid CID (should pass IPFS validation) - use util helper
		testCID := util.GenerateTestCID(t, "test-data-validation")

		// Act & Assert
		err := websiteService.(*WebsiteServiceDefault).validateTarget(string(pluginDb.WebsiteTargetTypeIPFS), testCID.String())
		require.NoError(tb, err, "Valid CID should pass IPFS validation")
	}, TestOptions)
}

func TestValidateTarget_IPFS_InvalidCID(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		// Invalid CID
		invalidCID := "not-a-valid-cid"

		// Act & Assert
		err := websiteService.(*WebsiteServiceDefault).validateTarget(string(pluginDb.WebsiteTargetTypeIPFS), invalidCID)
		require.Error(tb, err, "Invalid CID should fail validation")
		require.Contains(tb, err.Error(), "invalid CID", "Error should mention CID validation failure")
	}, TestOptions)
}

func TestValidateTarget_UnknownType(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		// Any hash with unknown type should fail type validation
		anyHash := "some-hash-value"

		// Act & Assert
		err := websiteService.(*WebsiteServiceDefault).validateTarget("unknown_type", anyHash)
		require.Error(tb, err, "Unknown target type should fail validation")
		require.Contains(tb, err.Error(), "invalid target", "Error should mention invalid target type")
	}, TestOptions)
}

func TestValidateTarget_IPNS_MultiplePeerIDFormats(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		// Test multiple valid peer ID formats and CIDv1 libp2p-key formats
		validPeerIDs := []string{
			TestPeerID1,         // ed25519 peer ID (from IPNS publish)
			TestPeerID2,         // Another valid ed25519 peer ID
			TestCIDv1Libp2pKey,  // CIDv1 libp2p-key (fallback)
		}

		// Act & Assert
		for _, peerID := range validPeerIDs {
			err := websiteService.(*WebsiteServiceDefault).validateTarget(string(pluginDb.WebsiteTargetTypeIPNS), peerID)
			require.NoError(tb, err, "Peer ID %s should pass validation", peerID)
		}
	}, TestOptions)
}

func TestValidateIPFSTarget_PinnedCID(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)
		svc := websiteService.(*WebsiteServiceDefault)

		mockPinSvc := core.GetService[*mocks.MockIPFSPinService](ctx, pluginCore.PIN_SERVICE)
		require.NotNil(tb, mockPinSvc)

		testCID := util.GenerateTestCID(t, "pinned-content")
		testUserID := uint(1)

		// Mock: pin exists and is pinned
		mockPinSvc.EXPECT().GetPinByCIDAndUser(mock.Anything, testCID, testUserID).
			Return(&pluginDb.IPFSPin{
				UserID: testUserID,
				CID:    testCID.Bytes(),
				Status: pluginDb.PinningStatusPinned,
			}, nil).Once()

		// Act & Assert — should succeed because the CID is pinned
		err := svc.validateIPFSTarget(ctx, testUserID, testCID.String())
		require.NoError(tb, err, "Pinned CID should pass validation")
	}, TestOptions)
}

func TestValidateIPFSTarget_UnpinnedCID(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)
		svc := websiteService.(*WebsiteServiceDefault)

		mockPinSvc := core.GetService[*mocks.MockIPFSPinService](ctx, pluginCore.PIN_SERVICE)
		require.NotNil(tb, mockPinSvc)

		testCID := util.GenerateTestCID(t, "unpinned-content")
		testUserID := uint(1)

		// Mock: no pin record found
		mockPinSvc.EXPECT().GetPinByCIDAndUser(mock.Anything, testCID, testUserID).
			Return(nil, gorm.ErrRecordNotFound).Once()

		// Act & Assert — should fail because the CID is not pinned
		err := svc.validateIPFSTarget(ctx, testUserID, testCID.String())
		require.Error(tb, err, "Unpinned CID should fail validation")
		require.ErrorIs(tb, err, ErrCIDNotPinned)
	}, TestOptions)
}

func TestValidateIPFSTarget_QueuedPin(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)
		svc := websiteService.(*WebsiteServiceDefault)

		mockPinSvc := core.GetService[*mocks.MockIPFSPinService](ctx, pluginCore.PIN_SERVICE)
		require.NotNil(tb, mockPinSvc)

		testCID := util.GenerateTestCID(t, "queued-content")
		testUserID := uint(1)

		// Mock: pin exists but is still queued
		mockPinSvc.EXPECT().GetPinByCIDAndUser(mock.Anything, testCID, testUserID).
			Return(&pluginDb.IPFSPin{
				UserID: testUserID,
				CID:    testCID.Bytes(),
				Status: pluginDb.PinningStatusQueued,
			}, nil).Once()

		// Act & Assert — should fail because the pin is still queued, not pinned
		err := svc.validateIPFSTarget(ctx, testUserID, testCID.String())
		require.Error(tb, err, "Queued (not yet pinned) CID should fail validation")
		require.ErrorIs(tb, err, ErrCIDNotPinned)
	}, TestOptions)
}

func TestValidateIPFSTarget_InvalidCID(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)
		svc := websiteService.(*WebsiteServiceDefault)

		// Act & Assert — invalid CID string should return an error
		err := svc.validateIPFSTarget(ctx, 1, "not-a-valid-cid")
		require.Error(tb, err, "Invalid CID string should fail validation")
	}, TestOptions)
}

func TestValidateIPNSKeyResolution_KeyExists(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)
		svc := websiteService.(*WebsiteServiceDefault)

		mockIPNSKeySvc := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, mockIPNSKeySvc)

		testUserID := uint(1)
		peerIDStr := TestPeerID1

		// Mock: key exists and belongs to the user
		mockIPNSKeySvc.EXPECT().GetPrivateKeyByPeerID(mock.Anything, peerIDStr).
			Return(nil, testUserID, nil).Once()

		// Act & Assert — should succeed because the key exists and belongs to the user
		err := svc.validateIPNSKeyResolution(ctx, testUserID, peerIDStr)
		require.NoError(tb, err, "Valid IPNS key belonging to the user should pass validation")
	}, TestOptions)
}

func TestValidateIPNSKeyResolution_KeyNotFound(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)
		svc := websiteService.(*WebsiteServiceDefault)

		mockIPNSKeySvc := core.GetService[*mocks.MockIPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
		require.NotNil(tb, mockIPNSKeySvc)

		// Act & Assert — should fail because no key exists for this peer ID
		peerIDStr := TestPeerID1
		mockIPNSKeySvc.EXPECT().GetPrivateKeyByPeerID(mock.Anything, peerIDStr).
			Return(nil, uint(0), gorm.ErrRecordNotFound).Once()

		err := svc.validateIPNSKeyResolution(ctx, 1, peerIDStr)
		require.Error(tb, err, "Non-existent IPNS key should fail validation")
		require.ErrorIs(tb, err, ErrIPNSKeyNotFound)
	}, TestOptions)
}

func TestIsValidIPNSTarget_ValidPeerID(t *testing.T) {
	// Valid peer ID should pass
	require.True(t, isValidIPNSTarget(TestPeerID1), "Valid peer ID should return true")
	require.True(t, isValidIPNSTarget(TestPeerID2), "Valid peer ID should return true")
}

func TestIsValidIPNSTarget_CIDv1Libp2pKey(t *testing.T) {
	// Valid CIDv1 with libp2p-key codec should pass
	require.True(t, isValidIPNSTarget(TestCIDv1Libp2pKey), "Valid CIDv1 libp2p-key should return true")
}

func TestIsValidIPNSTarget_InvalidPeerID(t *testing.T) {
	// Invalid peer ID should fail
	require.False(t, isValidIPNSTarget("not-valid-peer-id"), "Invalid peer ID should return false")
	require.False(t, isValidIPNSTarget(""), "Empty string should return false")
}

func TestIsValidIPNSTarget_InvalidCID(t *testing.T) {
	// Invalid CID format should fail
	require.False(t, isValidIPNSTarget("invalid-cid-format"), "Invalid CID should return false")
	require.False(t, isValidIPNSTarget("bogus"), "Bogus string should return false")
}

func TestIsValidIPNSTarget_CIDNotLibp2pKey(t *testing.T) {
	// Valid CID but not libp2p-key codec should fail
	nonLibp2pKeyCID := "bafkreiem6tcea4zz7g2z4w4ocjp7t3ve5s7uoixxxdh2ikvmq3retdhsy"
	require.False(t, isValidIPNSTarget(nonLibp2pKeyCID), "CID without libp2p-key codec should return false")
}

func TestIsValidIPNSTarget_CIDv0(t *testing.T) {
	// CIDv0 (Qm...) accidentally passes peer.Decode since both use base58btc
	// multihash encoding, but it's a content hash, not a peer ID.
	require.False(t, isValidIPNSTarget("QmWLqGsc1X914yZjFgqZ16uzPV69AZjrc4ioMemMhoHWee"), "CIDv0 should not be valid IPNS target")
}

func TestIsValidIPNSTarget_MultipleValidFormats(t *testing.T) {
	// Test all valid formats
	validTargets := []string{
		TestPeerID1,
		TestPeerID2,
		TestCIDv1Libp2pKey,
	}

	for _, target := range validTargets {
		require.True(t, isValidIPNSTarget(target), "Target %s should be valid", target)
	}
}


