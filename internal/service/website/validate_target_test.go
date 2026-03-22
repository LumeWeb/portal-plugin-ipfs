package website

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.lumeweb.com/portal/core"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	coreTesting "go.lumeweb.com/portal/core/testing"
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

func TestValidateIPNSTarget_ValidPeerID(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		// Use a valid peer ID
		peerIDString := TestPeerID1

		// Act & Assert
		valid, err := websiteService.(*WebsiteServiceDefault).validateIPNSTarget(ctx, peerIDString)
		require.NoError(tb, err, "Valid peer ID should pass validation")
		require.True(tb, valid, "Valid peer ID should return true")
	}, TestOptions)
}

func TestValidateIPNSTarget_CIDv1Libp2pKey(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		// Use a valid CIDv1 with libp2p-key codec
		cidv1Key := TestCIDv1Libp2pKey

		// Act & Assert
		valid, err := websiteService.(*WebsiteServiceDefault).validateIPNSTarget(ctx, cidv1Key)
		require.NoError(tb, err, "Valid CIDv1 with libp2p-key codec should pass validation")
		require.True(tb, valid, "Valid CIDv1 should return true")
	}, TestOptions)
}

func TestValidateIPNSTarget_InvalidPeerID(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		// Invalid peer ID string
		invalidPeerID := "not-valid-peer-id"

		// Act & Assert
		valid, err := websiteService.(*WebsiteServiceDefault).validateIPNSTarget(ctx, invalidPeerID)
		require.Error(t, err, "Invalid peer ID should fail validation")
		require.False(t, valid, "Invalid peer ID should return false")
	}, TestOptions)
}

func TestValidateIPNSTarget_InvalidCID(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		// Invalid CID format
		invalidCID := "invalid-cid-format"

		// Act & Assert
		valid, err := websiteService.(*WebsiteServiceDefault).validateIPNSTarget(ctx, invalidCID)
		require.Error(t, err, "Invalid CID should fail validation")
		require.False(t, valid, "Invalid CID should return false")
	}, TestOptions)
}

func TestValidateIPNSTarget_CIDNotLibp2pKey(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		// Valid CID but not libp2p-key codec
		nonLibp2pKeyCID := "bafkreiem6tcea4zz7g2z4w4ocjp7t3ve5s7uoixxxdh2ikvmq3retdhsy"

		// Act & Assert
		valid, err := websiteService.(*WebsiteServiceDefault).validateIPNSTarget(ctx, nonLibp2pKeyCID)
		require.Error(t, err, "CID without libp2p-key codec should fail validation")
		require.False(t, valid, "CID without libp2p-key codec should return false")
	}, TestOptions)
}

func TestValidateIPNSTarget_MultipleValidFormats(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		// Arrange
		websiteService := core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
		require.NotNil(tb, websiteService)

		// Test multiple valid peer ID formats and CIDv1 libp2p-key formats
		validTargets := []string{
			TestPeerID1,        // ed25519 peer ID
			TestPeerID2,        // Another valid ed25519 peer ID
			TestCIDv1Libp2pKey, // CIDv1 libp2p-key
		}

		// Act & Assert
		for _, target := range validTargets {
			valid, err := websiteService.(*WebsiteServiceDefault).validateIPNSTarget(ctx, target)
			require.NoError(tb, err, "Target %s should pass validation", target)
			require.True(tb, valid, "Target %s should return true", target)
		}
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


