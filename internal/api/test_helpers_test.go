package api

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/util"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
	"go.lumeweb.com/portal/db/models"
	"go.lumeweb.com/portal/db/types"
)

const (
	// TestPeerID is a valid libp2p peer ID for testing
	TestPeerID = "12D3KooWR4Mq4DEB9Nhz41sDDRKtqnWHjB9qzTmnPogUJLjxTD8z"

	// TestCID is a valid IPFS CID for testing
	TestCID = "bafybeieffnocaq7t4w4daagvydl32igft5oziyyaebqr6vx6rb3fwh2ab4"

	// TestUserEmail is the email address for test users
	TestUserEmail = "test@example.com"

	// TestUserPassword is the password for test users
	TestUserPassword = "example"

	// TestDomain is a domain name for testing website functionality
	TestDomain = "example.com"

	// TestIPNSName is a valid IPNS name for testing (derived from TestPeerID)
	TestIPNSName = "k51qzi5uqu5dlts3p5vfpw8kneqp5ye1ttb2jlt8qkt5mq9f2gvgmet6sec29r"
)

// mockHelper provides common mock setup functions for tests
type mockHelper struct {
	ctx coreTesting.TestContext
	t   *testing.T
}

func newMockHelper(t *testing.T, ctx coreTesting.TestContext) *mockHelper {
	return &mockHelper{
		ctx: ctx,
		t:   t,
	}
}

// createMockIPNSRecord creates a mock IPNS record with the specified CID value
func createMockIPNSRecord(t *testing.T, cidStr string) *ipns.Record {
	targetCID, err := cid.Decode(cidStr)
	require.NoError(t, err, "Failed to decode CID for mock IPNS record")

	ipnsPath := path.FromCid(targetCID)

	// Generate a test private key for creating the record
	privKey, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 2048)
	require.NoError(t, err, "Failed to generate private key for mock IPNS record")

	// Create the IPNS record
	record, err := ipns.NewRecord(privKey, ipnsPath, 1, time.Now().Add(time.Hour), time.Hour)
	require.NoError(t, err, "Failed to create mock IPNS record")

	return record
}

// createMockIPFSPin creates a standardized IPFSPin mock object
func createMockIPFSPin(userID uint, testCID cid.Cid, pinID types.BinaryUUID, status pluginDb.PinningStatus) *pluginDb.IPFSPin {
	return &pluginDb.IPFSPin{
		RequestID: pinID,
		UserID:    userID,
		CID:       testCID.Bytes(),
		Name:      "test",
		Status:    status,
		Origins:   nil,
		Meta:      nil,
		Delegates: nil,
		Info:      nil,
	}
}

// createMockUnixFSNode creates a standardized UnixFSNode mock object
func createMockUnixFSNode(testCID cid.Cid) *pluginDb.UnixFSNode {
	return &pluginDb.UnixFSNode{
		Name:      "test",
		Type:      2,
		BlockSize: 1024,
		ChildCID:  []cid.Cid{testCID},
	}
}

// SetupPinServiceMocks configures common pin service mock expectations
func (m *mockHelper) SetupPinServiceMocks(userID uint, testCID cid.Cid, pinID types.BinaryUUID) *mocks.MockIPFSPinService {
	mockPinService := core.GetService[*mocks.MockIPFSPinService](m.ctx, pluginCore.PIN_SERVICE)

	// Setup AddPin expectation
	mockPinService.EXPECT().AddPin(mock.Anything, mock.AnythingOfType("*db.IPFSPin")).Return(createMockIPFSPin(userID, testCID, pinID, pluginDb.PinningStatusQueued), nil).Maybe()

	// Setup GetPinByRequestID expectation
	mockPinService.EXPECT().GetPinByRequestID(mock.Anything, pinID).Return(createMockIPFSPin(userID, testCID, pinID, pluginDb.PinningStatusPinned), nil).Maybe()

	// Setup ListPinsForUser expectation
	mockPinService.EXPECT().ListPinsForUser(mock.Anything, userID, mock.Anything, mock.Anything, mock.Anything).Return([]*pluginDb.IPFSPin{createMockIPFSPin(userID, testCID, pinID, pluginDb.PinningStatusPinned)}, int64(1), nil).Maybe()

	// Setup DeletePinForUser error expectation for 404 cases (wrong user or not found)
	mockPinService.EXPECT().DeletePinForUser(mock.Anything, userID+1, pinID).Return(fmt.Errorf("pin not found for user")).Maybe()

	// Setup DeletePinForUser expectation
	mockPinService.EXPECT().DeletePinForUser(mock.Anything, userID, pinID).Return(nil).Maybe()

	// Setup ReplacePinForUser error expectation for 404 cases (wrong user or not found)
	mockPinService.EXPECT().ReplacePinForUser(mock.Anything, userID+1, mock.AnythingOfType("string"), pinID, mock.AnythingOfType("*db.IPFSPin")).Return(nil, fmt.Errorf("pin not found for user")).Maybe()

	// Setup ReplacePinForUser expectation
	mockPinService.EXPECT().ReplacePinForUser(mock.Anything, userID, mock.AnythingOfType("string"), pinID, mock.AnythingOfType("*db.IPFSPin")).Return(createMockIPFSPin(userID, testCID, pinID, pluginDb.PinningStatusPinned), nil).Maybe()

	// Setup GetPinByCIDAndUser expectation
	mockPinService.EXPECT().GetPinByCIDAndUser(mock.Anything, mock.AnythingOfType("cid.Cid"), userID).Return(createMockIPFSPin(userID, testCID, pinID, pluginDb.PinningStatusPinned), nil).Maybe()

	// Setup GetPinByRequestIDForUser expectation for 404 cases (wrong user or not found)
	mockPinService.EXPECT().GetPinByRequestIDForUser(mock.Anything, userID+1, pinID).Return(nil, nil).Maybe()

	// Setup GetPinByRequestIDForUser expectation
	mockPinService.EXPECT().GetPinByRequestIDForUser(mock.Anything, userID, pinID).Return(createMockIPFSPin(userID, testCID, pinID, pluginDb.PinningStatusPinned), nil).Maybe()

	return mockPinService
}

// SetupBlockServiceMocks configures common block service mock expectations
func (m *mockHelper) SetupBlockServiceMocks(testCID cid.Cid) *mocks.MockBlockService {
	mockBlockService := core.GetService[*mocks.MockBlockService](m.ctx, pluginCore.BLOCK_SERVICE)

	mockBlockService.EXPECT().GetBlockMeta(mock.Anything, testCID).Return(createMockUnixFSNode(testCID), nil).Maybe()

	mockBlockService.EXPECT().GetBlockMetaBatch(mock.Anything, mock.AnythingOfType("[]cid.Cid")).Return(map[string]*pluginDb.UnixFSNode{
		testCID.String(): createMockUnixFSNode(testCID),
	}, nil).Maybe()

	return mockBlockService
}

// SetupUploadServiceMocks configures common upload service mock expectations
func (m *mockHelper) SetupUploadServiceMocks(expectedCID cid.Cid) *mocks.MockUploadService {
	mockUploadService := core.GetService[*mocks.MockUploadService](m.ctx, pluginCore.UPLOAD_SERVICE)

	mockUploadService.EXPECT().HandleUploadWithMode(mock.Anything, mock.Anything, mock.AnythingOfType("uint"), mock.AnythingOfType("upload.ArchiveMode")).Return(expectedCID, "", nil).Maybe()

	return mockUploadService
}

// SetupFileManagerServiceMocks configures file manager service
func (m *mockHelper) SetupFileManagerServiceMocks() *mocks.MockFileManagerService {
	mockFileManagerService := core.GetService[*mocks.MockFileManagerService](m.ctx, pluginCore.FILE_MANAGER_SERVICE)

	return mockFileManagerService
}

// SetupDNSServiceMocks configures DNS service mock expectations
func (m *mockHelper) SetupDNSServiceMocks() *mocks.MockDNSService {
	mockDNSService := core.GetService[*mocks.MockDNSService](m.ctx, pluginCore.DNS_SERVICE)

	return mockDNSService
}

// SetupWebsiteServiceMocks configures website service mock expectations
func (m *mockHelper) SetupWebsiteServiceMocks(domain string, website *pluginDb.Website) *mocks.MockWebsiteService {
	mockWebsiteService := core.GetService[*mocks.MockWebsiteService](m.ctx, pluginCore.WEBSITE_SERVICE)

	if website != nil {
		mockWebsiteService.EXPECT().GetWebsiteByDomain(mock.Anything, domain).Return(website, pluginDb.DomainNamespaceICANN, nil).Maybe()
	} else {
		mockWebsiteService.EXPECT().GetWebsiteByDomain(mock.Anything, domain).Return(nil, pluginDb.DomainNamespaceICANN, nil).Maybe()
	}

	return mockWebsiteService
}

// SetupIPNSServiceMocks configures IPNS service mock expectations
func (m *mockHelper) SetupIPNSServiceMocks(userID uint) *mocks.MockIPNSKeyService {
	return m.SetupIPNSServiceMocksWithOptions(userID, true)
}

// SetupIPNSServiceMocksNoDefaults configures IPNS service mocks without default Maybe() expectations
// Use this for tests that need to set up error cases or custom behavior
func (m *mockHelper) SetupIPNSServiceMocksNoDefaults(userID uint) *mocks.MockIPNSKeyService {
	return m.SetupIPNSServiceMocksWithOptions(userID, false)
}

// SetupIPNSServiceMocksWithOptions configures IPNS service mocks with optional default expectations
func (m *mockHelper) SetupIPNSServiceMocksWithOptions(userID uint, withDefaults bool) *mocks.MockIPNSKeyService {
	mockIPNSKeyService := core.GetService[*mocks.MockIPNSKeyService](m.ctx, pluginCore.IPNS_KEY_SERVICE)

	// Create mock IPNS key
	testPeerID, _ := peer.Decode(TestPeerID)
	testPeerIDMultihash := mh.Multihash(testPeerID)
	mockKey := &pluginDb.IPFSIPNSKey{
		ID:              1,
		UserID:          userID,
		Name:            "test-key",
		PeerIDMultihash: testPeerIDMultihash,
	}

	if withDefaults {
		// Setup common IPNS key service expectations with Maybe()
		mockIPNSKeyService.EXPECT().CreateKey(mock.Anything, userID, mock.AnythingOfType("string"), mock.AnythingOfType("int")).Return(mockKey, nil).Maybe()
		mockIPNSKeyService.EXPECT().ImportKey(mock.Anything, userID, mock.AnythingOfType("string"), mock.AnythingOfType("string")).Return(mockKey, nil).Maybe()
		// Note: ListKeys is not set up with Maybe() - tests should set this up explicitly to avoid conflicts
		mockIPNSKeyService.EXPECT().GetKeyByID(mock.Anything, userID, mock.AnythingOfType("uint")).Return(mockKey, nil).Maybe()
		mockIPNSKeyService.EXPECT().DeleteKey(mock.Anything, userID, mock.AnythingOfType("uint")).Return(nil).Maybe()
		mockIPNSKeyService.EXPECT().GetPrivateKeyByPeerID(mock.Anything, mock.AnythingOfType("string")).Return(nil, userID, nil).Maybe()
	}
	// No defaults when withDefaults is false - tests should set up expectations explicitly

	return mockIPNSKeyService
}

// SetupWorkflowServiceMock configures workflow service mock for tests that need pin workflows
func (m *mockHelper) SetupWorkflowServiceMock() {
	// Setup core.WorkflowService mock using the higher-level helper
	mockWorkflowService := core.GetService[*coreTesting.MockWorkflowService](m.ctx, core.WORKFLOW_SERVICE)
	// StartWorkflow is called with: context, workflowName, and 5 workflow options = 6 total workflow options
	mockWorkflowService.ExpectStartWorkflowWithExactArgs("ipfs.network.pin", (*models.Request)(nil), nil, 5)
}

// SetupUploadWorkflowServiceMock configures workflow service mock for tests that need upload workflows
func (m *mockHelper) SetupUploadWorkflowServiceMock() {
	// Setup core.WorkflowService mock using the higher-level helper
	mockWorkflowService := core.GetService[*coreTesting.MockWorkflowService](m.ctx, core.WORKFLOW_SERVICE)
	// StartWorkflow is called with: context, workflowName, and 5 workflow options = 6 total workflow options
	mockWorkflowService.ExpectStartWorkflowWithExactArgs("ipfs.upload", (*models.Request)(nil), nil, 5)
}

// SetupAllCommonMocks configures all common service mocks for basic test scenarios
func (m *mockHelper) SetupAllCommonMocks(userID uint, testCID cid.Cid, pinID types.BinaryUUID) {
	m.SetupPinServiceMocks(userID, testCID, pinID)
	m.SetupBlockServiceMocks(testCID)
	m.SetupFileManagerServiceMocks()
	m.SetupDNSServiceMocks()
}

// SetupAuthenticatedTest creates a test user, logs them in, and sets up common mocks
func (m *mockHelper) SetupAuthenticatedTest() (string, uint, cid.Cid, types.BinaryUUID) {
	token, userID := createTestUser(m.ctx)
	testCID := cid.MustParse(TestCID)
	pinID := types.NewBinUUID()

	m.SetupAllCommonMocks(userID, testCID, pinID)

	return token, userID, testCID, pinID
}

// SetupAuthenticatedTestWithCID creates a test user, logs them in, and sets up mocks with custom CID
func (m *mockHelper) SetupAuthenticatedTestWithCID(testCID cid.Cid) (string, uint) {
	token, userID := createTestUser(m.ctx)
	pinID := types.NewBinUUID()

	m.SetupAllCommonMocks(userID, testCID, pinID)

	return token, userID
}

func createTestUserAndLogin(ctx coreTesting.TestContext) (string, uint) {
	mockAuth := core.GetService[*coreTesting.MockAuthService](ctx, core.AUTH_SERVICE)
	token, user, err := mockAuth.CreateAndLoginUser(TestUserEmail, TestUserPassword)
	if err != nil {
		ctx.T().Fatalf("failed to create and login test user: %v", err)
	}

	return token, user.ID
}

// createTestUser creates a test user and returns a JWT token without expecting LoginPassword to be called
// This is for tests that make authenticated requests but don't call the login endpoint
func createTestUser(ctx coreTesting.TestContext) (string, uint) {
	// Generate test token using the jwt helper without expecting LoginPassword call
	jwtHelper := coreTesting.NewJWTHelper(ctx)
	token, err := jwtHelper.CreateLoginToken(1)
	if err != nil {
		ctx.T().Fatalf("failed to create test token: %v", err)
	}

	return token, 1
}

func setAuthHeader(req *http.Request, token string) {
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
}

// makeRequest creates and executes an unauthenticated API request, returning the response
func (m *mockHelper) makeRequest(method, url string, body []byte) *httptest.ResponseRecorder {
	req := m.ctx.NewAPIRequest(method, url, body)
	rec := httptest.NewRecorder()
	m.ctx.Router().ServeHTTP(rec, req)
	return rec
}

// makeAuthenticatedRequest creates and executes an authenticated API request, returning the response
func (m *mockHelper) makeAuthenticatedRequest(method, url string, token string, body []byte) *httptest.ResponseRecorder {
	req := m.ctx.NewAPIRequest(method, url, body)
	setAuthHeader(req, token)
	rec := httptest.NewRecorder()
	m.ctx.Router().ServeHTTP(rec, req)
	return rec
}

// testGatewaySecret returns the gateway secret from the environment.
// TestMain sets a default if GATEWAY_SECRET is unset.
func testGatewaySecret() string {
	return os.Getenv("GATEWAY_SECRET")
}

// makeGatewayAuthenticatedRequest creates and executes a gateway-authenticated API request, returning the response
func (m *mockHelper) makeGatewayAuthenticatedRequest(method, url string, gatewaySecret string, body []byte) *httptest.ResponseRecorder {
	req := m.ctx.NewAPIRequest(method, url, body)
	req.Header.Set("X-Gateway-Secret", gatewaySecret)
	rec := httptest.NewRecorder()
	m.ctx.Router().ServeHTTP(rec, req)
	return rec
}

// assertJSONResponse is a helper to assert JSON response structure
func (m *mockHelper) assertJSONResponse(t *testing.T, rec *httptest.ResponseRecorder, expectedStatus int, target interface{}) {
	assert.Equal(t, expectedStatus, rec.Code)
	err := json.Unmarshal(rec.Body.Bytes(), target)
	assert.NoError(t, err)
}

// createTestFileDataSet creates a standardized set of test file paths for common scenarios
func createTestFileDataSet(userID uint, testCID1, testCID2, testCID3 cid.Cid) []*pluginDb.FilePath {
	return []*pluginDb.FilePath{
		createMockFilePath(userID, testCID1, "/file1.txt", "file1.txt", false),
		createMockFilePath(userID, testCID2, "/file2.txt", "file2.txt", false),
		createMockFilePath(userID, testCID3, "/file3.txt", "file3.txt", false),
	}
}

// createTestDirectoryDataSet creates a standardized set of test file paths with directories
func createTestDirectoryDataSet(userID uint, testCID1, testCID2, testCID3 cid.Cid) []*pluginDb.FilePath {
	return []*pluginDb.FilePath{
		createMockFilePath(userID, testCID1, "/file1.txt", "file1.txt", false),
		createMockFilePath(userID, testCID2, "/test_dir", "test_dir", true),
		createMockFilePath(userID, testCID3, "/test_dir/file2.txt", "file2.txt", false),
	}
}

// createBreadcrumbsDataSet creates a standardized set of test file paths for breadcrumb tests
func createBreadcrumbsDataSet(userID uint, testCID1, testCID2, testCID3 cid.Cid) []*pluginDb.FilePath {
	return []*pluginDb.FilePath{
		createMockFilePath(userID, testCID1, "/test_dir", "test_dir", true),
		createMockFilePath(userID, testCID2, "/test_dir/subdir", "subdir", true),
		createMockFilePath(userID, testCID3, "/test_dir/subdir/file.txt", "file.txt", false),
	}
}

// setupMultiplePinServiceMocks configures pin service mocks for multiple CIDs
func (m *mockHelper) setupMultiplePinServiceMocks(userID uint, testCIDs []cid.Cid) {
	mockPinService := core.GetService[*mocks.MockIPFSPinService](m.ctx, pluginCore.PIN_SERVICE)

	for _, testCID := range testCIDs {
		pinID := types.NewBinUUID()
		mockPinService.EXPECT().GetPinByCIDAndUser(mock.Anything, mock.AnythingOfType("cid.Cid"), userID).Return(
			createMockIPFSPin(userID, testCID, pinID, pluginDb.PinningStatusPinned), nil).Maybe()
	}
}

func createMockFilePath(userID uint, testCID cid.Cid, path, name string, isDirectory bool) *pluginDb.FilePath {
	parentPath := util.CalculateParentPath(path)

	depth := 0
	if path != "/" {
		segments := strings.Split(strings.Trim(path, "/"), "/")
		depth = len(segments)
	}

	return &pluginDb.FilePath{
		UserID:      userID,
		CID:         testCID.Bytes(),
		Path:        path,
		Name:        name,
		Type:        0,
		Size:        1024,
		IsDirectory: isDirectory,
		IsOrphan:    false,
		ParentPath:  parentPath,
		Depth:       depth,
	}
}

type pinTestHelper struct {
	token string
	pinID string
	cid   cid.Cid
}

func setupPinTest(t *testing.T, ctx coreTesting.TestContext) *pinTestHelper {
	helper := newMockHelper(t, ctx)
	token, _, testCID, pinID := helper.SetupAuthenticatedTest()

	return &pinTestHelper{
		token: token,
		pinID: pinID.String(),
		cid:   testCID,
	}
}

func createTestCAR(t *testing.T) ([]byte, cid.Cid) {
	content := []byte("test file content")

	pref := cid.Prefix{
		Version:  1,
		Codec:    uint64(multicodec.Raw),
		MhType:   mh.SHA2_256,
		MhLength: -1,
	}

	c, err := pref.Sum(content)
	if err != nil {
		t.Fatalf("failed to create CID: %v", err)
	}

	tmpFile, err := os.CreateTemp("", "test-car-*.car")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	cdest, err := blockstore.OpenReadWrite(tmpFile.Name(), []cid.Cid{c}, car.WriteAsCarV1(true))
	if err != nil {
		t.Fatalf("failed to create CAR writer: %v", err)
	}

	blk, err := blocks.NewBlockWithCid(content, c)
	if err != nil {
		t.Fatalf("failed to create block: %v", err)
	}
	err = cdest.Put(context.Background(), blk)
	if err != nil {
		t.Fatalf("failed to write block: %v", err)
	}

	if err := cdest.Finalize(); err != nil {
		t.Fatalf("failed to finalize CAR: %v", err)
	}

	carData, err := os.ReadFile(tmpFile.Name())
	if err != nil {
		t.Fatalf("failed to read temp CAR file: %v", err)
	}

	return carData, c
}

// createTestZIPFile creates a test ZIP file for API testing
func createTestZIPFile(t *testing.T, content string) *bytes.Buffer {
	var buf bytes.Buffer
	zipWriter := zip.NewWriter(&buf)

	writer, err := zipWriter.Create("test.txt")
	require.NoError(t, err)

	_, err = writer.Write([]byte(content))
	require.NoError(t, err)

	err = zipWriter.Close()
	require.NoError(t, err)

	return &buf
}
