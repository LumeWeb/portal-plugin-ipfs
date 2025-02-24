package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/samber/lo"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	"go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db/migrations"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/block"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/pin"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/upload"
	"go.lumeweb.com/portal/db/models"
	"go.lumeweb.com/portal/service"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"

	"github.com/stretchr/testify/assert"
	"go.lumeweb.com/portal/core"
	coreTesting "go.lumeweb.com/portal/core/testing"
)

var TestOptions = coreTesting.CombineOptions(
	coreTesting.WithStatefulMockRenterService(),
	coreTesting.WithServiceFactory(core.CRON_SERVICE, service.NewCronService),
	coreTesting.WithServiceFactory(core.UPLOAD_SERVICE, service.NewMetadataService),
	coreTesting.WithServiceFactory(core.PIN_SERVICE, service.NewPinService),
	coreTesting.WithServiceFactory(core.REQUEST_SERVICE, service.NewRequestService),
	coreTesting.WithServiceFactory(core.WORKFLOW_SERVICE, service.NewWorkflowCoordinator),
	coreTesting.WithServiceFactory(core.USER_SERVICE, service.NewUserService),
	coreTesting.WithServiceFactory(core.AUTH_SERVICE, service.NewAuthService),
	coreTesting.WithServiceFactory(core.STORAGE_SERVICE, service.NewStorageService),
	coreTesting.WithServiceFactory(pluginCore.PIN_SERVICE, pin.NewPinService),
	coreTesting.WithServiceFactory(pluginCore.BLOCK_SERVICE, block.NewBlockService),
	coreTesting.WithServiceFactory(pluginCore.UPLOAD_SERVICE, upload.NewUploadService),
	coreTesting.WithAPI(internal.ProtocolName, NewAPI),
	coreTesting.WithAPIID(internal.ProtocolName),
	coreTesting.WithProtocol(internal.ProtocolName, protocol.NewProtocol),
	coreTesting.WithProtocolConfig(internal.ProtocolName, config.ProtocolConfig{}),
	coreTesting.WithSQLitePluginMigrations(
		internal.ProtocolName, migrations.GetSQLite(),
	),
	coreTesting.WithCron(),
)

func createTestUserAndLogin(ctx coreTesting.TestContext) string {
	userSvc := core.GetService[core.UserService](ctx, core.USER_SERVICE)
	authSvc := core.GetService[core.AuthService](ctx, core.AUTH_SERVICE)

	_, err := userSvc.CreateAccount("test@example.com", "example", false)
	if err != nil {
		ctx.T().Fatalf("failed to create test user: %v", err)
	}

	token, _, err := authSvc.LoginPassword("test@example.com", "example", "127.0.0.1", false)
	if err != nil {
		ctx.T().Fatalf("failed to login test user: %v", err)
	}

	return token
}

func setAuthHeader(req *http.Request, token string) {
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
}

type pinTestHelper struct {
	token string
	pinID string
	cid   cid.Cid
}

func setupPinTest(t *testing.T, ctx coreTesting.TestContext) *pinTestHelper {
	token := createTestUserAndLogin(ctx)

	// Create Pin
	reqBody := `{"cid":"bafybeieffnocaq7t4w4daagvydl32igft5oziyyaebqr6vx6rb3fwh2ab4","name":"test"}`
	req := ctx.NewAPIRequest(http.MethodPost, "/pins", []byte(reqBody))
	setAuthHeader(req, token)
	rec := httptest.NewRecorder()
	ctx.Router().ServeHTTP(rec, req)

	var pinResp dto.PinStatusResponse
	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	err = json.Unmarshal(body, &pinResp)
	if err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	// Wait for workflow completion
	wfTest := coreTesting.NewWorkflowTest(ctx)
	_cid := cid.MustParse("bafybeieffnocaq7t4w4daagvydl32igft5oziyyaebqr6vx6rb3fwh2ab4")
	wfTest.WaitForWorkflowInstance(protocol.PIN_WORKFLOW, core.RequestFilter{Hash: _cid.Hash(), Status: lo.ToPtr(models.RequestStatusCompleted)}, 1*time.Hour)

	return &pinTestHelper{
		token: token,
		pinID: pinResp.RequestID,
		cid:   _cid,
	}
}

func TestAPI_listPins(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := setupPinTest(t, ctx)

		// Make HTTP request
		req := ctx.NewAPIRequest(http.MethodGet, "/pins", nil)
		setAuthHeader(req, helper.token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusOK, rec.Code)
		var response dto.PinResultsResponse
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.NotEmpty(t, response.Results)
	}, TestOptions)
}

func TestAPI_addPin(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		token := createTestUserAndLogin(ctx)

		// Make HTTP request
		reqBody := `{"cid":"bafybeieffnocaq7t4w4daagvydl32igft5oziyyaebqr6vx6rb3fwh2ab4","name":"test"}`
		req := ctx.NewAPIRequest(http.MethodPost, "/pins", []byte(reqBody))
		setAuthHeader(req, token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusAccepted, rec.Code)
		var response dto.PinStatusResponse
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.NotPanics(t, func() {
			uuid.MustParse(response.RequestID)
		})
	}, TestOptions)
}

func TestAPI_getPin(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := setupPinTest(t, ctx)

		// Make HTTP request
		req := ctx.NewAPIRequest(http.MethodGet, fmt.Sprintf("/pins/%s", helper.pinID), nil)
		setAuthHeader(req, helper.token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusOK, rec.Code)
		var response dto.PinStatusResponse
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.NotPanics(t, func() {
			uuid.MustParse(response.RequestID)
		})
	}, TestOptions)
}

func TestAPI_replacePin(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := setupPinTest(t, ctx)

		// Make HTTP request
		reqBody := `{"cid":"bafybeieffnocaq7t4w4daagvydl32igft5oziyyaebqr6vx6rb3fwh2ab4","name":"test"}`
		req := ctx.NewAPIRequest(http.MethodPost, fmt.Sprintf("/pins/%s", helper.pinID), []byte(reqBody))
		setAuthHeader(req, helper.token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusAccepted, rec.Code)
		var response dto.PinStatusResponse
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)
		assert.NotPanics(t, func() {
			uuid.MustParse(response.RequestID)
		})
	}, TestOptions)
}

func TestAPI_deletePin(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := setupPinTest(t, ctx)

		// Make HTTP request
		req := ctx.NewAPIRequest(http.MethodDelete, fmt.Sprintf("/pins/%s", helper.pinID), nil)
		setAuthHeader(req, helper.token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusAccepted, rec.Code)
	}, TestOptions)
}

func createTestCAR(t *testing.T) ([]byte, cid.Cid) {
	// Create a simple CAR file with one block containing "test file content"
	content := []byte("test file content")

	// Create a CID for our content
	pref := cid.Prefix{
		Version:  1,
		Codec:    uint64(multicodec.Raw),
		MhType:   multihash.SHA2_256,
		MhLength: -1, // default length
	}

	c, err := pref.Sum(content)
	if err != nil {
		t.Fatalf("failed to create CID: %v", err)
	}

	// Create temp file for CAR
	tmpFile, err := os.CreateTemp("", "test-car-*.car")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Create CAR writer with our actual root CID
	cdest, err := blockstore.OpenReadWrite(tmpFile.Name(), []cid.Cid{c}, car.WriteAsCarV1(true))
	if err != nil {
		t.Fatalf("failed to create CAR writer: %v", err)
	}

	// Write our block
	blk, err := blocks.NewBlockWithCid(content, c)
	if err != nil {
		t.Fatalf("failed to create block: %v", err)
	}
	err = cdest.Put(context.Background(), blk)
	if err != nil {
		t.Fatalf("failed to write block: %v", err)
	}

	// Finalize the CAR
	if err := cdest.Finalize(); err != nil {
		t.Fatalf("failed to finalize CAR: %v", err)
	}

	// Read the temp file back into memory for the test
	carData, err := os.ReadFile(tmpFile.Name())
	if err != nil {
		t.Fatalf("failed to read temp CAR file: %v", err)
	}

	return carData, c
}

func TestAPI_handleUpload(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		token := createTestUserAndLogin(ctx)

		// Create test CAR file
		carData, expectedCID := createTestCAR(t)

		// Make HTTP request with CAR file upload
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)
		part, _ := writer.CreateFormFile("file", "test.car")
		part.Write(carData)
		writer.Close()

		req := ctx.NewAPIRequest(http.MethodPost, "/api/upload", body.Bytes())
		setAuthHeader(req, token)
		req.Header.Set("Content-Type", writer.FormDataContentType())
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusOK, rec.Code)

		// Parse response to get CID
		var resp dto.PostUploadResponse
		err := json.Unmarshal(rec.Body.Bytes(), &resp)
		assert.NoError(t, err)
		assert.Equal(t, expectedCID.String(), resp.CID)
	}, TestOptions, coreTesting.WithMockS3())
}

func TestAPI_handleGetBlockMeta(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := setupPinTest(t, ctx)

		// Make HTTP request
		req := ctx.NewAPIRequest(http.MethodGet, fmt.Sprintf("/api/block/meta/%s", helper.cid.String()), nil)
		setAuthHeader(req, helper.token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusOK, rec.Code)
		var response dto.BlockMetaResponse
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)

		// Verify response structure
		assert.NotNil(t, response)
		assert.IsType(t, "", response.Name)
		assert.IsType(t, uint8(0), response.Type)
		assert.IsType(t, int64(0), response.BlockSize)
		assert.IsType(t, []string{}, response.ChildCID)
		assert.True(t, len(response.ChildCID) > 0, "ChildCID should not be empty")
	}, TestOptions)
}

func TestAPI_handleGetBlockMetaBatch(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := setupPinTest(t, ctx)

		// Make HTTP request
		reqBody := fmt.Sprintf(`{"cid":["%s"]}`, helper.cid.String())
		req := ctx.NewAPIRequest(http.MethodPost, "/api/block/meta/batch", []byte(reqBody))
		setAuthHeader(req, helper.token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusOK, rec.Code)
		var response map[string]*dto.BlockMetaResponse
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)

		// Verify response structure
		assert.NotEmpty(t, response)
		for cid, meta := range response {
			assert.NotEmpty(t, cid)
			assert.NotNil(t, meta)
			assert.IsType(t, "", meta.Name)
			assert.IsType(t, uint8(0), meta.Type)
			assert.IsType(t, int64(0), meta.BlockSize)
			assert.IsType(t, []string{}, meta.ChildCID)
			assert.True(t, len(meta.ChildCID) > 0, "ChildCID should not be empty")
		}
	}, TestOptions)
}

func TestAPI_handleGetInfo(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		token := createTestUserAndLogin(ctx)

		// Make HTTP request
		req := ctx.NewAPIRequest(http.MethodGet, "/api/info", nil)
		setAuthHeader(req, token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusOK, rec.Code)
		var response dto.InfoResponse
		err := json.Unmarshal(rec.Body.Bytes(), &response)
		assert.NoError(t, err)

		// Verify response structure
		assert.NotEmpty(t, response.PeerID)
		assert.NotEmpty(t, response.AnnouncementAddresses)
		assert.NotEmpty(t, response.ConnectionAddresses)

		// Verify announcement addresses format
		for _, addr := range response.AnnouncementAddresses {
			assert.Contains(t, addr, "/ip6/")
			assert.Contains(t, addr, "/tcp/4001")
		}

		// Verify connection addresses format
		for _, addr := range response.ConnectionAddresses {
			assert.Contains(t, addr, "/ip6/")
			assert.Contains(t, addr, "/tcp/4001/p2p/")
			assert.Contains(t, addr, response.PeerID)
		}
	}, TestOptions)
}

func TestAPI_handleIPFSGet(t *testing.T) {
	coreTesting.RunTestCaseWithDB(t, func(tb coreTesting.TB, ctx coreTesting.TestContext) {
		helper := setupPinTest(t, ctx)

		req := ctx.NewAPIRequest(http.MethodGet, fmt.Sprintf("/ipfs/%s", helper.cid.String()), nil)
		setAuthHeader(req, helper.token)
		rec := httptest.NewRecorder()
		ctx.Router().ServeHTTP(rec, req)

		// Verify response
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, "tornadocash", rec.Body.String())
	}, TestOptions)
}
