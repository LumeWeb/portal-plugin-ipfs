package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/labstack/echo/v4"
	"github.com/multiformats/go-multiaddr"
	"github.com/samber/lo"
	"github.com/tus/tusd/v2/pkg/handler"
	"go.lumeweb.com/httputil"
	"go.lumeweb.com/portal-middleware/auth/jwt"
	mcontext "go.lumeweb.com/portal-middleware/context"
	"go.lumeweb.com/portal-middleware/middleware"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/ipfs"
	"go.lumeweb.com/portal-router"
	"go.lumeweb.com/portal/config"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/db/types"
	"go.lumeweb.com/portal/event"
	"go.lumeweb.com/portal/service"
	"go.lumeweb.com/queryutil"
	queryUtilHttp "go.lumeweb.com/queryutil/http"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var _ core.API = (*API)(nil)
var _ core.APITusHandler = (*API)(nil)

const TUS_HTTP_ROUTE = "/api/upload/tus"

type API struct {
	ctx               core.Context
	config            config.Manager
	logger            *core.Logger
	coreUploadService core.UploadService
	uploadService     pluginCore.UploadService
	pinService        pluginCore.IPFSPinService
	blockService      pluginCore.BlockService
	workflowService   core.WorkflowService
	tus               core.TusHandler
	ipfs              ProtoNode
}

type ProtoNode interface {
	GetNode() *ipfs.Node
}

func NewAPI() (core.API, []core.ContextBuilderOption, error) {
	api := &API{}
	return api, core.ContextOptions(

		core.ContextWithStartupFunc(func(ctx core.Context) error {
			api.ctx = ctx
			api.config = ctx.Config()
			api.logger = ctx.APILogger(api)
			api.pinService = core.GetService[pluginCore.IPFSPinService](ctx, pluginCore.PIN_SERVICE)
			api.blockService = core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
			api.coreUploadService = core.GetService[core.UploadService](ctx, core.UPLOAD_SERVICE)
			api.uploadService = core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
			api.workflowService = core.GetService[core.WorkflowService](ctx, core.WORKFLOW_SERVICE)
			proto := core.GetProtocol(internal.ProtocolName)
			sproto := proto.(core.StorageProtocol)

			event.OnBootHTTP(ctx, func(ctx core.Context) error {
				var _tus core.TusHandler
				var err error
				_tus, err = service.CreateTusHandler(ctx, core.TUSHandlerConfig{
					Protocol: proto,
					BasePath: TUS_HTTP_ROUTE,
					CreatedUploadHandler: service.TUSDefaultUploadCreatedHandler(ctx, func(hook handler.HookEvent, uploaderId uint) (core.StorageHash, error) {
						return nil, nil
					}, nil),
					UploadProgressHandler:   service.TUSDefaultUploadProgressHandler(ctx),
					TerminatedUploadHandler: service.TUSDefaultUploadTerminatedHandler(ctx),
					CompletedUploadHandler: service.TUSDefaultUploadCompletedHandler(ctx, func(_ core.TusHandler, hook handler.HookEvent) {
						upload, err := api.tus.UploadReader(ctx, hook.Upload.ID, sproto, 0)

						if err != nil {
							api.logger.Error("Failed to get request reader", zap.Error(err))
							return
						}

						defer func(upload io.ReadCloser) {
							err := upload.Close()
							if err != nil {
								api.logger.Error("Failed to close reader", zap.Error(err))
							}
						}(upload)

						reader, err := api.createCARReader(upload)
						if err != nil {
							api.logger.Error("Failed to create CAR reader", zap.Error(err))
							err = api.tus.FailUploadById(ctx, sproto, hook.Upload.ID)
							if err != nil {
								api.logger.Error("Failed to fail ipfsUpload", zap.Error(err))
							}
							return
						}

						_, err = internal.GetCarRoots(reader, true)

						if err != nil {
							api.logger.Error("Failed to validate car", zap.Error(err))
							err = api.tus.FailUploadById(ctx, sproto, hook.Upload.ID)
							if err != nil {
								api.logger.Error("Failed to fail ipfsUpload", zap.Error(err))
							}
							return
						}
					}, protocol.TUS_UPLOAD_WORKFLOW),
					PreFinishResponse: service.TUSDefaultPreFinishResponse(func() core.TusHandler {
						return _tus
					}, func(hook handler.HookEvent, data io.Reader, size uint64) (core.StorageHash, error) {
						reader, err := createCARReader(data)
						if err != nil {
							return nil, err
						}

						roots, err := internal.GetCarRoots(reader, false)
						if err != nil {
							return nil, err
						}

						return internal.NewIPFSHash(roots[0]), nil
					}),
				})

				if err != nil {
					return fmt.Errorf("failed to create tus handler: %w", err)
				}
				api.tus = _tus

				return nil
			})
			api.ipfs = proto.(ProtoNode)

			return nil
		}),
	), nil
}

func (a *API) GetTusHandler() core.TusHandler {
	return a.tus
}

func (a *API) Name() string {
	return internal.ProtocolName
}

func (a *API) Subdomain() string {
	return internal.ProtocolName
}

func (a *API) AuthTokenName() string {
	return core.AUTH_TOKEN_NAME
}

func (a *API) Config() config.APIConfig {
	return &pluginConfig.APIConfig{}
}

func (a *API) OpenAPIInfo() router.APIInfoDefinition {
	return router.APIInfo().
		Title("IPFS Pinning Service API").
		Description(`
## About this spec

The IPFS Pinning Service API is intended to be an implementation-agnostic API:

- For use and implementation by pinning service providers

- For use in client mode by IPFS nodes and GUI-based applications


### Document scope and intended audience

The intended audience of this document is **IPFS developers** building pinning service clients or servers compatible with this OpenAPI spec.
Your input and feedback are welcome and valuable as we develop this API spec. Please join the design discussion at [github.com/ipfs/pinning-services-api-spec](https://github.com/ipfs/pinning-services-api-spec).


**IPFS users** should see the tutorial at [docs.ipfs.io/how-to/work-with-pinning-services/](https://docs.ipfs.io/how-to/work-with-pinning-services/) instead.


### Related resources

The latest version of this spec and additional resources can be found at:

- Specification: https://github.com/ipfs/pinning-services-api-spec/raw/main/ipfs-pinning-service.yaml

- Docs: https://ipfs.github.io/pinning-services-api-spec/

- Clients and services: https://github.com/ipfs/pinning-services-api-spec#adoption
`)
}

func (a *API) Configure(r router.Router, accessSvc core.AccessService) error {
	// Middleware setup
	authMw := middleware.AuthMiddleware(a.ctx, middleware.WithAuthErrorCallback(func(c echo.Context) (int, json.Marshaler) {
		err := NewError(ErrKeyUnauthorized, nil)
		return err.HttpStatus(), err
	}), middleware.WithAuthPurpose(jwt.PurposeLogin, jwt.PurposeAPI))

	// Pinning service routes
	pinRoutes := router.DefineRoutes(
		router.NewRoute(http.MethodGet, "/pins", a.listPins,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("List pin objects"),
				router.WithDescription("List all the pin objects, matching optional filters; when no filter is provided, only successful pins are returned"),
				router.WithTags("pins"),
				router.WithQueryParam("cid", "Return pin objects responsible for pinning the specified CID(s)", []string{}),
				router.WithQueryParam("name", "Return pin objects with specified name", ""),
				router.WithQueryParam("match", "Customize the text matching strategy", "exact"),
				router.WithQueryParam("status", "Return pin objects for pins with the specified status", []string{}),
				router.WithQueryParam("before", "Return results created (queued) before provided timestamp", "2020-07-27T17:32:28.276Z"),
				router.WithQueryParam("after", "Return results created (queued) after provided timestamp", "2020-07-27T17:32:28.276Z"),
				router.WithQueryParam("limit", "Max records to return", 10),
				router.WithQueryParam("meta", "Return pin objects that match specified metadata", "{}"),
				router.WithSuccessResponse(http.StatusOK, "Successful response", router.WithJSONContent(dto.PinResultsResponse{})),
			),
		),
		router.NewRoute(http.MethodPost, "/pins", a.addPin,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Add pin object"),
				router.WithDescription("Add a new pin object for the current access token"),
				router.WithTags("pins"),
				router.WithRequestBody(&dto.PinRequest{}, "Pin object", true),
				router.WithSuccessResponse(http.StatusAccepted, "Successful response", router.WithJSONContent(dto.PinStatusResponse{})),
			),
		),
		router.NewRoute(http.MethodGet, "/pins/:requestid", a.getPin,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Get pin object"),
				router.WithDescription("Get a pin object and its status"),
				router.WithTags("pins"),
				router.WithPathParam("requestid", "Unique identifier of a pin request", ""),
				router.WithSuccessResponse(http.StatusOK, "Successful response", router.WithJSONContent(dto.PinStatusResponse{})),
			),
		),
		router.NewRoute(http.MethodPost, "/pins/:requestid", a.replacePin,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Replace pin object"),
				router.WithDescription("Replace an existing pin object"),
				router.WithTags("pins"),
				router.WithPathParam("requestid", "Unique identifier of a pin request", ""),
				router.WithRequestBody(&dto.PinRequest{}, "Pin object", true),
				router.WithSuccessResponse(http.StatusAccepted, "Successful response", router.WithJSONContent(dto.PinStatusResponse{})),
			),
		),
		router.NewRoute(http.MethodDelete, "/pins/:requestid", a.deletePin,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Remove pin object"),
				router.WithDescription("Remove a pin object"),
				router.WithTags("pins"),
				router.WithPathParam("requestid", "Unique identifier of a pin request", ""),
				router.WithSuccessResponse(http.StatusAccepted, "Successful response"),
			),
		),
	)

	// Other IPFS routes
	apiGroup, err := r.Group("/api")
	if err != nil {
		return fmt.Errorf("failed to create api group: %w", err)
	}

	if err := router.RegisterRoutes(apiGroup, accessSvc, a.Subdomain(), pinRoutes, router.WithMiddlewares(authMw), router.WithCors()); err != nil {
		return fmt.Errorf("failed to register pin routes: %w", err)
	}

	ipfsRoutes := router.DefineRoutes(
		router.NewRoute(http.MethodPost, "/upload", a.handleUpload,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Upload a file"),
				router.WithDescription("Uploads a file to IPFS."),
				router.WithTags("ipfs"),
				router.WithFileUpload("File to upload", true),
				router.WithSuccessResponse(http.StatusOK, "File uploaded successfully"),
			),
		),
		router.NewRoute(http.MethodGet, "/block/meta/:cid", a.handleGetBlockMeta,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Get block metadata"),
				router.WithDescription("Gets metadata for a block."),
				router.WithTags("ipfs"),
				router.WithPathParam("cid", "The CID of the block.", ""),
				router.WithSuccessResponse(http.StatusOK, "Block metadata", router.WithJSONContent(dto.BlockMetaResponse{})),
			),
		),
		router.NewRoute(http.MethodPost, "/block/meta/batch", a.handleGetBlockMetaBatch,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Get block metadata in batch"),
				router.WithDescription("Gets metadata for multiple blocks in a single request."),
				router.WithTags("ipfs"),
				router.WithRequestBody(&dto.GetBlockMetaBatchRequest{}, "Batch request for block metadata", true),
				// TODO: Fix openapi processing of this map type
				// router.WithSuccessResponse(http.StatusOK, "Block metadata map", router.WithJSONContent(dto.BlockMap{})),
			),
		),
		router.NewRoute(http.MethodGet, "/info", a.handleGetInfo,
			router.WithSwagger(
				router.WithSummary("Get IPFS node info"),
				router.WithDescription("Gets information about the IPFS node."),
				router.WithTags("ipfs"),
				router.WithSuccessResponse(http.StatusOK, "Node information", router.WithJSONContent(dto.InfoResponse{})),
			),
		),
	)

	if err := router.RegisterRoutes(apiGroup, accessSvc, a.Subdomain(), ipfsRoutes, router.WithMiddlewares(authMw), router.WithCors()); err != nil {
		return fmt.Errorf("failed to register ipfs routes: %w", err)
	}

	// IPFS content addressing routes
	ipfsContentGroup, err := apiGroup.Group("/ipfs")
	if err != nil {
		return fmt.Errorf("failed to create ipfs content group: %w", err)
	}

	ipfsContentRoutes := router.DefineRoutes(
		router.NewRoute(http.MethodGet, "/:cid", a.handleIPFSGet,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Get IPFS content"),
				router.WithDescription("Retrieves content from IPFS by CID."),
				router.WithTags("ipfs"),
				router.WithPathParam("cid", "The CID of the content.", ""),
				// Raw response, so no JSON content
			),
		),
		router.NewRoute(http.MethodHead, "/:cid", a.handleIPFSGet,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Check IPFS content existence"),
				router.WithDescription("Checks if content exists on IPFS by CID."),
				router.WithTags("ipfs"),
				router.WithPathParam("cid", "The CID of the content.", ""),
			),
		),
	)

	if err = router.RegisterRoutes(ipfsContentGroup, accessSvc, a.Subdomain(), ipfsContentRoutes, router.WithMiddlewares(authMw), router.WithCors()); err != nil {
		return fmt.Errorf("failed to register ipfs content routes: %w", err)
	}

	err = a.tus.SetupRoute(r, a.Subdomain(), true, false, TUS_HTTP_ROUTE)
	if err != nil {
		return err
	}

	return nil
}

func (a *API) listPins(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	filter := dto.IPFSPinFilter{}
	if _, ok := httputil.DecodeAndValidateRequest(ctx, &filter); !ok {
		return nil
	}

	// Post-process statuses
	if err := filter.PostProcessStatuses(); err != nil {
		// Use default error handler for validation errors
		errorHandler := &httputil.DefaultErrorHandler{}
		errorHandler.HandleError(ctx, err)
		return nil
	}

	reqParser := pluginCore.NewIPFSPinParser(reqCtx, filter)

	filters, sort, pagination, err := queryutil.ParseFromCustomSource(reqParser)
	if err != nil {
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	pins, total, err := a.pinService.ListPins(reqCtx, filters, sort, pagination)
	if err != nil {
		a.logger.Error("Failed to list pins", zap.Error(err))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	var dtoResp dto.PinResultsResponse

	dtoResp.Count = uint64(total)

	queryUtilHttp.SetContentRangeHeader(c.Response(), "pins", pagination, pins, total)

	return httputil.EncodeResponse(ctx, pins, &dtoResp)
}

func (a *API) addPin(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()
	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}

	var req dto.PinRequest
	model, ok := httputil.DecodeAndValidateRequest(ctx, &req)

	if !ok {
		return nil
	}

	_pin, err := a.pinService.AddPin(reqCtx, model)
	if err != nil {
		a.logger.Error("Failed to add pin", zap.Error(err))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	_, err = a.workflowService.StartWorkflow(ctx.Request().Context(), protocol.PIN_WORKFLOW, core.WithWorkflowStructData(protocol.PinWorkflowData{
		PinRequestID: _pin.RequestID.ToUUID(),
	}, "json"),
		core.WithWorkflowSourceIP(c.RealIP()),
		core.WithWorkflowUserID(user),
		core.WithWorkflowStorageHash(internal.NewIPFSHash(cid.MustParse(_pin.CID))),
	)
	if err != nil {
		_ = ctx.Error(err, http.StatusInternalServerError)
	}

	ctx.Response().Before(func() {
		ctx.Response().Status = http.StatusAccepted
	})

	return httputil.EncodeResponse(ctx, _pin, &dto.PinStatusResponse{})
}

func (a *API) getPin(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	_uuid, err := uuid.Parse(c.Param("requestid"))
	if err != nil {
		apiErr := NewError(ErrKeyInvalidUUIDFormat, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	requestID := types.FromUUID(_uuid)

	_pin, err := a.pinService.GetPinByRequestID(reqCtx, requestID)
	if err != nil {
		a.logger.Error("Failed to get pin", zap.Error(err))
		apiErr := NewError(ErrKeyPinFetchFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	if _pin == nil {
		apiErr := NewError(ErrKeyPinFetchFailed, fmt.Errorf("pin not found"))
		return ctx.Error(apiErr, http.StatusNotFound)
	}

	return httputil.EncodeResponse(ctx, _pin, &dto.PinStatusResponse{})
}

func (a *API) replacePin(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	_uuid, err := uuid.Parse(c.Param("requestid"))
	if err != nil {
		apiErr := NewError(ErrKeyInvalidUUIDFormat, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}

	requestID := types.FromUUID(_uuid)

	var req dto.PinRequest
	model, ok := httputil.DecodeAndValidateRequest(ctx, &req)

	if !ok {
		return nil
	}

	_pin, err := a.pinService.ReplacePin(reqCtx, 0, "", requestID, model)
	if err != nil {
		a.logger.Error("Failed to replace pin", zap.Error(err))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	_, err = a.workflowService.StartWorkflow(ctx.Request().Context(), protocol.PIN_WORKFLOW, core.WithWorkflowStructData(protocol.PinWorkflowData{
		PinRequestID: _pin.RequestID.ToUUID(),
	}, "json"),
		core.WithWorkflowSourceIP(c.RealIP()),
		core.WithWorkflowUserID(user),
		core.WithWorkflowStorageHash(internal.NewIPFSHash(cid.MustParse(_pin.CID))),
	)
	if err != nil {
		_ = ctx.Error(err, http.StatusInternalServerError)
	}

	ctx.Response().Before(func() {
		ctx.Response().Status = http.StatusAccepted
	})

	return httputil.EncodeResponse(ctx, _pin, &dto.PinStatusResponse{})
}

func (a *API) deletePin(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	_uuid, err := uuid.Parse(c.Param("requestid"))
	if err != nil {
		apiErr := NewError(ErrKeyInvalidUUIDFormat, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	requestID := types.FromUUID(_uuid)

	if err := a.pinService.DeletePin(reqCtx, requestID); err != nil {
		a.logger.Error("Failed to delete pin", zap.Error(err))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	return ctx.NoContent(http.StatusAccepted)
}

func (a *API) handleIPFSGet(c echo.Context) error {
	ctx := httputil.Context(c)

	req := dto.IPFSRequest{}
	model, ok := httputil.DecodeAndValidateRequest(ctx, &req)

	if !ok {
		return nil
	}
	switch model.Format {
	case "raw":
		a.handleRawBlockRequest(ctx, model.CID, c.Response(), c.Request())
	case "car":
		// TODO: Implement CAR handling
		ctx.Response().Header().Set("Content-Type", "application/vnd.ipld.car")
		ctx.Response().WriteHeader(http.StatusNotImplemented)
	default:
		return ctx.Error(errors.New("Unsupported format"), http.StatusBadRequest)
	}

	return nil
}

func (a API) handleRawBlockRequest(ctx httputil.RequestContext, _cid cid.Cid, w http.ResponseWriter, r *http.Request) {
	// Check if the block exists before trying to fetch it
	exists, err := a.ipfs.GetNode().HasBlock(ctx.Request().Context(), _cid)
	if err != nil {
		a.logger.Error("Failed to check if block exists", zap.Error(err))
		apiErr := NewError(ErrKeyMetadataFetchFailed, err)
		_ = ctx.Error(apiErr, apiErr.HttpStatus())
		return
	}

	if !exists {
		apiErr := NewError(ErrKeyBlockNotFound, fmt.Errorf("Block not found: %s", _cid.String()))
		_ = ctx.Error(apiErr, apiErr.HttpStatus())
		return
	}

	_, err = a.coreUploadService.GetUpload(ctx.Request().Context(), internal.NewIPFSHash(_cid))
	if err != nil {
		a.logger.Error("Failed to get upload", zap.Error(err))
		apiErr := NewError(ErrKeyUploadNotFound, err)
		_ = ctx.Error(apiErr, apiErr.HttpStatus())
		return
	}

	block, err := a.ipfs.GetNode().GetBlock(ctx.Request().Context(), _cid)
	if err != nil {
		a.logger.Error("Failed to get block", zap.Error(err))
		apiErr := NewError(ErrKeyMetadataFetchFailed, err)
		_ = ctx.Error(apiErr, apiErr.HttpStatus())
		return
	}

	a.setTrustlessHeaders(w, r, _cid.String())
	_, _ = w.Write(block.RawData())
}

func (a API) setTrustlessHeaders(w http.ResponseWriter, r *http.Request, id string) {
	w.Header().Set("Content-Type", a.getTrustlessContentType(r))
	w.Header().Set("Cache-Control", "public, max-age=29030400, immutable")
	w.Header().Set("Etag", fmt.Sprintf("\"%s\"", id))
	w.Header().Set("X-Content-Type-Options", "nosniff")
}

func (a API) getTrustlessContentType(r *http.Request) string {
	format := r.URL.Query().Get("format")
	switch format {
	case "raw":
		return "application/vnd.ipld.raw"
	case "car":
		return "application/vnd.ipld.car"
	case "ipns-record":
		return "application/vnd.ipfs.ipns-record"
	default:
		return "application/octet-stream"
	}
}

func (a *API) handleUpload(c echo.Context) error {
	ctx := httputil.Context(c)

	user, err := mcontext.GetUserID(ctx.Context)
	if err != nil {
		apiErr := core.NewAccountError(core.ErrKeyLoginFailed, nil)
		_ = ctx.Error(apiErr, apiErr.HttpStatus())
		return nil
	}

	upload, err := ctx.PrepareFileUpload(int64(a.config.Config().Core.PostUploadLimit))
	if err != nil {
		_ = ctx.Error(err, http.StatusBadRequest)
		return nil
	}

	_cid, uploadId, err := a.uploadService.HandleUpload(ctx.Request().Context(), upload.File, user)
	if err != nil {
		_ = ctx.Error(err, http.StatusBadRequest)
	}

	_, err = a.workflowService.StartWorkflow(ctx.Request().Context(), protocol.UPLOAD_WORKFLOW, core.WithWorkflowStructData(protocol.PostUploadWorkflowData{
		UploadID: uploadId,
	}, "json"),
		core.WithWorkflowSourceIP(c.RealIP()),
		core.WithWorkflowUserID(user),
		core.WithWorkflowStorageHash(internal.NewIPFSHash(_cid)))
	if err != nil {
		return ctx.Error(err, http.StatusInternalServerError)
	}

	return httputil.EncodeResponse(ctx, &dto.PostUploadResponse{}, &dto.PostUploadResponse{CID: _cid.String()})
}

func (a *API) handleGetBlockMeta(c echo.Context) error {
	ctx := httputil.Context(c)

	req := dto.GetBlockMetaRequest{}
	model, ok := httputil.DecodeAndValidateRequest(ctx, &req)

	if !ok {
		return nil
	}

	meta, err := a.blockService.GetBlockMeta(ctx.Request().Context(), model.CID)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			apiErr := NewError(ErrKeyMetadataFetchFailed, err)
			return ctx.Error(apiErr, http.StatusNotFound)
		}

		a.logger.Error("Failed to get block meta", zap.Error(err))
		apiErr := NewError(ErrKeyMetadataFetchFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	return httputil.EncodeResponse(ctx, meta, &dto.BlockMetaResponse{})
}

func (a *API) handleGetBlockMetaBatch(c echo.Context) error {
	ctx := httputil.Context(c)

	req := dto.GetBlockMetaBatchRequest{}
	model, ok := httputil.DecodeAndValidateRequest(ctx, &req)

	if !ok {
		return nil
	}

	meta, err := a.blockService.GetBlockMetaBatch(ctx.Request().Context(), model.CID)

	if err != nil {
		a.logger.Error("Failed to get block meta", zap.Error(err))
		apiErr := NewError(ErrKeyMetadataFetchFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	return httputil.EncodeResponse(ctx, meta, &dto.GetBlockMetaBatchResponse{})
}

func (a *API) handleGetInfo(c echo.Context) error {
	ctx := httputil.Context(c)

	addrs, err := ipfs.AnnouncementAddresses()
	if err != nil {
		a.logger.Error("Failed to get announcement addresses", zap.Error(err))
		apiErr := NewError(ErrKeyMetadataFetchFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	connAddrs, err := a.ipfs.GetNode().ConnectionAddresses()
	if err != nil {
		a.logger.Error("Failed to get connection addresses", zap.Error(err))
		apiErr := NewError(ErrKeyMetadataFetchFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	nodeInfo := dto.NodeInfo{
		PeerID: a.ipfs.GetNode().PeerID().String(),
		AnnouncementAddresses: lo.Map(addrs, func(addr multiaddr.Multiaddr, _ int) string {
			return addr.String()
		}),
		ConnectionAddresses: lo.Map(connAddrs, func(addr multiaddr.Multiaddr, _ int) string {
			return addr.String()
		}),
	}

	return httputil.EncodeResponse(ctx, &nodeInfo, &dto.InfoResponse{})
}

func createCARReader(data io.Reader) (io.ReaderAt, error) {
	// Read the first carv1.DefaultMaxAllowedHeaderSize bytes into a buffer
	buf := make([]byte, car.DefaultMaxAllowedHeaderSize)
	n, err := io.ReadFull(data, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, err
	}

	// Create a bytes.Reader that supports ReaderAt
	reader := bytes.NewReader(buf[:n])
	return reader, nil
}

