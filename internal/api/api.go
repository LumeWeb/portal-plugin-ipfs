package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/ipld/go-car/v2"
	"github.com/labstack/echo/v4"
	"github.com/tus/tusd/v2/pkg/handler"
	portalMw "go.lumeweb.com/portal-middleware/middleware"
	"go.lumeweb.com/portal-middleware/auth/jwt"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginMw "go.lumeweb.com/portal-plugin-ipfs/internal/api/middleware"
	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/quota"
	uploadpkg "go.lumeweb.com/portal-plugin-ipfs/internal/upload"
	"go.lumeweb.com/portal-router"
	"go.lumeweb.com/portal/config"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/event"
	"go.lumeweb.com/portal/service"
	"go.lumeweb.com/queryutil"
	"go.uber.org/zap"
)

var _ core.API = (*API)(nil)
var _ core.APITusHandler = (*API)(nil)

const TUS_HTTP_ROUTE = "/api/upload/tus"

type API struct {
	*core.BaseComponent
	coreUploadService  core.UploadService
	uploadService      pluginCore.UploadService
	pinService         pluginCore.IPFSPinService
	blockService       pluginCore.BlockService
	fileManagerService pluginCore.FileManagerService
	workflowService    core.WorkflowService
	ipnsKeyService     pluginCore.IPNSKeyService
	websiteService     pluginCore.WebsiteService
	tus                core.TusHandler
	ipfs               protocol.ProtoNode
}

func NewAPI() (core.API, []core.ContextBuilderOption, error) {
	api := &API{}
	return api, core.ContextOptions(

		core.ContextWithStartupFunc(func(ctx core.Context) error {
			api.pinService = core.GetService[pluginCore.IPFSPinService](ctx, pluginCore.PIN_SERVICE)
			api.blockService = core.GetService[pluginCore.BlockService](ctx, pluginCore.BLOCK_SERVICE)
			api.coreUploadService = core.GetService[core.UploadService](ctx, core.UPLOAD_SERVICE)
			api.uploadService = core.GetService[pluginCore.UploadService](ctx, pluginCore.UPLOAD_SERVICE)
			api.workflowService = core.GetService[core.WorkflowService](ctx, core.WORKFLOW_SERVICE)
			api.ipnsKeyService = core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
			api.websiteService = core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
			proto := core.GetProtocol(internal.ProtocolName)
			sproto := proto.(core.StorageProtocol)
			event.OnBootStartupFuncsCompleted(ctx, func(ctx core.Context, eventCtx context.Context) error {
				var _tus core.TusHandler
				var err error
				_tus, err = service.CreateTusHandler(ctx, core.TUSHandlerConfig{
					Protocol: proto,
					BasePath: TUS_HTTP_ROUTE,
					CreatedUploadHandler: service.TUSDefaultUploadCreatedHandler(ctx, func(hook handler.HookEvent, uploaderId uint) (core.StorageHash, error) {
						size := hook.Upload.Size
						if size < 0 {
							api.Logger().Warn("Unexpected negative upload size in TUS hook", zap.Int64("size", size))
							return nil, core.ErrUploadQuotaExceeded
						}
						requestedBytes := uint64(size)
						if err := quota.ValidateUploadQuota(eventCtx, ctx, uploaderId, requestedBytes); err != nil {
							api.Logger().Error("Failed to check upload quota", zap.Error(err))
							return nil, err
						}
						return nil, nil
					}, nil),
					UploadProgressHandler:   service.TUSDefaultUploadProgressHandler(ctx),
					TerminatedUploadHandler: service.TUSDefaultUploadTerminatedHandler(ctx),
					CompletedUploadHandler: service.TUSDefaultUploadCompletedHandler(ctx, func(_ core.TusHandler, hook handler.HookEvent) {
						upload, err := api.tus.UploadReader(ctx, hook.Upload.ID, sproto, 0)
						if err != nil {
							api.Logger().Error("Failed to get request reader", zap.Error(err))
							return
						}
						defer closeUpload(upload, api.Logger())

						if !validateCARUpload(upload, api.tus, ctx, sproto, hook.Upload.ID, api.Logger()) {
							return
						}

						var userID *uint
						if hook.Upload.MetaData != nil {
							if uploaderID, exists := hook.Upload.MetaData["uploader_id"]; exists {
								if uid, err := strconv.ParseUint(uploaderID, 10, 64); err == nil {
									userIDVal := uint(uid)
									userID = &userIDVal
								} else {
									api.Logger().Warn("Failed to parse uploader_id from metadata", zap.String("uploader_id", uploaderID), zap.Error(err))
								}
							}
						}

						uploadID, err := strconv.ParseUint(hook.Upload.ID, 10, 64)
						if err != nil {
							api.Logger().Warn("Failed to parse upload ID", zap.String("upload_id", hook.Upload.ID), zap.Error(err))
							return
						}

						echoCtx, ok := service.TusGetEchoContext(hook.Context)
						var ip string
						if ok && echoCtx != nil {
							ip = echoCtx.RealIP()
						}

						size := hook.Upload.Size
						if size < 0 {
							api.Logger().Warn("Unexpected negative upload size in TUS completed hook", zap.Int64("size", size))
							return
						}
						quota.EmitUploadCompleted(eventCtx, ctx, userID, uint(uploadID), uint64(size), ip)
					}, protocol.TUS_UPLOAD_WORKFLOW,
						func(handlr core.TusHandler, hook handler.HookEvent, reader io.Reader) (core.StorageHash, error) {
							return getCARUploadHash(reader, api.tus, ctx, sproto, hook.Upload.ID, api.Logger())
						},
					),
					PreFinishResponse: service.TUSDefaultPreFinishResponse(func() core.TusHandler {
						return _tus
					}, func(hook handler.HookEvent, data io.Reader, size uint64) (core.StorageHash, error) {
						return processCARData(data)
					}),
				})

				if err != nil {
					return fmt.Errorf("failed to create tus handler: %w", err)
				}
				api.tus = _tus

				api.fileManagerService = core.GetService[pluginCore.FileManagerService](ctx, pluginCore.FILE_MANAGER_SERVICE)

				return nil
			})

			api.ipfs = proto.(protocol.ProtoNode)

			return nil
		}),
	), nil
}

func (a *API) ID() string {
	return a.Name()
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

func (a *API) GetConfig() config.APIConfig {
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
	authMw := portalMw.AuthMiddleware(a.Context(),
		portalMw.WithAuthErrorCallback(func(c echo.Context) (int, json.Marshaler) {
			err := NewError(ErrKeyUnauthorized, nil)
			return err.HttpStatus(), err
		}),
		portalMw.WithAuthPurpose(jwt.PurposeLogin, jwt.PurposeAPI),
	)

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

	fileManagerListProvider := queryutil.NewSchemaProvider().ForType(&dto.FileManagerListRequest{})
	fileManagerFilterProvider := queryutil.NewSchemaProvider().ForType(&dto.FileManagerFilter{})

	fileManagerRoutes := router.DefineRoutes(
		router.NewRoute(http.MethodGet, "/files", a.listFiles,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("List pinned files"),
				router.WithDescription("List all pinned files with their metadata"),
				router.WithSchema(fileManagerListProvider),
				router.WithTags("file-manager"),
				router.WithSuccessResponse(http.StatusOK, "Successful response", router.WithJSONContent(queryutil.Response[dto.FileManagerItem]{})),
			),
		),
		router.NewRoute(http.MethodGet, "/files/directory", a.listDirectoryContents,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("List directory contents"),
				router.WithDescription("List files and subdirectories within a specified parent directory path"),
				router.WithSchema(fileManagerFilterProvider),
				router.WithTags("file-manager"),
				router.WithSuccessResponse(http.StatusOK, "Successful response", router.WithJSONContent(queryutil.Response[dto.FileManagerItem]{})),
			),
		),
		router.NewRoute(http.MethodGet, "/files/breadcrumbs", a.getBreadcrumbs,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Get path breadcrumbs"),
				router.WithDescription("Retrieve breadcrumb navigation elements for a given file path"),
				router.WithSchema(fileManagerFilterProvider),
				router.WithTags("file-manager"),
				router.WithSuccessResponse(http.StatusOK, "Successful response", router.WithJSONContent(queryutil.Response[dto.FileManagerItem]{})),
			),
		),
	)

	apiGroup, err := r.Group("/api")
	if err != nil {
		return fmt.Errorf("failed to create api group: %w", err)
	}

	if err := router.RegisterRoutes(apiGroup, accessSvc, a.Subdomain(), pinRoutes, router.WithMiddlewares(authMw), router.WithCors()); err != nil {
		return fmt.Errorf("failed to register pin routes: %w", err)
	}

	if err := router.RegisterRoutes(apiGroup, accessSvc, a.Subdomain(), fileManagerRoutes, router.WithMiddlewares(authMw), router.WithCors()); err != nil {
		return fmt.Errorf("failed to register file manager routes: %w", err)
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

	ipfsContentRoutes := router.DefineRoutes(
		router.NewRoute(http.MethodGet, "/ipfs/:cid", a.handleIPFSGet,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Get IPFS content"),
				router.WithDescription("Retrieves content from IPFS by CID."),
				router.WithTags("ipfs"),
				router.WithPathParam("cid", "The CID of the content.", ""),
			),
		),
		router.NewRoute(http.MethodHead, "/ipfs/:cid", a.handleIPFSGet,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Check IPFS content existence"),
				router.WithDescription("Checks if content exists on IPFS by CID."),
				router.WithTags("ipfs"),
				router.WithPathParam("cid", "The CID of the content.", ""),
			),
		),
		router.NewRoute(http.MethodOptions, "/ipfs/:cid", a.handleIPFSOptions,
			router.WithSwagger(
				router.WithSummary("IPFS content OPTIONS"),
				router.WithDescription("OPTIONS endpoint for IPFS content. CORS preflight requests are handled by middleware; this handler serves as a fallback for non-preflight OPTIONS requests."),
				router.WithTags("ipfs"),
				router.WithPathParam("cid", "The CID of the content.", ""),
			),
		),
	)

	if err = router.RegisterRoutes(r, accessSvc, a.Subdomain(), ipfsContentRoutes, router.WithMiddlewares(authMw), router.WithCors()); err != nil {
		return fmt.Errorf("failed to register ipfs content routes: %w", err)
	}

	ipnsKeyRoutes := router.DefineRoutes(
		router.NewRoute(http.MethodPost, "/ipns/keys", a.createIPNSKey,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Create or import IPNS key"),
				router.WithDescription("Creates a new IPNS key or imports an existing one. If 'key' is provided, it imports; otherwise creates a new key."),
				router.WithTags("ipns"),
				router.WithRequestBody(&dto.IPNSKeyRequest{}, "IPNS key request", true),
				router.WithSuccessResponse(http.StatusCreated, "IPNS key created", router.WithJSONContent(dto.IPNSKeyResponse{})),
			),
		),
		router.NewRoute(http.MethodGet, "/ipns/keys", a.listIPNSKeys,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("List IPNS keys"),
				router.WithDescription("Lists all IPNS keys for the current user."),
				router.WithTags("ipns"),
				router.WithSuccessResponse(http.StatusOK, "List of IPNS keys", router.WithJSONContent([]dto.IPNSKeyResponse{})),
			),
		),
		router.NewRoute(http.MethodGet, "/ipns/keys/:id", a.getIPNSKey,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Get IPNS key details"),
				router.WithDescription("Retrieves details of a specific IPNS key."),
				router.WithTags("ipns"),
				router.WithPathParam("id", "IPNS key ID", ""),
				router.WithSuccessResponse(http.StatusOK, "IPNS key details", router.WithJSONContent(dto.IPNSKeyResponse{})),
			),
		),
		router.NewRoute(http.MethodDelete, "/ipns/keys/:id", a.deleteIPNSKey,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Delete IPNS key"),
				router.WithDescription("Deletes an IPNS key (soft delete). Cannot delete keys referenced by active websites."),
				router.WithTags("ipns"),
				router.WithPathParam("id", "IPNS key ID", ""),
				router.WithSuccessResponse(http.StatusNoContent, "IPNS key deleted"),
			),
		),
	)

	if err := router.RegisterRoutes(apiGroup, accessSvc, a.Subdomain(), ipnsKeyRoutes, router.WithMiddlewares(authMw), router.WithCors()); err != nil {
		return fmt.Errorf("failed to register ipns key routes: %w", err)
	}

	ipnsOpRoutes := router.DefineRoutes(
		router.NewRoute(http.MethodPost, "/ipns/publish", a.publishIPNS,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Publish CID to IPNS"),
				router.WithDescription("Publishes a CID to an IPNS key."),
				router.WithTags("ipns"),
				router.WithRequestBody(&dto.IPNSPublishRequest{}, "IPNS publish request", true),
				router.WithSuccessResponse(http.StatusOK, "IPNS publish result", router.WithJSONContent(dto.IPNSPublishResponse{})),
			),
		),
		router.NewRoute(http.MethodGet, "/ipns/resolve/:name", a.resolveIPNS,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Resolve IPNS name"),
				router.WithDescription("Resolves an IPNS name to its current CID."),
				router.WithTags("ipns"),
				router.WithPathParam("name", "IPNS name (peer ID)", ""),
				router.WithSuccessResponse(http.StatusOK, "IPNS resolve result", router.WithJSONContent(dto.IPNSResolveResponse{})),
			),
		),
		router.NewRoute(http.MethodPost, "/ipns/republish", a.republishIPNS,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Trigger IPNS republish"),
				router.WithDescription("Manually triggers IPNS record republishing for all keys."),
				router.WithTags("ipns"),
				router.WithSuccessResponse(http.StatusAccepted, "Republish triggered"),
			),
		),
	)

	if err := router.RegisterRoutes(apiGroup, accessSvc, a.Subdomain(), ipnsOpRoutes, router.WithMiddlewares(authMw), router.WithCors()); err != nil {
		return fmt.Errorf("failed to register ipns operation routes: %w", err)
	}

	websiteRoutes := router.DefineRoutes(
		router.NewRoute(http.MethodPost, "/websites", a.createWebsite,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Create website"),
				router.WithDescription("Creates a new website configuration. Returns 410 Gone if target is broken."),
				router.WithTags("websites"),
				router.WithRequestBody(&dto.WebsiteRequest{}, "Website request", true),
				router.WithSuccessResponse(http.StatusCreated, "Website created", router.WithJSONContent(dto.WebsiteResponse{})),
			),
		),
		router.NewRoute(http.MethodGet, "/websites", a.listWebsites,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("List websites"),
				router.WithDescription("Lists all websites for the current user with optional filtering."),
				router.WithTags("websites"),
				router.WithSuccessResponse(http.StatusOK, "List of websites", router.WithJSONContent(queryutil.Response[dto.WebsiteItem]{})),
			),
		),
		router.NewRoute(http.MethodGet, "/websites/:id", a.getWebsite,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Get website"),
				router.WithDescription("Retrieves details of a specific website. Returns 410 Gone if website is broken."),
				router.WithTags("websites"),
				router.WithPathParam("id", "Website ID", ""),
				router.WithSuccessResponse(http.StatusOK, "Website details", router.WithJSONContent(dto.WebsiteResponse{})),
			),
		),
		router.NewRoute(http.MethodPut, "/websites/:id", a.updateWebsite,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Update website"),
				router.WithDescription("Updates an existing website configuration."),
				router.WithTags("websites"),
				router.WithPathParam("id", "Website ID", ""),
				router.WithRequestBody(&dto.WebsiteRequest{}, "Website request", true),
				router.WithSuccessResponse(http.StatusOK, "Website updated", router.WithJSONContent(dto.WebsiteResponse{})),
			),
		),
		router.NewRoute(http.MethodDelete, "/websites/:id", a.deleteWebsite,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Delete website"),
				router.WithDescription("Deletes a website configuration (soft delete)."),
				router.WithTags("websites"),
				router.WithPathParam("id", "Website ID", ""),
				router.WithSuccessResponse(http.StatusNoContent, "Website deleted"),
			),
		),
		router.NewRoute(http.MethodPost, "/websites/:id/validate", a.validateWebsiteDNS,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Validate website DNS"),
				router.WithDescription("Triggers DNS TXT record validation for a website domain."),
				router.WithTags("websites"),
				router.WithPathParam("id", "Website ID", ""),
				router.WithSuccessResponse(http.StatusOK, "Validation result", router.WithJSONContent(dto.WebsiteValidateResponse{})),
			),
		),

	)

	if err := router.RegisterRoutes(apiGroup, accessSvc, a.Subdomain(), websiteRoutes, router.WithMiddlewares(authMw), router.WithCors()); err != nil {
		return fmt.Errorf("failed to register website routes: %w", err)
	}

	apiConfig := a.Config().GetAPI(internal.ProtocolName).(*pluginConfig.APIConfig)
	if apiConfig.GatewaySecret == "" {
		a.Logger().Warn("GatewaySecret is not configured - gateway authentication will fail for all requests")
	} else {
		a.Logger().Info("Gateway middleware initialized with configured secret")
	}
	gatewayAuthMw := pluginMw.GatewayAuth(apiConfig, a.Logger())
	gatewayRoutes := router.DefineRoutes(
		router.NewRoute(http.MethodGet, "/internal/websites/:domain", a.getGatewayWebsite,
			router.WithSwagger(
				router.WithSummary("Get website configuration for gateway"),
				router.WithDescription("Retrieves website configuration for gateway to serve content. Requires X-Gateway-Secret header."),
				router.WithTags("internal"),
				router.WithPathParam("domain", "The domain name of the website", ""),
				router.WithSuccessResponse(http.StatusOK, "Website configuration", router.WithJSONContent(dto.GatewayWebsiteResponse{})),
			),
		),
		router.NewRoute(http.MethodGet, "/internal/websites/:domain/status", a.getGatewayWebsiteStatus,
			router.WithSwagger(
				router.WithSummary("Get website status for gateway"),
				router.WithDescription("Retrieves website status information for gateway. Requires X-Gateway-Secret header."),
				router.WithTags("internal"),
				router.WithPathParam("domain", "The domain name of the website", ""),
				router.WithSuccessResponse(http.StatusOK, "Website status", router.WithJSONContent(dto.GatewayWebsiteStatusResponse{})),
			),
		),
	)

	if err := router.RegisterRoutes(r, accessSvc, a.Subdomain(), gatewayRoutes, router.WithMiddlewares(gatewayAuthMw), router.WithCors()); err != nil {
		return fmt.Errorf("failed to register gateway routes: %w", err)
	}

	// SSL status webhook routes with gateway auth (for Caddy plugin)
	sslStatusRoutes := router.DefineRoutes(
		router.NewRoute(http.MethodPost, "/websites/:domain/ssl-status", a.updateSSLStatus,
			router.WithSwagger(
				router.WithSummary("Update SSL status"),
				router.WithDescription("Webhook endpoint for Caddy plugin to update SSL certificate status. Requires X-Gateway-Secret header for authentication."),
				router.WithTags("websites", "webhooks"),
				router.WithPathParam("domain", "The domain name of the website", ""),
				router.WithRequestBody(&dto.SSLStatusUpdateRequest{}, "SSL status update", true),
				router.WithSuccessResponse(http.StatusOK, "SSL status updated", router.WithJSONContent(dto.WebsiteResponse{})),
				router.WithErrorResponses(router.DefineSwaggerErrorResponses(
					router.DefineSwaggerErrorResponse(http.StatusBadRequest, "Bad request - invalid domain or malformed request"),
					router.DefineSwaggerErrorResponse(http.StatusUnauthorized, "Unauthorized - missing or invalid X-Gateway-Secret header"),
					router.DefineSwaggerErrorResponse(http.StatusNotFound, "Website not found"),
					router.DefineSwaggerErrorResponse(http.StatusUnprocessableEntity, "Unprocessable entity - invalid status or timestamp format"),
					router.DefineSwaggerErrorResponse(http.StatusInternalServerError, "Internal server error"),
				)),
			),
		),
	)

	if err := router.RegisterRoutes(r, accessSvc, a.Subdomain(), sslStatusRoutes,
		router.WithMiddlewares(gatewayAuthMw),  // Use gateway auth, not user auth
		router.WithCors()); err != nil {
		return fmt.Errorf("failed to register SSL status routes: %w", err)
	}

	err = a.tus.SetupRoute(r, a.Subdomain(), true, false, TUS_HTTP_ROUTE)
	if err != nil {
		return err
	}

	return nil
}

func createCARReader(data io.Reader) (io.Reader, error) {
	buf := make([]byte, car.DefaultMaxAllowedHeaderSize)
	n, err := io.ReadFull(data, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, err
	}

	reader := bytes.NewReader(buf[:n])
	return reader, nil
}

func closeUpload(upload io.ReadCloser, logger *core.Logger) {
	err := upload.Close()
	if err != nil {
		logger.Error("Failed to close reader", zap.Error(err))
	}
}

func validateCARUpload(upload io.ReadCloser, tus core.TusHandler, ctx core.Context, sproto core.StorageProtocol, uploadId string, logger *core.Logger) bool {
	reader, err := createCARReader(upload)
	if err != nil {
		logger.Error("Failed to create CAR reader", zap.Error(err))
		err = tus.FailUploadById(ctx, sproto, uploadId)
		if err != nil {
			logger.Error("Failed to fail ipfsUpload", zap.Error(err))
		}
		return false
	}

	_, err = uploadpkg.GetCarRoots(reader, false)
	if err != nil {
		logger.Error("Failed to validate car", zap.Error(err))
		err = tus.FailUploadById(ctx, sproto, uploadId)
		if err != nil {
			logger.Error("Failed to fail ipfsUpload", zap.Error(err))
		}
		return false
	}

	return true
}

func getCARUploadHash(upload io.Reader, tus core.TusHandler, ctx core.Context, sproto core.StorageProtocol, uploadId string, logger *core.Logger) (core.StorageHash, error) {
	reader, err := createCARReader(upload)
	if err != nil {
		logger.Error("Failed to create CAR reader", zap.Error(err))
		err = tus.FailUploadById(ctx, sproto, uploadId)
		if err != nil {
			logger.Error("Failed to fail ipfsUpload", zap.Error(err))
		}
		return nil, err
	}

	cids, err := uploadpkg.GetCarRoots(reader, false)
	if err != nil {
		err = tus.FailUploadById(ctx, sproto, uploadId)
		if err != nil {
			logger.Error("Failed to fail ipfsUpload", zap.Error(err))
		}
		return nil, err
	}

	return internal.NewIPFSHash(cids[0]), nil
}

func processCARData(data io.Reader) (core.StorageHash, error) {
	reader, err := createCARReader(data)
	if err != nil {
		return nil, err
	}

	roots, err := uploadpkg.GetCarRoots(reader, false)
	if err != nil {
		return nil, err
	}

	return internal.NewIPFSHash(roots[0]), nil
}
