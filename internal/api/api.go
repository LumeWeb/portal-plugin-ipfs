package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	routingServer "github.com/ipfs/boxo/routing/http/server"
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
	dnsService         pluginCore.DNSService
	dnsConfig          *pluginConfig.DnsConfig
	tusService         core.TUSService
	requestService     core.RequestService
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
			api.dnsService = core.GetService[pluginCore.DNSService](ctx, pluginCore.DNS_SERVICE)
			api.dnsConfig = core.GetServiceConfig[*pluginConfig.DnsConfig](ctx, pluginCore.DNS_SERVICE)
			api.tusService = core.GetService[core.TUSService](ctx, core.TUS_SERVICE)
			api.requestService = core.GetService[core.RequestService](ctx, core.REQUEST_SERVICE)
			proto := core.GetProtocol(internal.ProtocolName)
			sproto := proto.(core.StorageProtocol)
			event.OnBootStartupFuncsCompleted(ctx, func(ctx core.Context, eventCtx context.Context) error {
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
							api.Logger().Error("Failed to get request reader", zap.Error(err))
							return
						}
						defer closeUpload(upload, api.Logger())

						if !validateCARUpload(upload, api.tus, ctx, sproto, hook.Upload.ID, api.Logger()) {
							return
						}

						size := hook.Upload.Size
						if size < 0 {
							api.Logger().Warn("Unexpected negative upload size in TUS completed hook", zap.Int64("size", size))
							return
						}
					}, protocol.TUS_UPLOAD_WORKFLOW,
						func(handlr core.TusHandler, hook handler.HookEvent, reader io.Reader) (core.StorageHash, error) {
							return getCARUploadHash(reader, api.tus, ctx, sproto, hook.Upload.ID, api.Logger())
						},
					),
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
		Title("Portal IPFS Plugin API").
		Version("1.0.0").
		Description(`
## Portal IPFS Plugin API

A comprehensive API for IPFS content management, including pinning services, file operations, IPNS key management, and website hosting.

### IPFS Pinning Service API Compatibility

This API is fully compatible with the [IPFS Pinning Service API specification](https://github.com/ipfs/pinning-services-api-spec), an implementation-agnostic API standard for pinning service providers. This ensures interoperability with existing IPFS pinning clients and tools.

### Features

- **Pinning**: Add, list, update, and remove pinned content
- **Content**: Upload files, retrieve IPFS content, and manage metadata
- **IPNS**: Manage IPNS keys and publish content
- **Files**: Browse and manage pinned files with directory navigation
- **Websites**: Create and manage website hosting with DNS and SSL automation

### Authentication

All API endpoints require authentication using JWT tokens obtained from the Portal authentication service.

### Rate Limiting

API requests are rate-limited based on user account tier. See Portal documentation for current limits.

### Documentation

For detailed API usage examples and integration guides, visit the Portal documentation website.
`).
		License("MIT", "https://opensource.org/licenses/MIT")
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
				router.WithSummary("List pinned content"),
				router.WithDescription(`Lists all pinned content with optional filtering.

Returns paginated list of pin objects representing content pinned to IPFS. Supports filtering by CID, name, status, metadata, and time range. When no filters are provided, only successfully pinned items are shown.

Use this to:
- Audit pinned content
- Search for specific pins
- Monitor pinning status

See also: POST /pins (add pin), GET /pins/{id} (get pin details)`),
				router.WithTags("Pinning"),
				router.WithQueryParam("cid", "Filter by content identifier. Supports multiple CIDs for batch queries. Example: QmHash1, QmHash2", []string{}),
				router.WithQueryParam("name", "Filter by pin name. Supports partial matching for search functionality.", ""),
				router.WithQueryParam("match", "Text matching strategy: 'exact' for exact match, 'iexact' for case-insensitive exact match, 'contains' for partial match", "exact"),
				router.WithQueryParam("status", "Filter by pin status: 'queued', 'pinning', 'pinned', 'failed', 'unpinned'. Example: pinned,failed", []string{}),
				router.WithQueryParam("before", "Filter for pins created before this ISO 8601 timestamp. Example: 2024-01-01T00:00:00Z", "2020-07-27T17:32:28.276Z"),
				router.WithQueryParam("after", "Filter for pins created after this ISO 8601 timestamp. Example: 2024-01-01T00:00:00Z", "2020-07-27T17:32:28.276Z"),
				router.WithQueryParam("limit", "Maximum number of records to return. Default: 10, Max: 100", 10),
				router.WithQueryParam("meta", "Filter by metadata key-value pairs. Format: JSON string. Example: {\"type\":\"document\"}", "{}"),
				router.WithSuccessResponse(http.StatusOK, "Successful response", router.WithJSONContent(dto.PinResultsResponse{})),
			),
		),
		router.NewRoute(http.MethodPost, "/pins", a.addPin,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Pin content to IPFS"),
				router.WithDescription(`Pins content to IPFS with optional metadata.

Creates a new pin object for the specified CID. Content will be pinned to the IPFS network and made available for retrieval. Supports adding names, origins, and custom metadata for organization.

Prerequisites: CID must be valid IPFS content identifier

See also: GET /pins (list pins), GET /pins/{id} (get pin details)`),
				router.WithTags("Pinning"),
				router.WithRequestBody(&dto.PinRequest{}, "Pin object", true),
				router.WithSuccessResponse(http.StatusAccepted, "Successful response", router.WithJSONContent(dto.PinStatusResponse{})),
			),
		),
		router.NewRoute(http.MethodGet, "/pins/:requestid", a.getPin,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Get pin details"),
				router.WithDescription(`Retrieves detailed information about a specific pin.

Returns the current status, metadata, and progress for a pin object. Use this to monitor pinning operations and verify content availability.

See also:.*`),
				router.WithTags("Pinning"),
				router.WithPathParam("requestid", "Unique identifier for the pin operation. Example: bafkreiexample", ""),
				router.WithSuccessResponse(http.StatusOK, "Successful response", router.WithJSONContent(dto.PinStatusResponse{})),
			),
		),
		router.NewRoute(http.MethodPost, "/pins/:requestid", a.replacePin,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Replace pinned content"),
				router.WithDescription(`Updates metadata and settings for an existing pin.

Modifies the name, origins, or metadata of a pinned item without changing the CID. Useful for reorganizing or updating pin information.

See also:.*`),
				router.WithTags("Pinning"),
				router.WithPathParam("requestid", "Unique identifier for the pin operation. Example: bafkreiexample", ""),
				router.WithRequestBody(&dto.PinRequest{}, "Pin object", true),
				router.WithSuccessResponse(http.StatusAccepted, "Successful response", router.WithJSONContent(dto.PinStatusResponse{})),
			),
		),
		router.NewRoute(http.MethodDelete, "/pins/:requestid", a.deletePin,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Remove pinned content"),
				router.WithDescription(`Removes a pin object, unpins content from the network.

Deletes the pin object and unpins the content from IPFS storage. Content may still be retrievable from other nodes that have cached it.

See also:.*`),
				router.WithTags("Pinning"),
				router.WithPathParam("requestid", "Unique identifier for the pin operation. Example: bafkreiexample", ""),
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
				router.WithSummary("Browse pinned files"),
				router.WithDescription(`Browses all pinned files and directories.

Returns a paginated list of files and folders in the root of your pinned content. Each item includes metadata like size, type, and creation date.

See also:.*`),
				router.WithSchema(fileManagerListProvider),
				router.WithTags("Files"),
				router.WithSuccessResponse(http.StatusOK, "Successful response", router.WithJSONContent(dto.FileManagerItemResponse{})),
			),
		),
		router.NewRoute(http.MethodGet, "/files/directory", a.listDirectoryContents,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("List directory"),
				router.WithDescription(`Lists contents of a specific directory.

Returns files and subdirectories within the specified path. Use for navigating through your pinned content hierarchy.

See also:.*`),
				router.WithSchema(fileManagerFilterProvider),
				router.WithTags("Files"),
				router.WithSuccessResponse(http.StatusOK, "Successful response", router.WithJSONContent(dto.FileManagerItemResponse{})),
			),
		),
		router.NewRoute(http.MethodGet, "/files/breadcrumbs", a.getBreadcrumbs,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Get navigation breadcrumbs"),
				router.WithDescription(`Gets navigation breadcrumbs for a file path.

Returns an array of parent directories from root to the specified path, enabling breadcrumb navigation in file browsers.

See also:.*`),
				router.WithSchema(fileManagerFilterProvider),
				router.WithTags("Files"),
				router.WithSuccessResponse(http.StatusOK, "Successful response", router.WithJSONContent(dto.FileManagerItemResponse{})),
			),
		),
	)

	// Register pin routes at root level to match IPFS pinning service API specification
	if err := router.RegisterRoutes(r, accessSvc, a.Subdomain(), pinRoutes, router.WithMiddlewares(authMw), router.WithCors()); err != nil {
		return fmt.Errorf("failed to register pin routes: %w", err)
	}

	apiGroup, err := r.Group("/api")
	if err != nil {
		return fmt.Errorf("failed to create api group: %w", err)
	}

	if err := router.RegisterRoutes(apiGroup, accessSvc, a.Subdomain(), fileManagerRoutes, router.WithMiddlewares(authMw), router.WithCors()); err != nil {
		return fmt.Errorf("failed to register file manager routes: %w", err)
	}

	ipfsRoutes := router.DefineRoutes(
		router.NewRoute(http.MethodPost, "/upload", a.handleUpload,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Upload file to IPFS"),
				router.WithDescription(`Uploads a file to IPFS and returns the content CID.

Supports single file uploads with automatic content verification. The uploaded file is added to IPFS and pinned for persistence.

See also:.*`),
				router.WithTags("Content"),
				router.WithFileUpload("File to upload", true),
				router.WithSuccessResponse(http.StatusOK, "File uploaded successfully"),
			),
		),
		router.NewRoute(http.MethodGet, "/upload/result/:identifier", a.handleUploadResult,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Get upload result"),
				router.WithDescription(`Retrieves the result of an upload operation by identifier.

Returns the root CID for completed uploads, or the current processing status for pending uploads.
Accepts either a TUS upload ID or a numeric request ID as the identifier.

See also: POST /upload (upload file), GET /pins/{id} (get pin details)`),
				router.WithTags("Content"),
				router.WithPathParam("identifier", "TUS upload ID or numeric request ID. Example: abc123-def456 or 42", ""),
				router.WithSuccessResponse(http.StatusOK, "Upload result", router.WithJSONContent(dto.UploadResultResponse{})),
			),
		),
		router.NewRoute(http.MethodGet, "/block/meta/:cid", a.handleGetBlockMeta,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Get block details"),
				router.WithDescription(`Retrieves metadata for a specific IPFS block.

Returns block size, type, and child CIDs for the specified content identifier. Useful for inspecting IPFS data structures.

See also:.*`),
				router.WithTags("Content"),
				router.WithPathParam("cid", "Content identifier (CID) of the IPFS block. Example: bafybeieffnocaq7t4w4daagvydl32igft5oziyyaebqr6vx6rb3fwh2ab4", ""),
				router.WithSuccessResponse(http.StatusOK, "Block metadata", router.WithJSONContent(dto.BlockMetaResponse{})),
			),
		),
		router.NewRoute(http.MethodPost, "/block/meta/batch", a.handleGetBlockMetaBatch,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Get multiple block details"),
				router.WithDescription(`Retrieves metadata for multiple IPFS blocks efficiently.

Batch endpoint for getting block information for multiple CIDs in a single request, reducing API calls.

See also:.*`),
				router.WithTags("Content"),
				router.WithRequestBody(&dto.GetBlockMetaBatchRequest{}, "Batch request for block metadata", true),
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
				router.WithSummary("Retrieve IPFS content"),
				router.WithDescription(`Retrieves IPFS content by CID.

Returns the raw content for the specified CID. Supports streaming for large files. Content is served directly from IPFS storage.

See also:.*`),
				router.WithTags("Content"),
				router.WithPathParam("cid", "The CID of the content.", ""),
			),
		),
		router.NewRoute(http.MethodHead, "/ipfs/:cid", a.handleIPFSGet,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Check content exists"),
				router.WithDescription(`Checks if content exists on IPFS.

Performs a HEAD request to verify content availability without downloading the data. Returns 200 if content exists, 404 if not found.

See also:.*`),
				router.WithTags("Content"),
				router.WithPathParam("cid", "The CID of the content.", ""),
			),
		),
		router.NewRoute(http.MethodOptions, "/ipfs/:cid", a.handleIPFSOptions,
			router.WithSwagger(
				router.WithSummary("IPFS content OPTIONS"),
				router.WithDescription(`CORS preflight handler for IPFS content.

Handles OPTIONS requests for IPFS content endpoints. Most CORS preflight requests are handled by middleware, this serves as a fallback.

See also:.*`),
				router.WithTags("Content"),
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
				router.WithSummary("Create IPNS key"),
				router.WithDescription(`Creates or imports an IPNS key for content publishing.

Generates a new IPNS key or imports an existing private key. IPNS keys allow publishing and updating mutable addresses that point to IPFS content.

See also:.*`),
				router.WithTags("IPNS"),
				router.WithRequestBody(&dto.IPNSKeyRequest{}, "IPNS key request", true),
				router.WithSuccessResponse(http.StatusCreated, "IPNS key created", router.WithJSONContent(dto.IPNSKeyResponse{})),
			),
		),
		router.NewRoute(http.MethodGet, "/ipns/keys", a.listIPNSKeys,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("List IPNS keys"),
				router.WithDescription(`Lists all IPNS keys owned by the current user.

Returns metadata for all IPNS keys including peer IDs, names, and creation dates. Useful for managing your IPNS publishing keys.

See also:.*`),
				router.WithTags("IPNS"),
				router.WithSuccessResponse(http.StatusOK, "List of IPNS keys", router.WithJSONContent(dto.IPNSKeyListResponseResponse{})),
			),
		),
		router.NewRoute(http.MethodGet, "/ipns/keys/:id", a.getIPNSKey,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Get IPNS key"),
				router.WithDescription(`Gets detailed information about an IPNS key.

Returns full key metadata including peer ID, name, and creation timestamp. Use to inspect key properties before publishing.

See also:.*`),
				router.WithTags("IPNS"),
				router.WithPathParam("id", "Numeric ID of the IPNS key. Example: 123", ""),
				router.WithSuccessResponse(http.StatusOK, "IPNS key details", router.WithJSONContent(dto.IPNSKeyResponse{})),
			),
		),
		router.NewRoute(http.MethodDelete, "/ipns/keys/:id", a.deleteIPNSKey,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Delete IPNS key"),
				router.WithDescription(`Deletes an IPNS key (soft delete).

Removes the IPNS key from your account. Keys referenced by active websites cannot be deleted. This is a soft delete that preserves records.

See also:.*`),
				router.WithTags("IPNS"),
				router.WithPathParam("id", "Numeric ID of the IPNS key. Example: 123", ""),
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
				router.WithSummary("Publish to IPNS"),
				router.WithDescription(`Publishes content to an IPNS key.

Updates an IPNS key to point to a new CID. Allows updating mutable addresses without changing the URL. Supports setting TTL for record validity.

See also:.*`),
				router.WithTags("IPNS"),
				router.WithRequestBody(&dto.IPNSPublishRequest{}, "IPNS publish request", true),
				router.WithSuccessResponse(http.StatusOK, "IPNS publish result", router.WithJSONContent(dto.IPNSPublishResponse{})),
			),
		),
		router.NewRoute(http.MethodGet, "/ipns/resolve/:name", a.resolveIPNS,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Resolve IPNS"),
				router.WithDescription(`Resolves an IPNS name to its current target CID.

Looks up the current CID that an IPNS name points to. Returns the resolved CID, validity information, and sequence number.

See also:.*`),
				router.WithTags("IPNS"),
				router.WithPathParam("name", "IPNS name (peer ID). Example: 12D3KooW...", ""),
				router.WithQueryParam("check_routing", "Whether to verify routing through the IPFS network. 1 = verify (queries DHT), 0 = local only. Default: 0", "0"),
				router.WithSuccessResponse(http.StatusOK, "IPNS resolve result", router.WithJSONContent(dto.IPNSResolveResponse{})),
			),
		),
		router.NewRoute(http.MethodPost, "/ipns/keys/:id/republish", a.republishIPNS,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Republish IPNS record"),
				router.WithDescription(`Republishes an IPNS record for a specific key owned by the authenticated user.

Re-broadcasts the current IPNS record to the network. Useful for refreshing records that may have fallen out of the DHT.

Prerequisites: User must own the specified IPNS key.`),
				router.WithTags("IPNS"),
				router.WithPathParam("id", "IPNS key ID", ""),
				router.WithSuccessResponse(http.StatusOK, "Republish result", router.WithJSONContent(dto.IPNSRepublishResponse{})),
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
				router.WithDescription(`Creates a new website configuration for IPFS content.

Sets up a website with a domain name pointing to IPFS content. Supports automatic DNS management and SSL certificate provisioning.

Prerequisites: Domain must be configured, target CID must exist

See also:.*`),
				router.WithTags("Websites"),
				router.WithRequestBody(&dto.WebsiteRequest{}, "Website request", true),
				router.WithSuccessResponse(http.StatusCreated, "Website created", router.WithJSONContent(dto.WebsiteResponse{})),
			),
		),
		router.NewRoute(http.MethodGet, "/websites", a.listWebsites,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("List websites"),
				router.WithDescription(`Lists all websites owned by the current user.

Returns paginated list of website configurations with status, domain, and target information. Supports filtering by status and other criteria.

See also:.*`),
				router.WithTags("Websites"),
				router.WithSuccessResponse(http.StatusOK, "List of websites", router.WithJSONContent(dto.WebsiteItemResponse{})),
			),
		),
		router.NewRoute(http.MethodGet, "/websites/:id", a.getWebsite,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Get website"),
				router.WithDescription(`Gets detailed information about a website.

Returns full website configuration including domain, target CID, SSL status, and DNS settings. Returns 410 Gone if the target content is broken.

See also:.*`),
				router.WithTags("Websites"),
				router.WithPathParam("id", "Website ID", ""),
				router.WithSuccessResponse(http.StatusOK, "Website details", router.WithJSONContent(dto.WebsiteResponse{})),
			),
		),
		router.NewRoute(http.MethodPut, "/websites/:id", a.updateWebsite,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Update website"),
				router.WithDescription(`Updates an existing website configuration.

Modifies domain, target CID, DNS hosting, or other settings for a website. Only fields included in the request body will be updated; omitted fields remain unchanged. Changes take effect after validation and DNS propagation.

See also:.*`),
				router.WithTags("Websites"),
				router.WithPathParam("id", "Website ID", ""),
				router.WithRequestBody(&dto.WebsiteUpdateRequest{}, "Website update request", true),
				router.WithSuccessResponse(http.StatusOK, "Website updated", router.WithJSONContent(dto.WebsiteResponse{})),
			),
		),
		router.NewRoute(http.MethodDelete, "/websites/:id", a.deleteWebsite,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Delete website"),
				router.WithDescription(`Deletes a website configuration.

Performs a soft delete, marking the website as deleted without removing it from the database. Website remains accessible until garbage collection.

See also:.*`),
				router.WithTags("Websites"),
				router.WithPathParam("id", "Website ID", ""),
				router.WithSuccessResponse(http.StatusNoContent, "Website deleted"),
			),
		),
		router.NewRoute(http.MethodPost, "/websites/:id/validate", a.validateWebsiteDNS,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Validate website DNS"),
				router.WithDescription(`Triggers DNS TXT record validation for a website.

Initiates DNS validation to verify domain ownership. Required before SSL certificate issuance. Returns validation result and any errors.

Prerequisites: DNS TXT record must be configured

See also:.*`),
				router.WithTags("Websites"),
				router.WithPathParam("id", "Website ID", ""),
				router.WithSuccessResponse(http.StatusOK, "Validation result", router.WithJSONContent(dto.WebsiteValidateResponse{})),
			),
		),

	)

	if err := router.RegisterRoutes(apiGroup, accessSvc, a.Subdomain(), websiteRoutes, router.WithMiddlewares(authMw), router.WithCors()); err != nil {
		return fmt.Errorf("failed to register website routes: %w", err)
	}

	websiteConfigRoutes := router.DefineRoutes(
		router.NewRoute(http.MethodGet, "/websites/config", a.getWebsiteConfig,
			router.WithSwagger(
				router.WithSummary("Get website hosting configuration"),
				router.WithDescription(`Returns website hosting configuration including the gateway domain.

Clients use this endpoint to discover the gateway domain they should point their custom domain's DNS records to when hosting a website. This is required when dns_hosting_enabled is false and the user manages their own DNS.

See also:.*`),
				router.WithTags("Websites"),
				router.WithSuccessResponse(http.StatusOK, "Website configuration", router.WithJSONContent(dto.WebsiteConfigResponse{})),
			),
		),
	)

	if err := router.RegisterRoutes(apiGroup, accessSvc, a.Subdomain(), websiteConfigRoutes, router.WithCors()); err != nil {
		return fmt.Errorf("failed to register website config routes: %w", err)
	}

	// DNS routes for zone and record management
	dnsRoutes := router.DefineRoutes(
		router.NewRoute(http.MethodPost, "/dns/zones", a.createZone,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Create DNS zone"),
				router.WithDescription(`Creates a new DNS zone for a domain.

Creates a zone for managing DNS records. The domain must be valid and not already exist.

See also:.*`),
				router.WithTags("DNS", "Zones"),
				router.WithRequestBody(&dto.ZoneRequest{}, "Zone request", true),
				router.WithSuccessResponse(http.StatusCreated, "Zone created", router.WithJSONContent(dto.ZoneResponse{})),
			),
		),
		router.NewRoute(http.MethodGet, "/dns/zones", a.listZones,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("List DNS zones"),
				router.WithDescription(`Lists all DNS zones owned by the current user.

Returns paginated list of zones with filtering support.

See also:.*`),
				router.WithTags("DNS", "Zones"),
				router.WithSuccessResponse(http.StatusOK, "List of zones", router.WithJSONContent(dto.ZoneListResponseResponse{})),
			),
		),
		router.NewRoute(http.MethodGet, "/dns/zones/:id", a.getZone,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Get DNS zone"),
				router.WithDescription(`Gets detailed information about a DNS zone.

Returns full zone configuration including domain and status.

See also:.*`),
				router.WithTags("DNS", "Zones"),
				router.WithPathParam("id", "Zone ID", ""),
				router.WithSuccessResponse(http.StatusOK, "Zone details", router.WithJSONContent(dto.ZoneResponse{})),
			),
		),
		router.NewRoute(http.MethodPut, "/dns/zones/:id", a.updateZone,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Update DNS zone"),
				router.WithDescription(`Updates a DNS zone configuration.

Only certain fields can be updated after creation.

See also:.*`),
				router.WithTags("DNS", "Zones"),
				router.WithPathParam("id", "Zone ID", ""),
				router.WithRequestBody(&dto.ZoneRequest{}, "Zone request", true),
				router.WithSuccessResponse(http.StatusOK, "Zone updated", router.WithJSONContent(dto.ZoneResponse{})),
			),
		),
		router.NewRoute(http.MethodDelete, "/dns/zones/:id", a.deleteZone,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Delete DNS zone"),
				router.WithDescription(`Deletes a DNS zone and all its records.

Performs a soft delete, marking the zone as deleted without removing it from the database.

See also:.*`),
				router.WithTags("DNS", "Zones"),
				router.WithPathParam("id", "Zone ID", ""),
				router.WithSuccessResponse(http.StatusNoContent, "Zone deleted"),
			),
		),
		router.NewRoute(http.MethodPost, "/dns/zones/:id/validate", a.validateZone,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Validate DNS zone"),
				router.WithDescription(`Validates nameservers for a DNS zone.

Checks if the nameservers are properly configured for the zone.

See also:.*`),
				router.WithTags("DNS", "Zones"),
				router.WithPathParam("id", "Zone ID", ""),
				router.WithSuccessResponse(http.StatusOK, "Validation result", router.WithJSONContent(dto.ValidationResponse{})),
			),
		),
		router.NewRoute(http.MethodGet, "/dns/zones/:id/status", a.getZoneStatus,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Get DNS zone status"),
				router.WithDescription(`Gets the current status of a DNS zone.

Returns zone status and configuration details.

See also:.*`),
				router.WithTags("DNS", "Zones"),
				router.WithPathParam("id", "Zone ID", ""),
				router.WithSuccessResponse(http.StatusOK, "Zone status", router.WithJSONContent(dto.ZoneResponse{})),
			),
		),
		router.NewRoute(http.MethodGet, "/dns/zones/:id/records", a.listRecords,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("List DNS records"),
				router.WithDescription(`Lists all DNS records for a zone.

Returns paginated list of records with filtering support.

See also:.*`),
				router.WithTags("DNS", "Records"),
				router.WithPathParam("id", "Zone ID", ""),
				router.WithSuccessResponse(http.StatusOK, "List of records", router.WithJSONContent(dto.RecordResponseResponse{})),
			),
		),
		router.NewRoute(http.MethodGet, "/dns/zones/:id/records/:name/:type", a.getRecord,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Get DNS record"),
				router.WithDescription(`Gets a specific DNS record by name and type.

Returns record details including content and TTL.

See also:.*`),
				router.WithTags("DNS", "Records"),
				router.WithPathParam("id", "Zone ID", ""),
				router.WithPathParam("name", "Record name", ""),
				router.WithPathParam("type", "Record type (A, AAAA, CNAME, TXT, etc.)", ""),
				router.WithSuccessResponse(http.StatusOK, "Record details", router.WithJSONContent(dto.RecordResponse{})),
			),
		),
		router.NewRoute(http.MethodPost, "/dns/zones/:id/records", a.createRecord,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Create DNS record"),
				router.WithDescription(`Creates a new DNS record in a zone.

Creates a record with the specified name, type, and content.

See also:.*`),
				router.WithTags("DNS", "Records"),
				router.WithPathParam("id", "Zone ID", ""),
				router.WithRequestBody(&dto.RecordRequest{}, "Record request", true),
				router.WithSuccessResponse(http.StatusCreated, "Record created", router.WithJSONContent(dto.RecordResponse{})),
			),
		),
		router.NewRoute(http.MethodPut, "/dns/zones/:id/records/:name/:type", a.updateRecord,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Update DNS record"),
				router.WithDescription(`Updates an existing DNS record.

Updates the content and TTL of a record.

See also:.*`),
				router.WithTags("DNS", "Records"),
				router.WithPathParam("id", "Zone ID", ""),
				router.WithPathParam("name", "Record name", ""),
				router.WithPathParam("type", "Record type (A, AAAA, CNAME, TXT, etc.)", ""),
				router.WithRequestBody(&dto.RecordRequest{}, "Record request", true),
				router.WithSuccessResponse(http.StatusOK, "Record updated", router.WithJSONContent(dto.RecordResponse{})),
			),
		),
		router.NewRoute(http.MethodDelete, "/dns/zones/:id/records/:name/:type", a.deleteRecord,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Delete DNS record"),
				router.WithDescription(`Deletes a DNS record from a zone.

Removes the specified record from the zone.

See also:.*`),
				router.WithTags("DNS", "Records"),
				router.WithPathParam("id", "Zone ID", ""),
				router.WithPathParam("name", "Record name", ""),
				router.WithPathParam("type", "Record type (A, AAAA, CNAME, TXT, etc.)", ""),
				router.WithSuccessResponse(http.StatusNoContent, "Record deleted"),
			),
		),
		router.NewRoute(http.MethodPost, "/dns/zones/:id/records/bulk", a.bulkRecords,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Bulk create DNS records"),
				router.WithDescription(`Creates multiple DNS records in a zone.

Creates all records in the request. Errors are reported for individual records that fail.

See also:.*`),
				router.WithTags("DNS", "Records"),
				router.WithPathParam("id", "Zone ID", ""),
				router.WithRequestBody(&dto.BulkRecordRequest{}, "Bulk record request", true),
				router.WithSuccessResponse(http.StatusOK, "Bulk operation result", router.WithJSONContent(dto.BulkRecordsResponse{})),
			),
		),
		router.NewRoute(http.MethodPost, "/dns/zones/:id/records/bulk-delete", a.bulkDeleteRecords,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Bulk delete DNS records"),
				router.WithDescription(`Deletes multiple DNS records from a zone.

Deletes all records in the request. Can be run in dry-run mode to preview changes.

See also:.*`),
				router.WithTags("DNS", "Records"),
				router.WithPathParam("id", "Zone ID", ""),
				router.WithRequestBody(&dto.BulkDeleteRequest{}, "Bulk delete request", true),
				router.WithSuccessResponse(http.StatusOK, "Bulk delete result", router.WithJSONContent(dto.BulkDeleteResponse{})),
			),
		),
	)

	if err := router.RegisterRoutes(apiGroup, accessSvc, a.Subdomain(), dnsRoutes, router.WithMiddlewares(authMw), router.WithCors()); err != nil {
		return fmt.Errorf("failed to register dns routes: %w", err)
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
				router.WithDescription(`Gets website configuration for gateway content serving.

Internal endpoint used by the gateway to retrieve website configuration. Requires X-Gateway-Secret header for authentication.

See also:.*`),
				router.WithTags("Gateway"),
				router.WithPathParam("domain", "Domain name for the website. Example: example.com", ""),
				router.WithSuccessResponse(http.StatusOK, "Website configuration", router.WithJSONContent(dto.GatewayWebsiteResponse{})),
			),
		),
		router.NewRoute(http.MethodGet, "/internal/websites/:domain/status", a.getGatewayWebsiteStatus,
			router.WithSwagger(
				router.WithSummary("Get website status for gateway"),
				router.WithDescription(`Gets website status for gateway monitoring.

Internal endpoint used by the gateway to check website health and availability. Requires X-Gateway-Secret header for authentication.

See also:.*`),
				router.WithTags("Gateway"),
				router.WithPathParam("domain", "Domain name for the website. Example: example.com", ""),
				router.WithSuccessResponse(http.StatusOK, "Website status", router.WithJSONContent(dto.GatewayWebsiteStatusResponse{})),
			),
		),
	)

	if err := router.RegisterRoutes(r, accessSvc, a.Subdomain(), gatewayRoutes, router.WithMiddlewares(gatewayAuthMw), router.WithCors()); err != nil {
		return fmt.Errorf("failed to register gateway routes: %w", err)
	}

	// Public info endpoint
	publicInfoRoutes := router.DefineRoutes(
		router.NewRoute(http.MethodGet, "/info", a.handleGetInfo,
			router.WithSwagger(
				router.WithSummary("Get node information"),
				router.WithDescription(`Retrieves information about the IPFS node.

Returns node identity (peer ID) and network connection addresses. Useful for diagnostics and verifying node connectivity.

See also:.*`),
				router.WithTags("Content"),
				router.WithSuccessResponse(http.StatusOK, "Node information", router.WithJSONContent(dto.InfoResponse{})),
			),
		),
	)

	if err := router.RegisterRoutes(apiGroup, accessSvc, a.Subdomain(), publicInfoRoutes, router.WithCors()); err != nil {
		return fmt.Errorf("failed to register public info routes: %w", err)
	}

	// Mount boxo Delegated Routing V1 HTTP server (IPIP-337).
	// Individual routes are registered with portal-router for swagger docs,
	// but all delegate to the boxo server handler for spec-compliant responses.
	routingHandler := routingServer.Handler(&pinnerDelegatedRouter{api: a})
	routingWrapped := echo.WrapHandler(routingHandler)

	routingV1Routes := router.DefineRoutes(
		router.NewRoute(http.MethodGet, "/routing/v1/ipns/:name", routingWrapped,
			router.WithSwagger(
				router.WithSummary("Get IPNS record"),
				router.WithDescription(`Returns the signed IPNS record for the given peer ID.

Implements the IPFS Delegated Routing V1 HTTP API (IPIP-337). The response is a raw IPNS record in binary format (application/vnd.ipfs.ipns-record).

See also: https://specs.ipfs.tech/ipips/ipip-337/`),
				router.WithTags("Routing"),
				router.WithPathParam("name", "Peer ID of the IPNS key", ""),
				router.WithSuccessResponse(http.StatusOK, "IPNS record", router.WithContent("application/vnd.ipfs.ipns-record", "Raw IPNS record bytes")),
			),
		),
		router.NewRoute(http.MethodGet, "/routing/v1/providers/:cid", routingWrapped,
			router.WithSwagger(
				router.WithSummary("Find providers for a CID"),
				router.WithDescription(`Returns provider records for the given CID.

Implements the IPFS Delegated Routing V1 HTTP API (IPIP-337). Supports both JSON and streaming NDJSON response formats via content negotiation (Accept header).

See also: https://specs.ipfs.tech/ipips/ipip-337/`),
				router.WithTags("Routing"),
				router.WithPathParam("cid", "Content identifier", ""),
				router.WithSuccessResponse(http.StatusOK, "Provider records", router.WithContent("application/x-ndjson", "NDJSON stream of provider records")),
			),
		),
	)

	if err := router.RegisterRoutes(r, accessSvc, a.Subdomain(), routingV1Routes, router.WithCors()); err != nil {
		return fmt.Errorf("failed to register routing routes: %w", err)
	}

	// SSL status routes
	// Public GET endpoint for developers to query SSL status
	publicSSLStatusRoutes := router.DefineRoutes(
		router.NewRoute(http.MethodGet, "/websites/:domain/ssl-status", a.getSSLStatus,
			router.WithSwagger(
				router.WithSummary("Get SSL status"),
				router.WithDescription(`Gets SSL certificate status for a website.

Public endpoint for checking SSL certificate status. Returns the current state of SSL provisioning including issuance date, expiration, and any errors.

See also:.*`),
				router.WithTags("Websites"),
				router.WithPathParam("domain", "Domain name for the website. Example: example.com", ""),
				router.WithSuccessResponse(http.StatusOK, "SSL status", router.WithJSONContent(dto.WebsiteResponse{})),
				router.WithErrorResponses(router.DefineSwaggerErrorResponses(
					router.DefineSwaggerErrorResponse(http.StatusBadRequest, "Invalid domain format or malformed request"),
					router.DefineSwaggerErrorResponse(http.StatusNotFound, "Website not found or does not exist"),
					router.DefineSwaggerErrorResponse(http.StatusInternalServerError, "Internal server error occurred"),
				)),
			),
		),
	)

	if err := router.RegisterRoutes(apiGroup, accessSvc, a.Subdomain(), publicSSLStatusRoutes, router.WithCors()); err != nil {
		return fmt.Errorf("failed to register public SSL status routes: %w", err)
	}

	// Internal POST endpoint for Caddy webhook with gateway auth
	internalSSLStatusRoutes := router.DefineRoutes(
		router.NewRoute(http.MethodPost, "/internal/websites/:domain/ssl-status", a.updateSSLStatus,
			router.WithSwagger(
				router.WithSummary("Update SSL status"),
				router.WithDescription(`Webhook endpoint for Caddy plugin to update SSL status.

Internal webhook called by Caddy when SSL certificates are issued or updated. Requires X-Gateway-Secret header for authentication.

See also:.*`),
				router.WithTags("internal"),
				router.WithPathParam("domain", "Domain name for the website. Example: example.com", ""),
				router.WithRequestBody(&dto.SSLStatusUpdateRequest{}, "SSL status update", true),
				router.WithSuccessResponse(http.StatusOK, "SSL status updated", router.WithJSONContent(dto.WebsiteResponse{})),
				router.WithErrorResponses(router.DefineSwaggerErrorResponses(
					router.DefineSwaggerErrorResponse(http.StatusBadRequest, "Invalid domain format or malformed request body"),
					router.DefineSwaggerErrorResponse(http.StatusUnauthorized, "Missing or invalid X-Gateway-Secret header in request"),
					router.DefineSwaggerErrorResponse(http.StatusNotFound, "Website domain not found"),
					router.DefineSwaggerErrorResponse(http.StatusUnprocessableEntity, "Invalid status value or timestamp format in request"),
					router.DefineSwaggerErrorResponse(http.StatusInternalServerError, "Internal server error occurred"),
				)),
			),
		),
	)

	if err := router.RegisterRoutes(r, accessSvc, a.Subdomain(), internalSSLStatusRoutes,
		router.WithMiddlewares(gatewayAuthMw),  // Use gateway auth, not user auth
		router.WithCors()); err != nil {
		return fmt.Errorf("failed to register internal SSL status routes: %w", err)
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
		logger.Warn("Upload is not CAR format, skipping CAR validation", zap.Error(err))
		return true
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
		logger.Warn("Upload is not CAR format, skipping hash computation", zap.Error(err))
		return nil, nil
	}

	return internal.NewIPFSHash(cids[0]), nil
}
