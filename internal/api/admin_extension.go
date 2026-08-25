package api

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
	"go.lumeweb.com/httputil"
	"go.lumeweb.com/ipfs-content/paths"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/service/domain"
	"go.lumeweb.com/portal-router"
	"go.lumeweb.com/portal/config"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal-middleware/auth/jwt"
	mcontext "go.lumeweb.com/portal-middleware/context"
	portalMw "go.lumeweb.com/portal-middleware/middleware"
	"go.lumeweb.com/queryutil"
	queryutilHttp "go.lumeweb.com/queryutil/http"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// AdminExtension extends the Admin API with IPFS website management functionality
type AdminExtension struct {
	ctx                core.Context
	logger             *core.Logger
	config             config.Manager
	db                 *gorm.DB
	websiteService     pluginCore.WebsiteService
	ipnsKeyService     pluginCore.IPNSKeyService
	delegatedDomainSvc *domain.DelegatedDomainService
}

// NewAdminExtension creates a new Admin API extension for IPFS website management
func NewAdminExtension() core.APIExtensionFactory {
	return func() (core.APIExtension, []core.ContextBuilderOption, error) {
		ext := &AdminExtension{}

		return ext, core.ContextOptions(core.ContextWithStartupFunc(func(ctx core.Context) error {
			ext.ctx = ctx
			ext.logger = ctx.NamedLogger("ipfs.admin_extension")
			ext.config = ctx.Config()

			// Get and verify required services
			ext.websiteService = core.GetService[pluginCore.WebsiteService](ctx, pluginCore.WEBSITE_SERVICE)
			if ext.websiteService == nil {
				return fmt.Errorf("website service not available")
			}

			ext.ipnsKeyService = core.GetService[pluginCore.IPNSKeyService](ctx, pluginCore.IPNS_KEY_SERVICE)
			if ext.ipnsKeyService == nil {
				return fmt.Errorf("ipns key service not available")
			}

			ext.delegatedDomainSvc = core.GetServiceOptional[*domain.DelegatedDomainService](ctx, pluginCore.DELEGATED_DOMAIN_SERVICE)

			return nil
		})), nil
	}
}

// TargetAPI returns the name of the API this extension targets
func (e *AdminExtension) TargetAPI() string {
	return "admin"
}

// Configure is called to set up routes on the admin API router
func (e *AdminExtension) Configure(gRouter router.Router, accessSvc core.AccessService) error {
	ipfsRouter, err := gRouter.Group("/api/ipfs")
	if err != nil {
		return err
	}

	// Authenticate operator tokens end-to-end so platform-domain handlers can
	// derive the operator's identity via GetUserID (same middleware as the main
	// API). Scoped to the platform-domain routes; the other admin handlers do
	// not require a per-request identity.
	authMw := portalMw.AuthMiddleware(e.Context(),
		portalMw.WithAuthPurpose(jwt.PurposeLogin, jwt.PurposeAPI),
	)

	if err := e.registerWebsiteHandlers(ipfsRouter, accessSvc); err != nil {
		return err
	}

	if err := e.registerIPNSHandlers(ipfsRouter, accessSvc); err != nil {
		return err
	}

	if err := e.registerPlatformDomainHandlers(ipfsRouter, accessSvc, authMw); err != nil {
		return err
	}

	return nil
}

// Config returns the config manager
func (e *AdminExtension) Config() config.Manager {
	return e.config
}

// SetConfig sets the config manager
func (e *AdminExtension) SetConfig(cfg config.Manager) {
	e.config = cfg
}

// Context returns the core context
func (e *AdminExtension) Context() core.Context {
	return e.ctx
}

// SetContext sets the core context
func (e *AdminExtension) SetContext(ctx core.Context) {
	e.ctx = ctx
}

// Logger returns the logger
func (e *AdminExtension) Logger() *core.Logger {
	return e.logger
}

// SetLogger sets the logger
func (e *AdminExtension) SetLogger(logger *core.Logger) {
	e.logger = logger
}

// ID returns the service ID
func (e *AdminExtension) ID() string {
	return "ipfs.admin_extension"
}

// DB returns the database connection
func (e *AdminExtension) DB() *gorm.DB {
	return e.db
}

// SetDB sets the database connection
func (e *AdminExtension) SetDB(db *gorm.DB) {
	e.db = db
}

// registerWebsiteHandlers registers website management routes
func (e *AdminExtension) registerWebsiteHandlers(gRouter router.Router, accessSvc core.AccessService) error {
	routes := router.DefineRoutes(
		router.NewRoute(http.MethodPost, "/websites/:id/block", e.blockWebsite,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Block website"),
				router.WithDescription(`Blocks a website by setting status to 'blocked'.

Admin-only operation that prevents deletion of problematic websites. Blocked websites remain in the system but cannot be modified or removed by users.

Prerequisites: Admin access required

See also: POST /websites/:id/unblock (unblock website)`),
				router.WithTags("Admin", "Websites", "Content"),
				router.WithPathParam("id", "Website ID", ""),
				router.WithSuccessResponse(http.StatusOK, "Website blocked", router.WithJSONContent(dto.WebsiteResponse{})),
			),
		),
		router.NewRoute(http.MethodPost, "/websites/:id/unblock", e.unblockWebsite,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Unblock website"),
				router.WithDescription(`Unblocks a website by restoring status to 'active'.

Admin-only operation that re-enables a previously blocked website, allowing users to manage it again.

Prerequisites: Admin access required

See also: POST /websites/:id/block (block website)`),
				router.WithTags("Admin", "Websites", "Content"),
				router.WithPathParam("id", "Website ID", ""),
				router.WithSuccessResponse(http.StatusOK, "Website unblocked", router.WithJSONContent(dto.WebsiteResponse{})),
			),
		),
	)

	apiGroup := internal.ProtocolName
	if err := router.RegisterRoutes(gRouter, accessSvc, apiGroup, routes); err != nil {
		return err
	}

	return nil
}

// parsePathID extracts and parses a uint ID from the path parameter
func (e *AdminExtension) parsePathID(c echo.Context, param string) (uint, error) {
	id, err := strconv.ParseUint(c.Param(param), 10, 64)
	if err != nil {
		return 0, err
	}
	return uint(id), nil
}

// blockWebsite blocks a website by setting its status to 'blocked'
func (e *AdminExtension) blockWebsite(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	websiteID, err := e.parsePathID(c, "id")
	if err != nil {
		apiErr := NewError(ErrKeyInvalidUUIDFormat, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	if err := e.websiteService.BlockWebsite(reqCtx, websiteID); err != nil {
		e.logger.Error("Failed to block website", zap.Error(err), zap.Uint("website_id", websiteID))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	return ctx.NoContent(http.StatusOK)
}

// unblockWebsite unblocks a website by setting its status back to 'active'
func (e *AdminExtension) unblockWebsite(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	websiteID, err := e.parsePathID(c, "id")
	if err != nil {
		apiErr := NewError(ErrKeyInvalidUUIDFormat, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	if err := e.websiteService.UnblockWebsite(reqCtx, websiteID); err != nil {
		e.logger.Error("Failed to unblock website", zap.Error(err), zap.Uint("website_id", websiteID))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	return ctx.NoContent(http.StatusOK)
}

func (e *AdminExtension) registerIPNSHandlers(gRouter router.Router, accessSvc core.AccessService) error {
	routes := router.DefineRoutes(
		router.NewRoute(http.MethodPost, "/ipns/republish", e.republishIPNS,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Trigger IPNS republish"),
				router.WithDescription(`Triggers IPNS record republishing for all keys on the node.

Admin-only operation that forces all IPNS records to be republished to the network. Useful for ensuring content remains available and records are refreshed.

Prerequisites: Admin access required`),
				router.WithTags("Admin", "IPNS"),
				router.WithSuccessResponse(http.StatusOK, "Republish result", router.WithJSONContent(dto.IPNSRepublishResponse{})),
			),
		),
	)

	apiGroup := internal.ProtocolName
	if err := router.RegisterRoutes(gRouter, accessSvc, apiGroup, routes); err != nil {
		return err
	}

	return nil
}

func (e *AdminExtension) republishIPNS(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	records, err := e.ipnsKeyService.ListPublished(reqCtx)
	if err != nil {
		e.logger.Error("Failed to list published records", zap.Error(err))
		apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("failed to list published records: %w", err))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	var count int
	for ipnsName, record := range records {
		peerID := ipnsName.Peer().String()

		if record == nil {
			e.logger.Warn("Nil record in published list, skipping", zap.String("peer_id", peerID))
			continue
		}

		privKey, _, err := e.ipnsKeyService.GetPrivateKeyByPeerID(reqCtx, peerID)
		if err != nil {
			e.logger.Warn("Failed to get private key for republish, skipping", zap.Error(err), zap.String("peer_id", peerID))
			continue
		}

		valuePath, err := record.Value()
		if err != nil {
			e.logger.Warn("Failed to get IPNS record value for republish, skipping", zap.Error(err), zap.String("peer_id", peerID))
			continue
		}

		cidStr, err := paths.ExtractCIDFromPathStrict(valuePath)
		if err != nil {
			e.logger.Warn("Invalid IPNS record path format, skipping", zap.Error(err), zap.String("peer_id", peerID))
			continue
		}

		if err := e.ipnsKeyService.PublishWithKey(core.DetachContext(reqCtx), privKey, cidStr, 0); err != nil {
			e.logger.Error("Failed to republish IPNS record, skipping", zap.Error(err), zap.String("peer_id", peerID))
			continue
		}

		count++
	}

	resp := dto.IPNSRepublishResponse{
		Count:   count,
		Message: fmt.Sprintf("Successfully republished %d IPNS record(s)", count),
	}

	return httputil.EncodeResponse(ctx, nil, &resp)
}
func (e *AdminExtension) registerPlatformDomainHandlers(gRouter router.Router, accessSvc core.AccessService, authMw echo.MiddlewareFunc) error {
	routes := router.DefineRoutes(
		router.NewRoute(http.MethodPost, "/platform-domains", e.createPlatformDomain,
			router.WithAccess(core.ACCESS_ADMIN_ROLE),
			router.WithSwagger(
				router.WithSummary("Register platform domain"),
				router.WithDescription(`Registers a platform-owned root domain that users can claim free subdomains under.

Operator-only operation. The operator's DNS zone for the root is auto-created (idempotently) from the authenticated operator.

See also: GET /platform-domains (list), PATCH /platform-domains/:id (enable/disable), DELETE /platform-domains/:id, POST /platform-domains/:id/bind`),
				router.WithTags("Admin", "PlatformDomains"),
				router.WithRequestBody(dto.PlatformDomainRequest{}, "Platform domain to register", true),
				router.WithSuccessResponse(http.StatusCreated, "Platform domain registered", router.WithJSONContent(dto.PlatformDomainResponse{})),
			),
		),
		router.NewRoute(http.MethodPost, "/platform-domains/:id/bind", e.bindPlatformRootApex,
			router.WithAccess(core.ACCESS_ADMIN_ROLE),
			router.WithSwagger(
				router.WithSummary("Bind website to platform root apex"),
				router.WithDescription(`Binds an operator-owned website directly to the root apex of a platform domain (e.g. "pinned.site").

Operator-only operation. The website must be owned by the authenticated operator. The apex binding reuses the platform root's auto-created zone.

See also: POST /platform-domains (register), PATCH /platform-domains/:id (enable/disable)`),
				router.WithTags("Admin", "PlatformDomains"),
				router.WithPathParam("id", "Platform domain ID", ""),
				router.WithRequestBody(dto.PlatformDomainBindRequest{}, "Website to bind to the root apex", true),
				router.WithSuccessResponse(http.StatusOK, "Website bound to platform root apex", router.WithJSONContent(dto.DomainResponse{})),
			),
		),
		router.NewRoute(http.MethodGet, "/platform-domains", e.listPlatformDomains,
			router.WithAccess(core.ACCESS_ADMIN_ROLE),
			router.WithSwagger(
				router.WithSummary("List platform domains"),
				router.WithDescription(`Lists all registered platform-owned roots, including disabled ones. Returns the standard paginated list shape ({data, total}) with query-param filtering and paging.`),
				router.WithTags("Admin", "PlatformDomains"),
				router.WithSuccessResponse(http.StatusOK, "Platform domains listed", router.WithJSONContent(dto.PlatformDomainListResponse{})),
			),
		),
		router.NewRoute(http.MethodPatch, "/platform-domains/:id", e.updatePlatformDomain,
			router.WithAccess(core.ACCESS_ADMIN_ROLE),
			router.WithSwagger(
				router.WithSummary("Update platform domain"),
				router.WithDescription(`Enables or disables a registered platform root. Disabling prevents new claims but does not delete existing bindings.`),
				router.WithTags("Admin", "PlatformDomains"),
				router.WithPathParam("id", "Platform domain ID", ""),
				router.WithRequestBody(dto.PlatformDomainUpdateRequest{}, "Platform domain update", true),
				router.WithSuccessResponse(http.StatusOK, "Platform domain updated", router.WithJSONContent(dto.PlatformDomainResponse{})),
			),
		),
		router.NewRoute(http.MethodDelete, "/platform-domains/:id", e.deletePlatformDomain,
			router.WithAccess(core.ACCESS_ADMIN_ROLE),
			router.WithSwagger(
				router.WithSummary("Delete platform domain"),
				router.WithDescription(`Removes a registered platform root. Existing subdomain bindings remain but can no longer be reconciled as platform subdomains.`),
				router.WithTags("Admin", "PlatformDomains"),
				router.WithPathParam("id", "Platform domain ID", ""),
				router.WithSuccessResponse(http.StatusNoContent, "Platform domain deleted"),
			router.WithoutDefaultSuccessResponse(),
			),
		),
	)

	apiGroup := internal.ProtocolName
	return router.RegisterRoutes(gRouter, accessSvc, apiGroup, routes, router.WithMiddlewares(authMw))
}

func (e *AdminExtension) createPlatformDomain(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	if e.delegatedDomainSvc == nil {
		apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("domain service not available"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	// The operator's zone is auto-created for the root; the operator is
	// derived from the authenticated admin context (never trusted from input).
	operatorUserID, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}

	req := dto.PlatformDomainRequest{}
	if _, ok := httputil.DecodeAndValidateRequest(ctx, &req); !ok {
		return nil
	}

	namespace := string(pluginDb.DomainNamespaceHNS)
	if req.Namespace != nil {
		namespace = *req.Namespace
	}

	pd, err := e.delegatedDomainSvc.CreatePlatformDomain(reqCtx, req.Domain, pluginDb.DomainNamespace(namespace), operatorUserID, req.Enabled)
	if err != nil {
		e.logger.Error("Failed to create platform domain", zap.Error(err))
		apiErr := NewError(ErrKeyValidationFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	resp := dto.PlatformDomainResponse{}
	resp.FromModel(pd)
	ctx.Response().Before(func() {
		ctx.Response().Status = http.StatusCreated
	})
	return httputil.EncodeResponse(ctx, pd, &resp)
}

// bindPlatformRootApex binds an operator-owned website to the root apex of a
// platform domain.
func (e *AdminExtension) bindPlatformRootApex(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	if e.delegatedDomainSvc == nil {
		apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("domain service not available"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	id, err := e.parsePathID(c, "id")
	if err != nil {
		apiErr := NewError(ErrKeyInvalidUUIDFormat, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	userID, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}

	req := dto.PlatformDomainBindRequest{}
	if _, ok := httputil.DecodeAndValidateRequest(ctx, &req); !ok {
		return nil
	}

	wd, err := e.delegatedDomainSvc.BindPlatformRootApex(reqCtx, req.WebsiteID, userID, id)
	if err != nil {
		e.logger.Error("Failed to bind platform root apex", zap.Error(err), zap.Uint("platform_domain_id", id), zap.Uint("website_id", req.WebsiteID))
		apiErr := NewError(ErrKeyValidationFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	resp := dto.DomainResponse{}
	if err := resp.FromModel(wd); err != nil {
		e.logger.Error("Failed to build domain response", zap.Error(err))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	return httputil.EncodeResponse(ctx, wd, &resp)
}

func (e *AdminExtension) listPlatformDomains(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	if e.delegatedDomainSvc == nil {
		apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("domain service not available"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	return queryutilHttp.ProcessListRequest[*pluginDb.PlatformDomain, dto.PlatformDomainResponse](
		c.Response(),
		c.Request(),
		"platform-domains",
		func(filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination) ([]*pluginDb.PlatformDomain, int64, error) {
			return e.delegatedDomainSvc.ListPlatformDomains(reqCtx, filters, sorts, pagination)
		},
		func(pd *pluginDb.PlatformDomain) dto.PlatformDomainResponse {
			var r dto.PlatformDomainResponse
			_ = r.FromModel(pd)
			return r
		},
	)
}

func (e *AdminExtension) updatePlatformDomain(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	if e.delegatedDomainSvc == nil {
		apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("domain service not available"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	id, err := e.parsePathID(c, "id")
	if err != nil {
		apiErr := NewError(ErrKeyInvalidUUIDFormat, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	req := dto.PlatformDomainUpdateRequest{}
	if _, ok := httputil.DecodeAndValidateRequest(ctx, &req); !ok {
		return nil
	}

	if req.Enabled == nil {
		apiErr := NewError(ErrKeyValidationFailed, fmt.Errorf("enabled is required"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	pd, err := e.delegatedDomainSvc.UpdatePlatformDomain(reqCtx, id, *req.Enabled)
	if err != nil {
		e.logger.Error("Failed to update platform domain", zap.Error(err), zap.Uint("id", id))
		apiErr := NewError(ErrKeyValidationFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	resp := dto.PlatformDomainResponse{}
	resp.FromModel(pd)
	return httputil.EncodeResponse(ctx, pd, &resp)
}

func (e *AdminExtension) deletePlatformDomain(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	if e.delegatedDomainSvc == nil {
		apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("domain service not available"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	id, err := e.parsePathID(c, "id")
	if err != nil {
		apiErr := NewError(ErrKeyInvalidUUIDFormat, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	if err := e.delegatedDomainSvc.DeletePlatformDomain(reqCtx, id); err != nil {
		e.logger.Error("Failed to delete platform domain", zap.Error(err), zap.Uint("id", id))
		apiErr := NewError(ErrKeyValidationFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	return ctx.NoContent(http.StatusNoContent)
}
