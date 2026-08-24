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

	if err := e.registerWebsiteHandlers(ipfsRouter, accessSvc); err != nil {
		return err
	}

	if err := e.registerIPNSHandlers(ipfsRouter, accessSvc); err != nil {
		return err
	}

	if err := e.registerPlatformDomainHandlers(ipfsRouter, accessSvc); err != nil {
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
func (e *AdminExtension) registerPlatformDomainHandlers(gRouter router.Router, accessSvc core.AccessService) error {
	routes := router.DefineRoutes(
		router.NewRoute(http.MethodPost, "/platform-domains", e.createPlatformDomain,
			router.WithAccess(core.ACCESS_ADMIN_ROLE),
			router.WithSwagger(
				router.WithSummary("Register platform domain"),
				router.WithDescription(`Registers a platform-owned root domain that users can claim free subdomains under.

Operator-only operation. The zone referenced by zone_id must already be provisioned and owned by the operator.

See also: GET /platform-domains (list), PATCH /platform-domains/:id (enable/disable), DELETE /platform-domains/:id`),
				router.WithTags("Admin", "PlatformDomains"),
				router.WithRequestBody(dto.PlatformDomainRequest{}, "Platform domain to register", true),
				router.WithSuccessResponse(http.StatusCreated, "Platform domain registered", router.WithJSONContent(dto.PlatformDomainResponse{})),
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
				router.WithSuccessResponse(http.StatusOK, "Platform domain deleted", router.WithJSONContent(nil)),
			),
		),
	)

	apiGroup := internal.ProtocolName
	return router.RegisterRoutes(gRouter, accessSvc, apiGroup, routes)
}

func (e *AdminExtension) createPlatformDomain(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	if e.delegatedDomainSvc == nil {
		apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("domain service not available"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	req := dto.PlatformDomainRequest{}
	if _, ok := httputil.DecodeAndValidateRequest(ctx, &req); !ok {
		return nil
	}

	pd, err := e.delegatedDomainSvc.CreatePlatformDomain(reqCtx, req.Domain, pluginDb.DomainNamespace(req.Namespace), req.ZoneID, req.Enabled)
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

	pd, err := e.delegatedDomainSvc.UpdatePlatformDomain(reqCtx, id, req.Enabled)
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

	return ctx.NoContent(http.StatusOK)
}
