package api

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
	"go.lumeweb.com/httputil"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal/config"
	"go.lumeweb.com/portal-router"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// AdminExtension extends the Admin API with IPFS website management functionality
type AdminExtension struct {
	ctx             core.Context
	logger          *core.Logger
	config          config.Manager
	db              *gorm.DB
	websiteService  pluginCore.WebsiteService
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
	// Create a subrouter for IPFS website management
	ipfsRouter, err := gRouter.Group("/api/ipfs")
	if err != nil {
		return err
	}

	// Register admin website routes
	if err := e.registerWebsiteHandlers(ipfsRouter, accessSvc); err != nil {
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
				router.WithDescription("Blocks a website by setting its status to 'blocked'. Blocked websites cannot be deleted by users (admin operation)."),
				router.WithTags("admin", "websites", "ipfs"),
				router.WithPathParam("id", "Website ID", ""),
				router.WithSuccessResponse(http.StatusOK, "Website blocked", router.WithJSONContent(dto.WebsiteResponse{})),
			),
		),
		router.NewRoute(http.MethodPost, "/websites/:id/unblock", e.unblockWebsite,
			router.WithAccess(core.ACCESS_USER_ROLE),
			router.WithSwagger(
				router.WithSummary("Unblock website"),
				router.WithDescription("Unblocks a website by setting its status back to 'active' (admin operation)."),
				router.WithTags("admin", "websites", "ipfs"),
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

// blockWebsite blocks a website by setting its status to 'blocked'
func (e *AdminExtension) blockWebsite(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	websiteID, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		apiErr := NewError(ErrKeyInvalidUUIDFormat, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	if err := e.websiteService.BlockWebsite(reqCtx, uint(websiteID)); err != nil {
		e.logger.Error("Failed to block website", zap.Error(err), zap.Uint("website_id", uint(websiteID)))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	return ctx.NoContent(http.StatusOK)
}

// unblockWebsite unblocks a website by setting its status back to 'active'
func (e *AdminExtension) unblockWebsite(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	websiteID, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		apiErr := NewError(ErrKeyInvalidUUIDFormat, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	if err := e.websiteService.UnblockWebsite(reqCtx, uint(websiteID)); err != nil {
		e.logger.Error("Failed to unblock website", zap.Error(err), zap.Uint("website_id", uint(websiteID)))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	return ctx.NoContent(http.StatusOK)
}
