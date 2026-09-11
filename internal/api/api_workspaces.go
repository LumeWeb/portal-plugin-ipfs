package api

import (
	"errors"
	"strconv"

	"github.com/labstack/echo/v4"
	"go.lumeweb.com/httputil"
	mcontext "go.lumeweb.com/portal-middleware/context"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	workspacesvc "go.lumeweb.com/portal-plugin-ipfs/internal/service/workspace"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/queryutil"
	queryutilHttp "go.lumeweb.com/queryutil/http"
	"go.uber.org/zap"
)

// workspacePathID parses the :id path parameter for workspace routes.
func (a *API) workspacePathID(c echo.Context) (uint, error) {
	id, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		apiErr := NewError(ErrKeyInvalidPathID, err)
		return 0, apiErr
	}
	return uint(id), nil
}

// workspaceError maps a workspace service error to the registered API error
// codes for workspace routes.
func workspaceError(err error) *core.Error {
	switch {
	case errors.Is(err, workspacesvc.ErrWorkspaceNotFound):
		return NewError(ErrKeyWorkspaceNotFound, err)
	case errors.Is(err, workspacesvc.ErrWorkspaceInvalidState):
		return NewError(ErrKeyWorkspaceInvalidState, err)
	case errors.Is(err, workspacesvc.ErrWorkspaceNotEnabled),
		errors.Is(err, workspacesvc.ErrWorkspacePlatformDomainUnavailable):
		return NewError(ErrKeyWorkspaceDisabled, err)
	case errors.Is(err, workspacesvc.ErrWorkspaceAlreadyExists),
		errors.Is(err, workspacesvc.ErrWorkspaceAlreadyAttached):
		return NewError(ErrKeyWorkspaceDuplicate, err)
	case errors.Is(err, workspacesvc.ErrWorkspaceProviderUnavailable):
		return NewError(ErrKeyWorkspaceProviderUnavailable, err)
	default:
		return NewError(ErrKeyWorkspaceOperationFailed, err)
	}
}

// createWorkspace creates a workspace owned by the authenticated user. The
// request may optionally attach it to a website the user owns (website_id);
// omitting it creates an unattached workspace. POST /workspaces
func (a *API) createWorkspace(c echo.Context) error {
	ctx := httputil.Context(c)
	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}

	var req dto.WorkspaceRequest
	if _, ok := httputil.DecodeAndValidateRequest[*dto.WorkspaceRequest](ctx, &req); !ok {
		return nil
	}

	ws, err := a.workspaceService.Create(ctx.Context.Request().Context(), user, req.WebsiteID)
	if err != nil {
		a.Logger().Error("Failed to create workspace", zap.Error(err), zap.Uint("user_id", user))
		apiErr := workspaceError(err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	return httputil.EncodeResponse(ctx, ws, &dto.WorkspaceResponse{})
}

// listWorkspaces returns the workspaces owned by the authenticated user
// (including unattached workspaces), filtered, sorted, and paginated.
// GET /workspaces
func (a *API) listWorkspaces(c echo.Context) error {
	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}

	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	listFunc := func(filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination) ([]*pluginDb.Workspace, int64, error) {
		return a.workspaceService.List(reqCtx, user, filters, sorts, pagination)
	}

	return queryutilHttp.ProcessListRequest[*pluginDb.Workspace, dto.WorkspaceResponse](
		c.Response(),
		c.Request(),
		"workspaces",
		listFunc,
		func(ws *pluginDb.Workspace) dto.WorkspaceResponse {
			var resp dto.WorkspaceResponse
			_ = resp.FromModel(ws)
			return resp
		},
	)
}

// resolveWorkspace lets a runtime container resolve its own workspace identity
// from the Coolify-injected COOLIFY_RESOURCE_UUID (the application/resource
// UUID) plus its workspace PORTAL_API_KEY. The API-key owner must match the
// workspace owner and the resource UUID must match the workspace's
// application_resource_id; a mismatch returns not-found (no existence leak).
// It returns workspace data plus the optional Website publish relationship,
// never exposing secrets. The UUID is read from the X-Coolify-Resource-UUID
// header or the resource_uuid query parameter.
// GET /workspaces/resolve
func (a *API) resolveWorkspace(c echo.Context) error {
	ctx := httputil.Context(c)
	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}

	resourceUUID := c.QueryParam("resource_uuid")
	if resourceUUID == "" {
		resourceUUID = c.Request().Header.Get("X-Coolify-Resource-UUID")
	}
	if resourceUUID == "" {
		apiErr := NewError(ErrKeyInvalidRequest, errors.New("missing COOLIFY_RESOURCE_UUID: supply resource_uuid query param or X-Coolify-Resource-UUID header"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	ws, website, err := a.workspaceService.ResolveRuntime(ctx.Context.Request().Context(), user, resourceUUID)
	if err != nil {
		a.Logger().Error("Failed to resolve workspace runtime",
			zap.Error(err), zap.Uint("user_id", user), zap.String("resource_uuid", resourceUUID))
		apiErr := workspaceError(err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	if ws == nil {
		apiErr := NewError(ErrKeyWorkspaceNotFound, errors.New("workspace: no workspace matches the resource UUID for this api key"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	var resp dto.WorkspaceResolveResponse
	if err := resp.FromResolve(ws, website); err != nil {
		return err
	}
	return ctx.Encode(&resp)
}

// getWorkspace returns a specific workspace owned by the authenticated user.
// GET /workspaces/:id
func (a *API) getWorkspace(c echo.Context) error {
	ctx := httputil.Context(c)
	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}
	workspaceID, err := a.workspacePathID(c)
	if err != nil {
		return ctx.Error(err, err.(*core.Error).HttpStatus())
	}

	ws, err := a.workspaceService.Get(ctx.Context.Request().Context(), user, workspaceID)
	if err != nil {
		a.Logger().Error("Failed to get workspace", zap.Error(err), zap.Uint("workspace_id", workspaceID), zap.Uint("user_id", user))
		apiErr := workspaceError(err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	if ws == nil {
		apiErr := NewError(ErrKeyWorkspaceNotFound, errors.New("workspace: workspace not found"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	return httputil.EncodeResponse(ctx, ws, &dto.WorkspaceResponse{})
}

// attachWorkspace links an existing unattached workspace (owned by the user)
// to a website the user owns. It verifies ownership of both and prevents
// duplicate attachment. POST /workspaces/:id/attach
func (a *API) attachWorkspace(c echo.Context) error {
	ctx := httputil.Context(c)
	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}
	workspaceID, err := a.workspacePathID(c)
	if err != nil {
		return ctx.Error(err, err.(*core.Error).HttpStatus())
	}

	var req dto.WorkspaceRequest
	if _, ok := httputil.DecodeAndValidateRequest[*dto.WorkspaceRequest](ctx, &req); !ok {
		return nil
	}
	if req.WebsiteID == nil {
		apiErr := NewError(ErrKeyInvalidRequest, errors.New("website_id is required to attach a workspace"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	ws, err := a.workspaceService.Attach(ctx.Context.Request().Context(), user, workspaceID, *req.WebsiteID)
	if err != nil {
		a.Logger().Error("Failed to attach workspace", zap.Error(err), zap.Uint("workspace_id", workspaceID), zap.Uint("user_id", user))
		apiErr := workspaceError(err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	return httputil.EncodeResponse(ctx, ws, &dto.WorkspaceResponse{})
}

// suspendWorkspace stops a ready workspace's application and marks it
// suspended. The shared database is never stopped per workspace.
// POST /workspaces/:id/suspend
func (a *API) suspendWorkspace(c echo.Context) error {
	ctx := httputil.Context(c)
	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}
	workspaceID, err := a.workspacePathID(c)
	if err != nil {
		return ctx.Error(err, err.(*core.Error).HttpStatus())
	}

	ws, err := a.workspaceService.Suspend(ctx.Context.Request().Context(), user, workspaceID)
	if err != nil {
		a.Logger().Error("Failed to suspend workspace", zap.Error(err), zap.Uint("workspace_id", workspaceID), zap.Uint("user_id", user))
		apiErr := workspaceError(err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	return httputil.EncodeResponse(ctx, ws, &dto.WorkspaceResponse{})
}

// resumeWorkspace starts a suspended workspace's application after
// re-provisioning its logical database/user on the shared resource, and waits
// for readiness. POST /workspaces/:id/resume
func (a *API) resumeWorkspace(c echo.Context) error {
	ctx := httputil.Context(c)
	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}
	workspaceID, err := a.workspacePathID(c)
	if err != nil {
		return ctx.Error(err, err.(*core.Error).HttpStatus())
	}

	ws, err := a.workspaceService.Resume(ctx.Context.Request().Context(), user, workspaceID)
	if err != nil {
		a.Logger().Error("Failed to resume workspace", zap.Error(err), zap.Uint("workspace_id", workspaceID), zap.Uint("user_id", user))
		apiErr := workspaceError(err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	return httputil.EncodeResponse(ctx, ws, &dto.WorkspaceResponse{})
}

// deleteWorkspace marks a workspace deleting, tears down provider resources,
// and soft-deletes the row. DELETE /workspaces/:id
func (a *API) deleteWorkspace(c echo.Context) error {
	ctx := httputil.Context(c)
	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}
	workspaceID, err := a.workspacePathID(c)
	if err != nil {
		return ctx.Error(err, err.(*core.Error).HttpStatus())
	}

	ws, err := a.workspaceService.Delete(ctx.Context.Request().Context(), user, workspaceID)
	if err != nil {
		a.Logger().Error("Failed to delete workspace", zap.Error(err), zap.Uint("workspace_id", workspaceID), zap.Uint("user_id", user))
		apiErr := workspaceError(err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	return httputil.EncodeResponse(ctx, ws, &dto.WorkspaceResponse{})
}

// getWorkspaceAccess returns the owner's proxy Basic Auth credentials. It
// accepts an optional ?rotate=true to rotate them. It only ever returns the
// proxy credentials, never the portal API key or database password.
// GET /workspaces/:id/access
func (a *API) getWorkspaceAccess(c echo.Context) error {
	ctx := httputil.Context(c)
	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}
	workspaceID, err := a.workspacePathID(c)
	if err != nil {
		return ctx.Error(err, err.(*core.Error).HttpStatus())
	}

	rotate := c.QueryParam("rotate") == "true"
	creds, err := a.workspaceService.RotateAccessCredentials(ctx.Context.Request().Context(), user, workspaceID, rotate)
	if err != nil {
		a.Logger().Error("Failed to get workspace access credentials", zap.Error(err), zap.Uint("workspace_id", workspaceID), zap.Uint("user_id", user))
		apiErr := workspaceError(err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	return httputil.EncodeResponse(ctx, creds, &dto.WorkspaceAccessResponse{})
}
