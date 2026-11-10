package api

import (
	"context"
	"errors"
	"fmt"

	"github.com/ipfs/go-cid"
	"github.com/labstack/echo/v4"
	"go.lumeweb.com/httputil"
	mcontext "go.lumeweb.com/portal-middleware/context"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/queryutil"
	queryHTTP "go.lumeweb.com/queryutil/http"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func (a *API) listFiles(c echo.Context) error {
	return a.handleFileManagerRequest(c, dto.FileListPolicy, "list files", func(ctx httputil.RequestContext, reqCtx context.Context, user uint) (queryutil.EntityFunc[*pluginDb.FilePath], error) {
		return func(filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination) ([]*pluginDb.FilePath, int64, error) {
			return a.fileManagerService.ListFiles(reqCtx, user, filters, sorts, pagination)
		}, nil
	}, queryutil.WithStringFields("name", "path", "parent_path", "cid"))
}

func (a *API) listDirectoryContents(c echo.Context) error {
	return a.handleFileManagerRequest(c, dto.DirectoryListPolicy, "list directory contents", func(ctx httputil.RequestContext, reqCtx context.Context, user uint) (queryutil.EntityFunc[*pluginDb.FilePath], error) {
		return func(filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination) ([]*pluginDb.FilePath, int64, error) {
			if len(filters) != 1 || filters[0].GetField() != "parent_path" {
				return nil, 0, fmt.Errorf("%w: one parent_path equality is required", queryutil.ErrInvalidQuery)
			}
			parentPath := filters[0].GetValue().(string)

			paths, err := a.fileManagerService.ListDirectoryContents(reqCtx, user, parentPath)
			if err != nil {
				return nil, 0, err
			}
			return paths, int64(len(paths)), nil
		}, nil
	}, queryutil.WithStringFields("parent_path"))
}

func (a *API) getBreadcrumbs(c echo.Context) error {
	return a.handleFileManagerRequest(c, dto.BreadcrumbListPolicy, "get breadcrumbs", func(ctx httputil.RequestContext, reqCtx context.Context, user uint) (queryutil.EntityFunc[*pluginDb.FilePath], error) {
		return func(filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination) ([]*pluginDb.FilePath, int64, error) {
			if len(filters) != 1 || filters[0].GetField() != "path" {
				return nil, 0, fmt.Errorf("%w: one path equality is required", queryutil.ErrInvalidQuery)
			}
			path := filters[0].GetValue().(string)
			breadcrumbs, err := a.fileManagerService.GetBreadcrumbs(reqCtx, user, path)
			if err != nil {
				return nil, 0, err
			}
			return breadcrumbs, int64(len(breadcrumbs)), nil
		}, nil
	}, queryutil.WithStringFields("path"))
}

func (a *API) convertFilePathToManagerItem(path *pluginDb.FilePath, userID uint) dto.FileManagerItem {
	c, err := cid.Cast(path.CID)
	if err != nil {
		a.Logger().Error("Failed to cast CID for file path", zap.Error(err), zap.String("path", path.Path))
		return dto.FileManagerItem{
			Path:        path.Path,
			Name:        path.Name,
			Type:        path.Type,
			Size:        uint64(path.Size),
			IsDirectory: path.IsDirectory,
			Depth:       path.Depth,
			Created:     path.CreatedAt,
			Updated:     path.UpdatedAt,
			CID:         "",
			Unpinnable:  true, // Default to unpinnable if CID is invalid
		}
	}

	// Check if this file has an associated IPFS pin
	// If there's no IPFS pin, it's unpinnable (true) - can't be unpinned via pinner API
	// If there is an IPFS pin, it's pinnable (false) - can be unpinned via pinner API
	unpinnable := true // Default to unpinnable (safer)
	// TODO: Pass request context instead of context.Background() to support proper cancellation
	pin, err := a.pinService.GetPinByCIDAndUser(context.Background(), c, userID)
	if err != nil && err != gorm.ErrRecordNotFound {
		a.Logger().Error("Failed to check IPFS pin status", zap.Error(err), zap.Stringer("cid", c), zap.Uint("user_id", userID))
	} else if pin != nil {
		// There's an IPFS pin, so it can be unpinned via pinner API
		unpinnable = false
	}

	return dto.FileManagerItem{
		Path:        path.Path,
		Name:        path.Name,
		Type:        path.Type,
		Size:        uint64(path.Size),
		IsDirectory: path.IsDirectory,
		Depth:       path.Depth,
		Created:     path.CreatedAt,
		Updated:     path.UpdatedAt,
		CID:         c.String(),
		Unpinnable:  unpinnable,
	}
}

func (a *API) handleFileManagerRequest(
	c echo.Context,
	policy queryutil.QueryPolicy,
	action string,
	serviceFunc func(httputil.RequestContext, context.Context, uint) (queryutil.EntityFunc[*pluginDb.FilePath], error),
	parserOptions ...queryutil.ParserOption,
) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	user, err := mcontext.GetUserID(c)
	if err != nil {
		apiErr := NewError(ErrKeyUnauthorized, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	svc, err := serviceFunc(ctx, reqCtx, user)
	if err != nil {
		// If error was already handled by the serviceFunc, return nil
		if errors.Is(err, context.Canceled) {
			return nil
		}
		a.Logger().Error(fmt.Sprintf("Failed to prepare %s request", action), zap.Error(err))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	return queryHTTP.ProcessListRequestWithPolicy[*pluginDb.FilePath, dto.FileManagerItem](
		c.Response(),
		c.Request(),
		"files",
		policy,
		svc,
		func(path *pluginDb.FilePath) dto.FileManagerItem {
			return a.convertFilePathToManagerItem(path, user)
		},
		parserOptions...,
	)
}
