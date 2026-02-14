package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/ipfs/go-cid"
	"github.com/labstack/echo/v4"
	"go.lumeweb.com/httputil"
	mcontext "go.lumeweb.com/portal-middleware/context"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/queryutil"
	queryUtilHttp "go.lumeweb.com/queryutil/http"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func (a *API) listFiles(c echo.Context) error {
	return a.handleFileManagerRequest(c, "list files", func(ctx httputil.RequestContext, reqCtx context.Context, user uint) (queryutil.EntityFunc[*pluginDb.FilePath], error) {
		return func(filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination) ([]*pluginDb.FilePath, int64, error) {
			// Apply special handling for parent_path filter in the API layer
			// This ensures hierarchical filtering works end-to-end
			for i, filter := range filters {
				if filter.GetField() == "parent_path" {
					if val, ok := filter.GetValue().(string); ok {
						// Validate and normalize the parent path
						validPath, err := dto.ValidateFileManagerPath(val)
						if err != nil {
							return nil, 0, fmt.Errorf("invalid parent_path: %w", err)
						}
						// Update the filter with the validated path
						filters[i] = queryutil.NewLogicalFilter("parent_path", filter.GetOperator(), validPath)
					}
				}
			}

			return a.fileManagerService.ListFiles(reqCtx, user, filters, sorts, pagination)
		}, nil
	})
}

func (a *API) listDirectoryContents(c echo.Context) error {
	return a.handleFileManagerRequest(c, "list directory contents", func(ctx httputil.RequestContext, reqCtx context.Context, user uint) (queryutil.EntityFunc[*pluginDb.FilePath], error) {
		return func(filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination) ([]*pluginDb.FilePath, int64, error) {
			// Extract parent_path from filters using queryutil helper
			parentPathFilter := queryutil.FindFilter(filters, "parent_path")
			if parentPathFilter == nil {
				return nil, 0, fmt.Errorf("parent_path is required")
			}

			parentPath, ok := parentPathFilter.GetValue().(string)
			if !ok {
				return nil, 0, fmt.Errorf("parent_path must be a string")
			}

			// Normalize empty parent_path to root path
			if parentPath == "" {
				parentPath = pluginDb.RootPath
			}

			// Validate the normalized/received path
			if v, err := dto.ValidateFileManagerPath(parentPath); err != nil {
				return nil, 0, err
			} else {
				parentPath = v
			}

			paths, err := a.fileManagerService.ListDirectoryContents(reqCtx, user, parentPath)
			if err != nil {
				return nil, 0, err
			}
			return paths, int64(len(paths)), nil
		}, nil
	})
}

func (a *API) getBreadcrumbs(c echo.Context) error {
	ctx := httputil.Context(c)

	// Extract and validate path before calling handleFileManagerRequest
	// This ensures we return a 400 status for invalid paths rather than 500
	path, err := extractAndValidatePath(c)
	if err != nil {
		return ctx.Error(err, http.StatusBadRequest)
	}

	return a.handleFileManagerRequest(c, "get breadcrumbs", func(ctx httputil.RequestContext, reqCtx context.Context, user uint) (queryutil.EntityFunc[*pluginDb.FilePath], error) {
		return func(filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination) ([]*pluginDb.FilePath, int64, error) {
			breadcrumbs, err := a.fileManagerService.GetBreadcrumbs(reqCtx, user, path)
			if err != nil {
				return nil, 0, err
			}
			return breadcrumbs, int64(len(breadcrumbs)), nil
		}, nil
	})
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
	action string,
	serviceFunc func(httputil.RequestContext, context.Context, uint) (queryutil.EntityFunc[*pluginDb.FilePath], error),
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

	return queryUtilHttp.ProcessListRequest[*pluginDb.FilePath, dto.FileManagerItem](
		c.Response(),
		c.Request(),
		"files",
		svc,
		func(path *pluginDb.FilePath) dto.FileManagerItem {
			return a.convertFilePathToManagerItem(path, user)
		},
	)
}

func extractAndValidatePath(c echo.Context) (string, error) {
	// Parse query parameters to get filters
	parser := queryutil.NewHTTPRequestParser(c.Request(), nil, nil)
	filters, _, _, err := queryutil.ParseFromSource(parser)
	if err != nil {
		return "", fmt.Errorf("failed to parse query parameters: %w", err)
	}

	// Extract path from filters using queryutil helper
	pathFilter := queryutil.FindFilter(filters, "path")
	if pathFilter == nil {
		return "", fmt.Errorf("path is required")
	}

	path, ok := pathFilter.GetValue().(string)
	if !ok {
		return "", fmt.Errorf("path must be a string")
	}

	// Validate path using our reusable helper
	validPath, err := dto.ValidateFileManagerPath(path)
	if err != nil {
		return "", err
	}

	return validPath, nil
}
