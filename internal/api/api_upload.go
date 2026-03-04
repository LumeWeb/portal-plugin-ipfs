package api

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"go.lumeweb.com/httputil"
	mcontext "go.lumeweb.com/portal-middleware/context"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginErrors "go.lumeweb.com/portal-plugin-ipfs/internal/errors"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	uploadpkg "go.lumeweb.com/portal-plugin-ipfs/internal/upload"
	"go.lumeweb.com/portal/core"
)

func (a *API) GetTusHandler() core.TusHandler {
	return a.tus
}

func (a *API) handleUpload(c echo.Context) error {
	ctx := httputil.Context(c)

	user, err := mcontext.GetUserID(ctx.Context)
	if err != nil {
		apiErr := core.NewAccountError(core.ErrKeyLoginFailed, nil)
		_ = ctx.Error(apiErr, apiErr.HttpStatus())
		return nil
	}

	upl, err := ctx.PrepareFileUpload(int64(a.Config().Config().Core.PostUploadLimit))
	if err != nil {
		_ = ctx.Error(err, http.StatusBadRequest)
		return nil
	}

	// Parse and validate upload request parameters using DTO
	var uploadReq dto.UploadRequest
	if _, ok := httputil.DecodeAndValidateRequest(ctx, &uploadReq); !ok {
		return nil
	}

	// Get validated zip mode from DTO
	archiveMode := uploadpkg.ParseArchiveMode(uploadReq.GetArchiveMode())

	// Use the new HandleUploadWithMode method
	_cid, uploadId, err := a.uploadService.HandleUploadWithMode(ctx.Request().Context(), upl.File, user, archiveMode)
	if err != nil {
		// Handle specific error types with appropriate HTTP status codes
		var apiErr *IPFSError

		// Check if it's an UploadError and extract the error type using the helper function
		if uploadErr, ok := uploadpkg.AsUploadError(err); ok {
			// Map upload error type to API error type using the helper function
			apiErr = NewError(pluginErrors.MapUploadErrorType(uploadErr.Type), uploadErr)
		} else {
			// Fallback to generic error for unexpected error types
			apiErr = NewError(ErrKeyFileUploadFailed, err)
		}

		_ = ctx.Error(apiErr, apiErr.HttpStatus())
		return nil
	}

	_, err = a.workflowService.StartWorkflow(ctx.Request().Context(), protocol.UPLOAD_WORKFLOW, core.WithWorkflowStructData(protocol.PostUploadWorkflowData{
		UploadID: uploadId,
	}, "json"),
		core.WithWorkflowSourceIP(c.RealIP()),
		core.WithWorkflowUserID(user),
		core.WithWorkflowProtocol(internal.ProtocolName),
		core.WithWorkflowStorageHash(internal.NewIPFSHash(_cid)))
	if err != nil {
		return ctx.Error(err, http.StatusInternalServerError)
	}

	return httputil.EncodeResponse(ctx, &dto.PostUploadResponse{}, &dto.PostUploadResponse{CID: _cid.String()})
}
