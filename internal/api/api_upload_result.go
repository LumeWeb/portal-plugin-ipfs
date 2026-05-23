package api

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
	"go.lumeweb.com/httputil"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	mcontext "go.lumeweb.com/portal-middleware/context"
	"go.lumeweb.com/portal/db/models"
	"go.lumeweb.com/portal/core"
)

func (a *API) handleUploadResult(c echo.Context) error {
	ctx := httputil.Context(c)
	identifier := c.Param("identifier")

	var requestID uint

	exists, tusReq := a.tusService.UploadExists(ctx.Request().Context(), a.ipfs.(core.StorageProtocol), identifier)
	if exists && tusReq != nil {
		requestID = tusReq.RequestID
	} else {
		parsedID, err := strconv.ParseUint(identifier, 10, 64)
		if err != nil {
			apiErr := NewError(ErrKeyUploadNotFound, nil)
			_ = ctx.Error(apiErr, apiErr.HttpStatus())
			return nil
		}
		requestID = uint(parsedID)
	}

	request, err := a.requestService.GetRequest(ctx.Request().Context(), requestID)
	if err != nil {
		apiErr := NewError(ErrKeyUploadNotFound, err)
		_ = ctx.Error(apiErr, apiErr.HttpStatus())
		return nil
	}

	userID, err := mcontext.GetUserID(ctx.Context)
	if err != nil {
		apiErr := NewError(ErrKeyUploadNotFound, nil)
		_ = ctx.Error(apiErr, apiErr.HttpStatus())
		return nil
	}
	if request.UserID == nil || *request.UserID != userID {
		apiErr := NewError(ErrKeyUploadNotFound, nil)
		_ = ctx.Error(apiErr, apiErr.HttpStatus())
		return nil
	}

	switch request.Status {
	case models.RequestStatusPending, models.RequestStatusProcessing:
		return c.JSON(http.StatusAccepted, &dto.UploadResultResponse{Status: models.RequestStatusProcessing})
	case models.RequestStatusCompleted:
		_cid, cidErr := internal.CIDFromHash(request.Hash, request.CIDType)
		if cidErr != nil {
			apiErr := NewError(ErrKeyFileProcessingFailed, cidErr)
			_ = ctx.Error(apiErr, apiErr.HttpStatus())
			return nil
		}
		return httputil.EncodeResponse(ctx, &dto.UploadResultResponse{}, &dto.UploadResultResponse{
			CID:    _cid.String(),
			Status: models.RequestStatusCompleted,
		})
	case models.RequestStatusDuplicate:
		_cid, cidErr := internal.CIDFromHash(request.Hash, request.CIDType)
		if cidErr != nil {
			apiErr := NewError(ErrKeyFileProcessingFailed, cidErr)
			_ = ctx.Error(apiErr, apiErr.HttpStatus())
			return nil
		}
		return httputil.EncodeResponse(ctx, &dto.UploadResultResponse{}, &dto.UploadResultResponse{
			CID:    _cid.String(),
			Status: models.RequestStatusCompleted,
		})
	case models.RequestStatusFailed:
		return c.JSON(http.StatusInternalServerError, &dto.UploadResultResponse{
			Status: models.RequestStatusFailed,
			Error:  request.StatusMessage,
		})
	default:
		apiErr := NewError(ErrKeyUploadNotFound, nil)
		_ = ctx.Error(apiErr, apiErr.HttpStatus())
		return nil
	}
}
