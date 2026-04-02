package api

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/ipfs/go-cid"
	"github.com/labstack/echo/v4"
	"go.lumeweb.com/httputil"
	"go.lumeweb.com/portal/core"
	pc "go.lumeweb.com/portal-plugin-ipfs/internal/protocol/context"
	quotaCore "go.lumeweb.com/portal-plugin-quota/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	"go.lumeweb.com/portal-plugin-ipfs/internal/quota"
	"go.uber.org/zap"
)

func (a *API) handleIPFSGet(c echo.Context) error {
	ctx := httputil.Context(c)

	req := dto.IPFSRequest{}
	model, ok := httputil.DecodeAndValidateRequest(ctx, &req)

	if !ok {
		return nil
	}
	switch model.Format {
	case "raw":
		if err := a.handleRawBlockRequest(ctx, model.CID, c.Response(), c.Request(), c); err != nil {
			// Error response already written inside handleRawBlockRequest.
			return nil
		}
	case "car":
		// TODO: Implement CAR handling
		ctx.Response().Header().Set("Content-Type", "application/vnd.ipld.car")
		ctx.Response().WriteHeader(http.StatusNotImplemented)
	default:
		return ctx.Error(errors.New("Unsupported format"), http.StatusBadRequest)
	}

	return nil
}

func (a API) handleRawBlockRequest(ctx httputil.RequestContext, _cid cid.Cid, w http.ResponseWriter, r *http.Request, c echo.Context) error {
	// Create context with client IP for quota tracking
	reqCtx := ctx.Request().Context()
	reqCtx = pc.ClientIPOption(reqCtx, c.RealIP())
	// Skip quota check in store since API already validates it
	reqCtx = pc.SkipQuotaCheckOption(reqCtx, true)

	// Check if the block exists before trying to fetch it
	exists, err := a.ipfs.GetNode().HasBlock(reqCtx, _cid)
	if err != nil {
		a.Logger().Error("Failed to check if block exists", zap.Error(err))
		apiErr := NewError(ErrKeyMetadataFetchFailed, err)
		_ = ctx.Error(apiErr, apiErr.HttpStatus())
		return apiErr
	}

	if !exists {
		apiErr := NewError(ErrKeyBlockNotFound, fmt.Errorf("Block not found: %s", _cid.String()))
		_ = ctx.Error(apiErr, apiErr.HttpStatus())
		return apiErr
	}

	upload, err := a.coreUploadService.GetUpload(reqCtx, internal.NewIPFSHash(_cid))
	if err != nil {
		a.Logger().Error("Failed to get upload", zap.Error(err))
		apiErr := NewError(ErrKeyUploadNotFound, err)
		_ = ctx.Error(apiErr, apiErr.HttpStatus())
		return apiErr
	}

	if upload == nil {
		apiErr := NewError(ErrKeyUploadNotFound, fmt.Errorf("upload not found for cid: %s", _cid.String()))
		_ = ctx.Error(apiErr, apiErr.HttpStatus())
		return apiErr
	}

	// Check download quota if quota service is available
	userID := upload.UserID

	// Use reservation system for HTTP handlers
	checkResult, err := quota.CheckDownloadQuota(reqCtx, a.Context(), userID, upload.Size, quotaCore.WithCreateReservation(c.RealIP()))
	if err != nil {
		a.Logger().Warn("Download quota check failed",
			zap.Uint("user_id", userID),
			zap.Uint64("upload_size", upload.Size),
			zap.Stringer("cid", _cid),
			zap.Error(err))
		apiErr := NewError(ErrKeyDownloadQuotaExceeded, err)
		_ = ctx.Error(apiErr, http.StatusTooManyRequests)
		return apiErr
	}

	// Check if quota is not allowed
	if checkResult != nil && !checkResult.Allowed {
		a.Logger().Debug("Download quota exceeded",
			zap.Uint("user_id", userID),
			zap.Uint64("upload_size", upload.Size),
			zap.Uint64("current_usage", checkResult.Details.CurrentUsage),
			zap.Any("limit", checkResult.Details.Limit))
		
		// Release reservation if one was created
		_ = checkResult.ReleaseReservation(core.DetachContext(reqCtx))
		
		apiErr := NewError(ErrKeyDownloadQuotaExceeded, core.ErrDownloadQuotaExceeded)
		_ = ctx.Error(apiErr, http.StatusTooManyRequests)
		return apiErr
	}

	// Extract reservation ID for use throughout the function
	var reservationID *uint
	if checkResult != nil {
		reservationID = checkResult.ReservationID
	}

	// Only fetch block data after quota validation passes
	block, err := a.ipfs.GetNode().GetBlock(reqCtx, _cid)
	if err != nil {
		a.Logger().Error("Failed to get block", zap.Error(err))
		
		// Release reservation if one was created
		if checkResult != nil {
			_ = checkResult.ReleaseReservation(core.DetachContext(reqCtx))
		}
		
		apiErr := NewError(ErrKeyMetadataFetchFailed, err)
		_ = ctx.Error(apiErr, apiErr.HttpStatus())
		return apiErr
	}

	// Get client IP for quota tracking
	ip := c.RealIP()

	a.setTrustlessHeaders(w, r, _cid.String())
	n, err := w.Write(block.RawData())
	if err != nil {
		// Release reservation if write failed
		if checkResult != nil {
			_ = checkResult.ReleaseReservation(core.DetachContext(reqCtx))
		}
		// Emit completion event even on failure for audit trail
		quota.EmitDownloadCompleted(core.DetachContext(reqCtx), a.Context(), upload.ID, uint64(n), ip, &userID, reservationID, false)
		return err
	}

	// Emit download completion event only after successful write
	// Use DetachContext to prevent canceled request context from reaching event handlers
	quota.EmitDownloadCompleted(core.DetachContext(reqCtx), a.Context(), upload.ID, uint64(n), ip, &userID, reservationID, true)
	return nil
}

func (a API) setTrustlessHeaders(w http.ResponseWriter, r *http.Request, id string) {
	w.Header().Set("Content-Type", a.getTrustlessContentType(r))
	w.Header().Set("Cache-Control", "public, max-age=29030400, immutable")
	w.Header().Set("Etag", fmt.Sprintf("\"%s\"", id))
	w.Header().Set("X-Content-Type-Options", "nosniff")
}

func (a API) getTrustlessContentType(r *http.Request) string {
	format := r.URL.Query().Get("format")
	switch format {
	case "raw":
		return "application/vnd.ipld.raw"
	case "car":
		return "application/vnd.ipld.car"
	case "ipns-record":
		return "application/vnd.ipfs.ipns-record"
	default:
		return "application/octet-stream"
	}
}

// handleIPFSOptions is a dummy handler for OPTIONS requests to IPFS content routes.
// It's expected that CORS middleware will handle the response before this handler is reached.
func (a *API) handleIPFSOptions(c echo.Context) error {
	// This handler should ideally not be reached for CORS preflight requests
	// because the CORS middleware should handle them and write the response.
	// If it is reached, it means the CORS middleware didn't handle the request,
	// or it's a non-preflight OPTIONS request.
	// Returning 204 No Content is standard for OPTIONS if not handled by CORS.
	return c.NoContent(http.StatusNoContent)
}
