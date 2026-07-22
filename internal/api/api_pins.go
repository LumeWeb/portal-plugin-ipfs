package api

import (
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"github.com/labstack/echo/v4"
	"go.lumeweb.com/httputil"
	mcontext "go.lumeweb.com/portal-middleware/context"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal/core"
	pc "go.lumeweb.com/portal-plugin-ipfs/internal/protocol/context"
	"go.lumeweb.com/portal/db/types"
	"go.lumeweb.com/queryutil"
	queryUtilHttp "go.lumeweb.com/queryutil/http"
	"go.uber.org/zap"
)

// startPinWorkflow initiates the pin workflow for a given pin request
func (a *API) startPinWorkflow(ctx httputil.RequestContext, c echo.Context, user uint, pin *pluginDb.IPFSPin) error {
	_, err := a.workflowService.StartWorkflow(
		ctx.Request().Context(),
		protocol.PIN_WORKFLOW,
		core.WithWorkflowStructData(protocol.PinWorkflowData{
			PinRequestID: pin.RequestID.ToUUID(),
		}, "json"),
		core.WithWorkflowSourceIP(c.RealIP()),
		core.WithWorkflowUserID(user),
		core.WithWorkflowProtocol(internal.ProtocolName),
		core.WithWorkflowStorageHash(internal.NewIPFSHash(cid.MustParse(pin.CID))),
	)
	return err
}

func (a *API) listPins(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()
	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}

	filter := dto.IPFSPinFilter{}
	if _, ok := httputil.DecodeAndValidateQueryRequest(ctx, &filter); !ok {
		return nil
	}

	// Post-process statuses
	if err := filter.PostProcessStatuses(); err != nil {
		// Use default error handler for validation errors
		errorHandler := &httputil.DefaultErrorHandler{}
		errorHandler.HandleError(ctx, err)
		return nil
	}

	reqParser := pluginCore.NewIPFSPinParser(reqCtx, filter)

	filters, sort, pagination, err := queryutil.ParseFromCustomSource(reqParser)
	if err != nil {
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	
	// Use ForUser variant to ensure user isolation
	pins, total, err := a.pinService.ListPinsForUser(reqCtx, user, filters, sort, pagination)
	if err != nil {
		a.Logger().Error("Failed to list pins", zap.Error(err))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	var dtoResp dto.PinResultsResponse

	dtoResp.Count = uint64(total)

	queryUtilHttp.SetContentRangeHeader(c.Response(), "pins", pagination, pins, total)

	return httputil.EncodeResponse(ctx, pins, &dtoResp)
}

func (a *API) addPin(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()
	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}

	var req dto.PinRequest
	model, ok := httputil.DecodeAndValidateRequest(ctx, &req)

	if !ok {
		return nil
	}

	// Set the user_id on the model to ensure user isolation
	model.UserID = user

	_pin, err := a.pinService.AddPin(reqCtx, model)
	if err != nil {
		a.Logger().Error("Failed to add pin", zap.Error(err))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	if err := a.startPinWorkflow(ctx, c, user, _pin); err != nil {
		a.Logger().Error("Failed to start pin workflow",
			zap.String("request_id", _pin.RequestID.String()),
			zap.Error(err))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	ctx.Response().Before(func() {
		ctx.Response().Status = http.StatusAccepted
	})

	return httputil.EncodeResponse(ctx, _pin, &dto.PinStatusResponse{})
}

func (a *API) getPin(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()
	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}

	_uuid, err := uuid.Parse(c.Param("requestid"))
	if err != nil {
		apiErr := NewError(ErrKeyInvalidUUIDFormat, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	requestID := types.FromUUID(_uuid)

	// Use ForUser variant to ensure user isolation
	_pin, err := a.pinService.GetPinByRequestIDForUser(reqCtx, user, requestID)
	if err != nil {
		a.Logger().Error("Failed to get pin", zap.Error(err))
		apiErr := NewError(ErrKeyPinFetchFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	if _pin == nil {
		apiErr := NewError(ErrKeyPinFetchFailed, fmt.Errorf("pin not found"))
		return ctx.Error(apiErr, http.StatusNotFound)
	}

	return httputil.EncodeResponse(ctx, _pin, &dto.PinStatusResponse{})
}

func (a *API) replacePin(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	_uuid, err := uuid.Parse(c.Param("requestid"))
	if err != nil {
		apiErr := NewError(ErrKeyInvalidUUIDFormat, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}

	requestID := types.FromUUID(_uuid)

	var req dto.PinRequest
	model, ok := httputil.DecodeAndValidateRequest(ctx, &req)

	if !ok {
		return nil
	}

	// Set the user_id on the model to ensure user isolation
	model.UserID = user

	// Use ForUser variant to ensure user isolation
	_pin, err := a.pinService.ReplacePinForUser(reqCtx, user, c.RealIP(), requestID, model)
	if err != nil {
		a.Logger().Error("Failed to replace pin", zap.Error(err))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	if err := a.startPinWorkflow(ctx, c, user, _pin); err != nil {
		a.Logger().Error("Failed to start pin workflow",
			zap.String("request_id", _pin.RequestID.String()),
			zap.Error(err))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	ctx.Response().Before(func() {
		ctx.Response().Status = http.StatusAccepted
	})

	return httputil.EncodeResponse(ctx, _pin, &dto.PinStatusResponse{})
}

func (a *API) deletePin(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()
	reqCtx = pc.ClientIPOption(reqCtx, c.RealIP())
	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}

	_uuid, err := uuid.Parse(c.Param("requestid"))
	if err != nil {
		apiErr := NewError(ErrKeyInvalidUUIDFormat, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	requestID := types.FromUUID(_uuid)

	// Use ForUser variant to ensure user isolation
	if err := a.pinService.DeletePinForUser(reqCtx, user, requestID); err != nil {
		a.Logger().Error("Failed to delete pin", zap.Error(err))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	return ctx.NoContent(http.StatusAccepted)
}

// handlePinOptions is a fallback handler for OPTIONS requests to pin routes.
// CORS middleware should handle preflight responses before this handler is reached.
// If reached, it serves as a safety net returning 204 No Content.
func (a *API) handlePinOptions(c echo.Context) error {
	return c.NoContent(http.StatusNoContent)
}
