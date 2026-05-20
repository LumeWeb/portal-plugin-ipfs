package api

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/ipfs/boxo/path"
	"github.com/ipfs/go-cid"
	"github.com/labstack/echo/v4"
	"go.lumeweb.com/httputil"
	"go.lumeweb.com/portal/core"
	mcontext "go.lumeweb.com/portal-middleware/context"
	"go.lumeweb.com/ipfs-content/paths"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.uber.org/zap"
	"go.lumeweb.com/queryutil"
	queryutilHttp "go.lumeweb.com/queryutil/http"
	"gorm.io/gorm"
)

// KeyTypeEd25519 is the key type identifier for Ed25519 keys.
const KeyTypeEd25519 = 1

// IPNS Key Handlers

// createIPNSKey creates or imports an IPNS key for the authenticated user.
// If req.Key is provided, it imports an existing key; otherwise creates a new Ed25519 key.
// Returns 201 Created with IPNSKeyResponse on success.
func (a *API) createIPNSKey(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()
	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}

	var req dto.IPNSKeyRequest
	if _, ok := httputil.DecodeAndValidateRequest(ctx, &req); !ok {
		return nil
	}

	var key *pluginDb.IPFSIPNSKey
	if req.Key != "" {
		// Import existing key
		key, err = a.ipnsKeyService.ImportKey(reqCtx, user, req.Name, req.Key)
		if err != nil {
			a.Logger().Error("Failed to import IPNS key", zap.Error(err), zap.Uint("user_id", user), zap.String("name", req.Name))
			apiErr := NewError(ErrKeyFileProcessingFailed, err)
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}
	} else {
		// Create new key (default Ed25519)
		key, err = a.ipnsKeyService.CreateKey(reqCtx, user, req.Name, KeyTypeEd25519)
		if err != nil {
			a.Logger().Error("Failed to create IPNS key", zap.Error(err), zap.Uint("user_id", user), zap.String("name", req.Name))
			apiErr := NewError(ErrKeyFileProcessingFailed, err)
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}
	}

	var resp dto.IPNSKeyResponse
	if err := resp.FromModel(key); err != nil {
		a.Logger().Error("Failed to convert IPNS key to response", zap.Error(err))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	ctx.Response().Before(func() {
		ctx.Response().Status = http.StatusCreated
	})
	return httputil.EncodeResponse(ctx, key, &resp)
}

// listIPNSKeys retrieves all IPNS keys owned by the authenticated user.
// Returns 200 OK with array of IPNSKeyResponse on success.
func (a *API) listIPNSKeys(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()
	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}

	return queryutilHttp.ProcessListRequest[*pluginDb.IPFSIPNSKey, dto.IPNSKeyListResponse](
		c.Response(),
		c.Request(),
		"ipns-keys",
		func(filters []queryutil.CrudFilter, sorts []queryutil.Sort, pagination queryutil.Pagination) ([]*pluginDb.IPFSIPNSKey, int64, error) {
			return a.ipnsKeyService.ListKeysWithFilters(reqCtx, user, filters, sorts, pagination)
		},
		func(key *pluginDb.IPFSIPNSKey) dto.IPNSKeyListResponse {
			var resp dto.IPNSKeyListResponse
			_ = resp.FromModel(key)
			return resp
		},
	)
}

// getIPNSKey retrieves a specific IPNS key by ID for the authenticated user.
// Returns 200 OK with IPNSKeyResponse on success.
func (a *API) getIPNSKey(c echo.Context) error {
	ctx := httputil.Context(c)

	keyID, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		apiErr := NewError(ErrKeyInvalidUUIDFormat, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}

	reqCtx := ctx.Context.Request().Context()
	key, err := a.ipnsKeyService.GetKeyByID(reqCtx, user, uint(keyID))
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			apiErr := NewError(ErrKeyUploadNotFound, err)
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}
		a.Logger().Error("Failed to get IPNS key", zap.Error(err), zap.Uint("key_id", uint(keyID)), zap.Uint("user_id", user))
		apiErr := NewError(ErrKeyPinFetchFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

		var resp dto.IPNSKeyResponse
	if err := resp.FromModel(key); err != nil {
		a.Logger().Error("Failed to convert IPNS key to response", zap.Error(err))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}
	return httputil.EncodeResponse(ctx, key, &resp)
}

// deleteIPNSKey deletes a specific IPNS key by ID for the authenticated user.
// Returns 204 No Content on success.
func (a *API) deleteIPNSKey(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()
	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}

	keyID, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		apiErr := NewError(ErrKeyInvalidUUIDFormat, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	if err := a.ipnsKeyService.DeleteKey(reqCtx, user, uint(keyID)); err != nil {
		a.Logger().Error("Failed to delete IPNS key", zap.Error(err), zap.Uint("key_id", uint(keyID)), zap.Uint("user_id", user))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	return ctx.NoContent(http.StatusNoContent)
}

// IPNS Operation Handlers

// publishIPNS publishes a CID to IPNS using a specific key owned by the authenticated user.
// Accepts optional TTL parameter for record lifetime.
// Returns 200 OK with IPNSPublishResponse on success.
func (a *API) publishIPNS(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()
	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}

	var req dto.IPNSPublishRequest
	if _, ok := httputil.DecodeAndValidateRequest(ctx, &req); !ok {
		return nil
	}

	// Get IPNSKeyService
	ipnsKeyService := core.GetService[pluginCore.IPNSKeyService](a.Context(), pluginCore.IPNS_KEY_SERVICE)
	if ipnsKeyService == nil {
		a.Logger().Error("IPNSKeyService not available")
		apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("IPNSKeyService not available"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	// Get the IPNS key by ID and verify ownership
	key, err := a.ipnsKeyService.GetKeyByID(reqCtx, user, req.KeyID)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			apiErr := NewError(ErrKeyUploadNotFound, err)
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}
		a.Logger().Error("Failed to get IPNS key", zap.Error(err), zap.Uint("key_id", req.KeyID), zap.Uint("user_id", user))
		apiErr := NewError(ErrKeyPinFetchFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	// Parse TTL if provided
	var ttl time.Duration
	if req.TTL != "" {
		ttl, err = time.ParseDuration(req.TTL)
		if err != nil {
			a.Logger().Error("Failed to parse TTL", zap.Error(err), zap.String("ttl", req.TTL))
			apiErr := NewError(ErrKeyInvalidRequest, fmt.Errorf("invalid TTL format: %w", err))
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}
	}

	// Publish the CID to IPNS using the peer ID
	err = ipnsKeyService.PublishCID(reqCtx, key.PeerID().String(), req.CID, ttl)
	if err != nil {
		a.Logger().Error("Failed to publish IPNS record", zap.Error(err), zap.Uint("key_id", req.KeyID), zap.String("cid", req.CID))
		apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("failed to publish IPNS record: %w", err))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	// Get the published record to return details
	record, err := ipnsKeyService.GetPublished(reqCtx, key.PeerID().String(), false)
	if err != nil {
		a.Logger().Error("Failed to get published IPNS record", zap.Error(err), zap.String("peer_id", key.PeerID().String()))
		// Don't fail the request if we can't retrieve the record, just return a basic response
		resp := dto.IPNSPublishResponse{
			Name:      key.PeerID().String(),
			Value:     req.CID,
			Published: time.Now(),
		}
		return httputil.EncodeResponse(ctx, nil, &resp)
	}

	if record == nil {
		resp := dto.IPNSPublishResponse{
			Name:      key.PeerID().String(),
			Value:     req.CID,
			Published: time.Now(),
		}
		return httputil.EncodeResponse(ctx, nil, &resp)
	}

	// Convert IPNS record to response
	valuePath, err := record.Value()
	if err != nil {
		a.Logger().Error("Failed to get IPNS record value", zap.Error(err))
		// Parse CID from string to create a path
		targetCid, cidErr := cid.Decode(req.CID)
		if cidErr != nil {
			a.Logger().Error("Failed to parse CID for fallback value", zap.Error(cidErr))
			return ctx.Error(fmt.Errorf("failed to get IPNS record value and parse CID: %w", err), http.StatusInternalServerError)
		}
		valuePath = path.FromCid(targetCid)
	}

	sequence, err := record.Sequence()
	if err != nil {
		a.Logger().Error("Failed to get IPNS record sequence", zap.Error(err))
		sequence = 0
	}

	validity, err := record.Validity()
	if err != nil {
		a.Logger().Error("Failed to get IPNS record validity", zap.Error(err))
		validity = time.Now()
	}

	resp := dto.IPNSPublishResponse{
		Name:      key.PeerID().String(),
		Value:     valuePath.String(),
		Sequence:  sequence,
		Validity:  validity,
		Published: time.Now(),
	}

	return httputil.EncodeResponse(ctx, nil, &resp)
}

// resolveIPNS resolves an IPNS name to its current CID value.
// Accepts optional check_routing query parameter to verify routing.
// Returns 200 OK with IPNSResolveResponse on success.
func (a *API) resolveIPNS(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	name := c.Param("name")
	if name == "" {
		apiErr := NewError(ErrKeyInvalidRequest, fmt.Errorf("IPNS name is required"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	// Get IPNSKeyService
	ipnsKeyService := core.GetService[pluginCore.IPNSKeyService](a.Context(), pluginCore.IPNS_KEY_SERVICE)
	if ipnsKeyService == nil {
		a.Logger().Error("IPNSKeyService not available")
		apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("IPNSKeyService not available"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	// Parse optional checkRouting query parameter (1 = true, 0 = false)
	checkRoutingInt, err := strconv.Atoi(c.QueryParam("check_routing"))
	checkRouting := err == nil && checkRoutingInt > 0

	// Get the published record
	record, err := ipnsKeyService.GetPublished(reqCtx, name, checkRouting)
	if err != nil {
		a.Logger().Error("Failed to resolve IPNS name", zap.Error(err), zap.String("name", name))
		apiErr := NewError(ErrKeyPinFetchFailed, fmt.Errorf("failed to resolve IPNS name: %w", err))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	// Check if record is nil (key not yet published)
	if record == nil {
		a.Logger().Debug("IPNS record not found", zap.String("name", name))
		apiErr := NewError(ErrKeyPinFetchFailed, fmt.Errorf("IPNS name not found: %s", name))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	// Convert IPNS record to response
	valuePath, err := record.Value()
	if err != nil {
		a.Logger().Error("Failed to get IPNS record value", zap.Error(err))
		apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("failed to get IPNS record value: %w", err))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	// Normalize CID if the resolved value contains one
	normalizedValue := paths.TryNormalizeCIDFromPath(valuePath)

	sequence, err := record.Sequence()
	if err != nil {
		a.Logger().Error("Failed to get IPNS record sequence", zap.Error(err))
		sequence = 0
	}

	validity, err := record.Validity()
	if err != nil {
		a.Logger().Error("Failed to get IPNS record validity", zap.Error(err))
		apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("failed to get IPNS record validity: %w", err))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	resp := dto.IPNSResolveResponse{
		Name:     name,
		Value:    normalizedValue,
		Sequence: sequence,
		Path:     dto.IPFSPath(normalizedValue),
		Expired:  time.Now().After(validity),
		Expires:  validity,
	}

	return httputil.EncodeResponse(ctx, nil, &resp)
}

func (a *API) republishIPNS(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	user, err := mcontext.GetUserID(c)
	if err != nil {
		return err
	}

	keyID, err := strconv.ParseUint(c.Param("id"), 10, 64)
	if err != nil {
		apiErr := NewError(ErrKeyInvalidUUIDFormat, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	ipnsKeyService := core.GetService[pluginCore.IPNSKeyService](a.Context(), pluginCore.IPNS_KEY_SERVICE)
	if ipnsKeyService == nil {
		a.Logger().Error("IPNSKeyService not available")
		apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("IPNSKeyService not available"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	key, err := a.ipnsKeyService.GetKeyByID(reqCtx, user, uint(keyID))
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			apiErr := NewError(ErrKeyUploadNotFound, err)
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}
		a.Logger().Error("Failed to get IPNS key", zap.Error(err), zap.Uint("key_id", uint(keyID)), zap.Uint("user_id", user))
		apiErr := NewError(ErrKeyPinFetchFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	record, err := ipnsKeyService.GetPublished(reqCtx, key.PeerID().String(), false)
	if err != nil {
		a.Logger().Error("Failed to get published record for republish", zap.Error(err), zap.String("peer_id", key.PeerID().String()))
		apiErr := NewError(ErrKeyPinFetchFailed, fmt.Errorf("failed to get published record: %w", err))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	var cidStr string
	if record != nil {
		valuePath, err := record.Value()
		if err != nil {
			a.Logger().Error("Failed to get IPNS record value for republish", zap.Error(err), zap.String("peer_id", key.PeerID().String()))
			apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("failed to get IPNS record value: %w", err))
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}
		cidStr, err = paths.ExtractCIDFromPathStrict(valuePath)
		if err != nil {
			apiErr := NewError(ErrKeyInvalidRequest, err)
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}
	} else {
		if key.LastPublishedCID != "" {
			a.Logger().Debug("No local record found, using last published CID from database", zap.String("peer_id", key.PeerID().String()), zap.String("cid", key.LastPublishedCID))
			cidStr = key.LastPublishedCID
		} else {
			a.Logger().Debug("No local record found, checking DHT", zap.String("peer_id", key.PeerID().String()))
			record, err = ipnsKeyService.GetPublished(reqCtx, key.PeerID().String(), true)
			if err != nil {
				a.Logger().Error("Failed to get published record from DHT", zap.Error(err), zap.String("peer_id", key.PeerID().String()))
				apiErr := NewError(ErrKeyPinFetchFailed, fmt.Errorf("failed to get published record from routing: %w", err))
				return ctx.Error(apiErr, apiErr.HttpStatus())
			}
			if record == nil {
				apiErr := NewError(ErrKeyPinFetchFailed, fmt.Errorf("no published record found for key %d", keyID))
				return ctx.Error(apiErr, apiErr.HttpStatus())
			}
			valuePath, err := record.Value()
			if err != nil {
				apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("failed to get IPNS record value from DHT: %w", err))
				return ctx.Error(apiErr, apiErr.HttpStatus())
			}
			cidStr, err = paths.ExtractCIDFromPathStrict(valuePath)
			if err != nil {
				apiErr := NewError(ErrKeyInvalidRequest, err)
				return ctx.Error(apiErr, apiErr.HttpStatus())
			}
		}
	}

	privKey, _, err := a.ipnsKeyService.GetPrivateKeyByPeerID(reqCtx, key.PeerID().String())
	if err != nil {
		a.Logger().Error("Failed to get private key for republish", zap.Error(err), zap.String("peer_id", key.PeerID().String()))
		apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("failed to get private key: %w", err))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	if err := ipnsKeyService.PublishWithKey(reqCtx, privKey, cidStr, 0); err != nil {
		a.Logger().Error("Failed to republish IPNS record", zap.Error(err), zap.String("peer_id", key.PeerID().String()))
		apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("failed to republish IPNS record: %w", err))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	resp := dto.IPNSRepublishResponse{
		Count:   1,
		Message: "Successfully republished IPNS record",
	}

	return httputil.EncodeResponse(ctx, nil, &resp)
}
