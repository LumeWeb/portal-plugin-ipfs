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
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.uber.org/zap"
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

	keys, err := a.ipnsKeyService.ListKeys(reqCtx, user)
	if err != nil {
		a.Logger().Error("Failed to list IPNS keys", zap.Error(err), zap.Uint("user_id", user))
		apiErr := NewError(ErrKeyFileProcessingFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	responses := make([]dto.IPNSKeyResponse, len(keys))
	for i, key := range keys {
		if err := responses[i].FromModel(&key); err != nil {
			a.Logger().Error("Failed to convert IPNS key to response", zap.Error(err))
			apiErr := NewError(ErrKeyFileProcessingFailed, err)
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}
	}

	return ctx.JSON(http.StatusOK, responses)
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
		return ctx.JSON(http.StatusOK, resp)
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

	return ctx.JSON(http.StatusOK, resp)
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

	// Parse optional checkRouting query parameter
	checkRouting := c.QueryParam("check_routing") == "true"

	// Get the published record
	record, err := ipnsKeyService.GetPublished(reqCtx, name, checkRouting)
	if err != nil {
		a.Logger().Error("Failed to resolve IPNS name", zap.Error(err), zap.String("name", name))
		apiErr := NewError(ErrKeyPinFetchFailed, fmt.Errorf("failed to resolve IPNS name: %w", err))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	// Convert IPNS record to response
	valuePath, err := record.Value()
	if err != nil {
		a.Logger().Error("Failed to get IPNS record value", zap.Error(err))
		apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("failed to get IPNS record value: %w", err))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

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
		Value:    valuePath.String(),
		Sequence: sequence,
		Path:     dto.IPFSPath(valuePath.String()),
		Expired:  time.Now().After(validity),
		Expires:  validity,
	}

	return ctx.JSON(http.StatusOK, resp)
}

// republishIPNS republishes IPNS records to keep them alive in the network.
// If key_id is provided in request body, republishes only that key.
// Otherwise republishes all records owned by the authenticated user.
// Returns 200 OK with IPNSRepublishResponse containing count of republished records.
func (a *API) republishIPNS(c echo.Context) error {
	ctx := httputil.Context(c)
	reqCtx := ctx.Context.Request().Context()

	// Parse optional key_id parameter from request body
	var req struct {
		KeyID *uint `json:"key_id,omitempty"`
	}
	if err := c.Bind(&req); err != nil {
		// If body parsing fails, it's okay - key_id is optional
	}

	// Get IPNSKeyService
	ipnsKeyService := core.GetService[pluginCore.IPNSKeyService](a.Context(), pluginCore.IPNS_KEY_SERVICE)
	if ipnsKeyService == nil {
		a.Logger().Error("IPNSKeyService not available")
		apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("IPNSKeyService not available"))
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	var count int

	if req.KeyID != nil {
		// Republish a specific key
		user, err := mcontext.GetUserID(c)
		if err != nil {
			return err
		}

		// Get the IPNS key by ID and verify ownership
		key, err := a.ipnsKeyService.GetKeyByID(reqCtx, user, *req.KeyID)
		if err != nil {
			a.Logger().Error("Failed to get IPNS key", zap.Error(err), zap.Uint("key_id", *req.KeyID), zap.Uint("user_id", user))
			apiErr := NewError(ErrKeyPinFetchFailed, err)
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}

		// Get the current published record
		record, err := ipnsKeyService.GetPublished(reqCtx, key.PeerID().String(), false)
		if err != nil {
			a.Logger().Error("Failed to get published record for republish", zap.Error(err), zap.String("peer_id", key.PeerID().String()))
			apiErr := NewError(ErrKeyPinFetchFailed, fmt.Errorf("failed to get published record: %w", err))
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}

		// Get the value from the record
		valuePath, err := record.Value()
		if err != nil {
			a.Logger().Error("Failed to get IPNS record value for republish", zap.Error(err), zap.String("peer_id", key.PeerID().String()))
			apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("failed to get IPNS record value: %w", err))
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}

		// Get the private key and republish
		privKey, _, err := a.ipnsKeyService.GetPrivateKeyByPeerID(reqCtx, key.PeerID().String())
		if err != nil {
			a.Logger().Error("Failed to get private key for republish", zap.Error(err), zap.String("peer_id", key.PeerID().String()))
			apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("failed to get private key: %w", err))
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}

		// Republish with the same value
		err = ipnsKeyService.PublishWithKey(reqCtx, privKey, valuePath.String(), 0)
		if err != nil {
			a.Logger().Error("Failed to republish IPNS record", zap.Error(err), zap.String("peer_id", key.PeerID().String()))
			apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("failed to republish IPNS record: %w", err))
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}

		count = 1
	} else {
		// Republish all keys
		records, err := ipnsKeyService.ListPublished(reqCtx)
		if err != nil {
			a.Logger().Error("Failed to list published records", zap.Error(err))
			apiErr := NewError(ErrKeyFileProcessingFailed, fmt.Errorf("failed to list published records: %w", err))
			return ctx.Error(apiErr, apiErr.HttpStatus())
		}

		// Republish each record
		for ipnsName, record := range records {
			peerID := ipnsName.Peer().String()

			privKey, _, err := a.ipnsKeyService.GetPrivateKeyByPeerID(reqCtx, peerID)
			if err != nil {
				a.Logger().Warn("Failed to get private key for republish, skipping", zap.Error(err), zap.String("peer_id", peerID))
				continue
			}

			valuePath, err := record.Value()
			if err != nil {
				a.Logger().Warn("Failed to get IPNS record value for republish, skipping", zap.Error(err), zap.String("peer_id", peerID))
				continue
			}

			err = ipnsKeyService.PublishWithKey(reqCtx, privKey, valuePath.String(), 0)
			if err != nil {
				a.Logger().Warn("Failed to republish IPNS record, skipping", zap.Error(err), zap.String("peer_id", peerID))
				continue
			}

			count++
		}
	}

	// Return response
	resp := dto.IPNSRepublishResponse{
		Count:   count,
		Message: fmt.Sprintf("Successfully republished %d IPNS record(s)", count),
	}

	return ctx.JSON(http.StatusOK, resp)
}
