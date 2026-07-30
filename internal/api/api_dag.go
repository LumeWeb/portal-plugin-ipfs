package api

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"go.lumeweb.com/httputil"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	"go.lumeweb.com/portal/core"
	"go.uber.org/zap"
)

// handleGetDAG resolves the complete block graph for a given CID.
//
// This endpoint is intentionally not user-scoped: IPFS content is public and
// content-addressed by design. The DAG topology (block sizes, child link CIDs)
// is encoded in the IPLD blocks themselves and is retrievable from any IPFS
// gateway by anyone who has the root CID. No ownership or pin check is applied
// because the data exposed here is not secret — it can be derived by fetching
// and parsing the raw blocks from the public network.
//
// If the portal ever introduces private pins or access-controlled content,
// this endpoint must be revisited to add a user-scoped ownership check before
// calling ResolveDAG.
func (a *API) handleGetDAG(c echo.Context) error {
	ctx := httputil.Context(c)

	req := dto.GetDAGRequest{}
	model, ok := httputil.DecodeAndValidatePathRequest(ctx, &req)
	if !ok {
		return nil
	}

	proto := core.GetProtocol(a.ipfs.Name())
	if proto == nil {
		a.Logger().Error("IPFS protocol not available")
		apiErr := NewError(ErrKeyMetadataFetchFailed, nil)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	dagProvider, ok := proto.(core.ProtocolDAGProvider)
	if !ok {
		a.Logger().Error("IPFS protocol does not support DAG traversal")
		apiErr := NewError(ErrKeyMetadataFetchFailed, nil)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	nodes, err := dagProvider.ResolveDAG(ctx.Request().Context(), model.CID)
	if err != nil {
		a.Logger().Error("Failed to resolve DAG", zap.Error(err))
		apiErr := NewError(ErrKeyMetadataFetchFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	if len(nodes) == 0 {
		apiErr := NewError(ErrKeyBlockNotFound, nil)
		return ctx.Error(apiErr, http.StatusNotFound)
	}

	result := &dto.DAGResolution{
		RootCID: model.CID,
		Nodes:   nodes,
	}

	return httputil.EncodeResponse(ctx, result, &dto.DAGResponse{})
}
