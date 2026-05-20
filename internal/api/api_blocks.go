package api

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/multiformats/go-multiaddr"
	"github.com/samber/lo"
	"go.lumeweb.com/httputil"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/dto"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/ipfs"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func (a *API) handleGetBlockMeta(c echo.Context) error {
	ctx := httputil.Context(c)

	req := dto.GetBlockMetaRequest{}
	model, ok := httputil.DecodeAndValidateRequest(ctx, &req)

	if !ok {
		return nil
	}

	meta, err := a.blockService.GetBlockMeta(ctx.Request().Context(), model.CID)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			apiErr := NewError(ErrKeyMetadataFetchFailed, err)
			return ctx.Error(apiErr, http.StatusNotFound)
		}

		a.Logger().Error("Failed to get block meta", zap.Error(err))
		apiErr := NewError(ErrKeyMetadataFetchFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	return httputil.EncodeResponse(ctx, meta, &dto.BlockMetaResponse{})
}

func (a *API) handleGetBlockMetaBatch(c echo.Context) error {
	ctx := httputil.Context(c)

	req := dto.GetBlockMetaBatchRequest{}
	model, ok := httputil.DecodeAndValidateRequest(ctx, &req)

	if !ok {
		return nil
	}

	meta, err := a.blockService.GetBlockMetaBatch(ctx.Request().Context(), model.CID)

	if err != nil {
		a.Logger().Error("Failed to get block meta", zap.Error(err))
		apiErr := NewError(ErrKeyMetadataFetchFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	return httputil.EncodeResponse(ctx, meta, &dto.GetBlockMetaBatchResponse{})
}

func (a *API) handleGetInfo(c echo.Context) error {
	ctx := httputil.Context(c)

	addrs, err := ipfs.AnnouncementAddresses(a.ipfs.GetNode().AnnounceWeb(), a.ipfs.GetNode().AnnounceDomain(), a.ipfs.GetNode().HostAddrs())
	if err != nil {
		a.Logger().Error("Failed to get announcement addresses", zap.Error(err))
		apiErr := NewError(ErrKeyMetadataFetchFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	connAddrs, err := a.ipfs.GetNode().ConnectionAddresses()
	if err != nil {
		a.Logger().Error("Failed to get connection addresses", zap.Error(err))
		apiErr := NewError(ErrKeyMetadataFetchFailed, err)
		return ctx.Error(apiErr, apiErr.HttpStatus())
	}

	nodeInfo := dto.NodeInfo{
		PeerID: a.ipfs.GetNode().PeerID().String(),
		AnnouncementAddresses: lo.Map(addrs, func(addr multiaddr.Multiaddr, _ int) string {
			return addr.String()
		}),
		ConnectionAddresses: lo.Map(connAddrs, func(addr multiaddr.Multiaddr, _ int) string {
			return addr.String()
		}),
	}

	return httputil.EncodeResponse(ctx, &nodeInfo, &dto.InfoResponse{})
}
