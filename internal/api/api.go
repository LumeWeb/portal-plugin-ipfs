package api

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/google/uuid"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/multiformats/go-multiaddr"
	"github.com/samber/lo"
	"github.com/tus/tusd/v2/pkg/handler"
	billingPluginService "go.lumeweb.com/portal-plugin-billing/service"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/messages"
	"go.lumeweb.com/portal-plugin-ipfs/internal/cron/define"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/ipfs"
	pluginService "go.lumeweb.com/portal-plugin-ipfs/internal/service"
	"go.lumeweb.com/portal/event"
	"go.lumeweb.com/portal/middleware"
	"go.lumeweb.com/portal/service"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"go.lumeweb.com/httputil"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal/config"
	"go.lumeweb.com/portal/core"
)

var _ core.API = (*API)(nil)

const TUS_HTTP_ROUTE = "/api/upload/tus"

type API struct {
	ctx        core.Context
	config     config.Manager
	logger     *core.Logger
	ipfs       *protocol.Protocol
	ipfsUpload *pluginService.UploadService
	cron       core.CronService
	tus        *service.TusHandler
	upload     core.UploadService
}

func NewAPI() (core.API, []core.ContextBuilderOption, error) {
	api := &API{}
	return api, core.ContextOptions(
		core.ContextWithStartupFunc(func(ctx core.Context) error {
			api.ctx = ctx
			api.config = ctx.Config()
			api.logger = ctx.APILogger(api)
			api.ipfs = core.GetProtocol(internal.ProtocolName).(*protocol.Protocol)
			api.ipfsUpload = core.GetService[*pluginService.UploadService](ctx, pluginService.UPLOAD_SERVICE)
			api.cron = core.GetService[core.CronService](ctx, core.CRON_SERVICE)
			api.upload = core.GetService[core.UploadService](ctx, core.UPLOAD_SERVICE)
			tus, err := service.CreateTusHandler(ctx, service.TusHandlerConfig{
				BasePath: TUS_HTTP_ROUTE,
				CreatedUploadHandler: service.TUSDefaultUploadCreatedHandler(ctx, func(hook handler.HookEvent, uploaderId uint) (core.StorageHash, error) {
					return nil, nil
				}, func(requestId uint) error {
					err := api.ipfsUpload.SetTusUploadRequestID(ctx, requestId)
					if err != nil {
						return err
					}

					return nil
				}),
				UploadProgressHandler:   service.TUSDefaultUploadProgressHandler(ctx),
				TerminatedUploadHandler: service.TUSDefaultUploadTerminatedHandler(ctx),
				CompletedUploadHandler: service.TUSDefaultUploadCompletedHandler(ctx, func(_ *service.TusHandler, hook handler.HookEvent) {
					upload, err := api.tus.UploadReader(ctx, hook.Upload.ID, 0)

					if err != nil {
						api.logger.Error("Failed to get request reader", zap.Error(err))
						return
					}

					defer func(upload io.ReadCloser) {
						err := upload.Close()
						if err != nil {
							api.logger.Error("Failed to close reader", zap.Error(err))
						}
					}(upload)

					root, err := validateCar(upload)

					if err != nil {
						api.logger.Error("Failed to validate car", zap.Error(err))
						err = api.tus.FailUploadById(ctx, hook.Upload.ID)
						if err != nil {
							api.logger.Error("Failed to fail ipfsUpload", zap.Error(err))
						}
						return
					}

					err = api.tus.SetHashById(ctx, hook.Upload.ID, internal.NewIPFSHash(root.Cid()))
					if err != nil {
						api.logger.Error("Failed to set ipfsUpload hash", zap.Error(err))
						return
					}
					err = api.cron.CreateJobIfNotExists(define.CronTaskTusUploadName, define.CronTaskTusUploadArgs{
						UploadID: hook.Upload.ID,
					})
					if err != nil {
						api.logger.Error("Failed to create ipfsUpload cron job", zap.Error(err))
					}
				}),
			})

			if err != nil {
				return fmt.Errorf("failed to create tus handler: %w", err)
			}

			api.tus = tus
			api.tus.SetStorageProtocol(api.ipfs)

			return nil
		}),
	), nil
}

type readSeekNopCloser struct {
	*bytes.Reader
}

func (rsnc readSeekNopCloser) Close() error {
	return nil
}

func (a API) Name() string {
	return internal.ProtocolName
}

func (a API) Subdomain() string {
	return internal.ProtocolName
}

func (a *API) Configure(router *mux.Router, accessSvc core.AccessService) error {
	// Middleware setup
	authMw := middleware.AuthMiddleware(middleware.AuthMiddlewareOptions{
		Context: a.ctx,
		Purpose: core.JWTPurposeLogin,
	})
	verifyMw := middleware.AccountVerifiedMiddleware(a.ctx)
	corsHandler := middleware.CorsMiddleware(nil)

	// Define route groups
	routeGroups := []struct {
		router *mux.Router
		routes []struct {
			path      string
			method    string
			handler   http.HandlerFunc
			useVerify bool
			access    string
		}
	}{
		{
			router: router.PathPrefix("").Subrouter(),
			routes: []struct {
				path      string
				method    string
				handler   http.HandlerFunc
				useVerify bool
				access    string
			}{
				{"/pins", "GET", a.handleGetPins, false, core.ACCESS_USER_ROLE},
				{"/pins", "POST", a.handleAddPin, true, core.ACCESS_USER_ROLE},
				{"/pins/{requestid}", "GET", a.handleGetPinByRequestId, false, core.ACCESS_USER_ROLE},
				{"/pins/{requestid}", "POST", a.handleReplacePinByRequestId, true, core.ACCESS_USER_ROLE},
				{"/pins/{requestid}", "DELETE", a.handleDeletePinByRequestId, true, core.ACCESS_USER_ROLE},
				{"/ipfs/{cid}", "GET", a.handleIPFSGet, true, core.ACCESS_USER_ROLE},
				{"/ipfs/{cid}", "HEAD", a.handleIPFSGet, true, core.ACCESS_USER_ROLE},
			},
		},
		{
			router: router.PathPrefix("/api").Subrouter(),
			routes: []struct {
				path      string
				method    string
				handler   http.HandlerFunc
				useVerify bool
				access    string
			}{
				{"/upload", "POST", a.handleUpload, true, core.ACCESS_USER_ROLE},
				{"/block/meta/{cid}", "GET", a.handleGetBlockMeta, true, core.ACCESS_USER_ROLE},
				{"/block/meta/batch", "POST", a.handleGetBlockMetaBatch, true, core.ACCESS_USER_ROLE},
			},
		},
		{
			router: router.PathPrefix("").Subrouter(),
			routes: []struct {
				path      string
				method    string
				handler   http.HandlerFunc
				useVerify bool
				access    string
			}{
				{"/api/info", "GET", a.handleGetInfo, false, ""},
			},
		},
	}

	// Register routes
	for _, group := range routeGroups {
		group.router.Use(corsHandler)
		group.router.Use(authMw)

		for _, route := range group.routes {
			r := group.router.HandleFunc(route.path, route.handler).Methods(route.method, "OPTIONS")
			if route.useVerify {
				r.Use(verifyMw)
			}

			if err := accessSvc.RegisterRoute(a.Subdomain(), route.path, route.method, route.access); err != nil {
				return fmt.Errorf("failed to register route %s %s: %w", route.method, route.path, err)
			}
		}
	}

	a.tus.SetupRoute(router, authMw, TUS_HTTP_ROUTE)

	return nil
}

func (a API) TusHandler() *service.TusHandler {
	return a.tus
}

func (a API) AuthTokenName() string {
	return core.AUTH_TOKEN_NAME
}

func (a API) Config() config.APIConfig {
	return nil
}

func (a API) handleGetPins(w http.ResponseWriter, r *http.Request) {
	ctx := httputil.Context(r, w)
	var req messages.GetPinsRequest

	if err := ctx.DecodeForm("cid", &req.CID); err != nil {
		return
	}
	if err := ctx.DecodeForm("name", &req.Name); err != nil {
		return
	}
	if err := ctx.DecodeForm("match", &req.Match); err != nil {
		return
	}
	if err := ctx.DecodeForm("status", &req.Status); err != nil {
		return
	}
	if err := ctx.DecodeForm("before", &req.Before); err != nil {
		return
	}
	if err := ctx.DecodeForm("after", &req.After); err != nil {
		return
	}
	if err := ctx.DecodeForm("_before", &req.BeforeCount); err != nil {
		return
	}
	if err := ctx.DecodeForm("_after", &req.AfterCount); err != nil {
		return
	}
	if err := ctx.DecodeForm("limit", &req.Limit); err != nil {
		return
	}

	user, err := middleware.GetUserFromContext(r.Context())
	if err != nil {
		_ = ctx.Error(core.NewAccountError(core.ErrKeyLoginFailed, nil), http.StatusBadRequest)
		return
	}

	results, err := a.ipfsUpload.GetPins(ctx, req, user)
	if err != nil {
		_ = ctx.Error(err, http.StatusInternalServerError)
		return
	}

	ctx.Response.Header().Set("X-Total-Count", strconv.FormatUint(results.Count, 10))
	ctx.Encode(results)
}

func (a API) handleAddPin(w http.ResponseWriter, r *http.Request) {
	ctx := httputil.Context(r, w)
	var req messages.AddPinRequest
	if err := ctx.Decode(&req); err != nil {
		return
	}

	user, err := middleware.GetUserFromContext(r.Context())
	if err != nil {
		_ = ctx.Error(core.NewAccountError(core.ErrKeyLoginFailed, nil), http.StatusBadRequest)
		return
	}

	status, err := a.ipfsUpload.AddQueuedPin(ctx, req.Pin, user, r.RemoteAddr)
	if err != nil {
		_ = ctx.Error(err, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	ctx.Encode(status)
}
func (a API) handleGetPinByRequestId(w http.ResponseWriter, r *http.Request) {
	ctx := httputil.Context(r, w)
	requestid := mux.Vars(r)["requestid"]

	id, err := uuid.Parse(requestid)
	if err != nil {
		_ = ctx.Error(fmt.Errorf("invalid requestid: %w", err), http.StatusBadRequest)
		return
	}

	user, err := middleware.GetUserFromContext(r.Context())
	if err != nil {
		_ = ctx.Error(core.NewAccountError(core.ErrKeyLoginFailed, nil), http.StatusBadRequest)
		return
	}

	status, err := a.ipfsUpload.GetPinStatus(ctx, id, user)
	if err != nil {
		_ = ctx.Error(err, http.StatusInternalServerError)
		return
	}

	ctx.Encode(status)
}

func (a API) handleReplacePinByRequestId(w http.ResponseWriter, r *http.Request) {
	ctx := httputil.Context(r, w)
	requestid := mux.Vars(r)["requestid"]

	id, err := uuid.Parse(requestid)
	if err != nil {
		_ = ctx.Error(fmt.Errorf("invalid requestid: %w", err), http.StatusBadRequest)
		return
	}

	user, err := middleware.GetUserFromContext(r.Context())
	if err != nil {
		_ = ctx.Error(core.NewAccountError(core.ErrKeyLoginFailed, nil), http.StatusBadRequest)
		return
	}

	var req messages.ReplacePinRequest
	if err := ctx.Decode(&req); err != nil {
		return
	}

	status, err := a.ipfsUpload.ReplacePin(ctx, id, req.Pin, user)
	if err != nil {
		_ = ctx.Error(err, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	ctx.Encode(status)
}

func (a API) handleDeletePinByRequestId(w http.ResponseWriter, r *http.Request) {
	ctx := httputil.Context(r, w)
	requestid := mux.Vars(r)["requestid"]

	id, err := uuid.Parse(requestid)
	if err != nil {
		_ = ctx.Error(fmt.Errorf("invalid requestid: %w", err), http.StatusBadRequest)
		return
	}

	user, err := middleware.GetUserFromContext(r.Context())
	if err != nil {
		_ = ctx.Error(core.NewAccountError(core.ErrKeyLoginFailed, nil), http.StatusBadRequest)
		return
	}

	err = a.ipfsUpload.DeletePin(ctx, id, user)
	if err != nil {
		_ = ctx.Error(err, http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func (a API) handleIPFSGet(w http.ResponseWriter, r *http.Request) {
	ctx := httputil.Context(r, w)
	_cid := mux.Vars(r)["cid"]

	var req messages.IPFSRequest
	if err := ctx.DecodeForm("format", &req.Format); err != nil {
		return
	}
	if err := ctx.DecodeForm("filename", &req.Filename); err != nil {
		return
	}
	if err := ctx.DecodeForm("download", &req.Download); err != nil {
		return
	}
	if err := ctx.DecodeForm("dag-scope", &req.DagScope); err != nil {
		return
	}
	if err := ctx.DecodeForm("entity-bytes", &req.EntityBytes); err != nil {
		return
	}
	if err := ctx.DecodeForm("car-version", &req.CarVersion); err != nil {
		return
	}
	if err := ctx.DecodeForm("car-order", &req.CarOrder); err != nil {
		return
	}
	if err := ctx.DecodeForm("car-dups", &req.CarDups); err != nil {
		return
	}

	// Set default values
	if req.Format == "" {
		req.Format = "raw"
	}
	if req.DagScope == "" {
		req.DagScope = "all"
	}

	// Validate and process the request
	switch req.Format {
	case "raw":
		a.handleRawBlockRequest(ctx, _cid, w, r)
	case "car":
		// TODO: Implement CAR handling
		w.Header().Set("Content-Type", "application/vnd.ipld.car")
		w.WriteHeader(http.StatusNotImplemented)
	default:
		http.Error(w, "Unsupported format", http.StatusBadRequest)
		return
	}
}

func (a API) handleRawBlockRequest(ctx httputil.RequestContext, _cid string, w http.ResponseWriter, r *http.Request) {
	pCid, err := cid.Parse(_cid)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to parse CID: %v", err), http.StatusBadRequest)
		return
	}

	user, err := middleware.GetUserFromContext(r.Context())
	if err != nil {
		_ = ctx.Error(core.NewAccountError(core.ErrKeyLoginFailed, nil), http.StatusBadRequest)
		return
	}

	// Check if the block exists before trying to fetch it
	exists, err := a.ipfs.GetNode().HasBlock(ctx, pCid)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to check if block exists: %v", err), http.StatusInternalServerError)
		return
	}

	if !exists {
		http.Error(w, fmt.Sprintf("Block not found: %s", pCid.String()), http.StatusNotFound)
		return
	}

	upload, err := a.upload.GetUpload(ctx, internal.NewIPFSHash(pCid))
	if err != nil {
		_ = ctx.Error(err, http.StatusInternalServerError)
		return
	}

	if core.ServiceExists(a.ctx, billingPluginService.QUOTA_SERVICE) {
		quotaService := core.GetService[billingPluginService.QuotaService](a.ctx, billingPluginService.QUOTA_SERVICE)
		allowed, err := quotaService.CheckDownloadQuota(user, upload.Size)
		if err != nil {
			_ = ctx.Error(err, http.StatusInternalServerError)
			return
		}

		if !allowed {
			_ = ctx.Error(err, http.StatusInsufficientStorage)
			return
		}
	}

	block, err := a.ipfs.GetNode().GetBlock(ctx, pCid)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get block: %v", err), http.StatusInternalServerError)
		return
	}

	err = event.FireDownloadCompletedEventAsync(a.ctx, upload.ID, upload.Size, r.RemoteAddr)
	if err != nil {
		a.logger.Error("Failed to fire storage object uploaded event", zap.Error(err))
	}

	a.setTrustlessHeaders(w, r, _cid)
	w.Header().Set("Content-Type", "application/vnd.ipld.raw")
	_, _ = w.Write(block.RawData())
}

func (a API) handleUpload(w http.ResponseWriter, r *http.Request) {
	ctx := httputil.Context(r, w)
	user, err := middleware.GetUserFromContext(r.Context())

	if err != nil {
		_ = ctx.Error(core.NewAccountError(core.ErrKeyLoginFailed, nil), http.StatusBadRequest)
		return
	}

	file, _, err := a.prepareFileUpload(r)
	if err != nil {
		_ = ctx.Error(err, http.StatusBadRequest)
		return
	}

	defer func(file io.ReadSeekCloser) {
		err := file.Close()
		if err != nil {
			a.logger.Error("Error closing file", zap.Error(err))
		}
	}(file)

	err = a.ipfsUpload.HandlePostUpload(ctx, file, user, r.RemoteAddr)
	if err != nil {
		sysError := NewError(ErrKeyFileUploadFailed, err)
		errorCode := sysError.HttpStatus()

		if errors.Is(err, pluginService.ErrStorageQuotaExceeded) ||
			errors.Is(err, pluginService.ErrDownloadQuotaExceeded) ||
			errors.Is(err, pluginService.ErrUploadQuotaExceeded) {
			errorCode = http.StatusInsufficientStorage
		}

		_ = ctx.Error(sysError, errorCode)
		return
	}

	ctx.Encode(&messages.PostUploadResponse{})
}

func (a API) prepareFileUpload(r *http.Request) (file io.ReadSeekCloser, size uint64, err error) {
	contentType := r.Header.Get("Content-Type")

	// Handle multipart form data uploads
	if strings.HasPrefix(contentType, "multipart/form-data") {
		if err := r.ParseMultipartForm(int64(a.config.Config().Core.PostUploadLimit)); err != nil {
			return nil, size, NewError(ErrKeyFileUploadFailed, err)
		}

		multipartFile, multipartHeader, err := r.FormFile("file")
		if err != nil {
			return nil, size, NewError(ErrKeyFileUploadFailed, err)
		}

		size = uint64(multipartHeader.Size)

		return multipartFile, size, nil
	}

	// Handle raw body uploads
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, size, NewError(ErrKeyFileUploadFailed, err)
	}

	buffer := readSeekNopCloser{bytes.NewReader(data)}

	size = uint64(len(data))

	return buffer, size, nil
}

func (a API) handleGetBlockMeta(w http.ResponseWriter, r *http.Request) {
	ctx := httputil.Context(r, w)
	_cid := mux.Vars(r)["cid"]

	pCid, err := cid.Parse(_cid)
	if err != nil {
		_ = ctx.Error(fmt.Errorf("failed to parse CID: %w", err), http.StatusBadRequest)
		return
	}

	meta, err := a.ipfsUpload.GetBlockMeta(ctx, pCid)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			_ = ctx.Error(fmt.Errorf("block not found: %w", err), http.StatusNotFound)
			return
		}

		_ = ctx.Error(fmt.Errorf("failed to get block: %w", err), http.StatusInternalServerError)
		return
	}

	ctx.Encode(meta)
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

func (a API) handleGetBlockMetaBatch(w http.ResponseWriter, r *http.Request) {
	ctx := httputil.Context(r, w)
	var req messages.GetBlockMetaBatchRequest
	if err := ctx.Decode(&req); err != nil {
		return
	}

	metas := make(map[string]*messages.BlockMetaResponse, len(req.CID))

	for _, _cid := range req.CID {
		parsedCid, err := cid.Decode(_cid)
		if err != nil {
			_ = ctx.Error(fmt.Errorf("invalid CID: %w", err), http.StatusBadRequest)
			return
		}

		meta, err := a.ipfsUpload.GetBlockMeta(ctx, parsedCid)
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				continue
			}
			_ = ctx.Error(err, http.StatusInternalServerError)
			return
		}

		metas[encoding.ToV1(parsedCid).String()] = meta

	}
	ctx.Encode(metas)
}

func (a API) handleGetInfo(w http.ResponseWriter, r *http.Request) {
	ctx := httputil.Context(r, w)

	addrs, err := ipfs.AnnouncementAddresses()
	if err != nil {
		_ = ctx.Error(err, http.StatusInternalServerError)
		return
	}

	connAddrs, err := a.ipfs.GetNode().ConnectionAddresses()
	if err != nil {
		_ = ctx.Error(err, http.StatusInternalServerError)
		return
	}

	ctx.Encode(&messages.InfoResponse{
		PeerID: a.ipfs.GetNode().PeerID().String(),
		AnnouncementAddresses: lo.Map(addrs, func(addr multiaddr.Multiaddr, _ int) string {
			return addr.String()
		}),
		ConnectionAddresses: lo.Map(connAddrs, func(addr multiaddr.Multiaddr, _ int) string {
			return addr.String()
		}),
	})
}

func validateCar(r io.Reader) (blocks.Block, error) {
	reader, err := car.NewBlockReader(r)
	if err != nil {
		return nil, err
	}

	rootBlock, err := reader.Next()
	if err != nil {
		return nil, err
	}

	return rootBlock, nil
}
