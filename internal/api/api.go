package api

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
	"github.com/tus/tusd/v2/pkg/handler"
	"go.lumeweb.com/portal-plugin-ipfs/internal/api/messages"
	"go.lumeweb.com/portal-plugin-ipfs/internal/cron/define"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	pluginService "go.lumeweb.com/portal-plugin-ipfs/internal/service"
	"go.lumeweb.com/portal/middleware"
	"go.lumeweb.com/portal/service"
	"go.uber.org/zap"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/rs/cors"
	"go.lumeweb.com/httputil"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal/config"
	"go.lumeweb.com/portal/core"
)

var _ core.API = (*API)(nil)

const TUS_HTTP_ROUTE = "/api/upload/tus"

type API struct {
	ctx    core.Context
	config config.Manager
	logger *core.Logger
	ipfs   *protocol.Protocol
	upload *pluginService.UploadService
	cron   core.CronService
	tus    *service.TusHandler
}

func NewAPI() (core.API, []core.ContextBuilderOption, error) {
	api := &API{}
	return api, core.ContextOptions(
		core.ContextWithStartupFunc(func(ctx core.Context) error {
			api.ctx = ctx
			api.config = ctx.Config()
			api.logger = ctx.APILogger(api)
			api.ipfs = core.GetProtocol(internal.ProtocolName).(*protocol.Protocol)
			api.upload = core.GetService[*pluginService.UploadService](ctx, pluginService.UPLOAD_SERVICE)
			api.cron = core.GetService[core.CronService](ctx, core.CRON_SERVICE)
			tus, err := service.CreateTusHandler(ctx, service.TusHandlerConfig{
				BasePath: TUS_HTTP_ROUTE,
				CreatedUploadHandler: service.TUSDefaultUploadCreatedHandler(ctx, func(hook handler.HookEvent, uploaderId uint) (core.StorageHash, error) {
					return nil, nil
				}, func(requestId uint) error {
					err := api.upload.SetTusUploadRequestID(ctx, requestId)
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
							api.logger.Error("Failed to fail upload", zap.Error(err))
						}
						return
					}

					err = api.tus.SetHashById(ctx, hook.Upload.ID, internal.NewIPFSHash(root.Cid()))
					if err != nil {
						api.logger.Error("Failed to set upload hash", zap.Error(err))
						return
					}
					err = api.cron.CreateJobIfNotExists(define.CronTaskTusUploadName, define.CronTaskTusUploadArgs{
						UploadID: hook.Upload.ID,
					})
					if err != nil {
						api.logger.Error("Failed to create upload cron job", zap.Error(err))
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

func (a API) Configure(router *mux.Router) error {
	authMiddlewareOpts := middleware.AuthMiddlewareOptions{
		Context: a.ctx,
		Purpose: core.JWTPurposeLogin,
	}

	authMw := middleware.AuthMiddleware(authMiddlewareOpts)

	defaultCors := cors.New(cors.Options{
		AllowOriginFunc: func(origin string) bool {
			return true
		},
		AllowedMethods:   []string{"POST", "GET", "DELETE", "HEAD"},
		AllowedHeaders:   []string{"*"},
		ExposedHeaders:   []string{"Content-Type", "X-Total-Count"},
		AllowCredentials: true,
	})

	pinRouter := router.PathPrefix("").Subrouter()
	pinRouter.Use(defaultCors.Handler)
	pinRouter.Use(authMw)

	// Configure Pinning Service routes
	pinRouter.HandleFunc("/pins", a.handleGetPins).Methods("GET", "OPTIONS")
	pinRouter.HandleFunc("/pins", a.handleAddPin).Methods("POST", "OPTIONS")
	pinRouter.HandleFunc("/pins/{requestid}", a.handleGetPinByRequestId).Methods("GET", "OPTIONS")
	pinRouter.HandleFunc("/pins/{requestid}", a.handleReplacePinByRequestId).Methods("POST", "OPTIONS")
	pinRouter.HandleFunc("/pins/{requestid}", a.handleDeletePinByRequestId).Methods("DELETE", "OPTIONS")

	// Configure Trustless Gateway routes
	pinRouter.HandleFunc("/ipfs/{cid}", a.handleIPFSGet).Methods("GET", "HEAD", "OPTIONS")

	apiRouter := router.PathPrefix("").Subrouter()
	apiRouter.Use(defaultCors.Handler)
	apiRouter.Use(authMw)
	// Car Post Upload
	apiRouter.HandleFunc("/api/upload", a.handleUpload).Methods("POST", "OPTIONS")

	// Configure TUS routes
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

	results, err := a.upload.GetPins(ctx, req, user)
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

	status, err := a.upload.AddQueuedPin(ctx, req.Pin, user, r.RemoteAddr)
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

	status, err := a.upload.GetPinStatus(ctx, id)
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

	var req messages.ReplacePinRequest
	if err := ctx.Decode(&req); err != nil {
		return
	}

	status, err := a.upload.ReplacePin(ctx, id, req.Pin)
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

	err = a.upload.DeletePin(ctx, id)
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

	block, err := a.ipfs.GetNode().GetBlock(ctx, pCid)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get block: %v", err), http.StatusInternalServerError)
		return
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

	err = a.upload.HandlePostUpload(ctx, file, user, r.RemoteAddr)
	if err != nil {
		_ = ctx.Error(NewError(ErrKeyFileUploadFailed, err), http.StatusBadRequest)
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
