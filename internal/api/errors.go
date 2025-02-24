package api

import (
	"net/http"

	core "go.lumeweb.com/portal/core"
)

// Error keys
const (
	Namespace                                 = "ipfs-plugin-api"
	ErrKeyFileUploadFailed     core.ErrorType = "ErrFileUploadFailed"
	ErrKeyMetadataFetchFailed  core.ErrorType = "ErrMetadataFetchFailed"
	ErrKeyPinFetchFailed       core.ErrorType = "ErrPinFetchFailed"
	ErrKeyInvalidUUIDFormat    core.ErrorType = "ErrInvalidUUIDFormat"
	ErrKeyFileProcessingFailed core.ErrorType = "ErrFileProcessingFailed"
	ErrKeyCIDParseFailed       core.ErrorType = "ErrKeyCIDParseFailed"
	ErrKeyBlockNotFound        core.ErrorType = "ErrKeyBlockNotFound"
	ErrKeyUploadNotFound       core.ErrorType = "ErrKeyUploadNotFound"
)

func init() {
	core.MustRegisterNamespace(Namespace)
	core.MustRegisterDefaultErrorMessages(Namespace, map[core.ErrorType]core.ErrorDefinition{
		ErrKeyFileUploadFailed:     {Key: ErrKeyFileUploadFailed, Message: "File upload failed due to an internal error."},
		ErrKeyMetadataFetchFailed:  {Key: ErrKeyMetadataFetchFailed, Message: "Failed to fetch metadata."},
		ErrKeyPinFetchFailed:       {Key: ErrKeyPinFetchFailed, Message: "Failed to fetch pin."},
		ErrKeyInvalidUUIDFormat:    {Key: ErrKeyInvalidUUIDFormat, Message: "Invalid UUID format provided: %s"},
		ErrKeyFileProcessingFailed: {Key: ErrKeyFileProcessingFailed, Message: "Failed to process the file."},
		ErrKeyCIDParseFailed:       {Key: ErrKeyCIDParseFailed, Message: "Failed to parse CID."},
		ErrKeyBlockNotFound:        {Key: ErrKeyBlockNotFound, Message: "Block not found."},
		ErrKeyUploadNotFound:       {Key: ErrKeyUploadNotFound, Message: "Upload not found."},
	})

	core.MustRegisterErrorCodes(Namespace, map[core.ErrorType]int{
		ErrKeyFileUploadFailed:     http.StatusInternalServerError,
		ErrKeyMetadataFetchFailed:  http.StatusInternalServerError,
		ErrKeyPinFetchFailed:       http.StatusInternalServerError,
		ErrKeyInvalidUUIDFormat:    http.StatusBadRequest,
		ErrKeyFileProcessingFailed: http.StatusInternalServerError,
		ErrKeyCIDParseFailed:       http.StatusBadRequest,
		ErrKeyBlockNotFound:        http.StatusNotFound,
		ErrKeyUploadNotFound:       http.StatusNotFound,
	})
}

func NewError(key core.ErrorType, err error, args ...any) *core.Error {
	return core.NewError(Namespace, key, err, args...)
}
