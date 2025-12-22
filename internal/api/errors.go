package api

import (
	"encoding/json"
	"net/http"
	"strings"

	"go.lumeweb.com/portal-plugin-ipfs/internal/errors"
	router "go.lumeweb.com/portal-router"
	core "go.lumeweb.com/portal/core"
)

// Error keys
const (
	Namespace = "ipfs-plugin-api"

	// Re-export error type constants from internal/errors
	ErrKeyUnsupportedFormat = errors.ErrKeyUnsupportedFormat
	ErrKeyCorruptedFile     = errors.ErrKeyCorruptedFile
	ErrKeyEmptyZIP          = errors.ErrKeyEmptyZIP
	ErrKeyPasswordProtected = errors.ErrKeyPasswordProtected
	ErrKeyFileUploadFailed  = errors.ErrKeyFileUploadFailed

	// Other API error types
	ErrKeyFileUploadAPIFailed   core.ErrorType = "ErrFileUploadFailed"
	ErrKeyMetadataFetchFailed   core.ErrorType = "ErrMetadataFetchFailed"
	ErrKeyPinFetchFailed        core.ErrorType = "ErrPinFetchFailed"
	ErrKeyInvalidUUIDFormat     core.ErrorType = "ErrInvalidUUIDFormat"
	ErrKeyFileProcessingFailed  core.ErrorType = "ErrFileProcessingFailed"
	ErrKeyCIDParseFailed        core.ErrorType = "ErrKeyCIDParseFailed"
	ErrKeyBlockNotFound         core.ErrorType = "ErrKeyBlockNotFound"
	ErrKeyUploadNotFound        core.ErrorType = "ErrKeyUploadNotFound"
	ErrKeyUnauthorized          core.ErrorType = "ErrKeyUnauthorized"
	ErrKeyDownloadQuotaExceeded core.ErrorType = "ErrDownloadQuotaExceeded"
	ErrKeyInvalidRequest        core.ErrorType = "ErrInvalidRequest"
)

var _ router.ResponseError = (*IPFSError)(nil)

// ErrorDetails represents the structured error response format
type ErrorDetails struct {
	Reason  string `json:"reason"`
	Details string `json:"details,omitempty"`
}

// ErrorWrapper wraps ErrorDetails for custom JSON marshaling
type ErrorWrapper struct {
	Error ErrorDetails `json:"error"`
}

// IPFSError represents an IPFS-specific error that can be marshaled to JSON
type IPFSError struct {
	coreErr *core.Error
}

// MarshalJSON implements json.Marshaler interface
func (e *IPFSError) MarshalJSON() ([]byte, error) {
	if e == nil || e.coreErr == nil {
		return json.Marshal(ErrorWrapper{Error: ErrorDetails{Reason: "Unknown"}})
	}
	reason := string(e.coreErr.Key)

	// First strip "ErrKey" prefix if present
	if strings.HasPrefix(reason, "ErrKey") {
		reason = reason[6:] // Strip "ErrKey" prefix
	}

	// Then strip "Err" prefix if present
	if strings.HasPrefix(reason, "Err") {
		reason = reason[3:] // Strip "Err" prefix
	}

	details := ErrorDetails{
		Reason:  reason,
		Details: e.coreErr.Message,
	}

	wrapper := ErrorWrapper{Error: details}
	return json.Marshal(wrapper)
}

func (e *IPFSError) Error() string {
	return e.coreErr.Error()
}

func (e *IPFSError) HttpStatus() int {
	return e.coreErr.HttpStatus()
}

// Unwrap exposes the underlying core.Error for errors.Is/As.
func (e *IPFSError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.coreErr
}

func init() {
	core.MustRegisterNamespace(Namespace)
	core.MustRegisterDefaultErrorMessages(Namespace, map[core.ErrorType]core.ErrorDefinition{
		ErrKeyFileUploadAPIFailed:   {Key: ErrKeyFileUploadAPIFailed, Message: "File upload failed due to an internal error."},
		ErrKeyMetadataFetchFailed:   {Key: ErrKeyMetadataFetchFailed, Message: "Failed to fetch metadata."},
		ErrKeyPinFetchFailed:        {Key: ErrKeyPinFetchFailed, Message: "Failed to fetch pin."},
		ErrKeyInvalidUUIDFormat:     {Key: ErrKeyInvalidUUIDFormat, Message: "Invalid UUID format provided: %s"},
		ErrKeyFileProcessingFailed:  {Key: ErrKeyFileProcessingFailed, Message: "Failed to process the file."},
		ErrKeyCIDParseFailed:        {Key: ErrKeyCIDParseFailed, Message: "Failed to parse CID."},
		ErrKeyBlockNotFound:         {Key: ErrKeyBlockNotFound, Message: "Block not found."},
		ErrKeyUploadNotFound:        {Key: ErrKeyUploadNotFound, Message: "Upload not found."},
		ErrKeyUnauthorized:          {Key: ErrKeyUnauthorized, Message: "Access denied. Please check your credentials and try again."},
		ErrKeyDownloadQuotaExceeded: {Key: ErrKeyDownloadQuotaExceeded, Message: "Download quota exceeded. Please try again later."},
		ErrKeyInvalidRequest:        {Key: ErrKeyInvalidRequest, Message: "Invalid request parameter: %s"},
		ErrKeyUnsupportedFormat:     {Key: ErrKeyUnsupportedFormat, Message: "Unsupported file format. Supported formats: CAR, ZIP"},
		ErrKeyCorruptedFile:         {Key: ErrKeyCorruptedFile, Message: "Corrupted or invalid file format"},
		ErrKeyEmptyZIP:              {Key: ErrKeyEmptyZIP, Message: "Empty ZIP file cannot be converted"},
		ErrKeyPasswordProtected:     {Key: ErrKeyPasswordProtected, Message: "Password-protected ZIP files are not supported"},
	})

	core.MustRegisterErrorCodes(Namespace, map[core.ErrorType]int{
		ErrKeyFileUploadAPIFailed:   http.StatusInternalServerError,
		ErrKeyMetadataFetchFailed:   http.StatusInternalServerError,
		ErrKeyPinFetchFailed:        http.StatusInternalServerError,
		ErrKeyInvalidUUIDFormat:     http.StatusBadRequest,
		ErrKeyFileProcessingFailed:  http.StatusInternalServerError,
		ErrKeyCIDParseFailed:        http.StatusBadRequest,
		ErrKeyBlockNotFound:         http.StatusNotFound,
		ErrKeyUploadNotFound:        http.StatusNotFound,
		ErrKeyUnauthorized:          http.StatusUnauthorized,
		ErrKeyDownloadQuotaExceeded: http.StatusTooManyRequests,
	})
}

func NewError(key core.ErrorType, err error, args ...any) *IPFSError {
	return &IPFSError{core.NewError(Namespace, key, err, args...)}
}
