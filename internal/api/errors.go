package api

import (
	"net/http"

	router "go.lumeweb.com/portal-router"
	swagger "go.lumeweb.com/gswagger"
	core "go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal/errors"
)

// Error keys
const (
	Namespace = "ipfs"

	// Re-export error type constants from internal/errors
	ErrKeyUnsupportedFormat = errors.ErrKeyUnsupportedFormat
	ErrKeyCorruptedFile     = errors.ErrKeyCorruptedFile
	ErrKeyEmptyZIP          = errors.ErrKeyEmptyZIP
	ErrKeyPasswordProtected = errors.ErrKeyPasswordProtected
	ErrKeyFileUploadFailed  = errors.ErrKeyFileUploadFailed

	// Other API error types
	ErrKeyFileUploadAPIFailed   core.ErrorType = "FILE_UPLOAD_API_FAILED"
	ErrKeyMetadataFetchFailed   core.ErrorType = "METADATA_FETCH_FAILED"
	ErrKeyPinFetchFailed        core.ErrorType = "PIN_FETCH_FAILED"
	ErrKeyInvalidUUIDFormat     core.ErrorType = "INVALID_UUID_FORMAT"
	ErrKeyFileProcessingFailed  core.ErrorType = "FILE_PROCESSING_FAILED"
	ErrKeyCIDParseFailed        core.ErrorType = "CID_PARSE_FAILED"
	ErrKeyBlockNotFound         core.ErrorType = "BLOCK_NOT_FOUND"
	ErrKeyUploadNotFound        core.ErrorType = "UPLOAD_NOT_FOUND"
	ErrKeyUnauthorized          core.ErrorType = "UNAUTHORIZED"
	ErrKeyUploadQuotaExceeded   core.ErrorType = "UPLOAD_QUOTA_EXCEEDED"
	ErrKeyStorageQuotaExceeded  core.ErrorType = "STORAGE_QUOTA_EXCEEDED"
	ErrKeyDownloadQuotaExceeded core.ErrorType = "DOWNLOAD_QUOTA_EXCEEDED"
	ErrKeyInvalidRequest        core.ErrorType = "INVALID_REQUEST"
	ErrKeyInvalidIdentifier     core.ErrorType = "INVALID_IDENTIFIER"
	ErrKeyPermissionDenied      core.ErrorType = "PERMISSION_DENIED"
	ErrKeyDeleteFailed          core.ErrorType = "DELETE_FAILED"
	ErrKeyUpdateFailed          core.ErrorType = "UPDATE_FAILED"

	// DNS error types
	ErrKeyInvalidRecordType   core.ErrorType = "INVALID_RECORD_TYPE"
	ErrKeyRecordNotFound      core.ErrorType = "RECORD_NOT_FOUND"
	ErrKeyZoneNotFound        core.ErrorType = "ZONE_NOT_FOUND"
	ErrKeyInvalidDomainFormat core.ErrorType = "INVALID_DOMAIN_FORMAT"
	ErrKeyDuplicateRecord     core.ErrorType = "DUPLICATE_RECORD"
	ErrKeyValidationFailed    core.ErrorType = "VALIDATION_FAILED"
	ErrKeyInvalidRecordName   core.ErrorType = "INVALID_RECORD_NAME"

	// Website validation error types
	ErrKeyInvalidCID    core.ErrorType = "INVALID_CID"
	ErrKeyInvalidTarget core.ErrorType = "INVALID_TARGET"
)

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
		ErrKeyUploadQuotaExceeded:   {Key: ErrKeyUploadQuotaExceeded, Message: "Upload quota exceeded. Please try again later."},
		ErrKeyStorageQuotaExceeded:  {Key: ErrKeyStorageQuotaExceeded, Message: "Storage quota exceeded. Please try again later."},
		ErrKeyDownloadQuotaExceeded: {Key: ErrKeyDownloadQuotaExceeded, Message: "Download quota exceeded. Please try again later."},
		ErrKeyInvalidRequest:        {Key: ErrKeyInvalidRequest, Message: "Invalid request parameter: %s"},
		ErrKeyInvalidIdentifier:     {Key: ErrKeyInvalidIdentifier, Message: "Invalid identifier format"},
		ErrKeyUnsupportedFormat:     {Key: ErrKeyUnsupportedFormat, Message: "Unsupported file format. Supported formats: CAR, ZIP"},
		ErrKeyCorruptedFile:         {Key: ErrKeyCorruptedFile, Message: "Corrupted or invalid file format"},
		ErrKeyEmptyZIP:              {Key: ErrKeyEmptyZIP, Message: "Empty ZIP file cannot be converted"},
		ErrKeyPasswordProtected:     {Key: ErrKeyPasswordProtected, Message: "Password-protected ZIP files are not supported"},
		ErrKeyFileUploadFailed:      {Key: ErrKeyFileUploadFailed, Message: "Failed to process upload."},
		ErrKeyInvalidRecordType:     {Key: ErrKeyInvalidRecordType, Message: "Invalid DNS record type"},
		ErrKeyRecordNotFound:        {Key: ErrKeyRecordNotFound, Message: "DNS record not found"},
		ErrKeyZoneNotFound:          {Key: ErrKeyZoneNotFound, Message: "DNS zone not found"},
		ErrKeyInvalidDomainFormat:   {Key: ErrKeyInvalidDomainFormat, Message: "Invalid domain format"},
		ErrKeyDuplicateRecord:       {Key: ErrKeyDuplicateRecord, Message: "Duplicate DNS record"},
		ErrKeyValidationFailed:      {Key: ErrKeyValidationFailed, Message: "DNS validation failed"},
		ErrKeyInvalidRecordName:     {Key: ErrKeyInvalidRecordName, Message: "Invalid DNS record name"},
		ErrKeyPermissionDenied:      {Key: ErrKeyPermissionDenied, Message: "Permission denied"},
		ErrKeyInvalidCID:            {Key: ErrKeyInvalidCID, Message: "Invalid CID provided"},
		ErrKeyInvalidTarget:         {Key: ErrKeyInvalidTarget, Message: "Invalid target hash or peer ID provided"},
		ErrKeyDeleteFailed:          {Key: ErrKeyDeleteFailed, Message: "Failed to delete zone"},
		ErrKeyUpdateFailed:          {Key: ErrKeyUpdateFailed, Message: "Failed to update zone"},
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
		ErrKeyFileUploadFailed:      http.StatusInternalServerError,
		ErrKeyUploadQuotaExceeded:   http.StatusTooManyRequests,
		ErrKeyStorageQuotaExceeded:  http.StatusTooManyRequests,
		ErrKeyDownloadQuotaExceeded: http.StatusTooManyRequests,
		ErrKeyInvalidDomainFormat:   http.StatusBadRequest,
		ErrKeyRecordNotFound:        http.StatusNotFound,
		ErrKeyZoneNotFound:          http.StatusNotFound,
		ErrKeyDuplicateRecord:       http.StatusConflict,
		ErrKeyValidationFailed:      http.StatusInternalServerError,
		ErrKeyInvalidRecordName:     http.StatusUnprocessableEntity,
		ErrKeyInvalidRequest:        http.StatusUnprocessableEntity,
		ErrKeyInvalidIdentifier:     http.StatusUnprocessableEntity,
		ErrKeyInvalidCID:            http.StatusUnprocessableEntity,
		ErrKeyInvalidTarget:         http.StatusUnprocessableEntity,
		ErrKeyPermissionDenied:      http.StatusForbidden,
		ErrKeyDeleteFailed:          http.StatusInternalServerError,
		ErrKeyUpdateFailed:          http.StatusInternalServerError,
	})
}

func NewError(key core.ErrorType, err error, args ...any) *core.Error {
	return core.NewError(Namespace, key, err, args...)
}

func DefineErrorResponse(status int, description string) map[int]swagger.ContentValue {
	return router.DefineSwaggerErrorResponse(status, description)
}
