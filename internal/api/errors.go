package api

import (
	"net/http"

	swagger "go.lumeweb.com/gswagger"
	"go.lumeweb.com/portal-plugin-ipfs/internal/errors"
	router "go.lumeweb.com/portal-router"
	core "go.lumeweb.com/portal/core"
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
	// ErrKeyCIDNotPinned is returned when a website update targets a CID that
	// exists but is not pinned in the user's account. It is user-correctable
	// (pin the CID first), so it surfaces as 422 rather than a generic 500.
	ErrKeyCIDNotPinned core.ErrorType = "CID_NOT_PINNED"
	// ErrKeyIPNSKeyNotFound is returned when an IPNS target references a key
	// that does not exist or is not owned by the requesting user (422).
	ErrKeyIPNSKeyNotFound core.ErrorType = "IPNS_KEY_NOT_FOUND"
	// ErrKeyDNSValidationFailed is returned when website DNS validation cannot
	// be completed because a required DNS record is missing or unresolvable
	// (e.g. the verification TXT record is not yet published). User-correctable,
	// so it surfaces as 422 rather than a generic 500.
	ErrKeyDNSValidationFailed core.ErrorType = "DNS_VALIDATION_FAILED"

	// Website domain binding error types
	ErrKeyDomainNotFound core.ErrorType = "DOMAIN_NOT_FOUND"
	// ErrKeyDomainInUse is returned when a website update tries to set a
	// primary domain that is already live-bound to a different website. The
	// create-only CreateDomain path would otherwise surface a raw MySQL 1062
	// duplicate-key as a 500; this surfaces it as an explicit ownership
	// conflict (409) instead.
	ErrKeyDomainInUse   core.ErrorType = "DOMAIN_IN_USE"
	ErrKeyInvalidPathID core.ErrorType = "INVALID_PATH_ID"
	// ErrKeyNoStoredCertificate is returned when a DANE republish is requested
	// for a domain that has no certificate/key stored (e.g. none was ever
	// pushed via the cert webhook, or the binding is not DANE-capable).
	ErrKeyNoStoredCertificate core.ErrorType = "NO_STORED_CERTIFICATE"
)

// HTTP status classes (RFC 9110 / RFC 4918) — keep these consistent:
//   - 400 Bad Request: malformed / unparseable request (e.g. a non-numeric
//     path param, invalid JSON). Use ErrKeyInvalidPathID / DTO bind failures.
//   - 422 Unprocessable Entity: request parses but fails semantic validation
//     (empty field, bad field combination, invalid CID/TTL/domain/record
//     value). Use the ErrKeyInvalid* validation keys / httputil.ValidationError.
//
// The ipfs-sdk swagger documents these statuses via portal-router's
// DefaultCoreErrorResponses (400, 404, 422, 500) + auth (401, 403).

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
		ErrKeyInvalidRecordName:     {Key: ErrKeyInvalidRecordName, Message: "Invalid DNS record name: %v"},
		ErrKeyPermissionDenied:      {Key: ErrKeyPermissionDenied, Message: "Permission denied"},
		ErrKeyInvalidCID:            {Key: ErrKeyInvalidCID, Message: "Invalid CID provided"},
		ErrKeyInvalidTarget:         {Key: ErrKeyInvalidTarget, Message: "Invalid target hash or peer ID provided"},
		ErrKeyCIDNotPinned:          {Key: ErrKeyCIDNotPinned, Message: "CID is not pinned. Please pin the CID and try again."},
		ErrKeyIPNSKeyNotFound:       {Key: ErrKeyIPNSKeyNotFound, Message: "IPNS key not found or not owned by your account."},
		ErrKeyDNSValidationFailed:   {Key: ErrKeyDNSValidationFailed, Message: "DNS validation could not be completed. Please ensure the required DNS records are published and reachable."},
		ErrKeyDomainNotFound:        {Key: ErrKeyDomainNotFound, Message: "Domain not found"},
		ErrKeyDomainInUse:           {Key: ErrKeyDomainInUse, Message: "Domain is already in use by another website"},
		ErrKeyInvalidPathID:         {Key: ErrKeyInvalidPathID, Message: "Invalid path parameter: %s"},
		ErrKeyNoStoredCertificate:   {Key: ErrKeyNoStoredCertificate, Message: "No stored certificate for domain; nothing to republish"},
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
		ErrKeyCIDNotPinned:          http.StatusUnprocessableEntity,
		ErrKeyIPNSKeyNotFound:       http.StatusUnprocessableEntity,
		ErrKeyDNSValidationFailed:   http.StatusUnprocessableEntity,
		ErrKeyPermissionDenied:      http.StatusForbidden,
		ErrKeyDomainNotFound:        http.StatusNotFound,
		ErrKeyDomainInUse:           http.StatusConflict,
		ErrKeyInvalidPathID:         http.StatusBadRequest,
		ErrKeyNoStoredCertificate:   http.StatusConflict,
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
