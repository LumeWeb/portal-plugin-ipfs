package errors

import (
	"go.lumeweb.com/portal/core"
)

// Upload error type constants for use by upload package
const (
	UploadErrUnsupportedFormat = "ErrUnsupportedFormat"
	UploadErrCorruptedFile     = "ErrCorruptedFile"
	UploadErrEmptyZIP          = "ErrEmptyZIP"
	UploadErrPasswordProtected = "ErrPasswordProtected"
	UploadErrFileUploadFailed  = "ErrFileUploadFailed"
)

// API error type constants
const (
	ErrKeyUnsupportedFormat core.ErrorType = "ErrUnsupportedFormat"
	ErrKeyCorruptedFile     core.ErrorType = "ErrCorruptedFile"
	ErrKeyEmptyZIP          core.ErrorType = "ErrEmptyZIP"
	ErrKeyPasswordProtected core.ErrorType = "ErrPasswordProtected"
	ErrKeyFileUploadFailed  core.ErrorType = "ErrFileUploadFailed"

	// Website error types
	ErrInvalidWebsiteStatus   core.ErrorType = "INVALID_WEBSITE_STATUS"
	ErrInvalidTargetType      core.ErrorType = "INVALID_TARGET_TYPE"
	ErrInvalidSSLStatus       core.ErrorType = "INVALID_SSL_STATUS"
	ErrSSLStatusUpdateFailed  core.ErrorType = "SSL_STATUS_UPDATE_FAILED"
	ErrInvalidDomain          core.ErrorType = "INVALID_DOMAIN"
	ErrInvalidZoneStatus      core.ErrorType = "INVALID_ZONE_STATUS"
	ErrWebsiteNotFound        core.ErrorType = "WEBSITE_NOT_FOUND"
	ErrInvalidTimestamp       core.ErrorType = "INVALID_TIMESTAMP"
)
