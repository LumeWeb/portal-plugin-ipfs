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
)
