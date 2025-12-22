package errors

import "go.lumeweb.com/portal/core"

// MapUploadErrorType maps upload error type strings to API error types
func MapUploadErrorType(uploadErrorType string) core.ErrorType {
	switch uploadErrorType {
	case UploadErrUnsupportedFormat:
		return ErrKeyUnsupportedFormat
	case UploadErrCorruptedFile:
		return ErrKeyCorruptedFile
	case UploadErrEmptyZIP:
		return ErrKeyEmptyZIP
	case UploadErrPasswordProtected:
		return ErrKeyPasswordProtected
	case UploadErrFileUploadFailed:
		return ErrKeyFileUploadFailed
	default:
		return ErrKeyFileUploadFailed
	}
}
