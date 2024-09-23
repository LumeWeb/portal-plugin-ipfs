package api

import (
	"fmt"
	"net/http"
)

// S5-specific error keys
const (
	// File-related errors
	ErrKeyFileUploadFailed     = "ErrFileUploadFailed"
	ErrKeyFileDownloadFailed   = "ErrFileDownloadFailed"
	ErrKeyMetadataFetchFailed  = "ErrMetadataFetchFailed"
	ErrKeyInvalidFileFormat    = "ErrInvalidFileFormat"
	ErrKeyUnsupportedFileType  = "ErrUnsupportedFileType"
	ErrKeyFileProcessingFailed = "ErrFileProcessingFailed"
)

// Default error messages for S5-specific errors
var defaultErrorMessages = map[string]string{
	ErrKeyFileUploadFailed:     "File ipfsUpload failed due to an internal error.",
	ErrKeyFileDownloadFailed:   "File download failed.",
	ErrKeyMetadataFetchFailed:  "Failed to fetch metadata for the resource.",
	ErrKeyInvalidFileFormat:    "Invalid file format provided.",
	ErrKeyUnsupportedFileType:  "Unsupported file type.",
	ErrKeyFileProcessingFailed: "Failed to process the file.",
}

// Mapping of S5-specific error keys to HTTP status codes
var errorCodeToHttpStatus = map[string]int{
	ErrKeyFileUploadFailed:     http.StatusInternalServerError,
	ErrKeyFileDownloadFailed:   http.StatusInternalServerError,
	ErrKeyMetadataFetchFailed:  http.StatusInternalServerError,
	ErrKeyInvalidFileFormat:    http.StatusBadRequest,
	ErrKeyUnsupportedFileType:  http.StatusBadRequest,
	ErrKeyFileProcessingFailed: http.StatusInternalServerError,
}

// Error struct for representing S5-specific errors
type Error struct {
	Key     string
	Message string
	Err     error
}

// Error method to implement the error interface
func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

func (e *Error) HttpStatus() int {
	if code, exists := errorCodeToHttpStatus[e.Key]; exists {
		return code
	}
	return http.StatusInternalServerError
}

func NewError(key string, err error, customMessage ...string) *Error {
	message, exists := defaultErrorMessages[key]
	if !exists {
		message = "An unknown error occurred"
	}
	if len(customMessage) > 0 {
		message = customMessage[0]
	}

	return &Error{
		Key:     key,
		Message: message,
		Err:     err,
	}
}
