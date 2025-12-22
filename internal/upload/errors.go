package upload

import (
	"fmt"

	"go.lumeweb.com/portal-plugin-ipfs/internal/errors"
)

// UploadError represents specific upload-related errors with predefined error types
type UploadError struct {
	Type    string
	Message string
	Err     error
}

func (e *UploadError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

func (e *UploadError) Unwrap() error {
	return e.Err
}

// Error type constructors
func NewUnsupportedFormatError(err error) *UploadError {
	return &UploadError{
		Type:    errors.UploadErrUnsupportedFormat,
		Message: "Unsupported file format. Supported formats: CAR, ZIP",
		Err:     err,
	}
}

func NewCorruptedFileError(err error) *UploadError {
	return &UploadError{
		Type:    errors.UploadErrCorruptedFile,
		Message: "Corrupted or invalid file format",
		Err:     err,
	}
}

func NewEmptyZIPError(err error) *UploadError {
	return &UploadError{
		Type:    errors.UploadErrEmptyZIP,
		Message: "Empty ZIP file cannot be converted",
		Err:     err,
	}
}

func NewPasswordProtectedError(err error) *UploadError {
	return &UploadError{
		Type:    errors.UploadErrPasswordProtected,
		Message: "Password-protected ZIP files are not supported",
		Err:     err,
	}
}

func NewInvalidPathError(path string, err error) *UploadError {
	return &UploadError{
		Type:    errors.UploadErrCorruptedFile,
		Message: fmt.Sprintf("Invalid file path in ZIP: %s", path),
		Err:     err,
	}
}

func NewExtractionError(err error) *UploadError {
	return &UploadError{
		Type:    errors.UploadErrCorruptedFile,
		Message: "Failed to extract ZIP contents",
		Err:     err,
	}
}

func NewProcessorError(format string, mode string, err error) *UploadError {
	return &UploadError{
		Type:    errors.UploadErrFileUploadFailed,
		Message: fmt.Sprintf("Failed to create processor for format %s with mode %s", format, mode),
		Err:     err,
	}
}

func NewProcessingError(err error) *UploadError {
	return &UploadError{
		Type:    errors.UploadErrFileUploadFailed,
		Message: "Failed to process upload",
		Err:     err,
	}
}

// Helper function to check if an error is a specific upload error type
func IsUploadErrorType(err error, errorType string) bool {
	if err == nil {
		return false
	}

	// Check if it's an UploadError with matching type
	if uploadErr, ok := err.(*UploadError); ok {
		return uploadErr.Type == errorType
	}

	return false
}

// Helper function to extract error type from an upload error
func GetUploadErrorType(err error) string {
	if err == nil {
		return ""
	}

	// Check if it's an UploadError
	if uploadErr, ok := err.(*UploadError); ok {
		return uploadErr.Type
	}

	return ""
}
