package db

import "errors"

// Package-level errors for database operations
var (
	ErrDuplicateFilePath = errors.New("file path already exists")
)
