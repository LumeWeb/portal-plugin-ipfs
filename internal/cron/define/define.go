package define

import "github.com/google/uuid"

const CronTaskPinName = "IPFSPin"

type CronTaskPinArgs struct {
	RequestID uuid.UUID
}

func CronTaskPinArgsFactory() any {
	return &CronTaskPinArgs{}
}

const CronTaskTusUploadName = "IPFSTusUpload"

type CronTaskTusUploadArgs struct {
	UploadID string
}

func CronTaskTusUploadArgsFactory() any {
	return &CronTaskTusUploadArgs{}
}

const CronTaskPostUploadName = "IPFSPostUpload"

type CronTaskPostUploadArgs struct {
	RequestID uint
	UploadID  string
}

func CronTaskPostUploadArgsFactory() any {
	return &CronTaskPostUploadArgs{}
}

const CronTaskPostUploadCleanupName = "IPFSPostUploadCleanup"

type CronTaskPostUploadCleanupArgs struct {
	RequestID uint
	UploadID  string
}

func CronTaskPostUploadCleanupArgsFactory() any {
	return &CronTaskPostUploadCleanupArgs{}
}

const CronTaskTusUploadCleanupName = "IPFSTusUploadCleanup"

type CronTaskTusUploadCleanupArgs struct {
	UploadID string
}

func CronTaskTusUploadCleanupArgsFactory() any {
	return &CronTaskTusUploadCleanupArgs{}
}
