package protocol

import (
	"github.com/google/uuid"
)

const (
	PIN_WORKFLOW        = "ipfs.network.pin"
	UPLOAD_WORKFLOW     = "ipfs.upload"
	TUS_UPLOAD_WORKFLOW = "ipfs.tus.upload"
	FILE_PATH_WORKFLOW  = "ipfs.file.path"
)

type PinWorkflowData struct {
	PinRequestID uuid.UUID `json:"pin_request_id"`
	Cids         []string  `json:"cids,omitempty"`
}

type PostUploadWorkflowData struct {
	UploadID string `json:"upload_id"`
}

type FilePathWorkflowInputData struct {
	CIDs        []string `json:"cids"`
	RelatedCIDs []string `json:"related_cids,omitempty"`
	UserID      uint     `json:"user_id"`
}
