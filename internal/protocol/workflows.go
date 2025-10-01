package protocol

import (
	"github.com/google/uuid"
)

const (
	PIN_WORKFLOW             = "ipfs.network.pin"
	PIN_CHILD_BLOCK_WORKFLOW = "ipfs.network.pin.children"
	UPLOAD_WORKFLOW          = "ipfs.upload"
	TUS_UPLOAD_WORKFLOW      = "ipfs.tus.upload"
	FILE_PATH_WORKFLOW       = "ipfs.file.path"
)

type PinWorkflowData struct {
	PinRequestID uuid.UUID `json:"pin_request_id"`
	Cids         []string  `json:"cids"`
}

type PinChildBlockWorkflowData struct {
	Cid string `json:"cid"`
}
type PostUploadWorkflowData struct {
	UploadID string `json:"upload_id"`
}
