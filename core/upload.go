package core

import (
	"context"
	"github.com/ipfs/go-cid"
	"go.lumeweb.com/portal/core"
	"io"
)

const UPLOAD_SERVICE = "ipfs.upload"

type UploadService interface {
	// HandleUpload processes an upload stream and creates pins for the extracted CIDs
	HandleUpload(ctx context.Context, reader io.ReadSeekCloser, userId uint) (cid.Cid, string, error)

	// ProcessCIDs processes a list of CIDs and creates upload and pin records for a user
	ProcessCIDs(ctx context.Context, cids []cid.Cid, userId uint) error

	core.Service
}
