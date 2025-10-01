package core

import (
	"context"
	"github.com/ipfs/go-cid"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/core"
	"io"
)

const UPLOAD_SERVICE = "ipfs.upload"

type UploadService interface {
	// HandleUpload processes an upload stream and extracts root CIDs from CAR files.
	// It stores the upload as temporary storage and returns the root CID and upload ID.
	// This method does NOT create any pin records - pins are created by ProcessUpload and CreateRootPin.
	HandleUpload(ctx context.Context, reader io.ReadSeekCloser, userId uint) (cid.Cid, string, error)

	// ProcessUpload processes a list of CIDs and creates upload and core pin records for a user.
	// It creates upload records and core pin records for ALL provided CIDs (both roots and children),
	// but does NOT create any IPFS pin records.
	ProcessUpload(ctx context.Context, cids []cid.Cid, userId uint) error

	// CreateRootPin creates an IPFS pin record for a single root CID.
	// This method should only be called for actual root CIDs, not child blocks.
	// Returns the created IPFS pin record which contains the request ID for tracking.
	CreateRootPin(ctx context.Context, cid cid.Cid, userId uint) (*db.IPFSPin, error)

	core.Service
}
