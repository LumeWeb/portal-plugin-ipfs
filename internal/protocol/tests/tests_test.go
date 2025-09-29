package tests

import (
	"runtime"
	"testing"

	"github.com/ipfs/go-cid"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal/db/models"
)

var (
	_, currentFile, _, _ = runtime.Caller(0)
	carFileName          = "../../testing/fixtures/cars/bbb.car"
)

func createTestRequest(t *testing.T, cid cid.Cid, userID *uint) *models.Request {
	hashBytes := cid.Hash()

	req := &models.Request{
		Operation: protocol.FilePathOperationName(),
		Protocol:  internal.ProtocolName,
		Status:    models.RequestStatusPending,
		Hash:      hashBytes,
	}
	if userID != nil {
		req.UserID = userID
	}
	return req
}

func uintPtr(i uint) *uint {
	return &i
}
