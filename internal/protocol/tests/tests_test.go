package tests

import (
	"runtime"
	"testing"

	"github.com/ipfs/go-cid"
	"go.lumeweb.com/portal/db/models"
	"gorm.io/gorm"
)

var (
	_, currentFile, _, _ = runtime.Caller(0)
	carFileName          = "../../testing/fixtures/cars/bbb.car"
)

func createTestRequest(t *testing.T, cid cid.Cid, userID *uint) *models.Request {
	req := &models.Request{
		Model:  gorm.Model{ID: 1},
		Status: models.RequestStatusProcessing,
	}

	req.Hash = cid.Hash()

	if userID != nil {
		req.UserID = userID
	}

	return req
}

func uintPtr(i uint) *uint {
	return &i
}
