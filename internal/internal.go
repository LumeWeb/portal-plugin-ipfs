package internal

import (
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/db/models"
)

const ProtocolName = "ipfs"

func RequestStatusToPinStatus(statusType models.RequestStatusType) db.PinningStatus {
	switch statusType {
	case models.RequestStatusPending:
		return db.PinningStatusQueued
	case models.RequestStatusProcessing:
		return db.PinningStatusPinning
	case models.RequestStatusCompleted:
		return db.PinningStatusPinned
	case models.RequestStatusFailed:
		return db.PinningStatusFailed
	default:
		return db.PinningStatusQueued
	}
}

func PinStatusToRequestStatus(status db.PinningStatus) models.RequestStatusType {
	switch status {
	case db.PinningStatusQueued:
		return models.RequestStatusPending
	case db.PinningStatusPinning:
		return models.RequestStatusProcessing
	case db.PinningStatusPinned:
		return models.RequestStatusCompleted
	case db.PinningStatusFailed:
		return models.RequestStatusFailed
	default:
		return models.RequestStatusPending
	}
}
