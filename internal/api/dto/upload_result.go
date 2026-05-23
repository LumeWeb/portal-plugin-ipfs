package dto

import (
	"go.lumeweb.com/httputil"
	"go.lumeweb.com/portal/db/models"
)

var _ httputil.DTOResponse[*UploadResultResponse] = (*UploadResultResponse)(nil)

type UploadResultResponse struct {
	CID    string                   `json:"cid,omitempty"`
	Status models.RequestStatusType `json:"status"`
	Error  string                   `json:"error,omitempty"`
}

func (u *UploadResultResponse) FromModel(_ *UploadResultResponse) error {
	return nil
}
