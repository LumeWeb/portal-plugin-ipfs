package dto

import "go.lumeweb.com/httputil"

var _ httputil.DTOResponse[*PingModel] = (*PingResponse)(nil)

type PingResponse struct {
	Status string `json:"status"`
}

type PingModel struct {
	Status string
}

func (r *PingResponse) FromModel(m *PingModel) error {
	r.Status = m.Status
	return nil
}
