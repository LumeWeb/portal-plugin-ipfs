package dto

import "go.lumeweb.com/httputil"

var _ httputil.DTOResponse[*PostUploadResponse] = (*PostUploadResponse)(nil)

type PostUploadResponse struct {
	CID string
}

func (p *PostUploadResponse) FromModel(_ *PostUploadResponse) error {
	return nil
}
