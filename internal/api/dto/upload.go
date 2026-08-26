package dto

import (
	"github.com/Oudwins/zog"
	"go.lumeweb.com/httputil"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/upload"
)

var _ httputil.DTOResponse[*PostUploadResponse] = (*PostUploadResponse)(nil)
var _ httputil.DTOValidator = (*UploadRequest)(nil)
var _ httputil.DTORequest[*UploadRequest] = (*UploadRequest)(nil)

// UploadRequest represents the query parameters for upload requests
type UploadRequest struct {
	ArchiveMode string `query:"archive_mode"`
	// Name is an optional custom pin name for the uploaded content. It is
	// independent of the uploaded file's filename and only used to name the
	// resulting pin when present.
	Name string `query:"name"`
}

func (u *UploadRequest) ToModel() (*UploadRequest, error) {
	return nil, nil
}

// Schema implements httputil.DTOValidator
func (u *UploadRequest) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"ArchiveMode": zog.String().OneOf([]string{upload.ArchiveConvert.String(), upload.ArchivePreserve.String()}).Optional(),
		"Name":        zog.String().Max(pluginDb.MaxPinNameLength).Optional(),
	})
}

// GetArchiveMode returns the archive mode with default value
func (u *UploadRequest) GetArchiveMode() string {
	if u.ArchiveMode == "" {
		return "convert"
	}
	return u.ArchiveMode
}

type PostUploadResponse struct {
	CID string `json:"CID"`
}

func (p *PostUploadResponse) FromModel(_ *PostUploadResponse) error {
	return nil
}
