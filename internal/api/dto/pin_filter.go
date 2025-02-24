package dto

import (
	"github.com/Oudwins/zog"
	"go.lumeweb.com/httputil"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/config"
	"time"
)

var _ httputil.DTOValidator = (*IPFSPinFilter)(nil)
var _ httputil.DTORequest[IPFSPinFilter] = (*IPFSPinFilter)(nil)

// TextMatchingStrategy defines the text matching strategy for name searches.
type TextMatchingStrategy string

const (
	TextMatchingStrategyExact    TextMatchingStrategy = "exact"
	TextMatchingStrategyIExact   TextMatchingStrategy = "iexact"
	TextMatchingStrategyPartial  TextMatchingStrategy = "partial"
	TextMatchingStrategyIPartial TextMatchingStrategy = "ipartial"
)

// IPFSPinFilter encapsulates the filtering options for listing pin jobs.
type IPFSPinFilter struct {
	CIDs   []string             `json:"cid,omitempty"`
	Name   string               `json:"name,omitempty"`
	Match  TextMatchingStrategy `json:"match,omitempty"`
	Status []db.PinningStatus   `json:"status,omitempty"`
	Before *time.Time           `json:"before,omitempty"`
	After  *time.Time           `json:"after,omitempty"`
	Meta   map[string]string    `json:"meta,omitempty"`
	Limit  int                  `json:"limit,omitempty"`
}

func (I IPFSPinFilter) Schema() *zog.StructSchema {
	return zog.Struct(zog.Shape{
		"CIDs": zog.Slice(zog.String()).Optional(),
		"Name": zog.String().Max(255).Optional(),
		"Match": config.ZogStringLike[TextMatchingStrategy]().OneOf(
			[]TextMatchingStrategy{
				TextMatchingStrategyExact,
				TextMatchingStrategyIExact,
				TextMatchingStrategyPartial,
				TextMatchingStrategyIPartial,
			}),
		"Status": zog.Slice(config.ZogStringLike[db.PinningStatus]().OneOf([]db.PinningStatus{
			db.PinningStatusQueued,
			db.PinningStatusPinning,
			db.PinningStatusPinned,
			db.PinningStatusFailed,
		})).Optional(),
		"Before": zog.Ptr(zog.Time().Optional()),
		"After":  zog.Ptr(zog.Time().Optional()),
		"Meta":   zog.Ptr(zog.Struct(zog.Shape{})),
		"Limit":  zog.Int().GTE(1).LTE(1000).Default(10).Optional(),
	})
}

func (I IPFSPinFilter) ToModel() (IPFSPinFilter, error) {
	return I, nil
}
