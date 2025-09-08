package dto

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/Oudwins/zog"
	"go.lumeweb.com/httputil"
	"go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal/config"
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
	CIDs   []string             `query:"cid"`
	Name   string               `query:"name"`
	Match  TextMatchingStrategy `query:"match"`
	Status []db.PinningStatus   `query:"status"`
	Before *time.Time           `query:"before"`
	After  *time.Time           `query:"after"`
	Meta   map[string]string    `query:"meta"`
	Limit  int                  `query:"limit"`
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
		// TODO: Disabling validation at zog until we can handle preprocessing or creating a custom schema type
		/*		"Status": zog.Slice(zog.StringLike[db.PinningStatus]().OneOf([]db.PinningStatus{
				db.PinningStatusQueued,
				db.PinningStatusPinning,
				db.PinningStatusPinned,
				db.PinningStatusFailed,
			})).Optional(),*/
		"Before": zog.Ptr(zog.Time().Optional()),
		"After":  zog.Ptr(zog.Time().Optional()),
		"Meta":   zog.Ptr(zog.Struct(zog.Shape{})),
		"Limit":  zog.Int().GTE(1).LTE(1000).Default(10).Optional(),
	})
}

func (I IPFSPinFilter) ToModel() (IPFSPinFilter, error) {
	return I, nil
}

// PostProcessStatuses handles comma-separated status values in the filter
func (I *IPFSPinFilter) PostProcessStatuses() error {
	if len(I.Status) == 0 {
		return nil
	}

	// Process all status elements, split on commas, trim whitespace and convert to lowercase
	processedStatuses := make(map[db.PinningStatus]bool)

	for _, status := range I.Status {
		statusStr := string(status)
		if strings.Contains(statusStr, ",") {
			// Split comma-separated values
			parts := strings.Split(statusStr, ",")
			for _, part := range parts {
				trimmed := strings.TrimSpace(part)
				if trimmed != "" {
					// Convert to lowercase for normalization
					normalized := db.PinningStatus(strings.ToLower(trimmed))
					processedStatuses[normalized] = true
				}
			}
		} else {
			trimmed := strings.TrimSpace(statusStr)
			if trimmed != "" {
				// Convert to lowercase for normalization
				normalized := db.PinningStatus(strings.ToLower(trimmed))
				processedStatuses[normalized] = true
			}
		}
	}

	// Rebuild I.Status from the map to remove duplicates
	// Preserve stable order by sorting the keys
	statusKeys := make([]db.PinningStatus, 0, len(processedStatuses))
	for status := range processedStatuses {
		statusKeys = append(statusKeys, status)
	}

	// Sort to maintain stable order
	slices.Sort(statusKeys)

	I.Status = statusKeys

	// Validate all statuses
	validStatuses := map[db.PinningStatus]bool{
		db.PinningStatusQueued:  true,
		db.PinningStatusPinning: true,
		db.PinningStatusPinned:  true,
		db.PinningStatusFailed:  true,
	}

	for _, status := range I.Status {
		if !validStatuses[status] {
			return fmt.Errorf("invalid status: %s", status)
		}
	}

	return nil
}
