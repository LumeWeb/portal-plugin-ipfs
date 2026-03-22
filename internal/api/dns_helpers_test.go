package api

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.lumeweb.com/portal/core"
	"gorm.io/gorm"
)

func TestMapDNSErrorToAPIError(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		expectedKey  core.ErrorType
	}{
		{
			name:        "nil error",
			err:         nil,
			expectedKey: ErrKeyUpdateFailed,
		},
		{
			name:        "gorm record not found",
			err:         gorm.ErrRecordNotFound,
			expectedKey: ErrKeyZoneNotFound,
		},
		{
			name:        "wrapped gorm error",
			err:         fmt.Errorf("wrapped: %w", gorm.ErrRecordNotFound),
			expectedKey: ErrKeyZoneNotFound,
		},
		{
			name:        "powerdns 409 conflict",
			err:         fmt.Errorf("PowerDNS API returned status 409, body: error"),
			expectedKey: ErrKeyDuplicateRecord,
		},
		{
			name:        "powerdns 404 not found",
			err:         fmt.Errorf("PowerDNS API returned status 404, body: zone not found"),
			expectedKey: ErrKeyZoneNotFound,
		},
		{
			name:        "powerdns 422 validation error",
			err:         fmt.Errorf("PowerDNS API returned status 422, body: validation failed"),
			expectedKey: ErrKeyValidationFailed,
		},
		{
			name:        "internal server error",
			err:         fmt.Errorf("internal error"),
			expectedKey: ErrKeyUpdateFailed,
		},
		{
			name:        "powerdns 500 error",
			err:         fmt.Errorf("PowerDNS API returned status 500, body: server error"),
			expectedKey: ErrKeyUpdateFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mapDNSErrorToAPIError(tt.err)
			assert.Equal(t, tt.expectedKey, result)
		})
	}
}

func TestContainsStatusCode(t *testing.T) {
	tests := []struct {
		name       string
		errMsg     string
		statusCode int
		expected   bool
	}{
		{
			name:       "status 409 in message",
			errMsg:     "PowerDNS API returned status 409, body: conflict",
			statusCode: 409,
			expected:   true,
		},
		{
			name:       "returned 404 in message",
			errMsg:     "API returned 404 not found",
			statusCode: 404,
			expected:   true,
		},
		{
			name:       "HTTP 422 in message",
			errMsg:     "HTTP 422 unprocessable entity",
			statusCode: 422,
			expected:   true,
		},
		{
			name:       "plain 500 in message",
			errMsg:     "error code 500",
			statusCode: 500,
			expected:   true,
		},
		{
			name:       "no status code",
			errMsg:     "general error",
			statusCode: 409,
			expected:   false,
		},
		{
			name:       "different status code",
			errMsg:     "status 404",
			statusCode: 409,
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := containsStatusCode(tt.errMsg, tt.statusCode)
			assert.Equal(t, tt.expected, result)
		})
	}
}
