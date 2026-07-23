package website

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalizeDomain(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"plain domain", "example.com", "example.com"},
		{"www prefix", "www.example.com", "example.com"},
		{"www subdomain", "www.sub.example.com", "sub.example.com"},
		{"uppercase with www", "WWW.Example.COM", "example.com"},
		{"mixed case no www", "Example.COM", "example.com"},
		{"only www", "www.com", "com"},
		{"deep www", "www.www.example.com", "www.example.com"},
		{"leading space", " www.example.com", "example.com"},
		{"trailing space", "www.example.com ", "example.com"},
		{"both spaces", " www.example.com ", "example.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeDomain(tt.input)
			assert.Equal(t, tt.expected, got)
		})
	}
}
