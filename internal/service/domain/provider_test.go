package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.lumeweb.com/portal-plugin-ipfs/internal/testing/mocks"
)


func TestRegistry_RegisterAndGet(t *testing.T) {
	r := NewRegistry()
	mockProv := mocks.NewMockDomainProvider(t)
	mockProv.EXPECT().Protocol().Return("test").Maybe()

	r.Register(mockProv)
	assert.Equal(t, mockProv, r.Get("test"))
	assert.Nil(t, r.Get("missing"))
	assert.Equal(t, []string{"test"}, r.Names())
}

func TestRegistry_DuplicatePanics(t *testing.T) {
	r := NewRegistry()
	mockProv := mocks.NewMockDomainProvider(t)
	mockProv.EXPECT().Protocol().Return("test").Maybe()

	r.Register(mockProv)
	assert.Panics(t, func() {
		mockProv2 := mocks.NewMockDomainProvider(t)
		mockProv2.EXPECT().Protocol().Return("test").Maybe()
		r.Register(mockProv2)
	})
}

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
			got := NormalizeDomain(tt.input)
			assert.Equal(t, tt.expected, got)
		})
	}
}
