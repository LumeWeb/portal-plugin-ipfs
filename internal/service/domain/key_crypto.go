package domain

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"

	pluginConfig "go.lumeweb.com/portal-plugin-ipfs/internal/config"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal/core"
)

// daneEncryptionKey extracts and parses the configured base64 AES-256 key.
func (s *DelegatedDomainService) daneEncryptionKey() ([]byte, error) {
	var raw string
	if s.BaseComponent != nil {
		dnsCfg := core.GetServiceConfig[*pluginConfig.DnsConfig](s.Context(), pluginCore.DNS_SERVICE)
		if dnsCfg != nil {
			raw = dnsCfg.DANEKeyEncryptionKey
		}
	}
	if raw == "" {
		return nil, errors.New("dane key encryption key not configured")
	}
	key, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("dane key encryption key is not valid base64: %w", err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("dane key encryption key must decode to 32 bytes, got %d", len(key))
	}
	return key, nil
}

// encryptPrivateKey encrypts a PEM-encoded private key at rest with AES-256-GCM.
// The output is base64(nonce || ciphertext), self-contained for DB storage.
func (s *DelegatedDomainService) encryptPrivateKey(ctx context.Context, keyPEM string) (string, error) {
	key, err := s.daneEncryptionKey()
	if err != nil {
		return "", err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("create aes cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("create gcm: %w", err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("generate nonce: %w", err)
	}
	sealed := gcm.Seal(nonce, nonce, []byte(keyPEM), nil)
	return base64.StdEncoding.EncodeToString(sealed), nil
}

// decryptPrivateKey reverses encryptPrivateKey, returning the original PEM.
func (s *DelegatedDomainService) decryptPrivateKey(ctx context.Context, enc string) (string, error) {
	key, err := s.daneEncryptionKey()
	if err != nil {
		return "", err
	}
	sealed, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		return "", fmt.Errorf("decoded ciphertext is not valid base64: %w", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("create aes cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("create gcm: %w", err)
	}
	if len(sealed) < gcm.NonceSize() {
		return "", errors.New("ciphertext too short")
	}
	nonce, ciphertext := sealed[:gcm.NonceSize()], sealed[gcm.NonceSize():]
	plain, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("decrypt key (is the DANE key encryption key the same that encrypted it?): %w", err)
	}
	return string(plain), nil
}
