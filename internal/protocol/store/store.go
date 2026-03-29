package store

import (
	"github.com/ipfs/go-cid"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
)

func cidKey(c cid.Cid) string {
	return encoding.ToV1(c).String()
}
