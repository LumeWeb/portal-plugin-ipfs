package tasks

import (
	"errors"
	"github.com/ipfs/go-cid"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/cron/define"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
	"go.lumeweb.com/portal/core"
	"gorm.io/gorm"
	"time"
)

const retryInterval = 5 * time.Second
const maxRetries = 10

func CronTaskUnixFSUpdateMetadata(args *define.CronTaskUnixFSUpdateMetadataArgs, ctx core.Context) error {
	proto := core.GetProtocol(internal.ProtocolName).(*protocol.Protocol)

	decodedCID, err := cid.Decode(args.CID)
	if err != nil {
		return err
	}

	decodedCID = encoding.NormalizeCid(decodedCID)

	err = waitForRecord(decodedCID, ctx)
	if err != nil {
		return err
	}

	err = proto.GetMetadataStore().UpdateUnixFSMetadata(decodedCID, &pluginDb.UnixFSNode{Name: args.Name})
	if err != nil {
		return err
	}

	return nil
}

func waitForRecord(c cid.Cid, ctx core.Context) error {
	proto := core.GetProtocol(internal.ProtocolName).(*protocol.Protocol)
	c = encoding.NormalizeCid(c)

	for i := 0; i < maxRetries; i++ {
		node, err := proto.GetMetadataStore().GetUnixFSMetadata(c)

		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		if node != nil {
			return nil
		}

		// Wait before retrying
		select {
		case <-time.After(retryInterval):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return errors.New("max retries exceeded")
}
