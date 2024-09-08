package tasks

import (
	"github.com/ipfs/go-cid"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.lumeweb.com/portal-plugin-ipfs/internal/cron/define"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol"
	"go.lumeweb.com/portal-plugin-ipfs/internal/protocol/encoding"
	"go.lumeweb.com/portal/core"
)

func CronTaskUnixFSUpdateMetadata(args *define.CronTaskUnixFSUpdateMetadataArgs, _ core.Context) error {
	proto := core.GetProtocol(internal.ProtocolName).(*protocol.Protocol)

	decodedCID, err := cid.Decode(args.CID)
	if err != nil {
		return err
	}

	decodedCID = encoding.NormalizeCid(decodedCID)

	err = proto.GetMetadataStore().UpdateUnixFSMetadata(decodedCID, &pluginDb.UnixFSNode{Name: args.Name})
	if err != nil {
		return err
	}

	return nil
}
