package config

import (
	"errors"
	"github.com/go-viper/mapstructure/v2"
	"github.com/libp2p/go-libp2p/core/peer"
	"gopkg.in/yaml.v3"
)

var (
	_ yaml.Marshaler           = (*IPFSPeer)(nil)
	_ mapstructure.Unmarshaler = (*IPFSPeer)(nil)
)

type IPFSPeer struct {
	peer.AddrInfo
}

func (pi IPFSPeer) MarshalYAML() (interface{}, error) {
	addrs, err := peer.AddrInfoToP2pAddrs(&pi.AddrInfo)
	if err != nil {
		return nil, err
	}

	if len(addrs) == 0 {
		return nil, errors.New("no addresses")
	}

	return addrs[0].String(), nil
}

func (pi *IPFSPeer) DecodeMapstructure(value interface{}) error {
	if _, ok := value.(string); !ok {
		return errors.New("peer must be a string")
	}

	addr, err := peer.AddrInfoFromString(value.(string))
	if err != nil {
		return err
	}

	pi.AddrInfo = *addr

	return nil
}
