package config

import (
	"errors"
	"github.com/go-viper/mapstructure/v2"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/mitchellh/copystructure"
	"gopkg.in/yaml.v3"
	"reflect"
)

var (
	_ yaml.Marshaler           = (*IPFSPeer)(nil)
	_ mapstructure.Unmarshaler = (*IPFSPeer)(nil)
)

func init() {
	copystructure.Copiers[reflect.TypeOf(IPFSPeer{})] = func(value interface{}) (interface{}, error) {
		return value, nil
	}
}

type IPFSPeer struct {
	ai peer.AddrInfo
}

func (pi IPFSPeer) MarshalYAML() (interface{}, error) {
	addrs, err := peer.AddrInfoToP2pAddrs(&pi.ai)
	if err != nil {
		return nil, err
	}

	if len(addrs) == 0 {
		return nil, errors.New("no addresses")
	}

	return addrs[0].String(), nil
}

func (pi *IPFSPeer) DecodeMapstructure(value interface{}) error {
	var addr *peer.AddrInfo
	var err error

	switch value.(type) {
	case string:
		addr, err = peer.AddrInfoFromString(value.(string))
	case IPFSPeer:
		_peer := value.(IPFSPeer)
		addr = _peer.ToAddrInfo()

	default:
		return errors.New("peer must be a string")
	}
	if err != nil {
		return err
	}

	pi.ai = *addr

	return nil
}
func (pi *IPFSPeer) ToAddrInfo() *peer.AddrInfo {
	return &pi.ai
}

func NewIPFSPeer(ai peer.AddrInfo) IPFSPeer {
	return IPFSPeer{ai: ai}
}
