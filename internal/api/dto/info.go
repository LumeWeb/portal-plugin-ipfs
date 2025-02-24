package dto

import "go.lumeweb.com/httputil"

var _ httputil.DTOResponse[*NodeInfo] = (*InfoResponse)(nil)

type InfoResponse struct {
	PeerID                string   `json:"peer_id"`
	AnnouncementAddresses []string `json:"announcement_addresses"`
	ConnectionAddresses   []string `json:"connection_addresses"`
}

type NodeInfo struct {
	PeerID                string
	AnnouncementAddresses []string
	ConnectionAddresses   []string
}

func (i *InfoResponse) FromModel(nodeInfo *NodeInfo) error {
	i.PeerID = nodeInfo.PeerID
	i.AnnouncementAddresses = nodeInfo.AnnouncementAddresses
	i.ConnectionAddresses = nodeInfo.ConnectionAddresses
	return nil
}
