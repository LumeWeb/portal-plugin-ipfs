package ipfs

import (
	"context"

	"github.com/ipfs/boxo/bitswap"
	bsmsg "github.com/ipfs/boxo/bitswap/message"
	pc "go.lumeweb.com/portal-plugin-ipfs/internal/protocol/context"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// PeerIPBitswap extends bitswap.Bitswap to inject peer IP addresses into context during message handling.
// It extracts the remote peer's IP address from network connections and adds it to the context,
// allowing downstream handlers to access the peer's IP for tracking and quota purposes.
type PeerIPBitswap struct {
	*bitswap.Bitswap
	host host.Host
}

// NewPeerIPBitswap creates a new PeerIPBitswap that embeds a bitswap.Bitswap instance
// and stores a reference to the libp2p host for extracting peer connection information.
func NewPeerIPBitswap(bitswapInstance *bitswap.Bitswap, h host.Host) *PeerIPBitswap {
	return &PeerIPBitswap{
		Bitswap: bitswapInstance,
		host:    h,
	}
}

// ReceiveMessage intercepts incoming bitswap messages to extract the peer's IP address
// and injects it into the context before delegating to the embedded Bitswap handler.
//
// The IP extraction process:
// 1. Uses the host's network to find connections to the sender peer
// 2. Extracts the remote address from the first connection
// 3. Adds the IP string to the context with pc.ClientIPKey
// 4. Delegates to the embedded Bitswap.ReceiveMessage with the modified context
//
// This allows downstream code (e.g., BlockStore.Get) to access the peer's IP for
// quota tracking and download attribution.
func (ps *PeerIPBitswap) ReceiveMessage(ctx context.Context, p peer.ID, incoming bsmsg.BitSwapMessage) {
	// Get connections to this peer
	conns := ps.host.Network().ConnsToPeer(p)
	
	// Extract IP address from connection and add to context using pc.ClientIPOption
	if len(conns) > 0 {
		remoteAddr := conns[0].RemoteMultiaddr().String()
		ctx = pc.ClientIPOption(ctx, remoteAddr)
	}
	
	// Delegate to embedded Bitswap with modified context
	ps.Bitswap.ReceiveMessage(ctx, p, incoming)
}
