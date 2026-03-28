package ipfs

import (
	"net"
	"strings"

	"github.com/ipfs/boxo/bitswap/tracer"
	bsmsg "github.com/ipfs/boxo/bitswap/message"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// PeerRequestTracker composes the boxo bitswap tracer interface with additional
// functionality for managing peer request tracking and lifecycle.
type PeerRequestTracker interface {
	tracer.Tracer
	SetupDisconnectListeners()
}

// extractPeerIP extracts the IP address or hostname from a multiaddr.
// It returns the first valid IP or address component found in the multiaddr.
// For example:
//   /ip4/192.168.1.1/tcp/4001 -> 192.168.1.1
//   /ip6/::1/tcp/4001 -> ::1
//   /dns4/example.com/tcp/4001 -> example.com
func extractPeerIP(maddr ma.Multiaddr) string {
	// Convert to string and parse manually for simplicity
	addrStr := maddr.String()

	// Split by '/' and look for known protocols
	// Multiaddr format: /protocol1/value1/protocol2/value2/...
	parts := strings.Split(addrStr, "/")

	for i, part := range parts {
		if part == "" {
			continue
		}

		// Check for IP4 protocol followed by value
		if part == "ip4" && i+1 < len(parts) {
			ipStr := parts[i+1]
			if ip := net.ParseIP(ipStr); ip != nil && !ip.IsUnspecified() && !ip.IsLoopback() {
				return ipStr
			}
		}

		// Check for IP6 protocol followed by value
		if part == "ip6" && i+1 < len(parts) {
			ipStr := parts[i+1]
			if ip := net.ParseIP(ipStr); ip != nil && !ip.IsUnspecified() && !ip.IsLoopback() {
				// Remove brackets if present (e.g., [::1])
				return strings.Trim(ipStr, "[]")
			}
		}

		// Check for DNS protocols followed by hostname
		if part == "dns" || part == "dns4" || part == "dns6" || part == "dnsaddr" {
			if i+1 < len(parts) && parts[i+1] != "" {
				return parts[i+1]
			}
		}
	}

	return ""
}

// trackWantsFromBitSwapMessage extracts WANT entries from a bitswap message and tracks them
// in the BlockRequestTracker. This is a pure helper function that extracts the business
// logic for easier testing without interface mocking.
// It iterates over the list of WANT entries and records that the specified peer requested each block.
func trackWantsFromBitSwapMessage(msg bsmsg.BitSwapMessage, peerIP string, tracker *BlockRequestTracker) {
	if tracker == nil || peerIP == "" {
		return
	}

	wantList := msg.Wantlist()
	if wantList != nil {
		for _, entry := range wantList {
			tracker.AddRequest(entry.Cid, peerIP)
		}
	}
}

// BitswapTracer observes bitswap messages to track peer requests for blocks.
// It implements the tracer.Tracer interface from boxo/bitswap/tracer.
// It extracts peer IP addresses from connections and tracks which peers are
// requesting which blocks, enabling probabilistic attribution for quota tracking
// when direct client context is unavailable.
type BitswapTracer struct {
	tracker *BlockRequestTracker
	host    host.Host
}

// NewBitswapTracer creates a new PeerRequestTracker with the given tracker and host.
// The host is used to extract peer IP addresses from connections.
func NewBitswapTracer(tracker *BlockRequestTracker, h host.Host) PeerRequestTracker {
	return &BitswapTracer{
		tracker: tracker,
		host:    h,
	}
}

// MessageReceived is called when a bitswap message is received from a peer.
// It extracts the peer's IP address and tracks any WANT requests in the message.
// This implements the tracer.Tracer interface.
func (bt *BitswapTracer) MessageReceived(p peer.ID, incoming bsmsg.BitSwapMessage) {
	// Get connections to this peer
	conns := bt.host.Network().ConnsToPeer(p)

	// Extract IP address from connection and track wants
	if len(conns) > 0 {
		peerIP := extractPeerIP(conns[0].RemoteMultiaddr())
		trackWantsFromBitSwapMessage(incoming, peerIP, bt.tracker)
	}
}

// MessageSent is called when a bitswap message is sent to a peer.
// This implements the tracer.Tracer interface but doesn't need to track outgoing wants.
func (bt *BitswapTracer) MessageSent(p peer.ID, outgoing bsmsg.BitSwapMessage) {
	// No action needed for outbound messages
}

// handlePeerDisconnected handles the cleanup when a peer disconnects from the node.
// It removes the peer's IP from all tracked block requests to prevent ghost wants
// from accumulating over time.
func (bt *BitswapTracer) handlePeerDisconnected(net network.Network, conn network.Conn) {
	// Get the peer's IP address from the remote multiaddr
	peerIP := extractPeerIP(conn.RemoteMultiaddr())

	// Remove this peer from all tracked CIDs
	if peerIP != "" {
		bt.tracker.RemovePeerFromAll(peerIP)
	}
}

// SetupDisconnectListeners registers network event listeners to handle peer disconnections.
// This must be called after the tracer is created to start listening for disconnect events.
func (bt *BitswapTracer) SetupDisconnectListeners() {
	bt.host.Network().Notify(&network.NotifyBundle{
		DisconnectedF: func(net network.Network, conn network.Conn) {
			bt.handlePeerDisconnected(net, conn)
		},
	})
}
