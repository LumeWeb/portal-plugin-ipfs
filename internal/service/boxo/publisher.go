package boxo

import (
	"context"
	"fmt"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/namesys"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	"go.lumeweb.com/portal/core"
	"go.lumeweb.com/portal-plugin-ipfs/internal"
	"go.uber.org/zap"
)

// PublishWithTTL is a helper to create a TTL publish option
func PublishWithTTL(ttl time.Duration) namesys.PublishOption {
	return namesys.PublishWithTTL(ttl)
}

// IPNSPublisherService wraps boxo/namesys.IPNSPublisher for IPNS record publishing
type IPNSPublisherService struct {
	*core.BaseComponent
	publisher *namesys.IPNSPublisher
}

// NewIPNSPublisherService creates a new IPNS publisher service
func NewIPNSPublisherService() (core.Service, []core.ContextBuilderOption, error) {
	svc := &IPNSPublisherService{}

	opts := core.ContextOptions(
		core.ContextWithStartupFunc(func(ctx core.Context) error {
			// Get the IPFS protocol
			proto := core.GetProtocol(internal.ProtocolName)
			if proto == nil {
				return fmt.Errorf("IPFS protocol not found")
			}

			// Get the IPNS node access
			ipnsNode, ok := proto.(pluginCore.IPNSBoxoServices)
			if !ok {
				return fmt.Errorf("IPFS protocol does not implement IPNSBoxoServices")
			}

			node := ipnsNode.GetIPNSNode()
			if node == nil {
				return fmt.Errorf("IPNS node not found")
			}

			publisher := node.GetPublisher()
			if publisher == nil {
				return fmt.Errorf("IPNS publisher not found")
			}

			svc.publisher = publisher
			return nil
		}),
	)

	return svc, opts, nil
}

func (s *IPNSPublisherService) ID() string {
	return pluginCore.IPNS_PUBLISHER_SERVICE
}

// PublishCID publishes a CID to an IPNS key
// keyID is the peer ID (IPNS name) to publish to
// cidStr is the CID to publish as the IPNS record value
// ttl is the time-to-live for the IPNS record (use 0 for default)
func (s *IPNSPublisherService) PublishCID(ctx context.Context, keyID string, cidStr string, ttl time.Duration) error {
	// Convert to core.Context for tracing
	coreCtx := ctx.(core.Context)
	traceCtx, span := core.TraceMethod(coreCtx, "IPNSPublisherService.PublishCID")
	defer span.End()

	// Get IPNSKeyService to retrieve the private key
	ipnsKeyService := core.GetService[pluginCore.IPNSKeyService](coreCtx, pluginCore.IPNS_KEY_SERVICE)
	if ipnsKeyService == nil {
		return fmt.Errorf("IPNSKeyService not available")
	}

	// Get the private key by peer ID
	privKey, _, err := ipnsKeyService.GetPrivateKeyByPeerID(coreCtx, keyID)
	if err != nil {
		s.Logger().Error("Failed to get private key for IPNS publish",
			zap.Error(err),
			zap.String("key_id", keyID),
		)
		return fmt.Errorf("failed to get private key: %w", err)
	}

	// Parse the CID
	targetCid, err := cid.Decode(cidStr)
	if err != nil {
		return fmt.Errorf("invalid CID %s: %w", cidStr, err)
	}

	// Create an IPNS path from the CID
	ipnsPath := path.FromCid(targetCid)

	// Build publish options
	var options []namesys.PublishOption
	if ttl > 0 {
		options = append(options, namesys.PublishWithTTL(ttl))
	}

	// Publish the IPNS record
	err = s.publisher.Publish(traceCtx, privKey, ipnsPath, options...)
	if err != nil {
		s.Logger().Error("Failed to publish IPNS record",
			zap.Error(err),
			zap.String("key_id", keyID),
			zap.String("cid", cidStr),
		)
		return fmt.Errorf("failed to publish IPNS record: %w", err)
	}

	peerID, _ := peer.IDFromPrivateKey(privKey)
	s.Logger().Info("Successfully published IPNS record",
		zap.String("peer_id", peerID.String()),
		zap.String("cid", cidStr),
	)

	return nil
}

// PublishWithKey publishes a CID using the provided private key
func (s *IPNSPublisherService) PublishWithKey(ctx context.Context, privKey crypto.PrivKey, cidStr string, ttl time.Duration) error {
	// Convert to core.Context for tracing
	coreCtx := ctx.(core.Context)
	traceCtx, span := core.TraceMethod(coreCtx, "IPNSPublisherService.PublishWithKey")
	defer span.End()

	// Parse the CID
	targetCid, err := cid.Decode(cidStr)
	if err != nil {
		return fmt.Errorf("invalid CID %s: %w", cidStr, err)
	}

	// Create an IPNS path from the CID
	ipnsPath := path.FromCid(targetCid)

	// Build publish options
	var options []namesys.PublishOption
	if ttl > 0 {
		options = append(options, namesys.PublishWithTTL(ttl))
	}

	// Publish the IPNS record
	err = s.publisher.Publish(traceCtx, privKey, ipnsPath, options...)
	if err != nil {
		s.Logger().Error("Failed to publish IPNS record",
			zap.Error(err),
			zap.String("cid", cidStr),
		)
		return fmt.Errorf("failed to publish IPNS record: %w", err)
	}

	peerID, _ := peer.IDFromPrivateKey(privKey)
	s.Logger().Info("Successfully published IPNS record",
		zap.String("peer_id", peerID.String()),
		zap.String("cid", cidStr),
	)

	return nil
}

// GetPublished retrieves the latest published record for an IPNS name
func (s *IPNSPublisherService) GetPublished(ctx context.Context, keyID string, checkRouting bool) (*ipns.Record, error) {
	// Convert to core.Context for tracing
	coreCtx := ctx.(core.Context)
	traceCtx, span := core.TraceMethod(coreCtx, "IPNSPublisherService.GetPublished")
	defer span.End()

	// Parse the peer ID
	peerID, err := peer.Decode(keyID)
	if err != nil {
		return nil, fmt.Errorf("invalid peer ID %s: %w", keyID, err)
	}

	// Create IPNS name
	name := ipns.NameFromPeer(peerID)

	// Get the published record
	record, err := s.publisher.GetPublished(traceCtx, name, checkRouting)
	if err != nil {
		return nil, fmt.Errorf("failed to get published record: %w", err)
	}

	return record, nil
}

// ListPublished returns all IPNS records published by this node
func (s *IPNSPublisherService) ListPublished(ctx context.Context) (map[ipns.Name]*ipns.Record, error) {
	// Convert to core.Context for tracing
	coreCtx := ctx.(core.Context)
	traceCtx, span := core.TraceMethod(coreCtx, "IPNSPublisherService.ListPublished")
	defer span.End()

	records, err := s.publisher.ListPublished(traceCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to list published records: %w", err)
	}

	return records, nil
}
