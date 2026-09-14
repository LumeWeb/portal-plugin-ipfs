// This file implements the per-workspace DNS reconciliation phase: it makes
// the workspace's authoring hostname (<label>.build.<platform-root>)
// authoritative in the platform root's PowerDNS zone, mirroring the Coolify
// application domain provisioning that assumes the integrated proxy (Caddy)
// receives traffic for that name.
//
// The record target is NOT configured anywhere: the record points at the IP
// Coolify has registered for the placement server (ResolveServerIP), which is
// the machine the integrated proxy listens on. A/AAAA is selected from the
// address family of that IP.
//
// Idempotency: CreateRecord issues a PowerDNS RRSet REPLACE, so every
// reconcile pass converges whether the record exists or drifted. Teardown
// deletes both address-family RRSets for the hostname so a server-IP family
// change between provision and delete can never strand an old record.
package workspace

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	pluginCore "go.lumeweb.com/portal-plugin-ipfs/core"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
)

// workspaceDNSRecordTTL is the TTL for per-workspace A/AAAA records. It is
// short enough that reprovisioning a workspace or moving the placement server
// converges quickly; the RRSet belongs to exactly one workspace, so replacing
// it wholesale is always safe.
const workspaceDNSRecordTTL = 300

// Sentinel errors for the DNS provisioning phase.
var (
	// ErrWorkspaceZoneUnavailable is returned when the workspace's platform
	// root has no provisioned PowerDNS zone. The zone lifecycle belongs to the
	// delegated-domain service (operator platform setup); the reconciler
	// retries until it exists.
	ErrWorkspaceZoneUnavailable = errors.New("workspace: platform root has no provisioned DNS zone")
	// ErrWorkspaceDNSRecordFailed is returned when writing the workspace's
	// A/AAAA RRSet into the platform zone fails. The underlying provider
	// (PowerDNS) errors are wrapped so retry classification still applies.
	ErrWorkspaceDNSRecordFailed = errors.New("workspace: failed to publish workspace DNS record")
)

// ReconcileDNS advances the DNS phase for ws: it resolves the record target
// from the Coolify placement server, resolves the platform root's zone, and
// upserts the workspace hostname's A (or AAAA) record into it. It is retryable
// and idempotent (RRSet REPLACE), so a retry after any failure converges.
func (s *WorkspaceService) ReconcileDNS(ctx context.Context, ws *pluginDb.Workspace) error {
	if s.config == nil {
		return fmt.Errorf("workspace: config not available")
	}
	if s.dnsSvc == nil {
		return fmt.Errorf("workspace: dns service not available")
	}
	if s.provider == nil {
		return fmt.Errorf("workspace: provider not available")
	}
	if err := s.ensurePlatformDomain(ctx, ws); err != nil {
		return err
	}

	ip, recordType, err := s.resolveProxyRecord(ctx)
	if err != nil {
		return err
	}

	zone, err := s.resolvePlatformZone(ctx, ws)
	if err != nil {
		return err
	}

	// The record owner is the workspace hostname relative to the zone apex
	// (e.g. "ws-1234.build" inside the "pinned.site" zone).
	owner, err := recordOwnerOnZone(ws.Hostname(), zone.Domain)
	if err != nil {
		return err
	}

	if _, err := s.dnsSvc.CreateRecord(ctx, zone.ID, owner, recordType, ip, workspaceDNSRecordTTL); err != nil {
		return fmt.Errorf("%w: %s %s: %v", ErrWorkspaceDNSRecordFailed, owner, recordType, err)
	}
	return nil
}

// DeleteDNSRecords removes the workspace hostname's A and AAAA RRSets from the
// platform root's zone during teardown. Both address families are deleted so
// the record is gone regardless of which family the placement server had at
// provisioning time (a PowerDNS DELETE of a non-existing RRSet is a no-op). A
// missing zone means the hostname never had record space and is already
// deleted from the caller's perspective.
func (s *WorkspaceService) DeleteDNSRecords(ctx context.Context, ws *pluginDb.Workspace) error {
	if s.dnsSvc == nil {
		return fmt.Errorf("workspace: dns service not available")
	}
	if err := s.ensurePlatformDomain(ctx, ws); err != nil {
		return err
	}
	zone, err := s.resolvePlatformZone(ctx, ws)
	if err != nil {
		// The zone lifecycle is owned elsewhere; when it is gone there is
		// nothing left to delete for this hostname.
		if errors.Is(err, ErrWorkspaceZoneUnavailable) {
			return nil
		}
		return err
	}
	owner, err := recordOwnerOnZone(ws.Hostname(), zone.Domain)
	if err != nil {
		return err
	}
	for _, recordType := range []string{string(pluginCore.RecordTypeA), string(pluginCore.RecordTypeAAAA)} {
		if err := s.dnsSvc.DeleteRecord(ctx, zone.ID, owner, recordType); err != nil {
			return fmt.Errorf("%w: delete %s %s: %v", ErrWorkspaceDNSRecordFailed, owner, recordType, err)
		}
	}
	return nil
}

// resolveProxyRecord resolves the workspace hostnames' DNS target from the
// Coolify placement server: the registered server IP, mapped to an A (IPv4)
// or AAAA (IPv6) record type by address family.
func (s *WorkspaceService) resolveProxyRecord(ctx context.Context) (string, string, error) {
	serverUUID := s.config.Coolify.ServerUUID
	ip, err := s.provider.ResolveServerIP(ctx, serverUUID)
	if err != nil {
		return "", "", fmt.Errorf("workspace: resolve placement server ip: %w", err)
	}
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return "", "", fmt.Errorf("workspace: coolify server %s reports invalid ip %q", serverUUID, ip)
	}
	if parsed.To4() != nil {
		return ip, string(pluginCore.RecordTypeA), nil
	}
	return ip, string(pluginCore.RecordTypeAAAA), nil
}

// resolvePlatformZone returns the PowerDNS zone the workspace hostname's
// records live in: the zone of the workspace's platform root (the same
// one-zone topology the delegated-domain service applies to platform claims).
// The zone itself is never created or deleted here — its lifecycle belongs to
// the delegated-domain service's platform flow.
func (s *WorkspaceService) resolvePlatformZone(ctx context.Context, ws *pluginDb.Workspace) (*pluginDb.DNSZone, error) {
	root := ws.PlatformDomain.Domain
	if root == "" {
		return nil, fmt.Errorf("workspace %d: platform domain is not loaded", ws.ID)
	}
	zone, err := s.dnsSvc.GetZoneByDomain(ctx, root)
	if err != nil {
		return nil, fmt.Errorf("workspace: lookup platform zone for %q: %w", root, err)
	}
	if zone == nil {
		return nil, fmt.Errorf("%w: %q", ErrWorkspaceZoneUnavailable, root)
	}
	return zone, nil
}

// recordOwnerOnZone returns the record owner for hostname relative to the
// zone apex (e.g. "ws-1234.build" for "ws-1234.build.pinned.site" inside the
// "pinned.site" zone). PowerDNS RRSet names are maintained relative to the
// zone by the DNS service's record helpers, and a hostname that does not
// descend from the zone is an internal inconsistency that fails closed.
func recordOwnerOnZone(hostname, zoneDomain string) (string, error) {
	root := strings.TrimSuffix(zoneDomain, ".")
	if root == "" {
		return "", fmt.Errorf("workspace: zone domain %q is empty", zoneDomain)
	}
	if !strings.HasSuffix(hostname, "."+root) {
		return "", fmt.Errorf("workspace: hostname %q does not descend from zone %q", hostname, root)
	}
	return strings.TrimSuffix(hostname, "."+root), nil
}
