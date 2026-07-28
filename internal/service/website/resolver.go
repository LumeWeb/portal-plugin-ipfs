package website

import (
	"context"
	"net"
	"time"

	dnslink "github.com/dnslink-std/go"
)

type DNSResolver interface {
	ResolveDNSLink(domain string) (dnslink.Result, error)
	LookupTXT(ctx context.Context, domain string) ([]string, error)
}

// LiveResolver performs DNS lookups using either the system default resolver
// or a custom net.Resolver (for alt-root namespaces like HNS).
//
// Different roots (ICANN, HNS, etc.) may require different resolvers because
// alt-root names live on separate namespaces/blockchains and are not visible
// to standard recursive resolvers.
type LiveResolver struct {
	resolver *net.Resolver
}

// NewLiveResolver returns a DNSResolver that targets the given server
// address (e.g. "1.2.3.4:53" or "hns-doh.example:443" style — currently only
// traditional DNS dial is supported; DoH would be a separate implementation).
//
// If addr is empty, the system default resolver is used.
func NewLiveResolver(addr string) DNSResolver {
	if addr == "" {
		return LiveResolver{}
	}
	return LiveResolver{
		resolver: &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
				d := net.Dialer{}
				return d.DialContext(ctx, network, addr)
			},
		},
	}
}

func (r LiveResolver) ResolveDNSLink(domain string) (dnslink.Result, error) {
	if r.resolver == nil {
		return dnslink.Resolve(domain)
	}

	// Use dnslink's custom Resolver with our LookupTXT wrapper.
	custom := &dnslink.Resolver{
		LookupTXT: wrapLookupTXT(r.resolver),
	}
	return custom.Resolve(domain)
}

func (r LiveResolver) LookupTXT(ctx context.Context, domain string) ([]string, error) {
	if r.resolver == nil {
		return net.DefaultResolver.LookupTXT(ctx, domain)
	}
	return r.resolver.LookupTXT(ctx, domain)
}

// wrapLookupTXT adapts net.Resolver.LookupTXT to dnslink's LookupTXTFunc.
func wrapLookupTXT(nr *net.Resolver) func(string) ([]dnslink.LookupEntry, error) {
	return func(name string) ([]dnslink.LookupEntry, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		txts, err := nr.LookupTXT(ctx, name)
		if err != nil {
			// Preserve NXDOMAIN behavior expected by dnslink.
			if dnsErr, ok := err.(*net.DNSError); ok && dnsErr.IsNotFound {
				return nil, dnslink.NewDNSRCodeError(3, name) // NXDOMAIN
			}
			return nil, err
		}
		entries := make([]dnslink.LookupEntry, len(txts))
		for i, t := range txts {
			entries[i] = dnslink.LookupEntry{Value: t, Ttl: 0}
		}
		return entries, nil
	}
}
