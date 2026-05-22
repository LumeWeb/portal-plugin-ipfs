package website

import (
	"net"

	dnslink "github.com/dnslink-std/go"
)

type DNSResolver interface {
	ResolveDNSLink(domain string) (dnslink.Result, error)
	LookupTXT(domain string) ([]string, error)
}

type LiveResolver struct{}

func (LiveResolver) ResolveDNSLink(domain string) (dnslink.Result, error) {
	return dnslink.Resolve(domain)
}

func (LiveResolver) LookupTXT(domain string) ([]string, error) {
	return net.LookupTXT(domain)
}
