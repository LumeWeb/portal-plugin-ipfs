package domainpolicy

// TargetKind identifies the content type a website binding serves.
type TargetKind int

const (
	TargetKindUnknown TargetKind = iota
	TargetKindIPFS
	TargetKindIPNS
)

// String returns a stable diagnostic name for the target kind.
func (t TargetKind) String() string {
	switch t {
	case TargetKindIPFS:
		return "ipfs"
	case TargetKindIPNS:
		return "ipns"
	default:
		return "unknown"
	}
}

// Valid reports whether the target kind is a known, non-unknown value.
func (t TargetKind) Valid() bool {
	switch t {
	case TargetKindIPFS, TargetKindIPNS:
		return true
	default:
		return false
	}
}

// NewTargetKind validates and returns the given target kind, rejecting
// unknown values.
func NewTargetKind(t TargetKind) (TargetKind, error) {
	if !t.Valid() {
		return TargetKindUnknown, newInvalid("target kind", "unknown value %d", int(t))
	}
	return t, nil
}

// targetKindPrefix maps a target kind to the exact gateway path prefix used
// by DNSLink records. The prefixes are
// internal/db.WebsiteTargetType's IPFSPrefix and IPNSPrefix, which
// ToDNSLinkPath concatenates byte-for-byte in front of the hash with no
// trimming or normalization.
func (t TargetKind) targetKindPrefix() string {
	switch t {
	case TargetKindIPFS:
		return "/ipfs/"
	case TargetKindIPNS:
		return "/ipns/"
	default:
		return ""
	}
}

// ContentTarget is the content a binding serves: a target kind plus its
// value (a CID for IPFS, a peer ID for IPNS). It is a value type; it holds
// no network or database identity.
type ContentTarget struct {
	Kind  TargetKind
	Value string
}

// NewContentTarget validates and returns a ContentTarget, rejecting unknown
// kinds and empty values.
func NewContentTarget(kind TargetKind, value string) (ContentTarget, error) {
	if !kind.Valid() {
		return ContentTarget{}, newInvalid("content target", "unknown kind %d", int(kind))
	}
	if value == "" {
		return ContentTarget{}, newInvalid("content target", "empty value")
	}
	return ContentTarget{Kind: kind, Value: value}, nil
}

// Valid reports whether the target has a known kind and a non-empty value,
// matching what NewContentTarget accepts.
func (t ContentTarget) Valid() bool {
	return t.Kind.Valid() && t.Value != ""
}

// DNSLinkPath returns the gateway path fragment recorded in the DNSLink TXT
// record: "/ipfs/<cid>" or "/ipns/<peer-id>".
//
// Characterization against internal/db.WebsiteTargetType.ToDNSLinkPath: that
// function performs a plain prefix concatenation
// ("/ipfs/" + hash for IPFS, "/ipns/" + hash for IPNS) with no trimming or
// normalization of the hash. DNSLinkPath reproduces that byte-exactly for
// the kinds ContentTarget accepts (IPFS/IPNS with non-empty values are the
// only constructible cases).
func (t ContentTarget) DNSLinkPath() string {
	return t.Kind.targetKindPrefix() + t.Value
}
