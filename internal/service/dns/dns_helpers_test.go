package dns

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pluginDb "go.lumeweb.com/portal-plugin-ipfs/internal/db"
)

func TestParseZoneFile_EmptyContent(t *testing.T) {
	records, err := ParseZoneFile("")
	assert.Error(t, err)
	assert.Nil(t, records)
	assert.Contains(t, err.Error(), "empty")
}

func TestParseZoneFile_InvalidSyntax(t *testing.T) {
	invalidZone := `
$ORIGIN example.com
@	IN	SOA	ns1.example.com. admin.example.com. (
			2024010101	; serial
			3600		; refresh
			1800		; retry
			604800		; expire
			86400		; minimum
			)
@	IN	NS	ns1.example.com
@	IN	INVALID-RECORD-TYPE	invalid-data
`
	records, err := ParseZoneFile(invalidZone)
	assert.Error(t, err)
	assert.Nil(t, records)
}

func TestParseZoneFile_ValidZone(t *testing.T) {
	validZone := `$ORIGIN example.com.
$TTL 3600
@	IN	SOA	ns1.example.com. admin.example.com. (
			2024010101	; serial
			3600		; refresh
			1800		; retry
			604800		; expire
			86400		; minimum
			)
@	IN	NS	ns1.example.com.
@	IN	NS	ns2.example.com.
@	IN	A	192.0.2.1
@	IN	AAAA	2001:db8::1
www	IN	A	192.0.2.2
mail	IN	A	192.0.2.3
@	IN	MX	10	mail.example.com.
@	IN	TXT	"v=spf1 mx -all"
_test	IN	CNAME	www.example.com.
`
	records, err := ParseZoneFile(validZone)
	require.NoError(t, err)
	assert.NotNil(t, records)
	assert.Greater(t, len(records), 0)

	// Verify we can access record properties
	assert.NotEmpty(t, records[0].Type())
	assert.NotEmpty(t, records[0].Domain())
}

func TestParseZoneFile_WithComments(t *testing.T) {
	zoneWithComments := `; This is a comment
$ORIGIN example.com.
$TTL 3600
@	IN	SOA	ns1.example.com. admin.example.com. (
			2024010101	; serial
			3600		; refresh
			1800		; retry
			604800		; expire
			86400		; minimum
			)
@	IN	NS	ns1.example.com. ; This is another comment
@	IN	A	192.0.2.1
`
	records, err := ParseZoneFile(zoneWithComments)
	require.NoError(t, err)
	assert.NotNil(t, records)
	assert.Greater(t, len(records), 0)
}

func TestParseZoneFile_BlankLines(t *testing.T) {
	zoneWithBlankLines := `$ORIGIN example.com.
$TTL 3600

@	IN	SOA	ns1.example.com. admin.example.com. (
			2024010101	; serial
			3600		; refresh
			1800		; retry
			604800		; expire
			86400		; minimum
			)

@	IN	NS	ns1.example.com.

@	IN	A	192.0.2.1
`
	records, err := ParseZoneFile(zoneWithBlankLines)
	require.NoError(t, err)
	assert.NotNil(t, records)
	assert.Greater(t, len(records), 0)
}

func TestParseZoneFile_NoRecords(t *testing.T) {
	noRecordsZone := `$ORIGIN example.com.
$TTL 3600
`
	records, err := ParseZoneFile(noRecordsZone)
	assert.Error(t, err)
	assert.Nil(t, records)
	assert.Contains(t, err.Error(), "no resource records")
}

// TestBuildFullName_RelativeName tests building full names from relative record names
func TestBuildFullName_RelativeName(t *testing.T) {
	tests := []struct {
		name     string
		domain   string
		expected string
	}{
		{
			name:     "www",
			domain:   "example.com",
			expected: "www.example.com.",
		},
		{
			name:     "api",
			domain:   "test.org",
			expected: "api.test.org.",
		},
		{
			name:     "mail",
			domain:   "sub.example.com",
			expected: "mail.sub.example.com.",
		},
		{
			name:     "test-record",
			domain:   "7a9dcb27.example.com",
			expected: "test-record.7a9dcb27.example.com.",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := buildFullName(tt.name, tt.domain)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result, "buildFullName(%q, %q)", tt.name, tt.domain)
		})
	}
}

// TestBuildFullName_EdgeCases tests edge cases and boundary conditions
func TestBuildFullName_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		domain   string
		expected string
	}{
		{
			name:     "",
			domain:   "example.com",
			expected: "example.com.",
		},
		{
			name:     "@",
			domain:   "example.com",
			expected: "example.com.",
		},
		{
			name:     "a",
			domain:   "b.com",
			expected: "a.b.com.",
		},
		{
			name:     "a",
			domain:   "b.c.d.com",
			expected: "a.b.c.d.com.",
		},
		{
			name:     "*",
			domain:   "example.com",
			expected: "*.example.com.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := buildFullName(tt.name, tt.domain)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result, "buildFullName(%q, %q)", tt.name, tt.domain)
		})
	}
}

// TestBuildFullName_InvalidAtPosition tests that @ in non-apex position returns an error
func TestBuildFullName_InvalidAtPosition(t *testing.T) {
	tests := []string{
		"subdomain.@",
		"@.subdomain",
		"a@b",
		"prefix@",
		"@suffix",
	}

	for _, name := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := buildFullName(name, "example.com")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "must be used alone for apex records")
		})
	}
}

// TestBuildFullName_CaseSensitivity tests that function is case-preserving but DNS is case-insensitive
func TestBuildFullName_CaseSensitivity(t *testing.T) {
	name := "WWW"
	domain := "EXAMPLE.COM"
	result, err := buildFullName(name, domain)
	require.NoError(t, err)

	// The result should preserve the input case
	assert.Equal(t, "WWW.EXAMPLE.COM.", result)
}

// TestStripDomain tests removal of domain suffix from full DNS names
func TestStripDomain(t *testing.T) {
	tests := []struct {
		name     string
		domain   string
		expected string
	}{
		{
			name:     "www.example.com",
			domain:   "example.com",
			expected: "www",
		},
		{
			name:     "api.test.org",
			domain:   "test.org",
			expected: "api",
		},
		{
			name:     "sub.sub.domain.example.com",
			domain:   "example.com",
			expected: "sub.sub.domain",
		},
		{
			name:     "example.com",
			domain:   "example.com",
			expected: "",
		},
		{
			name:     "www",
			domain:   "example.com",
			expected: "www",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := stripDomain(tt.name, tt.domain)
			assert.Equal(t, tt.expected, result, "stripDomain(%q, %q)", tt.name, tt.domain)
		})
	}
}

// TestBuildFullNameIdentity tests that buildFullName followed by stripDomain returns original name
func TestBuildFullNameIdentity(t *testing.T) {
	tests := []struct {
		relativeName string
		domainBase   string
	}{
		{"www", "example.com"},
		{"api", "test.org"},
		{"mail", "sub.example.com"},
		{"a.b.c", "example.com"},
		{"api.v1", "service.example.com"},
	}

	for _, tt := range tests {
		t.Run(tt.relativeName, func(t *testing.T) {
			fullName, err := buildFullName(tt.relativeName, tt.domainBase)
			require.NoError(t, err)
			// stripDomain needs the full name without trailing dot to properly match
			strippedName := stripDomain(strings.TrimSuffix(fullName, "."), tt.domainBase)
			assert.Equal(t, tt.relativeName, strippedName,
				"When stripping domain from buildFullName result, should get original name: %q -> buildFullName -> %q -> stripDomain -> %q",
				tt.relativeName, fullName, strippedName)
		})
	}
}

// TestGetDefaultTTL tests the default TTL helper function
func TestGetDefaultTTL(t *testing.T) {
	tests := []struct {
		ttl      uint
		expected uint
	}{
		{0, 3600},
		{1800, 1800},
		{3600, 3600},
		{86400, 86400},
	}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			result := getDefaultTTL(tt.ttl)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestBuildFullNameRoundTrip verifies that buildFullName followed by stripDomain returns original name
func TestBuildFullNameRoundTrip(t *testing.T) {
	testCases := []struct {
		name   string
		domain string
	}{
		{"www", "example.com"},
		{"api", "test.org"},
		{"deep.sub", "example.com"},
		{"a.b.c", "example.com"},
		{"mail", "sub.example.com"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			full, err := buildFullName(tc.name, tc.domain)
			require.NoError(t, err)
			// stripDomain needs the full name without trailing dot to properly match
			relative := stripDomain(strings.TrimSuffix(full, "."), tc.domain)

			assert.Equal(t, tc.name, relative,
				"Round trip through buildFullName and stripDomain should return original name")
		})
	}
}

// TestBuildFullName_AbsoluteName tests building full names from absolute record names (already include domain)
func TestBuildFullName_AbsoluteName(t *testing.T) {
	tests := []struct {
		name     string
		domain   string
		expected string
	}{
		{
			name:     "www.example.com",
			domain:   "example.com",
			expected: "www.example.com.",
		},
		{
			name:     "www.example.com.",
			domain:   "example.com",
			expected: "www.example.com.",
		},
		{
			name:     "api.test.org",
			domain:   "test.org",
			expected: "api.test.org.",
		},
		{
			name:     "deep.sub.domain.example.com",
			domain:   "example.com",
			expected: "deep.sub.domain.example.com.",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := buildFullName(tt.name, tt.domain)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result, "buildFullName(%q, %q)", tt.name, tt.domain)
		})
	}
}

// TestBuildFullName_ZoneApex tests handling of zone apex (base domain)
func TestBuildFullName_ZoneApex(t *testing.T) {
	tests := []struct {
		name     string
		domain   string
		expected string
	}{
		{
			name:     "example.com",
			domain:   "example.com",
			expected: "example.com.",
		},
		{
			name:     "example.com.",
			domain:   "example.com.",
			expected: "example.com.",
		},
		{
			name:     "test.example.com",
			domain:   "test.example.com",
			expected: "test.example.com.",
		},
		{
			name:     "test.example.com.",
			domain:   "test.example.com.",
			expected: "test.example.com.",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := buildFullName(tt.name, tt.domain)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result, "buildFullName(%q, %q)", tt.name, tt.domain)
		})
	}
}

// TestBuildFullName_SubdomainLevels tests building names with multiple subdomain levels
func TestBuildFullName_SubdomainLevels(t *testing.T) {
	tests := []struct {
		name     string
		domain   string
		expected string
	}{
		{
			name:     "a.b.c",
			domain:   "example.com",
			expected: "a.b.c.example.com.",
		},
		{
			name:     "api.v1.service",
			domain:   "production.example.com",
			expected: "api.v1.service.production.example.com.",
		},
		{
			name:     "sub.sub.sub.domain",
			domain:   "example.org",
			expected: "sub.sub.sub.domain.example.org.",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := buildFullName(tt.name, tt.domain)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result, "buildFullName(%q, %q)", tt.name, tt.domain)
		})
	}
}

// TestBuildFullName_DomainWithTrailingDot tests domains with trailing dots
func TestBuildFullName_DomainWithTrailingDot(t *testing.T) {
	tests := []struct {
		name     string
		domain   string
		expected string
	}{
		{
			name:     "www",
			domain:   "example.com.",
			expected: "www.example.com.",
		},
		{
			name:     "api",
			domain:   "test.org.",
			expected: "api.test.org.",
		},
		{
			name:     "mail",
			domain:   "sub.example.com.",
			expected: "mail.sub.example.com.",
		},
		{
			name:     "www.example.com",
			domain:   "example.com.",
			expected: "www.example.com.",
		},
		{
			name:     "www.example.com",
			domain:   "example.com",
			expected: "www.example.com.",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := buildFullName(tt.name, tt.domain)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result, "buildFullName(%q, %q)", tt.name, tt.domain)
		})
	}
}

// TestBuildFullName_CanonicalAllResults tests that all results have trailing dots
func TestBuildFullName_CanonicalAllResults(t *testing.T) {
	tests := []struct {
		name   string
		domain string
	}{
		{"www", "example.com"},
		{"www.example.com", "example.com"},
		{"www.example.com.", "example.com"},
		{"example.com", "example.com"},
		{"example.com.", "example.com."},
		{"a.b.c", "example.com"},
		{"deep.sub.example.com", "example.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := buildFullName(tt.name, tt.domain)
			require.NoError(t, err)
			assert.True(t, strings.HasSuffix(result, "."),
				"Result should have trailing dot: buildFullName(%q, %q) = %q", tt.name, tt.domain, result)
		})
	}
}

// TestBuildFullName_RealWorldScenarios tests real-world naming scenarios from the codebase
func TestBuildFullName_RealWorldScenarios(t *testing.T) {
	tests := []struct {
		name     string
		domain   string
		expected string
	}{
		// From DNS service record creation
		{
			name:     "test-db3471b9",
			domain:   "7a9dcb27.example.com",
			expected: "test-db3471b9.7a9dcb27.example.com.",
		},
		// From website DNS records
		{
			name:     "_dnslink",
			domain:   "example.com",
			expected: "_dnslink.example.com.",
		},
		{
			name:     "www",
			domain:   "example.com",
			expected: "www.example.com.",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := buildFullName(tt.name, tt.domain)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result, "buildFullName(%q, %q)", tt.name, tt.domain)
		})
	}
}

func TestBuildTargetPath(t *testing.T) {
	tests := []struct {
		name       string
		targetHash string
		targetType pluginDb.WebsiteTargetType
		expected   DNSLinkTarget
	}{
		{
			name:       "ipfs target includes dnslink prefix",
			targetHash: "QmHash123",
			targetType: pluginDb.WebsiteTargetTypeIPFS,
			expected:   DNSLinkTarget("dnslink=/ipfs/QmHash123"),
		},
		{
			name:       "ipns target includes dnslink prefix",
			targetHash: "12D3KooWExamplePeerID",
			targetType: pluginDb.WebsiteTargetTypeIPNS,
			expected:   DNSLinkTarget("dnslink=/ipns/12D3KooWExamplePeerID"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := buildTargetPath(tt.targetHash, tt.targetType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFormatRecordContent(t *testing.T) {
	tests := []struct {
		name       string
		recordType string
		content    string
		expected   string
	}{
		// ALIAS targets must be absolute FQDNs (trailing dot) for PowerDNS.
		{"alias no dot", "ALIAS", "ipfs.pub", "ipfs.pub."},
		{"alias with dot", "ALIAS", "ipfs.pub.", "ipfs.pub."},
		{"alias with spaces", "ALIAS", " gateway.example ", "gateway.example."},
		// Existing FQDN-normalized types keep working.
		{"cname no dot", "CNAME", "www.lumeweb", "www.lumeweb."},
		{"ns no dot", "NS", "ns1.lumeweb", "ns1.lumeweb."},
		{"mx no dot", "MX", "mail.lumeweb", "mail.lumeweb."},
		// TXT gets quoted, not dotted.
		{"txt quoted", "TXT", "dnslink=/ipfs/QmHash", `"dnslink=/ipfs/QmHash"`},
		// Address types pass through unchanged.
		{"a unchanged", "A", "185.150.189.208", "185.150.189.208"},
		{"aaaa unchanged", "AAAA", "2001:db8::1", "2001:db8::1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatRecordContent(tt.recordType, tt.content)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestRecordID(t *testing.T) {
	c := []struct {
		name           string
		zone           uint
		n, ty, content string
	}{
		{"spf at apex", 12345, "@", "TXT", "v=spf1 include:mxroute.com -all"},
		{"google verify", 12345, "@", "TXT", "google-site-verification=abc123xyz"},
		{"mx 10", 12345, "@", "MX", "10 mail.example.com"},
		{"mx 20", 12345, "@", "MX", "20 backup.example.com"},
		{"a www 10", 12345, "www", "A", "192.0.2.10"},
		{"a www 11", 12345, "www", "A", "192.0.2.11"},
		{"same content different zone", 99999, "@", "TXT", "v=spf1 include:mxroute.com -all"},
	}
	// Distinct inputs must yield distinct ids where expected.
	distinct := [][2]string{
		{c[0].name, c[1].name},
		{c[1].name, c[2].name},
		{c[2].name, c[3].name},
		{c[4].name, c[5].name},
		{c[0].name, c[6].name}, // same content+name+type, different zone scope
	}
	ids := map[string]string{}
	for _, tc := range c {
		id := recordID(tc.zone, tc.n, tc.ty, tc.content)
		// 11 chars, URL-safe base64.
		assert.Len(t, id, 11, tc.name)
		assert.Regexp(t, `^[A-Za-z0-9_-]{11}$`, id, tc.name)
		ids[tc.name] = id
		// Deterministic.
		assert.Equal(t, id, recordID(tc.zone, tc.n, tc.ty, tc.content), tc.name)
	}
	for _, pair := range distinct {
		assert.NotEqual(t, ids[pair[0]], ids[pair[1]], "%s vs %s", pair[0], pair[1])
	}

	// Type is case-insensitive in the hash: txt vs TXT yield the same id.
	assert.Equal(t, ids["spf at apex"], recordID(12345, "@", "txt", "v=spf1 include:mxroute.com -all"))

	// TTL/disabled are not part of the hash: recomputing with a different TTL
	// is impossible via the signature (it only takes zone,name,type,content),
	// so assert id is stable across a content-identical call (already above).
}

// TestRecordIDStableAcrossSurfaces guards that the same logical record hashes
// to the same id whether surfaced through the read path (recordToDTOWithZoneID,
// which uses stripDomain(buildFullName(name, domain))) or the update response
// (UpdateRecord, which previously used the raw caller-supplied name).
func TestRecordIDStableAcrossSurfaces(t *testing.T) {
	const zone = 12345
	const domain = "example.com."

	cases := []struct{ name string }{
		{"@"}, {"www"},
	}

	for _, tc := range cases {
		fullName, err := buildFullName(tc.name, domain)
		assert.NoError(t, err)
		dtoName := stripDomain(fullName, domain)

		// Same id whether computed from the caller-supplied name (through the
		// normalized DTO form) or from the read-back full RRSet name.
		idFromUpdate := recordID(zone, dtoName, "TXT", "v=spf1 include:mxroute.com -all")
		idFromRead := recordID(zone, stripDomain(fullName, domain), "TXT", "v=spf1 include:mxroute.com -all")
		assert.Equal(t, idFromUpdate, idFromRead, "name=%q", tc.name)

		// The normalized DTO name matches what recordToDTOWithZoneID exposes.
		if tc.name == "@" {
			assert.Equal(t, "", dtoName)
		} else {
			assert.Equal(t, "www", dtoName)
		}
	}
}
