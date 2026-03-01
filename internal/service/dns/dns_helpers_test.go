package dns

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
