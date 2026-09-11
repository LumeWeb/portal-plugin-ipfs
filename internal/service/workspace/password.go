// Password derivation for logical workspace database credentials.
//
// There is deliberately NO operator-configured password secret: each
// workspace's logical MySQL/MariaDB database password is derived
// deterministically with HKDF-SHA256 (RFC 5869) keyed by the portal identity
// private key (Core.Identity.PrivateKey, already present in the portal core
// config) plus a per-workspace random salt persisted on the workspace row. This
// keeps the portal's own key as the single source of secrecy with no separate
// config secret, while the per-workspace salt lets an individual workspace's
// password be rotated independently (regenerate the salt) without touching the
// identity key or other workspaces.
//
// Design notes:
//   - domain separation / version: the HKDF `info` component embeds an
//     algorithm/version label plus the workspace identity (its numeric ID), so
//     keys derived for different purposes or algorithm versions never collide
//     and the scheme can be rotated by bumping the version label.
//   - secrecy: the derived password is held in memory for the current
//     provisioning call only and is NEVER persisted, logged, or included in a
//     trace. Only the non-secret salt is stored on the workspace row.
//   - MySQL/MariaDB + SQL safety: the output is an URL-safe base64 payload
//     (with a "ws_" prefix) whose alphabet contains no quotes, backslashes, or
//     semicolons, so it is safe to interpolate inside a quoted MySQL/MariaDB
//     string literal by mysqlprovision and always satisfies password strength /
//     minimum-length policies for the caching_sha2_password / mariadb plugins.
package workspace

import (
	"crypto/ed25519"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
)

const (
	// dbPasswordInfoPrefix is the domain-separation / algorithm-version label
	// embedded in the HKDF `info` for every derived workspace password. Bump
	// the version to deliberately rotate the derivation scheme for all
	// workspaces.
	dbPasswordInfoPrefix = "portal.ipfs.workspace.db-password:v1:"
	// dbPasswordSaltLen is the length in bytes of the per-workspace random HKDF
	// salt. 16 bytes gives ample per-workspace entropy while staying small on
	// the row.
	dbPasswordSaltLen = 16
	// dbPasswordKeyLen is the number of derived key bytes fed to the base64
	// encoding. 32 bytes yields a strong 43-char base64 payload.
	dbPasswordKeyLen = 32
	// dbPasswordPrefix is prepended to the encoded payload so the derived
	// password never starts with a digit/symbol and stays a recognizable,
	// quote/backslash-free token.
	dbPasswordPrefix = "ws_"
)

// errEmptyIdentityKey is returned when the derivation cannot proceed because
// the portal identity private key is empty/unavailable.
var errEmptyIdentityKey = errors.New("workspace: portal identity private key is unavailable")

// newDatabasePasswordSalt returns a fresh random per-workspace HKDF salt,
// encoded URL-safe base64 so it can be persisted on the workspace row. Only
// this non-secret salt is ever stored; the derived password is not.
func newDatabasePasswordSalt() (string, error) {
	buf := make([]byte, dbPasswordSaltLen)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("workspace: failed to generate database password salt: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

// deriveDatabasePassword deterministically derives a strong logical-database
// password for a workspace with HKDF-SHA256 (RFC 5869) keyed by the portal
// identity private key, domain-separated by the algorithm/version label and the
// workspace identity, and salted by the per-workspace random salt. The same
// identity key + salt + workspace ID always yields the same password, so
// reconciliation and the runtime agree without ever persisting it. An empty
// identity key is treated as an error rather than deriving from nothing.
func deriveDatabasePassword(identityKey ed25519.PrivateKey, workspaceID uint, salt []byte) (string, error) {
	if len(identityKey) == 0 {
		return "", errEmptyIdentityKey
	}
	if len(salt) == 0 {
		return "", errors.New("workspace: database password salt is required")
	}
	info := dbPasswordInfoPrefix + strconv.FormatUint(uint64(workspaceID), 10)
	key, err := hkdf.Key(sha256.New, identityKey, salt, info, dbPasswordKeyLen)
	if err != nil {
		return "", fmt.Errorf("workspace: failed to derive database password: %w", err)
	}
	return dbPasswordPrefix + base64.RawURLEncoding.EncodeToString(key), nil
}
