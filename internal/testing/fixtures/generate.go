// Package fixtures provides test fixture generation utilities.
// This file contains go:generate directives to rebuild test fixtures.
//
// Run: go generate ./internal/testing/fixtures
//
// This generates fixtures in two steps:
// 1. Run ipfs-content's go generate to generate shared fixtures
// 2. Generate portal-plugin-ipfs specific fixtures (empty.car, invalid.car)
package fixtures

//go:generate bash ./generate_ipfs_content.sh
