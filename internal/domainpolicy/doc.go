// Package domainpolicy contains the pure vocabulary for portal website and
// domain hosting policy.
//
// This package is a pure domain-policy package. It expresses the validated
// value types of the domain-hosting behavior matrix: naming systems,
// resolution routes, authority loci, zone allocation, lifecycle states,
// hosting requests, content targets, security plans, proof gates, and record
// intents. It must remain free of infrastructure dependencies.
//
// This package may not import any of the following:
//
//   - internal/db (or any other persistence layer)
//   - API DTOs (internal/api and its subpackages)
//   - GORM
//   - portal core
//   - service packages (internal/service or its subpackages)
//   - DNS clients
//   - chain clients
//
// Only the Go standard library is permitted. Guard against violations with:
//
//	go list -mod=mod -f '{{join .Imports "\n"}}' ./internal/domainpolicy
//
// Values here fail closed: every constructor rejects unknown or inconsistent
// input, and unknown is always the zero value of an enum.
package domainpolicy
