// Package fixtures provides test fixture generation utilities.
// This file contains go:generate directives to rebuild test fixtures.
//
// Run: go generate ./internal/testing/fixtures
package fixtures

//go:generate ./generate_car.sh
//go:generate ./generate_block.sh
//go:generate go run ./invalid_car_generator.go
//go:generate go run ./empty_car_generator.go
