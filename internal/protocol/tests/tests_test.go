package tests

import "runtime"

var (
	_, currentFile, _, _ = runtime.Caller(0)
	carFileName          = "../../testing/fixtures/cars/bbb.car"
)
