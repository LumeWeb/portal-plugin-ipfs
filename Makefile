.PHONY: test

test:
	GOLANG_PROTOBUF_REGISTRATION_CONFLICT=warn go test ./...
