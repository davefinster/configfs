.proto: buildProto buildWebProto

buildProto:
	protoc --go_out=./internal/proto --go_opt=paths=source_relative --go-grpc_out=./internal/proto --go-grpc_opt=paths=source_relative -I proto/ configfs.proto

installProto:
	go install google.golang.org/protobuf/cmd/protoc-gen-go && \
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc

SERVER_TAILSCALE_FLAGS_FILE ?= .tailscale.server.flags
FS_TAILSCALE_FLAGS_FILE     ?= .tailscale.fs.flags
SERVER_TAILSCALE_FLAGS := $(shell grep -vhE '^[[:space:]]*(#|$$)' $(SERVER_TAILSCALE_FLAGS_FILE) 2>/dev/null)
FS_TAILSCALE_FLAGS     := $(shell grep -vhE '^[[:space:]]*(#|$$)' $(FS_TAILSCALE_FLAGS_FILE) 2>/dev/null)

runServer:
	go run cmd/server/main.go $(SERVER_TAILSCALE_FLAGS)

runFs:
	go build -o tmp/fs cmd/fs/main.go
	sudo ./tmp/fs $(FS_TAILSCALE_FLAGS)

.PHONY: test
test:
	go test ./...
