.PHONY: test build container dapr

# -race is what catches concurrent workflows stepping on each other, which
# unit tests on their own cannot see
test:
	go test -v ./... -race -covermode=atomic -coverprofile=coverage.out

build:
	go build -o ./bin/server ./cmd/server.go

container:
	docker build -t summary-orchestrator .

dapr:
	dapr run --app-id=summary-orchestrator --app-protocol=http --app-port 8080 --dapr-grpc-port 50001 --resources-path ./dapr/components -- go run cmd/server.go
