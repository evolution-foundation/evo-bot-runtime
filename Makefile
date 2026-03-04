.PHONY: run test build lint docker

run:
	go run main.go

test:
	go test ./...

build:
	go build -o bin/evo-bot-runtime .

lint:
	gear validate
	go vet ./...

docker:
	docker build -t evo-bot-runtime .
