BINARY ?= bin/taskflow
ADDR ?= :8080

.PHONY: help build run test cover lint

help: ## показать список целей
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-8s %s\n", $$1, $$2}'

build: ## собрать бинарник
	go build -o $(BINARY) ./cmd/taskflow

run: build ## запустить HTTP-сервер
	$(BINARY) serve -addr $(ADDR)

test: ## тесты с детектором гонок
	go test -race ./...

cover: ## покрытие по всему проекту
	go test -coverprofile=cover.out ./...
	go tool cover -func=cover.out | tail -1

lint: ## go vet и golangci-lint
	go vet ./...
	golangci-lint run
