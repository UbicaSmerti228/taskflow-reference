BINARY ?= bin/taskflow
ADDR ?= :8080

.PHONY: help build run test test-short cover lint up down

help: ## показать список целей
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-11s %s\n", $$1, $$2}'

build: ## собрать бинарник
	go build -o $(BINARY) ./cmd/taskflow

run: build ## запустить сервер (нужна переменная DATABASE_URL)
	$(BINARY) serve -addr $(ADDR)

test: ## все тесты; интеграционные поднимают PostgreSQL в Docker
	go test -race ./...

test-short: ## только быстрые тесты, без Docker
	go test -race -short ./...

up: ## поднять api и postgres в Docker
	docker compose up -d --build --wait

down: ## остановить контейнеры, данные остаются в томе
	docker compose down

cover: ## покрытие по всему проекту
	go test -coverprofile=cover.out ./...
	go tool cover -func=cover.out | tail -1

lint: ## go vet и golangci-lint
	go vet ./...
	golangci-lint run
