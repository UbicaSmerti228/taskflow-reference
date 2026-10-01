BINARY ?= bin/taskflow

.PHONY: help build run test test-short cover lint up down smoke

help: ## показать список целей
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-11s %s\n", $$1, $$2}'

build: ## собрать бинарник
	go build -tags nomsgpack -o $(BINARY) ./cmd/taskflow

run: build ## запустить сервер (нужны DATABASE_URL, REDIS_URL и JWT_SECRET)
	$(BINARY) serve

test: ## все тесты; интеграционные поднимают PostgreSQL в Docker
	go test -race ./...

test-short: ## только быстрые тесты, без Docker
	go test -race -short ./...

up: ## поднять api, postgres и redis в Docker
	docker compose up -d --build --wait

down: ## остановить контейнеры, данные остаются в томе
	docker compose down

smoke: ## проверить запущенный сервис через HTTP (нужны curl и jq)
	scripts/smoke.sh

cover: ## покрытие по всему проекту
	go test -coverprofile=cover.out ./...
	go tool cover -func=cover.out | tail -1

lint: ## go vet и golangci-lint
	go vet ./...
	golangci-lint run
