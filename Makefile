.PHONY: help build run test test-short bench cover lint proto proto-tools dashboard up down smoke observe

help: ## показать список целей
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

build: ## собрать оба бинарника в bin/
	go build -tags nomsgpack -o bin/ ./cmd/taskflow ./cmd/notifier

run: build ## запустить TaskFlow без Docker (нужны переменные из taskflow без аргументов)
	bin/taskflow serve

test: ## все тесты; интеграционные поднимают PostgreSQL в Docker
	go test -race ./...

test-short: ## только быстрые тесты, без Docker
	go test -race -short ./...

bench: ## бенчмарки горячего пути; разбор результатов в docs/perf.md
	go test -bench=. -benchmem -run='^$$' ./internal/service ./internal/httpapi

cover: ## покрытие по всему проекту
	go test -coverprofile=cover.out ./...
	go tool cover -func=cover.out | tail -1

lint: ## go vet, golangci-lint и buf lint
	go vet ./...
	golangci-lint run
	buf lint

# Версии генераторов закреплены: у всех получается одинаковый код, и CI может сверить его с репозиторием.
proto-tools: ## поставить плагины генерации кода из .proto (сам buf: https://buf.build/docs/cli/installation/)
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2

proto: ## сгенерировать Go-код из proto/ в internal/gen/
	buf lint
	buf generate

dashboard: ## пересобрать дашборд Grafana из scripts/gen-dashboard.py
	python3 scripts/gen-dashboard.py

up: ## поднять оба сервиса, базу, Redis, Kafka, Prometheus и Grafana в Docker
	docker compose up -d --build --wait

down: ## остановить контейнеры, данные остаются в томах
	docker compose down

smoke: ## проверить запущенный сервис через HTTP (нужны curl и jq)
	scripts/smoke.sh

observe: ## проверить, что метрики собираются и дашборд не пустой (после make smoke)
	scripts/check-observability.sh
