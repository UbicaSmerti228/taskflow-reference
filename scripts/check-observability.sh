#!/usr/bin/env bash
# Проверка стенда наблюдаемости после того, как по сервису прошли запросы (например, scripts/smoke.sh):
# Prometheus собирает метрики обоих сервисов, а каждый запрос дашборда Grafana возвращает данные.
# Использование: scripts/check-observability.sh [адрес Prometheus] [адрес Grafana]
set -euo pipefail

PROM="${1:-http://localhost:9090}"
GRAFANA="${2:-http://localhost:3000}"
DASHBOARD="$(dirname "$0")/../deploy/grafana/dashboards/taskflow.json"

# query ЗАПРОС — печатает число рядов в ответе Prometheus.
query() {
  curl -fsS --get "$PROM/api/v1/query" --data-urlencode "query=$1" |
    jq -e 'if .status == "success" then .data.result | length else error("запрос не выполнен") end'
}

# Prometheus опрашивает сервисы раз в несколько секунд: ждём, пока оба ответят.
for attempt in $(seq 1 30); do
  if [ "$(query 'up == 1')" = 2 ]; then
    echo "ok  Prometheus видит оба сервиса"
    break
  fi
  if [ "$attempt" = 30 ]; then
    echo "ОШИБКА: Prometheus не собирает метрики с обоих сервисов:" >&2
    curl -fsS "$PROM/api/v1/targets" | jq '.data.activeTargets[] | {job: .labels.job, health, lastError}' >&2
    exit 1
  fi
  sleep 2
done
# Каждый запрос дашборда должен вернуть хотя бы один ряд: опечатка в имени метрики дала бы пустую панель.
# Ждём до 40 секунд: rate() нужны хотя бы два замера ряда, а ряд появляется только с первым событием.
failed=0
while IFS= read -r expr; do
  rows=0
  for attempt in $(seq 1 20); do
    rows="$(query "$expr")"
    [ "$rows" -ge 1 ] && break
    sleep 2
  done
  if [ "$rows" -ge 1 ]; then
    echo "ok  $expr"
  else
    echo "ОШИБКА: запрос дашборда ничего не вернул: $expr" >&2
    failed=1
  fi
done < <(jq -r '.panels[].targets[].expr' "$DASHBOARD")
[ "$failed" = 0 ]

curl -fsS --retry 30 --retry-delay 2 --retry-all-errors "$GRAFANA/api/health" | jq -e '.database == "ok"' >/dev/null
echo "ok  Grafana работает"
# Дашборд и источник данных загружены из файлов. Запрос идёт без входа: на стенде разработки анонимный просмотр включён.
title="$(curl -fsS "$GRAFANA/api/dashboards/uid/taskflow" | jq -r .dashboard.title)"
[ "$title" = TaskFlow ] || { echo "ОШИБКА: дашборд не загружен" >&2; exit 1; }
echo "ok  дашборд TaskFlow загружен"
