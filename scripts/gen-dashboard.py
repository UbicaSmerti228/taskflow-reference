#!/usr/bin/env python3
"""Собирает deploy/grafana/dashboards/taskflow.json.

Дашборд описан здесь коротким списком панелей: JSON Grafana многословен, и править его руками неудобно.
Запуск: python3 scripts/gen-dashboard.py
"""
import json
import pathlib

DS = {"type": "prometheus", "uid": "prometheus"}

# (заголовок, единица измерения, [(запрос PromQL, подпись ряда)], пояснение)
PANELS = [
    # --- RED: Rate, Errors, Duration ---
    ("Запросов в секунду по маршрутам", "reqps",
     [('sum by (route) (rate(taskflow_http_requests_total[1m]))', "{{route}}")],
     "Rate. Считается по счётчику запросов: rate() даёт прирост в секунду."),
    ("Доля ответов 5xx", "percent",
     [('100 * sum(rate(taskflow_http_requests_total{status=~"5.."}[1m])) / sum(rate(taskflow_http_requests_total[1m])) or vector(0)', "ошибки")],
     "Errors. 4xx сюда не входят: это ошибки клиента. «or vector(0)» рисует ноль, когда ошибок не было вовсе."),
    ("Время ответа, 95-й перцентиль", "s",
     [('histogram_quantile(0.95, sum by (le, route) (rate(taskflow_http_request_duration_seconds_bucket[5m])))', "{{route}}")],
     "Duration. 95% запросов быстрее этого значения. Среднее скрыло бы медленный хвост."),
    ("Время запроса к базе, 95-й перцентиль", "s",
     [('histogram_quantile(0.95, sum by (le, op) (rate(taskflow_repository_call_duration_seconds_bucket[5m])))', "{{op}}")],
     "Если растёт время ответа, здесь видно, виновата ли база."),
    # --- события ---
    ("Outbox: событий в очереди", "short",
     [('taskflow_outbox_pending', "ждут публикации")],
     "Должно быть около нуля. Растёт — события не доходят до Kafka."),
    ("Outbox: публикация", "ops",
     [('rate(taskflow_outbox_published_total[1m])', "опубликовано"),
      ('rate(taskflow_outbox_publish_failures_total[1m])', "неудачные попытки")],
     "Сколько событий в секунду уходит в Kafka и как часто публикация срывается."),
    ("Notifier: события по исходу", "ops",
     [('sum by (result) (rate(notifier_events_total[1m]))', "{{result}}")],
     "saved — новое уведомление, duplicate — повторная доставка, failed — сбой обработки."),
    ("Notifier: gRPC-вызовы по коду", "ops",
     [('sum by (code) (rate(notifier_grpc_calls_total[1m]))', "{{code}}")],
     "Всё, кроме OK, заслуживает внимания."),
    # --- процесс ---
    ("Горутины", "short",
     [('go_goroutines', "{{job}}")],
     "Ровная линия — норма. Постоянный рост — утечка горутин."),
    ("Память в куче", "bytes",
     [('go_memstats_heap_inuse_bytes', "{{job}}")],
     "Пила — работа сборщика мусора. Рост без спадов — утечка памяти."),
]


def panel(i, title, unit, targets, description):
    return {
        "id": i + 1,
        "type": "timeseries",
        "title": title,
        "description": description,
        "datasource": DS,
        "gridPos": {"h": 8, "w": 12, "x": 12 * (i % 2), "y": 8 * (i // 2)},
        "targets": [
            {"refId": chr(ord("A") + n), "datasource": DS, "expr": expr, "legendFormat": legend}
            for n, (expr, legend) in enumerate(targets)
        ],
        "fieldConfig": {"defaults": {"unit": unit, "min": 0}, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
    }


dashboard = {
    "uid": "taskflow",
    "title": "TaskFlow",
    "tags": ["taskflow"],
    "timezone": "browser",
    "schemaVersion": 39,
    "version": 1,
    "refresh": "5s",
    "time": {"from": "now-15m", "to": "now"},
    "panels": [panel(i, *p) for i, p in enumerate(PANELS)],
}

out = pathlib.Path(__file__).resolve().parent.parent / "deploy/grafana/dashboards/taskflow.json"
out.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
print(f"{out}: панелей {len(PANELS)}")
