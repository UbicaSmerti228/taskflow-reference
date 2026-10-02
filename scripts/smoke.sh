#!/usr/bin/env bash
# Проверка запущенного сервиса: проходит главный путь пользователя через HTTP.
# Использование: scripts/smoke.sh [адрес]   по умолчанию http://localhost:8080
# Для самоподписанного сертификата: CURL_OPTS=-k scripts/smoke.sh https://localhost
set -euo pipefail

BASE="${1:-http://localhost:8080}"
EMAIL="smoke-$(date +%s)-$RANDOM@example.com"
CREDS="{\"email\":\"$EMAIL\",\"password\":\"correct horse\"}"

# call МЕТОД ПУТЬ [ТЕЛО] [ТОКЕН] — печатает тело ответа, последней строкой — статус.
call() {
  local args=(-sS ${CURL_OPTS:-} -X "$1" -w '\n%{http_code}' -H 'Content-Type: application/json')
  [ -n "${3:-}" ] && args+=(-d "$3")
  [ -n "${4:-}" ] && args+=(-H "Authorization: Bearer $4")
  curl "${args[@]}" "$BASE$2"
}

# expect СТАТУС ОПИСАНИЕ ОТВЕТ — сверяет статус и печатает тело без него.
expect() {
  local status body
  status="$(tail -n1 <<<"$3")"
  body="$(sed '$d' <<<"$3")"
  if [ "$status" != "$1" ]; then
    echo "ОШИБКА: $2: статус $status, ожидался $1" >&2
    echo "$body" >&2
    exit 1
  fi
  echo "ok  $2 ($status)" >&2
  echo "$body"
}

echo "Проверяю $BASE" >&2
expect 200 "готовность" "$(call GET /readyz)" >/dev/null
expect 401 "задачи без токена закрыты" "$(call GET /api/v1/tasks)" >/dev/null
expect 201 "регистрация" "$(call POST /api/v1/auth/register "$CREDS")" >/dev/null
expect 409 "повторная регистрация отклонена" "$(call POST /api/v1/auth/register "$CREDS")" >/dev/null

TOKENS="$(expect 200 "вход" "$(call POST /api/v1/auth/login "$CREDS")")"
ACCESS="$(jq -r .access_token <<<"$TOKENS")"
REFRESH="$(jq -r .refresh_token <<<"$TOKENS")"

TASK="$(expect 201 "создание задачи" "$(call POST /api/v1/tasks '{"title":"проверка сервиса"}' "$ACCESS")")"
ID="$(jq -r .id <<<"$TASK")"

# Уведомление проходит всю цепочку: outbox → Kafka → notifier → gRPC → этот запрос.
# Она асинхронная, поэтому ждём: событие публикуется раз в секунду, читатель Kafka входит в группу несколько секунд.
for attempt in $(seq 1 60); do
  FEED="$(call GET /api/v1/notifications "" "$ACCESS")"
  if [ "$(tail -n1 <<<"$FEED")" = 200 ] && [ "$(sed '$d' <<<"$FEED" | jq --argjson id "$ID" '[.items[] | select(.kind == "task.created" and .task_id == $id)] | length')" = 1 ]; then
    echo "ok  уведомление о новой задаче дошло через Kafka (попытка $attempt)" >&2
    break
  fi
  if [ "$attempt" = 60 ]; then
    echo "ОШИБКА: уведомление не появилось за 60 секунд, последний ответ:" >&2
    echo "$FEED" >&2
    exit 1
  fi
  sleep 1
done

expect 200 "чтение задачи" "$(call GET "/api/v1/tasks/$ID" "" "$ACCESS")" >/dev/null
expect 200 "чтение задачи из кэша" "$(call GET "/api/v1/tasks/$ID" "" "$ACCESS")" >/dev/null
expect 200 "изменение задачи" "$(call PATCH "/api/v1/tasks/$ID" '{"done":true}' "$ACCESS")" >/dev/null

DONE="$(expect 200 "после изменения кэш не отдаёт старое" "$(call GET "/api/v1/tasks/$ID" "" "$ACCESS")")"
[ "$(jq -r .done <<<"$DONE")" = "true" ] || { echo "ОШИБКА: задача не отмечена выполненной: $DONE" >&2; exit 1; }

LIST="$(expect 200 "список задач" "$(call GET '/api/v1/tasks?done=true' "" "$ACCESS")")"
[ "$(jq -r .total <<<"$LIST")" = "1" ] || { echo "ОШИБКА: в списке не одна задача: $LIST" >&2; exit 1; }

NEW="$(expect 200 "обновление токенов" "$(call POST /api/v1/auth/refresh "{\"refresh_token\":\"$REFRESH\"}")")"
expect 401 "старый refresh-токен не работает" "$(call POST /api/v1/auth/refresh "{\"refresh_token\":\"$REFRESH\"}")" >/dev/null
expect 204 "удаление задачи новым токеном" "$(call DELETE "/api/v1/tasks/$ID" "" "$(jq -r .access_token <<<"$NEW")")" >/dev/null
expect 204 "выход" "$(call POST /api/v1/auth/logout "{\"refresh_token\":\"$(jq -r .refresh_token <<<"$NEW")\"}")" >/dev/null

echo "Все проверки прошли" >&2
