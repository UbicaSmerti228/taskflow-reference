-- +goose Up
-- Transactional outbox: событие пишется в той же транзакции, что и изменение задачи.
-- Отдельный воркер публикует строки в Kafka и ставит published_at.
CREATE TABLE outbox (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id     text        NOT NULL UNIQUE, -- повторная запись того же события ничего не добавит
    topic        text        NOT NULL,
    key          text        NOT NULL,
    payload      jsonb       NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);

-- Воркер читает только неопубликованные строки. Индекс частичный: он не растёт вместе с таблицей.
CREATE INDEX outbox_pending_idx ON outbox (id) WHERE published_at IS NULL;

-- +goose Down
DROP TABLE outbox;
