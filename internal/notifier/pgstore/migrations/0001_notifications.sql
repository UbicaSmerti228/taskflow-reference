-- +goose Up
-- Таблицы сервиса notifier живут в его собственной схеме: TaskFlow их не читает и не меняет.

-- Обработанные события. Первичный ключ не даёт обработать одно событие дважды.
CREATE TABLE processed_events (
    event_id     text PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE notifications (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id    bigint      NOT NULL, -- внешнего ключа нет: пользователи живут в другом сервисе
    kind       text        NOT NULL CHECK (kind <> ''),
    task_id    bigint      NOT NULL,
    title      text        NOT NULL,
    created_at timestamptz NOT NULL
);

-- Лента пользователя: WHERE user_id = … AND id < … ORDER BY id DESC.
CREATE INDEX notifications_user_id_id_idx ON notifications (user_id, id DESC);

-- +goose Down
DROP TABLE notifications;
DROP TABLE processed_events;
