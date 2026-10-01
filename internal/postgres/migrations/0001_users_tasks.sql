-- +goose Up
CREATE TABLE users (
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email      text        NOT NULL UNIQUE CHECK (email <> ''),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE tasks (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id     bigint      NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    title       text        NOT NULL CHECK (btrim(title) <> ''),
    done        boolean     NOT NULL DEFAULT false,
    due_at      timestamptz,
    reminded_at timestamptz,
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- Список задач пользователя и пагинация по ключу: WHERE user_id = … AND id > … ORDER BY id.
CREATE INDEX tasks_user_id_id_idx ON tasks (user_id, id);

-- Обход воркера напоминаний. Индекс частичный: в него попадают только задачи, которые ещё ждут напоминания.
CREATE INDEX tasks_reminder_idx ON tasks (due_at)
    WHERE NOT done AND reminded_at IS NULL AND due_at IS NOT NULL;

-- +goose Down
DROP TABLE tasks;
DROP TABLE users;
