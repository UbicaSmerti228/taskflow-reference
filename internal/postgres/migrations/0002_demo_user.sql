-- +goose Up
-- До появления аутентификации все задачи принадлежат одному пользователю.
INSERT INTO users (email) VALUES ('demo@taskflow.local');

-- +goose Down
DELETE FROM users WHERE email = 'demo@taskflow.local';
