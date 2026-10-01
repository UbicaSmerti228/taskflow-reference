-- +goose Up
-- Пустой хеш означает «войти нельзя»: так у пользователя demo из прошлой версии нет пароля.
ALTER TABLE users ADD COLUMN password_hash text NOT NULL DEFAULT '';

-- Email хранится в нижнем регистре, иначе Ann@x.com и ann@x.com стали бы разными пользователями.
ALTER TABLE users ADD CONSTRAINT users_email_lower CHECK (email = lower(email));

-- +goose Down
ALTER TABLE users DROP CONSTRAINT users_email_lower;
ALTER TABLE users DROP COLUMN password_hash;
