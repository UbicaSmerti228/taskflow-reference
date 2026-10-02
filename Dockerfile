# syntax=docker/dockerfile:1

# Сборка: полный образ Go с компилятором. Оба сервиса собираются здесь один раз.
FROM golang:1.27 AS build
WORKDIR /src

# Зависимости отдельным слоем: он пересобирается, только когда меняется go.mod или go.sum.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO_ENABLED=0 даёт статический бинарник без libc, -s -w убирают отладочную информацию.
# Тег nomsgpack выключает в Gin поддержку MessagePack: она не нужна, а весит 6 МБ.
RUN CGO_ENABLED=0 go build -tags nomsgpack -trimpath -ldflags="-s -w" -o /out/ ./cmd/taskflow ./cmd/notifier

# Запуск: в образе только бинарник, сертификаты и пользователь без прав root.
# У каждого сервиса свой образ: docker build --target notifier .
FROM gcr.io/distroless/static-debian12:nonroot AS notifier
COPY --from=build /out/notifier /notifier
EXPOSE 9000
ENTRYPOINT ["/notifier"]
CMD ["serve"]

# Последняя стадия — образ по умолчанию: docker build . собирает TaskFlow.
FROM gcr.io/distroless/static-debian12:nonroot AS taskflow
COPY --from=build /out/taskflow /taskflow
EXPOSE 8080
ENTRYPOINT ["/taskflow"]
CMD ["serve"]
