# syntax=docker/dockerfile:1

# Сборка: полный образ Go с компилятором.
FROM golang:1.27 AS build
WORKDIR /src

# Зависимости отдельным слоем: он пересобирается, только когда меняется go.mod или go.sum.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO_ENABLED=0 даёт статический бинарник без libc, -s -w убирают отладочную информацию.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /taskflow ./cmd/taskflow

# Запуск: в образе только бинарник, сертификаты и пользователь без прав root.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /taskflow /taskflow
EXPOSE 8080
ENTRYPOINT ["/taskflow"]
CMD ["serve", "-addr", ":8080"]
