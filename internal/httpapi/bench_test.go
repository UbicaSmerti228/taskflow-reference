package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

// Запуск: go test -bench=. -benchmem -run='^$' ./internal/httpapi
// Весь путь запроса GET /api/v1/tasks/{id}: middleware, проверка токена, сервис, кэш, JSON-ответ.
// Сети и базы здесь нет — замер показывает цену собственного кода. Выводы: docs/perf.md.
func BenchmarkGetTask(b *testing.B) {
	e := newEnv(b)
	token := e.login("ann@example.com").AccessToken
	rec := e.do("POST", "/api/v1/tasks", token, `{"title":"сдать квартальный отчёт"}`)
	if rec.Code != http.StatusCreated {
		b.Fatalf("create: status %d", rec.Code)
	}
	path := "/api/v1/tasks/" + strconv.FormatInt(decode[struct{ ID int64 }](b, rec).ID, 10)

	// Запрос собран один раз: httptest.NewRequest сам выделяет 4 КБ под буфер чтения,
	// и в цикле эта память попала бы в результат, хотя к сервису отношения не имеет.
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Authorization", "Bearer "+token)

	b.ReportAllocs()
	for b.Loop() {
		e.logs.Reset() // лог запросов пишется, как в бою, но не копится
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("status %d", rec.Code)
		}
	}
}
