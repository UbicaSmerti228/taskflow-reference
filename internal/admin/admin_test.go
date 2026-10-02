package admin

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/UbicaSmerti228/taskflow-reference/internal/metrics"
)

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestMetrics(t *testing.T) {
	reg := metrics.NewRegistry()
	metrics.NewHTTP(reg).Request("GET", "/healthz", 200, time.Millisecond)

	rec := get(t, Handler(reg), "/metrics")
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics: status %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		`taskflow_http_requests_total{method="GET",route="/healthz",status="200"} 1`,
		`taskflow_http_request_duration_seconds_bucket{method="GET",route="/healthz",le="0.001"} 1`,
		"go_goroutines ",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("в /metrics нет строки %s", want)
		}
	}
}

func TestPprof(t *testing.T) {
	h := Handler(metrics.NewRegistry())
	if rec := get(t, h, "/debug/pprof/"); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "goroutine") {
		t.Errorf("/debug/pprof/: status %d", rec.Code)
	}
	// Профиль горутин в текстовом виде: по нему ищут утечки.
	if rec := get(t, h, "/debug/pprof/goroutine?debug=1"); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "goroutine profile") {
		t.Errorf("/debug/pprof/goroutine: status %d", rec.Code)
	}
	if rec := get(t, h, "/debug/pprof/heap"); rec.Code != http.StatusOK || rec.Body.Len() == 0 {
		t.Errorf("/debug/pprof/heap: status %d, тело %d байт", rec.Code, rec.Body.Len())
	}
	if rec := get(t, h, "/debug/pprof/profile?seconds=1"); rec.Code != http.StatusOK || rec.Body.Len() == 0 {
		t.Errorf("/debug/pprof/profile: status %d, тело %d байт", rec.Code, rec.Body.Len())
	}
	if rec := get(t, h, "/"); rec.Code != http.StatusNotFound {
		t.Errorf("/: status %d, want 404", rec.Code)
	}
}
