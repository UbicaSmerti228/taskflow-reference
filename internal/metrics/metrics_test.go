package metrics

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestHTTP(t *testing.T) {
	reg := NewRegistry()
	m := NewHTTP(reg)
	m.Request("GET", "/api/v1/tasks/:id", 200, 3*time.Millisecond)
	m.Request("GET", "/api/v1/tasks/:id", 200, 30*time.Millisecond)
	m.Request("GET", "/api/v1/tasks/:id", 404, time.Millisecond)

	want := `
# HELP taskflow_http_requests_total Число обработанных HTTP-запросов.
# TYPE taskflow_http_requests_total counter
taskflow_http_requests_total{method="GET",route="/api/v1/tasks/:id",status="200"} 2
taskflow_http_requests_total{method="GET",route="/api/v1/tasks/:id",status="404"} 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "taskflow_http_requests_total"); err != nil {
		t.Error(err)
	}
	// Гистограмма считает все три запроса одним рядом: статус в неё не входит.
	if n := testutil.CollectAndCount(m.duration); n != 1 {
		t.Errorf("рядов гистограммы %d, want 1", n)
	}
}

func TestRepo(t *testing.T) {
	reg := NewRegistry()
	m := NewRepo(reg)
	m.Call("tasks.get", false, time.Millisecond)
	m.Call("tasks.get", true, time.Millisecond)
	if got := testutil.ToFloat64(m.errors.WithLabelValues("tasks.get")); got != 1 {
		t.Errorf("сбоев %v, want 1", got)
	}
}

func TestOutbox(t *testing.T) {
	reg := NewRegistry()
	m := NewOutbox(reg)
	m.Published(3)
	m.Published(2)
	m.Failed()
	m.Pending(7)
	if got := testutil.ToFloat64(m.published); got != 5 {
		t.Errorf("опубликовано %v, want 5", got)
	}
	if got := testutil.ToFloat64(m.failures); got != 1 {
		t.Errorf("отказов %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.pending); got != 7 {
		t.Errorf("в очереди %v, want 7", got)
	}
}

func TestEventsAndGRPC(t *testing.T) {
	reg := NewRegistry()
	e, g := NewEvents(reg), NewGRPC(reg)
	e.Event("task.created", "saved")
	e.Event("task.created", "duplicate")
	e.Event("task.created", "duplicate")
	g.Call("/svc/List", "OK", time.Millisecond)
	if got := testutil.ToFloat64(e.total.WithLabelValues("task.created", "duplicate")); got != 2 {
		t.Errorf("дубликатов %v, want 2", got)
	}
	if got := testutil.ToFloat64(g.calls.WithLabelValues("/svc/List", "OK")); got != 1 {
		t.Errorf("вызовов %v, want 1", got)
	}

	subscribers := 4.0
	RegisterGauge(reg, "notifier_stream_subscribers", "Число открытых стримов.", func() float64 { return subscribers })
	want := `
# HELP notifier_stream_subscribers Число открытых стримов.
# TYPE notifier_stream_subscribers gauge
notifier_stream_subscribers 4
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "notifier_stream_subscribers"); err != nil {
		t.Error(err)
	}
}

// В реестре есть метрики процесса: без них на дашборде не видно ни горутин, ни памяти.
func TestRegistryHasRuntimeMetrics(t *testing.T) {
	families, err := NewRegistry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, f := range families {
		names[f.GetName()] = true
	}
	for _, want := range []string{"go_goroutines", "go_memstats_heap_alloc_bytes"} {
		if !names[want] {
			t.Errorf("в реестре нет метрики %s", want)
		}
	}
}
