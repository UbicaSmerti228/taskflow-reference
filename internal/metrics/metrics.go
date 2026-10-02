// Package metrics описывает метрики Prometheus обоих сервисов.
//
// Остальные пакеты про Prometheus не знают: каждый объявляет у себя маленький интерфейс
// (outbox.Metrics, httpapi.Metrics и так далее), а типы отсюда их реализуют.
package metrics

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// NewRegistry создаёт реестр с метриками самого процесса: горутины, память, сборщик мусора, файловые дескрипторы.
// Реестр свой, а не глобальный: в него попадает только то, что зарегистрировано явно, и тесты не мешают друг другу.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return reg
}

// Границы корзин гистограмм в секундах: от 1 мс до 5 с. Запросы сервиса укладываются в этот диапазон;
// если границы не подходят под реальные времена, перцентили будут грубыми.
var buckets = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

// HTTP — метрики HTTP-сервера по методу RED: частота запросов, ошибки, длительность.
type HTTP struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

// NewHTTP регистрирует метрики HTTP-сервера.
func NewHTTP(reg prometheus.Registerer) *HTTP {
	m := &HTTP{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskflow_http_requests_total",
			Help: "Число обработанных HTTP-запросов.",
		}, []string{"method", "route", "status"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "taskflow_http_request_duration_seconds",
			Help:    "Время обработки HTTP-запроса.",
			Buckets: buckets,
		}, []string{"method", "route"}),
	}
	reg.MustRegister(m.requests, m.duration)
	return m
}

// Request учитывает один запрос. route — шаблон маршрута (/api/v1/tasks/:id), а не путь с id:
// иначе на каждый id появился бы свой временной ряд, и Prometheus захлебнулся бы.
func (m *HTTP) Request(method, route string, status int, took time.Duration) {
	m.requests.WithLabelValues(method, route, strconv.Itoa(status)).Inc()
	m.duration.WithLabelValues(method, route).Observe(took.Seconds())
}

// Repo — метрики обращений к хранилищу.
type Repo struct {
	duration *prometheus.HistogramVec
	errors   *prometheus.CounterVec
}

// NewRepo регистрирует метрики хранилища.
func NewRepo(reg prometheus.Registerer) *Repo {
	m := &Repo{
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "taskflow_repository_call_duration_seconds",
			Help:    "Время одного обращения к хранилищу.",
			Buckets: buckets,
		}, []string{"op"}),
		errors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "taskflow_repository_errors_total",
			Help: "Число обращений к хранилищу, закончившихся сбоем.",
		}, []string{"op"}),
	}
	reg.MustRegister(m.duration, m.errors)
	return m
}

// Call учитывает одно обращение к хранилищу.
func (m *Repo) Call(op string, failed bool, took time.Duration) {
	m.duration.WithLabelValues(op).Observe(took.Seconds())
	if failed {
		m.errors.WithLabelValues(op).Inc()
	}
}

// Outbox — метрики публикации событий.
type Outbox struct {
	published prometheus.Counter
	failures  prometheus.Counter
	pending   prometheus.Gauge
}

// NewOutbox регистрирует метрики публикации событий.
func NewOutbox(reg prometheus.Registerer) *Outbox {
	m := &Outbox{
		published: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "taskflow_outbox_published_total",
			Help: "Число событий, опубликованных в Kafka.",
		}),
		failures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "taskflow_outbox_publish_failures_total",
			Help: "Число неудачных попыток публикации.",
		}),
		// Главная метрика outbox: если она растёт, события не доходят до Kafka.
		pending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "taskflow_outbox_pending",
			Help: "Число событий, которые ждут публикации.",
		}),
	}
	reg.MustRegister(m.published, m.failures, m.pending)
	return m
}

// Published учитывает опубликованные события.
func (m *Outbox) Published(n int) { m.published.Add(float64(n)) }

// Failed учитывает неудачную попытку публикации.
func (m *Outbox) Failed() { m.failures.Inc() }

// Pending запоминает длину очереди.
func (m *Outbox) Pending(n int) { m.pending.Set(float64(n)) }

// Events — метрики обработки событий в notifier.
type Events struct {
	total *prometheus.CounterVec
}

// NewEvents регистрирует метрики обработки событий.
func NewEvents(reg prometheus.Registerer) *Events {
	m := &Events{total: prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "notifier_events_total",
		Help: "Число событий из Kafka по типу и исходу обработки.",
	}, []string{"type", "result"})}
	reg.MustRegister(m.total)
	return m
}

// Event учитывает одно событие. result: saved, duplicate, ignored, malformed или failed.
func (m *Events) Event(eventType, result string) { m.total.WithLabelValues(eventType, result).Inc() }

// GRPC — метрики gRPC-сервера.
type GRPC struct {
	calls    *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

// NewGRPC регистрирует метрики gRPC-сервера.
func NewGRPC(reg prometheus.Registerer) *GRPC {
	m := &GRPC{
		calls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifier_grpc_calls_total",
			Help: "Число завершённых gRPC-вызовов.",
		}, []string{"method", "code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "notifier_grpc_call_duration_seconds",
			Help:    "Время обработки gRPC-вызова. У стрима — время его жизни.",
			Buckets: buckets,
		}, []string{"method"}),
	}
	reg.MustRegister(m.calls, m.duration)
	return m
}

// Call учитывает один завершённый вызов.
func (m *GRPC) Call(method, code string, took time.Duration) {
	m.calls.WithLabelValues(method, code).Inc()
	m.duration.WithLabelValues(method).Observe(took.Seconds())
}

// RegisterGauge регистрирует метрику, значение которой считается в момент опроса.
func RegisterGauge(reg prometheus.Registerer, name, help string, value func() float64) {
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help}, value))
}
