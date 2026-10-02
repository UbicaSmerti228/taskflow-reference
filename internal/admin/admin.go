// Package admin собирает служебный HTTP-обработчик: метрики и профилировщик.
//
// Он слушает отдельный порт, который не публикуется наружу. Профиль раскрывает устройство сервиса
// и сам нагружает процесс, а метрики выдают объёмы и адреса — посторонним это видеть незачем.
package admin

import (
	"net/http"
	"net/http/pprof"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Handler возвращает обработчик с /metrics и /debug/pprof/.
//
// Обработчики pprof подключены явно. Обычный способ — import _ "net/http/pprof" — регистрирует их
// в http.DefaultServeMux; если тот же mux обслуживает публичный порт, профилировщик окажется в интернете.
func Handler(reg prometheus.Gatherer) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/debug/pprof/", pprof.Index) // heap, goroutine, allocs и другие профили по имени
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile) // CPU-профиль: ?seconds=30
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}
