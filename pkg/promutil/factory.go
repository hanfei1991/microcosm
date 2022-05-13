package promutil

import "github.com/prometheus/client_golang/prometheus"

// Routine to get a Factory:
// 1. Servermaster/Executor maintains a process-level prometheus.Registerer singleton.
// 2. 'BaseMaster/BaseWorker' interface offers a method 'func PromFactory() Factory'.
// 3. When app implements 'MasterImpl/WorkerImpl', it can get a Factory object by BaseWorker.PromFactory()
// Actually, the return Factory object would be the wrappingFactory which can produce prometheus metric object
// with tenant and task information of dataflow engine.
// 4. App uses Factory.NewCounter(xxx) to produce the native prometheus object without any concern about the
// registration and http handler. Similar to usage of promauto.

type Factory interface {
	// NewCounter works like the function of the same name in the prometheus
	// package, but it automatically registers the Counter with the Factory's
	// Registerer. Panic if it can't register successfully.
	NewCounter(opts prometheus.CounterOpts) prometheus.Counter

	// NewCounterVec works like the function of the same name in the
	// prometheus, package but it automatically registers the CounterVec with
	// the Factory's Registerer. Panic if it can't register successfully.
	NewCounterVec(opts prometheus.CounterOpts, labelNames []string) *prometheus.CounterVec

	// NewGauge works like the function of the same name in the prometheus
	// package, but it automatically registers the Gauge with the Factory's
	// Registerer. Panic if it can't register successfully.
	NewGauge(opts prometheus.GaugeOpts) prometheus.Gauge

	// NewGaugeVec works like the function of the same name in the prometheus
	// package but it automatically registers the GaugeVec with the Factory's
	// Registerer. Panic if it can't register successfully.
	NewGaugeVec(opts prometheus.GaugeOpts, labelNames []string) *prometheus.GaugeVec

	// NewHistogram works like the function of the same name in the prometheus
	// package but it automatically registers the Histogram with the Factory's
	// Registerer. Panic if it can't register successfully.
	NewHistogram(opts prometheus.HistogramOpts) prometheus.Histogram

	// NewHistogramVec works like the function of the same name in the
	// prometheus package but it automatically registers the HistogramVec
	// with the Factory's Registerer. Panic if it can't register successfully.
	NewHistogramVec(opts prometheus.HistogramOpts, labelNames []string) *prometheus.HistogramVec
}
