package promutil

import "github.com/prometheus/client_golang/prometheus"

type wrappingFactory struct {
	r *Registry
	// workerID identify which worker the factory owns
	// It's used to unregister all collectors when worker(jobmaster/worker) commit suicide
	workerID WorkerID
	// prefix is added to the metric name to avoid cross app metric conflict
	// e.g. $prefix_$namespace_$subsystem_$name
	prefix string
	// constLabels is added to user metric by default to avoid metric conflict
	constLabels prometheus.Labels
}

// NewCounter works like the function of the same name in the prometheus
// package, but it automatically registers the Counter with the Factory's
// Registerer. Panic if it can't register successfully. Thread-safe.
func (f *wrappingFactory) NewCounter(opts prometheus.CounterOpts) prometheus.Counter {
	c := prometheus.NewCounter(opts)
	if f.r != nil {
		f.r.MustRegister(f.workerID, &prometheus.wrappingCollector{
			wrappedCollector: c,
			prefix:           f.prefix,
			labels:           f.labels,
		})
	}
	return c
}

// NewCounterVec works like the function of the same name in the
// prometheus, package but it automatically registers the CounterVec with
// the Factory's Registerer. Panic if it can't register successfully.Thread-safe.
func (f *wrappingFactory) NewCounterVec(opts prometheus.CounterOpts, labelNames []string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(opts, labelNames)
	if f.r != nil {
		f.r.MustRegister(f.workerID, &prometheus.wrappingCollector{
			wrappedCollector: c,
			prefix:           f.prefix,
			labels:           f.labels,
		})
	}
	return c
}

// NewGauge works like the function of the same name in the prometheus
// package, but it automatically registers the Gauge with the Factory's
// Registerer. Panic if it can't register successfully.Thread-safe.
func (f *wrappingFactory) NewGauge(opts prometheus.GaugeOpts) prometheus.Gauge {
	c := prometheus.NewGauge(opts)
	if f.r != nil {
		f.r.MustRegister(f.workerID, &prometheus.wrappingCollector{
			wrappedCollector: c,
			prefix:           f.prefix,
			labels:           f.labels,
		})
	}
	return c
}

// NewGaugeVec works like the function of the same name in the prometheus
// package but it automatically registers the GaugeVec with the Factory's
// Registerer. Panic if it can't register successfully.Thread-safe.
func (f *wrappingFactory) NewGaugeVec(opts prometheus.GaugeOpts, labelNames []string) *prometheus.GaugeVec {
	c := prometheus.NewGaugeVec(opts, labelNames)
	if f.r != nil {
		f.r.MustRegister(f.workerID, &prometheus.wrappingCollector{
			wrappedCollector: c,
			prefix:           f.prefix,
			labels:           f.labels,
		})
	}
	return c
}

// NewHistogram works like the function of the same name in the prometheus
// package but it automatically registers the Histogram with the Factory's
// Registerer. Panic if it can't register successfully.Thread-safe.
func (f *wrappingFactory) NewHistogram(opts prometheus.HistogramOpts) prometheus.Histogram {
	c := prometheus.NewHistogram(opts)
	if f.r != nil {
		f.r.MustRegister(f.workerID, &prometheus.wrappingCollector{
			wrappedCollector: c,
			prefix:           f.prefix,
			labels:           f.labels,
		})
	}
	return c
}

// NewHistogramVec works like the function of the same name in the
// prometheus package but it automatically registers the HistogramVec
// with the Factory's Registerer. Panic if it can't register successfully.Thread-safe.
func (f *wrappingFactory) NewHistogramVec(opts prometheus.HistogramOpts, labelNames []string) *prometheus.HistogramVec {
	c := prometheus.NewHistogramVec(opts, labelNames)
	if f.r != nil {
		f.r.MustRegister(f.workerID, &prometheus.wrappingCollector{
			wrappedCollector: c,
			prefix:           f.prefix,
			labels:           f.labels,
		})
	}
	return c
}
