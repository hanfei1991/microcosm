package promutil

import (
	libModel "github.com/hanfei1991/microcosm/lib/model"

	//nolint:staticcheck // Ignore SA1019. Need to keep deprecated package for compatibility.
	"github.com/prometheus/client_golang/prometheus"
)

type wrappingFactory struct {
	r *Registry
	// ID identify the worker(jobmaster/worker) the factory owns
	// It's used to unregister all collectors when worker commit suicide
	id libModel.WorkerID
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
	c := prometheus.NewCounter(*wrapCounterOpts(f.prefix, f.constLabels, &opts))
	f.r.MustRegister(f.id, c)
	return c
}

// NewCounterVec works like the function of the same name in the
// prometheus, package but it automatically registers the CounterVec with
// the Factory's Registerer. Panic if it can't register successfully.Thread-safe.
func (f *wrappingFactory) NewCounterVec(opts prometheus.CounterOpts, labelNames []string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(*wrapCounterOpts(f.prefix, f.constLabels, &opts), labelNames)
	f.r.MustRegister(f.id, c)
	return c
}

// NewGauge works like the function of the same name in the prometheus
// package, but it automatically registers the Gauge with the Factory's
// Registerer. Panic if it can't register successfully.Thread-safe.
func (f *wrappingFactory) NewGauge(opts prometheus.GaugeOpts) prometheus.Gauge {
	c := prometheus.NewGauge(*wrapGaugeOpts(f.prefix, f.constLabels, &opts))
	f.r.MustRegister(f.id, c)
	return c
}

// NewGaugeVec works like the function of the same name in the prometheus
// package but it automatically registers the GaugeVec with the Factory's
// Registerer. Panic if it can't register successfully.Thread-safe.
func (f *wrappingFactory) NewGaugeVec(opts prometheus.GaugeOpts, labelNames []string) *prometheus.GaugeVec {
	c := prometheus.NewGaugeVec(*wrapGaugeOpts(f.prefix, f.constLabels, &opts), labelNames)
	f.r.MustRegister(f.id, c)
	return c
}

// NewHistogram works like the function of the same name in the prometheus
// package but it automatically registers the Histogram with the Factory's
// Registerer. Panic if it can't register successfully.Thread-safe.
func (f *wrappingFactory) NewHistogram(opts prometheus.HistogramOpts) prometheus.Histogram {
	c := prometheus.NewHistogram(*wrapHistogramOpts(f.prefix, f.constLabels, &opts))
	f.r.MustRegister(f.id, c)
	return c
}

// NewHistogramVec works like the function of the same name in the
// prometheus package but it automatically registers the HistogramVec
// with the Factory's Registerer. Panic if it can't register successfully.Thread-safe.
func (f *wrappingFactory) NewHistogramVec(opts prometheus.HistogramOpts, labelNames []string) *prometheus.HistogramVec {
	c := prometheus.NewHistogramVec(*wrapHistogramOpts(f.prefix, f.constLabels, &opts), labelNames)
	f.r.MustRegister(f.id, c)
	return c
}

func wrapCounterOpts(prefix string, constLabels prometheus.Labels, opts *prometheus.CounterOpts) *prometheus.CounterOpts {
	opts.Namespace = prefix + "_" + opts.Namespace
	cls := opts.ConstLabels
	for name, value := range constLabels {
		if _, exists := cls[name]; exists {
			panic("duplicate label name")
		}
		cls[name] = value
	}

	return opts
}

func wrapGaugeOpts(prefix string, constLabels prometheus.Labels, opts *prometheus.GaugeOpts) *prometheus.GaugeOpts {
	opts.Namespace = prefix + "_" + opts.Namespace
	cls := opts.ConstLabels
	for name, value := range constLabels {
		if _, exists := cls[name]; exists {
			panic("duplicate label name")
		}
		cls[name] = value
	}

	return opts
}

func wrapHistogramOpts(prefix string, constLabels prometheus.Labels, opts *prometheus.HistogramOpts) *prometheus.HistogramOpts {
	opts.Namespace = prefix + "_" + opts.Namespace
	cls := opts.ConstLabels
	for name, value := range constLabels {
		if _, exists := cls[name]; exists {
			panic("duplicate label name")
		}
		cls[name] = value
	}

	return opts
}
