package promutil

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

//TODO:
// 2. offer different With method to produce factory for framework/user master/user worker
// 4. unregister metric when master/worker suicide
// 5. job_type register
// 6. framework metric

const (
	// constLableTenantKey and constLabelProjectKey is used to recognize metric for tenant/project
	constLableTenantKey  = "tenant"
	constLabelProjectKey = "project_id"
	// constLabelJobKey is used to recognize jobs of the same job type
	constLableJobKey = "job_id"
	// constLabelJobKey is used to recognize workers of the same job
	constLableWorkerKey = "worker_id"
)

type Registry struct {
	sync.Mutex
	prometheus.Registry
	
	collectorByWorker map[WorkerID][]Prometheus.Collector
}

// Register implements Registerer.
func (r *Registry) Register(c Collector) error {
	r.Lock()
	defer r.Unlock()

	if err := r.Registry.Register(c); err != nil {
		return err; // TODO: wrap
	}


}


// NOTICE: we don't use prometheus.DefaultRegistry in case of incorrect usage of a non-standard metric
var (
	globalMetricRegistry                     = Register prometheus.NewRegistry()
	globalMetricGatherer prometheus.Gatherer = globalMetricRegistry
)

func init() {
	globalMetricRegistry.MustRegister(collectors.NewProcessCollector(ProcessCollectorOpts{}))
	globalMetricRegistry.MustRegister(collectors.NewGoCollector())
}

// HttpHandlerForMetric return http.Handler for prometheus metric
func HttpHandlerForMetric() http.Handler {
	return promhttp.HandlerFor(
		globalMetricGatherer,
		HandlerOpts{},
	)
}

// With return a Factory which can produce native prometheus metric with tenant/task const lables
func With(tenantID tenant.TenantID, jobID JobID) Factory {
	return &wrappingFactory{
		r: globalMetricRegistry,
		labels: prometheus.Labels{
			lableTenantKey:  tenantID,
			labelProjectKey: xxx,
			lableJobKey:     jobID,
		},
	}
}

type wrappingFactory struct {
	r prometheus.Registerer
	// workerID identify which worker the factory owns
	// It's used to unregister all collectors when worker(jobmaster/worker) commit suicide 
	workerID WorkerID
	// prefix is added to the metric name to avoid cross app metric conflict
	// e.g. $prefix_$namespace_$subsystem_$name
	// TODO: we can use diiferent registry for different job type to avoid cross app metric conflict
	// need some investigate
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
		f.r.MustRegister(&prometheus.wrappingCollector{
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
		f.r.MustRegister(&prometheus.wrappingCollector{
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
		f.r.MustRegister(&prometheus.wrappingCollector{
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
		f.r.MustRegister(&prometheus.wrappingCollector{
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
		f.r.MustRegister(&prometheus.wrappingCollector{
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
		f.r.MustRegister(&prometheus.wrappingCollector{
			wrappedCollector: c,
			prefix:           f.prefix,
			labels:           f.labels,
		})
	}
	return c
}
