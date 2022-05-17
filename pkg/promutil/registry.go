package promutil

import (
	"sync"

	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	dto "github.com/prometheus/client_model/go"
)

// NOTICE: we don't use prometheus.DefaultRegistry in case of incorrect usage of a
// non-wrapped metrici by app(user)
var (
	globalMetricRegistry                     = NewRegistry()
	globalMetricGatherer prometheus.Gatherer = globalMetricRegistry
)

func init() {
	globalMetricRegistry.MustRegister(systemID, collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	// NOTICE: v1.12.1 revert some runtime metric for go collector
	// ref: https://github.com/prometheus/client_golang/releases
	globalMetricRegistry.MustRegister(systemID, collectors.NewGoCollector(collectors.WithGoCollections(
		collectors.GoRuntimeMemStatsCollection|collectors.GoRuntimeMetricsCollection)))
}

// Registry is used for registering metric
type Registry struct {
	sync.Mutex // TODO: what kind of lock??
	*prometheus.Registry

	// collectorByWorker is for cleaning all collectors for specific worker(jobmaster/worker)
	// when it commits suicide if lost heartbeat
	collectorByWorker map[libModel.WorkerID][]prometheus.Collector
}

// NewRegistry new a Registry
func NewRegistry() *Registry {
	return &Registry{
		Registry:          prometheus.NewRegistry(),
		collectorByWorker: make(map[libModel.WorkerID][]prometheus.Collector),
	}
}

// MustRegister registers the provided Collector of the specified worker
func (r *Registry) MustRegister(workerID libModel.WorkerID, c prometheus.Collector) {
	if c == nil {
		return
	}
	r.Lock()
	defer r.Unlock()

	r.Registry.MustRegister(c)

	var (
		cls    []prometheus.Collector
		exists bool
	)
	cls, exists = r.collectorByWorker[workerID]
	if !exists {
		cls = make([]prometheus.Collector, 0)
	}
	cls = append(cls, c)
	r.collectorByWorker[workerID] = cls
}

// Unregister unregisters all Collectors of the specified worker
func (r *Registry) Unregister(workerID libModel.WorkerID) {
	r.Lock()
	defer r.Unlock()

	cls, exists := r.collectorByWorker[workerID]
	if exists {
		for _, collector := range cls {
			r.Registry.Unregister(collector)
		}
		delete(r.collectorByWorker, workerID)
	}
}

// Gather implements Gatherer interface
func (r *Registry) Gather() ([]*dto.MetricFamily, error) {
	r.Lock()
	defer r.Unlock()

	return r.Registry.Gather()
}
