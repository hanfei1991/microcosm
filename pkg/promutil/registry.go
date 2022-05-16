package promutil

// NOTICE: we don't use prometheus.DefaultRegistry in case of incorrect usage of a 
// non-wrapped metrici by app(user)
var (
	globalMetricRegistry                     = NewRegistry()
	globalMetricGatherer prometheus.Gatherer = globalMetricRegistry
)

// Registry is used for registering metric
type Registry struct {
	sync.Mutex // TODO: what kind of lock??
	*prometheus.Registry

	// collectorByWorker is for cleaning all collectors for specific worker(jobmaster/worker)
	// when it commits suicide if lost heartbeat
	collectorByWorker map[libModel.WorkerID][]Prometheus.Collector
}

// NewRegistry new a Registry
func NewRegistry() *Registry {
	return &Registry {
		prometheus.Registry: prometheus.NewRegistry(),
		collectorByWorker: make(map[libModel.WorkerID][]Prometheus.Collector)
	}
}

// Register registers the provided Collector of the specified worker
func (r *Registry) MustRegister(workerID libModel.WorkerID, c Collector) error {
	r.Lock()
	defer r.Unlock()

	r.Registry.MustRegister(c)
	
	var (
		cls []Prometheus.Collector
		exists bool
	)
	cls, exists = r.collectorByWorker[workerID]	
	if !exists {
		cls = make([]Prometheus.Collector)	
		r.collectorByWorker[workerID] = cls
	}
	cls = cls.append(c) 
}

// Unregister unregisters all Collectors of the specified worker
func (r *Registry) Unregister(workerID libModel.WorkerID) {
	r.Lock()
	defer r.Unlock()

	cls, exists := r.collectorByWorker[workerID]
	if exists {
		for collector := range cls {
			r.Registry.Unregister(collector)
		}
		delete(r.collectorByWorker, workerID)
	}
}

// Gather implements Gatherer interface
func (r *Registry) Gather() ([]*dto.MetricFamily, err) {
	r.Lock()
	defer r.Unlock()
	
	return r.Registry.Gather()	
}

func init() {
	globalMetricRegistry.MustRegister(systemID, collectors.NewProcessCollector(ProcessCollectorOpts{}))
	globalMetricRegistry.MustRegister(systemID, collectors.NewGoCollector())
}
