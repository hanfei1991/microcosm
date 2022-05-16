package promutil

import (
	"net/http"

	"github.com/hanfei1991/microcosm/pkg/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Routine to get a Factory:
// 1. Servermaster/Executor maintains a process-level prometheus.Registerer singleton.
// 2. 'BaseMaster/BaseWorker' interface offers a method 'func PromFactory() Factory'.
// 3. When app implements 'MasterImpl/WorkerImpl', it can get a Factory object by BaseWorker.PromFactory().
// Actually, the return Factory object would be the wrappingFactory which can produce prometheus metric object
// with tenant and task information of dataflow engine.
// 4. App uses Factory.NewCounter(xxx) to produce the native prometheus object without any concern about the
// registration and http handler. Similar to usage of promauto.

//TODO:
// 2. offer different With method to produce factory for framework/user master/user worker
// 5. job_type register
// 6. framework metric

const (
	systemID    = "system"
	frameworkID = "dateflow-framework"
)

const (
	// constLableTenantKey and constLabelProjectKey is used to recognize metric for tenant/project
	constLableTenantKey  = "tenant"
	constLabelProjectKey = "project_id"
	// constLabelJobKey is used to recognize jobs of the same job type
	constLableJobKey = "job_id"
	// constLabelWorkerKey is used to recognize workers of the same job
	constLableWorkerKey = "worker_id"
)

// HttpHandlerForMetric return http.Handler for prometheus metric
func HttpHandlerForMetric() http.Handler {
	return promhttp.HandlerFor(
		globalMetricGatherer,
		HandlerOpts{},
	)
}

// With return a Factory which can produce native prometheus metric with tenant/task const lables
// For the jobmaster
func With(tenantID tenant.TenantID, jobID JobID) Factory {
	return &wrappingFactory{
		r: globalMetricRegistry,
		labels: prometheus.Labels{
			lableTenantKey:  tenantID,
			labelProjectKey: "",
			lableJobKey:     jobID,
		},
	}
}
