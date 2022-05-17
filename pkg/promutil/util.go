package promutil

import (
	"net/http"

	libModel "github.com/hanfei1991/microcosm/lib/model"
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

const (
	systemID    = "dataflow-system"
	frameworkID = "dateflow-framework"
)

const (
	/// framework const lable
	constLabelFrameworkKey = "framework"

	/// app const label
	// constLableTenantKey and constLabelProjectKey is used to recognize metric for tenant/project
	constLableTenantKey  = "tenant"
	constLabelProjectKey = "project_id"
	// constLabelJobKey is used to recognize jobs of the same job type
	constLableJobKey = "job_id"
	// constLabelWorkerKey is used to recognize workers of the same job
	constLableWorkerKey = "worker_id"
)

// HttpHandlerForMetric return http.Handler for prometheus metric
func HTTPHandlerForMetric() http.Handler {
	return promhttp.HandlerFor(
		globalMetricGatherer,
		promhttp.HandlerOpts{},
	)
}

// NewFactory4JobMaster return a Factory for jobmaster
// TODO: jobType need format
func NewFactory4JobMaster(info tenant.ProjectInfo, jobType libModel.JobType, jobID libModel.MasterID) Factory {
	return &wrappingFactory{
		r:      globalMetricRegistry,
		prefix: jobType,
		constLabels: prometheus.Labels{
			constLableTenantKey:  info.TenantID,
			constLabelProjectKey: info.ProjectID,
			constLableJobKey:     jobID,
		},
	}
}

// NewFactory4Worker return a Factory for worker
// TODO: jobType need format
func NewFactory4Worker(info tenant.ProjectInfo, jobType libModel.JobType, jobID libModel.MasterID,
	workerID libModel.WorkerID) Factory {
	return &wrappingFactory{
		r:      globalMetricRegistry,
		prefix: jobType,
		constLabels: prometheus.Labels{
			constLableTenantKey:  info.TenantID,
			constLabelProjectKey: info.ProjectID,
			constLableJobKey:     jobID,
			constLableWorkerKey:  workerID,
		},
	}
}

// NewFactory4Framework return a Factory for dataflow framework
// NOTICE: we use auto service label to distinguish different dataflow engine
// or different executor
func NewFactory4Framework() Factory {
	return &wrappingFactory{
		r: globalMetricRegistry,
		constLabels: prometheus.Labels{
			constLabelFrameworkKey: "true",
		},
	}
}
