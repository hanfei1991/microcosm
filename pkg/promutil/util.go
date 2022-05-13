package promutil

import (
"github.com/prometheus/client_golang/prometheus"
)

//TODO:
// 1. implement the wrappingFactory
// 2. offer different With method to produce factory for framework/user master/user worker
// 3. http handler
// 4. unregister metric when master/worker suicide
// 5. job_type register

const (
	// constLableTenantKey and constLabelProjectKey is used to 
	// recognize metric for tenant/project
    constLableTenantKey = "tenant"
    constLabelProjectKey = "project_id"
	// constLabelJobKey is used to recognize job of the same job type
    constLableJobKey = "job_id"
	// constLabelJobKey is used to recognize worker of the same job
	constLableWorkerKey = "worker_id"
)

func With(r prometheus.Registerer，tenantID TenantID, jobID JobID) Factory { 
    return &wrappingFactory{
        r: r,
        labels: prometheus.Labels{
            lableTenantKey: tenantID,
            labelProjectKey: xxx,
            lableJobKey: jobID,
        }
    } 
}

type wrappingFactory struct {
    r prometheus.Registerer
    // constLabels is added to user metric by default to avoid metric conflict
	constLabels prometheus.Labels
	// prefix is added to the metric name to avoid cross app metric conflict
	// e.g. $prefix_$namespace_$subsystem_$name
	prefix string
}

func (f * wrappingFactory)NewCounter(opts prometheus.CounterOpts) prometheus.Counter {
        c := prometheus.NewCounter(opts)
        if f.r != nil {
             f.r.MustRegister(&prometheus.wrappingCollector{
                wrappedCollector: c,
                labels:           r.labels,
                })
        }
        return c
}

