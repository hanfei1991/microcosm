package promutil

import (
	"testing"

	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/pkg/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestNewFactory4JobMaster(t *testing.T) {
	cases := []struct {
		info    tenant.ProjectInfo
		jobType libModel.JobType
		jobID   libModel.MasterID
		output  Factory
	}{
		{
			info: tenant.ProjectInfo{
				TenantID:  "user0",
				ProjectID: "project0",
			},
			jobType: "DM",
			jobID:   "job0",
			output: &wrappingFactory{
				r:      globalMetricRegistry,
				prefix: "DM",
				id:     "job0",
				constLabels: prometheus.Labels{
					constLableTenantKey:  "user0",
					constLabelProjectKey: "project0",
					constLabelJobKey:     "job0",
				},
			},
		},
	}

	for _, c := range cases {
		f := NewFactory4JobMaster(c.info, c.jobType, c.jobID)
		require.Equal(t, c.output, f)
	}
}

func TestNewFactory4Worker(t *testing.T) {
	cases := []struct {
		info     tenant.ProjectInfo
		jobType  libModel.JobType
		jobID    libModel.MasterID
		workerID libModel.WorkerID
		output   Factory
	}{
		{
			info: tenant.ProjectInfo{
				TenantID:  "user0",
				ProjectID: "project0",
			},
			jobType:  "DM",
			jobID:    "job0",
			workerID: "worker0",
			output: &wrappingFactory{
				r:      globalMetricRegistry,
				prefix: "DM",
				id:     "worker0",
				constLabels: prometheus.Labels{
					constLableTenantKey:  "user0",
					constLabelProjectKey: "project0",
					constLabelJobKey:     "job0",
					constLabelWorkerKey:  "worker0",
				},
			},
		},
	}

	for _, c := range cases {
		f := NewFactory4Worker(c.info, c.jobType, c.jobID, c.workerID)
		require.Equal(t, c.output, f)
	}
}

func TestNewFactory4Framework(t *testing.T) {
	cases := []struct {
		output Factory
	}{
		{
			output: &wrappingFactory{
				r:  globalMetricRegistry,
				id: frameworkID,
				constLabels: prometheus.Labels{
					constLabelFrameworkKey: "true",
				},
			},
		},
	}

	for _, c := range cases {
		f := NewFactory4Framework()
		require.Equal(t, c.output, f)
	}
}
