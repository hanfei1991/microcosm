package promutil

import (
	"testing"

	"github.com/hanfei1991/microcosm/pkg/tenant"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestWrapCounterOpts(t *testing.T) {
	t.Parallel()

	cases := []struct {
		prefix      string
		constLabels prometheus.Labels
		inputOpts   *prometheus.CounterOpts
		outputOpts  *prometheus.CounterOpts
	}{
		{
			prefix: "",
			inputOpts: &prometheus.CounterOpts{
				Name: "test",
			},
			outputOpts: &prometheus.CounterOpts{
				Name: "test",
			},
		},
		{
			prefix: "DM",
			inputOpts: &prometheus.CounterOpts{
				Namespace: "ns",
				Name:      "test",
			},
			outputOpts: &prometheus.CounterOpts{
				Namespace: "DM_ns",
				Name:      "test",
			},
		},
		{
			constLabels: prometheus.Labels{
				"k2": "v2",
			},
			inputOpts: &prometheus.CounterOpts{
				ConstLabels: prometheus.Labels{
					"k0": "v0",
					"k1": "v1",
				},
			},
			outputOpts: &prometheus.CounterOpts{
				ConstLabels: prometheus.Labels{
					"k0": "v0",
					"k1": "v1",
					"k2": "v2",
				},
			},
		},
	}

	for _, c := range cases {
		output := wrapCounterOpts(c.prefix, c.constLabels, c.inputOpts)
		require.Equal(t, c.outputOpts, output)
	}
}

func TestWrapCounterOptsLableDuplicate(t *testing.T) {
	t.Parallel()

	defer func() {
		err := recover()
		require.NotNil(t, err)
		require.Regexp(t, "duplicate label name", err.(string))
	}()

	constLabels := prometheus.Labels{
		"k0": "v0",
	}
	inputOpts := &prometheus.CounterOpts{
		ConstLabels: prometheus.Labels{
			"k0": "v0",
			"k1": "v1",
		},
	}
	_ = wrapCounterOpts("", constLabels, inputOpts)
	// unreachable
	require.True(t, false)
}

func TestNewCounter(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	require.NotNil(t, reg)

	tenant := tenant.ProjectInfo{
		TenantID:  "user0",
		ProjectID: "project0",
	}
	labelKey := "k0"
	labelValue := "v0"
	jobType := "DM"
	jobID := "job0"
	jobKey := constLabelJobKey
	projectKey := constLabelProjectKey
	tenantKey := constLabelTenantKey

	factory := NewFactory4JobMasterImpl(
		reg,
		tenant,
		jobType,
		jobID,
	)
	counter := factory.NewCounter(prometheus.CounterOpts{
		Namespace: "dm",
		Subsystem: "syncer",
		Name:      "http_request",
		ConstLabels: prometheus.Labels{
			labelKey: labelValue, // user defined const labels
		},
	})
	counter.Inc()
	counter.Add(float64(10))
	var (
		out dto.Metric
		t3  = float64(11)
	)

	require.Nil(t, counter.Write(&out))
	compareMetric(t, &dto.Metric{
		Label: []*dto.LabelPair{
			// all const labels
			{
				Name:  &jobKey,
				Value: &jobID,
			},
			{
				Name:  &labelKey,
				Value: &labelValue,
			},
			{
				Name:  &projectKey,
				Value: &tenant.ProjectID,
			},
			{
				Name:  &tenantKey,
				Value: &tenant.TenantID,
			},
		},
		Counter: &dto.Counter{
			Value: &t3,
		},
	},
		&out,
	)

	// different jobID of the same project, but with same metric
	jobID = "job1"
	factory = NewFactory4JobMasterImpl(
		reg,
		tenant,
		jobType,
		jobID,
	)
	counter = factory.NewCounter(prometheus.CounterOpts{
		Namespace: "dm",
		Subsystem: "syncer",
		Name:      "http_request",
		ConstLabels: prometheus.Labels{
			labelKey: labelValue, // user defined const labels
		},
	})

	// different project but with same metric
	tenant.ProjectID = "project1"
	factory = NewFactory4JobMasterImpl(
		reg,
		tenant,
		jobType,
		jobID,
	)
	counter = factory.NewCounter(prometheus.CounterOpts{
		Namespace: "dm",
		Subsystem: "syncer",
		Name:      "http_request",
		ConstLabels: prometheus.Labels{
			labelKey: labelValue, // user defined const labels
		},
	})

	// JobMaster and Worker of the same job type can't has same
	// metric name
	workerID := "worker0"
	factory = NewFactory4WorkerImpl(
		reg,
		tenant,
		jobType,
		jobID,
		workerID,
	)
	counter = factory.NewCounter(prometheus.CounterOpts{
		Namespace: "dm",
		Subsystem: "worker",
		Name:      "http_request",
		ConstLabels: prometheus.Labels{
			labelKey: labelValue, // user defined const labels
		},
	})

	// different workerID of the same job, but with same metric
	workerID = "worker1"
	factory = NewFactory4WorkerImpl(
		reg,
		tenant,
		jobType,
		jobID,
		workerID,
	)
	counter = factory.NewCounter(prometheus.CounterOpts{
		Namespace: "dm",
		Subsystem: "worker",
		Name:      "http_request",
		ConstLabels: prometheus.Labels{
			labelKey: labelValue, // user defined const labels
		},
	})

	// framework with same metric
	factory = NewFactory4FrameworkImpl(
		reg,
	)
	counter = factory.NewCounter(prometheus.CounterOpts{
		Namespace: "dm",
		Subsystem: "worker",
		Name:      "http_request",
		ConstLabels: prometheus.Labels{
			labelKey: labelValue, // user defined const labels
		},
	})
}

// const label conflict with inner const labels
func TestNewCounterFailConstLabelConflict(t *testing.T) {
	t.Parallel()

	defer func() {
		err := recover()
		require.NotNil(t, err)
		require.Regexp(t, "duplicate label name", err.(string))
	}()

	reg := NewRegistry()
	require.NotNil(t, reg)

	factory := NewFactory4JobMasterImpl(
		reg,
		tenant.ProjectInfo{
			TenantID:  "user0",
			ProjectID: "proj0",
		},
		"DM",
		"job0",
	)
	_ = factory.NewCounter(prometheus.CounterOpts{
		Namespace: "dm",
		Subsystem: "worker",
		Name:      "http_request",
		ConstLabels: prometheus.Labels{
			constLabelJobKey: "job0", // conflict with inner const labels
		},
	})
}

func TestNewCounterVec(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	require.NotNil(t, reg)

	factory := NewFactory4JobMasterImpl(
		reg,
		tenant.ProjectInfo{
			TenantID:  "user0",
			ProjectID: "proj0",
		},
		"DM",
		"job0",
	)
	counterVec := factory.NewCounterVec(prometheus.CounterOpts{
		Namespace: "dm",
		Subsystem: "worker",
		Name:      "http_request",
		ConstLabels: prometheus.Labels{
			"k1": "v1",
		},
	},
		[]string{"k2", "k3", "k4"},
	)

	counter, err := counterVec.GetMetricWithLabelValues([]string{"v1", "v2", "v3"}...)
	require.NoError(t, err)
	counter.Inc()

	// unmatch label values count
	_, err = counterVec.GetMetricWithLabelValues([]string{"v1", "v2"}...)
	require.Error(t, err)

	counter, err = counterVec.GetMetricWith(prometheus.Labels{
		"k2": "v2", "k3": "v3", "k4": "v4",
	})
	require.NoError(t, err)
	counter.Inc()

	// unmatch label values count
	counter, err = counterVec.GetMetricWith(prometheus.Labels{
		"k3": "v3", "k4": "v4",
	})
	require.Error(t, err)

	require.True(t, counterVec.DeleteLabelValues([]string{"v1", "v2", "v3"}...))
	require.False(t, counterVec.DeleteLabelValues([]string{"v1", "v2"}...))
	require.False(t, counterVec.DeleteLabelValues([]string{"v1", "v2", "v4"}...))

	require.True(t, counterVec.Delete(prometheus.Labels{
		"k2": "v2", "k3": "v3", "k4": "v4",
	}))
	require.False(t, counterVec.Delete(prometheus.Labels{
		"k3": "v3", "k4": "v4",
	}))
	require.False(t, counterVec.Delete(prometheus.Labels{
		"k2": "v3", "k3": "v3", "k4": "v4",
	}))

	curryCounterVec, err := counterVec.CurryWith(prometheus.Labels{
		"k2": "v2",
	})
	require.NoError(t, err)
	counter, err = curryCounterVec.GetMetricWith(prometheus.Labels{
		"k3": "v3", "k4": "v4",
	})
	require.NoError(t, err)
	counter.Add(1)

	// unmatch label values count after curry
	_, err = curryCounterVec.GetMetricWith(prometheus.Labels{
		"k2": "v2", "k3": "v3", "k4": "v4",
	})
	require.Error(t, err)
}

func compareMetric(t *testing.T, expected *dto.Metric, actual *dto.Metric) {
	// compare label pairs
	require.Equal(t, len(expected.Label), len(actual.Label))
	for i, label := range expected.Label {
		require.Equal(t, label.Name, actual.Label[i].Name)
		require.Equal(t, label.Value, actual.Label[i].Value)
	}

	if expected.Counter != nil {
		compareCounter(t, expected.Counter, actual.Counter)
	} else if expected.Gauge != nil {
		compareCounter(t, expected.Counter, actual.Counter)
	} else if expected.Histogram != nil {
		// TODO
	} else {
		require.Fail(t, "unexpected metric type")
	}
}

func compareCounter(t *testing.T, expected *dto.Counter, actual *dto.Counter) {
	require.NotNil(t, expected)
	require.NotNil(t, actual)
	require.Equal(t, expected.Value, actual.Value)
	if expected.Exemplar == nil {
		require.Nil(t, actual.Exemplar)
	} else {
		require.Equal(t, len(expected.Exemplar.Label), len(actual.Exemplar.Label))
		for i, label := range expected.Exemplar.Label {
			require.Equal(t, label.Name, actual.Exemplar.Label[i].Name)
			require.Equal(t, label.Value, actual.Exemplar.Label[i].Value)
		}
	}
}

// nolint: deadcode
func compareGauge(t *testing.T, expected *dto.Gauge, actual *dto.Gauge) {
	require.NotNil(t, expected)
	require.NotNil(t, actual)
	require.Equal(t, expected.Value, actual.Value)
}
