package promutil

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
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

// Test prometheus metric method, should not change there behavior
