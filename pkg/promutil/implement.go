package promutil

import (
	"fmt"
	"sort"
	"strings"

	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/pkg/promutil/internal"

	//nolint:staticcheck // Ignore SA1019. Need to keep deprecated package for compatibility.
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
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
	c := prometheus.NewCounter(opts)
	if f.r != nil {
		f.r.MustRegister(f.id, &wrappingCollector{
			wrappedCollector: c,
			prefix:           f.prefix,
			labels:           f.constLabels,
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
		f.r.MustRegister(f.id, &wrappingCollector{
			wrappedCollector: c,
			prefix:           f.prefix,
			labels:           f.constLabels,
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
		f.r.MustRegister(f.id, &wrappingCollector{
			wrappedCollector: c,
			prefix:           f.prefix,
			labels:           f.constLabels,
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
		f.r.MustRegister(f.id, &wrappingCollector{
			wrappedCollector: c,
			prefix:           f.prefix,
			labels:           f.constLabels,
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
		f.r.MustRegister(f.id, &wrappingCollector{
			wrappedCollector: c,
			prefix:           f.prefix,
			labels:           f.constLabels,
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
		f.r.MustRegister(f.id, &wrappingCollector{
			wrappedCollector: c,
			prefix:           f.prefix,
			labels:           f.constLabels,
		})
	}
	return c
}

// refer to: https://github.com/prometheus/client_golang/blob/5d584e2717ef525673736d72cd1d12e304f243d7/prometheus/wrap.go#L121
type wrappingCollector struct {
	wrappedCollector prometheus.Collector
	prefix           string
	labels           prometheus.Labels
}

// Collect implements Collecter interface
func (c *wrappingCollector) Collect(ch chan<- prometheus.Metric) {
	wrappedCh := make(chan prometheus.Metric)
	go func() {
		c.wrappedCollector.Collect(wrappedCh)
		close(wrappedCh)
	}()
	for m := range wrappedCh {
		ch <- &wrappingMetric{
			wrappedMetric: m,
			prefix:        c.prefix,
			labels:        c.labels,
		}
	}
}

// Describe implements Collector interface
func (c *wrappingCollector) Describe(ch chan<- *prometheus.Desc) {
	wrappedCh := make(chan *prometheus.Desc)
	go func() {
		c.wrappedCollector.Describe(wrappedCh)
		close(wrappedCh)
	}()
	for desc := range wrappedCh {
		ch <- wrapDesc(desc, c.prefix, c.labels)
	}
}

func (c *wrappingCollector) unwrapRecursively() prometheus.Collector {
	switch wc := c.wrappedCollector.(type) {
	case *wrappingCollector:
		return wc.unwrapRecursively()
	default:
		return wc
	}
}

type wrappingMetric struct {
	wrappedMetric prometheus.Metric
	prefix        string
	labels        prometheus.Labels
}

// Desc implements Metric interface
func (m *wrappingMetric) Desc() *prometheus.Desc {
	return wrapDesc(m.wrappedMetric.Desc(), m.prefix, m.labels)
}

// Write implements Write interface
func (m *wrappingMetric) Write(out *dto.Metric) error {
	if err := m.wrappedMetric.Write(out); err != nil {
		return err
	}
	if len(m.labels) == 0 {
		// No wrapping labels.
		return nil
	}
	for ln, lv := range m.labels {
		out.Label = append(out.Label, &dto.LabelPair{
			Name:  proto.String(ln),
			Value: proto.String(lv),
		})
	}
	sort.Sort(internal.LabelPairSorter(out.Label))
	return nil
}

// TODO: Very tricky to get wrap the Desc since it's designed to be immutable
// Any good suggestion?
// "Desc{fqName: %q, help: %q, constLabels: {%s}, variableLabels: %v}",
type LabelPair struct {
	Name  string `json:`
	Value string
}

type Desc struct {
	// fqName has been built from Namespace, Subsystem, and Name.
	fqName string `json: fqName`
	// help provides some helpful information about this metric.
	help string `json:help`
	// constLabelPairs contains precalculated DTO label pairs based on
	// the constant labels.
	constLabelPairs []LabelPair `json:constLabels`
	// variableLabels contains names of labels for which the metric
	// maintains variable values.
	variableLabels []string `json:variableLabels`
}

// extractDesc extract the inner fields in a triky way
func extractDesc(desc *prometheus.Desc) (*Desc, error) {
	// original string: "Desc{fqName: %q, help: %q, constLabels: {xx=xx, xx=xx}, variableLabels: [xx, xx, xx]}",
	_ = strings.TrimPrefix(desc.String(), "Desc")
	var innerDesc Desc
	// TODO: implement

	return &innerDesc, nil
}

func wrapDesc(desc *prometheus.Desc, prefix string, labels prometheus.Labels) *prometheus.Desc {
	if desc == nil {
		return nil
	}
	innerDesc, err := extractDesc(desc)
	if err != nil {
		return prometheus.NewInvalidDesc(fmt.Errorf("wrap Metric Desc fail:%v", err))
	}
	constLabels := prometheus.Labels{}
	for _, lp := range innerDesc.constLabelPairs {
		constLabels[lp.Name] = lp.Value
	}
	for ln, lv := range labels {
		if _, alreadyUsed := constLabels[ln]; alreadyUsed {
			return prometheus.NewInvalidDesc(fmt.Errorf("attempted wrapping with already existing label name %q", ln))
		}
		constLabels[ln] = lv
	}
	// NewDesc will do remaining validations.
	newDesc := prometheus.NewDesc(prefix+innerDesc.fqName, innerDesc.help, innerDesc.variableLabels, constLabels)
	// Propagate errors if there was any. This will override any errer
	// created by NewDesc above, i.e. earlier errors get precedence.
	//if desc.err != nil {
	//	newDesc.err = desc.err
	//}
	return newDesc
}
