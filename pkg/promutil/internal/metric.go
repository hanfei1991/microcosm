package internal

// refer to: https://github.com/prometheus/client_golang/blob/5d584e2717ef525673736d72cd1d12e304f243d7/prometheus/internal/metric.go#L24

import (
	dto "github.com/prometheus/client_model/go"
)

// LabelPairSorter implements sort.Interface. It is used to sort a slice of
// dto.LabelPair pointers.
type LabelPairSorter []*dto.LabelPair

func (s LabelPairSorter) Len() int {
	return len(s)
}

func (s LabelPairSorter) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s LabelPairSorter) Less(i, j int) bool {
	return s[i].GetName() < s[j].GetName()
}
