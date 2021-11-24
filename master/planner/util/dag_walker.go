package util

import (
	"github.com/hanfei1991/microcosm/model"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
)

const (
	// the maximum depth of the DAG
	// TODO add a user configurable parameter
	defaultMaximalDepth = 100
)

// DAGWalker walks the DAG and calls the callback function for each node.
// NOTE: We use a struct instead of a function to provide better extensibility
// for the future in case we want to implement more complicated graph algorithms.
type DAGWalker struct {
	visited      map[model.NodeID]struct{}
	onVertex     func(*model.Node) error
	maximalDepth int
}

// NewDAGWalker creates a new DAGWalker.
func NewDAGWalker(onVertex func(*model.Node) error) *DAGWalker {
	return &DAGWalker{
		onVertex:     onVertex,
		maximalDepth: defaultMaximalDepth,
	}
}

// Walk walks the DAG and calls the callback function for each node.
func (w *DAGWalker) Walk(dag *model.DAG) error {
	w.visited = make(map[model.NodeID]struct{})
	return w.doWalk(dag.Root, 0)
}

func (w *DAGWalker) doWalk(node *model.Node, depth int) error {
	if node == nil {
		log.Panic("unexpected nil node")
	}
	if depth > w.maximalDepth {
		// TODO add a custom error
		return errors.Errorf("exceed maximal depth %d", w.maximalDepth)
	}

	if _, ok := w.visited[node.ID]; ok {
		return nil
	}
	if err := w.onVertex(node); err != nil {
		return errors.Trace(err)
	}
	w.visited[node.ID] = struct{}{}
	for _, nextNode := range node.Outputs {
		if err := w.doWalk(nextNode, depth+1); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
