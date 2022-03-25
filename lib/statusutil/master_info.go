package statusutil

import "github.com/hanfei1991/microcosm/pkg/p2p"

type (
	MasterID = string
	WorkerID = string
	Epoch    = int64
)

// MasterInfoProvider is an object that can provide necessary
// information so that the Writer can contact the master.
type MasterInfoProvider interface {
	MasterID() MasterID
	MasterNode() p2p.NodeID
	Epoch() Epoch
}
