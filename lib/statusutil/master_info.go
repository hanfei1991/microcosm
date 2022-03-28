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

type MockMasterInfoProvider struct {
	masterID   MasterID
	masterNode p2p.NodeID
	epoch      Epoch
}

func (p *MockMasterInfoProvider) MasterID() MasterID {
	return p.masterID
}

func (p *MockMasterInfoProvider) MasterNode() p2p.NodeID {
	return p.masterNode
}

func (p *MockMasterInfoProvider) Epoch() Epoch {
	return p.epoch
}
