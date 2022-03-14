package internal

import (
	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/pkg/externalresource/model"
)

type BaseProxy struct {
	ResourceID model.ResourceID
	ExecutorID model.ExecutorID
	WorkerID   model.WorkerID
	JobID      model.JobID

	MasterCli client.MasterClient
}

func (p *BaseProxy) ID() model.ResourceID{
	return p.ResourceID
}