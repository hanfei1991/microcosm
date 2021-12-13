package resource

import (
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
)

// RescMgrr manages the resources of the clusters.
type RescMgr interface {
	// Register registers new executor resource
	Register(id model.ExecutorID, capacitiy RescUnit)

	// Allocate allocates executor resources to given tasks
	Allocate(tasks []*pb.ScheduleTask) (bool, *pb.TaskSchedulerResponse)

	// Update updates executor resource usage and running status
	Update(id model.ExecutorID, use RescUnit, status model.ExecutorStatus) error
}

// RescUnit is the min unit of resource that we count.
type RescUnit int

type ExecutorResource struct {
	ID     model.ExecutorID
	Status model.ExecutorStatus

	Capacity RescUnit
	Reserved RescUnit
	Used     RescUnit
}
