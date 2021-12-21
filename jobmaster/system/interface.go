package system

import (
	"context"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/metadata"
)

// JobMaster maintains and manages the submitted job.
type JobMaster interface {
	// DispatchJob dispatches new tasks.
	DispatchTasks(tasks ...*model.Task)
	// Start the job master.
	Start(ctx context.Context, metaKV metadata.MetaKV) error
	// Stop the job master.
	Stop(ctx context.Context) error
	// ID returns the current job id.
	ID() model.ID
}
