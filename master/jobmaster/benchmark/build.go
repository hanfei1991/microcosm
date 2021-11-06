package benchmark

import (
	"github.com/hanfei1991/microcosom/master/cluster"
	"github.com/hanfei1991/microcosom/model"
	"github.com/hanfei1991/microcosom/pkg/autoid"
)

// BUild JobMaster for benchmark workload.
func BuildBenchmarkJobMaster(rawConfig string, idAllocator *autoid.Allocator, resourceMgr cluster.ResourceMgr, client cluster.ExecutorClient) (*Master, error) {
	config, err := configFromJson(rawConfig)
	if err != nil {
		return nil, err
	}

	job := &model.Job{
		ID: model.JobID(idAllocator.AllocID()),
	}

	tableTasks := make([]*model.Task, 0)
	for i:=0; i< config.TableNum; i++ {
		t := &model.Task{
			JobID: job.ID,
			ID: model.TaskID(idAllocator.AllocID()),
			Cost: 1,
		}
		tableTasks = append(tableTasks, t)
	}

	job.Tasks = tableTasks

	master := &Master{
		Config: config,
		job: job,
		resouceManager: resourceMgr,
		client: client,
	}
	return master, nil
}