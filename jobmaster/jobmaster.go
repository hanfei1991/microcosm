// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package JobMaster

import (
	"time"

	"github.com/hanfei1991/microcosm/master"
	"github.com/hanfei1991/microcosm/model"
)

type SyncType int32

const (
	DM  SyncType = 0
	CDC SyncType = 1
)

type Status int32

const (
	Unknown       Status = 0
	UnInitialized Status = 1
	Running       Status = 2
	Stopped       Status = 3
)

type TaskInfo struct {
	id             model.TaskID
	jobID          model.JobID
	status         Status    // the current status of the task
	lastUpdateTime time.Time // last heartbeat time
	// the executor address on which the task located

}

// a jobmaster is responsible for a sync job
type jobMaster struct {
	// ID is allocated by the jobmanager, which will keep unchanged if restarted
	id model.JobID
	// master client used to keep connection with the servermaster
	masterClient *master.Client
	// sync job type
	jobType SyncType
	// the current status of the jobmaster
	status Status
	// keep the status of the tasks
	taskList map[model.TaskID]*TaskInfo
}

func NewJobMaster() *jobMaster {
	// todo
	return &jobMaster{}
}

// init the jobmaster
func (jm *jobMaster) Init() error {
	// Todo : step1 load information from metastore

	// Todo : step2 do some simple checks,such as the validation of the upstream and downstrem .

	return nil
}

func (jm *jobMaster) Run() error {
	jm.status = Running
	return nil
}

// handle the message send from the task
func (jm *jobMaster) TaskReportHandler() error {
	return nil
}

// handle the message send from the servermaster
func (jm *jobMaster) MasterCommandHandler() error {
	return nil
}

// spit the sync job into some tasks
func (jm *jobMaster) createTasks() error {
	return nil
}

// apply for an executor to run the task , submit the task.
func (jm *jobMaster) submitTask() error {
	return nil
}

// check whether each task is running ,if not ,submit task again
func (jm *jobMaster) monitorTaskStatus() error {
	return nil
}

// report its status to the jobmanager at fixed time
func (jm *jobMaster) reportStatus() error {
	return nil
}
