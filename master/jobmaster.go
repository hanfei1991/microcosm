package master

import "github.com/hanfei1991/microcosom/master/scheduler"

type JobMaster interface {
	Launch(*scheduler.Scheduler) error
	ID() string
}