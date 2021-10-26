package benchmark

import "github.com/hanfei1991/microcosom/master/scheduler"

type Master struct {
	*Config
	job *model.Job
}

func (m *Master) buildJob() error {

}

func (m *Master) ID() {

}

func (m *Master) ScheduleAndLaunchJob(s *scheduler.Scheduler) error {
	err := s.Schedule(m.job)
	if err != nil {
		return err
	}
	m.start()
	err = s.Launch(m.job)
	if err != nil {
		return err
	}
}

// Listen the events from every tasks
func (m *Master) start() {
	// Register Listen Handler to Msg Servers

	// Run watch goroutines
}