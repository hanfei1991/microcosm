package config

import "time"

type TimeoutConfig struct {
	WorkerTimeoutDuration            time.Duration
	WorkerTimeoutGracefulDuration    time.Duration
	WorkerHeartbeatInterval          time.Duration
	WorkerReportStatusInterval       time.Duration
	MasterHeartbeatCheckLoopInterval time.Duration
}

var defaultTimeoutConfig = TimeoutConfig{
	WorkerTimeoutDuration:         time.Second * 15,
	WorkerTimeoutGracefulDuration: time.Second * 5,
	WorkerHeartbeatInterval:       time.Second * 3,
	WorkerReportStatusInterval:    time.Second * 3,
	// We use a very short loop interval to increase the throughput of handling
	// status updates.
	MasterHeartbeatCheckLoopInterval: time.Millisecond * 10,
}.Adjust()

// Adjust validates the TimeoutConfig and adjusts it
func (config TimeoutConfig) Adjust() TimeoutConfig {
	var tc TimeoutConfig = config
	// worker timeout duration must be 2 times larger than worker heartbeat interval
	if tc.WorkerTimeoutDuration < 2*tc.WorkerHeartbeatInterval+time.Second*3 {
		tc.WorkerTimeoutDuration = 2*tc.WorkerHeartbeatInterval + time.Second*3
	}
	return tc
}

func DefaultTimeoutConfig() TimeoutConfig {
	return defaultTimeoutConfig
}
