package model

// JobMasterV2 holds the config and checkpoint status of a job
type JobMasterV2 struct {
	ID         string       `json:"id"`
	Tp         WorkloadType `json:"type"`
	Config     []byte       `json:"config"`
	Checkpoint []byte       `json:"checkpoint"`
}
