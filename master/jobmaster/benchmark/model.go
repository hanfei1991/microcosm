package benchmark

import (
	"encoding/json"

	"github.com/hanfei1991/microcosm/model"
)

type SyncDDLRequest struct {
	Id model.TaskID  `json:"id"`
	// 
}

type SyncDDLResponse struct {
	TaskID model.TaskID `json:"id"`
	//
	Err string `json:"err"`
}

func (s *SyncDDLRequest) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

func (s *SyncDDLRequest) Unmarshal(data []byte)  error {
	return json.Unmarshal(data, s)
}

func (s *SyncDDLResponse) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

func (s *SyncDDLResponse) Unmarshal(data []byte)  error {
	return json.Unmarshal(data, s)
}