package statusutil

import "encoding/json"

// workerStatusForTest is used to test that the type constraint
// status[T any] is defined as expected.
type workerStatusForTest struct {
	Code         int    `json:"code"`
	ErrorMessage string `json:"error-message"`
	ExtBytes     []byte `json:"ext-bytes"`
}

func (s *workerStatusForTest) HasChanged(other *workerStatusForTest) bool {
	return s.Code == other.Code
}

func (s *workerStatusForTest) Marshal() ([]byte, error) {
	return json.Marshal(s)
}

func (s *workerStatusForTest) Unmarshal(raw []byte) error {
	return json.Unmarshal(raw, s)
}

type checkConstraint[ST status[ST]] struct{}

// checks that *workerStatusForTest satisfies status[*workerStatusForTest]
var _ = checkConstraint[*workerStatusForTest]{}
