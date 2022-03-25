package statusutil

type status[T any] interface {
	HasChanged(other T) bool
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
}
