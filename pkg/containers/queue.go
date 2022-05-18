package containers

// Queue abstracts a generics FIFO queue, which is thread-safe
type Queue[T any] interface {
	Add(elem T)
	Pop() (T, bool)
	Peek() (T, bool)
	Size() int
}
