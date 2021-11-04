package autoid

type Allocator struct {
	id int32 
}

func NewAllocator() *Allocator {
	return &Allocator{}
}

func (a *Allocator) AllocID() int32 {
	a.id ++
	return a.id
}