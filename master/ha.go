package master

type HAStore interface {
	// Put Key/Value
	Put(string , string )
}