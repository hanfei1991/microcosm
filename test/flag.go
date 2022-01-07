package test

import (
	"go.uber.org/atomic"
)

// globalTestFlag indicates if this program is in test mode.
// If so, we use mock-grpc rather than a real one.
var globalTestFlag = *atomic.NewBool(false)

// GetGlobalTestFlag returns the value of global test flag
func GetGlobalTestFlag() bool {
	return globalTestFlag.Load()
}

// UpdateTestFlag update global test flag to given value
func UpdateTestFlag(val bool) {
	globalTestFlag.Store(val)
}
