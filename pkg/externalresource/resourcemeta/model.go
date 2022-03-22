package resourcemeta

import (
	"path/filepath"
	"time"

	derror "github.com/hanfei1991/microcosm/pkg/errors"

	"github.com/hanfei1991/microcosm/model"
)

type (
	WorkerID   = string
	ResourceID = string
	JobID      = string
	ExecutorID = model.ExecutorID
)

// ResourceMeta is the records stored in the metastore.
type ResourceMeta struct {
	ID       ResourceID `json:"id"`
	Job      JobID      `json:"job"`
	Worker   WorkerID   `json:"worker"`
	Executor ExecutorID `json:"executor"`
	Deleted  bool       `json:"deleted"`
}

// GCTodoEntry records a future need for GC'ing a resource.
type GCTodoEntry struct {
	ID           ResourceID `json:"id"`
	Job          JobID      `json:"job"`
	TargetGCTime time.Time  `json:"target_gc_time"`
}

// ResourceType represents the type of the resource
type ResourceType string

const (
	ResourceTypeLocalFile = ResourceType("local")
	ResourceTypeS3        = ResourceType("s3")
)

// ParseResourcePath returns the ResourceType and the path suffix.
func ParseResourcePath(path ResourceID) (ResourceType, string, error) {
	segments := filepath.SplitList(path)
	if len(segments) == 0 {
		return "", "", derror.ErrIllegalResourcePath.GenWithStackByArgs(path)
	}

	var resourceType ResourceType
	switch segments[0] {
	case "local":
		resourceType = ResourceTypeLocalFile
	case "s3":
		resourceType = ResourceTypeS3
	default:
		return "", "", derror.ErrIllegalResourcePath.GenWithStackByArgs(path)
	}

	suffix := filepath.Join(segments[1:]...)
	return resourceType, suffix, nil
}
