package resourcemeta

import (
	"path"
	"strings"
	"time"

	derror "github.com/hanfei1991/microcosm/pkg/errors"

	"github.com/hanfei1991/microcosm/pkg/meta/orm/model"
)

// GCTodoEntry records a future need for GC'ing a resource.
type GCTodoEntry struct {
	ID           model.ResourceID `json:"id"`
	Job          model.JobID      `json:"job"`
	TargetGCTime time.Time        `json:"target_gc_time"`
}

// ResourceType represents the type of the resource
type ResourceType string

const (
	ResourceTypeLocalFile = ResourceType("local")
	ResourceTypeS3        = ResourceType("s3")
)

// ParseResourcePath returns the ResourceType and the path suffix.
func ParseResourcePath(rpath model.ResourceID) (ResourceType, string, error) {
	if !strings.HasPrefix(rpath, "/") {
		return "", "", derror.ErrIllegalResourcePath.GenWithStackByArgs(rpath)
	}
	rpath = strings.TrimPrefix(rpath, "/")
	segments := strings.Split(rpath, "/")
	if len(segments) == 0 {
		return "", "", derror.ErrIllegalResourcePath.GenWithStackByArgs(rpath)
	}

	var resourceType ResourceType
	switch segments[0] {
	case "local":
		resourceType = ResourceTypeLocalFile
	case "s3":
		resourceType = ResourceTypeS3
	default:
		return "", "", derror.ErrIllegalResourcePath.GenWithStackByArgs(rpath)
	}

	suffix := path.Join(segments[1:]...)
	return resourceType, suffix, nil
}
