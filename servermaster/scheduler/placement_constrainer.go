package scheduler

import (
	"context"

	"github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta"
)

// PlacementConstrainer describes an object that provides
// the placement constraint for an external resource.
type PlacementConstrainer interface {
	GetPlacementConstraint(
		ctx context.Context,
		id resourcemeta.ResourceID,
	) (resourcemeta.ExecutorID, bool, error)
}
