package srvdiscovery

import (
	"context"
)

// defines some type alias used in service discovery module
type (
	Revision = int64
	UUID     = string
)

// ServiceResource defines the resource of service
type ServiceResource struct {
	Addr string `json:"addr"`
}

// WatchResp defines the change set from a Watch API of Discovery interface
type WatchResp struct {
	addSet map[UUID]ServiceResource
	delSet map[UUID]ServiceResource
	err    error
}

// Discovery defines interface of a simple service discovery
type Discovery interface {
	// Snapshot returns a full set of service resources and the revision of snapshot
	Snapshot(ctx context.Context) (map[UUID]ServiceResource, error)

	// Watch watches the change of service resources, the watched events will be
	// returned through a channel.
	Watch(ctx context.Context) <-chan WatchResp
}
