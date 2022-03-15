package externalresource

import (
	"context"
	"strings"
	"sync"

	derror "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/externalresource/model"
)

// ResourceType abstracts a single resource type, such as local storage or s3.
type ResourceType interface {
	// IsBoundToExecutor returns true if the resource needs to be cleaned
	// up if its creator executor goes down.
	IsBoundToExecutor() bool

	// CleanUp is called in the ServerMaster to trigger a clean-up.
	CleanUp(ctx context.Context, id model.ResourceID) error

	// CreateStorage is used to create a storage.Proxy if the resource is
	// a storage. If we add other resource types, this function can be a no-op.
	// If CreateStorage returns an error, it should clean up after itself.
	CreateStorage(ctx context.Context, baseProxy *BaseProxy, suffix string) (Proxy, error)
}

// ResourceTypeParser is a registry of all resource types.
type ResourceTypeParser struct {
	mu            sync.RWMutex
	resourceTypes map[string]ResourceType
}

func NewResourceTypeParser() *ResourceTypeParser {
	return &ResourceTypeParser{
		resourceTypes: make(map[string]ResourceType),
	}
}

func (p *ResourceTypeParser) ParseResourcePath(path model.ResourceID) (tp ResourceType, suffix string, err error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for prefix, tp := range p.resourceTypes {
		if !strings.HasPrefix(path, prefix) {
			continue
		}
		return tp, strings.TrimPrefix(path, prefix), nil
	}

	return nil, "", derror.ErrResourceTypeNotFound.GenWithStackByArgs(path)
}

func (p *ResourceTypeParser) RegisterResourceType(prefix string, tp ResourceType) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	normalizedPrefix := normalizeResourcePrefix(prefix)
	if _, exists := p.resourceTypes[normalizedPrefix]; exists {
		// The resource type already exists
		return false
	}

	p.resourceTypes[normalizedPrefix] = tp
	return true
}

func normalizeResourcePrefix(prefix string) string {
	ret := strings.TrimSuffix(prefix, "/")
	if !strings.HasPrefix(prefix, "/") {
		ret = "/" + ret
	}
	return ret
}
