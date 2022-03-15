package externalresource

import (
	"context"

	"github.com/hanfei1991/microcosm/pkg/externalresource/model"
	"github.com/stretchr/testify/mock"
)

type MockBroker struct {
	mock.Mock
}

func NewMockBroker() *MockBroker {
	return &MockBroker{}
}

func (b *MockBroker) OpenStorage(
	ctx context.Context,
	workerID model.WorkerID,
	jobID model.JobID,
	resourcePath model.ResourceID,
) (Proxy, error) {
	args := b.Mock.Called(ctx, workerID, jobID, resourcePath)
	return args.Get(0).(Proxy), args.Error(1)
}

func (b *MockBroker) OnWorkerClosed(ctx context.Context, workerID model.WorkerID, jobID model.JobID) {
	b.Mock.Called(ctx, workerID, jobID)
}
