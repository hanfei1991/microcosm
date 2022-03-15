package externalresource

import (
	"context"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/pkg/externalresource/model"
)

type Broker interface {
	OpenStorage(
		ctx context.Context,
		workerID model.WorkerID,
		jobID model.JobID,
		resourcePath model.ResourceID,
	) (Proxy, error)

	OnWorkerClosed(
		ctx context.Context,
		workerID model.WorkerID,
		jobID model.JobID,
	)
}

// BrokerImpl is singleton per executor, it communicates with Manager of server master.
type BrokerImpl struct {
	executorID string
	pathPrefix string
	masterCli  client.MasterClient // nil when in test

	resourceTypeParser *ResourceTypeParser
	localFileService   *LocalService
}

func NewBroker(executorID, pathPrefix string, masterCli client.MasterClient) *BrokerImpl {
	typeParser := NewResourceTypeParser()
	typeParser.RegisterResourceType(ResourceTypeLocalFile, &LocalFileType{})
	return &BrokerImpl{
		executorID:         executorID,
		pathPrefix:         pathPrefix,
		masterCli:          masterCli,
		resourceTypeParser: typeParser,
		localFileService: &LocalService{
			LocalFileRootPath: pathPrefix,
		},
	}
}

func (b *BrokerImpl) OpenStorage(
	ctx context.Context,
	workerID model.WorkerID,
	jobID model.JobID,
	resourcePath model.ResourceID,
) (Proxy, error) {
	tp, suffix, err := b.resourceTypeParser.ParseResourcePath(resourcePath)
	if err != nil {
		return nil, err
	}

	baseProxy := &BaseProxy{
		ResourceID:   resourcePath,
		ExecutorID:   model.ExecutorID(b.executorID),
		WorkerID:     workerID,
		JobID:        jobID,
		MasterCli:    b.masterCli,
		ResourceType: tp,
	}
	if err := baseProxy.Initialize(ctx); err != nil {
		return nil, err
	}

	return tp.CreateStorage(ctx, baseProxy, suffix)
}

func (b *BrokerImpl) OnWorkerClosed(
	ctx context.Context,
	workerID model.WorkerID,
	jobID model.JobID,
) {
	// TODO implement me
}
