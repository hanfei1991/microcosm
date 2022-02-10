package lib

import (
	"context"
	"fmt"

	"github.com/pingcap/errors"

	"github.com/hanfei1991/microcosm/pkg/p2p"
)

type StatusSender struct {
	workerMetaClient WorkerMetadataClient
	messageSender    p2p.MessageSender
	masterClient     *masterClient

	workerID WorkerID
}

func workerStatusUpdatedTopic(masterID MasterID, workerID WorkerID) string {
	return fmt.Sprintf("worker-status-updated-%s-%s", masterID, workerID)
}

func (s *StatusSender) SendStatus(ctx context.Context, status WorkerStatus) error {
	if err := status.marshalExt(); err != nil {
		return errors.Trace(err)
	}

	if err := s.workerMetaClient.Store(ctx, &status); err != nil {
		return errors.Trace(err)
	}

	ok, err := s.messageSender.SendToNode(
		ctx,
		s.masterClient.MasterID(),
		workerStatusUpdatedTopic(s.masterClient.MasterID(), s.workerID),
		status)
	if err != nil {
		return errors.Trace(err)
	}

	if !ok {
		// handle retry
	}
	return nil
}
