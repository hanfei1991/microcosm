package master

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	"github.com/hanfei1991/microcosm/lib/config"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/lib/statusutil"
	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/clock"
	"github.com/hanfei1991/microcosm/pkg/meta/kvclient/mock"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

type workerManageTestSuite struct {
	manager       *WorkerManager
	masterNode    p2p.NodeID
	meta          metaclient.KVClient
	messageSender p2p.MessageSender
	clock         *clock.Mock

	events map[libModel.WorkerID]*masterEvent
}

func (s *workerManageTestSuite) AdvanceClockBy(duration time.Duration) {
	s.clock.Add(duration)
}

func (s *workerManageTestSuite) SimulateHeartbeat(
	workerID libModel.WorkerID, epoch libModel.Epoch, node p2p.NodeID,
) error {
	return s.manager.HandleHeartbeat(&libModel.HeartbeatPingMessage{
		SendTime:     s.clock.Mono(),
		FromWorkerID: workerID,
		Epoch:        epoch,
	}, node)
}

func (s *workerManageTestSuite) SimulateWorkerUpdateStatus(
	workerID libModel.WorkerID, status *libModel.WorkerStatus, epoch libModel.Epoch,
) error {
	bytes, err := status.Marshal()
	if err != nil {
		return err
	}
	_, err = s.meta.Put(
		context.Background(),
		adapter.WorkerKeyAdapter.Encode(s.manager.masterID, workerID),
		string(bytes))
	if err != nil {
		return nil
	}

	s.manager.OnWorkerStatusUpdateMessage(&statusutil.WorkerStatusMessage{
		Worker:      workerID,
		MasterEpoch: epoch,
		Status:      status,
	})
	return nil
}

func (s *workerManageTestSuite) PutMeta(workerID libModel.WorkerID, status *libModel.WorkerStatus) error {
	bytes, err := status.Marshal()
	if err != nil {
		return err
	}
	_, err = s.meta.Put(
		context.Background(),
		adapter.WorkerKeyAdapter.Encode(s.manager.masterID, workerID),
		string(bytes))
	return err
}

func (s *workerManageTestSuite) onWorkerOnline(ctx context.Context, handle WorkerHandle) error {
	s.events[handle.ID()] = &masterEvent{
		Tp:     workerOnlineEvent,
		Handle: handle,
	}
	return nil
}

func (s *workerManageTestSuite) onWorkerOffline(ctx context.Context, handle WorkerHandle) error {
	s.events[handle.ID()] = &masterEvent{
		Tp:     workerOfflineEvent,
		Handle: handle,
	}
	return nil
}

func (s *workerManageTestSuite) onWorkerStatusUpdated(ctx context.Context, handle WorkerHandle) error {
	s.events[handle.ID()] = &masterEvent{
		Tp:     workerStatusUpdatedEvent,
		Handle: handle,
	}
	return nil
}

func (s *workerManageTestSuite) onWorkerDispatched(ctx context.Context, handle WorkerHandle, err error) error {
	s.events[handle.ID()] = &masterEvent{
		Tp:     workerDispatched,
		Handle: handle,
		Err:    err,
	}
	return nil
}

func (s *workerManageTestSuite) WaitForEvent(t *testing.T, workerID libModel.WorkerID) *masterEvent {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	rl := rate.NewLimiter(rate.Every(10*time.Millisecond), 1)

	for {
		select {
		case <-timeoutCtx.Done():
			t.Fatalf("waitForEventTimed out, workerID: %s", workerID)
		default:
		}

		// The Tick should return very quickly.
		tickCtx, cancel := context.WithTimeout(timeoutCtx, 100*time.Millisecond)
		err := s.manager.Tick(tickCtx)
		cancel()
		require.NoError(t, err)

		event, exists := s.events[workerID]
		if !exists {
			err := rl.Wait(timeoutCtx)
			require.NoError(t, err)

			continue
		}

		require.Equal(t, workerID, event.Handle.ID())
		delete(s.events, workerID)
		return event
	}
}

func (s *workerManageTestSuite) Close() {
	s.manager.Close()
}

func NewWorkerManageTestSuite(isInit bool) *workerManageTestSuite {
	ret := &workerManageTestSuite{
		meta:          mock.NewMetaMock(),
		masterNode:    "executor-0",
		messageSender: p2p.NewMockMessageSender(),
		clock:         clock.NewMock(),
		events:        make(map[libModel.WorkerID]*masterEvent),
	}

	manager := NewWorkerManager(
		"master-1",
		1,
		ret.meta,
		ret.messageSender,
		ret.onWorkerOnline,
		ret.onWorkerOffline,
		ret.onWorkerStatusUpdated,
		ret.onWorkerDispatched,
		isInit,
		config.DefaultTimeoutConfig(),
		ret.clock)
	ret.manager = manager
	return ret
}

func TestCreateWorkerAndWorkerOnline(t *testing.T) {
	suite := NewWorkerManageTestSuite(true)
	suite.manager.OnCreatingWorker("worker-1", "executor-1")
	err := suite.SimulateHeartbeat("worker-1", 1, "executor-1")
	require.NoError(t, err)

	event := suite.WaitForEvent(t, "worker-1")
	require.Equal(t, workerOnlineEvent, event.Tp)
	suite.Close()
}

func TestCreateWorkerAndWorkerTimesOut(t *testing.T) {
	suite := NewWorkerManageTestSuite(true)
	suite.manager.OnCreatingWorker("worker-1", "executor-1")
	suite.AdvanceClockBy(30 * time.Second)

	event := suite.WaitForEvent(t, "worker-1")
	require.Equal(t, workerOfflineEvent, event.Tp)
	require.NotNil(t, event.Handle.GetTombstone())
	suite.Close()
}

func TestCreateWorkerAndWorkerStatusUpdatedAndTimesOut(t *testing.T) {
	suite := NewWorkerManageTestSuite(true)
	suite.manager.OnCreatingWorker("worker-1", "executor-1")

	err := suite.SimulateHeartbeat("worker-1", 1, "executor-1")
	require.NoError(t, err)

	event := suite.WaitForEvent(t, "worker-1")
	require.Equal(t, workerOnlineEvent, event.Tp)

	err = suite.SimulateWorkerUpdateStatus("worker-1", &libModel.WorkerStatus{
		Code: libModel.WorkerStatusFinished,
	}, 1)
	require.NoError(t, err)

	event = suite.WaitForEvent(t, "worker-1")
	require.Equal(t, workerStatusUpdatedEvent, event.Tp)
	require.Equal(t, &libModel.WorkerStatus{
		Code: libModel.WorkerStatusFinished,
	}, event.Handle.Status())
	suite.Close()
}

func TestRecoverAfterFailover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	suite := NewWorkerManageTestSuite(false)
	err := suite.PutMeta("worker-1", &libModel.WorkerStatus{
		Code: libModel.WorkerStatusNormal,
	})
	require.NoError(t, err)
	err = suite.PutMeta("worker-2", &libModel.WorkerStatus{
		Code: libModel.WorkerStatusNormal,
	})
	require.NoError(t, err)
	err = suite.PutMeta("worker-3", &libModel.WorkerStatus{
		Code: libModel.WorkerStatusNormal,
	})
	require.NoError(t, err)
	err = suite.PutMeta("worker-4", &libModel.WorkerStatus{
		Code: libModel.WorkerStatusNormal,
	})
	require.NoError(t, err)

	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		err := suite.manager.InitAfterRecover(ctx)
		require.NoError(t, err)
	}()

	err = suite.SimulateHeartbeat("worker-1", 1, "executor-1")
	require.NoError(t, err)
	err = suite.SimulateHeartbeat("worker-2", 1, "executor-2")
	require.NoError(t, err)
	err = suite.SimulateHeartbeat("worker-3", 1, "executor-3")
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		select {
		case <-doneCh:
			return true
		default:
		}
		suite.AdvanceClockBy(1 * time.Second)
		return false
	}, 1*time.Second, 10*time.Millisecond)

	require.True(t, suite.manager.IsInitialized())
	require.Len(t, suite.manager.GetWorkers(), 4)
	require.Contains(t, suite.manager.GetWorkers(), "worker-1")
	require.Contains(t, suite.manager.GetWorkers(), "worker-2")
	require.Contains(t, suite.manager.GetWorkers(), "worker-3")
	require.Contains(t, suite.manager.GetWorkers(), "worker-4")
	require.Nil(t, suite.manager.GetWorkers()["worker-1"].GetTombstone())
	require.Nil(t, suite.manager.GetWorkers()["worker-2"].GetTombstone())
	require.Nil(t, suite.manager.GetWorkers()["worker-3"].GetTombstone())
	require.NotNil(t, suite.manager.GetWorkers()["worker-4"].GetTombstone())
	suite.Close()
}
