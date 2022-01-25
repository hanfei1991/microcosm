package dm

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/unit"
	"github.com/pingcap/tiflow/dm/dumpling"
	"github.com/pingcap/tiflow/dm/loader"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/syncer"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/pkg/p2p"
)

const (
	WorkerDumpUnit = iota + 1
	WorkerLoadUnit
	WorkerSyncUnit
)

type SubTaskMaster struct {
	*lib.BaseMaster

	id        lib.MasterID
	cfg       *config.SubTaskConfig
	workerSeq []lib.WorkerType
	workerID  lib.WorkerID
}

// TODO: does InitImpl has a run time limit?
func (s SubTaskMaster) InitImpl(ctx context.Context) error {
	switch s.cfg.Mode {
	case config.ModeAll:
		s.workerSeq = []lib.WorkerType{
			WorkerDumpUnit,
			WorkerLoadUnit,
			WorkerSyncUnit,
		}
	case config.ModeFull:
		s.workerSeq = []lib.WorkerType{
			WorkerDumpUnit,
			WorkerLoadUnit,
		}
	case config.ModeIncrement:
		s.workerSeq = []lib.WorkerType{
			WorkerSyncUnit,
		}
	default:
		return errors.Errorf("unknown mode: %s", s.cfg.Mode)
	}

	// build DM Units to make use of IsFresh, to skip finished units
	unitSeq := make([]unit.Unit, 0, len(s.workerSeq))
	for _, tp := range s.workerSeq {
		u := s.buildDMUnit(tp)
		err := u.Init(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		unitSeq = append(unitSeq, u)
	}
	defer func() {
		for _, u := range unitSeq {
			u.Close()
		}
	}()

	lastNotFresh := 0
	for i := len(unitSeq) - 1; i >= 0; i-- {
		isFresh, err := unitSeq[i].IsFreshTask(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if !isFresh {
			lastNotFresh = i
		}
	}

	s.workerSeq = s.workerSeq[lastNotFresh:]
	if len(s.workerSeq) == 0 {
		return nil
	}
	log.L().Debug("s.workerSeq", zap.Any("workerSeq", s.workerSeq))
	var err error
	s.workerID, err = s.CreateWorker(s.workerSeq[0], s.cfg, 0)
	return errors.Trace(err)
}

func (s SubTaskMaster) buildDMUnit(tp lib.WorkerType) unit.Unit {
	switch tp {
	case WorkerDumpUnit:
		return dumpling.NewDumpling(s.cfg)
	case WorkerLoadUnit:
		if s.cfg.NeedUseLightning() {
			return loader.NewLightning(s.cfg, nil, "subtask-master")
		}
		return loader.NewLoader(s.cfg, nil, "subtask-master")
	case WorkerSyncUnit:
		return syncer.NewSyncer(s.cfg, nil, nil)
	}
	return nil
}

func (s SubTaskMaster) Tick(ctx context.Context) error {
	log.L().Info("tick")
	status := s.GetWorkers()[s.workerID].Status()
	if status.Code == lib.WorkerStatusFinished {
		log.L().Info("worker finished", zap.String("workerID", string(s.workerID)))
		if len(s.workerSeq) > 0 {
			s.workerSeq = s.workerSeq[1:]
			if len(s.workerSeq) > 0 {
				var err error
				s.workerID, err = s.CreateWorker(s.workerSeq[0], s.cfg, 0)
				if err != nil {
					return errors.Trace(err)
				}
			} else {
				// TODO: find a way to close itself
				//s.Close(ctx)
			}
		}
	}
	return nil
}

func (s SubTaskMaster) OnMasterRecovered(ctx context.Context) error {
	log.L().Info("on master recovered")
	return nil
}

func (s SubTaskMaster) OnWorkerDispatched(worker lib.WorkerHandle, result error) error {
	log.L().Info("on worker dispatched")
	return nil
}

func (s SubTaskMaster) OnWorkerOnline(worker lib.WorkerHandle) error {
	log.L().Info("on worker online")
	return nil
}

func (s SubTaskMaster) OnWorkerOffline(worker lib.WorkerHandle, reason error) error {
	log.L().Info("on worker offline")
	return nil
}

func (s SubTaskMaster) OnWorkerMessage(worker lib.WorkerHandle, topic p2p.Topic, message interface{}) error {
	log.L().Info("on worker message")
	return nil
}

func (s SubTaskMaster) CloseImpl(ctx context.Context) error {
	log.L().Info("close")
	return nil
}
