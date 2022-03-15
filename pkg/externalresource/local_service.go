package externalresource

import (
	"os"
	"path/filepath"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/pkg/externalresource/model"
)

type LocalService struct {
	LocalFileRootPath string
}

func (s *LocalService) Remove(suffix string) error {
	path := filepath.Join(s.LocalFileRootPath, suffix)
	err := os.RemoveAll(path)
	if err != nil {
		log.L().Error("failed to remove resource file",
			zap.String("path", path), zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

func (s *LocalService) RemoveAllForWorker(workerID model.WorkerID) error {
	folder := filepath.Join(s.LocalFileRootPath, workerID)
	err := os.RemoveAll(folder)
	if err != nil {
		log.L().Error("failed to remove resource folder",
			zap.String("folder", folder), zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}
