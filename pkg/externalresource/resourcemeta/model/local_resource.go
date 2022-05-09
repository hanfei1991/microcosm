package model

import (
	"path/filepath"

	libModel "github.com/hanfei1991/microcosm/lib/model"
)

type LocalFileResourceDescriptor struct {
	BasePath   string
	Creator    libModel.WorkerID
	ResourceID ResourceID
}

func (d *LocalFileResourceDescriptor) AbsolutePath() string {
	return filepath.Join(d.BasePath, d.Creator, d.ResourceID)
}
