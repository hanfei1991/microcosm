package metautil

import (
	"time"
)

type Model struct {
	ID        uint      `gorm:"primaryKey"`
	CreatedAt time.Time // NOTICE: autoupdate inside the orm lib, but not sql backend
	UpdatedAt time.Time
}

type ProjectInfo struct {
	Model
	ProjectID   string `gorm:"type:char(36) not null;uniqueIndex:uidx_id"`
	ProjectName string `gorm:"type:varchar(64) not null"`
}

type ProjectOperation struct {
	ID        uint      `gorm:"primaryKey;auto_increment"`
	ProjectID string    `gorm:"type:char(36) not null;index:idx_op"`
	Operation string    `gorm:"type:varchar(16) not null"`
	JobID     string    `gorm:"type:char(36) not null"`
	CreatedAt time.Time `gorm:"autoCreateTime;index:idx_op"`
}

type JobInfo struct {
	Model
	ProjectID string `gorm:"type:char(36) not null;index:idx_st,priority:1"`
	JobID     string `gorm:"type:char(36) not null;uniqueIndex:uidx_id"`
	JobType   int    `gorm:"type:tinyint not null"`
	JobStatus int    `gorm:"type:tinyint not null;index:idx_st,priority:2"`
	JobAddr   string `gorm:"type:varchar(64) not null"`
	JobConfig []byte `grom:"type:blob"` // TODO: why translate to longblob?
}

type WorkerInfo struct {
	Model
	ProjectID    string `gorm:"type:char(36) not null"`
	JobID        string `gorm:"type:char(36) not null;uniqueIndex:uidx_id,priority:1;index:idx_st,priority:1"`
	WorkerID     string `gorm:"type:char(36) not null;uniqueIndex:uidx_id,priority:2"`
	WorkerType   int    `gorm:"type:tinyint not null"`
	WorkerStatus int    `gorm:"type:tinyint not null;index:idx_st,priority:2"`
	WorkerErrMsg string `gorm:"type:varchar(128)"`
	WorkerConfig []byte `gorm:"type:blob"` // TODO: why translate to longblob?
}

type ResourceInfo struct {
	Model
	ProjectID      string `gorm:"type:char(36) not null"`
	ResourceID     string `gorm:"type:char(36) not null;uniqueIndex:uidx_id;index:idx_ji,priority:2;index:idx_ei,priority:2"`
	JobID          string `gorm:"type:char(36) not null;index:idx_ji,priority:1"`
	WorkerID       string `gorm:"type:char(36) not null"`
	ExecutorID     string `gorm:"type:char(36) not null;index:idx_ei,priority:1"`
	ResourceStatus bool   `gorm:"type:BOOLEAN"`
}
