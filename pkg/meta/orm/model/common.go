package model

import (
	"time"
)

// CreatedAt/UpdatedAt will autoupdate in the gorm lib, not in sql backend
type Model struct {
	SeqID     uint      `json:"seq-id" gorm:"primaryKey;autoIncrement"`
	CreatedAt time.Time `json:"created-at"`
	UpdatedAt time.Time `json:"updated-at"`
}
