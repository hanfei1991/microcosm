package model

// using transaction to generate increasing epoch
type LogicEpoch struct {
	Model
	Epoch int64 `gorm:"type:bigint not null default 1"`
}
