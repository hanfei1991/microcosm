package model

// using transaction to generate increasing epoch
type LogicEpoch struct {
	Model
	Epoch int `gorm:"type:bigint not null default 1"`
}
