package model

import (
	"context"

	ormModel "github.com/hanfei1991/microcosm/pkg/orm/model"
	"gorm.io/gorm"
)

const (
	DefaultEpochPK  = 1
	DefaultMinEpoch = 1
)

// TODO: merge to the orm ormModel
// using transaction to generate increasing epoch
type LogicEpoch struct {
	ormModel.Model
	Epoch int64 `gorm:"type:bigint not null default 1"`
}

func InitializeEpoch(ctx context.Context, db *gorm.DB) error {
	var logicEp LogicEpoch
	// first check and add first record if not exists
	// TODO: replace into
	if err := db.First(&logicEp, DefaultEpochPK).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			if res := db.Create(&LogicEpoch{
				Model: ormModel.Model{
					SeqID: DefaultEpochPK,
				},
				Epoch: DefaultMinEpoch,
			}).Error; res != nil {
				return res
			}

			return nil
		}

		return err
	}

	// already exists, do nothing
	return nil
}

func GenEpoch(ctx context.Context, db *gorm.DB) (int64, error) {
	var epoch int64
	err := db.Transaction(func(tx *gorm.DB) error {
		//(1)update epoch = epoch + 1
		if err := tx.Model(&LogicEpoch{
			Model: ormModel.Model{
				SeqID: DefaultEpochPK,
			},
		}).Update("epoch", gorm.Expr("epoch + ?", 1)).Error; err != nil {
			// return any error will rollback
			return err
		}

		//(2)select epoch
		var logicEp LogicEpoch
		if err := tx.First(&logicEp, DefaultEpochPK).Error; err != nil {
			return err
		}
		epoch = logicEp.Epoch

		// return nil will commit the whole transaction
		return nil
	})
	if err != nil {
		return 0, err
	}

	return epoch, nil
}
