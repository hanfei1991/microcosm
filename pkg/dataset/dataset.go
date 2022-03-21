package dataset

import (
	"context"
	"encoding/json"

	"github.com/pingcap/errors"

	"github.com/hanfei1991/microcosm/pkg/adapter"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
)

// DataSet
//nolint:structcheck
type DataSet[E any, T DataEntry[E]] struct {
	metaclient metaclient.KV
	keyPrefix  adapter.KeyAdapter
}

type DataEntry[E any] interface {
	ID() string
	*E
}

func NewDataSet[E any, T DataEntry[E]](metaclient metaclient.KV, keyPrefix adapter.KeyAdapter) *DataSet[E, T] {
	return &DataSet[E, T]{
		metaclient: metaclient,
		keyPrefix:  keyPrefix,
	}
}

func (d *DataSet[E, T]) Get(ctx context.Context, id string) (T, error) {
	getResp, kvErr := d.metaclient.Get(ctx, d.getKey(id))
	if kvErr != nil {
		return nil, errors.Trace(kvErr)
	}

	if len(getResp.Kvs) == 0 {
		return nil, derror.ErrDatasetEntryNotFound.GenWithStackByArgs(d.getKey(id))
	}
	rawBytes := getResp.Kvs[0].Value

	var retVal E
	if err := json.Unmarshal(rawBytes, &retVal); err != nil {
		return nil, errors.Trace(err)
	}
	return &retVal, nil
}

func (d *DataSet[E, T]) Upsert(ctx context.Context, entry T) error {
	rawBytes, err := json.Marshal(entry)
	if err != nil {
		return errors.Trace(err)
	}

	if _, err := d.metaclient.Put(ctx, d.getKey(entry.ID()), string(rawBytes)); err != nil {
		return err
	}
	return nil
}

func (d *DataSet[E, T]) Delete(ctx context.Context, id string) error {
	if _, err := d.metaclient.Delete(ctx, d.getKey(id)); err != nil {
		return err
	}
	return nil
}

func (d *DataSet[E, T]) getKey(id string) string {
	return d.keyPrefix.Encode(id)
}
