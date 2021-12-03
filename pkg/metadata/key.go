package metadata

import (
	"strings"

	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"go.uber.org/zap"
)

const (
	// MetaStoreKeyBase is the common prefix of keys in dataflow system
	MetaStoreKeyBase = "/dataflow"
	executorKey      = "/executor"
)

// DataFlowKeyType is the type of etcd key
type DataFlowKeyType = int

// types of etcd key
const (
	DataFlowKeyTypeUnknown DataFlowKeyType = iota
	DataFlowKeyTypeExecutor
)

type DataFlowKey struct {
	Tp     DataFlowKeyType
	NodeID string
}

// Parse parses the given metastore key
func (k *DataFlowKey) Parse(key string) error {
	if !strings.HasPrefix(key, MetaStoreKeyBase) {
		return errors.ErrInvalidMetaStoreKey.GenWithStackByArgs(key)
	}
	key = key[len(MetaStoreKeyBase):]
	switch {
	case strings.HasPrefix(key, executorKey):
		k.Tp = DataFlowKeyTypeExecutor
		k.NodeID = key[len(executorKey)+1:]
	default:
		return errors.ErrInvalidMetaStoreKeyTp.GenWithStackByArgs(key)
	}
	return nil
}

func (k *DataFlowKey) String() string {
	switch k.Tp {
	case DataFlowKeyTypeExecutor:
		return MetaStoreKeyBase + executorKey + "/" + k.NodeID
	default:
		log.L().Panic("unsupport key type", zap.Int("tp", k.Tp))
	}
	return ""
}
