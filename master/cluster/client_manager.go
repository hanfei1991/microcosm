package cluster

import (
	"context"
	"sync"

	"github.com/hanfei1991/microcosm/model"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"go.uber.org/zap"
)

func NewClientManager() *ClientManager {
	return &ClientManager{
		executors: make(map[model.ExecutorID]ExecutorClient),
	}
}

type ClientManager struct {
	mu sync.RWMutex

	master *MasterClient
	executors map[model.ExecutorID]ExecutorClient
}

func (c *ClientManager) MasterClient() *MasterClient {
	return c.master
}

func (c *ClientManager) ExecutorClient(id model.ExecutorID) ExecutorClient {
	c.mu.RLock()	
	defer c.mu.RUnlock()
	ret, _ := c.executors[id]
	return ret
}

// TODO Right now the interface and params are not consistant. We should abstract a "grpc pool"
// interface to maintain a pool of grpc connections.
func (c *ClientManager) AddMasterClient(ctx context.Context, addrs []string) error {
	var err error
	c.master, err = NewMasterClient(ctx, addrs)
	return err
}

func (c *ClientManager) AddExecutor(id model.ExecutorID, addr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.executors[id]; ok {
		return nil
	}
	log.L().Info("client manager adds executor", zap.String("id", string(id)), zap.String("addr", addr))
	client, err := newExecutorClient(addr)	
	if err != nil {
		return err
	}
	c.executors[id] = client
	return nil
}