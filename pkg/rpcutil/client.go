package rpcutil

import (
	"context"
	"strings"
	"sync"

	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type closeableConnIface interface {
	Close() error
}

// failoverRpcClientType should be limited to rpc testClient types, but golang can't
// let us do it. So we left an alias to any.
type failoverRpcClientType any

// clientHolder groups a RPC client and it's closing function.
type clientHolder[T failoverRpcClientType] struct {
	conn   closeableConnIface
	client T
}

type dialFunc[T failoverRpcClientType] func(ctx context.Context, addr string) (*clientHolder[T], error)

// FailoverRpcClients represent RPC on this type of testClient can use any testClient
// to connect to the server.
type FailoverRpcClients[T failoverRpcClientType] struct {
	urls        []string
	leader      string
	clientsLock sync.RWMutex
	clients     map[string]*clientHolder[T]
	dialer      dialFunc[T]
}

func NewFailoverRpcClients[T failoverRpcClientType](
	ctx context.Context,
	urls []string,
	dialer dialFunc[T],
) (*FailoverRpcClients[T], error) {
	ret := &FailoverRpcClients[T]{
		urls:    urls,
		clients: make(map[string]*clientHolder[T]),
		dialer:  dialer,
	}
	err := ret.init(ctx, urls)
	if err != nil {
		return nil, err
	}
	// leader will be updated on heartbeat
	ret.leader = ret.urls[0]
	return ret, nil
}

func (c *FailoverRpcClients[T]) init(ctx context.Context, urls []string) error {
	c.UpdateClients(ctx, urls, "")
	if len(c.clients) == 0 {
		return errors.ErrGrpcBuildConn.GenWithStack("failed to dial to master, urls: %v", urls)
	}
	return nil
}

// UpdateClients receives a list of server master addresses, dials to server
// master that is not maintained in current MasterClient.
func (c *FailoverRpcClients[T]) UpdateClients(ctx context.Context, urls []string, leaderURL string) {
	c.clientsLock.Lock()
	defer c.clientsLock.Unlock()

	c.leader = leaderURL

	notFound := make(map[string]struct{}, len(c.clients))
	for addr := range c.clients {
		notFound[addr] = struct{}{}
	}

	for _, addr := range urls {
		// TODO: refine address with and without scheme
		addr = strings.Replace(addr, "http://", "", 1)
		delete(notFound, addr)
		if _, ok := c.clients[addr]; !ok {
			log.L().Info("add new server master testClient", zap.String("addr", addr))
			cliH, err := c.dialer(ctx, addr)
			if err != nil {
				log.L().Warn("dial to server master failed", zap.String("addr", addr), zap.Error(err))
				continue
			}
			c.urls = append(c.urls, addr)
			c.clients[addr] = cliH
		}
	}

	for k := range notFound {
		if err := c.clients[k].conn.Close(); err != nil {
			log.L().Warn("close server master testClient failed", zap.String("addr", k), zap.Error(err))
		}
		delete(c.clients, k)
	}
}

// DoFailoverRPC calls RPC on given clients one by one until one succeeds.
// It should be a method of FailoverRpcClients, but golang can't let us do it, so
// we use a public function.
func DoFailoverRPC[
	C failoverRpcClientType,
	Req any,
	Resp any,
	F func(C, context.Context, Req, ...grpc.CallOption) (Resp, error),
](
	ctx context.Context,
	clients *FailoverRpcClients[C],
	req Req,
	rpc F,
) (Resp, error) {
	clients.clientsLock.RLock()
	defer clients.clientsLock.RUnlock()

	var (
		resp Resp
		err	 error
	)

	for _, cli := range clients.clients {
		resp, err = rpc(cli.client, ctx, req)
		if err == nil {
			return resp, nil
		}
	}
	return resp, err
}
