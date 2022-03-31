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

// failoverRPCClientType should be limited to rpc testClient types, but golang can't
// let us do it. So we left an alias to any.
type failoverRPCClientType any

// clientHolder groups a RPC client and it's closing function.
type clientHolder[T failoverRPCClientType] struct {
	conn   closeableConnIface
	client T
}

type dialFunc[T failoverRPCClientType] func(ctx context.Context, addr string) (*clientHolder[T], error)

// FailoverRPCClients represent RPC on this type of testClient can use any testClient
// to connect to the server.
type FailoverRPCClients[T failoverRPCClientType] struct {
	urls        []string
	leader      string
	clientsLock sync.RWMutex
	clients     map[string]*clientHolder[T]
	dialer      dialFunc[T]
}

func NewFailoverRPCClients[T failoverRPCClientType](
	ctx context.Context,
	urls []string,
	dialer dialFunc[T],
) (*FailoverRPCClients[T], error) {
	ret := &FailoverRPCClients[T]{
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

func (c *FailoverRPCClients[T]) init(ctx context.Context, urls []string) error {
	c.UpdateClients(ctx, urls, "")
	if len(c.clients) == 0 {
		return errors.ErrGrpcBuildConn.GenWithStack("failed to dial to master, urls: %v", urls)
	}
	return nil
}

// UpdateClients receives a list of rpc server addresses, dials to server that is
// not maintained in current FailoverRPCClients and close redundant clients.
func (c *FailoverRPCClients[T]) UpdateClients(ctx context.Context, urls []string, leaderURL string) {
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
// It should be a method of FailoverRPCClients, but golang can't let us do it, so
// we use a public function.
func DoFailoverRPC[
	C failoverRPCClientType,
	Req any,
	Resp any,
	F func(C, context.Context, Req, ...grpc.CallOption) (Resp, error),
](
	ctx context.Context,
	clients *FailoverRPCClients[C],
	req Req,
	rpc F,
) (resp Resp, err error) {
	clients.clientsLock.RLock()
	defer clients.clientsLock.RUnlock()

	for _, cli := range clients.clients {
		resp, err = rpc(cli.client, ctx, req)
		if err == nil {
			return resp, nil
		}
	}
	return resp, err
}
