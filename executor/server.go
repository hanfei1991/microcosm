package executor

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/hanfei1991/microcosom/model"
	"github.com/hanfei1991/microcosom/pb"
	"github.com/hanfei1991/microcosom/pkg/log"
	"google.golang.org/grpc"
)

type Server struct {
	cfg *Config

	srv *grpc.Server
	cli *MasterClient
	ID  model.ExecutorID

	lastHearbeatTime time.Time
}

func NewServer(cfg *Config) *Server {
	s := Server {
		cfg: cfg,
	}
	return &s
}

func (s *Server) SubmitSubJob(ctx context.Context, req *pb.SubmitSubJobRequest) (*pb.SubmitSubJobResponse, error) {
	return &pb.SubmitSubJobResponse{}, nil
}

func (s *Server) CancelSubJob(ctx context.Context, req *pb.CancelSubJobRequest) (*pb.CancelSubJobResponse, error) {
	return &pb.CancelSubJobResponse{}, nil
}

func (s *Server) Start(ctx context.Context) error {
	// Start grpc server

	rootLis, err := net.Listen("tcp", s.cfg.WorkerAddr)

	if err != nil {
		return err
	}

	s.srv = grpc.NewServer()
	pb.RegisterExecutorServer(s.srv, s)

	err = s.srv.Serve(rootLis)
	if err != nil {
		return err
	}

	// Register myself
	s.cli, err = NewMasterClient(s.cfg)
	if err != nil {
		return err
	}
	registerReq := &pb.RegisterExecutorRequest{
		Address: s.cfg.WorkerAddr,
		Capability: 100,
	}
	resp, err := s.cli.RegisterExecutor(registerReq)
	if err != nil {
		return err
	}
	s.ID = model.ExecutorID(resp.ExecutorId)
	// Start Heartbeat
	ticker := time.NewTicker(500 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return nil
		case t := <-ticker.C:
			req := &pb.HeartbeatRequest{
				ExecutorId: int32(s.ID),
				Status: int32(model.Running),
				Timestamp: uint64(t.Unix()),
				Ttl: uint64(s.cfg.KeepAliveTTL),
			}
			resp, err := s.cli.client.Heartbeat(ctx, req)
			if err != nil {
				log.L().Error("heartbeat meet error")
				if s.lastHearbeatTime.Add(time.Duration(s.cfg.KeepAliveTTL)).Before(time.Now()) {
					return err
				}
				continue
			}
			if resp.ErrMessage != "" {
				return errors.New(resp.ErrMessage)
			}
			s.lastHearbeatTime = t
		}
	}
}