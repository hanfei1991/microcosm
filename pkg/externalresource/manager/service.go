package manager

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/ctxmu"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
)

type Service struct {
	mu       *ctxmu.CtxMutex
	accessor *resourcemeta.MetadataAccessor
	cache    map[resourcemeta.ResourceID]*resourcemeta.ResourceMeta

	executors ExecutorInfoProvider

	wg sync.WaitGroup

	cancelMu sync.RWMutex
	cancel   context.CancelFunc

	offlinedExecutors chan resourcemeta.ExecutorID
}

const (
	offlineExecutorQueueSize = 1024
)

func NewService(metaclient metaclient.KV, executorInfoProvider ExecutorInfoProvider) *Service {
	return &Service{
		accessor:          resourcemeta.NewMetadataAccessor(metaclient),
		cache:             make(map[resourcemeta.ResourceID]*resourcemeta.ResourceMeta),
		executors:         executorInfoProvider,
		offlinedExecutors: make(chan resourcemeta.ExecutorID, offlineExecutorQueueSize),
	}
}

func (s *Service) CreateResource(
	ctx context.Context,
	request *pb.CreateResourceRequest,
) (*pb.CreateResourceResponse, error) {
	if !s.mu.Lock(ctx) {
		return nil, status.Error(codes.Canceled, ctx.Err().Error())
	}
	defer s.mu.Unlock()

	resourceRecord := &resourcemeta.ResourceMeta{
		ID:       request.GetResourceId(),
		Job:      request.GetJobId(),
		Worker:   request.GetCreatorWorkerId(),
		Executor: resourcemeta.ExecutorID(request.GetCreatorExecutor()),
		Deleted:  false,
	}

	ok, err := s.accessor.CreateResource(ctx, resourceRecord)
	if err != nil {
		st, stErr := status.New(codes.Internal, err.Error()).WithDetails(&pb.ResourceError{
			ErrorCode:  pb.ResourceErrorCode_ResourceManagerInternalError,
			StackTrace: errors.ErrorStack(err),
		})
		if stErr != nil {
			return nil, stErr
		}
		return nil, st.Err()
	}

	if !ok {
		st, stErr := status.New(codes.Internal, err.Error()).WithDetails(&pb.ResourceError{
			ErrorCode: pb.ResourceErrorCode_ResourceIDConflict,
		})
		if stErr != nil {
			return nil, stErr
		}
		return nil, st.Err()
	}

	// Note that the cache is updated AFTER the metastore is updated.
	if _, exists := s.cache[request.GetResourceId()]; exists {
		log.L().Panic("Cache is inconsistent",
			zap.String("resource-id", request.GetResourceId()))
	}
	s.cache[request.GetResourceId()] = resourceRecord

	return &pb.CreateResourceResponse{}, nil
}

func (s *Service) GetPlacementConstraint(
	ctx context.Context,
	id resourcemeta.ResourceID,
) (resourcemeta.ExecutorID, bool, error) {
	logger := log.L().WithFields(zap.String("resource-id", id))

	rType, _, err := resourcemeta.ParseResourcePath(id)
	if err != nil {
		return "", false, err
	}

	if rType != resourcemeta.ResourceTypeLocalFile {
		logger.Info("Resource does not need a constraint",
			zap.String("resource-id", id), zap.String("type", string(rType)))
		return "", false, nil
	}

	if !s.mu.Lock(ctx) {
		return "", false, errors.Trace(ctx.Err())
	}
	defer s.mu.Unlock()

	record, exists := s.cache[id]
	if !exists {
		logger.Info("Resource cache miss")
		var err error

		startTime := time.Now()
		record, exists, err = s.accessor.GetResource(ctx, id)
		getResourceDuration := time.Since(startTime)

		logger.Info("Resource meta fetch completed", zap.Duration("duration", getResourceDuration))

		if err != nil {
			return "", false, err
		}
		if !exists {
			return "", false, derror.ErrResourceDoesNotExist.GenWithStackByArgs(id)
		}
	} else {
		logger.Info("Resource cache hit")
	}

	if record.Deleted {
		logger.Info("Resource meta is marked as deleted", zap.Any("record", record))
		return "", false, derror.ErrResourceDoesNotExist.GenWithStackByArgs(id)
	}

	if !s.executors.HasExecutor(string(record.Executor)) {
		logger.Info("Resource meta indicates a non-existent executor",
			zap.String("executor-id", string(record.Executor)))
		return "", false, derror.ErrResourceDoesNotExist.GenWithStackByArgs(id)
	}

	return record.Executor, true, nil
}

func (s *Service) OnExecutorOffline(executorID resourcemeta.ExecutorID) error {
	select {
	case s.offlinedExecutors <- executorID:
		return nil
	default:
	}
	log.L().Warn("Too many offlined executors, dropping event",
		zap.String("executor-id", string(executorID)))
	return nil
}

func (s *Service) StartBackgroundWorker() {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancelMu.Lock()
	s.cancel = cancel
	s.cancelMu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer log.L().Info("Resource manager's background task exited")
		s.runBackgroundWorker(ctx)
	}()
}

func (s *Service) runBackgroundWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case executorID := <-s.offlinedExecutors:
			s.handleExecutorOffline(ctx, executorID)
		}
	}
}

func (s *Service) handleExecutorOffline(ctx context.Context, executorID resourcemeta.ExecutorID) {
	if !s.mu.Lock(ctx) {
		return
	}
	defer s.mu.Unlock()

	for _, record := range s.cache {
		if record.Executor != executorID {
			continue
		}
		record.Deleted = true
		log.L().Info("Mark record as deleted", zap.Any("record", record))
		// TODO asynchronously delete these records from the metastore.
	}
}
