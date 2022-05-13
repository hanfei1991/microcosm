package manager

import (
	"context"
	"sync"

	"github.com/gogo/status"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"

	"github.com/hanfei1991/microcosm/pb"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	resModel "github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta/model"
	pkgOrm "github.com/hanfei1991/microcosm/pkg/orm"
	"github.com/hanfei1991/microcosm/pkg/rpcutil"
)

// Service implements pb.ResourceManagerServer
// TODOs:
// (1) Refactor cache-related logic
// (2) Add RemoveResource method for explicit resource releasing
// (3) Implement automatic resource GC
type Service struct {
	metaclient pkgOrm.Client

	executors ExecutorInfoProvider

	wg       sync.WaitGroup
	cancelCh chan struct{}

	offlinedExecutors chan resModel.ExecutorID
	preRPCHook        *rpcutil.PreRPCHook[pb.ResourceManagerClient]
}

const (
	offlineExecutorQueueSize = 1024
)

// NewService creates a new externalresource manage service
func NewService(
	metaclient pkgOrm.Client,
	executorInfoProvider ExecutorInfoProvider,
	preRPCHook *rpcutil.PreRPCHook[pb.ResourceManagerClient],
) *Service {
	return &Service{
		metaclient:        metaclient,
		executors:         executorInfoProvider,
		offlinedExecutors: make(chan resModel.ExecutorID, offlineExecutorQueueSize),
		preRPCHook:        preRPCHook,
	}
}

// QueryResource implements ResourceManagerClient.QueryResource
func (s *Service) QueryResource(ctx context.Context, request *pb.QueryResourceRequest) (*pb.QueryResourceResponse, error) {
	var resp2 *pb.QueryResourceResponse
	shouldRet, err := s.preRPCHook.PreRPC(ctx, request, &resp2)
	if shouldRet {
		return resp2, err
	}

	record, err := s.metaclient.GetResourceByID(ctx, request.GetResourceId())
	if err != nil {
		if pkgOrm.IsNotFoundError(err) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.Aborted, err.Error())
	}

	if record.Deleted {
		return nil, status.Error(codes.NotFound, "resource marked as deleted")
	}
	return record.ToQueryResourceResponse(), nil
}

// CreateResource implements ResourceManagerClient.CreateResource
func (s *Service) CreateResource(
	ctx context.Context,
	request *pb.CreateResourceRequest,
) (*pb.CreateResourceResponse, error) {
	var resp2 *pb.CreateResourceResponse
	shouldRet, err := s.preRPCHook.PreRPC(ctx, request, &resp2)
	if shouldRet {
		return resp2, err
	}

	resourceRecord := &resModel.ResourceMeta{
		// TODO: projectID
		ID:       request.GetResourceId(),
		Job:      request.GetJobId(),
		Worker:   request.GetCreatorWorkerId(),
		Executor: resModel.ExecutorID(request.GetCreatorExecutor()),
		Deleted:  false,
	}

	err = s.metaclient.CreateResource(ctx, resourceRecord)
	if derror.ErrDuplicateResourceID.Equal(err) {
		st, stErr := status.New(codes.Internal, "resource manager error").WithDetails(&pb.ResourceError{
			ErrorCode: pb.ResourceErrorCode_ResourceIDConflict,
		})
		if stErr != nil {
			return nil, stErr
		}
		return nil, st.Err()
	}
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

	return &pb.CreateResourceResponse{}, nil
}

// GetPlacementConstraint is called by the Scheduler to determine whether
// a resource the worker relies on requires the worker running on a specific
// executor.
// Returns:
// (1) A local resource is required and the resource exists: (executorID, true, nil)
// (2) A local resource is required but the resource is not found: ("", false, ErrResourceDoesNotExist)
// (3) No placement constraint is needed: ("", false, nil)
// (4) Other errors: ("", false, err)
func (s *Service) GetPlacementConstraint(
	ctx context.Context,
	id resModel.ResourceID,
) (resModel.ExecutorID, bool, error) {
	logger := log.L().WithFields(zap.String("resource-id", id))

	rType, _, err := resModel.ParseResourcePath(id)
	if err != nil {
		return "", false, err
	}

	if rType != resModel.ResourceTypeLocalFile {
		logger.Info("Resource does not need a constraint",
			zap.String("resource-id", id), zap.String("type", string(rType)))
		return "", false, nil
	}

	record, err := s.metaclient.GetResourceByID(ctx, id)
	if err != nil {
		return "", false, err
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

func (s *Service) onExecutorOffline(executorID resModel.ExecutorID) error {
	select {
	case s.offlinedExecutors <- executorID:
		return nil
	default:
	}
	log.L().Warn("Too many offlined executors, dropping event",
		zap.String("executor-id", string(executorID)))
	return nil
}

// StartBackgroundWorker starts all background worker of this service
func (s *Service) StartBackgroundWorker() {
	s.cancelCh = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		<-s.cancelCh
		cancel()
	}()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer log.L().Info("Resource manager's background task exited")
		s.runBackgroundWorker(ctx)
	}()
}

// Stop can only be called after StartBackgroundWorker.
func (s *Service) Stop() {
	close(s.cancelCh)
	s.wg.Wait()
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

func (s *Service) handleExecutorOffline(ctx context.Context, executorID resModel.ExecutorID) {
	// TODO
}
