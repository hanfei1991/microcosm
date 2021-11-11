package runtime

import (
	"context"
	"log"

	"github.com/hanfei1991/microcosom/pkg/workerpool"
	"sync"
)

type queue struct {
	sync.Mutex
	tasks []*taskContainer
}

func (q *queue) pop() *taskContainer {
	q.Lock()
	defer q.Unlock()
	if len(q.tasks) == 0 {
		return nil
	}
	task := q.tasks[0]
	q.tasks = q.tasks[1:]
	return task
}

func (q *queue) push(t *taskContainer) {
	q.Lock()
	defer q.Unlock()
	q.tasks = append(q.tasks, t)
}

type Scheduler struct {
	ctx *taskContext
	q   queue
}

func (s *Scheduler) getWaker(task *taskContainer) func() {
	return func() {
		// you can't wake or it is already been waked.
		if !task.tryAwake() {
			return
		}
		task.setRunnable()
		s.q.push(task)
	}
}

func (s *Scheduler) ShowStats(sec int) {
	for tid, stats := range s.ctx.stats {
		log.Printf("tid %d qps %d avgLag %d ms", tid, stats.recordCnt/sec, stats.totalLag.Milliseconds()/int64(stats.recordCnt))
	}
}

func (s *Scheduler) Run(ctx context.Context) {
	//log.Printf("scheduler running")
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		t := s.q.pop()
		if t == nil {
			// idle
			continue
		}
		status := t.Poll()
		if status == Blocked {
			if t.tryBlock() {
				//log.Printf("task %d blocked success", t.id)
				continue
			}
		}
		t.setRunnable()
		s.q.push(t)
	}
}

func NewScheduler() *Scheduler {
	ctx := &taskContext{
		ioPool: workerpool.NewDefaultAsyncPool(20),
		//stats :   make([]tableStats, cfg.TableNum),
	}
	s := &Scheduler{ctx: ctx}
	return s
	//receiveTasks := make([]*taskContainer, 0)
	//taskID := 0
	//for _, addr := range cfg.Servers {
	//	op := &opReceive{
	//		addr:  addr,
	//		data:  make(chan *Record, 4096),
	//		cache: make([][]*Record, cfg.TableNum),
	//	}
	//	err := op.prepare(ctx)
	//	if err != nil {
	//		return nil, errors.Trace(err)
	//	}
	//	receiveTasks = append(receiveTasks, &taskContainer{
	//		id:     taskID,
	//		ctx:    ctx,
	//		op:     op,
	//		status: int32(Runnable),
	//	})
	//	taskID++
	//}
	//log.Printf("finish construct receive tasks")
	//s.q = queue{
	//	tasks: receiveTasks,
	//}

	//for i := 0; i < cfg.TableNum; i++ {
	//	hashTask := &taskContainer{
	//		id:     taskID,
	//		op:     &opHash{},
	//		ctx:    ctx,
	//		status: int32(Blocked),
	//	}
	//	for _, task := range receiveTasks {
	//		s.connectTasks(task, hashTask)
	//	}
	//	taskID++
	//	sinkTask := &taskContainer{
	//		id: taskID,
	//		op: &opSink{
	//			writer: fileWriter{
	//				filePath: fmt.Sprintf("t_%d.txt", i),
	//				tid: i,
	//			},
	//		},
	//		ctx:    ctx,
	//		status: int32(Blocked),
	//	}
	//	taskID++
	//	s.connectTasks(hashTask, sinkTask)
	//	hashTask.prepare(ctx)
	//	err := sinkTask.prepare(ctx)
	//	if err != nil {
	//		return nil, errors.Trace(err)
	//	}
	//}
}
