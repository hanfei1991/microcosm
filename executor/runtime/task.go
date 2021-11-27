package runtime

import (
	//	"fmt"
	//"log"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/hanfei1991/microcosm/model"
)

type TaskStatus int32

const (
	Runnable TaskStatus = iota
	Blocked
	Waking
)

type Record struct {
	start   time.Time
	end     time.Time
	payload interface{}
	tid     int32
}

func (r *Record) toString() string {
	return fmt.Sprintf("start %s end %s payload %s\n", r.start.String(), r.end.String())
}

type Channel struct {
	innerChan chan *Record
	sendCtx *taskContext
	recvCtx *taskContext
}

func (c *Channel) readBatch(batch int) []*Record {
	records := make([]*Record, 0, batch)
	for i := 0; i < batch; i++ {
		select {
		case record := <-c.innerChan:
			records = append(records, record)
		default:
			break
		}
	}
	if len(records) > 0 {
		c.sendCtx.wake()
	}
	return records
}

func (c *Channel) writeBatch(records []*Record) ([]*Record, bool) {
	for i, record := range records {
		select {
		case c.innerChan <- record:
		default:
			if i > 0 {
				c.recvCtx.wake()
			}
			return records[i:], i == 0
		}
	}
	c.recvCtx.wake()
	return nil, false
}

type taskContext struct {
	wake func()
	err error // meet error during async job
}

// a vector of records
type Chunk []*Record

type taskContainer struct {
	cfg    *model.Task
	id     model.TaskID
	status int32
	inputCache   []Chunk
	outputCache  []Chunk
	op     operator
	inputs []*Channel
	output []*Channel
	//	waker  func()
	ctx *taskContext
}

func (t *taskContainer) prepare() error {
//	t.outputCache = make([]Chunk, len(t.output))
	return t.op.prepare()
}

func (t *taskContainer) tryAwake() bool {
	for {
		//		log.Printf("try wake task %d", t.id)
		if atomic.CompareAndSwapInt32(&t.status, int32(Blocked), int32(Waking)) {
			// log.Printf("wake task %d successful", t.id)
			return true
		}

		if atomic.CompareAndSwapInt32(&t.status, int32(Runnable), int32(Waking)) {
			// log.Printf("task %d runnable", t.id)
			return false
		}

		if atomic.LoadInt32(&t.status) == int32(Waking) {
			// log.Printf("task %d waking", t.id)
			return false
		}
	}
}

func (t *taskContainer) tryBlock() bool {
	return atomic.CompareAndSwapInt32(&t.status, int32(Runnable), int32(Blocked))
}

func (t *taskContainer) setRunnable() {
	atomic.StoreInt32(&t.status, int32(Runnable))
}

func (t *taskContainer) tryFlush() (blocked bool) {
	hasBlocked := false
	for i, cache := range t.outputCache {
		blocked := false
		t.outputCache[i], blocked = t.output[i].writeBatch(cache)
		if blocked {
			hasBlocked = true
		}
	}
	return hasBlocked
}

func (t *taskContainer) readDataFromInput(idx int, batch int) Chunk{
	if len(t.inputCache[idx]) != 0 {
		chk := t.inputCache[idx]
		t.inputCache[idx] = t.inputCache[idx][:0]
		return chk
	}
	return t.inputs[idx].readBatch(batch)
}

func (t *taskContainer) Poll() TaskStatus {
	//	log.Printf("task %d polling", t.id)
	if t.tryFlush() {
		return Blocked
	}
	idx := t.op.nextWantedInputIdx()
	r := make(Chunk, 0, 128)
    if idx == -1 {
		for i := range t.inputs {
			r = append(r, t.readDataFromInput(i, 128)...)
		}
	} else {
		r = t.readDataFromInput(idx, 128)
	}

	if len(r) == 0 && len(t.inputs) != 0 {
		// we don't have any input data
		return Blocked
	}

	// do compute
	blocked := false
	var outputs []Chunk
	var err error
	for _, record := range r {
		outputs, blocked, err = t.op.next(t.ctx, record, idx)
		if err != nil {
			// report error to job manager
		}
		for i, output := range outputs {
			t.outputCache[i] = append(t.outputCache[i], output...)
		}
		if blocked {
			break
		}
	}

	if t.tryFlush() {
		// log.Printf("task %d flush blocked", t.id)
		return Blocked
	}
	if blocked {
		return Blocked
	}
	return Runnable
}
