package conc

import (
	"errors"
	"sync"
	"time"
)

/*
*
* Tasker is a simple goroutine pool that limits the number of concurrent tasks
* based on the provided configuration. It supports enqueuing tasks and waiting for
* them to complete, returning the results and the first error encountered (if any).
*
* Usage:
*
* tasker := NewTasker[string,int]()
*
* tasker.SetTask(func(v string) (int, error) {
* 	if len(v) <= 3 {
* 		return 0, fmt.Errorf("input(%s) cannot be less than 3 characters", v)
* 	}
* 	res, err := expensiveComputation(v)
* 	return res, err
* })
*
* tasker.Enqueue("hello")
* tasker.Enqueue("world")
*
* vals, err := tasker.Wait()
 */

type Tasker[In, Out any] struct {
	limit       int
	timeout     time.Duration
	chanBufSize int
	wg          sync.WaitGroup
	sema        chan struct{}
	taskFn      func(In) (Out, error)
	timeoutCh   <-chan time.Time
	timeoutOnce sync.Once
	resCh       chan Out
	errCh       chan error
	inflight    bool
}

func NewTasker[In, Out any](opts ...Option) *Tasker[In, Out] {
	var maxGoRoutines int
	chanBufSize := defaultChBufferSize // min
	timeout := defaultTimeout
	for _, opt := range opts {
		if opt.maxGoRoutines > 0 {
			maxGoRoutines = opt.maxGoRoutines
		}
		if opt.timeout > 0 {
			timeout = opt.timeout
		}
		if opt.outBufSize > 0 {
			chanBufSize = opt.outBufSize
		}
	}

	var sema chan struct{}
	if maxGoRoutines > 0 {
		sema = make(chan struct{}, maxGoRoutines)
	}

	return &Tasker[In, Out]{
		limit:       maxGoRoutines,
		chanBufSize: chanBufSize,
		timeout:     timeout,
		resCh:       make(chan Out, chanBufSize),
		errCh:       make(chan error, 1),
		sema:        sema,
	}
}

func (t *Tasker[In, Out]) reportFirstErr(err error) {
	select {
	case t.errCh <- err:
	default:
		// Channel full, error already reported - don't block
	}
}

func (t *Tasker[In, Out]) SetTask(fn func(In) (Out, error)) {
	if t.inflight {
		panic("[tasker] task function cannot be modified in flight")
	}
	t.taskFn = fn
}

func (t *Tasker[In, Out]) Enqueue(in In) {
	if t.taskFn == nil {
		panic("[tasker] task function is not defined")
	}

	t.inflight = true

	if t.timeoutCh == nil {
		t.timeoutOnce.Do(func() {
			t.timeoutCh = time.After(t.timeout)
		})
	}

	if t.sema != nil {
		t.sema <- struct{}{}
	}

	t.wg.Go(func() {
		defer func() {
			if t.sema != nil {
				<-t.sema //release
			}
		}()

		select {
		case <-t.timeoutCh:
			t.reportFirstErr(errors.New("timeout"))
			return
		default:
			out, err := t.taskFn(in)
			if err != nil {
				t.reportFirstErr(err)
			} else {
				t.resCh <- out
			}
		}

	})
}

func (t *Tasker[In, Out]) Wait() ([]Out, error) {
	go func() {
		t.wg.Wait()
		close(t.resCh)
		close(t.errCh)
	}()

	var vals []Out
	for {
		select {
		case val, open := <-t.resCh:
			if !open {
				return vals, nil
			}
			vals = append(vals, val)
		case e := <-t.errCh:
			if e != nil {
				return nil, e
			}
		}
	}
}
