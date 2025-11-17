package conc

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrTimeout   = errors.New("[discrete-tasker] overall timeout")
	ErrOpTimeout = errors.New("[discrete-tasker] operation timeout")
)

/*
*
* DiscreteTasker is a simple goroutine pool that limits the number of concurrent tasks
* based on the provided configuration. It supports enqueuing tasks and waiting for
* them to complete, returning the results and the first error encountered (if any).
*
* Usage:
*
* tasker := NewDiscreteTasker[string,int]()
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

type DiscreteTasker[In, Out any] struct {
	limit         int
	timeout       time.Duration
	chanBufSize   int
	wg            sync.WaitGroup
	sema          chan struct{}
	taskFn        func(In) (Out, error)
	timeoutCh     <-chan time.Time
	timeoutOnce   sync.Once
	eachOpTimeout time.Duration
	resCh         chan TaskItemResult[In, Out]
	inflight      bool
}

type TaskItemResult[In, Out any] struct {
	In  *In
	Out Out
	Err error
}

func (t TaskItemResult[In, Out]) Ok() bool {
	return t.Err != nil
}

func newResultErr[In, Out any](in *In, err error) TaskItemResult[In, Out] {
	return TaskItemResult[In, Out]{In: in, Err: err}
}

func newResult[In, Out any](in *In, out Out) TaskItemResult[In, Out] {
	return TaskItemResult[In, Out]{In: in, Out: out}
}

func NewDiscreteTasker[In, Out any](opts ...Option) *DiscreteTasker[In, Out] {
	var maxGoRoutines int
	chanBufSize := defaultChBufferSize // min
	timeout := defaultTimeout
	eachOpTimeout := defaultTimeout
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
		if opt.eachTimeout > 0 {
			eachOpTimeout = opt.eachTimeout
		}
	}

	var sema chan struct{}
	if maxGoRoutines > 0 {
		sema = make(chan struct{}, maxGoRoutines)
	}

	return &DiscreteTasker[In, Out]{
		limit:         maxGoRoutines,
		chanBufSize:   chanBufSize,
		timeout:       timeout,
		resCh:         make(chan TaskItemResult[In, Out], chanBufSize),
		sema:          sema,
		eachOpTimeout: eachOpTimeout,
	}
}

// Close closes the result channel and wait for all goroutines to finish.
func (t *DiscreteTasker[In, Out]) Close() {
	t.wg.Wait()
	close(t.resCh)
}

// SetTask sets the task function to run. It can only be set if no task is inflight.
func (t *DiscreteTasker[In, Out]) SetTask(fn func(In) (Out, error)) {
	if t.inflight {
		panic("[discrete-tasker] task function cannot be modified in flight")
	}
	t.taskFn = fn
}

// Enqueue enqueues the given input to run the task concurrently.
func (t *DiscreteTasker[In, Out]) Enqueue(in In) {
	if t.taskFn == nil {
		panic("[discrete-tasker] task function is not defined")
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

		opTimeoutCh := time.After(t.eachOpTimeout)

		select {
		case <-opTimeoutCh:
			t.resCh <- newResultErr[In, Out](&in, ErrOpTimeout)
			return
		case <-t.timeoutCh:
			t.resCh <- newResultErr[In, Out](&in, ErrTimeout)
			return
		default:
			out, err := t.taskFn(in)
			if err != nil {
				t.resCh <- newResultErr[In, Out](&in, err)
			} else {
				t.resCh <- newResult[In, Out](&in, out)
			}
		}

	})
}

// Wait blocks until all tasks are done or timeout is reached. It returns the results and the first error
// encountered (if any).
func (t *DiscreteTasker[In, Out]) Wait() []TaskItemResult[In, Out] {
	go func() {
		t.wg.Wait()
		close(t.resCh)
	}()

	var vals []TaskItemResult[In, Out]
	for val := range t.resCh {
		vals = append(vals, val)
	}
	return vals
}
