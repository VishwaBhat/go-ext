package conc

import (
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
* // Example to fetch Abc
* tasker := NewTasker[User](
*   WithMaxGoRoutines(10), 									// concurrency limit
*   WithTimeout(100 * time.Millisecond),    // overall timeout
* )
*
* for _, id := ids {
* 	tasker.Enqueue(func() (User, error) {
*			user, err := fetchUser(id)
* 		if err != nil  {
* 			return 0, fmt.Errorf("user fetch error for id(%d): %w", id, err)
* 		}
* 		return user, err
* 	})
* }
*
*
* users, err := tasker.Wait()
 */

type Tasker[Out any] struct {
	limit       int
	timeout     time.Duration
	chanBufSize int
	wg          sync.WaitGroup
	sema        chan struct{}
	timeoutCh   <-chan time.Time
	timeoutOnce sync.Once
	resCh       chan Out
	errCh       chan error
	inflight    bool
}

func NewTasker[Out any](opts ...Option) *Tasker[Out] {
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

	return &Tasker[Out]{
		limit:       maxGoRoutines,
		chanBufSize: chanBufSize,
		timeout:     timeout,
		resCh:       make(chan Out, chanBufSize),
		errCh:       make(chan error, 1),
		sema:        sema,
	}
}

func (t *Tasker[Out]) reportFirstErr(err error) {
	select {
	case t.errCh <- err:
	default:
		// Channel full, error already reported - don't block
	}
}

func (t *Tasker[Out]) Close() {
	t.wg.Wait()
	close(t.resCh)
	close(t.errCh)
}

func (t *Tasker[Out]) Enqueue(fn func() (Out, error)) {
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
			t.reportFirstErr(ErrTimeout)
			return
		default:
			out, err := fn()
			if err != nil {
				t.reportFirstErr(err)
			} else {
				t.resCh <- out
			}
		}

	})
}

func (t *Tasker[Out]) Wait() ([]Out, error) {
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
