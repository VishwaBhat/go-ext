package conc

import (
	"context"
	"runtime"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

var (
	defaultTimeout      time.Duration = time.Hour
	defaultChBufferSize               = 10
	defaultConcurrency                = runtime.NumCPU()
)

type Option struct {
	maxGoRoutines int
	timeout       time.Duration
	outBufSize    int
}

func WithMaxGoRoutines(limit int) Option {
	return Option{maxGoRoutines: limit}
}

func WithTimeout(timeout time.Duration) Option {
	return Option{timeout: timeout}
}

func WithOutBufferSize(size int) Option {
	return Option{outBufSize: size}
}

type Runner[In, Out any] struct {
	limit       int
	timeout     time.Duration
	chanBufSize int
}

func NewRunner[In, Out any](opts ...Option) Runner[In, Out] {
	limit := defaultConcurrency
	outBufSize := defaultChBufferSize
	timeout := defaultTimeout
	for _, opt := range opts {
		if opt.maxGoRoutines > 0 {
			limit = opt.maxGoRoutines
		}
		if opt.timeout > 0 {
			timeout = opt.timeout
		}
		if opt.outBufSize > 0 {
			outBufSize = opt.outBufSize
		}
	}
	return Runner[In, Out]{
		limit:       limit,
		chanBufSize: outBufSize,
		timeout:     timeout,
	}
}

func (r Runner[In, Out]) MapWithContext(ctx context.Context, elements []In, run func(In) (Out, error), filters ...func(Out) bool) ([]Out, error) {

	runCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	resCh := make(chan Out, r.chanBufSize)
	errCh := make(chan error, 1)

	var sema *semaphore.Weighted
	if r.limit > 0 {
		sema = semaphore.NewWeighted(int64(r.limit))
	}

	var wg sync.WaitGroup
	for _, el := range elements {
		wg.Go(func() {
			if sema != nil {
				if e := sema.Acquire(runCtx, 1); e != nil {
					errCh <- runCtx.Err()
					return
				}
				defer sema.Release(1)
			} else {
				if runCtx.Err() != nil {
					errCh <- runCtx.Err()
					return
				}
			}
			res, err := run(el)
			if err != nil {
				select {
				case <-runCtx.Done():
				case errCh <- err:
				}
			}

			for _, filter := range filters {
				if filter(res) {
					return
				}
			}

			select {
			case <-runCtx.Done():
			case resCh <- res:
			}

		})
	}

	go func() {
		wg.Wait()
		close(resCh)
		close(errCh)
	}()

	results := make([]Out, 0, len(elements))

	for {
		select {
		case <-runCtx.Done():
			return results, runCtx.Err()
		case res, open := <-resCh:
			if !open {
				return results, nil
			}
			results = append(results, res)
		case err, open := <-errCh:
			if err != nil {
				return results, err
			}
			if !open {
				return results, nil
			}
		}
	}
}

func (r Runner[In, Out]) Map(elements []In, fn func(In) (Out, error), filters ...func(Out) bool) ([]Out, error) {
	return r.MapWithContext(context.Background(), elements, fn, filters...)
}

type TransformFunc[In, Out any] func(In, any) (Out, error)

type IntermediateFunc[In any] func(In, any) (any, error)

func (r Runner[In, Out]) Pipeline(
	ctx context.Context,
	elements []In,
	transform TransformFunc[In, Out],
	intermediaries ...IntermediateFunc[In],
) ([]Out, error) {

	runCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	resCh := make(chan Out, r.chanBufSize)
	errCh := make(chan error, 1)

	var sema *semaphore.Weighted
	if r.limit > 0 {
		sema = semaphore.NewWeighted(int64(r.limit))
	}

	var wg sync.WaitGroup
	for _, el := range elements {
		wg.Go(func() {
			if sema != nil {
				if e := sema.Acquire(runCtx, 1); e != nil {
					errCh <- runCtx.Err()
					return
				}
				defer sema.Release(1)
			} else {
				if runCtx.Err() != nil {
					errCh <- runCtx.Err()
					return
				}
			}

			var prev any
			for _, intermediate := range intermediaries {
				if runCtx.Err() != nil {
					errCh <- runCtx.Err()
					return
				}
				next, err := intermediate(el, prev)
				if err != nil {
					errCh <- err
					return
				}
				prev = next
			}

			res, err := transform(el, prev)
			if err != nil {
				select {
				case <-runCtx.Done():
				case errCh <- err:
				}
			}

			select {
			case <-runCtx.Done():
			case resCh <- res:
			}

		})
	}

	go func() {
		wg.Wait()
		close(resCh)
		close(errCh)
	}()

	results := make([]Out, 0, len(elements))

	for {
		select {
		case <-runCtx.Done():
			return results, runCtx.Err()
		case res, open := <-resCh:
			if !open {
				return results, nil
			}
			results = append(results, res)
		case err, open := <-errCh:
			if err != nil {
				return results, err
			}
			if !open {
				return results, nil
			}
		}
	}
}
