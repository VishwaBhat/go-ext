package conc

import (
	"errors"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestNewDiscreteTasker(t *testing.T) {
	defer goleak.VerifyNone(t)
	t.Run("should create a new tasker with default values", func(t *testing.T) {
		tasker := NewDiscreteTasker[string, int]()

		if tasker == nil {
			t.Fatal("NewDiscreteTasker returned nil")
		}
		if tasker.limit != 0 {
			t.Errorf("Expected limit to be 0, got %d", tasker.limit)
		}
		if tasker.chanBufSize != defaultChBufferSize {
			t.Errorf("Expected outBufSize to be %d, got %d", defaultChBufferSize, tasker.chanBufSize)
		}
		if tasker.timeout != defaultTimeout {
			t.Errorf("Expected timeout to be %s, got %s", defaultTimeout, tasker.timeout)
		}
		if tasker.sema != nil {
			t.Errorf("Expected sema to be nil, got %v", tasker.sema)
		}
	})

	t.Run("should create a new tasker with custom options", func(t *testing.T) {
		customLimit := 5
		customBufSize := 10
		customTimeout := 5 * time.Second

		tasker := NewDiscreteTasker[string, int](
			WithMaxGoRoutines(customLimit),
			WithOutBufferSize(customBufSize),
			WithTimeout(customTimeout),
		)

		if tasker == nil {
			t.Fatal("NewDiscreteTasker returned nil")
		}
		if tasker.limit != customLimit {
			t.Errorf("Expected limit to be %d, got %d", customLimit, tasker.limit)
		}
		if tasker.chanBufSize != customBufSize {
			t.Errorf("Expected outBufSize to be %d, got %d", customBufSize, tasker.chanBufSize)
		}
		if tasker.timeout != customTimeout {
			t.Errorf("Expected timeout to be %s, got %s", customTimeout, tasker.timeout)
		}
		if tasker.sema == nil {
			t.Errorf("Expected sema to be non-nil, got %v", tasker.sema)
		}
		if cap(tasker.sema) != customLimit {
			t.Errorf("Expected sema capacity to be %d, got %d", customLimit, cap(tasker.sema))
		}
	})

}

func TestDiscreteTaskerSetTask(t *testing.T) {
	t.Run("should set the task function", func(t *testing.T) {
		tasker := NewDiscreteTasker[string, int]()
		taskFn := func(s string) (int, error) { return len(s), nil }
		tasker.SetTask(taskFn)

		if tasker.taskFn == nil {
			t.Fatal("SetTask did not set the task function")
		}

		// Test that setting the task function again panics if inflight
		tasker.inflight = true
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("The code did not panic when setting task function while inflight")
			}
		}()
		tasker.SetTask(taskFn)
	})
}

func TestDiscreteTaskerEnqueue(t *testing.T) {
	t.Run("should enqueue a task and handle its execution", func(t *testing.T) {
		tasker := NewDiscreteTasker[string, int]()
		wantErr := errors.New("insufficient data")
		taskFn := func(s string) (int, error) {
			if len(s) < 3 {
				return 0, wantErr
			}
			return len(s), nil
		}
		tasker.SetTask(taskFn)
		inOut := map[string]any{
			"test":    4,
			"test123": 7,
			"abc":     3,
			"a":       wantErr,
		}

		for in := range inOut {
			tasker.Enqueue(in)
		}

		res := tasker.Wait()
		for _, r := range res {
			out, ok := inOut[*r.In]
			if !ok {
				t.Errorf("No corresponding out for in: %v", r.In)
			}
			if err, isErr := out.(error); isErr {
				if !errors.Is(r.Err, err) {
					t.Errorf("Expected error %v, got %v", err, r.Err)
				}
				continue
			}
			if r.Out != out {
				t.Errorf("Expected output %v, got %v", out, r.Out)
			}
		}
	})

	t.Run("should panic if task function is not set", func(t *testing.T) {
		tasker := NewDiscreteTasker[string, int]()
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("The code did not panic when enqueueing without a task function")
			}
		}()
		tasker.Enqueue("test")
	})

	t.Run("should respect the timeout for each operation", func(t *testing.T) {
		tasker := NewDiscreteTasker[string, int](WithEachTimeout(time.Nanosecond))
		taskFn := func(s string) (int, error) { time.Sleep(200 * time.Millisecond); return 0, nil }
		tasker.SetTask(taskFn)
		tasker.Enqueue("test")
		results := tasker.Wait()
		if len(results) != 1 || !errors.Is(results[0].Err, ErrOpTimeout) {
			t.Errorf("Expected ErrOpTimeout error, got %v", results[0].Err)
		}
	})
}
