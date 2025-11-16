package conc

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestNewTasker(t *testing.T) {
	defer goleak.VerifyNone(t)
	t.Run("should create a new tasker with default values", func(t *testing.T) {
		tasker := NewTasker[string, int]()

		if tasker == nil {
			t.Fatal("NewTasker returned nil")
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

		tasker := NewTasker[string, int](
			WithMaxGoRoutines(customLimit),
			WithOutBufferSize(customBufSize),
			WithTimeout(customTimeout),
		)

		if tasker == nil {
			t.Fatal("NewTasker returned nil")
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

func TestEnqueueAndWait(t *testing.T) {
	defer goleak.VerifyNone(t)
	t.Run("should enqueue tasks and wait for them to complete", func(t *testing.T) {
		tasker := NewTasker[string, int]()
		tasker.SetTask(func(v string) (int, error) {
			return len(v), nil
		})
		tasker.Enqueue("hello")
		tasker.Enqueue("world")
		vals, err := tasker.Wait()
		require.NoError(t, err)
		expectedVals := []int{5, 5}
		require.Equal(t, expectedVals, vals)
	})
	t.Run("should handle errors from the task function", func(t *testing.T) {
		tasker := NewTasker[string, int]()
		tasker.SetTask(func(v string) (int, error) {
			if v == "error" {
				return 0, errors.New("simulated error")
			}
			return len(v), nil
		})
		tasker.Enqueue("error")
		tasker.Enqueue("first")
		tasker.Enqueue("second")
		vals, err := tasker.Wait()
		require.Error(t, err)
		require.EqualError(t, err, "simulated error")
		require.Empty(t, vals)
	})

	t.Run("should handle timeout", func(t *testing.T) {
		tasker := NewTasker[string, int](WithTimeout(1*time.Millisecond), WithMaxGoRoutines(1))
		tasker.SetTask(func(v string) (int, error) {
			time.Sleep(10 * time.Millisecond) // Simulate a long-running task
			return len(v), nil
		})
		tasker.Enqueue("hello")
		tasker.Enqueue("world")
		vals, err := tasker.Wait()
		require.Error(t, err)
		require.EqualError(t, err, "timeout")
		require.Empty(t, vals)
	})
	t.Run("should wait for all tasks to complete even if some fail", func(t *testing.T) {
		tasker := NewTasker[string, int]()
		tasker.SetTask(func(v string) (int, error) {
			if v == "fail" {
				return 0, errors.New("simulated error")
			}
			return len(v), nil
		})
		tasker.Enqueue("hello")
		tasker.Enqueue("world")
		tasker.Enqueue("fail")
		vals, err := tasker.Wait()
		require.Error(t, err)
		require.EqualError(t, err, "simulated error")
		require.Empty(t, vals)
	})
}

func TestTaskerBlocksOnMultipleErrors(t *testing.T) {
	tasker := NewTasker[int, int](
		WithMaxGoRoutines(5),
		WithTimeout(10*time.Second),
		WithOutBufferSize(10),
	)

	// Task that always fails
	tasker.SetTask(func(v int) (int, error) {
		time.Sleep(50 * time.Millisecond)
		return 0, fmt.Errorf("error for input %d", v)
	})

	// Enqueue 10 tasks that will all fail
	for i := 0; i < 10; i++ {
		tasker.Enqueue(i)
	}

	done := make(chan struct{})
	var waitErr error

	go func() {
		_, waitErr = tasker.Wait()
		close(done)
	}()

	select {
	case <-done:
		if waitErr == nil {
			t.Error("expected an error but got nil")
		}
		t.Logf("Wait returned with error: %v", waitErr)
	case <-time.After(3 * time.Second):
		t.Fatal("BLOCKED: Wait() didn't return within 3s - goroutines stuck on errCh")
	}
}
