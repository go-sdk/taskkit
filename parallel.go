package taskkit

import (
	"context"
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"github.com/go-sdk/core/errx"
)

type parallelConfig struct {
	limit       int
	itemTimeout time.Duration
	failFast    bool
}

// ParallelOption 调整 Parallel 的执行策略。
type ParallelOption func(*parallelConfig) error

// WithLimit 设置同时执行的最大元素数量。
func WithLimit(limit int) ParallelOption {
	return func(config *parallelConfig) error {
		if limit <= 0 {
			return ErrInvalidParallelLimit
		}
		config.limit = limit
		return nil
	}
}

// WithItemTimeout 设置每个元素单独执行的最长时间。
func WithItemTimeout(timeout time.Duration) ParallelOption {
	return func(config *parallelConfig) error {
		if timeout <= 0 {
			return ErrInvalidTimeout
		}
		config.itemTimeout = timeout
		return nil
	}
}

// WithFailFast 在首个元素失败后停止派发尚未开始的元素。
func WithFailFast() ParallelOption {
	return func(config *parallelConfig) error {
		config.failFast = true
		return nil
	}
}

// ItemError 记录 Parallel 中失败元素的输入下标。
type ItemError struct {
	Index int
	Err   error
}

func (e *ItemError) Error() string { return fmt.Sprintf("parallel item %d: %v", e.Index, e.Err) }

func (e *ItemError) Unwrap() error { return e.Err }

// Parallel 使用固定数量的 worker 并发处理全部元素。
func Parallel[T any](ctx context.Context, items []T, fn func(context.Context, T) error, options ...ParallelOption) error {
	if ctx == nil {
		return ErrContextRequired
	}
	if fn == nil {
		return ErrTaskRequired
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(items) == 0 {
		return nil
	}
	config := parallelConfig{limit: runtime.GOMAXPROCS(0)}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(&config); err != nil {
			return err
		}
	}
	config.limit = min(config.limit, len(items))

	workerContext, cancel := context.WithCancel(ctx)
	defer cancel()
	indices := make(chan int)
	errorsByIndex := make([]error, len(items))
	var workers sync.WaitGroup
	workers.Add(config.limit)
	for range config.limit {
		go func() {
			defer workers.Done()
			for index := range indices {
				itemContext := workerContext
				itemCancel := func() {}
				if config.itemTimeout > 0 {
					itemContext, itemCancel = context.WithTimeout(workerContext, config.itemTimeout)
				}
				err := runParallelItem(itemContext, items[index], fn)
				itemCancel()
				if err == nil {
					continue
				}
				errorsByIndex[index] = &ItemError{Index: index, Err: err}
				if config.failFast {
					cancel()
				}
			}
		}()
	}

dispatch:
	for index := range items {
		select {
		case indices <- index:
		case <-workerContext.Done():
			break dispatch
		}
	}
	close(indices)
	workers.Wait()

	result := make([]error, 0)
	for _, err := range errorsByIndex {
		if err != nil {
			result = append(result, err)
		}
	}
	if len(result) > 0 {
		return errx.Join(result...)
	}
	return ctx.Err()
}

func runParallelItem[T any](ctx context.Context, item T, fn func(context.Context, T) error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &PanicError{Value: recovered, Stack: debug.Stack()}
		}
	}()
	return fn(ctx, item)
}
