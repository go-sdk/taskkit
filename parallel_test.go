package taskkit

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sdk/core/errx"
	"github.com/go-sdk/core/testx"
)

func TestParallelHonorsLimit(t *testing.T) {
	var running atomic.Int64
	var maximum atomic.Int64
	gate := make(chan struct{})
	started := make(chan struct{}, 4)
	done := make(chan error, 1)
	go func() {
		done <- Parallel(context.Background(), []int{1, 2, 3, 4}, func(context.Context, int) error {
			current := running.Add(1)
			for {
				previous := maximum.Load()
				if current <= previous || maximum.CompareAndSwap(previous, current) {
					break
				}
			}
			started <- struct{}{}
			<-gate
			running.Add(-1)
			return nil
		}, WithLimit(2))
	}()

	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			testx.FailNow(t, "parallel workers did not start")
		}
	}
	close(gate)
	testx.NoError(t, <-done)
	testx.EqualValues(t, 2, maximum.Load())
}

func TestParallelAggregatesErrorsByInputOrder(t *testing.T) {
	first := errx.New("first")
	third := errx.New("third")
	err := Parallel(context.Background(), []int{0, 1, 2}, func(_ context.Context, value int) error {
		switch value {
		case 0:
			return first
		case 2:
			return third
		default:
			return nil
		}
	}, WithLimit(3))

	testx.ErrorIs(t, err, first)
	testx.ErrorIs(t, err, third)
	testx.ErrorContains(t, err, "parallel item 0")
	testx.ErrorContains(t, err, "parallel item 2")
}

func TestParallelRecoversPanic(t *testing.T) {
	err := Parallel(context.Background(), []int{1}, func(context.Context, int) error {
		panic("failed")
	})
	testx.ErrorIs(t, err, ErrPanicRecovered)
	var panicErr *PanicError
	testx.ErrorAs(t, err, &panicErr)
	testx.NotEmpty(t, panicErr.Stack)
}
