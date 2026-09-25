package taskkit

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/go-sdk/core/logx"
	"github.com/go-sdk/core/testx"
)

func TestManagerStartsAutomaticallyAndManagesJobs(t *testing.T) {
	manager, err := NewManager(WithLocation(time.UTC))
	testx.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		testx.NoError(t, manager.Shutdown(ctx))
	})

	runs := make(chan *Context, 1)
	job, err := manager.Add(
		"job-1",
		Every(10*time.Millisecond),
		func(ctx *Context) error {
			select {
			case runs <- ctx:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		},
		WithName("Job 1"),
		WithTags("test"),
		WithSingleton(),
	)
	testx.NoError(t, err)
	testx.Equal(t, "job-1", job.ID)
	testx.Equal(t, "Job 1", job.Name)

	select {
	case taskContext := <-runs:
		testx.Equal(t, "job-1", taskContext.JobID())
		testx.NotEmpty(t, taskContext.RunID())
	case <-time.After(time.Second):
		testx.FailNow(t, "scheduled task did not run")
	}

	listed := manager.List()
	testx.Len(t, listed, 1)
	testx.NoError(t, manager.Remove("job-1"))
	_, ok := manager.Get("job-1")
	testx.False(t, ok)
}

func TestManagerRejectsInvalidUpdateWithoutRemovingJob(t *testing.T) {
	manager, err := NewManager(WithLocation(time.UTC))
	testx.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		testx.NoError(t, manager.Shutdown(ctx))
	})

	task := func(*Context) error { return nil }
	_, err = manager.Add("job-1", Every(time.Hour), task)
	testx.NoError(t, err)
	_, err = manager.Update("job-1", Cron("invalid"), task)
	testx.Error(t, err)
	_, ok := manager.Get("job-1")
	testx.True(t, ok)
}

func TestManagerPauseWithCanceledContextKeepsRunningState(t *testing.T) {
	manager, err := NewManager(WithLocation(time.UTC))
	testx.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		testx.NoError(t, manager.Shutdown(ctx))
	})

	_, err = manager.Add("job-1", Once(time.Now().Add(time.Hour)), func(*Context) error { return nil })
	testx.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	testx.ErrorIs(t, manager.Pause(ctx), context.Canceled)
	testx.NoError(t, manager.Run("job-1"))
}

func TestManagerGetAfterShutdownDoesNotQueryScheduler(t *testing.T) {
	manager, err := NewManager(WithLocation(time.UTC))
	testx.NoError(t, err)
	_, err = manager.Add("job-1", Once(time.Now().Add(time.Hour)), func(*Context) error { return nil })
	testx.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	testx.NoError(t, manager.Shutdown(ctx))

	startedAt := time.Now()
	job, ok := manager.Get("job-1")
	testx.True(t, ok)
	testx.Equal(t, time.Time{}, job.Status.NextRunAt)
	if elapsed := time.Since(startedAt); elapsed >= 500*time.Millisecond {
		t.Fatalf("Get blocked after shutdown: %s", elapsed)
	}
}

func TestManagerInheritsBaseContextLogger(t *testing.T) {
	var output bytes.Buffer
	baseLogger := logx.Output(&output).With().Str("trace_id", "trace-1").Logger()
	manager, err := NewManager(
		WithLocation(time.UTC),
		WithBaseContext(baseLogger.WithContext(context.Background())),
	)
	testx.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		testx.NoError(t, manager.Shutdown(ctx))
	})

	inherited := make(chan bool, 1)
	_, err = manager.Add("job-1", Once(time.Now().Add(time.Hour)), func(ctx *Context) error {
		logx.Ctx(ctx).Info().Msg("task body")
		inherited <- strings.Contains(output.String(), `"trace_id":"trace-1"`)
		return nil
	})
	testx.NoError(t, err)
	testx.NoError(t, manager.Run("job-1"))
	select {
	case actual := <-inherited:
		testx.True(t, actual)
	case <-time.After(time.Second):
		t.Fatal("task did not run")
	}
}
