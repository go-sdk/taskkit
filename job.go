package taskkit

import (
	"context"
	"runtime/debug"
	"slices"
	"sync"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/go-sdk/core/errx"
	"github.com/go-sdk/core/logx"
	"github.com/go-sdk/core/seq"
	"github.com/google/uuid"
)

// Task 是由 Manager 调度执行的任务函数。
type Task func(*Context) error

// Result 表示最近一次任务执行的结果。
type Result string

const (
	ResultUnknown Result = "unknown"
	ResultSuccess Result = "success"
	ResultFailed  Result = "failed"
	ResultPanic   Result = "panic"
)

// JobStatus 是任务当前运行状态的只读快照。
type JobStatus struct {
	Running         int
	LastResult      Result
	LastStartedAt   time.Time
	LastCompletedAt time.Time
	NextRunAt       time.Time
}

// Job 是 Manager 中任务定义和运行状态的只读快照。
type Job struct {
	ID       string
	Name     string
	Tags     []string
	Schedule Schedule
	Status   JobStatus
}

type jobRuntime struct {
	sync.RWMutex
	running         int
	lastResult      Result
	lastStartedAt   time.Time
	lastCompletedAt time.Time
}

func (r *jobRuntime) started(at time.Time) {
	r.Lock()
	defer r.Unlock()
	r.running++
	r.lastStartedAt = at
}

func (r *jobRuntime) completed(at time.Time, result Result) {
	r.Lock()
	defer r.Unlock()
	r.running--
	r.lastResult = result
	r.lastCompletedAt = at
}

func (r *jobRuntime) snapshot() JobStatus {
	r.RLock()
	defer r.RUnlock()
	return JobStatus{
		Running:         r.running,
		LastResult:      r.lastResult,
		LastStartedAt:   r.lastStartedAt,
		LastCompletedAt: r.lastCompletedAt,
	}
}

type jobEntry struct {
	id           string
	name         string
	tags         []string
	schedule     Schedule
	scheduledJob gocron.Job
	runtime      *jobRuntime
}

func (e *jobEntry) snapshot(includeNextRun bool) *Job {
	status := e.runtime.snapshot()
	if includeNextRun {
		if nextRun, err := e.scheduledJob.NextRun(); err == nil {
			status.NextRunAt = nextRun
		}
	}
	return &Job{
		ID:       e.id,
		Name:     e.name,
		Tags:     slices.Clone(e.tags),
		Schedule: e.schedule,
		Status:   status,
	}
}

func wrapTask(entry *jobEntry, config jobConfig, task Task) func(context.Context) error {
	return func(ctx context.Context) (resultErr error) {
		var cancel func()
		if config.timeout > 0 {
			ctx, cancel = context.WithTimeout(ctx, config.timeout)
			defer cancel()
		}

		startedAt := time.Now()
		taskContext := newContext(ctx, contextMetadata{
			jobID:     entry.id,
			jobName:   entry.name,
			runID:     seq.UUIDShort(),
			startedAt: startedAt,
			tags:      entry.tags,
		})
		logger := logx.Ctx(ctx).With().
			Str("job_id", taskContext.JobID()).
			Str("job_name", taskContext.JobName()).
			Str("run_id", taskContext.RunID()).
			Strs("tags", taskContext.Tags()).
			Logger()
		taskContext.Context = logger.WithContext(taskContext.Context)

		entry.runtime.started(startedAt)
		logger.Info().Msg("task started")
		result := ResultSuccess
		defer func() {
			completedAt := time.Now()
			if recovered := recover(); recovered != nil {
				result = ResultPanic
				resultErr = &PanicError{Value: recovered, Stack: debug.Stack()}
				logger.Error().
					Interface("panic", recovered).
					Bytes("stack", resultErr.(*PanicError).Stack).
					Dur("duration", completedAt.Sub(startedAt)).
					Msg("task panic recovered")
			} else if resultErr != nil {
				result = ResultFailed
				logger.Error().Err(resultErr).Dur("duration", completedAt.Sub(startedAt)).Msg("task failed")
			} else {
				logger.Info().Dur("duration", completedAt.Sub(startedAt)).Msg("task completed")
			}
			entry.runtime.completed(completedAt, result)
		}()
		return task(taskContext)
	}
}

// PanicError 保存已恢复 panic 的值和调用堆栈。
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string { return ErrPanicRecovered.Error() }

func (e *PanicError) Unwrap() error { return ErrPanicRecovered }

func jobOptions(id string, config jobConfig, baseContext *Context) []gocron.JobOption {
	options := []gocron.JobOption{
		gocron.WithName(id),
		gocron.WithTags(config.tags...),
		gocron.WithContext(baseContext),
		gocron.WithEventListeners(gocron.AfterLockError(func(_ uuid.UUID, _ string, err error) {
			logx.Error().Err(err).Str("job_id", id).Msg("task distributed lock failed")
		})),
	}
	if config.singleton {
		options = append(options, gocron.WithSingletonMode(gocron.LimitModeReschedule))
	}
	if config.intervalFromCompletion {
		options = append(options, gocron.WithIntervalFromCompletion())
	}
	return options
}

func wrapJobError(err error, operation, id string) error {
	if err == nil {
		return nil
	}
	return errx.Wrapf(err, "%s job %q", operation, id)
}
