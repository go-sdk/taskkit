package taskkit

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/go-sdk/core/errx"
	"github.com/go-sdk/core/lifex"
)

type managerState uint8

const (
	managerStateRunning managerState = iota
	managerStatePaused
	managerStateClosed
)

// Manager 集中管理进程内的任务定义、调度和运行状态。
type Manager struct {
	lifecycleMu     sync.Mutex
	mu              sync.RWMutex
	scheduler       gocron.Scheduler
	context         context.Context
	cancel          context.CancelFunc
	jobs            map[string]*jobEntry
	state           managerState
	shutdownTimeout time.Duration
	location        *time.Location
	shutdownErr     error
}

// NewManager 创建并立即启动任务管理器，同时登记进程解构函数。
func NewManager(options ...ManagerOption) (*Manager, error) {
	config := managerConfig{
		baseContext:     context.Background(),
		location:        time.Local,
		shutdownTimeout: defaultShutdownTimeout,
	}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(&config); err != nil {
			return nil, errx.Wrap(err, "configure task manager")
		}
	}

	schedulerOptions := []gocron.SchedulerOption{
		gocron.WithLocation(config.location),
		gocron.WithLogger(schedulerLogger{}),
		gocron.WithStopTimeout(config.shutdownTimeout),
	}
	if config.locker != nil {
		schedulerOptions = append(schedulerOptions, gocron.WithDistributedLocker(config.locker))
	}
	scheduler, err := gocron.NewScheduler(schedulerOptions...)
	if err != nil {
		return nil, errx.Wrap(err, "create task scheduler")
	}
	baseContext, cancel := context.WithCancel(config.baseContext)
	manager := &Manager{
		scheduler:       scheduler,
		context:         baseContext,
		cancel:          cancel,
		jobs:            make(map[string]*jobEntry),
		state:           managerStateRunning,
		shutdownTimeout: config.shutdownTimeout,
		location:        config.location,
	}
	scheduler.Start()
	lifex.OnDeinit(manager.shutdown)
	return manager, nil
}

// Add 注册任务；Manager 已启动时会立即应用调度规则。
func (m *Manager) Add(id string, schedule Schedule, task Task, options ...JobOption) (*Job, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, ErrJobIDRequired
	}
	if schedule == nil {
		return nil, ErrScheduleRequired
	}
	if task == nil {
		return nil, ErrTaskRequired
	}
	config, err := applyJobOptions(id, options)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state == managerStateClosed {
		return nil, ErrManagerClosed
	}
	if _, ok := m.jobs[id]; ok {
		return nil, errx.Wrapf(ErrJobAlreadyExists, "job %q", id)
	}
	if err := schedule.validate(time.Now(), m.location); err != nil {
		return nil, wrapJobError(err, "validate", id)
	}
	entry := &jobEntry{
		id:       id,
		name:     config.name,
		tags:     slices.Clone(config.tags),
		schedule: schedule,
		runtime:  &jobRuntime{lastResult: ResultUnknown},
	}
	baseContext := newContext(m.context, contextMetadata{
		jobID:   id,
		jobName: config.name,
		tags:    config.tags,
	})
	scheduledJob, err := m.scheduler.NewJob(
		schedule.definition(),
		gocron.NewTask(wrapTask(entry, config, task)),
		jobOptions(id, config, baseContext)...,
	)
	if err != nil {
		return nil, wrapJobError(err, "add", id)
	}
	entry.scheduledJob = scheduledJob
	m.jobs[id] = entry
	return entry.snapshot(true), nil
}

// Update 原位替换现有任务的调度和执行配置，底层任务标识保持不变。
func (m *Manager) Update(id string, schedule Schedule, task Task, options ...JobOption) (*Job, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, ErrJobIDRequired
	}
	if schedule == nil {
		return nil, ErrScheduleRequired
	}
	if task == nil {
		return nil, ErrTaskRequired
	}
	config, err := applyJobOptions(id, options)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state == managerStateClosed {
		return nil, ErrManagerClosed
	}
	entry, ok := m.jobs[id]
	if !ok {
		return nil, errx.Wrapf(ErrJobNotFound, "job %q", id)
	}
	if err := schedule.validate(time.Now(), m.location); err != nil {
		return nil, wrapJobError(err, "validate", id)
	}
	replacement := &jobEntry{
		id:           id,
		name:         config.name,
		tags:         slices.Clone(config.tags),
		schedule:     schedule,
		scheduledJob: entry.scheduledJob,
		runtime:      entry.runtime,
	}
	baseContext := newContext(m.context, contextMetadata{
		jobID:   id,
		jobName: config.name,
		tags:    config.tags,
	})
	scheduledJob, err := m.scheduler.Update(
		entry.scheduledJob.ID(),
		schedule.definition(),
		gocron.NewTask(wrapTask(replacement, config, task)),
		jobOptions(id, config, baseContext)...,
	)
	if err != nil {
		return nil, wrapJobError(err, "update", id)
	}
	replacement.scheduledJob = scheduledJob
	m.jobs[id] = replacement
	return replacement.snapshot(true), nil
}

// Remove 移除任务并取消该任务正在使用的 Context。
func (m *Manager) Remove(id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return ErrJobIDRequired
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state == managerStateClosed {
		return ErrManagerClosed
	}
	entry, ok := m.jobs[id]
	if !ok {
		return errx.Wrapf(ErrJobNotFound, "job %q", id)
	}
	err := m.scheduler.RemoveJob(entry.scheduledJob.ID())
	if err != nil && !errx.Is(err, gocron.ErrJobNotFound) {
		return wrapJobError(err, "remove", id)
	}
	delete(m.jobs, id)
	return nil
}

// Get 返回指定任务当前状态的只读快照。
func (m *Manager) Get(id string) (*Job, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	entry, ok := m.jobs[strings.TrimSpace(id)]
	if !ok {
		return nil, false
	}
	return entry.snapshot(m.state != managerStateClosed), true
}

// List 返回按业务 ID 排序的全部任务状态快照。
func (m *Manager) List() []*Job {
	m.mu.RLock()
	defer m.mu.RUnlock()
	jobs := make([]*Job, 0, len(m.jobs))
	includeNextRun := m.state != managerStateClosed
	for _, entry := range m.jobs {
		jobs = append(jobs, entry.snapshot(includeNextRun))
	}
	slices.SortFunc(jobs, func(a, b *Job) int { return strings.Compare(a.ID, b.ID) })
	return jobs
}

// Run 提交一次立即执行请求，不等待任务执行完成。
func (m *Manager) Run(id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return ErrJobIDRequired
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	switch m.state {
	case managerStatePaused:
		return ErrManagerPaused
	case managerStateClosed:
		return ErrManagerClosed
	}
	entry, ok := m.jobs[id]
	if !ok {
		return errx.Wrapf(ErrJobNotFound, "job %q", id)
	}
	return wrapJobError(entry.scheduledJob.RunNow(), "run", id)
}

// Pause 暂停全部调度并等待正在执行的任务退出。
func (m *Manager) Pause(ctx context.Context) error {
	if ctx == nil {
		return ErrContextRequired
	}
	if err := ctx.Err(); err != nil {
		return errx.Wrap(err, "pause task manager")
	}
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	m.mu.Lock()
	if m.state == managerStateClosed {
		m.mu.Unlock()
		return ErrManagerClosed
	}
	if m.state == managerStatePaused {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()
	if err := m.scheduler.StopJobsWithContext(ctx); err != nil {
		return errx.Wrap(err, "pause task manager")
	}
	m.mu.Lock()
	m.state = managerStatePaused
	m.mu.Unlock()
	return nil
}

// Resume 恢复已经暂停的任务调度。
func (m *Manager) Resume() error {
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	m.mu.Lock()
	if m.state == managerStateClosed {
		m.mu.Unlock()
		return ErrManagerClosed
	}
	if m.state == managerStateRunning {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()
	m.scheduler.Start()
	m.mu.Lock()
	m.state = managerStateRunning
	m.mu.Unlock()
	return nil
}

// Shutdown 永久关闭 Manager，并在 ctx 到期前等待正在执行的任务退出。
func (m *Manager) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return ErrContextRequired
	}
	m.lifecycleMu.Lock()
	defer m.lifecycleMu.Unlock()
	m.mu.Lock()
	if m.state == managerStateClosed {
		err := m.shutdownErr
		m.mu.Unlock()
		return err
	}
	m.state = managerStateClosed
	m.mu.Unlock()
	m.cancel()
	shutdownErr := m.scheduler.ShutdownWithContext(ctx)
	if shutdownErr != nil {
		shutdownErr = errx.Wrap(shutdownErr, "shutdown task manager")
	}
	m.mu.Lock()
	m.shutdownErr = shutdownErr
	m.mu.Unlock()
	return shutdownErr
}

func (m *Manager) shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), m.shutdownTimeout)
	defer cancel()
	return m.Shutdown(ctx)
}

func applyJobOptions(id string, options []JobOption) (jobConfig, error) {
	config := jobConfig{name: id}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(&config); err != nil {
			return jobConfig{}, errx.Wrapf(err, "configure job %q", id)
		}
	}
	if config.name == "" {
		config.name = id
	}
	return config, nil
}
