package taskkit

import "github.com/go-sdk/core/errx"

var (
	// ErrManagerClosed 表示任务管理器已经永久关闭。
	ErrManagerClosed = errx.New("task manager is closed")
	// ErrManagerPaused 表示任务管理器当前暂停调度。
	ErrManagerPaused = errx.New("task manager is paused")
	// ErrContextRequired 表示没有提供标准 Context。
	ErrContextRequired = errx.New("context must not be nil")
	// ErrJobIDRequired 表示任务业务标识为空。
	ErrJobIDRequired = errx.New("job id must not be empty")
	// ErrJobNotFound 表示指定任务不存在。
	ErrJobNotFound = errx.New("job not found")
	// ErrJobAlreadyExists 表示任务业务标识已经注册。
	ErrJobAlreadyExists = errx.New("job already exists")
	// ErrScheduleRequired 表示没有提供任务调度规则。
	ErrScheduleRequired = errx.New("job schedule must not be nil")
	// ErrTaskRequired 表示没有提供任务执行函数。
	ErrTaskRequired = errx.New("job task must not be nil")
	// ErrInvalidTimeout 表示超时时间不是正数。
	ErrInvalidTimeout = errx.New("timeout must be greater than zero")
	// ErrInvalidParallelLimit 表示并发上限不是正数。
	ErrInvalidParallelLimit = errx.New("parallel limit must be greater than zero")
	// ErrPanicRecovered 表示任务执行期间发生的 panic 已被恢复。
	ErrPanicRecovered = errx.New("task panic recovered")
)
