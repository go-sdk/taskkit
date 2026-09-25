package taskkit

import (
	"context"
	"strings"
	"time"

	"github.com/go-co-op/gocron/v2"
)

const defaultShutdownTimeout = 10 * time.Second

type managerConfig struct {
	baseContext     context.Context
	location        *time.Location
	locker          gocron.Locker
	shutdownTimeout time.Duration
}

// ManagerOption 调整 Manager 的公共配置。
type ManagerOption func(*managerConfig) error

// WithBaseContext 设置所有任务的根 Context；取消该 Context 会停止后续调度。
func WithBaseContext(ctx context.Context) ManagerOption {
	return func(config *managerConfig) error {
		if ctx == nil {
			return ErrContextRequired
		}
		config.baseContext = ctx
		return nil
	}
}

// WithLocation 设置 Cron 和固定时间任务使用的时区。
func WithLocation(location *time.Location) ManagerOption {
	return func(config *managerConfig) error {
		if location == nil {
			return gocron.ErrWithLocationNil
		}
		config.location = location
		return nil
	}
}

// WithLocker 设置多副本任务执行使用的分布式锁。
func WithLocker(locker gocron.Locker) ManagerOption {
	return func(config *managerConfig) error {
		if locker == nil {
			return gocron.ErrWithDistributedLockerNil
		}
		config.locker = locker
		return nil
	}
}

// WithShutdownTimeout 设置生命周期自动关闭时等待任务退出的最长时间。
func WithShutdownTimeout(timeout time.Duration) ManagerOption {
	return func(config *managerConfig) error {
		if timeout <= 0 {
			return ErrInvalidTimeout
		}
		config.shutdownTimeout = timeout
		return nil
	}
}

type jobConfig struct {
	name                   string
	tags                   []string
	singleton              bool
	timeout                time.Duration
	intervalFromCompletion bool
}

// JobOption 调整单个任务的执行配置。
type JobOption func(*jobConfig) error

// WithName 设置任务的展示名称；分布式锁始终使用稳定的业务 ID。
func WithName(name string) JobOption {
	return func(config *jobConfig) error {
		config.name = strings.TrimSpace(name)
		return nil
	}
}

// WithTags 设置用于分类和查询的任务标签。
func WithTags(tags ...string) JobOption {
	return func(config *jobConfig) error {
		config.tags = normalizeTags(tags)
		return nil
	}
}

// WithSingleton 跳过同一任务上一次执行尚未结束时产生的新调度。
func WithSingleton() JobOption {
	return func(config *jobConfig) error {
		config.singleton = true
		return nil
	}
}

// WithTimeout 设置每次任务执行的最长时间。
func WithTimeout(timeout time.Duration) JobOption {
	return func(config *jobConfig) error {
		if timeout <= 0 {
			return ErrInvalidTimeout
		}
		config.timeout = timeout
		return nil
	}
}

// WithIntervalFromCompletion 使 Every 从上一次执行完成时间计算下一次运行时间。
func WithIntervalFromCompletion() JobOption {
	return func(config *jobConfig) error {
		config.intervalFromCompletion = true
		return nil
	}
}

func normalizeTags(tags []string) []string {
	values := make([]string, 0, len(tags))
	seen := make(map[string]struct{}, len(tags))
	for _, tag := range tags {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			continue
		}
		if _, ok := seen[tag]; ok {
			continue
		}
		seen[tag] = struct{}{}
		values = append(values, tag)
	}
	return values
}
