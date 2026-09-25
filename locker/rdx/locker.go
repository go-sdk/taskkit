// Package rdx 提供适配 gocron 的 Redis 分布式任务锁。
package rdx

import (
	"strings"
	"time"

	redislock "github.com/go-co-op/gocron-redis-lock/v2"
	"github.com/go-co-op/gocron/v2"
	"github.com/go-sdk/core/errx"
	"github.com/redis/go-redis/v9"
)

const (
	defaultKeyPrefix       = "taskkit:"
	defaultExpiry          = 30 * time.Second
	defaultAutoExtendEvery = 10 * time.Second
	defaultTries           = 1
)

var (
	// ErrClientRequired 表示没有提供 Redis 客户端。
	ErrClientRequired = errx.New("redis client must not be nil")
	// ErrInvalidExpiry 表示锁租约时长不是正数。
	ErrInvalidExpiry = errx.New("redis lock expiry must be greater than zero")
	// ErrInvalidAutoExtend 表示自动续租间隔无效。
	ErrInvalidAutoExtend = errx.New("redis lock auto extend interval must be less than expiry")
	// ErrInvalidTries 表示获取锁的尝试次数不是正数。
	ErrInvalidTries = errx.New("redis lock tries must be greater than zero")
)

// Config 定义 Redis 任务锁的命名空间和租约策略。
type Config struct {
	KeyPrefix       string
	Expiry          time.Duration
	AutoExtendEvery time.Duration
	Tries           int
}

// New 创建带持有者校验和自动续租的 Redis 任务锁。
func New(client redis.UniversalClient, config Config) (gocron.Locker, error) {
	if client == nil {
		return nil, ErrClientRequired
	}
	config = normalizeConfig(config)
	if config.Expiry <= 0 {
		return nil, ErrInvalidExpiry
	}
	if config.AutoExtendEvery <= 0 || config.AutoExtendEvery >= config.Expiry {
		return nil, ErrInvalidAutoExtend
	}
	if config.Tries <= 0 {
		return nil, ErrInvalidTries
	}
	locker, err := redislock.NewRedisLockerWithOptions(
		client,
		redislock.WithKeyPrefix(config.KeyPrefix),
		redislock.WithAutoExtendDuration(config.AutoExtendEvery),
		redislock.WithRedsyncOptions(
			redislock.WithExpiry(config.Expiry),
			redislock.WithTries(config.Tries),
		),
	)
	if err != nil {
		return nil, errx.Wrap(err, "create redis task locker")
	}
	return locker, nil
}

func normalizeConfig(config Config) Config {
	config.KeyPrefix = strings.TrimSpace(config.KeyPrefix)
	if config.KeyPrefix == "" {
		config.KeyPrefix = defaultKeyPrefix
	} else if !strings.HasSuffix(config.KeyPrefix, ":") {
		config.KeyPrefix += ":"
	}
	if config.Expiry == 0 {
		config.Expiry = defaultExpiry
	}
	if config.AutoExtendEvery == 0 {
		config.AutoExtendEvery = defaultAutoExtendEvery
	}
	if config.Tries == 0 {
		config.Tries = defaultTries
	}
	return config
}
