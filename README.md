# taskkit

`taskkit` 是基于 gocron v2 的 Go 任务基础类库，提供自动启动的动态任务管理器、任务级 Context、统一日志和 panic 恢复，以及泛型受控并发执行器。

## 环境要求

- Go 1.27 或更高版本

## 安装

```bash
go get github.com/go-sdk/taskkit
```

## 定时任务

```go
manager, err := taskkit.NewManager(taskkit.WithLocation(time.Local))
if err != nil {
	return err
}

_, err = manager.Add(
	"certificate-renewal",
	taskkit.Cron("0 */6 * * *"),
	func(ctx *taskkit.Context) error {
		logx.Ctx(ctx).Info().Str("run_id", ctx.RunID()).Msg("renew certificates")
		return renew(ctx)
	},
	taskkit.WithName("证书续期"),
	taskkit.WithTags("certificate", "maintenance"),
	taskkit.WithSingleton(),
	taskkit.WithTimeout(30*time.Minute),
)
if err != nil {
	return err
}
```

`NewManager` 返回前已经启动调度器，之后新增或更新的任务立即生效。Manager 自动注册到 `core/lifex`，正常使用 `lifex.Wait` 的程序不需要单独关闭；独立使用时也可以显式调用 `Shutdown`。

支持三类调度：

```go
taskkit.Cron("0 2 * * *")
taskkit.CronWithSeconds("*/10 * * * * *")
taskkit.Every(5 * time.Minute)
taskkit.Once(time.Now().Add(time.Hour))
```

`Every` 默认按计划开始时间计算下一次运行。需要保证两次执行之间保留完整间隔时，增加 `WithIntervalFromCompletion()`。

`Once` 执行后仍保留在 Manager 的状态表中，`NextRunAt` 变为空；调用方可以查询最后结果、更新为新调度或显式移除。

## 动态管理

```go
job, ok := manager.Get("certificate-renewal")
jobs := manager.List()

err = manager.Run("certificate-renewal") // 只等待执行请求被调度
err = manager.Update("certificate-renewal", taskkit.Every(time.Hour), renewTask)
err = manager.Remove("certificate-renewal")

err = manager.Pause(ctx)
err = manager.Resume()
err = manager.Shutdown(ctx)
```

业务 ID 在一个 Manager 内唯一，同时固定作为 gocron job name 和分布式锁键。展示名称不会影响锁身份。

## 受控并发

```go
err := taskkit.Parallel(
	ctx,
	ids,
	func(ctx context.Context, id string) error {
		return process(ctx, id)
	},
	taskkit.WithLimit(10),
	taskkit.WithItemTimeout(time.Minute),
)
```

默认会处理全部元素并按输入下标聚合错误。`WithFailFast()` 会在首个错误后停止派发新元素，并通过 Context 通知已经运行的元素退出。

## Redis 多副本锁

Redis 支持位于可选子包：

```go
import taskrdx "github.com/go-sdk/taskkit/locker/rdx"

locker, err := taskrdx.New(redisClient, taskrdx.Config{
	KeyPrefix: "certops:tasks:",
	Expiry: 30 * time.Second,
	AutoExtendEvery: 10 * time.Second,
})
if err != nil {
	return err
}

manager, err := taskkit.NewManager(taskkit.WithLocker(locker))
```

锁租约不等同于 exactly-once。涉及不可重复副作用时，业务仍需使用幂等键、唯一约束、条件更新或 fencing token 拒绝过期执行者。
