# 项目地图

## 项目定位

`github.com/go-sdk/taskkit` 是任务基础类库，基于 gocron v2 提供自动启动的内存调度器、动态任务管理、任务级 Context、统一日志和 panic 恢复，以及独立的受控并发执行器。

## 目录结构

```text
taskkit/
├── locker/rdx/locker.go   可选 Redis 分布式任务锁
├── context.go             任务执行 Context 和元数据
├── error.go               公共哨兵错误
├── job.go                 Task、Job 快照、状态和执行包装
├── logger.go              按环境变量启用的 gocron 到 core/logx 日志适配
├── manager.go             动态任务注册和生命周期
├── options.go             Manager 和 Job 配置
├── parallel.go            固定 worker 数的泛型并发执行器
├── schedule.go            Cron、Every 和 Once 调度规则
├── doc.go                 根包说明
├── AGENTS.md              仓库协作规范
├── PROJECT_MAP.md         项目结构和关键调用链
├── README.md              公共 API 与行为说明
├── Makefile               代码检查和测试命令
└── go.mod                 Go 模块和依赖定义
```

## 调度链路

```text
NewManager
    -> 应用时区、关闭超时、可选 gocron 日志和 Locker
    -> 创建并立即启动 gocron Scheduler
    -> lifex.OnDeinit(Manager.Shutdown)

Manager.Add / Update
    -> 校验业务 ID、Schedule、Task 和 Options
    -> 在更新旧任务前预校验调度规则
    -> 将 Task 包装为 context、日志、状态和 recover 执行链
    -> 使用业务 ID 作为 gocron job name 和分布式锁键
    -> 动态写入已运行的 Scheduler
```

`Run` 只确认立即执行请求已经提交，不等待本次执行结束。`Pause` 停止调度并等待正在执行的任务，之后可由 `Resume` 恢复；`Shutdown` 会取消任务 Context 并永久关闭 Scheduler。

## Context 和日志链路

```text
gocron job context
    -> 可选单次执行 timeout
    -> 写入 job ID、name、tags、run ID 和开始时间
    -> 附加 core/logx Logger
    -> Task(*taskkit.Context)
    -> 记录成功、错误或 panic 及耗时
    -> 更新 JobStatus
```

`Context` 嵌入标准 `context.Context`。业务继续向数据库、HTTP 客户端或 `Parallel` 传递它时，取消信号和任务元数据都会保留。

gocron 调度器自身的内部日志默认关闭。创建 Manager 前将 `TASKKIT_GOCRON_LOG` 设置为 `1`、`t`、`true`、`y`、`yes` 或 `on`（忽略大小写）时，内部日志通过 `core/logx` 输出；任务开始、完成、失败、panic 和分布式锁失败日志不受该开关影响。

## 并发执行链路

```text
Parallel(ctx, items, fn)
    -> 创建固定数量 worker
    -> 按输入顺序派发元素
    -> 可选单项 timeout
    -> 单项 panic 恢复
    -> 按输入下标稳定聚合错误
```

`WithFailFast` 只停止尚未派发的元素，并取消共享 Context；已经执行的函数必须主动处理 Context 才能及时退出。

## 多副本锁

`locker/rdx` 封装 `gocron-redis-lock/v2`，默认使用 `taskkit:` 锁键前缀、30 秒租约、每 10 秒自动续租和单次获取尝试。锁竞争失败时本次任务会被跳过并等待下一次调度。

Redis 租约不提供 exactly-once。续租失败、进程暂停或网络分区可能使旧持有者和新持有者短暂并行，关键业务必须额外使用幂等键、唯一约束、版本条件更新或 fencing token。

## 扩展边界

- 根包不依赖数据库，不持久化任务定义。
- Go 函数无法持久化；数据库调度需要稳定 handler 名称、进程内注册表和多副本同步协议。
- gocron-ui 会直接操作底层 Scheduler，可能绕过 Manager 的状态，因此当前不提供 UI 适配。

## 验证边界

- `make lint` 会整理依赖并运行 golangci-lint。
- `make test` 会使用竞态检测运行本地确定性测试，需要用户明确授权。
- `go build ./...` 只证明纯编译通过，不证明时间调度、进程退出、Redis 续租或多副本互斥的运行时行为。
