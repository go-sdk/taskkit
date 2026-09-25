package taskkit

import (
	"context"
	"slices"
	"time"

	"github.com/go-sdk/core/osx"
)

type contextKey struct{}

type contextMetadata struct {
	jobID     string
	jobName   string
	runID     string
	startedAt time.Time
	tags      []string
}

// Context 在标准 context.Context 上提供当前任务执行的只读元数据。
type Context struct {
	context.Context
	metadata contextMetadata
}

// FromContext 返回当前 context 中的任务执行元数据视图。
func FromContext(ctx context.Context) *Context {
	if ctx == nil {
		osx.Panic("task context must not be nil")
	}
	metadata, _ := ctx.Value(contextKey{}).(contextMetadata)
	metadata.tags = slices.Clone(metadata.tags)
	return &Context{Context: ctx, metadata: metadata}
}

func newContext(ctx context.Context, metadata contextMetadata) *Context {
	metadata.tags = slices.Clone(metadata.tags)
	return FromContext(context.WithValue(ctx, contextKey{}, metadata))
}

// JobID 返回当前任务稳定的业务标识。
func (c *Context) JobID() string { return c.metadata.jobID }

// JobName 返回当前任务的展示名称。
func (c *Context) JobName() string { return c.metadata.jobName }

// RunID 返回本次执行的唯一标识。
func (c *Context) RunID() string { return c.metadata.runID }

// StartedAt 返回本次执行实际开始的时间。
func (c *Context) StartedAt() time.Time { return c.metadata.startedAt }

// Tags 返回当前任务标签的副本。
func (c *Context) Tags() []string { return slices.Clone(c.metadata.tags) }
