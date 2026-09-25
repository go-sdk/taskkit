package taskkit

import (
	"context"
	"testing"
	"time"

	"github.com/go-sdk/core/testx"
)

func TestContextMetadataSurvivesDerivedContext(t *testing.T) {
	startedAt := time.Date(2026, time.September, 25, 10, 0, 0, 0, time.UTC)
	ctx := newContext(context.Background(), contextMetadata{
		jobID:     "job-1",
		jobName:   "Job 1",
		runID:     "run-1",
		startedAt: startedAt,
		tags:      []string{"daily"},
	})
	derived, cancel := context.WithCancel(ctx)
	cancel()

	actual := FromContext(derived)
	testx.Equal(t, "job-1", actual.JobID())
	testx.Equal(t, "Job 1", actual.JobName())
	testx.Equal(t, "run-1", actual.RunID())
	testx.Equal(t, startedAt, actual.StartedAt())
	testx.Equal(t, []string{"daily"}, actual.Tags())
	testx.ErrorIs(t, actual.Err(), context.Canceled)
}
