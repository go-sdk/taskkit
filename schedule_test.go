package taskkit

import (
	"testing"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/go-sdk/core/testx"
)

func TestScheduleValidation(t *testing.T) {
	now := time.Date(2026, time.September, 25, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		schedule Schedule
		target   error
	}{
		{name: "cron", schedule: Cron("0 * * * *")},
		{name: "cron with seconds", schedule: CronWithSeconds("*/5 * * * * *")},
		{name: "invalid cron", schedule: Cron("invalid"), target: gocron.ErrCronJobParse},
		{name: "duration", schedule: Every(time.Minute)},
		{name: "zero duration", schedule: Every(0), target: gocron.ErrDurationJobIntervalZero},
		{name: "once", schedule: Once(now.Add(time.Hour))},
		{name: "past once", schedule: Once(now), target: gocron.ErrOneTimeJobStartDateTimePast},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.schedule.validate(now, time.UTC)
			if tt.target == nil {
				testx.NoError(t, err)
				return
			}
			testx.ErrorIs(t, err, tt.target)
		})
	}
}
