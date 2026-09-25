package taskkit

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/go-sdk/core/errx"
	"github.com/robfig/cron/v3"
)

// Schedule 描述任务的调度规则。具体实现由 taskkit 提供。
type Schedule interface {
	fmt.Stringer
	definition() gocron.JobDefinition
	validate(time.Time, *time.Location) error
}

type cronSchedule struct {
	expression  string
	withSeconds bool
}

func (s cronSchedule) String() string {
	if s.withSeconds {
		return "cron with seconds: " + s.expression
	}
	return "cron: " + s.expression
}

func (s cronSchedule) definition() gocron.JobDefinition {
	return gocron.CronJob(s.expression, s.withSeconds)
}

func (s cronSchedule) validate(now time.Time, location *time.Location) error {
	expression := s.expression
	if !strings.HasPrefix(expression, "TZ=") && !strings.HasPrefix(expression, "CRON_TZ=") {
		expression = fmt.Sprintf("CRON_TZ=%s %s", location.String(), expression)
	}
	var (
		parsed cron.Schedule
		err    error
	)
	if s.withSeconds {
		parser := cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
		parsed, err = parser.Parse(expression)
	} else {
		parsed, err = cron.ParseStandard(expression)
	}
	if err != nil {
		return errx.Join(gocron.ErrCronJobParse, err)
	}
	if parsed.Next(now).IsZero() {
		return gocron.ErrCronJobInvalid
	}
	return nil
}

type durationSchedule struct {
	interval time.Duration
}

func (s durationSchedule) String() string { return "every " + s.interval.String() }

func (s durationSchedule) definition() gocron.JobDefinition {
	return gocron.DurationJob(s.interval)
}

func (s durationSchedule) validate(_ time.Time, _ *time.Location) error {
	if s.interval == 0 {
		return gocron.ErrDurationJobIntervalZero
	}
	if s.interval < 0 {
		return gocron.ErrDurationJobIntervalNegative
	}
	return nil
}

type onceSchedule struct {
	at time.Time
}

func (s onceSchedule) String() string { return "once at " + s.at.Format(time.RFC3339Nano) }

func (s onceSchedule) definition() gocron.JobDefinition {
	return gocron.OneTimeJob(gocron.OneTimeJobStartDateTime(s.at))
}

func (s onceSchedule) validate(now time.Time, _ *time.Location) error {
	if !s.at.After(now) {
		return gocron.ErrOneTimeJobStartDateTimePast
	}
	return nil
}

// Cron 创建使用标准五段表达式的 Cron 调度规则。
func Cron(expression string) Schedule {
	return cronSchedule{expression: expression}
}

// CronWithSeconds 创建使用六段表达式且包含秒字段的 Cron 调度规则。
func CronWithSeconds(expression string) Schedule {
	return cronSchedule{expression: expression, withSeconds: true}
}

// Every 创建按固定时间间隔运行的调度规则。
func Every(interval time.Duration) Schedule {
	return durationSchedule{interval: interval}
}

// Once 创建在指定时间运行一次的调度规则。
func Once(at time.Time) Schedule {
	return onceSchedule{at: at}
}
