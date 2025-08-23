package bitemporal

import (
	"context"
	"time"
)

const temporalContextKey = "TemporalContextKey"

type TemporalContext struct {
	ValidMoment  time.Time
	SystemMoment time.Time
}

// InitializeContext if there is no temporalContext set it to now now
func InitializeContext(ctx context.Context) context.Context {
	if _, ok := ctx.Value(temporalContextKey).(TemporalContext); !ok {
		ctx = WithSystemMoment(ctx, time.Now())
		ctx = WithValidTime(ctx, time.Now())
	}
	return ctx
}

func GetValidMoment(ctx context.Context) time.Time {
	if t, ok := ctx.Value(temporalContextKey).(TemporalContext); ok {
		return t.ValidMoment
	}
	return time.Time{}
}

func WithValidTime(ctx context.Context, moment time.Time) context.Context {
	t, _ := ctx.Value(temporalContextKey).(TemporalContext)
	t.ValidMoment = moment
	return context.WithValue(ctx, temporalContextKey, t)
}

func GetSystemMoment(ctx context.Context) time.Time {
	if t, ok := ctx.Value(temporalContextKey).(TemporalContext); ok {
		return t.SystemMoment
	}
	return time.Time{}
}

func WithSystemMoment(ctx context.Context, moment time.Time) context.Context {
	t, _ := ctx.Value(temporalContextKey).(TemporalContext)
	t.SystemMoment = moment
	return context.WithValue(ctx, temporalContextKey, t)
}
