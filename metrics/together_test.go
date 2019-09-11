package metrics_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cep21/gometrics/metrics"
	"github.com/cep21/gometrics/metrics/metricsext"
	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/require"
)

type countingDest struct {
	i int64
}

func (c *countingDest) Aggregate(_ context.Context, aggs []metrics.TimeSeriesAggregation) error {
	for _, a := range aggs {
		atomic.AddInt64(&c.i, int64(a.Aggregation.Va.SampleCount))
	}
	return nil
}

var _ metrics.AggregationSink = &countingDest{}

func TestAllTogether(t *testing.T) {
	mockClock := clock.NewMock()
	cd := countingDest{}
	reg := &metrics.Registry{
		AggregationConstructor: func(ts *metrics.TimeSeries) metrics.Aggregator {
			return &metricsext.RollingAggregation{
				Now: mockClock.Now,
				AggregatorFactory: func() metrics.ValueAggregator {
					return &metricsext.LocklessValueAggregator{}
				},
			}
		},
	}

	buff := &metricsext.BufferedSink{
		Destination: &cd,
		Config: metricsext.BufferConfig{
			OnDroppedAggregation: func(e error, aggregations []metrics.TimeSeriesAggregation) {
				panic("Should not happen")
			},
		},
	}
	flusher := metricsext.PeriodicFlusher{
		TimeTicker: func(duration time.Duration) (times <-chan time.Time, i func()) {
			tt := mockClock.Ticker(duration)
			return tt.C, tt.Stop
		},
		Flushable: &metricsext.AggregationFlusher{
			Source: reg,
			Sink:   buff,
		},
	}
	metricsext.Float(reg, "one_every_sec", nil).Observe(1)
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		require.NoError(t, buff.Start())
	}()
	go func() {
		defer wg.Done()
		require.NoError(t, flusher.Start())
	}()
	mockClock.Add(time.Minute)
	time.Sleep(time.Millisecond)
	mockClock.Add(time.Minute)
	time.Sleep(time.Millisecond)
	mockClock.Add(time.Minute)
	time.Sleep(time.Millisecond)

	require.NoError(t, flusher.Close())
	require.NoError(t, buff.Close())
	wg.Wait()
	require.EqualValues(t, 1, cd.i)
}

func TestRollups(t *testing.T) {
	mockClock := clock.NewMock()
	mockClock.Set(time.Now())
	cd := countingDest{}
	baseReg := &metrics.Registry{
		AggregationConstructor: func(ts *metrics.TimeSeries) metrics.Aggregator {
			return &metricsext.RollingAggregation{
				Now: mockClock.Now,
				AggregatorFactory: func() metrics.ValueAggregator {
					return &metricsext.LocklessValueAggregator{}
				},
			}
		},
	}
	reg := metricsext.RegistryWithRollup(baseReg, []string{"name"})

	buff := &metricsext.BufferedSink{
		Destination: &metricsext.RollupSink{
			Sink: &metricsext.CompressionSink{
				Sink: &cd,
			},
			TSSource: reg,
		},
		Config: metricsext.BufferConfig{
			OnDroppedAggregation: func(e error, aggregations []metrics.TimeSeriesAggregation) {
				panic("Should not happen")
			},
		},
	}
	flusher := metricsext.PeriodicFlusher{
		TimeTicker: func(duration time.Duration) (times <-chan time.Time, i func()) {
			tt := mockClock.Ticker(duration)
			return tt.C, tt.Stop
		},
		Flushable: &metricsext.AggregationFlusher{
			Source: baseReg,
			Sink:   buff,
		},
	}
	metricsext.Float(reg, "one_every_sec", map[string]string{"name": "jack", "place": "dallas"}).Observe(1)
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		require.NoError(t, buff.Start())
	}()
	go func() {
		defer wg.Done()
		require.NoError(t, flusher.Start())
	}()
	mockClock.Add(time.Minute)
	time.Sleep(time.Millisecond)
	mockClock.Add(time.Minute)
	time.Sleep(time.Millisecond)
	mockClock.Add(time.Minute)
	time.Sleep(time.Millisecond)

	require.NoError(t, flusher.Close())
	require.NoError(t, buff.Close())
	wg.Wait()
	require.EqualValues(t, 2, cd.i)
}
