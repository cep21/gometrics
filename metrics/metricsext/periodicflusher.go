package metricsext

import (
	"context"
	"sync"
	"time"

	"github.com/cep21/gometrics/metrics"
)

// Logger is used to log generic key/value pairs of information
type Logger interface {
	Log(kvs ...interface{})
}

// Flushable is anything that can be "flushed" in some periodic fashion.  Used by PeriodicFlusher
type Flushable interface {
	Flush(ctx context.Context) error
}

// AggregationFlusher implements the Flushable interface to send metrics from a source to a sink
type AggregationFlusher struct {
	Source metrics.AggregationSource
	Sink   metrics.AggregationSink
}

// Flush sends all metrics from soure, into sink
func (f *AggregationFlusher) Flush(ctx context.Context) error {
	return f.Sink.Aggregate(ctx, f.Source.FlushMetrics())
}

var _ Flushable = &AggregationFlusher{}

// PeriodicFlusher calls some flush method every X period of time
type PeriodicFlusher struct {
	Flushable Flushable

	// Optional
	FlushTimeout time.Duration
	Interval     time.Duration
	Logger       Logger
	// Returns a time.Ticker that sends time to chan on duration interval.  func() is the cleanup function for this
	// ticker.  Uses time.Ticker by default
	TimeTicker func(time.Duration) (<-chan time.Time, func())

	once             sync.Once
	startEverStarted bool
	startDone        chan struct{}
	onClose          chan struct{}
}

func (f *PeriodicFlusher) interval() time.Duration {
	if f.Interval == 0 {
		return time.Minute
	}
	return f.Interval
}

func (f *PeriodicFlusher) setup(inStart bool) {
	f.once.Do(func() {
		f.onClose = make(chan struct{})
		f.startDone = make(chan struct{})
		f.startEverStarted = inStart
	})
}

func (f *PeriodicFlusher) flushWithContext() error {
	ctx := context.Background()
	if f.FlushTimeout != 0 {
		var onDone context.CancelFunc
		ctx, onDone = context.WithTimeout(ctx, f.FlushTimeout)
		defer onDone()
	}
	return f.Flushable.Flush(ctx)
}

// Start blocks till Close ends, calling Flushable every Interval duration, or until Close is called
func (f *PeriodicFlusher) Start() error {
	f.setup(true)
	defer close(f.startDone)
	timeChan, onClose := f.timeTicker(f.interval())
	defer onClose()
	for {
		select {
		case <-f.onClose:
			if err := f.flushWithContext(); err != nil {
				f.log("err", err)
			}
			return nil
		case <-timeChan:
			if err := f.flushWithContext(); err != nil {
				f.log("err", err)
			}
		}
	}
}

// Close stops the flushing calls
func (f *PeriodicFlusher) Close() error {
	f.setup(false)
	close(f.onClose)
	if f.startEverStarted {
		<-f.startDone
	}
	return nil
}

func (f *PeriodicFlusher) log(kvs ...interface{}) {
	if f.Logger != nil {
		f.Logger.Log(kvs...)
	}
}

func (f *PeriodicFlusher) timeTicker(interval time.Duration) (<-chan time.Time, func()) {
	if f.TimeTicker != nil {
		return f.TimeTicker(interval)
	}
	tick := time.NewTicker(interval)
	return tick.C, tick.Stop
}
