package metricsext

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/cep21/gometrics/metrics"
)

// BufferConfig configures a Buffer
type BufferConfig struct {
	BufferSize           int
	OnDroppedAggregation func(error, []metrics.TimeSeriesAggregation)
	FlushTimeout         time.Duration
	BlockOnAggregate     bool
}

// BufferedSink aggregations before they are sent to a sink
type BufferedSink struct {
	Destination metrics.AggregationSink
	Config      BufferConfig

	buff      chan []metrics.TimeSeriesAggregation
	onClose   chan struct{}
	startDone chan struct{}
	setupOnce sync.Once

	startEverStarted     bool
	startEverStartedOnce sync.Once
}

func (b *BufferedSink) setupInStart(inStart bool) {
	b.setupStruct()
	b.startEverStartedOnce.Do(func() {
		b.startEverStarted = inStart
	})
}

func (b *BufferedSink) setupStruct() {
	b.setupOnce.Do(func() {
		b.onClose = make(chan struct{})
		b.startDone = make(chan struct{})
		buffSize := b.Config.BufferSize
		if buffSize == 0 {
			buffSize = 12
		}
		b.buff = make(chan []metrics.TimeSeriesAggregation, buffSize)
	})
}

func (b *BufferedSink) onDropped(err error, aggs []metrics.TimeSeriesAggregation) {
	if b.Config.OnDroppedAggregation != nil {
		b.Config.OnDroppedAggregation(err, aggs)
	}
}

// Aggregate adds aggregations to a channel then returns.  Does not block on context by default.
func (b *BufferedSink) Aggregate(ctx context.Context, aggs []metrics.TimeSeriesAggregation) error {
	b.setupStruct()
	select {
	case b.buff <- aggs:
		return nil
	default:
		if b.Config.BlockOnAggregate {
			select {
			case <-ctx.Done():
				b.onDropped(ctx.Err(), aggs)
				return ctx.Err()
			case b.buff <- aggs:
				return nil
			}
		}
		b.onDropped(nil, aggs)
		return errors.New("dropped aggregation: chan full")
	}
}

// Start blocks until Close is called: draining from the buffer into a sink
func (b *BufferedSink) Start() error {
	b.setupInStart(true)
	defer close(b.startDone)
	for {
		select {
		case <-b.onClose:
			return nil
		case aggs := <-b.buff:
			if err := b.flushWithContext(aggs); err != nil {
				b.onDropped(err, aggs)
			}
		}
	}
}

// Close stops the goroutine that drains aggregations from the buffer
func (b *BufferedSink) Close() error {
	b.setupInStart(false)
	close(b.onClose)
	if b.startEverStarted {
		<-b.startDone
	}
	return nil
}

func (b *BufferedSink) flushWithContext(aggs []metrics.TimeSeriesAggregation) error {
	ctx := context.Background()
	if b.Config.FlushTimeout != 0 {
		var onDone context.CancelFunc
		ctx, onDone = context.WithTimeout(ctx, b.Config.FlushTimeout)
		defer onDone()
	}
	return b.Destination.Aggregate(ctx, aggs)
}

var _ metrics.AggregationSink = &BufferedSink{}
