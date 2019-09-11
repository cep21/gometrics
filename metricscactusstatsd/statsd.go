package metricscactusstatsd

import (
	"errors"
	"strings"
	"time"

	"github.com/cep21/gometrics/metrics"
	"github.com/cep21/gometrics/metrics/metricsext"
	"github.com/cactus/go-statsd-client/statsd"
)

// Statsd does statsd metrics to a metric registry
// Namespacing picked from https://github.com/statsd/statsd/blob/master/docs/namespacing.md
type Statsd struct {
	Registry metrics.BaseRegistry
	prefix   string
}

func (s *Statsd) Inc(key string, v int64, _ float32) error {
	metricsext.Counter(s.Registry, "counters."+s.metricName(key), nil).Observe(float64(v))
	return nil
}

func (s *Statsd) Dec(key string, v int64, _ float32) error {
	// Cloudwatch is dumb and doing this will cause you to loose your p99 metrics
	metricsext.Counter(s.Registry, "counters."+s.metricName(key), nil).Observe(float64(-v))
	return nil
}

func (s *Statsd) Gauge(key string, v int64, _ float32) error {
	metricsext.Gauge(s.Registry, "gauges."+s.metricName(key), nil).Observe(float64(v))
	return nil
}

func (s *Statsd) GaugeDelta(key string, v int64, _ float32) error {
	return errors.New("gauge delta is unimplemented")
}

func (s *Statsd) Timing(key string, v int64, f float32) error {
	return s.TimingDuration(key, time.Duration(v*time.Millisecond.Nanoseconds()), f)
}

func (s *Statsd) TimingDuration(key string, v time.Duration, _ float32) error {
	metricsext.Duration(s.Registry, "timers."+s.metricName(key), nil).Observe(v)
	return nil
}

func (s *Statsd) Set(string, string, float32) error {
	return errors.New("set is unimplemented")
}

func (s *Statsd) SetInt(string, int64, float32) error {
	return errors.New("set int is unimplemented")
}

func (s *Statsd) Raw(string, string, float32) error {
	return errors.New("raw is unimplemented")
}

func (s *Statsd) SetSamplerFunc(statsd.SamplerFunc) {
	// Not worth implementing
}

func (s *Statsd) metricName(key string) string {
	if len(s.prefix) <= 0 {
		return key
	}
	return s.prefix + "." + key
}

func (s *Statsd) NewSubStatter(key string) statsd.SubStatter {
	key = strings.Trim(key, ".")
	if len(key) <= 0 {
		return s
	}
	return &Statsd{
		Registry: s.Registry,
		prefix:   key,
	}
}

var _ statsd.SubStatter = &Statsd{}
