package metricsext

import (
	"testing"

	"github.com/cep21/gometrics/metrics"
	"github.com/stretchr/testify/require"
)

type rollupFromTimeSeriesRun struct {
	name     string
	ts       *metrics.TimeSeries
	TSSource metrics.TimeSeriesSource
	rollup   []string

	res *metrics.TimeSeries
}

func TestRollupFromTimeSeries(t *testing.T) {
	runs := []rollupFromTimeSeriesRun{
		{
			name: "Substage",
			ts: &metrics.TimeSeries{
				Tsi: metrics.TimeSeriesIdentifier{
					MetricName: "hi",
					Dimensions: map[string]string{
						"Substage": "canary",
						"Stage":    "production",
					},
				},
			},
			TSSource: &metrics.Registry{},
			rollup:   []string{"Substage"},

			res: &metrics.TimeSeries{
				Tsi: metrics.TimeSeriesIdentifier{
					MetricName: "hi",
					Dimensions: map[string]string{
						"Stage": "production",
					},
				},
			},
		},
	}
	for _, run := range runs {
		t.Run(run.name, func(t *testing.T) {
			require.Equal(t, run.res.Tsi.UID(), rollupFromTimeSeries(run.ts, run.TSSource, run.rollup).Tsi.UID())
		})
	}
}
