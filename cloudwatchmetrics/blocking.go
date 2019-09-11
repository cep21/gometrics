package cloudwatchmetrics

import (
	"context"

	"github.com/cep21/gometrics/cwmessagebatch"
	"github.com/cep21/gometrics/metrics"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
)

type Config struct {
	StorageResolution int64
	Namespace         string
}

type CloudwatchAggregator struct {
	Sender cwmessagebatch.Aggregator
	Config Config
}

func (b *CloudwatchAggregator) namespace() *string {
	if b.Config.Namespace == "" {
		return aws.String("custom")
	}
	return &b.Config.Namespace
}

func (b *CloudwatchAggregator) storageResolution() *int64 {
	if b.Config.StorageResolution != 0 {
		return &b.Config.StorageResolution
	}
	// Cloudwatch default is 60 sec
	return nil
}

func (b *CloudwatchAggregator) Aggregate(ctx context.Context, aggregations []metrics.TimeSeriesAggregation) error {
	allDatum := make([]*cloudwatch.MetricDatum, 0, len(aggregations))
	for _, agg := range aggregations {
		datum := intoMetricDatum(agg, b.storageResolution())
		if datum == nil {
			continue
		}
		allDatum = append(allDatum, datum)
	}
	_, err := b.Sender.PutMetricDataWithContext(ctx, &cloudwatch.PutMetricDataInput{
		MetricData: allDatum,
		Namespace:  b.namespace(),
	})
	return err
}

func intoMetricDatum(m metrics.TimeSeriesAggregation, storageResolution *int64) *cloudwatch.MetricDatum {
	if m.Aggregation.Va.SampleCount == 0 {
		// Cloudwatch just doesn't let you send sample count 0.  Not sure what to do here ...
		return nil
	}
	baseDatum := &cloudwatch.MetricDatum{
		Dimensions:        awsDimensions(m.TS.Tsi.Dimensions),
		MetricName:        &m.TS.Tsi.MetricName,
		Timestamp:         aws.Time(m.Aggregation.Tw.Middle().UTC()),
		StorageResolution: storageResolution,
	}
	if unit := m.TS.Tsm.Value(metrics.MetaDataUnit); unit != nil {
		if unitAsS, ok := unit.(string); ok {
			baseDatum.Unit = aws.String(unitAsS)
		}
	}
	if m.Aggregation.Va.SampleCount == 1 {
		baseDatum.Value = &m.Aggregation.Va.Sum
		return baseDatum
	}
	baseDatum.StatisticValues = statisticsSet(m.Aggregation.Va)
	countsAllOne := true
	for _, b := range m.Aggregation.Va.Buckets {
		if b.Count != 1 {
			countsAllOne = false
		}
		baseDatum.Counts = append(baseDatum.Counts, aws.Float64(float64(b.Count)))
		baseDatum.Values = append(baseDatum.Values, aws.Float64(b.Middle()))
	}
	if countsAllOne {
		baseDatum.Counts = nil
	}
	return baseDatum
}

func statisticsSet(va metrics.ValueAggregation) *cloudwatch.StatisticSet {
	return &cloudwatch.StatisticSet{
		Maximum:     &va.Maximum,
		Minimum:     &va.Minimum,
		SampleCount: aws.Float64(float64(va.SampleCount)),
		Sum:         &va.Sum,
	}
}

func awsDimensions(dims map[string]string) []*cloudwatch.Dimension {
	ret := make([]*cloudwatch.Dimension, 0, len(dims))
	for k, v := range dims {
		ret = append(ret, &cloudwatch.Dimension{
			Name:  aws.String(k),
			Value: aws.String(v),
		})
	}
	return ret
}

var _ metrics.AggregationSink = &CloudwatchAggregator{}
