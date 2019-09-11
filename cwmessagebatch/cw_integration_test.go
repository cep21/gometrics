// +build integration

package cwmessagebatch

import (
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/codahale/hdrhistogram"

	"github.com/aws/aws-sdk-go/aws/awsutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const freshRunInt = 6

var datumTimestamp time.Time

func init() {
	datumTimestamp = time.Now().UTC().Truncate(time.Second)
}
func baseDatum(metricName string) *cloudwatch.MetricDatum {
	return &cloudwatch.MetricDatum{
		Timestamp:         &datumTimestamp,
		MetricName:        aws.String(generateMetricName(metricName)),
		StorageResolution: aws.Int64(1),
	}
}

type expectedPoints func(t *testing.T)

var testNamespace = "custom/cwmessagebatch"

type datumStruct struct {
	name string
	f    func(t *testing.T) expectedPoints
}

func TestIntegrationAggregator(t *testing.T) {
	runs := []datumStruct{
		{
			name: "ManyDatum",
			f:    testManyDatum,
		},
		{
			name: "testSendingZero",
			f:    testSendingZero,
		},
		{
			name: "testHdrHistogram",
			f:    testHdrHistogram,
		},
		{
			name: "ValuesOneDatumNoStatistics",
			f:    testManyValuesOneDatumNoStatistics,
		},
		{
			name: "SameTSMultipleStatistics",
			f:    testSameTSMultipleStatistics,
		},
		{
			name: "StatisticsSetLies",
			f:    testStatisticsSetLies,
		},
		{
			name: "ManyValuesOneDatumWithCloseStatistics",
			f:    testManyValuesOneDatumWithCloseStatistics,
		},
		{
			name: "ManyValuesBadSampleCount",
			f:    testManyValuesBadSampleCount,
		},
		{
			name: "ManyValuesOneDatumWithStatistics",
			f:    testManyValuesOneDatumWithStatistics,
		},
		{
			name: "ManyValuesNoSplitOneDatumWithStatistics",
			f:    testManyValuesNoSplitOneDatumWithStatistics,
		},
		{
			name: "ManyValuesSplitOneDatumWithStatistics",
			f:    testManyValuesSplitOneDatumWithStatistics,
		},
		{
			name: "TestPyramidHeight",
			f:    testPyramidHeight,
		},
		{
			name: "TestPyramidHeightOffsetAggregation",
			f:    testPyramidHeightOffsetAggregation,
		},
	}
	verify := make([]expectedPoints, 0, len(runs))
	verifyNames := make([]string, 0, len(runs))
	for _, run := range runs {
		t.Run(run.name, func(t *testing.T) {
			verification := run.f(t)
			if verification != nil {
				verify = append(verify, verification)
				verifyNames = append(verifyNames, run.name)
			}
		})
	}
	t.Log("Sleeping for cloudwatch to process datapoints")
	time.Sleep(time.Second * 3)
	t.Log("Verifying points")
	for i, v := range verify {
		t.Run("Verify"+verifyNames[i], func(t *testing.T) {
			v(t)
		})
	}
}

// This works fine
func testManyDatum(t *testing.T) expectedPoints {
	a := setupClient(t, nil)
	// Should become 3 batches of the same datum
	const numValues = 21
	// Make a bunch of datum
	dat := make([]*cloudwatch.MetricDatum, 0, numValues)
	for i := 0; i < numValues; i++ {
		dt := baseDatum("TestIntegrationAggregatorManyDatum")
		dt.Value = aws.Float64(float64(i))
		dat = append(dat, dt)
	}
	_, err := a.PutMetricData(&cloudwatch.PutMetricDataInput{
		Namespace:  &testNamespace,
		MetricData: dat,
	})
	// This should split into two 3 requests
	require.NoError(t, err)
	return func(t *testing.T) {
		matchSingleDatum(t, baseDatum("TestIntegrationAggregatorManyDatum"), a.Client, &cloudwatch.Datapoint{
			SampleCount: aws.Float64(numValues),
			Minimum:     aws.Float64(0),
			Maximum:     aws.Float64(numValues - 1),
			Sum:         aws.Float64(numValues * (numValues - 1) / 2),
			ExtendedStatistics: map[string]*float64{
				"p50": aws.Float64(10.330486782497703),
			},
		})
	}
}

// This works fine (stat set ignored)
func testManyValuesOneDatumNoStatistics(t *testing.T) expectedPoints {
	a := setupClient(t, nil)
	// Should become 3 batches of values, but without statistics set
	const numValues = 150*2 + 1
	// Make a bunch of datum
	dat := baseDatum("TestIntegrationAggregatorManyValuesOneDatumNoStatistics")
	for i := 0; i < numValues; i++ {
		dat.Counts = append(dat.Counts, aws.Float64(1))
		dat.Values = append(dat.Values, aws.Float64(float64(i)*10))
	}
	_, err := a.PutMetricData(&cloudwatch.PutMetricDataInput{
		Namespace:  &testNamespace,
		MetricData: []*cloudwatch.MetricDatum{dat},
	})
	require.NoError(t, err)
	return func(t *testing.T) {
		matchSingleDatum(t, baseDatum("TestIntegrationAggregatorManyValuesOneDatumNoStatistics"), a.Client, &cloudwatch.Datapoint{
			SampleCount: aws.Float64(numValues),
			Minimum:     aws.Float64(0),
			Maximum:     aws.Float64(numValues*10 - 10),
			Sum:         aws.Float64(10 * (numValues * (numValues - 1) / 2)),
			ExtendedStatistics: map[string]*float64{
				"p50": aws.Float64(1502.7563988172972),
			},
		})
	}
}

// This works fine (no stats expected)
func testSameTSMultipleStatistics(t *testing.T) expectedPoints {
	a := setupClient(t, nil)
	dat := baseDatum("TestIntegrationAggregatorSameTSMultipleStatistics")
	dat.StatisticValues = &cloudwatch.StatisticSet{
		Minimum:     aws.Float64(2),
		Maximum:     aws.Float64(20),
		SampleCount: aws.Float64(5),
		Sum:         aws.Float64(44),
	}
	dat2 := baseDatum("TestIntegrationAggregatorSameTSMultipleStatistics")
	dat2.StatisticValues = &cloudwatch.StatisticSet{
		Minimum:     aws.Float64(1),
		Maximum:     aws.Float64(10),
		SampleCount: aws.Float64(5),
		Sum:         aws.Float64(19),
	}
	_, err := a.PutMetricData(&cloudwatch.PutMetricDataInput{
		Namespace:  &testNamespace,
		MetricData: []*cloudwatch.MetricDatum{dat, dat2},
	})
	require.NoError(t, err)
	return func(t *testing.T) {
		matchSingleDatum(t, baseDatum("TestIntegrationAggregatorSameTSMultipleStatistics"), a.Client, &cloudwatch.Datapoint{
			SampleCount: aws.Float64(10),
			Minimum:     aws.Float64(1),
			Maximum:     aws.Float64(20),
			Sum:         aws.Float64(63),
		})
	}
}

// The values and the statistics set don't make sense together.  They are pretty much impossible.  See what happens
func testStatisticsSetLies(t *testing.T) expectedPoints {
	a := setupClient(t, nil)
	const numValues = 10
	// Make a bunch of datum
	dat := baseDatum("TestIntegrationAggregatorStatisticsSetLies")
	dat.StatisticValues = &cloudwatch.StatisticSet{
		Minimum:     aws.Float64(0),
		Maximum:     aws.Float64(1000),
		SampleCount: aws.Float64(100),
		Sum:         aws.Float64(5000),
	}
	// Values get ignored ... sad face :/
	for i := 0; i < numValues; i++ {
		dat.Counts = append(dat.Counts, aws.Float64(1))
		dat.Values = append(dat.Values, aws.Float64(float64(i)*10))
	}
	_, err := a.PutMetricData(&cloudwatch.PutMetricDataInput{
		Namespace:  &testNamespace,
		MetricData: []*cloudwatch.MetricDatum{dat},
	})
	require.NoError(t, err)
	return func(t *testing.T) {
		matchSingleDatum(t, baseDatum("TestIntegrationAggregatorStatisticsSetLies"), a.Client, &cloudwatch.Datapoint{
			SampleCount: aws.Float64(100),
			Minimum:     aws.Float64(0),
			Maximum:     aws.Float64(1000),
			Sum:         aws.Float64(5000),
		})
	}
}

// This works fine (since limit <= 150 no splitting required)
func testManyValuesOneDatumWithCloseStatistics(t *testing.T) expectedPoints {
	a := setupClient(t, nil)
	// Should become 3 batches
	//const numValues = 150 * 2 + 1
	const numValues = 149
	// Make a bunch of datum
	dat := baseDatum("TestIntegrationAggregatorManyValuesOneDatumWithCloseStatistics")
	dat.StatisticValues = &cloudwatch.StatisticSet{
		Minimum:     aws.Float64(0),                                 // Keep min the same
		Maximum:     aws.Float64((numValues-1)*10 + 10000),          // Move the max and sum way up
		SampleCount: aws.Float64(numValues),                         // Sample count still matches
		Sum:         aws.Float64(numValues*(numValues-1)/2 + 10000), // Move the max and sum way up
	}
	for i := 0; i < numValues; i++ {
		dat.Counts = append(dat.Counts, aws.Float64(1))
		dat.Values = append(dat.Values, aws.Float64(float64(i)*10))
	}
	_, err := a.PutMetricData(&cloudwatch.PutMetricDataInput{
		Namespace:  &testNamespace,
		MetricData: []*cloudwatch.MetricDatum{dat},
	})
	require.NoError(t, err)
	return func(t *testing.T) {
		matchSingleDatum(t, baseDatum("TestIntegrationAggregatorManyValuesOneDatumWithCloseStatistics"), a.Client, &cloudwatch.Datapoint{
			SampleCount: aws.Float64(numValues),
			Minimum:     aws.Float64(0),
			Maximum:     aws.Float64((numValues-1)*10 + 10000),
			Sum:         aws.Float64(numValues*(numValues-1)/2 + 10000),
		})
	}
}

// Bad sample count doesn't work
func testManyValuesBadSampleCount(t *testing.T) expectedPoints {
	a := setupClient(t, nil)
	// Should become 3 batches
	//const numValues = 150 * 2 + 1
	const numValues = 149
	// Make a bunch of datum
	dat := baseDatum("TestIntegrationAggregatorManyValuesBadSampleCount")
	dat.StatisticValues = &cloudwatch.StatisticSet{
		Minimum:     aws.Float64(0),
		Maximum:     aws.Float64((numValues - 1) * 10),
		SampleCount: aws.Float64(numValues - 100),
		Sum:         aws.Float64(numValues * (numValues - 1) / 2),
	}
	for i := 0; i < numValues; i++ {
		dat.Counts = append(dat.Counts, aws.Float64(1))
		dat.Values = append(dat.Values, aws.Float64(float64(i)*10))
	}
	_, err := a.PutMetricData(&cloudwatch.PutMetricDataInput{
		Namespace:  &testNamespace,
		MetricData: []*cloudwatch.MetricDatum{dat},
	})
	require.NoError(t, err)
	return func(t *testing.T) {
		matchSingleDatum(t, baseDatum("TestIntegrationAggregatorManyValuesBadSampleCount"), a.Client, &cloudwatch.Datapoint{
			SampleCount: aws.Float64(numValues - 100),
			Minimum:     aws.Float64(0),
			Maximum:     aws.Float64((numValues - 1) * 10),
			Sum:         aws.Float64(numValues * (numValues - 1) / 2),
		})
	}
}

// This works fine (since limit <= 150 no splitting required)
func testManyValuesOneDatumWithStatistics(t *testing.T) expectedPoints {
	a := setupClient(t, nil)
	// Should become 3 batches
	//const numValues = 150 * 2 + 1
	const numValues = 149
	// Make a bunch of datum
	dat := baseDatum("TestIntegrationAggregatorManyValuesOneDatumWithStatistics")
	dat.StatisticValues = &cloudwatch.StatisticSet{
		Minimum:     aws.Float64(0),
		Maximum:     aws.Float64((numValues - 1) * 10),
		Sum:         aws.Float64(numValues * (numValues - 1) / 2),
		SampleCount: aws.Float64(numValues),
	}
	for i := 0; i < numValues; i++ {
		dat.Counts = append(dat.Counts, aws.Float64(1))
		dat.Values = append(dat.Values, aws.Float64(float64(i)*10))
	}
	_, err := a.PutMetricData(&cloudwatch.PutMetricDataInput{
		Namespace:  &testNamespace,
		MetricData: []*cloudwatch.MetricDatum{dat},
	})
	require.NoError(t, err)
	return func(t *testing.T) {
		matchSingleDatum(t, baseDatum("TestIntegrationAggregatorManyValuesOneDatumWithStatistics"), a.Client, &cloudwatch.Datapoint{
			SampleCount: aws.Float64(numValues),
			Minimum:     aws.Float64(0),
			Maximum:     aws.Float64((numValues - 1) * 10),
			Sum:         aws.Float64(numValues * (numValues - 1) / 2),
			ExtendedStatistics: map[string]*float64{
				"p50": aws.Float64(742.8110878212035),
			},
		})
	}
}

// Works fine since it fits
func testManyValuesNoSplitOneDatumWithStatistics(t *testing.T) expectedPoints {
	a := setupClient(t, nil)
	// Should be one batch.  Will work just fine
	const numValues = 150
	var arr []float64
	for i := 0; i < numValues; i++ {
		arr = append(arr, float64(i))
	}
	dat := baseDatum("TestIntegrationAggregatorManyValuesNoSplitOneDatumWithStatistics")
	makeDatum(dat, arr)
	_, err := a.PutMetricData(&cloudwatch.PutMetricDataInput{
		Namespace:  &testNamespace,
		MetricData: []*cloudwatch.MetricDatum{dat},
	})
	require.NoError(t, err)
	return func(t *testing.T) {
		matchSingleDatum(t, baseDatum("TestIntegrationAggregatorManyValuesNoSplitOneDatumWithStatistics"), a.Client, &cloudwatch.Datapoint{
			SampleCount: aws.Float64(numValues),
			Minimum:     aws.Float64(0),
			Maximum:     aws.Float64(numValues - 1),
			Sum:         aws.Float64(numValues * (numValues - 1) / 2),
			ExtendedStatistics: map[string]*float64{
				"p50": aws.Float64(74.64814214590068),
			},
		})
	}
}

func testSendingZero(t *testing.T) expectedPoints {
	a := setupClient(t, nil)
	dat := baseDatum("testSendingZero")
	_, err := a.PutMetricData(&cloudwatch.PutMetricDataInput{
		Namespace:  &testNamespace,
		MetricData: []*cloudwatch.MetricDatum{dat},
	})
	require.Error(t, err)

	//Expect 300 data points.
	return nil
}

func testHdrHistogram(t *testing.T) expectedPoints {
	a := setupClient(t, nil)
	// Create a huge histogram
	const numValues = 10000
	h := hdrhistogram.New(0, numValues, 2)
	sum := float64(0)
	for i := 0; i < numValues; i++ {
		sum += float64(i)
		require.NoError(t, h.RecordValue(int64(i)))
	}
	dat := baseDatum("TestHdrHistogram")
	for _, bar := range h.Distribution() {
		dat.Values = append(dat.Values, aws.Float64(float64(bar.From+bar.To)/2))
		dat.Counts = append(dat.Counts, aws.Float64(float64(bar.Count)))
	}
	dat.StatisticValues = &cloudwatch.StatisticSet{
		Maximum:     aws.Float64(float64(h.Max())),
		Minimum:     aws.Float64(float64(h.Min())),
		Sum:         &sum,
		SampleCount: aws.Float64(float64(h.TotalCount())),
	}
	_, err := a.PutMetricData(&cloudwatch.PutMetricDataInput{
		Namespace:  &testNamespace,
		MetricData: []*cloudwatch.MetricDatum{dat},
	})
	require.NoError(t, err)
	return func(t *testing.T) {
		matchSingleDatum(t, baseDatum("TestHdrHistogram"), a.Client, &cloudwatch.Datapoint{
			SampleCount: aws.Float64(numValues),
			Minimum:     aws.Float64(0),
			Maximum:     aws.Float64(10047),
			Sum:         aws.Float64(sum),
			ExtendedStatistics: map[string]*float64{
				"p50": aws.Float64(4993.860642759922),
			},
		})
	}
}

func testManyValuesSplitOneDatumWithStatistics(t *testing.T) expectedPoints {
	a := setupClient(t, nil)
	// Should become 2 batches of 150 each
	const numValues = 150 * 2
	expectedSum := 0
	var arr []float64
	for i := 0; i < numValues; i++ {
		arr = append(arr, float64(i))
		expectedSum += i
	}
	dat := baseDatum("TestIntegrationAggregatorManyValuesSplitOneDatumWithStatistics")
	makeDatum(dat, arr)
	_, err := a.PutMetricData(&cloudwatch.PutMetricDataInput{
		Namespace:  &testNamespace,
		MetricData: []*cloudwatch.MetricDatum{dat},
	})
	require.NoError(t, err)
	//Expect 300 data points.
	return func(t *testing.T) {
		matchSingleDatum(t, baseDatum("TestIntegrationAggregatorManyValuesSplitOneDatumWithStatistics"), a.Client, &cloudwatch.Datapoint{
			SampleCount: aws.Float64(numValues),
			Minimum:     aws.Float64(0),
			Maximum:     aws.Float64(numValues - 1),
			Sum:         aws.Float64(numValues * (numValues - 1) / 2),
			ExtendedStatistics: map[string]*float64{
				"p50": aws.Float64(148.97588388530315),
			},
		})
	}
}

func testPyramidHeight(t *testing.T) expectedPoints {
	a := setupClient(t, nil)
	// Should become 2 batches of 150 each
	const pyramidHeight = 100
	expectedSum := 0
	numValues := 0
	var arr []float64
	for i := 0; i < pyramidHeight; i++ {
		for j := 0; j < i; j++ {
			arr = append(arr, float64(i))
			expectedSum += i
			numValues++
		}
	}
	dat := baseDatum("testPyramidHeight")
	makeDatum(dat, arr)
	_, err := a.PutMetricData(&cloudwatch.PutMetricDataInput{
		Namespace:  &testNamespace,
		MetricData: []*cloudwatch.MetricDatum{dat},
	})
	require.NoError(t, err)
	//Expect 300 data points.
	return func(t *testing.T) {
		matchSingleDatum(t, baseDatum("testPyramidHeight"), a.Client, &cloudwatch.Datapoint{
			SampleCount: aws.Float64(float64(numValues)),
			Minimum:     aws.Float64(1),
			Maximum:     aws.Float64(pyramidHeight - 1),
			Sum:         aws.Float64(float64(expectedSum)),
			ExtendedStatistics: map[string]*float64{
				"p50": aws.Float64(70.38556285900441),
			},
		})
	}
}

func testPyramidHeightOffsetAggregation(t *testing.T) expectedPoints {
	a := setupClient(t, nil)
	// Should become 2 batches of 150 each
	const pyramidHeight = 100
	expectedSum := 0
	numValues := 0
	var arr []float64
	for i := 0; i < pyramidHeight; i++ {
		for j := 0; j < i; j++ {
			arr = append(arr, float64(i))
			expectedSum += i
			numValues++
		}
	}
	dat := baseDatum("testPyramidHeightOffsetAggregation")
	makeDatum(dat, arr)
	dat.StatisticValues.Sum = aws.Float64(*dat.StatisticValues.Sum + 100)
	dat.StatisticValues.Minimum = aws.Float64(0)
	dat.StatisticValues.Maximum = aws.Float64(*dat.StatisticValues.Maximum + 100)
	_, err := a.PutMetricData(&cloudwatch.PutMetricDataInput{
		Namespace:  &testNamespace,
		MetricData: []*cloudwatch.MetricDatum{dat},
	})
	require.NoError(t, err)
	//Expect 300 data points.
	return func(t *testing.T) {
		matchSingleDatum(t, baseDatum("testPyramidHeightOffsetAggregation"), a.Client, &cloudwatch.Datapoint{
			SampleCount: aws.Float64(float64(numValues)),
			Minimum:     aws.Float64(0),
			Maximum:     aws.Float64(pyramidHeight - 1 + 100),
			Sum:         aws.Float64(float64(expectedSum + 100)),
			ExtendedStatistics: map[string]*float64{
				"p50": aws.Float64(70.38556285900441),
			},
		})
	}
}

// -------------------- Helper functions below this

func printLogger(t *testing.T) func(r *request.Request) {
	return func(r *request.Request) {
		asR, ok := r.Params.(*cloudwatch.PutMetricDataInput)
		if ok {
			t.Log(awsutil.Prettify(asR))
		}
		if r.Error != nil {
			t.Log(r.Error)
		}
	}
}

func setupClient(t *testing.T, pb func(r *request.Request)) *Aggregator {
	sess, err := session.NewSession()
	assert.NoError(t, err)
	cwClient := cloudwatch.New(sess)
	if pb != nil {
		cwClient.Handlers.Complete.PushBack(pb)
	}
	return &Aggregator{
		Client: cwClient,
	}
}

func generateMetricName(s string) string {
	if freshRunInt == 0 {
		return s
	}
	return s + strconv.Itoa(freshRunInt)
}

func floatByCount(arr []float64) map[float64]int {
	ret := make(map[float64]int)
	for _, a := range arr {
		ret[a]++
	}
	return ret
}

func makeDatum(in *cloudwatch.MetricDatum, arr []float64) {
	if len(arr) == 0 {
		return
	}
	in.StatisticValues = &cloudwatch.StatisticSet{
		Minimum:     aws.Float64(arr[0]),
		Maximum:     aws.Float64(arr[0]),
		SampleCount: aws.Float64(1),
		Sum:         aws.Float64(arr[0]),
	}
	for i := 1; i < len(arr); i++ {
		f := aws.Float64(arr[i])
		if *f < *in.StatisticValues.Minimum {
			in.StatisticValues.Minimum = f
		}
		if *f > *in.StatisticValues.Maximum {
			in.StatisticValues.Maximum = f
		}
		in.StatisticValues.SampleCount = aws.Float64(*in.StatisticValues.SampleCount + 1)
		in.StatisticValues.Sum = aws.Float64(*in.StatisticValues.Sum + *f)
	}
	sort.Float64s(arr)
	floatCounts := floatByCount(arr)
	isAllOne := true
	for _, f := range arr {
		if count, exists := floatCounts[f]; exists {
			in.Values = append(in.Values, aws.Float64(float64(f)))
			in.Counts = append(in.Counts, aws.Float64(float64(count)))
			if count != 1 {
				isAllOne = false
			}
			delete(floatCounts, f)
		}
	}
	if isAllOne {
		in.Counts = nil
	}
}

func matchSingleDatum(t *testing.T, dt *cloudwatch.MetricDatum, client *cloudwatch.CloudWatch, dp *cloudwatch.Datapoint) {
	getOut, err := client.GetMetricStatistics(&cloudwatch.GetMetricStatisticsInput{
		MetricName: dt.MetricName,
		Dimensions: dt.Dimensions,
		StartTime:  dt.Timestamp,
		EndTime:    aws.Time(dt.Timestamp.Add(time.Duration(*dt.StorageResolution) * time.Second)),
		Period:     dt.StorageResolution,
		Namespace:  &testNamespace,
		Statistics: []*string{
			aws.String("Sum"),
			aws.String("Minimum"),
			aws.String("Maximum"),
			aws.String("SampleCount"),
		},
	})
	require.NoError(t, err)
	require.Len(t, getOut.Datapoints, 1)
	require.EqualValues(t, *getOut.Datapoints[0].SampleCount, *dp.SampleCount)
	require.EqualValues(t, getOut.Datapoints[0].Timestamp, dt.Timestamp)
	require.EqualValues(t, *getOut.Datapoints[0].Minimum, *dp.Minimum)
	require.EqualValues(t, *getOut.Datapoints[0].Maximum, *dp.Maximum)
	require.EqualValues(t, *getOut.Datapoints[0].Sum, *dp.Sum)

	if len(dp.ExtendedStatistics) == 0 {
		return
	}
	extended := make([]*string, 0, len(dp.ExtendedStatistics))
	for k := range dp.ExtendedStatistics {
		extended = append(extended, aws.String(k))
	}

	getOut, err = client.GetMetricStatistics(&cloudwatch.GetMetricStatisticsInput{
		MetricName:         dt.MetricName,
		Dimensions:         dt.Dimensions,
		StartTime:          dt.Timestamp,
		EndTime:            aws.Time(dt.Timestamp.Add(time.Duration(*dt.StorageResolution) * time.Second)),
		Period:             dt.StorageResolution,
		Namespace:          &testNamespace,
		ExtendedStatistics: extended,
	})
	require.NoError(t, err)
	require.Len(t, getOut.Datapoints, 1)
	require.Len(t, getOut.Datapoints[0].ExtendedStatistics, len(dp.ExtendedStatistics))
	for k, v := range getOut.Datapoints[0].ExtendedStatistics {
		require.InDelta(t, *dp.ExtendedStatistics[k], *v, .01)
	}
}
