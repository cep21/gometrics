package metrics

import (
	"reflect"
	"testing"
)

func TestTimeSeriesIdentifier_UID(t *testing.T) {
	type fields struct {
		MetricName string
		Dimensions map[string]string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "UID of empty",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := &TimeSeriesIdentifier{
				MetricName: tt.fields.MetricName,
				Dimensions: tt.fields.Dimensions,
			}
			if got := ts.UID(); got != tt.want {
				t.Errorf("TimeSeriesIdentifier.UID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimeSeriesIdentifier_String(t *testing.T) {
	type fields struct {
		MetricName string
		Dimensions map[string]string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "string of nothing",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := &TimeSeriesIdentifier{
				MetricName: tt.fields.MetricName,
				Dimensions: tt.fields.Dimensions,
			}
			if got := ts.String(); got != tt.want {
				t.Errorf("TimeSeriesIdentifier.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_mustWrite(t *testing.T) {
	type args struct {
		in0 int
		err error
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "must won't panic for no error",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mustWrite(tt.args.in0, tt.args.err)
		})
	}
}

func Test_mustNotErr(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "must won't panic for no error",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mustNotErr(tt.args.err)
		})
	}
}

func Test_uniqueCopy(t *testing.T) {
	type args struct {
		t TimeSeriesIdentifier
	}
	tests := []struct {
		name string
		args args
		want TimeSeriesIdentifier
	}{
		{
			name: "unique of empty is empty",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := uniqueCopy(tt.args.t); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("uniqueCopy() = %v, want %v", got, tt.want)
			}
		})
	}
}
