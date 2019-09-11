package metrics

import (
	"reflect"
	"testing"
	"time"
)

func TestBucket_Middle(t *testing.T) {
	type fields struct {
		Count int32
		Start float64
		End   float64
	}
	tests := []struct {
		name   string
		fields fields
		want   float64
	}{
		{
			name: "no range",
			fields: fields{
				Start: 10,
				End:   10,
			},
			want: 10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Bucket{
				Count: tt.fields.Count,
				Start: tt.fields.Start,
				End:   tt.fields.End,
			}
			if got := b.Middle(); got != tt.want {
				t.Errorf("Bucket.Middle() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_bucketMerge(t *testing.T) {
	type args struct {
		a []Bucket
		b []Bucket
	}
	tests := []struct {
		name string
		args args
		want []Bucket
	}{
		{
			name: "nil merges",
			args: args{
				a: nil,
				b: nil,
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := bucketMerge(tt.args.a, tt.args.b); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("bucketMerge() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_appendToRange(t *testing.T) {
	type args struct {
		a   []Bucket
		ret map[bucketRange]int32
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "no range",
			args: args{
				a:   nil,
				ret: nil,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			appendToRange(tt.args.a, tt.args.ret)
		})
	}
}

func Test_minTime(t *testing.T) {
	start := time.Now()
	type args struct {
		a time.Time
		b time.Time
	}
	tests := []struct {
		name string
		args args
		want time.Time
	}{
		{
			name: "first smallest",
			args: args{
				a: start,
				b: start.Add(time.Hour),
			},
			want: start,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := minTime(tt.args.a, tt.args.b); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("minTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_maxTime(t *testing.T) {
	start := time.Now()
	type args struct {
		a time.Time
		b time.Time
	}
	tests := []struct {
		name string
		args args
		want time.Time
	}{
		{
			name: "first biggest",
			args: args{
				a: start,
				b: start.Add(-time.Hour),
			},
			want: start,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := maxTime(tt.args.a, tt.args.b); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("maxTime() = %v, want %v", got, tt.want)
			}
		})
	}
}
