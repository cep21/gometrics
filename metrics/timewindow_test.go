package metrics

import (
	"reflect"
	"testing"
	"time"
)

func TestTimeWindow_End(t *testing.T) {
	start := time.Now()
	type fields struct {
		Start    time.Time
		Duration time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		want   time.Time
	}{
		{
			name: "add nothing",
			fields: fields{
				Start: start,
			},
			want: start,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tw := TimeWindow{
				Start:    tt.fields.Start,
				Duration: tt.fields.Duration,
			}
			if got := tw.End(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TimeWindow.End() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimeWindow_Union(t *testing.T) {
	start := time.Now()
	type fields struct {
		Start    time.Time
		Duration time.Duration
	}
	type args struct {
		w TimeWindow
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   TimeWindow
	}{
		{
			name: "union with self",
			fields: fields{
				Start: start,
			},
			args: args{
				w: TimeWindow{
					Start: start,
				},
			},
			want: TimeWindow{
				Start: start,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tw := TimeWindow{
				Start:    tt.fields.Start,
				Duration: tt.fields.Duration,
			}
			if got := tw.Union(tt.args.w); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TimeWindow.Union() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimeWindow_Middle(t *testing.T) {
	start := time.Now()
	type fields struct {
		Start    time.Time
		Duration time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		want   time.Time
	}{
		{
			name: "middle of nothing",
			fields: fields{
				Start: start,
			},
			want: start,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tw := TimeWindow{
				Start:    tt.fields.Start,
				Duration: tt.fields.Duration,
			}
			if got := tw.Middle(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TimeWindow.Middle() = %v, want %v", got, tt.want)
			}
		})
	}
}
