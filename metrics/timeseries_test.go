package metrics

import (
	"reflect"
	"testing"
)

func TestTimeSeriesMetadata_Value(t *testing.T) {
	type args struct {
		key interface{}
	}
	tests := []struct {
		name string
		t    TimeSeriesMetadata
		args args
		want interface{}
	}{
		{
			name: "on nil",
			t:    nil,
			args: args{
				"hello",
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.t.Value(tt.args.key); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TimeSeriesMetadata.Value() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTimeSeriesMetadata_WithValue(t *testing.T) {
	type args struct {
		key   interface{}
		value interface{}
	}
	tests := []struct {
		name string
		t    TimeSeriesMetadata
		args args
		want TimeSeriesMetadata
	}{
		{
			name: "set hello",
			t:    nil,
			args: args{
				key:   "name",
				value: "hello",
			},
			want: TimeSeriesMetadata(map[interface{}]interface{}{
				"name": "hello",
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.t.WithValue(tt.args.key, tt.args.value); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("TimeSeriesMetadata.WithValue() = %v, want %v", got, tt.want)
			}
		})
	}
}
