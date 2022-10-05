package zstd

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestNew(t *testing.T) {
	z, err := New(1)

	if err != nil {
		t.Errorf("New() error = %v", err)
	}
	if z == nil {
		t.Errorf("New() = nil")
	}

	if z.writer == nil {
		t.Errorf("New().writer = nil")
	}
}

func TestZSTD_Compress(t *testing.T) {
	buf := bytes.Buffer{}
	writer, _ := zstd.NewWriter(&buf, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(1))
	type args struct {
		data []byte
	}
	tests := []struct {
		name    string
		z       *ZSTD
		args    args
		lock    bool
		want    []byte
		wantErr bool
	}{
		{
			"not_locked",
			&ZSTD{
				writer: writer,
				writerOpts: []zstd.EOption{
					zstd.WithEncoderConcurrency(1),
					zstd.WithEncoderLevel(1),
					zstd.WithZeroFrames(true),
				},
			},
			args{[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
			false,
			[]byte{40, 181, 47, 253, 4, 0, 81, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 135, 25, 84, 50},
			false,
		},
		{
			"locked",
			&ZSTD{
				writer: writer,
				writerOpts: []zstd.EOption{
					zstd.WithEncoderConcurrency(1),
					zstd.WithEncoderLevel(1),
					zstd.WithZeroFrames(true),
				},
			},
			args{[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
			true,
			[]byte{40, 181, 47, 253, 4, 0, 81, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 135, 25, 84, 50},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.lock {
				tt.z.mutex.Lock()
			}
			got, err := tt.z.Compress(tt.args.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("ZSTD.Compress() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ZSTD.Compress() = %v, want %v", got, tt.want)
			}
		})
	}
}
