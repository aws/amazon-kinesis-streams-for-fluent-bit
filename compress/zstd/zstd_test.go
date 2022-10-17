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

func TestBufferReuse(t *testing.T) {
	compress1Want := []byte{40, 181, 47, 253, 4, 0, 25, 0, 0, 1, 2, 3, 165, 229, 78, 12}
	compress2Want := []byte{40, 181, 47, 253, 4, 0, 41, 0, 0, 4, 5, 6, 7, 8, 41, 208, 126, 18}
	compress3Want := []byte{40, 181, 47, 253, 4, 0, 17, 0, 0, 9, 10, 235, 159, 206, 197}

	z, err := New(1)
	if err != nil {
		t.Errorf("New() error = %v", err)
	}

	compressed1, err := z.Compress([]byte{1, 2, 3})
	if err != nil {
		t.Errorf("New() error = %v", err)
	}
	compressed2, err := z.Compress([]byte{4, 5, 6, 7, 8})
	if err != nil {
		t.Errorf("New() error = %v", err)
	}
	compressed3, err := z.Compress([]byte{9, 10})
	if err != nil {
		t.Errorf("New() error = %v", err)
	}

	if !bytes.Equal(compressed1, compress1Want) {
		t.Errorf("compressed1 got %v, want %v ", compressed1, compress1Want)
	}
	if !bytes.Equal(compressed2, compress2Want) {
		t.Errorf("compressed2 got %v, want %v ", compressed2, compress2Want)
	}
	if !bytes.Equal(compressed3, compress3Want) {
		t.Errorf("compressed3 got %v, want %v ", compressed3, compress3Want)
	}
}
