package gzip

import (
	"bytes"
	"compress/gzip"
	"reflect"
	"testing"
)

func TestNew(t *testing.T) {
	z, err := New(5)

	if err != nil {
		t.Errorf("New() error = %v", err)
	}
	if z == nil {
		t.Errorf("New() = nil")
	}

	if z.level != 5 {
		t.Errorf("New().level = %v, want %v", z.level, 5)
	}

	if z.writer == nil {
		t.Errorf("New().writer = nil")
	}
}

func TestGZip_Compress(t *testing.T) {
	buf := bytes.Buffer{}
	type args struct {
		data []byte
	}
	tests := []struct {
		name    string
		z       *GZip
		args    args
		lock    bool
		want    []byte
		wantErr bool
	}{
		{
			"not_locked",
			&GZip{
				writer: gzip.NewWriter(&buf),
			},
			args{[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
			false,
			[]byte{31, 139, 8, 0, 0, 0, 0, 0, 0, 255, 98, 96, 100, 98, 102, 97, 101, 99, 231, 224, 4, 4, 0, 0, 255, 255, 70, 215, 108, 69, 10, 0, 0, 0},
			false,
		},
		{
			"locked",
			&GZip{
				writer: gzip.NewWriter(&buf),
				level:  gzip.DefaultCompression,
			},
			args{[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
			true,
			[]byte{31, 139, 8, 0, 0, 0, 0, 0, 0, 255, 98, 96, 100, 98, 102, 97, 101, 99, 231, 224, 4, 4, 0, 0, 255, 255, 70, 215, 108, 69, 10, 0, 0, 0},
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
				t.Errorf("GZip.Compress() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GZip.Compress() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBufferReuse(t *testing.T) {
	compress1Want := []byte{31, 139, 8, 0, 0, 0, 0, 0, 4, 255, 0, 3, 0, 252, 255, 1, 2, 3, 1, 0, 0, 255, 255, 29, 128, 188, 85, 3, 0, 0, 0}
	compress2Want := []byte{31, 139, 8, 0, 0, 0, 0, 0, 4, 255, 0, 5, 0, 250, 255, 4, 5, 6, 7, 8, 1, 0, 0, 255, 255, 168, 195, 107, 65, 5, 0, 0, 0}
	compress3Want := []byte{31, 139, 8, 0, 0, 0, 0, 0, 4, 255, 0, 2, 0, 253, 255, 9, 10, 1, 0, 0, 255, 255, 168, 64, 206, 112, 2, 0, 0, 0}

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
