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
