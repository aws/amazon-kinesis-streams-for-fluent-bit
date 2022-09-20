package compress

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/canva/amazon-kinesis-streams-for-fluent-bit/compress/gzip"
	"github.com/canva/amazon-kinesis-streams-for-fluent-bit/compress/noop"
	"github.com/canva/amazon-kinesis-streams-for-fluent-bit/compress/zstd"
)

func IsType[T Compression](c Compression) bool {
	_, ok := c.(T)
	return ok
}

func TestNew(t *testing.T) {
	type args struct {
		conf *Config
	}
	tests := []struct {
		name          string
		args          args
		typeCheckFunc func(Compression) bool
		wantErr       bool
	}{
		{
			"noop",
			args{
				&Config{
					Format: "noop",
				},
			},
			IsType[noop.Noop],
			false,
		},
		{
			"gzip",
			args{
				&Config{
					Format: "gzip",
					Level:  1,
				},
			},
			IsType[*gzip.GZip],
			false,
		},
		{
			"zstd",
			args{
				&Config{
					Format: "zstd",
					Level:  1,
				},
			},
			IsType[*zstd.ZSTD],
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(tt.args.conf)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.typeCheckFunc(got) {
				t.Errorf("wrong New().(type) = %T", got)
			}
		})
	}
}

func TestInit(t *testing.T) {
	defaultCompression = nil
	Init(&Config{
		Format: FormatNoop,
	})
	if defaultCompression == nil {
		t.Error("Init(); defaultCompression = nil")
	}
}

func TestCompress(t *testing.T) {
	defaultCompression = noop.Noop{}

	want := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	got, err := Compress([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	if err != nil {
		t.Errorf("Noop.Compress() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Noop.Compress() = %v, want %v", got, want)
	}
}

func BenchmarkCompressions(b *testing.B) {
	cbs := generateCompressionBenchmarks(b)
	for _, cb := range cbs {
		b.Run(cb.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				cb.compression.Compress(cb.data)
			}
		})
	}
}

func BenchmarkCompressionsAlwaysNew(b *testing.B) {
	var c Compression
	cbs := generateCompressionAlwaysNewBenchmarks(b)
	for _, cb := range cbs {
		b.Run(cb.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				c, _ = New(cb.config)
				c.Compress(cb.data)
			}
		})
	}
}

type compressionBenchmark struct {
	name        string
	compression Compression
	data        []byte
}

func generateCompressionBenchmarks(b *testing.B) []*compressionBenchmark {
	payloads := generateCompressionPayloads(b)
	type configTemplate struct {
		format Format
		levels []int
	}
	configTemplates := []*configTemplate{
		{
			FormatNoop,
			[]int{0},
		},
		{
			FormatGZip,
			[]int{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			FormatZSTD,
			[]int{1, 2, 3, 4},
		},
	}

	var totalLen int

	for _, confTemp := range configTemplates {
		totalLen += len(confTemp.levels)
	}
	totalLen *= len(payloads)

	cbs := make([]*compressionBenchmark, totalLen)

	var i int
	for _, payload := range payloads {
		for _, confTemp := range configTemplates {
			for _, l := range confTemp.levels {
				conf := &Config{
					Format: confTemp.format,
					Level:  l,
				}
				c, err := New(conf)
				if err != nil {
					b.Fatalf("New() failed for %+v: %v", conf, err)
				}

				// Warm up
				c.Compress(payload)

				cbs[i] = &compressionBenchmark{
					fmt.Sprintf("%s_level_%d_len_%d", conf.Format, conf.Level, len(payload)),
					c,
					payload,
				}
				i++
			}
		}
	}

	return cbs
}

type compressionAlwaysNewBenchmark struct {
	name   string
	config *Config
	data   []byte
}

func generateCompressionAlwaysNewBenchmarks(b *testing.B) []*compressionAlwaysNewBenchmark {
	payloads := generateCompressionPayloads(b)
	type configTemplate struct {
		format Format
		levels []int
	}
	configTemplates := []*configTemplate{
		{
			FormatNoop,
			[]int{0},
		},
		{
			FormatGZip,
			[]int{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			FormatZSTD,
			[]int{1, 2, 3, 4},
		},
	}

	var totalLen int

	for _, confTemp := range configTemplates {
		totalLen += len(confTemp.levels)
	}
	totalLen *= len(payloads)

	cbs := make([]*compressionAlwaysNewBenchmark, totalLen)

	var i int
	for _, payload := range payloads {
		for _, confTemp := range configTemplates {
			for _, l := range confTemp.levels {
				cbs[i] = &compressionAlwaysNewBenchmark{
					fmt.Sprintf("%s_level_%d_len_%d", confTemp.format, l, len(payload)),
					&Config{
						Format: confTemp.format,
						Level:  l,
					},
					payload,
				}
				i++
			}
		}
	}

	return cbs
}

func generateCompressionPayloads(b *testing.B) [][]byte {
	var sizes = [...]int{100, 1_000, 10_000, 150_000}
	filePath := filepath.Clean(filepath.Join("testdata", "linux_2k.log"))
	content, err := os.ReadFile(filePath)
	if err != nil {
		b.Fatalf("unable to read the file %s: %v", filePath, err)
	}

	payloads := make([][]byte, len(sizes))
	for i, size := range sizes {
		payloads[i] = content[:size]
	}

	return payloads
}
