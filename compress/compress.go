// Package compress is responsible for compression data.
package compress

import (
	"github.com/canva/amazon-kinesis-streams-for-fluent-bit/compress/gzip"
	"github.com/canva/amazon-kinesis-streams-for-fluent-bit/compress/noop"
	"github.com/canva/amazon-kinesis-streams-for-fluent-bit/compress/zstd"
)

// Format is the format of compression that happens.
type Format string

// Format is the format of compression that happens.
const (
	FormatNoop = Format("noop")
	FormatGZip = Format("gzip")
	FormatZSTD = Format("zstd")
)

// Config holds configurations need for creating a new Compress instance.
type Config struct {
	Format Format
	Level  int
}

// Compression will compress any given array of bytes.
type Compression interface {
	Compress([]byte) ([]byte, error)
}

// New creates an instance of Compression.
// Based on the Format value, it creates corresponding instance.
func New(conf *Config) (Compression, error) {
	switch conf.Format {
	default:
		return noop.New(), nil
	case FormatGZip:
		return gzip.New(conf.Level)
	case FormatZSTD:
		return zstd.New(conf.Level)
	}
}

// defaultCompression holds global compression instance.
var defaultCompression Compression

// Init will initialise global compression instance.
func Init(conf *Config) error {
	var err error

	defaultCompression, err = New(conf)

	return err
}

// Compress will compress any given array of bytes.
func Compress(data []byte) ([]byte, error) {
	return defaultCompression.Compress(data)
}
