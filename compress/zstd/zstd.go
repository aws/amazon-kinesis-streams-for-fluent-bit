// Package zstd is an implementation of Compression interface using zstd format.
package zstd

import (
	"bytes"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// ZSTD implements Compression interface using gzip format.
type ZSTD struct {
	mutex      sync.Mutex
	buffer     bytes.Buffer
	writer     *zstd.Encoder
	writerOpts []zstd.EOption
}

// New creates a new ZSTD instance.
func New(level int) (*ZSTD, error) {
	z := ZSTD{
		writerOpts: []zstd.EOption{
			zstd.WithEncoderConcurrency(1),
			zstd.WithEncoderLevel(zstd.EncoderLevel(level)),
			zstd.WithZeroFrames(true),
		},
	}

	var err error
	z.writer, err = zstd.NewWriter(&z.buffer, z.writerOpts...)
	if err != nil {
		return nil, err
	}

	return &z, nil
}

// Compress will compress data using zstd format.
// In case of any errors, it will return raw data without compression.
func (z *ZSTD) Compress(data []byte) ([]byte, error) {
	var err error

	tmpBuffer := &z.buffer
	tmpWriter := z.writer
	if z.mutex.TryLock() {
		defer z.mutex.Unlock()

		tmpBuffer.Reset()
		tmpWriter.Reset(&z.buffer)
	} else {
		// This should not happen since we only run a single thread.
		// Good to have this logic here if we want to support concurrent processing.
		tmpBuffer = new(bytes.Buffer)
		tmpWriter, err = zstd.NewWriter(tmpBuffer, z.writerOpts...)
		if err != nil {
			return data, err
		}
	}

	_, err = tmpWriter.Write(data)
	if err != nil {
		return data, err
	}
	err = tmpWriter.Close()
	if err != nil {
		return data, err
	}

	return tmpBuffer.Bytes(), nil
}
