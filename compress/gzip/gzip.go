// Package gzip is an implementation of Compression interface using gzip format.
package gzip

import (
	"bytes"
	"compress/gzip"
	"sync"
)

// GZip implements Compression interface using gzip format.
type GZip struct {
	mutex  sync.Mutex
	buffer bytes.Buffer
	writer *gzip.Writer
	level  int
}

// New creates a new GZip instance.
func New(level int) (*GZip, error) {
	z := GZip{
		level: level,
	}

	var err error
	z.writer, err = gzip.NewWriterLevel(&z.buffer, level)
	if err != nil {
		return nil, err
	}

	return &z, nil
}

// Compress will compress data using gzip format.
// In case of any errors, it will return raw data without compression.
func (z *GZip) Compress(data []byte) ([]byte, error) {
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
		tmpWriter, err = gzip.NewWriterLevel(tmpBuffer, z.level)
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
