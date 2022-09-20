// Package noop is an implementation of Compression interface that doesn't compress data.
package noop

// Noop is an implementation of Compression interface that doesn't compress data.
type Noop struct{}

// New creates a new instance of Noop.
func New() Noop {
	return Noop{}
}

// Compress will return given data without any compression.
func (z Noop) Compress(data []byte) ([]byte, error) {
	return data, nil
}
