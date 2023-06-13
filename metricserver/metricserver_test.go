package metricserver

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_New(t *testing.T) {
	t.Run("Valid Server", func(t *testing.T) {
		assert := assert.New(t)

		actual, err := New()
		assert.NoError(err)
		assert.NotNil(actual)

		assert.Equal(DEFAULT_PORT, actual.port)
		assert.NotNil(actual.meterProvider)
	})

	t.Run("With Port", func(t *testing.T) {
		assert := assert.New(t)

		expected := 10101

		server, err := New(WithPort(expected))

		assert.NoError(err)
		assert.NotNil(server)
		assert.Equal(expected, server.port)
	})
}
