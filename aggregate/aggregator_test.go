package aggregate

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const concurrencyRetryLimit = 4

func TestAddRecordCalculatesCorrectSize(t *testing.T) {
	aggregator := NewAggregator()

	_, err := aggregator.AddRecord("test partition key", []byte("test value"))
	assert.Equal(t, nil, err, "Expected aggregator not to return error")
	assert.Equal(t, 36, aggregator.aggSize, "Expected aggregator to compute correct size")

	_, err = aggregator.AddRecord("test partition key 2", []byte("test value 2"))
	assert.Equal(t, nil, err, "Expected aggregator not to return error")
	assert.Equal(t, 76, aggregator.aggSize, "Expected aggregator to compute correct size")
}
