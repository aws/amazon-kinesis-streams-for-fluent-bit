package aggregate

import (
	"testing"

	"github.com/aws/amazon-kinesis-streams-for-fluent-bit/util"
	"github.com/stretchr/testify/assert"
)

const concurrencyRetryLimit = 4

func TestAddRecordCalculatesCorrectSize(t *testing.T) {
	generator := util.NewRandomStringGenerator(18)
	aggregator := NewAggregator(generator, &Config{})

	_, err := aggregator.AddRecord("", false, []byte("test value"))
	assert.Equal(t, nil, err, "Expected aggregator not to return error")
	assert.Equal(t, 36, aggregator.aggSize, "Expected aggregator to compute correct size")

	_, err = aggregator.AddRecord("test partition key 2", true, []byte("test value 2"))
	assert.Equal(t, nil, err, "Expected aggregator not to return error")
	assert.Equal(t, 76, aggregator.aggSize, "Expected aggregator to compute correct size")
}

func TestAddRecordDoesNotAddNewRandomPartitionKey(t *testing.T) {
	generator := util.NewRandomStringGenerator(18)
	aggregator := NewAggregator(generator, &Config{})

	_, err := aggregator.AddRecord("", false, []byte("test value"))
	assert.Equal(t, nil, err, "Expected aggregator not to return error")
	assert.Equal(t, 36, aggregator.aggSize, "Expected aggregator to compute correct size")

	_, err = aggregator.AddRecord("", false, []byte("test value 2"))
	assert.Equal(t, nil, err, "Expected aggregator not to return error")
	assert.Equal(t, 1, len(aggregator.partitionKeys), "Expected aggregator to reuse partitionKey value")
}
