package kinesis

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/aws/amazon-kinesis-firehose-for-fluent-bit/plugins"
	"github.com/aws/amazon-kinesis-streams-for-fluent-bit/aggregate"
	"github.com/aws/amazon-kinesis-streams-for-fluent-bit/kinesis/mock_kinesis"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	fluentbit "github.com/fluent/fluent-bit-go/output"
	"github.com/golang/mock/gomock"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

const concurrencyRetryLimit = 4

// newMockOutputPlugin creates an mock OutputPlugin object
func newMockOutputPlugin(client *mock_kinesis.MockPutRecordsClient, isAggregate bool) (*OutputPlugin, error) {

	timer, _ := plugins.NewTimeout(func(d time.Duration) {
		logrus.Errorf("[kinesis] timeout threshold reached: Failed to send logs for %v", d)
		logrus.Errorf("[kinesis] Quitting Fluent Bit")
		os.Exit(1)
	})

	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, 8)
	random := &random{
		seededRandom: seededRand,
		buffer:       b,
	}

	var aggregator *aggregate.Aggregator
	if isAggregate {
		aggregator = aggregate.NewAggregator()
	}

	return &OutputPlugin{
		stream:       "stream",
		client:       client,
		dataKeys:     "",
		partitionKey: "",
		timer:        timer,
		PluginID:     0,
		random:       random,
		concurrencyRetryLimit: concurrencyRetryLimit,
		isAggregate:  isAggregate,
		aggregator:   aggregator,
	}, nil
}

// Test cases for TestStringOrByteArray
var testCases = []struct {
	input  interface{}
	output string
}{
	{"testString", "testString"},
	{35344, ""},
	{[]byte{'b', 'y', 't', 'e'}, "byte"},
	{nil, ""},
}

func TestStringOrByteArray(t *testing.T) {
	for _, testCase := range testCases {
		result := stringOrByteArray(testCase.input)
		if result != testCase.output {
			t.Errorf("[Test Failed] Expeced: %s, Returned: %s", testCase.output, result)
		}
	}
}

func TestAddRecord(t *testing.T) {
	records := make([]*kinesis.PutRecordsRequestEntry, 0, 500)

	record := map[interface{}]interface{}{
		"testkey": []byte("test value"),
	}

	outputPlugin, _ := newMockOutputPlugin(nil, false)

	timeStamp := time.Now()
	retCode := outputPlugin.AddRecord(&records, record, &timeStamp)
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to be FLB_OK")
	assert.Len(t, records, 1, "Expected output to contain 1 record")
}

func TestAddRecordAndFlush(t *testing.T) {
	records := make([]*kinesis.PutRecordsRequestEntry, 0, 500)

	record := map[interface{}]interface{}{
		"testkey": []byte("test value"),
	}

	ctrl := gomock.NewController(t)
	mockKinesis := mock_kinesis.NewMockPutRecordsClient(ctrl)

	mockKinesis.EXPECT().PutRecords(gomock.Any()).Return(&kinesis.PutRecordsOutput{
		FailedRecordCount: aws.Int64(0),
	}, nil)

	outputPlugin, _ := newMockOutputPlugin(mockKinesis, false)

	timeStamp := time.Now()
	retCode := outputPlugin.AddRecord(&records, record, &timeStamp)
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to be FLB_OK")

	retCode = outputPlugin.Flush(&records)
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to be FLB_OK")
}

func TestAddRecordAndFlushAggregate(t *testing.T) {
	records := make([]*kinesis.PutRecordsRequestEntry, 0, 500)

	record := map[interface{}]interface{}{
		"testkey": []byte("test value"),
	}

	ctrl := gomock.NewController(t)
	mockKinesis := mock_kinesis.NewMockPutRecordsClient(ctrl)

	mockKinesis.EXPECT().PutRecords(gomock.Any()).Return(&kinesis.PutRecordsOutput{
		FailedRecordCount: aws.Int64(0),
	}, nil)

	outputPlugin, _ := newMockOutputPlugin(mockKinesis, true)

	checkIsAggregate := outputPlugin.IsAggregate()
	assert.Equal(t, checkIsAggregate, true, "Expected IsAggregate() to return true")

	timeStamp := time.Now()
	retCode := outputPlugin.AddRecord(&records, record, &timeStamp)
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected AddRecord return code to be FLB_OK")

	retCode = outputPlugin.FlushAggregatedRecords(&records)
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected FlushAggregatedRecords return code to be FLB_OK")

	retCode = outputPlugin.Flush(&records)
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected Flush return code to be FLB_OK")
}

func TestAddRecordWithConcurrency(t *testing.T) {
	records := make([]*kinesis.PutRecordsRequestEntry, 0, 500)

	record := map[interface{}]interface{}{
		"testkey": []byte("test value"),
	}

	ctrl := gomock.NewController(t)
	mockKinesis := mock_kinesis.NewMockPutRecordsClient(ctrl)

	mockKinesis.EXPECT().PutRecords(gomock.Any()).Return(&kinesis.PutRecordsOutput{
		FailedRecordCount: aws.Int64(0),
	}, nil)

	outputPlugin, _ := newMockOutputPlugin(mockKinesis, false)
	// Enable concurrency
	outputPlugin.Concurrency = 2

	timeStamp := time.Now()
	retCode := outputPlugin.AddRecord(&records, record, &timeStamp)
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected AddRecord return code to be FLB_OK")

	retCode = outputPlugin.FlushConcurrent(len(records), records)
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected FlushConcurrent return code to be FLB_OK")
}
