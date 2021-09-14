package kinesis

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/aws/amazon-kinesis-firehose-for-fluent-bit/plugins"
	"github.com/aws/amazon-kinesis-streams-for-fluent-bit/aggregate"
	"github.com/aws/amazon-kinesis-streams-for-fluent-bit/kinesis/mock_kinesis"
	"github.com/aws/amazon-kinesis-streams-for-fluent-bit/util"
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

	stringGen := util.NewRandomStringGenerator(8)

	var aggregator *aggregate.Aggregator
	if isAggregate {
		aggregator = aggregate.NewAggregator(stringGen)
	}

	return &OutputPlugin{
		stream:                "stream",
		client:                client,
		dataKeys:              "",
		partitionKey:          "",
		timer:                 timer,
		PluginID:              0,
		stringGen:             stringGen,
		concurrencyRetryLimit: concurrencyRetryLimit,
		isAggregate:           isAggregate,
		aggregator:            aggregator,
		replaceDots:           "-",
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

func TestTruncateLargeLogEvent(t *testing.T) {
	records := make([]*kinesis.PutRecordsRequestEntry, 0, 500)

	record := map[interface{}]interface{}{
		"somekey": make([]byte, 1024*1024),
	}

	outputPlugin, _ := newMockOutputPlugin(nil, false)

	timeStamp := time.Now()
	retCode := outputPlugin.AddRecord(&records, record, &timeStamp)
	actualData, err := outputPlugin.processRecord(record, len("testKey"))
	if err != nil {
		logrus.Errorf("[kinesis %d] %v\n", outputPlugin.PluginID, err)
	}

	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to be FLB_OK")
	assert.Len(t, records, 1, "Expected output to contain 1 record")
	assert.Len(t, actualData, 1024*1024-len("testKey"), "Expected length is less than 1MB")
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

var compressors = map[string]func([]byte) ([]byte, error){
	"zlib": zlibCompress,
	"gzip": gzipCompress,
}

func TestCompression(t *testing.T) {

	testData := []byte("Test Data: This is test data for compression.  This data is needs to have with some repetitive values, so compression is effective.")

	for z, f := range compressors {
		compressedBuf, err := f(testData)
		assert.Equalf(t, err, nil, "Expected successful %s compression of data", z)
		assert.Lessf(t, len(compressedBuf), len(testData), "%s compressed data buffer should contain fewer bytes", z)
	}
}

func TestCompressionEmpty(t *testing.T) {

	for z, f := range compressors {
		_, err := f(nil)
		assert.NotEqualf(t, err, nil, "%s compressing 'nil' data should return an error", z)
	}
}

func TestDotReplace(t *testing.T) {
	records := make([]*kinesis.PutRecordsRequestEntry, 0, 500)
	record := map[interface{}]interface{}{
		"message.key": map[interface{}]interface{}{
			"messagevalue":      []byte("some.message"),
			"message.value/one": []byte("some message"),
			"message.value/two": []byte("some message"),
		},
		"kubernetes": map[interface{}]interface{}{
			"app":                    []byte("test app label"),
			"app.kubernetes.io/name": []byte("test key with dots"),
		},
	}

	outputPlugin, _ := newMockOutputPlugin(nil, false)

	timeStamp := time.Now()
	retCode := outputPlugin.AddRecord(&records, record, &timeStamp)
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to be FLB_OK")
	assert.Len(t, records, 1, "Expected output to contain 1 record")

	data := records[0].Data

	var log map[string]map[string]interface{}
	json.Unmarshal(data, &log)

	assert.Equal(t, "test app label", log["kubernetes"]["app"])
	assert.Equal(t, "test key with dots", log["kubernetes"]["app-kubernetes-io/name"])
	assert.Equal(t, "some.message", log["message-key"]["messagevalue"])
	assert.Equal(t, "some message", log["message-key"]["message-value/one"])
	assert.Equal(t, "some message", log["message-key"]["message-value/two"])
}

func TestGetPartitionKey(t *testing.T) {
	record := map[interface{}]interface{}{
		"testKey": []byte("test value with no nested keys"),
		"testKeyWithOneNestedKey": map[interface{}]interface{}{
			"nestedKey": []byte("test value with one nested key"),
		},
		"testKeyWithNestedKeys": map[interface{}]interface{}{
			"outerKey": map[interface{}]interface{}{
				"innerKey": []byte("test value with inner key"),
			},
		},
	}

	//test getPartitionKey() with single partition key
	outputPlugin, _ := newMockOutputPlugin(nil, false)
	outputPlugin.partitionKey = "testKey"
	value, hasValue := outputPlugin.getPartitionKey(record)
	assert.Equal(t, true, hasValue, "Should find value")
	assert.Equal(t, value, "test value with no nested keys")

	//test getPartitionKey() with nested partition key
	outputPlugin.partitionKey = "testKeyWithOneNestedKey->nestedKey"
	value, hasValue = outputPlugin.getPartitionKey(record)
	assert.Equal(t, true, hasValue, "Should find value")
	assert.Equal(t, value, "test value with one nested key")

	outputPlugin.partitionKey = "testKeyWithNestedKeys->outerKey->innerKey"
	value, hasValue = outputPlugin.getPartitionKey(record)
	assert.Equal(t, true, hasValue, "Should find value")
	assert.Equal(t, value, "test value with inner key")

	//test getPartitionKey() with partition key not found
	outputPlugin.partitionKey = "some key"
	value, hasValue = outputPlugin.getPartitionKey(record)
	assert.Equal(t, false, hasValue, "Should not find value")
	assert.Len(t, value, 0, "This should be an empty string")

	outputPlugin.partitionKey = "testKeyWithOneNestedKey"
	value, hasValue = outputPlugin.getPartitionKey(record)
	assert.Equal(t, false, hasValue, "Should not find value")
	assert.Len(t, value, 0, "This should be an empty string")

	outputPlugin.partitionKey = "testKeyWithOneNestedKey->someKey"
	value, hasValue = outputPlugin.getPartitionKey(record)
	assert.Equal(t, false, hasValue, "Should not find value")
	assert.Len(t, value, 0, "This should be an empty string")
}
