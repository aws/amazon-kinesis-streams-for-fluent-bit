// Copyright 2019-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may
// not use this file except in compliance with the License. A copy of the
// License is located at
//
//  http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.

//go:generate mockgen -destination mock_kinesis/mock_kinesis.go -copyright_file=../COPYRIGHT github.com/aws/amazon-kinesis-streams-for-fluent-bit/kinesis PutRecordsClient

// Package kinesis contains the OutputPlugin which sends log records to Kinesis Stream
package kinesis

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"sync/atomic"
	"time"

	"github.com/aws/amazon-kinesis-firehose-for-fluent-bit/plugins"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/fluent/fluent-bit-go/output"
	fluentbit "github.com/fluent/fluent-bit-go/output"
	jsoniter "github.com/json-iterator/go"
	"github.com/lestrrat-go/strftime"
	"github.com/sirupsen/logrus"
)

const (
	partitionKeyCharset = "abcdefghijklmnopqrstuvwxyz" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

const (
	// Kinesis API Limit https://docs.aws.amazon.com/sdk-for-go/api/service/kinesis/#Kinesis.PutRecords
	maximumRecordsPerPut      = 500
	maximumPutRecordBatchSize = 1024 * 1024 * 5 // 5 MB
	maximumRecordSize         = 1024 * 1024     // 1 MB

	partitionKeyMaxLength = 256
)

const (
	// We use strftime format specifiers because this will one day be re-written in C
	defaultTimeFmt = "%Y-%m-%dT%H:%M:%S"
)

// PutRecordsClient contains the kinesis PutRecords method call
type PutRecordsClient interface {
	PutRecords(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error)
}

type random struct {
	seededRandom *rand.Rand
	buffer       []byte
}

// OutputPlugin sends log records to kinesis
type OutputPlugin struct {
	// The name of the stream that you want log records sent to
	stream string
	// If specified, only these keys and values will be send as the log record
	dataKeys string
	// If specified, the value of that data key will be used as the partition key.
	// Otherwise a random string will be used.
	// Partition key decides in which shard of your stream the data belongs to
	partitionKey string
	// Decides whether to append a newline after each data record
	appendNewline         bool
	timeKey               string
	fmtStrftime           *strftime.Strftime
	client                PutRecordsClient
	timer                 *plugins.Timeout
	PluginID              int
	random                *random
	Concurrency           int
	concurrencyRetryLimit int
	// Concurrency is the limit, goroutineCount represents the running goroutines
	goroutineCount int32
	// Used to implement backoff for concurrent flushes
	concurrentRetries uint32
}

// NewOutputPlugin creates an OutputPlugin object
func NewOutputPlugin(region, stream, dataKeys, partitionKey, roleARN, endpoint, timeKey, timeFmt string, concurrency, retryLimit int, appendNewline bool, pluginID int) (*OutputPlugin, error) {
	client, err := newPutRecordsClient(roleARN, region, endpoint)
	if err != nil {
		return nil, err
	}

	timer, err := plugins.NewTimeout(func(d time.Duration) {
		logrus.Errorf("[kinesis %d] timeout threshold reached: Failed to send logs for %s\n", pluginID, d.String())
		logrus.Errorf("[kinesis %d] Quitting Fluent Bit", pluginID)
		os.Exit(1)
	})

	if err != nil {
		return nil, err
	}

	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	random := &random{
		seededRandom: seededRand,
		buffer:       make([]byte, 8),
	}

	var timeFormatter *strftime.Strftime
	if timeKey != "" {
		if timeFmt == "" {
			timeFmt = defaultTimeFmt
		}
		timeFormatter, err = strftime.New(timeFmt)
		if err != nil {
			logrus.Errorf("[kinesis %d] Issue with strftime format in 'time_key_format'", pluginID)
			return nil, err
		}
	}

	return &OutputPlugin{
		stream:                stream,
		client:                client,
		dataKeys:              dataKeys,
		partitionKey:          partitionKey,
		appendNewline:         appendNewline,
		timeKey:               timeKey,
		fmtStrftime:           timeFormatter,
		timer:                 timer,
		PluginID:              pluginID,
		random:                random,
		Concurrency:           concurrency,
		concurrencyRetryLimit: retryLimit,
	}, nil
}

// newPutRecordsClient creates the Kinesis client for calling the PutRecords method
func newPutRecordsClient(roleARN string, awsRegion string, endpoint string) (*kinesis.Kinesis, error) {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(awsRegion),
	})
	if err != nil {
		return nil, err
	}

	svcConfig := &aws.Config{}
	if endpoint != "" {
		defaultResolver := endpoints.DefaultResolver()
		cwCustomResolverFn := func(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
			if service == "kinesis" {
				return endpoints.ResolvedEndpoint{
					URL: endpoint,
				}, nil
			}
			return defaultResolver.EndpointFor(service, region, optFns...)
		}
		svcConfig.EndpointResolver = endpoints.ResolverFunc(cwCustomResolverFn)
	}

	if roleARN != "" {
		creds := stscreds.NewCredentials(sess, roleARN)
		svcConfig.Credentials = creds
	}

	client := kinesis.New(sess, svcConfig)
	client.Handlers.Build.PushBackNamed(plugins.CustomUserAgentHandler())
	return client, nil
}

// AddRecord accepts a record and adds it to the buffer
// the return value is one of: FLB_OK FLB_RETRY FLB_ERROR
func (outputPlugin *OutputPlugin) AddRecord(records *[]*kinesis.PutRecordsRequestEntry, record map[interface{}]interface{}, timeStamp *time.Time) int {
	if outputPlugin.timeKey != "" {
		buf := new(bytes.Buffer)
		err := outputPlugin.fmtStrftime.Format(buf, *timeStamp)
		if err != nil {
			logrus.Errorf("[kinesis %d] Could not create timestamp %v\n", outputPlugin.PluginID, err)
			return fluentbit.FLB_ERROR
		}
		record[outputPlugin.timeKey] = buf.String()
	}

	partitionKey := outputPlugin.getPartitionKey(record)
	data, err := outputPlugin.processRecord(record, partitionKey)
	if err != nil {
		logrus.Errorf("[kinesis %d] %v\n", outputPlugin.PluginID, err)
		// discard this single bad record instead and let the batch continue
		return fluentbit.FLB_OK
	}

	*records = append(*records, &kinesis.PutRecordsRequestEntry{
		Data:         data,
		PartitionKey: aws.String(partitionKey),
	})
	return fluentbit.FLB_OK
}

// Flush sends the current buffer of log records
// Returns FLB_OK, FLB_RETRY, FLB_ERROR
func (outputPlugin *OutputPlugin) Flush(records *[]*kinesis.PutRecordsRequestEntry) int {
	// Use a different buffer to batch the logs
	requestBuf := make([]*kinesis.PutRecordsRequestEntry, 0, maximumRecordsPerPut)
	dataLength := 0

	for i, record := range *records {
		newRecordSize := len(record.Data) + len(aws.StringValue(record.PartitionKey))

		if len(requestBuf) == maximumRecordsPerPut || (dataLength+newRecordSize) > maximumPutRecordBatchSize {
			retCode, err := outputPlugin.sendCurrentBatch(&requestBuf, &dataLength)
			if err != nil {
				logrus.Errorf("[kinesis %d] %v\n", outputPlugin.PluginID, err)
			}
			if retCode != fluentbit.FLB_OK {
				unsent := (*records)[i:]
				// requestBuf will contain records sendCurrentBatch failed to send,
				// combine those with the records yet to be sent/batched
				*records = append(requestBuf, unsent...)
				return retCode
			}
		}

		requestBuf = append(requestBuf, record)
		dataLength += newRecordSize
	}

	// send any remaining records
	retCode, err := outputPlugin.sendCurrentBatch(&requestBuf, &dataLength)
	if err != nil {
		logrus.Errorf("[kinesis %d] %v\n", outputPlugin.PluginID, err)
	}

	if retCode == output.FLB_OK {
		logrus.Debugf("[kinesis %d] Flushed %d logs\n", outputPlugin.PluginID, len(*records))
	}

	// requestBuf will contain records sendCurrentBatch failed to send
	*records = requestBuf
	return retCode
}

// FlushWithRetries sends the current buffer of log records, with retries
func (outputPlugin *OutputPlugin) FlushWithRetries(count int, records []*kinesis.PutRecordsRequestEntry) {
	var retCode, tries int

	currentRetries := outputPlugin.getConcurrentRetries()
	outputPlugin.addGoroutineCount(1)

	for tries = 0; tries < outputPlugin.concurrencyRetryLimit; tries++ {
		if currentRetries > 0 {
			// Wait if other goroutines are retrying, as well as implement a progressive backoff
			if currentRetries > uint32(outputPlugin.concurrencyRetryLimit) {
				time.Sleep(time.Duration((1<<uint32(outputPlugin.concurrencyRetryLimit))*100) * time.Millisecond)
			} else {
				time.Sleep(time.Duration((1<<currentRetries)*100) * time.Millisecond)
			}
		}

		logrus.Debugf("[kinesis %d] Sending (%d) records, currentRetries=(%d)", outputPlugin.PluginID, len(records), currentRetries)
		retCode = outputPlugin.Flush(&records)
		if retCode != output.FLB_RETRY {
			break
		}
		currentRetries = outputPlugin.addConcurrentRetries(1)
		logrus.Infof("[kinesis %d] Going to retry with (%d) records, currentRetries=(%d)", outputPlugin.PluginID, len(records), currentRetries)
	}

	outputPlugin.addGoroutineCount(-1)
	if tries > 0 {
		outputPlugin.addConcurrentRetries(-tries)
	}

	switch retCode {
	case output.FLB_ERROR:
		logrus.Errorf("[kinesis %d] Failed to send (%d) records with error", outputPlugin.PluginID, len(records))
	case output.FLB_RETRY:
		logrus.Errorf("[kinesis %d] Failed to send (%d) records after retries %d", outputPlugin.PluginID, len(records), outputPlugin.concurrencyRetryLimit)
	case output.FLB_OK:
		logrus.Debugf("[kinesis %d] Flushed %d records\n", outputPlugin.PluginID, count)
	}
}

// FlushConcurrent sends the current buffer of log records in a goroutine with retries
// Returns FLB_OK, FLB_RETRY
// Will return FLB_RETRY if the limit of concurrency has been reached
func (outputPlugin *OutputPlugin) FlushConcurrent(count int, records []*kinesis.PutRecordsRequestEntry) int {

	runningGoRoutines := outputPlugin.getGoroutineCount()
	if runningGoRoutines+1 > int32(outputPlugin.Concurrency) {
		logrus.Infof("[kinesis %d] flush returning retry, concurrency limit reached (%d)\n", outputPlugin.PluginID, runningGoRoutines)
		return output.FLB_RETRY
	}

	curRetries := outputPlugin.getConcurrentRetries()
	if curRetries > 0 {
		logrus.Infof("[kinesis %d] flush returning retry, kinesis retries in progress (%d)\n", outputPlugin.PluginID, curRetries)
		return output.FLB_RETRY
	}

	go outputPlugin.FlushWithRetries(count, records)

	return output.FLB_OK

}

func (outputPlugin *OutputPlugin) processRecord(record map[interface{}]interface{}, partitionKey string) ([]byte, error) {
	if outputPlugin.dataKeys != "" {
		record = plugins.DataKeys(outputPlugin.dataKeys, record)
	}

	var err error
	record, err = plugins.DecodeMap(record)
	if err != nil {
		logrus.Debugf("[kinesis %d] Failed to decode record: %v\n", outputPlugin.PluginID, record)
		return nil, err
	}

	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	data, err := json.Marshal(record)
	if err != nil {
		logrus.Debugf("[kinesis %d] Failed to marshal record: %v\n", outputPlugin.PluginID, record)
		return nil, err
	}

	// append a newline after each log record
	if outputPlugin.appendNewline {
		data = append(data, []byte("\n")...)
	}

	if len(data)+len(partitionKey) > maximumRecordSize {
		return nil, fmt.Errorf("Log record greater than max size allowed by Kinesis")
	}

	return data, nil
}

func (outputPlugin *OutputPlugin) sendCurrentBatch(records *[]*kinesis.PutRecordsRequestEntry, dataLength *int) (int, error) {
	if len(*records) == 0 {
		return fluentbit.FLB_OK, nil
	}
	outputPlugin.timer.Check()
	response, err := outputPlugin.client.PutRecords(&kinesis.PutRecordsInput{
		Records:    *records,
		StreamName: aws.String(outputPlugin.stream),
	})
	if err != nil {
		logrus.Errorf("[kinesis %d] PutRecords failed with %v\n", outputPlugin.PluginID, err)
		outputPlugin.timer.Start()
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == kinesis.ErrCodeProvisionedThroughputExceededException {
				logrus.Warnf("[kinesis %d] Throughput limits for the stream may have been exceeded.", outputPlugin.PluginID)
			}
		}
		return fluentbit.FLB_RETRY, err
	}
	logrus.Debugf("[kinesis %d] Sent %d events to Kinesis\n", outputPlugin.PluginID, len(*records))

	return outputPlugin.processAPIResponse(records, dataLength, response)
}

// processAPIResponse processes the successful and failed records
// it returns an error iff no records succeeded (i.e.) no progress has been made
func (outputPlugin *OutputPlugin) processAPIResponse(records *[]*kinesis.PutRecordsRequestEntry, dataLength *int, response *kinesis.PutRecordsOutput) (int, error) {

	var retCode int = fluentbit.FLB_OK
	var limitsExceeded bool

	if aws.Int64Value(response.FailedRecordCount) > 0 {
		// start timer if all records failed (no progress has been made)
		if aws.Int64Value(response.FailedRecordCount) == int64(len(*records)) {
			outputPlugin.timer.Start()
			return fluentbit.FLB_RETRY, fmt.Errorf("PutRecords request returned with no records successfully recieved")
		}

		logrus.Warnf("[kinesis %d] %d/%d records failed to be delivered. Will retry.\n", outputPlugin.PluginID, aws.Int64Value(response.FailedRecordCount), len(*records))
		failedRecords := make([]*kinesis.PutRecordsRequestEntry, 0, aws.Int64Value(response.FailedRecordCount))
		// try to resend failed records
		for i, record := range response.Records {
			if record.ErrorMessage != nil {
				logrus.Debugf("[kinesis %d] Record failed to send with error: %s\n", outputPlugin.PluginID, aws.StringValue(record.ErrorMessage))
				failedRecords = append(failedRecords, (*records)[i])
			}

			if aws.StringValue(record.ErrorCode) == kinesis.ErrCodeProvisionedThroughputExceededException {
				retCode = fluentbit.FLB_RETRY
				limitsExceeded = true
			}
		}

		if limitsExceeded {
			logrus.Warnf("[kinesis %d] Throughput limits for the stream may have been exceeded.", outputPlugin.PluginID)
		}

		*records = (*records)[:0]
		*records = append(*records, failedRecords...)
		*dataLength = 0
		for _, record := range *records {
			*dataLength += len(record.Data)
		}
	} else {
		// request fully succeeded
		outputPlugin.timer.Reset()
		*records = (*records)[:0]
		*dataLength = 0
	}
	return retCode, nil
}

// randomString generates a random string of length 8
// it uses the math/rand library
func (outputPlugin *OutputPlugin) randomString() string {
	for i := range outputPlugin.random.buffer {
		outputPlugin.random.buffer[i] = partitionKeyCharset[outputPlugin.random.seededRandom.Intn(len(partitionKeyCharset))]
	}
	return string(outputPlugin.random.buffer)
}

// getPartitionKey returns the value for a given valid key
// if the given key is empty or invalid, it returns a random string
func (outputPlugin *OutputPlugin) getPartitionKey(record map[interface{}]interface{}) string {
	partitionKey := outputPlugin.partitionKey
	if partitionKey != "" {
		for k, v := range record {
			dataKey := stringOrByteArray(k)
			if dataKey == partitionKey {
				value := stringOrByteArray(v)
				if value != "" {
					if len(value) > partitionKeyMaxLength {
						value = value[0:partitionKeyMaxLength]
					}
					return value
				}
			}
		}
	}
	return outputPlugin.randomString()
}

// stringOrByteArray returns the string value if the input is a string or byte array otherwise an empty string
func stringOrByteArray(v interface{}) string {
	switch t := v.(type) {
	case []byte:
		return string(t)
	case string:
		return t
	default:
		return ""
	}
}

// getConcurrentRetries value (goroutine safe)
func (outputPlugin *OutputPlugin) getConcurrentRetries() uint32 {
	return atomic.LoadUint32(&outputPlugin.concurrentRetries)
}

// addConcurrentRetries will update the value (goroutine safe)
func (outputPlugin *OutputPlugin) addConcurrentRetries(val int) uint32 {
	return atomic.AddUint32(&outputPlugin.concurrentRetries, uint32(val))
}

// getConcurrentRetries value (goroutine safe)
func (outputPlugin *OutputPlugin) getGoroutineCount() int32 {
	return atomic.LoadInt32(&outputPlugin.goroutineCount)
}

// addConcurrentRetries will update the value (goroutine safe)
func (outputPlugin *OutputPlugin) addGoroutineCount(val int) int32 {
	return atomic.AddInt32(&outputPlugin.goroutineCount, int32(val))
}
