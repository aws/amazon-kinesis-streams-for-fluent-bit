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

//go:generate mockgen -destination mock_kinesis/mock_kinesis.go -copyright_file=../COPYRIGHT github.com/canva/amazon-kinesis-streams-for-fluent-bit/kinesis PutRecordsClient

// Package kinesis contains the OutputPlugin which sends log records to Kinesis Stream
package kinesis

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"strings"
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

	"github.com/canva/amazon-kinesis-streams-for-fluent-bit/util"

	"github.com/canva/amazon-kinesis-streams-for-fluent-bit/aggregate"
)

const (
	truncatedSuffix                  = "[Truncated...]"
	truncationReductionPercent       = 90
	truncationCompressionMaxAttempts = 10
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

// CompressionType indicates the type of compression to apply to each record
type CompressionType string

const (
	// CompressionNone disables compression
	CompressionNone CompressionType = "none"
	// CompressionZlib enables zlib compression
	CompressionZlib = "zlib"
	// CompressionGzip enables gzip compression
	CompressionGzip = "gzip"
)

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
	logKey                string
	client                PutRecordsClient
	timer                 *plugins.Timeout
	PluginID              int
	stringGen             *util.RandomStringGenerator
	Concurrency           int
	concurrencyRetryLimit int
	// Concurrency is the limit, goroutineCount represents the running goroutines
	goroutineCount int32
	// Used to implement backoff for concurrent flushes
	concurrentRetries uint32
	isAggregate       bool
	aggregator        *aggregate.Aggregator
	compression       CompressionType
	// If specified, dots in key names should be replaced with other symbols
	replaceDots string
}

// NewOutputPlugin creates an OutputPlugin object
func NewOutputPlugin(region, stream, dataKeys, partitionKey, roleARN, kinesisEndpoint, stsEndpoint, timeKey, timeFmt, logKey, replaceDots string, concurrency, retryLimit int, isAggregate, appendNewline bool, compression CompressionType, pluginID int, httpRequestTimeout time.Duration, aggregationMaximumRecordSize, skipAggregationRecordSize *int) (*OutputPlugin, error) {
	client, err := newPutRecordsClient(roleARN, region, kinesisEndpoint, stsEndpoint, pluginID, httpRequestTimeout)
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

	stringGen := util.NewRandomStringGenerator(8)

	var timeFormatter *strftime.Strftime
	if timeKey != "" {
		if timeFmt == "" {
			timeFmt = defaultTimeFmt
		}
		timeFormatter, err = strftime.New(timeFmt, strftime.WithMilliseconds('L'), strftime.WithMicroseconds('f'))
		if err != nil {
			logrus.Errorf("[kinesis %d] Issue with strftime format in 'time_key_format'", pluginID)
			return nil, err
		}
	}

	var aggregator *aggregate.Aggregator
	if isAggregate {
		aggregator = aggregate.NewAggregator(stringGen, &aggregate.Config{
			MaximumRecordSize: aggregationMaximumRecordSize,
			MaxAggRecordSize:  skipAggregationRecordSize,
		})
	}

	return &OutputPlugin{
		stream:                stream,
		client:                client,
		dataKeys:              dataKeys,
		partitionKey:          partitionKey,
		appendNewline:         appendNewline,
		timeKey:               timeKey,
		fmtStrftime:           timeFormatter,
		logKey:                logKey,
		timer:                 timer,
		PluginID:              pluginID,
		stringGen:             stringGen,
		Concurrency:           concurrency,
		concurrencyRetryLimit: retryLimit,
		isAggregate:           isAggregate,
		aggregator:            aggregator,
		compression:           compression,
		replaceDots:           replaceDots,
	}, nil
}

// newPutRecordsClient creates the Kinesis client for calling the PutRecords method
func newPutRecordsClient(roleARN string, awsRegion string, kinesisEndpoint string, stsEndpoint string, pluginID int, httpRequestTimeout time.Duration) (*kinesis.Kinesis, error) {
	customResolverFn := func(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
		if service == endpoints.KinesisServiceID && kinesisEndpoint != "" {
			return endpoints.ResolvedEndpoint{
				URL: kinesisEndpoint,
			}, nil
		} else if service == endpoints.StsServiceID && stsEndpoint != "" {
			return endpoints.ResolvedEndpoint{
				URL: stsEndpoint,
			}, nil
		}
		return endpoints.DefaultResolver().EndpointFor(service, region, optFns...)
	}
	httpClient := &http.Client{
		Timeout: httpRequestTimeout,
	}

	// Fetch base credentials
	baseConfig := &aws.Config{
		Region:                        aws.String(awsRegion),
		EndpointResolver:              endpoints.ResolverFunc(customResolverFn),
		CredentialsChainVerboseErrors: aws.Bool(true),
		HTTPClient:                    httpClient,
	}

	sess, err := session.NewSession(baseConfig)
	if err != nil {
		return nil, err
	}

	var svcSess = sess
	var svcConfig = baseConfig
	eksRole := os.Getenv("EKS_POD_EXECUTION_ROLE")
	if eksRole != "" {
		logrus.Debugf("[kinesis %d] Fetching EKS pod credentials.\n", pluginID)
		eksConfig := &aws.Config{}
		creds := stscreds.NewCredentials(svcSess, eksRole)
		eksConfig.Credentials = creds
		eksConfig.Region = aws.String(awsRegion)
		eksConfig.HTTPClient = httpClient
		svcConfig = eksConfig

		svcSess, err = session.NewSession(svcConfig)
		if err != nil {
			return nil, err
		}
	}
	if roleARN != "" {
		logrus.Debugf("[kinesis %d] Fetching credentials for %s\n", pluginID, roleARN)
		stsConfig := &aws.Config{}
		creds := stscreds.NewCredentials(svcSess, roleARN)
		stsConfig.Credentials = creds
		stsConfig.Region = aws.String(awsRegion)
		stsConfig.HTTPClient = httpClient
		svcConfig = stsConfig

		svcSess, err = session.NewSession(svcConfig)
		if err != nil {
			return nil, err
		}
	}

	client := kinesis.New(svcSess, svcConfig)
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

	partitionKey, hasPartitionKey := outputPlugin.getPartitionKey(record)
	var partitionKeyLen = len(partitionKey)
	if !hasPartitionKey {
		partitionKeyLen = outputPlugin.stringGen.Size
	}
	data, err := outputPlugin.processRecord(record, partitionKeyLen)
	if err != nil {
		logrus.Errorf("[kinesis %d] %v\n", outputPlugin.PluginID, err)
		// discard this single bad record instead and let the batch continue
		return fluentbit.FLB_OK
	}

	if !outputPlugin.isAggregate {
		if !hasPartitionKey {
			partitionKey = outputPlugin.stringGen.RandomString()
		}
		logrus.Debugf("[kinesis %d] Got value: %s for a given partition key.\n", outputPlugin.PluginID, partitionKey)
		*records = append(*records, &kinesis.PutRecordsRequestEntry{
			Data:         data,
			PartitionKey: aws.String(partitionKey),
		})
	} else {
		// Use the KPL aggregator to buffer records isAggregate is true
		aggRecord, err := outputPlugin.aggregator.AddRecord(partitionKey, hasPartitionKey, data)
		if err != nil {
			logrus.Errorf("[kinesis %d] Failed to aggregate record %v\n", outputPlugin.PluginID, err)
			// discard this single bad record instead and let the batch continue
			return fluentbit.FLB_OK
		}

		// If aggRecord isn't nil, then a full kinesis record has been aggregated
		if aggRecord != nil {
			*records = append(*records, aggRecord)
		}
	}

	return fluentbit.FLB_OK
}

// FlushAggregatedRecords must be called after
// Returns FLB_OK, FLB_RETRY, FLB_ERROR
func (outputPlugin *OutputPlugin) FlushAggregatedRecords(records *[]*kinesis.PutRecordsRequestEntry) int {

	aggRecord, err := outputPlugin.aggregator.AggregateRecords()
	if err != nil {
		logrus.Errorf("[kinesis %d] Failed to aggregate record %v\n", outputPlugin.PluginID, err)
		return fluentbit.FLB_ERROR
	}

	if aggRecord != nil {
		*records = append(*records, aggRecord)
	}

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

	for tries = 0; tries <= outputPlugin.concurrencyRetryLimit; tries++ {
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

func replaceDots(obj map[interface{}]interface{}, replacement string) map[interface{}]interface{} {
	for k, v := range obj {
		var curK = k
		switch kt := k.(type) {
		case string:
			curK = strings.ReplaceAll(kt, ".", replacement)
		}
		delete(obj, k)
		switch vt := v.(type) {
		case map[interface{}]interface{}:
			v = replaceDots(vt, replacement)
		}

		obj[curK] = v
	}

	return obj
}

func (outputPlugin *OutputPlugin) processRecord(record map[interface{}]interface{}, partitionKeyLen int) ([]byte, error) {
	if outputPlugin.dataKeys != "" {
		record = plugins.DataKeys(outputPlugin.dataKeys, record)
	}

	var err error
	record, err = plugins.DecodeMap(record)
	if err != nil {
		logrus.Debugf("[kinesis %d] Failed to decode record: %v\n", outputPlugin.PluginID, record)
		return nil, err
	}

	if outputPlugin.replaceDots != "" {
		record = replaceDots(record, outputPlugin.replaceDots)
	}

	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	var data []byte

	if outputPlugin.logKey != "" {
		log, err := plugins.LogKey(record, outputPlugin.logKey)
		if err != nil {
			return nil, err
		}

		data, err = plugins.EncodeLogKey(log)
	} else {
		data, err = json.Marshal(record)
	}

	if err != nil {
		logrus.Debugf("[kinesis %d] Failed to marshal record: %v\n", outputPlugin.PluginID, record)
		return nil, err
	}

	// append a newline after each log record
	if outputPlugin.appendNewline {
		data = append(data, []byte("\n")...)
	}

	// max truncation size
	maxDataSize := maximumRecordSize - partitionKeyLen

	switch outputPlugin.compression {
	case CompressionZlib:
		data, err = compressThenTruncate(zlibCompress, data, maxDataSize, []byte(truncatedSuffix), *outputPlugin)
	case CompressionGzip:
		data, err = compressThenTruncate(gzipCompress, data, maxDataSize, []byte(truncatedSuffix), *outputPlugin)
	default:
	}
	if err != nil {
		return nil, err
	}

	if len(data)+partitionKeyLen > maximumRecordSize {
		logrus.Warnf("[kinesis %d] Found record with %d bytes, truncating to 1MB, stream=%s\n", outputPlugin.PluginID, len(data)+partitionKeyLen, outputPlugin.stream)
		data = data[:maxDataSize-len(truncatedSuffix)]
		data = append(data, []byte(truncatedSuffix)...)
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

func getFromMap(dataKey string, record map[interface{}]interface{}) interface{} {
	for k, v := range record {
		currentKey := stringOrByteArray(k)
		if currentKey == dataKey {
			return v
		}
	}

	return ""
}

// getPartitionKey returns the value for a given valid key
// if the given key is empty or invalid, it returns empty
// second return value indicates whether a partition key was found or not
func (outputPlugin *OutputPlugin) getPartitionKey(record map[interface{}]interface{}) (string, bool) {
	partitionKey := outputPlugin.partitionKey
	if partitionKey != "" {
		partitionKeys := strings.Split(partitionKey, "->")
		num := len(partitionKeys)
		for count, dataKey := range partitionKeys {
			newRecord := getFromMap(dataKey, record)
			if count == num-1 {
				value := stringOrByteArray(newRecord)
				if value != "" {
					if len(value) > partitionKeyMaxLength {
						value = value[0:partitionKeyMaxLength]
					}
					return value, true
				}
			}
			_, ok := newRecord.(map[interface{}]interface{})
			if ok {
				record = newRecord.(map[interface{}]interface{})
			} else {
				logrus.Errorf("[kinesis %d] The partition key could not be found in the record, using a random string instead", outputPlugin.PluginID)
				return "", false
			}
		}
	}
	return "", false
}

// CompressorFunc is a function that compresses a byte slice
type CompressorFunc func([]byte) ([]byte, error)

func zlibCompress(data []byte) ([]byte, error) {
	var b bytes.Buffer

	if data == nil {
		return nil, fmt.Errorf("No data to compress.  'nil' value passed as data")
	}

	zw := zlib.NewWriter(&b)
	_, err := zw.Write(data)
	if err != nil {
		return data, err
	}
	err = zw.Close()
	if err != nil {
		return data, err
	}

	return b.Bytes(), nil
}

func gzipCompress(data []byte) ([]byte, error) {
	var b bytes.Buffer

	if data == nil {
		return nil, fmt.Errorf("No data to compress.  'nil' value passed as data")
	}

	zw := gzip.NewWriter(&b)
	_, err := zw.Write(data)
	if err != nil {
		return data, err
	}
	err = zw.Close()
	if err != nil {
		return data, err
	}

	return b.Bytes(), nil
}

// Compress Then Truncate
// compresses data with CompressorFunction and iteratively truncates data
// adding the truncation suffix if the CompressorFunction output exceeds maxOutLen.
// The output is compressed and possibly truncated data whose length guaranteed to
// be less than or equal to maxOutLen.
func compressThenTruncate(compressorFunc CompressorFunc, data []byte, maxOutLen int, truncatedSuffix []byte, outputPlugin OutputPlugin) ([]byte, error) {
	var compressedData []byte
	var truncationBuffer []byte
	var originalCompressedLen int
	var compressedLen int
	var err error

	/* Iterative approach to truncation */
	isTruncated := false
	compressedLen = math.MaxInt64
	truncatedInLen := len(data)
	truncationBuffer = data
	truncationCompressionAttempts := 0
	for compressedLen > maxOutLen {
		compressedData, err = compressorFunc(truncationBuffer)
		if err != nil {
			return nil, err
		}
		compressedLen = len(compressedData)

		/* Truncation needed */
		if compressedLen > maxOutLen {
			truncationCompressionAttempts++
			logrus.Debugf("[kinesis %d] iterative truncation round stream=%s\n",
				outputPlugin.PluginID, outputPlugin.stream)

			/* Base case: input compressed empty string, output still too large */
			if truncatedInLen == 0 {
				logrus.Errorf("[kinesis %d] truncation failed, compressed empty input too "+
					"large stream=%s\n", outputPlugin.PluginID, outputPlugin.stream)
				return nil, errors.New("compressed empty to large")
			}

			/* Base case: too many attempts - just to be extra safe */
			if truncationCompressionAttempts > truncationCompressionMaxAttempts {
				logrus.Errorf("[kinesis %d] truncation failed, too many compression attempts "+
					"stream=%s\n", outputPlugin.PluginID, outputPlugin.stream)
				return nil, errors.New("too many compression attempts")
			}

			/* Calculate corrected input size */
			truncatedInLenPrev := truncatedInLen
			truncatedInLen = (maxOutLen * truncatedInLen) / compressedLen
			truncatedInLen = (truncatedInLen * truncationReductionPercent) / 100

			/* Ensure working down */
			if truncatedInLen >= truncatedInLenPrev {
				truncatedInLen = truncatedInLenPrev - 1
			}

			/* Allocate truncation buffer */
			if !isTruncated {
				isTruncated = true
				originalCompressedLen = compressedLen
				truncationBuffer = make([]byte, truncatedInLen)
				copy(truncationBuffer, data[:truncatedInLen])
			}

			/* Slap on truncation suffix */
			if truncatedInLen < len(truncatedSuffix) {
				/* No room for the truncation suffix. Terminal error */
				logrus.Errorf("[kinesis %d] truncation failed, no room for suffix "+
					"stream=%s\n", outputPlugin.PluginID, outputPlugin.stream)
				return nil, errors.New("no room for suffix")
			}
			truncationBuffer = truncationBuffer[:truncatedInLen]
			copy(truncationBuffer[len(truncationBuffer)-len(truncatedSuffix):], truncatedSuffix)
		}
	}

	if isTruncated {
		logrus.Warnf("[kinesis %d] Found compressed record with %d bytes, "+
			"truncating to %d bytes after compression, stream=%s\n",
			outputPlugin.PluginID, originalCompressedLen, len(compressedData), outputPlugin.stream)
	}

	return compressedData, nil
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

// IsAggregate indicates if this instance of the plugin has KCL aggregation enabled.
func (outputPlugin *OutputPlugin) IsAggregate() bool {
	return outputPlugin.isAggregate
}
