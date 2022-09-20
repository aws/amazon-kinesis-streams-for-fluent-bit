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

package main

import (
	"C"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/aws/amazon-kinesis-firehose-for-fluent-bit/plugins"
	kinesisAPI "github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/sirupsen/logrus"

	"github.com/canva/amazon-kinesis-streams-for-fluent-bit/compress"
	"github.com/canva/amazon-kinesis-streams-for-fluent-bit/enricher"
	"github.com/canva/amazon-kinesis-streams-for-fluent-bit/kinesis"
)

const (
	// Kinesis API Limit https://docs.aws.amazon.com/sdk-for-go/api/service/kinesis/#Kinesis.PutRecords
	maximumRecordsPerPut     = 500
	maximumConcurrency       = 10
	defaultConcurrentRetries = 4
)

var (
	pluginInstances []*kinesis.OutputPlugin
)

func addPluginInstance(ctx unsafe.Pointer) error {
	pluginID := len(pluginInstances)
	output.FLBPluginSetContext(ctx, pluginID)
	instance, err := newKinesisOutput(ctx, pluginID)
	if err != nil {
		return err
	}

	pluginInstances = append(pluginInstances, instance)
	return nil
}

func getPluginInstance(ctx unsafe.Pointer) *kinesis.OutputPlugin {
	pluginID := output.FLBPluginGetContext(ctx).(int)
	return pluginInstances[pluginID]
}

func newKinesisOutput(ctx unsafe.Pointer, pluginID int) (*kinesis.OutputPlugin, error) {
	stream := output.FLBPluginConfigKey(ctx, "stream")
	logrus.Infof("[kinesis %d] plugin parameter stream = '%s'", pluginID, stream)
	region := output.FLBPluginConfigKey(ctx, "region")
	logrus.Infof("[kinesis %d] plugin parameter region = '%s'", pluginID, region)
	dataKeys := output.FLBPluginConfigKey(ctx, "data_keys")
	logrus.Infof("[kinesis %d] plugin parameter data_keys = '%s'", pluginID, dataKeys)
	partitionKey := output.FLBPluginConfigKey(ctx, "partition_key")
	logrus.Infof("[kinesis %d] plugin parameter partition_key = '%s'", pluginID, partitionKey)
	roleARN := output.FLBPluginConfigKey(ctx, "role_arn")
	logrus.Infof("[kinesis %d] plugin parameter role_arn = '%s'", pluginID, roleARN)
	kinesisEndpoint := output.FLBPluginConfigKey(ctx, "endpoint")
	logrus.Infof("[kinesis %d] plugin parameter endpoint = '%s'", pluginID, kinesisEndpoint)
	stsEndpoint := output.FLBPluginConfigKey(ctx, "sts_endpoint")
	logrus.Infof("[kinesis %d] plugin parameter sts_endpoint = '%s'", pluginID, stsEndpoint)
	appendNewline := output.FLBPluginConfigKey(ctx, "append_newline")
	logrus.Infof("[kinesis %d] plugin parameter append_newline = %s", pluginID, appendNewline)
	timeKey := output.FLBPluginConfigKey(ctx, "time_key")
	logrus.Infof("[kinesis %d] plugin parameter time_key = '%s'", pluginID, timeKey)
	timeKeyFmt := output.FLBPluginConfigKey(ctx, "time_key_format")
	logrus.Infof("[kinesis %d] plugin parameter time_key_format = '%s'", pluginID, timeKeyFmt)
	concurrency := output.FLBPluginConfigKey(ctx, "experimental_concurrency")
	logrus.Infof("[kinesis %d] plugin parameter experimental_concurrency = '%s'", pluginID, concurrency)
	concurrencyRetries := output.FLBPluginConfigKey(ctx, "experimental_concurrency_retries")
	logrus.Infof("[kinesis %d] plugin parameter experimental_concurrency_retries = '%s'", pluginID, concurrencyRetries)
	logKey := output.FLBPluginConfigKey(ctx, "log_key")
	logrus.Infof("[kinesis %d] plugin parameter log_key = '%s'", pluginID, logKey)
	aggregation := output.FLBPluginConfigKey(ctx, "aggregation")
	logrus.Infof("[kinesis %d] plugin parameter aggregation = '%s'", pluginID, aggregation)
	compression := output.FLBPluginConfigKey(ctx, "compression")
	logrus.Infof("[kinesis %d] plugin parameter compression = '%s'", pluginID, compression)
	replaceDots := output.FLBPluginConfigKey(ctx, "replace_dots")
	logrus.Infof("[kinesis %d] plugin parameter replace_dots = '%s'", pluginID, replaceDots)
	httpRequestTimeout := output.FLBPluginConfigKey(ctx, "http_request_timeout")
	logrus.Infof("[kinesis %d] plugin parameter http_request_timeout = '%s'", pluginID, httpRequestTimeout)

	aggregationMaximumRecordSize := output.FLBPluginConfigKey(ctx, "aggregation_maximum_record_size")
	logrus.Infof("[kinesis %d] plugin parameter aggregation_maximum_record_size = %q", pluginID, aggregationMaximumRecordSize)
	skipAggregationRecordSize := output.FLBPluginConfigKey(ctx, "skip_aggregation_record_size")
	logrus.Infof("[kinesis %d] plugin parameter skip_aggregation_record_size = %q", pluginID, skipAggregationRecordSize)
	aggregationCompression := output.FLBPluginConfigKey(ctx, "aggregation_compression")
	logrus.Infof("[kinesis %d] plugin parameter aggregation_compression = %q", pluginID, aggregationCompression)
	aggregationCompressionLevel := output.FLBPluginConfigKey(ctx, "aggregation_compression_level")
	logrus.Infof("[kinesis %d] plugin parameter aggregation_compression_level = %q", pluginID, aggregationCompressionLevel)

	enrichRecords := output.FLBPluginConfigKey(ctx, "enrich_records")
	logrus.Infof("[kinesis %d] plugin parameter enrich_records = %q", pluginID, enrichRecords)

	if stream == "" || region == "" {
		return nil, fmt.Errorf("[kinesis %d] stream and region are required configuration parameters", pluginID)
	}

	if partitionKey == "log" {
		return nil, fmt.Errorf("[kinesis %d] 'log' cannot be set as the partition key", pluginID)
	}

	if partitionKey == "" {
		logrus.Infof("[kinesis %d] no partition key provided. A random one will be generated.", pluginID)
	}

	appendNL := false
	if strings.ToLower(appendNewline) == "true" {
		appendNL = true
	}

	isAggregate := false
	if strings.ToLower(aggregation) == "true" {
		isAggregate = true
	}

	if isAggregate && partitionKey != "" {
		logrus.Warnf("[kinesis %d] 'partition_key' has different behavior when 'aggregation' enabled. All aggregated records will use a partition key sourced from the first record in the batch", pluginID)
	}

	var concurrencyInt, concurrencyRetriesInt int
	var err error
	if concurrency != "" {
		concurrencyInt, err = parseNonNegativeConfig("experimental_concurrency", concurrency, pluginID)
		if err != nil {
			return nil, err
		}

		if concurrencyInt > maximumConcurrency {
			return nil, fmt.Errorf("[kinesis %d] Invalid 'experimental_concurrency' value (%s) specified, must be less than or equal to %d", pluginID, concurrency, maximumConcurrency)
		}

		if concurrencyInt > 0 {
			logrus.Warnf("[kinesis %d] WARNING: Enabling concurrency can lead to data loss.  If 'experimental_concurrency_retries' is reached data will be lost.", pluginID)
		}
	}

	if concurrencyRetries != "" {
		concurrencyRetriesInt, err = parseNonNegativeConfig("experimental_concurrency_retries", concurrencyRetries, pluginID)
		if err != nil {
			return nil, err
		}
	} else {
		concurrencyRetriesInt = defaultConcurrentRetries
	}

	var comp kinesis.CompressionType
	if strings.ToLower(compression) == string(kinesis.CompressionZlib) {
		comp = kinesis.CompressionZlib
	} else if strings.ToLower(compression) == string(kinesis.CompressionGzip) {
		comp = kinesis.CompressionGzip
	} else if strings.ToLower(compression) == string(kinesis.CompressionNone) || compression == "" {
		comp = kinesis.CompressionNone
	} else {
		return nil, fmt.Errorf("[kinesis %d] Invalid 'compression' value (%s) specified, must be 'zlib', 'gzip', 'none', or undefined", pluginID, compression)
	}

	var httpRequestTimeoutDuration time.Duration
	if httpRequestTimeout != "" {
		httpRequestTimeoutInt, err := parseNonNegativeConfig("http_request_timeout", httpRequestTimeout, pluginID)
		if err != nil {
			return nil, err
		}
		httpRequestTimeoutDuration = time.Duration(httpRequestTimeoutInt) * time.Second
	}

	var (
		aggregationMaximumRecordSizeInt *int
		skipAggregationRecordSizeInt    *int
		aggregationCompressionFormat    compress.Format
		aggregationCompressionLevelInt  int
	)
	if aggregationMaximumRecordSize != "" {
		intVal, err := parseNonNegativeConfig("aggregation_maximum_record_size", aggregationMaximumRecordSize, pluginID)
		if err != nil {
			return nil, err
		}
		aggregationMaximumRecordSizeInt = &intVal
	}
	if skipAggregationRecordSize != "" {
		intVal, err := parseNonNegativeConfig("skip_aggregation_record_size", skipAggregationRecordSize, pluginID)
		if err != nil {
			return nil, err
		}
		skipAggregationRecordSizeInt = &intVal
	}
	switch aggregationCompression {
	case "", string(compress.FormatNoop):
		aggregationCompressionFormat = compress.FormatNoop
	case string(compress.FormatGZip):
		aggregationCompressionFormat = compress.FormatGZip
	case string(compress.FormatZSTD):
		aggregationCompressionFormat = compress.FormatZSTD
	default:
		return nil, fmt.Errorf("[kinesis %d] Invalid 'aggregation_compression' value %q specified, must be 'noop', 'gzip', 'zstd', or undefined", pluginID, aggregationCompression)
	}
	if aggregationCompressionLevel != "" {
		aggregationCompressionLevelInt, err = parseNonNegativeConfig("aggregation_compression_level", aggregationCompressionLevel, pluginID)
		if err != nil {
			return nil, err
		}
	}

	enricherEnable := false
	if strings.ToLower(enrichRecords) == "true" {
		enricherEnable = true
	}

	enricher.Init(enricherEnable)
	compress.Init(&compress.Config{
		Format: aggregationCompressionFormat,
		Level:  aggregationCompressionLevelInt,
	})

	return kinesis.NewOutputPlugin(region, stream, dataKeys, partitionKey, roleARN, kinesisEndpoint, stsEndpoint, timeKey, timeKeyFmt, logKey, replaceDots, concurrencyInt, concurrencyRetriesInt, isAggregate, appendNL, comp, pluginID, httpRequestTimeoutDuration, aggregationMaximumRecordSizeInt, skipAggregationRecordSizeInt)
}

func parseNonNegativeConfig(configName string, configValue string, pluginID int) (int, error) {
	configValueInt, err := strconv.Atoi(configValue)
	if err != nil {
		return 0, fmt.Errorf("[kinesis %d] Invalid '%s' value (%s) specified: %v", pluginID, configName, configValue, err)
	}
	if configValueInt < 0 {
		return 0, fmt.Errorf("[kinesis %d] Invalid '%s' value (%s) specified, must be a non-negative number", pluginID, configName, configValue)
	}
	return configValueInt, nil
}

// The "export" comments have syntactic meaning
// This is how the compiler knows a function should be callable from the C code

//export FLBPluginRegister
func FLBPluginRegister(ctx unsafe.Pointer) int {
	return output.FLBPluginRegister(ctx, "kinesis", "Amazon Kinesis Data Streams Fluent Bit Plugin.")
}

//export FLBPluginInit
func FLBPluginInit(ctx unsafe.Pointer) int {
	plugins.SetupLogger()
	err := addPluginInstance(ctx)
	if err != nil {
		logrus.Errorf("[kinesis] Failed to initialize plugin: %v\n", err)
		return output.FLB_ERROR
	}
	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	kinesisOutput := getPluginInstance(ctx)

	fluentTag := C.GoString(tag)

	events, count, retCode := unpackRecords(kinesisOutput, data, length)
	if retCode != output.FLB_OK {
		logrus.Errorf("[kinesis %d] failed to unpackRecords with tag: %s\n", kinesisOutput.PluginID, fluentTag)

		return retCode
	}

	logrus.Debugf("[kinesis %d] Flushing %d logs with tag: %s\n", kinesisOutput.PluginID, count, fluentTag)
	if kinesisOutput.Concurrency > 0 {
		return kinesisOutput.FlushConcurrent(count, events)
	}

	return kinesisOutput.Flush(&events)
}

func unpackRecords(kinesisOutput *kinesis.OutputPlugin, data unsafe.Pointer, length C.int) ([]*kinesisAPI.PutRecordsRequestEntry, int, int) {
	var ret int
	var ts interface{}
	var timestamp time.Time
	var record map[interface{}]interface{}
	count := 0

	records := make([]*kinesisAPI.PutRecordsRequestEntry, 0, maximumRecordsPerPut)

	// Create Fluent Bit decoder
	dec := output.NewDecoder(data, int(length))

	for {
		//Extract Record
		ret, ts, record = output.GetRecord(dec)
		if ret != 0 {
			break
		}

		switch tts := ts.(type) {
		case output.FLBTime:
			timestamp = tts.Time
		case uint64:
			// when ts is of type uint64 it appears to
			// be the amount of seconds since unix epoch.
			timestamp = time.Unix(int64(tts), 0)
		default:
			timestamp = time.Now()
		}

		record = enricher.EnrichRecord(record, timestamp)

		retCode := kinesisOutput.AddRecord(&records, record, &timestamp)
		if retCode != output.FLB_OK {
			return nil, 0, retCode
		}

		count++
	}

	if kinesisOutput.IsAggregate() {
		retCode := kinesisOutput.FlushAggregatedRecords(&records)
		if retCode != output.FLB_OK {
			return nil, 0, retCode
		}
	}

	return records, count, output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {

	return output.FLB_OK
}

func main() {
}
