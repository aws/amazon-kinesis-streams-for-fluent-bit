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
	"github.com/aws/amazon-kinesis-streams-for-fluent-bit/kinesis"
	kinesisAPI "github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/sirupsen/logrus"
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
	endpoint := output.FLBPluginConfigKey(ctx, "endpoint")
	logrus.Infof("[kinesis %d] plugin parameter endpoint = '%s'", pluginID, endpoint)
	appendNewline := output.FLBPluginConfigKey(ctx, "append_newline")
	logrus.Infof("[kinesis %d] plugin parameter append_newline = %s", pluginID, appendNewline)
	timeKey := output.FLBPluginConfigKey(ctx, "time_key")
	logrus.Infof("[firehose %d] plugin parameter time_key = '%s'\n", pluginID, timeKey)
	timeKeyFmt := output.FLBPluginConfigKey(ctx, "time_key_format")
	logrus.Infof("[firehose %d] plugin parameter time_key_format = '%s'\n", pluginID, timeKeyFmt)
	concurrency := output.FLBPluginConfigKey(ctx, "experimental_concurrency")
	logrus.Infof("[firehose %d] plugin parameter experimental_concurrency = '%s'\n", pluginID, concurrency)
	concurrencyRetries := output.FLBPluginConfigKey(ctx, "experimental_concurrency_retries")
	logrus.Infof("[firehose %d] plugin parameter experimental_concurrency_retries = '%s'\n", pluginID, concurrencyRetries)

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

	var concurrencyInt, concurrencyRetriesInt int
	var err error
	if concurrency != "" {
		concurrencyInt, err = strconv.Atoi(concurrency)
		if err != nil {
			logrus.Errorf("[kinesis %d] Invalid 'experimental_concurrency' value %s specified: %v", pluginID, concurrency, err)
			return nil, err
		}
		if concurrencyInt < 0 {
			return nil, fmt.Errorf("[kinesis %d] Invalid 'experimental_concurrency' value (%s) specified, must be a non-negative number", pluginID, concurrency)
		}

		if concurrencyInt > maximumConcurrency {
			return nil, fmt.Errorf("[kinesis %d] Invalid 'experimental_concurrency' value (%s) specified, must be less than or equal to %d", pluginID, concurrency, maximumConcurrency)
		}

		if concurrencyInt > 0 {
			logrus.Warnf("[kinesis %d] WARNING: Enabling concurrency can lead to data loss.  If 'experimental_concurrency_retries' is reached data will be lost.", pluginID)
		}
	}

	if concurrencyRetries != "" {
		concurrencyRetriesInt, err = strconv.Atoi(concurrencyRetries)
		if err != nil {
			return nil, fmt.Errorf("[kinesis %d] Invalid 'experimental_concurrency_retries' value (%s) specified: %v", pluginID, concurrencyRetries, err)
		}
		if concurrencyRetriesInt < 0 {
			return nil, fmt.Errorf("[kinesis %d] Invalid 'experimental_concurrency_retries' value (%s) specified, must be a non-negative number", pluginID, concurrencyRetries)
		}
	} else {
		concurrencyRetriesInt = defaultConcurrentRetries
	}

	return kinesis.NewOutputPlugin(region, stream, dataKeys, partitionKey, roleARN, endpoint, timeKey, timeKeyFmt, concurrencyInt, concurrencyRetriesInt, appendNL, pluginID)
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

		retCode := kinesisOutput.AddRecord(&records, record, &timestamp)
		if retCode != output.FLB_OK {
			return nil, 0, retCode
		}

		count++
	}

	return records, count, output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {

	return output.FLB_OK
}

func main() {
}
