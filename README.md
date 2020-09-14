[![Test Actions Status](https://github.com/aws/amazon-kinesis-streams-for-fluent-bit/workflows/Build/badge.svg)](https://github.com/aws/amazon-kinesis-streams-for-fluent-bit/actions)
## Fluent Bit Plugin for Amazon Kinesis Data Streams

A Fluent Bit output plugin for Amazon Kinesis Data Streams.

#### Security disclosures

If you think you’ve found a potential security issue, please do not post it in the Issues.  Instead, please follow the instructions [here](https://aws.amazon.com/security/vulnerability-reporting/) or email AWS security directly at [aws-security@amazon.com](mailto:aws-security@amazon.com).

### Plugin Options

* `region`: The region which your Kinesis Data Stream is in.
* `stream`: The name of the Kinesis Data Stream that you want log records sent to.
* `partition_key`: A partition key is used to group data by shard within a stream. A Kinesis Data Stream uses the partition key that is associated with each data record to determine which shard a given data record belongs to. For example, if your logs come from Docker containers, you can use container_id as the partition key, and the logs will be grouped and stored on different shards depending upon the id of the container they were generated from. As the data within a shard are coarsely ordered, you will get all your logs from one container in one shard roughly in order. If you don't set a partition key or put an invalid one, a random key will be generated, and the logs will be directed to random shards. If the partition key is invalid, the plugin will print an warning message.
* `data_keys`: By default, the whole log record will be sent to Kinesis. If you specify key name(s) with this option, then only those keys and values will be sent to Kinesis. For example, if you are using the Fluentd Docker log driver, you can specify `data_keys log` and only the log message will be sent to Kinesis. If you specify multiple keys, they should be comma delimited.
* `log_key`: By default, the whole log record will be sent to Kinesis. If you specify a key name with this option, then only the value of that key will be sent to Kinesis. For example, if you are using the Fluentd Docker log driver, you can specify `log_key log` and only the log message will be sent to Kinesis.
* `role_arn`: ARN of an IAM role to assume (for cross account access).
* `endpoint`: Specify a custom endpoint for the Kinesis Streams API.
* `sts_endpoint`: Specify a custom endpoint for the STS API; used to assume your custom role provided with `role_arn`.
* `append_newline`: If you set append_newline as true, a newline will be addded after each log record.
* `time_key`: Add the timestamp to the record under this key. By default the timestamp from Fluent Bit will not be added to records sent to Kinesis.
* `time_key_format`: [strftime](http://man7.org/linux/man-pages/man3/strftime.3.html) compliant format string for the timestamp; for example, `%Y-%m-%dT%H:%M:%S%z`. This option is used with `time_key`. You can also use `%L` for milliseconds and `%f` for microseconds.
* `experimental_concurrency`: Specify a limit of concurrent go routines for flushing records to kinesis.  By default `experimental_concurrency` is set to 0 and records are flushed in Fluent Bit's single thread. This means that requests to Kinesis will block the execution of Fluent Bit.  If this value is set to `4` for example then calls to Flush records from fluentbit will spawn concurrent go routines until the limit of `4` concurrent go routines are running.  Once the `experimental_concurrency` limit is reached calls to Flush will return a retry code.  The upper limit of the `experimental_concurrency` option is `10`.  WARNING:  Enabling `experimental_concurrency` can lead to data loss if the retry count is reached.  Enabling concurrency will increase resource usage (memory and CPU).
* `experimental_concurrency_retries`: Specify a limit to the number of retries concurrent goroutines will attempt.  By default `4` retries will be attempted before records are dropped.
* `aggregation`: Setting `aggregation` to `true` will enable KPL aggregation of records sent to Kinesis.  This feature isn't compatible with the `partition_key` feature.  See the KPL aggregation section below for more details.
* `compression`: Setting `compression` to `zlib` will enable zlib compression of each record.  By default this feature is disabled and records are not compressed.

### Permissions

The plugin requires `kinesis:PutRecords` permissions.

### Credentials

This plugin uses the AWS SDK Go, and uses its [default credential provider chain](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html). If you are using the plugin on Amazon EC2 or Amazon ECS or Amazon EKS, the plugin will use your EC2 instance role or [ECS Task role permissions](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html) or [EKS IAM Roles for Service Accounts for pods](https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html). The plugin can also retrieve credentials from a [shared credentials file](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html), or from the standard `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` environment variables.

### Environment Variables

* `FLB_LOG_LEVEL`: Set the log level for the plugin. Valid values are: `debug`, `info`, and `error` (case insensitive). Default is `info`. **Note**: Setting log level in the Fluent Bit Configuration file using the Service key will not affect the plugin log level (because the plugin is external).
* `SEND_FAILURE_TIMEOUT`: Allows you to configure a timeout if the plugin can not send logs to Kinesis Streams. The timeout is specified as a [Golang duration](https://golang.org/pkg/time/#ParseDuration), for example: `5m30s`. If the plugin has failed to make any progress for the given period of time, then it will exit and kill Fluent Bit. This is useful in scenarios where you want your logging solution to fail fast if it has been misconfigured (i.e. network or credentials have not been set up to allow it to send to Kinesis Streams).

### Fluent Bit Versions

This plugin has been tested with Fluent Bit 1.2.0+. It may not work with older Fluent Bit versions. We recommend using the latest version of Fluent Bit as it will contain the newest features and bug fixes.

### Example Fluent Bit Config File

```
[INPUT]
    Name        forward
    Listen      0.0.0.0
    Port        24224

[OUTPUT]
    Name            kinesis
    Match           *
    region          us-west-2
    stream          my-kinesis-stream-name
    partition_key   container_id
    append_newline  true
```

### AWS for Fluent Bit

We distribute a container image with Fluent Bit and this plugin.

##### GitHub

[github.com/aws/aws-for-fluent-bit](https://github.com/aws/aws-for-fluent-bit)

##### Docker Hub

[amazon/aws-for-fluent-bit](https://hub.docker.com/r/amazon/aws-for-fluent-bit/tags)

##### Amazon ECR

You can use our SSM Public Parameters to find the Amazon ECR image URI in your region:

```
aws ssm get-parameters-by-path --path /aws/service/aws-for-fluent-bit/
```

For more see [our docs](https://github.com/aws/aws-for-fluent-bit#public-images).


### KPL aggregation

KPL aggregation can be enabled by setting the `aggregation` parameter to `true` (default is false).  With aggregation enabled each Record in the PutRecords request can contain multiple serialized records in the KCL protobuf structure.  This batch of records will only count as a single record towards the Kinesis records per second limit (currently 1000 records/sec per shard).

The advantages of enabling KPL aggregation are:

 - Increased throughput, and decreased Kinesis costs for smaller records (records less than 1K).
 - Less overhead in error checking PutRecords results (fewer PutRecords results to verify).
 - Firehose will de-aggregate the records automatically (free de-aggregation if Firehose is leveraged).

The disadvantages are:
 - The flush time (or buffer size) will need to be tuned to take advantage of aggregation (more on that below).
 - You must use the KCL library to read data from kinesis to de-aggregate the protobuf serialization (if Firehose isn't the consumer).
 - The `partition_key` feature isn't compatible with aggregation given multiple records are in each PutRecord structure.  The `partition_key` value of the first record in the batch will be used to route the entire batch to a given shard.  Given this limitation, using both `partition_key` and `aggregation` simultaneously isn't recommended.

KPL Aggregated Record Reference:  https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md

#### Tuning for aggregation

When using `aggregation` the buffers and flush time may need to be tuned.  For low volume use cases a longer flush time maybe preferable to take full advantage of the aggregation cost savings.

More specifically, increasing the flush value will ensure the most records are aggregated taking full advantage of the cost savings.

```
[SERVICE]
     Flush 20
```


### Example Fluent Bit Aggregation Config File

```
[SERVICE]
     Flush 20

[INPUT]
    Name        forward
    Listen      0.0.0.0
    Port        24224

[OUTPUT]
    Name            kinesis
    Match           *
    region          us-west-2
    stream          my-kinesis-stream-name
    aggregation     true
    append_newline  true
```

### ZLIB Compression

Enabling `zlib` compression will compress each record individually reducing the network bandwidth required to send logs.  Using this feature in conjunction with `aggregation` can greatly reduce the number of Kinesis shards required.

Compression Advantages:

   - Reduces network bandwidth required
   - Reduces Kinesis shard count in some scenarios

Compression Disadvantages:

   - Fluentbit will require more CPU and memory to send records
   - A consumer must decompress the records


Example config:

```
[SERVICE]
     Flush 20

[INPUT]
    Name        forward
    Listen      0.0.0.0
    Port        24224

[OUTPUT]
    Name            kinesis
    Match           *
    region          us-west-2
    stream          my-kinesis-stream-name
    compression     zlib
    append_newline  true
```
