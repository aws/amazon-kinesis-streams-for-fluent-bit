# Changelog

## 1.10.0
* Feature - Add support for building this plugin on Windows. *Note that this is only support in this plugin repo for Windows compilation.*

## 1.9.0
* Feature - Add timeout config for AWS SDK Go HTTP calls (#178)
* Bug - Fix message loss issue using concurrency feature with 0 retries (#179)

## 1.8.1
* Bug - Fix truncation issue after compression (#183)

## 1.8.0
* Feature - Add support for gzip compression of records (#162)

## 1.7.3
* Enhancement - Upgrade Go version to 1.17

## 1.7.2
* Bug - Fix aggregator size estimation (#155)
* Bug - Fix partition key computation for aggregation (#158)

## 1.7.0
* Feature - Add new option replace_dots to replace dots in key names (#79)
* Enhancement - Add support for nested partition_key in log record (#30)
* Enhancement - Change the log severity from `info` to `debug` for aggregation log statements (#78)

## 1.6.1
* Bug - Truncate records to max size (#74)

## 1.6.0
* Feature - Add support for zlib compression of records (#26)
* Feature - Add KPL aggregation support (#16)

## 1.5.0
* Feature - Add log_key option for kinesis output plugin (#40)

## 1.4.0
* Feature - Add sts_endpoint param for custom STS API endpoint (#39)

## 1.3.0
* Feature - Add experimental concurrency feature (#33)

## 1.2.2
* Bug - Remove exponential backoff code (#27)

## 1.2.1
* Bug Fix - Updated logic to calculate the individual and maximum record size (#22)

## 1.2.0
* Feature - Add time_key and time_key_format config options to add timestamp to records (#17)

## 1.1.0
* Feature - Support IAM Roles for Service Accounts in Amazon EKS (#6)
* Enhancement - Change the log severity from `error` to `warning` for retryable API errors (#7)

## 1.0.0
Initial release of the Amazon Kinesis Streams for Fluent Bit Plugin
