# Changelog

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
