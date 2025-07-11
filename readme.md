# Bolt Trip Data Pipeline and KPI Reporting

## Overview

This project implements a real-time data streaming and batch KPI reporting pipeline for trip data using AWS services:

- **Data ingestion:** Local CSV data is streamed to **Amazon Kinesis Data Streams**.
- **Real-time processing:** A **Lambda function** is triggered by Kinesis to validate and write trip events to **DynamoDB**.
- **Batch KPI calculation:** A scheduled **AWS Glue Python Shell job** runs daily at 11:59 PM UTC to compute KPIs from DynamoDB and write JSON reports to **Amazon S3**.


## Architecture and AWS Service Choices

| Component | AWS Service | Justification |
| :-- | :-- | :-- |
| Data streaming | Amazon Kinesis Data Streams | High-throughput, scalable, ordered streaming for real-time ingestion of trip events. |
| Real-time processing | AWS Lambda | Serverless, event-driven processing of streaming data with automatic scaling and low latency. |
| Storage and querying | Amazon DynamoDB | NoSQL database with fast, scalable key-value access, perfect for storing trip event state. |
| Batch processing \& KPIs | AWS Glue Python Shell Job | Cost-effective, serverless Python environment for batch processing and KPI calculation. |
| KPI output storage | Amazon S3 | Durable, cost-effective storage for JSON KPI reports with easy integration for analytics tools. |


This architecture follows a **Lambda + Kinesis streaming pattern** for real-time ingestion and uses **Glue scheduled batch jobs** for complex aggregation and reporting — a common, scalable, and cost-effective design pattern in AWS data pipelines[^1][^2][^3].

![alt text](images/Architecture.svg)

## DynamoDB Schema and Data Integrity

### Table: `bolt`

| Attribute Name | Type | Description | Notes on Validation and Integrity |
| :-- | :-- | :-- | :-- |
| `trip_id` | String | Partition key, unique trip identifier | Must be present in all events |
| `pickup_location_id` | Number | Start location ID (trip start event only) | Validated as integer; required for start events |
| `dropoff_location_id` | Number | End location ID (trip start event only) | Validated as integer; required for start events |
| `vendor_id` | Number | Vendor ID (trip start event only) | Validated as integer |
| `pickup_datetime` | String | ISO 8601 datetime string (trip start) | Validated format; used as last_updated timestamp |
| `estimated_dropoff_datetime` | String | ISO 8601 datetime string (trip start) | Validated format |
| `estimated_fare_amount` | Number | Estimated fare (trip start) | Validated as decimal |
| `dropoff_datetime` | String | ISO 8601 datetime string (trip end event) | Validated format; used as last_updated timestamp |
| `rate_code` | Number | Rate code (trip end event) | Validated as decimal |
| `passenger_count` | Number | Number of passengers (trip end event) | Validated as decimal |
| `trip_distance` | Number | Distance traveled (trip end event) | Validated as decimal |
| `fare_amount` | Number | Actual fare amount (trip end event) | Validated as decimal |
| `tip_amount` | Number | Tip amount (trip end event) | Validated as decimal |
| `payment_type` | Number | Payment type code (trip end event) | Validated as decimal |
| `trip_type` | Number | Trip type code (trip end event) | Validated as decimal |
| `trip_date` | String | Date extracted from `dropoff_datetime` | Used for KPI grouping |
| `trip_completed` | Boolean | Flag indicating if trip start and end events received | Set true when both events processed |
| `last_updated` | String | ISO 8601 timestamp of last update | Used to ensure only newer data overwrites older |

### Data Validation and Referential Integrity

- Incoming records are validated for **required fields**, **data types**, and **ISO 8601 datetime formats**.
- The Lambda function enforces **schema conformity** for start and end trip events separately.
- Updates to DynamoDB use a **conditional write** based on `last_updated` to prevent stale data overwrites.
- `trip_completed` is set only when both start and end events have been received.
- `trip_date` is extracted from `dropoff_datetime` to enable consistent KPI aggregation.
- Missing or null fields cause validation errors and are logged but do not crash the pipeline.

This design ensures **referential integrity** between start and end events keyed by `trip_id` and prevents inconsistent or partial data from corrupting KPIs[^4].

## Simulation

### 1. Prerequisites

- AWS CLI configured with appropriate permissions.
- Python 3.10+ installed locally.
- AWS resources created: Kinesis stream (`bolt_streams`), DynamoDB table (`bolt`), Lambda function, Glue job, and S3 bucket for KPI output.


### 2. Stream Data from Local CSV to Kinesis

Run the provided `stream_data.py` script to simulate streaming trip start and end events interleaved from CSV files:

```bash
python stream_data.py
```

This script:

- Reads `trip_start.csv` and `trip_end.csv`.
- Interleaves rows to simulate real-time event order.
- Sends JSON records to Kinesis with `trip_id` as partition key.


### 3. Lambda Function Processing

The Lambda function is triggered by Kinesis events:

- Decodes and validates each record.
- Updates DynamoDB with start or end event data.
- Maintains data integrity using conditional writes and timestamps.
- Marks trips as completed when both events are received.


### 4. Daily KPI Calculation with Glue

A Glue Python Shell job runs daily at 11:59 PM UTC:

- Scans the entire DynamoDB table.
- Calculates KPIs per `trip_date`: total trips, total fare, min/max/average fare.
- Writes KPI JSON files to S3 under `kpi_output/date=YYYY-MM-DD/kpi.json`.


### 5. Deployment and Scheduling

- Use the provided **GitHub Actions workflow** to deploy Lambda code and Glue job scripts.
- Glue job is scheduled via AWS Glue’s built-in scheduler with cron expression `59 23 * * ? *` (daily 11:59 PM UTC).



## Summary

This architecture leverages AWS managed services for a **scalable, reliable, and cost-effective** pipeline:

- **Kinesis + Lambda** for real-time ingestion and validation.
- **DynamoDB** for fast, schema-enforced storage with conditional writes.
- **Glue Python Shell** for scheduled batch KPI aggregation.
- **S3** for durable KPI storage and downstream analytics.

The design ensures **data quality, schema conformity, and referential integrity** while providing clear separation of concerns and easy extensibility.



