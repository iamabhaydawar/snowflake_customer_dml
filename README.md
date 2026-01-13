![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)

# Customer Data DML - Snowflake Data Model Loading Medallion/ Matillion Architecture

## Overview

This repository contains SQL scripts and data files for loading customer data into Snowflake. It is a dedicated project for customer data ingestion and dimensional modeling in Snowflake, focusing on creating and populating the customer dimension table. Additionally, it includes comprehensive Snowflake Snowpipe implementation for automated data ingestion from Google Cloud Storage (GCS) to Snowflake using Pub/Sub notifications.

## Project Description

This project provides a complete workflow for:
- Creating the necessary Snowflake database and schema structure
- Defining the customer dimension table schema
- Loading customer data from CSV files into Snowflake
- Setting up Snowpipe for automated data ingestion from Google Cloud Storage (GCS)
- Implementing scheduled tasks for automated data transformation pipelines

## Features

- **Automated Data Ingestion**: Snowpipe automatically loads data from GCS when new files arrive
- **Cloud Storage Integration**: Secure integration between Snowflake and Google Cloud Storage
- **Pub/Sub Notifications**: Real-time event-driven data loading using GCP Pub/Sub
- **CSV File Processing**: Handles CSV files with automatic schema mapping
- **Monitoring & Management**: Built-in commands to monitor pipe status and data loading history

## Repository Structure

```
customer-data-dml/
├── README.md                      # Project documentation
├── steps.sql                      # Snowflake DDL and setup scripts (customer data & Snowpipe)
├── snowpipe_testing.sql           # Snowpipe setup for automated data ingestion from GCS
├── schedule_task.sql               # Task scheduling for automated data transformation pipeline
├── customer_10k_good_data.csv     # Customer data file (10,000 records)
└── data/                          # Sample CSV data files for orders
    ├── orders_20231210.csv
    ├── orders_20231211.csv
    ├── orders_20241228.csv
    └── orders_20241229.csv
```

## Prerequisites

- Snowflake account with ACCOUNTADMIN role access (for Snowpipe setup)
- Access to create databases, schemas, and tables
- SnowSQL or Snowflake Web UI for executing SQL commands
- Google Cloud Platform (GCP) account (for Snowpipe)
- GCS bucket created (`snowpipe-raw-data-gds` in this example)
- GCP Pub/Sub topic and subscription configured

## Getting Started

### Step 1: Execute Setup Script

Run the `steps.sql` file in your Snowflake environment. This script contains:

#### Customer Data Setup:
1. Create the `gds_dev` database
2. Create the `dims` schema
3. Create the `dims.customers` table with the following structure:
   - `customer_pk` (NUMBER) - Primary key
   - `salutation` (VARCHAR)
   - `first_name` (VARCHAR)
   - `last_name` (VARCHAR)
   - `gender` (VARCHAR)
   - `marital_status` (VARCHAR)
   - `day_of_birth` (DATE)
   - `birth_country` (VARCHAR)
   - `email_address` (VARCHAR)
   - `city_name` (VARCHAR)
   - `zip_code` (VARCHAR)
   - `country_name` (VARCHAR)
   - `gmt_timezone_offset` (NUMBER)
   - `preferred_cust_flag` (BOOLEAN)
   - `registration_time` (TIMESTAMP_LTZ)

#### Snowpipe Setup:
1. **Create Database**: `snowpipe_dev`
2. **Create Table**: `orders_data_lz` with schema:
   - `order_id` (INT)
   - `product` (VARCHAR)
   - `quantity` (INT)
   - `order_status` (VARCHAR)
   - `order_date` (DATE)
3. **Create Storage Integration**: `gcs_bucket_read_int`
4. **Create Stage**: `snowpipe_stage`
5. **Create Notification Integration**: `notification_from_pubsub_int`
6. **Create Snowpipe**: `gcs_to_snowflake_pipe`

### Step 2: Load Customer Data

After creating the table structure, load the customer data from `customer_10k_good_data.csv` using Snowflake's data loading capabilities:

#### Option A: Using Snowflake Web UI
1. Navigate to the `dims.customers` table
2. Click "Load Data"
3. Upload `customer_10k_good_data.csv`
4. Configure the file format (CSV with header row)
5. Execute the load

#### Option B: Using COPY INTO Command
```sql
USE DATABASE gds_dev;
USE SCHEMA dims;

CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  RECORD_DELIMITER = '\n'
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = FALSE
  ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
  ESCAPE = 'NONE'
  ESCAPE_UNENCLOSED_FIELD = '\134'
  DATE_FORMAT = 'AUTO'
  TIMESTAMP_FORMAT = 'AUTO'
  NULL_IF = ('NULL', 'null', '')
  COMMENT = 'CSV format for customer data';

-- Create stage (if using internal stage)
CREATE OR REPLACE STAGE customer_stage
  FILE_FORMAT = csv_format;

-- Upload file to stage (via SnowSQL or Web UI)
-- Then load data
COPY INTO dims.customers
FROM @customer_stage/customer_10k_good_data.csv
FILE_FORMAT = csv_format
ON_ERROR = 'ABORT_STATEMENT';
```

### Step 3: Verify Data

After loading, verify the data was loaded correctly:

```sql
SELECT COUNT(*) FROM dims.customers;
SELECT * FROM dims.customers LIMIT 10;
```

## Data File Details

### Customer Data
- **File**: `customer_10k_good_data.csv`
- **Records**: ~10,000 customer records
- **Format**: CSV with header row
- **Encoding**: UTF-8

### Orders Data
- **Files**: `data/orders_*.csv`
- **Format**: CSV files with the following structure:
```csv
order_id,product,quantity,order_status,order_date
1,Keyboard,2,Completed,2023-08-07
2,Mouse,1,Pending,2023-08-07
```

## Database Schema

### Customer Data
- **Database**: `gds_dev`
- **Schema**: `dims`
- **Table**: `customers`

### Orders Data
- **Database**: `snowpipe_dev`
- **Table**: `orders_data_lz`

## Snowpipe Setup

The `snowpipe_testing.sql` file contains scripts for setting up automated data ingestion using Snowpipe with Google Cloud Storage (GCS). This includes:

- **Storage Integration**: Configures secure access to GCS buckets
- **External Stage**: Creates a stage pointing to GCS bucket location
- **Notification Integration**: Sets up Pub/Sub integration for event-driven ingestion
- **Snowpipe**: Creates an automated pipe that loads data from GCS to Snowflake tables

### Snowpipe Components

1. **Storage Integration** (`gcs_bucket_read_int`): Enables secure access to GCS bucket `snowpipe_raw_data_ds`
2. **External Stage** (`snowpipe_stage`): References the GCS bucket location
3. **Notification Integration** (`notification_from_pubsub_int`): Connects to GCP Pub/Sub for event notifications
4. **Snowpipe** (`gcs_to_snowflake_pipe`): Automatically ingests CSV files from GCS into `orders_data_lz` table

### Prerequisites for Snowpipe

- Google Cloud Storage bucket with appropriate permissions
- GCP Pub/Sub topic and subscription configured
- Service accounts with required IAM roles:
  - Storage: `storage.buckets.list`, `storage.objects.get`, `storage.objects.list`
  - Pub/Sub: `Pub/Sub Subscriber` role
- Snowflake ACCOUNTADMIN role for creating integrations

### Google Cloud Storage Setup

Create a GCS bucket and configure Pub/Sub notifications:

```bash
# Create Pub/Sub topic
gcloud pubsub topics create gcs-to-pubsub-notification

# Create Pub/Sub subscription
gcloud pubsub subscriptions create gcs-to-pubsub-notification-sub \
  --topic=gcs-to-pubsub-notification

# Create GCS notification
gsutil notification create -t gcs-to-pubsub-notification -f json \
  gs://snowpipe-raw-data-gds/
```

### GCP IAM Configuration

Grant necessary permissions to Snowflake service accounts:

1. **Storage Integration Service Account**: Grant `Storage Object Viewer` role on the GCS bucket
2. **Pub/Sub Integration Service Account**: Grant `Pub/Sub Subscriber` role on the subscription

### Usage

1. Execute `snowpipe_testing.sql` in Snowflake (requires ACCOUNTADMIN role)
2. Configure GCS bucket notifications to publish to Pub/Sub topic
3. Upload CSV files to the GCS bucket:
```bash
gsutil cp orders_20231210.csv gs://snowpipe-raw-data-gds/
```
4. Snowpipe will automatically detect and load new files into the `orders_data_lz` table

### Monitor Snowpipe Status

```sql
-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('gcs_to_snowflake_pipe');

-- View copy history
SELECT * 
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME=>'orders_data_lz', 
  START_TIME=> DATEADD(hours, -1, CURRENT_TIMESTAMP())
));

-- Query loaded data
SELECT * FROM orders_data_lz;
```

### Manage Snowpipe

```sql
-- Pause Snowpipe
ALTER PIPE gcs_to_snowflake_pipe SET PIPE_EXECUTION_PAUSED = true;

-- Resume Snowpipe
ALTER PIPE gcs_to_snowflake_pipe SET PIPE_EXECUTION_PAUSED = false;

-- Drop Snowpipe
DROP PIPE gcs_to_snowflake_pipe;
```

## Task Scheduling

The `schedule_task.sql` file implements a medallion architecture (Bronze-Silver-Gold) data transformation pipeline using Snowflake Tasks. This demonstrates automated ETL workflows with scheduled execution and task dependencies.

### Architecture Overview

The script implements a three-layer data architecture:

1. **Bronze Layer** (`raw_transactions`): Raw transaction data ingestion
2. **Silver Layer** (`filtered_transactions`): Filtered and cleaned transaction data
3. **Gold Layer** (`aggregated_transactions`): Aggregated business metrics

### Task Components

1. **filter_transactions_task**: 
   - Scheduled to run every 1 minute
   - Performs MERGE operation from `raw_transactions` to `filtered_transactions`
   - Filters transactions with status 'completed' or 'refunded'
   - Transforms timestamp to date format

2. **aggregate_transactions_task**:
   - Runs after `filter_transactions_task` completes (task dependency)
   - Aggregates filtered transactions by date
   - Calculates total quantity, completed transactions, and refunded transactions
   - Performs MERGE operation into `aggregated_transactions` table

### Task Dependencies

The tasks are configured with dependencies:
- `aggregate_transactions_task` depends on `filter_transactions_task`
- Ensures data flows sequentially through the pipeline

### Usage

1. Execute `schedule_task.sql` in Snowflake
2. Tasks are created in SUSPENDED state by default
3. Resume tasks using:
   ```sql
   ALTER TASK filter_transactions_task RESUME;
   ALTER TASK aggregate_transactions_task RESUME;
   ```
4. Monitor task execution history:
   ```sql
   SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME=>'filter_transactions_task'));
   SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME=>'aggregate_transactions_task'));
   ```

### Prerequisites

- Snowflake warehouse (`COMPUTE_WH`) must exist
- Appropriate privileges to create tasks and tables
- Tasks require warehouse resources for execution

## Key Components

### Storage Integration
Provides secure, credential-free access to GCS buckets without storing cloud provider credentials in Snowflake.

### Stage
A named location that points to your GCS bucket where data files are stored before loading.

### Notification Integration
Enables Snowpipe to receive notifications from GCP Pub/Sub when new files are uploaded to the bucket.

### Snowpipe
The automated data ingestion service that loads data from the stage into the target table when triggered by Pub/Sub notifications.

## Troubleshooting

- **Pipe not loading data**: Check pipe status and ensure notifications are properly configured
- **Permission errors**: Verify IAM roles are correctly assigned to Snowflake service accounts
- **File format issues**: Ensure CSV files match the expected format and encoding

## Notes

- This is a separate customer data loading project specifically for Snowflake
- The SQL scripts are designed to be executed in sequence
- Ensure you have the necessary permissions before executing the scripts
- The data files contain sample data for testing and development purposes

## Support

For issues or questions related to this project, please refer to the Snowflake documentation or contact your database administrator.

## License

MIT License

## Author

Created as part of a Snowflake Snowpipe implementation project.
