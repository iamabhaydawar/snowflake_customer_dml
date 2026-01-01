# Customer Data DML - Snowflake Data Model Loading Worksheet

## Overview

This repository contains SQL scripts and data files for loading customer data into Snowflake. It is a dedicated project for customer data ingestion and dimensional modeling in Snowflake, focusing on creating and populating the customer dimension table.

## Project Description

This project provides a complete workflow for:
- Creating the necessary Snowflake database and schema structure
- Defining the customer dimension table schema
- Loading customer data from CSV files into Snowflake

## Repository Structure

```
customer-data-dml/
├── README.md                      # Project documentation
├── steps.sql                      # Snowflake DDL and setup scripts
└── customer_10k_good_data.csv     # Customer data file (10,000 records)
```

## Prerequisites

- Snowflake account with appropriate privileges
- Access to create databases, schemas, and tables
- SnowSQL or Snowflake Web UI for executing SQL commands

## Getting Started

### Step 1: Execute Setup Script

Run the `steps.sql` file in your Snowflake environment. This script will:

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

### Step 2: Load Data

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

- **File**: `customer_10k_good_data.csv`
- **Records**: ~10,000 customer records
- **Format**: CSV with header row
- **Encoding**: UTF-8

## Database Schema

- **Database**: `gds_dev`
- **Schema**: `dims`
- **Table**: `customers`

## Notes

- This is a separate customer data loading project specifically for Snowflake
- The SQL scripts are designed to be executed in sequence
- Ensure you have the necessary permissions before executing the scripts
- The data file contains sample customer data for testing and development purposes

## Support

For issues or questions related to this project, please refer to the Snowflake documentation or contact your database administrator.

## License

This project is for internal use and data loading purposes.

