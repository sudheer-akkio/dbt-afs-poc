# Data Loader Scripts

This directory contains SQL scripts for loading data from S3 into Snowflake. The scripts are organized in a logical execution order.

## Execution Order

Run these scripts in the following order:

1. **01_setup.sql** - Creates schema, S3 stage, and file formats
2. **02_create_tables.sql** - Creates all table definitions
3. **03_load_data.sql** - Loads data from S3 into tables
4. **04_verify.sql** - Verifies data loads and checks for errors

## Script Descriptions

### 01_setup.sql
- Creates the `DEMO.AFS_POC` schema
- Sets up the S3 stage (`afs_s3_stage`) with credentials
- Defines file formats:
  - `parquet_format` - For Parquet files (demographics)
  - `gzip_csv_format` - For GZIP-compressed CSV files (purchase intelligence data)
- Verifies access to S3 paths

### 02_create_tables.sql
- Creates all 7 tables based on the data dictionary:
  - `DEMOGRAPHICS` - Individual demographic information
  - `MERCHANT` - Merchant information
  - `CARD` - Card information
  - `TRANSACTION` - Transaction records
  - `BRAND_TAGGING` - Brand tagging relationships
  - `BRAND_LOCATION` - Store location information
  - `BRAND_TAXONOMY` - Brand and store taxonomy

### 03_load_data.sql
- Loads data from S3 into each table using `COPY INTO` statements
- Each load uses appropriate file format and error handling

### 04_verify.sql
- Lists available S3 subfolders
- Provides row counts for all loaded tables
- Checks COPY history for errors

## Usage

You can run these scripts individually in Snowflake, or combine them if needed:

```sql
-- Option 1: Run individually
-- Execute 01_setup.sql
-- Execute 02_create_tables.sql
-- Execute 03_load_data.sql
-- Execute 04_verify.sql

-- Option 2: Combine for a full run
-- Concatenate all files in order
```

## Notes

- All scripts assume you're working in the `DEMO.AFS_POC` schema
- File paths and dates may need to be updated based on your S3 structure
- AWS credentials are embedded in `01_setup.sql` - consider using Snowflake secrets/external functions for production

