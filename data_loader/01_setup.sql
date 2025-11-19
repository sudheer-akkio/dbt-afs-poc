-- ============================================================================
-- SETUP: Schema, Stage, and File Formats
-- ============================================================================
-- This script sets up the infrastructure needed for loading data from S3
-- Run this first before creating tables or loading data
-- ============================================================================

-- Step 1: Create the schema
CREATE SCHEMA IF NOT EXISTS DEMO.AFS_POC;
USE SCHEMA DEMO.AFS_POC;

-- Step 2: Create the external stage with your S3 credentials
-- NOTE: Replace the placeholders below with your actual AWS credentials
-- For security, use environment variables or Snowflake's credential management
-- Metadata:
--   - S3 Bucket Name: afs-akkio
--   - Bucket Region: us-east-1
CREATE OR REPLACE STAGE afs_s3_stage
  URL = 's3://afs-akkio/'
  CREDENTIALS = (
    AWS_KEY_ID = 'YOUR_AWS_ACCESS_KEY_ID'
    AWS_SECRET_KEY = 'YOUR_AWS_SECRET_ACCESS_KEY'
  );

-- Step 3: Create file formats for different file types

-- Parquet file format for demographics
CREATE OR REPLACE FILE FORMAT parquet_format
  TYPE = 'PARQUET';

-- Gzip compressed CSV format for purchase intelligence data
CREATE OR REPLACE FILE FORMAT gzip_csv_format
  TYPE = 'CSV'
  COMPRESSION = 'GZIP'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  FIELD_DELIMITER = ',';

-- Gzip compressed TSV (tab-separated) format for brand_location and other tab-delimited files
-- Data Format Rules:
--   - Compression: GZIP
--   - Field Delimiter: Tab (\t)
--   - Column Header: No (SKIP_HEADER = 0 means no header row to skip)
CREATE OR REPLACE FILE FORMAT gzip_tsv_format
  TYPE = 'CSV'
  COMPRESSION = 'GZIP'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 0
  FIELD_DELIMITER = '\t';

-- Step 4: Verify access to the files
LIST @afs_s3_stage/files_from_affinity/;

