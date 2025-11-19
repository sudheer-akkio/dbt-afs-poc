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

-- Step 4: Verify access to the files
LIST @afs_s3_stage/files_from_affinity/;

