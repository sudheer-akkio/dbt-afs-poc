-- ============================================================================
-- DATA LOADING: Copy data from S3 into tables
-- ============================================================================
-- This script loads data from S3 into the created tables
-- Run this after 02_create_tables.sql
--
-- Data Format Rules (Akkio Eval):
--   - S3 Bucket: afs-akkio
--   - Bucket Region: us-east-1
--   - Data Format: gzip compressed (.gz files)
--   - Field Delimiter: tab delimited (\t)
--   - Column Header: No (files have no header row)
--   - Trans Dates: 2023-10-01 to 2025-09-30
-- ============================================================================

USE SCHEMA DEMO.AFS_POC;

-- Load Demographics from Parquet files
COPY INTO DEMO.AFS_POC.INDIVIDUAL_DEMOGRAPHIC_SPINE
FROM @afs_s3_stage/files_from_affinity/2025-11-13/individual_demographic_spine/
FILE_FORMAT = parquet_format
PATTERN = '.*\.parquet'
MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
ON_ERROR = 'CONTINUE';