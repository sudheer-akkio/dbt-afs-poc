-- ============================================================================
-- DATA LOADING: Copy data from S3 into tables
-- ============================================================================
-- This script loads data from S3 into the created tables
-- Run this after 02_create_tables.sql
-- ============================================================================

USE SCHEMA DEMO.AFS_POC;

-- Load Demographics from Parquet files
COPY INTO DEMO.AFS_POC.DEMOGRAPHICS
FROM @afs_s3_stage/files_from_affinity/2025-11-13/individual_demographic_spine/
FILE_FORMAT = parquet_format
PATTERN = '.*\.parquet'
MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
ON_ERROR = 'CONTINUE';

-- Load Brand Location from gzipped CSV files
COPY INTO DEMO.AFS_POC.BRAND_LOCATION (
  locationid, address, city, state, zip, country, est_open_date, est_close_date
)
FROM @afs_s3_stage/files_from_affinity/purchase_intelligence/eval/2025-11-10/brand_location/
FILE_FORMAT = gzip_csv_format
PATTERN = '.*\.gz'
ON_ERROR = 'CONTINUE';

-- Load Transactions
COPY INTO DEMO.AFS_POC.TRANSACTION (
  txid, mtid, membccid, trans_date, trans_time, 
  trans_time_zone, trans_amount, delivery_date
)
FROM @afs_s3_stage/files_from_affinity/purchase_intelligence/eval/2025-11-10/transaction/
FILE_FORMAT = gzip_csv_format
PATTERN = '.*\.gz'
ON_ERROR = 'CONTINUE';

-- Load Merchant data
COPY INTO DEMO.AFS_POC.MERCHANT (
  mtid, MERCH_DESC, mcc, merch_city, merch_state, 
  merch_zip, merch_country, delivery_date
)
FROM @afs_s3_stage/files_from_affinity/purchase_intelligence/eval/2025-11-10/merchant/
FILE_FORMAT = gzip_csv_format
PATTERN = '.*\.gz'
ON_ERROR = 'CONTINUE';

-- Load Card data
COPY INTO DEMO.AFS_POC.CARD (
  membccid, card_zip, card_type, areaid, afs_individual_id, delivery_date
)
FROM @afs_s3_stage/files_from_affinity/purchase_intelligence/eval/2025-11-10/card/
FILE_FORMAT = gzip_csv_format
PATTERN = '.*\.gz'
ON_ERROR = 'CONTINUE';

-- Load Brand Tagging data
COPY INTO DEMO.AFS_POC.BRAND_TAGGING (
  mtid, store_id, brand_id, channel, locationid
)
FROM @afs_s3_stage/files_from_affinity/purchase_intelligence/eval/2025-11-10/brand_tagging/
FILE_FORMAT = gzip_csv_format
PATTERN = '.*\.gz'
ON_ERROR = 'CONTINUE';

-- Load Brand Taxonomy data
COPY INTO DEMO.AFS_POC.BRAND_TAXONOMY (
  store_id, brand_id, store_name, brand_name, store_type, 
  brand_type, brand_tagging_classification
)
FROM @afs_s3_stage/files_from_affinity/purchase_intelligence/eval/2025-11-10/brand_taxonomy/
FILE_FORMAT = gzip_csv_format
PATTERN = '.*\.gz'
ON_ERROR = 'CONTINUE';

