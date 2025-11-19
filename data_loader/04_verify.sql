-- ============================================================================
-- VERIFICATION: Check data loads and identify any errors
-- ============================================================================
-- This script verifies that data was loaded successfully
-- Run this after 03_load_data.sql
-- ============================================================================

USE SCHEMA DEMO.AFS_POC;

-- List all subfolders to see what's available
LIST @afs_s3_stage/files_from_affinity/purchase_intelligence/eval/2025-11-10/;

-- Verify row counts for all loaded tables
SELECT 'DEMOGRAPHICS' AS table_name, COUNT(*) AS row_count FROM DEMO.AFS_POC.DEMOGRAPHICS
UNION ALL
SELECT 'BRAND_LOCATION', COUNT(*) FROM DEMO.AFS_POC.BRAND_LOCATION
UNION ALL
SELECT 'MERCHANT', COUNT(*) FROM DEMO.AFS_POC.MERCHANT
UNION ALL
SELECT 'CARD', COUNT(*) FROM DEMO.AFS_POC.CARD
UNION ALL
SELECT 'TRANSACTION', COUNT(*) FROM DEMO.AFS_POC.TRANSACTION
UNION ALL
SELECT 'BRAND_TAGGING', COUNT(*) FROM DEMO.AFS_POC.BRAND_TAGGING
UNION ALL
SELECT 'BRAND_TAXONOMY', COUNT(*) FROM DEMO.AFS_POC.BRAND_TAXONOMY;

-- Check for any load errors in the COPY history
-- Note: This checks DEMOGRAPHICS as an example. You may want to check other tables too.
SELECT 
  TABLE_NAME,
  STATUS,
  ROW_COUNT,
  ROW_PARSED,
  FIRST_ERROR_MESSAGE,
  FIRST_ERROR_LINE_NUMBER
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'DEMO.AFS_POC.DEMOGRAPHICS',
  START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
));

