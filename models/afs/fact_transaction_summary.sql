{{ config(
    alias='FACT_TRANSACTION_SUMMARY',
    materialized='table',
    unique_key = ['trans_date', 'AKKIO_ID'],
    post_hook=[
        "alter table {{this}} cluster by (trans_date, akkio_id)"         
    ]
)}}

-- ============================================================================
-- FACT_TRANSACTION_SUMMARY: Daily Transaction Summary Table
-- Aggregates transaction detail by trans_date and AKKIO_ID for performance
-- Optimized for RAG engine queries that need summary-level data
-- Source: fact_transaction_enriched (detail table)
-- ============================================================================

WITH 
detail AS (SELECT * FROM {{ ref('fact_transaction_enriched') }} )
SELECT
    trans_date,
    AKKIO_ID,
    
    -- Transaction Metrics
    COUNT(*) AS transaction_count,
    SUM(trans_amount) AS total_transaction_amount,
    AVG(trans_amount) AS avg_transaction_amount,
    MIN(trans_amount) AS min_transaction_amount,
    MAX(trans_amount) AS max_transaction_amount,
    
    -- Time Attributes (most common)
    MODE(trans_time_zone) AS trans_time_zone,
    MAX(transaction_delivery_date) AS latest_delivery_date,
    MIN(transaction_delivery_date) AS earliest_delivery_date,
    
    -- Card Attributes (most common)
    MODE(card_type) AS card_type,
    MODE(card_zip) AS card_zip,
    MODE(areaid) AS areaid,
    
    -- Merchant Attributes (aggregated lists)
    string_agg(DISTINCT merchant_description, ',') AS merchant_description_str_list,
    string_agg(DISTINCT merchant_category_code, ',') AS merchant_category_code_str_list,
    string_agg(DISTINCT merchant_city, ',') AS merchant_city_str_list,
    string_agg(DISTINCT merchant_state, ',') AS merchant_state_str_list,
    string_agg(DISTINCT merchant_zip, ',') AS merchant_zip_str_list,
    string_agg(DISTINCT merchant_country, ',') AS merchant_country_str_list,
    
    -- Brand Attributes (aggregated lists)
    string_agg(DISTINCT store_name, ',') AS store_name_str_list,
    string_agg(DISTINCT brand_name, ',') AS brand_name_str_list,
    string_agg(DISTINCT store_type, ',') AS store_type_str_list,
    string_agg(DISTINCT brand_type, ',') AS brand_type_str_list,
    string_agg(DISTINCT brand_tagging_classification, ',') AS brand_tagging_classification_str_list,
    string_agg(DISTINCT transaction_channel, ',') AS transaction_channel_str_list,
    
    -- Store Location Attributes (aggregated lists)
    string_agg(DISTINCT store_city, ',') AS store_city_str_list,
    string_agg(DISTINCT store_state, ',') AS store_state_str_list,
    string_agg(DISTINCT store_zip, ',') AS store_zip_str_list,
    string_agg(DISTINCT store_country, ',') AS store_country_str_list,
    
    -- Unique Counts
    COUNT(DISTINCT mtid) AS unique_merchant_count,
    COUNT(DISTINCT store_id) AS unique_store_count,
    COUNT(DISTINCT brand_id) AS unique_brand_count,
    COUNT(DISTINCT merchant_category_code) AS unique_mcc_count,
    COUNT(DISTINCT transaction_channel) AS unique_channel_count
    
FROM
    detail
WHERE AKKIO_ID IS NOT NULL
GROUP BY
    trans_date, AKKIO_ID

