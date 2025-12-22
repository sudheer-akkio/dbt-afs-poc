{{ config(
    alias='FACT_TRANSACTION_ENRICHED',
    materialized='incremental',
    unique_key=['txid'],
    post_hook=[
        "alter table {{this}} cluster by (trans_date, AKKIO_ID)"
    ]
)}}

-- ============================================================================
-- FACT_TRANSACTION_ENRICHED: Enriched Transaction Fact Table
-- This adds AKKIO_ID to transactions for easy joining to attributes table
-- Includes merchant, brand, and location attributes
-- Joins 6 source tables: TRANSACTION, CARD, MERCHANT, BRAND_TAGGING,
-- BRAND_TAXONOMY, BRAND_LOCATION
-- ============================================================================

SELECT 
    -- Transaction Facts
    t.txid,
    t.trans_date,
    t.trans_time,
    t.trans_time_zone,
    t.trans_amount,
    t.delivery_date AS transaction_delivery_date,
    
    -- Individual ID (KEY: added for joining to attributes table)
    c.afs_individual_id AS AKKIO_ID,
    
    -- Card Info (keep for reference)
    t.membccid,
    c.card_type,
    c.card_zip,
    c.areaid,
    
    -- Merchant Attributes
    m.mtid,
    m.MERCH_DESC AS merchant_description,
    m.mcc AS merchant_category_code,
    m.merch_city AS merchant_city,
    m.merch_state AS merchant_state,
    m.merch_zip AS merchant_zip,
    m.merch_country AS merchant_country,
    
    -- Brand Tagging
    bt.store_id,
    bt.brand_id,
    bt.channel AS transaction_channel,  -- ONLINE or B&M
    bt.locationid,
    
    -- Brand Taxonomy
    btax.store_name,
    btax.brand_name,
    btax.store_type,
    btax.brand_type,
    btax.brand_tagging_classification,
    
    -- Brand Location
    bl.address AS store_address,
    bl.city AS store_city,
    bl.state AS store_state,
    bl.zip AS store_zip,
    bl.country AS store_country,
    bl.est_open_date AS store_open_date,
    bl.est_close_date AS store_close_date
    
FROM {{ source('afs_poc', 'TRANSACTION') }} t
-- Join to Card to get AKKIO_ID
LEFT JOIN {{ source('afs_poc', 'CARD') }} c 
    ON t.membccid = c.membccid
-- Join to Merchant
LEFT JOIN {{ source('afs_poc', 'MERCHANT') }} m 
    ON t.mtid = m.mtid
-- Join to Brand Tagging
LEFT JOIN {{ source('afs_poc', 'BRAND_TAGGING') }} bt 
    ON t.mtid = bt.mtid
-- Join to Brand Taxonomy
LEFT JOIN {{ source('afs_poc', 'BRAND_TAXONOMY') }} btax 
    ON bt.store_id = btax.store_id 
    AND bt.brand_id = btax.brand_id
-- Join to Brand Location
LEFT JOIN {{ source('afs_poc', 'BRAND_LOCATION') }} bl 
    ON bt.locationid = bl.locationid

-- ============================================================================
-- INCREMENTAL LOGIC: Process only new transactions
-- Supports both incremental mode and batch processing via vars
-- ============================================================================
WHERE t.trans_date IS NOT NULL
    {% if var('start_date', None) and var('end_date', None) %}
        -- Batch processing mode: use --vars '{"start_date": "2024-01-01", "end_date": "2024-01-31"}'
        AND t.trans_date BETWEEN '{{ var("start_date") }}' AND '{{ var("end_date") }}'
    {% elif is_incremental() %}
        -- Normal incremental mode: process only new data
        AND t.trans_date > (SELECT MAX(trans_date) FROM {{ this }})
    {% else %}
        -- Full refresh: Last 6 months of data based on trans_date
        -- AND t.trans_date >= DATEADD(month, -6, CURRENT_DATE())
    {% endif %}

-- To change the date range, modify the DATEADD function above:
--   - For 12 months: Change -6 to -12
--   - For 18 months: Change -6 to -18
--   - For full data: Comment out or remove the WHERE clause entirely

