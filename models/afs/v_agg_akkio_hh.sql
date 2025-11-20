{{ config(
    materialized='table',
    post_hook=[    
        "alter table {{this}} cluster by (PARTITION_DATE, AKKIO_HH_ID)", 
    ]
)}}

/*
    AFS Household Aggregation Table
    
    Purpose: Household-level aggregation of demographic attributes for analytics.
    Source: v_akkio_attributes_latest
    Grain: One row per AKKIO_HH_ID (household)
    
    Note: Since AKKIO_HH_ID = AKKIO_ID in the source, this is currently 1:1,
    but structured for future scenarios where multiple individuals may share a household.
*/

SELECT
    -- Primary Key
    AKKIO_HH_ID,
    
    -- Weight (fixed at 11 as proxy)
    11 AS HH_WEIGHT,
    
    -- Home Ownership
    HOMEOWNER_STATUS AS HOMEOWNER,
    
    -- Household Income
    HOUSEHOLD_INCOME_K AS INCOME,
    INCOME_BUCKET,
    
    -- Household Wealth
    WEALTH,
    WEALTH_BUCKET,
    
    -- Household Composition
    ADULTS_IN_HH,
    CHILDREN,
    
    -- Geographic attributes
    STATE_ABBR,
    ZIP_CODE,
    CITY,
    COUNTY_NAME,
    
    -- Temporal
    PARTITION_DATE

FROM {{ ref('v_akkio_attributes_latest') }}

