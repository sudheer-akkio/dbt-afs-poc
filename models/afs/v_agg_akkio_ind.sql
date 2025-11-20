{{ config(
    materialized='table',
    post_hook=[    
        "alter table {{this}} cluster by (PARTITION_DATE, AKKIO_ID)", 
    ]
)}}

/*
    AFS Individual Aggregation Table
    
    Purpose: Individual-level aggregation of demographic attributes for analytics.
    Source: v_akkio_attributes_latest
    Grain: One row per AKKIO_ID (individual)
    
    Note: Currently AKKIO_HH_ID = AKKIO_ID (1:1), but structured for future scenarios
    where multiple individuals may share a household.
*/

SELECT
    -- Primary Keys
    attr.AKKIO_ID,
    attr.AKKIO_HH_ID,
    
    -- Weight (fixed at 11 per requirements)
    11 AS WEIGHT,
    
    -- Demographics (convert NULL to 'UNDETERMINED' to match Horizon schema for insights compatibility)
    COALESCE(attr.GENDER, 'UNDETERMINED') AS GENDER,
    attr.AGE,
    attr.AGE_BUCKET,
    attr.ETHNICITY,
    attr.EDUCATION_LEVEL,
    attr.MARITAL_STATUS,
    
    -- Household-level attributes (needed for audience queries - same as Horizon's V_AGG_BLU_IND)
    attr.HOMEOWNER_STATUS AS HOMEOWNER,
    attr.HOUSEHOLD_INCOME_K AS INCOME,
    attr.INCOME_BUCKET,
    attr.WEALTH,
    attr.WEALTH_BUCKET,
    
    -- Political and Business Attributes
    attr.POLITICS,
    attr.POLITICS_NORMALIZED,
    attr.BUSINESS_OWNER,
    attr.BUSINESS_OWNER_FLAG,
    
    -- Household Composition
    attr.ADULTS_IN_HH,
    attr.CHILDREN,
    
    -- Contact identifiers (counts, not arrays, for insights compatibility)
    -- Placeholders for potential future enrichment from additional data sources
    0 AS MAIDS,
    0 AS IPS,
    0 AS EMAILS,
    0 AS PHONES,
    
    -- Geographic attributes
    attr.STATE_ABBR,
    attr.ZIP_CODE,
    attr.CITY,
    attr.COUNTY_NAME,
    
    -- Market Area (CBSA - Core Based Statistical Area)
    attr.CBSA_CODE,
    attr.CBSA_TYPE,
    attr.METRO_FLAG,
    attr.MARKET_AREA_TYPE,
    
    -- Additional core demographics (original fields available if needed: et_grp_1, et_code_1, educ_model_1, yearofbirth_1)
    attr.EDUCATION_MODEL,
    attr.YEAR_OF_BIRTH,
    
    -- Temporal
    attr.PARTITION_DATE

FROM {{ ref('v_akkio_attributes_latest') }} attr

