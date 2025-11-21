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
    Grain: One row per AKKIO_HH_ID (household) per PARTITION_DATE
    
    Note: Since AKKIO_HH_ID = AKKIO_ID in the source, this is currently 1:1,
    but structured for future scenarios where multiple individuals may share a household.
*/

WITH household_data AS (
    SELECT
        PARTITION_DATE,
        AKKIO_HH_ID,
        HOUSEHOLD_INCOME_K,
        INCOME_BUCKET,
        HOMEOWNER_STATUS,
        NUMBER_OF_CHILDREN,
        PRESENCE_OF_CHILDREN,
        NUM_PEOPLE_IN_HOUSEHOLD_GROUP,
        CHILD_AGE_GROUP,
        MEDIAN_HOME_VALUE_BY_STATE,
        STATE_ABBR
    FROM {{ ref('v_akkio_attributes_latest') }}
)

SELECT
    -- Primary Keys
    PARTITION_DATE,
    AKKIO_HH_ID,
    
    -- Household Income
    HOUSEHOLD_INCOME_K AS INCOME,
    
    -- Income Bucket: Convert to integer codes (1-11) for insights
    -- 1: $0-35K, 2: $35-45K, 3: $45-55K, 4: $55-70K, 5: $70-85K,
    -- 6: $85-100K, 7: $100-125K, 8: $125-150K, 9: $150-200K, 10: $200K+, 11: Unknown
    CASE 
        WHEN INCOME_BUCKET IN ('1K_14K', '15K_24K', '25K_34K') THEN 1
        WHEN INCOME_BUCKET = '35K_49K' AND HOUSEHOLD_INCOME_K < 45 THEN 2
        WHEN INCOME_BUCKET = '35K_49K' THEN 3
        WHEN INCOME_BUCKET = '50K_74K' AND HOUSEHOLD_INCOME_K < 70 THEN 4
        WHEN INCOME_BUCKET = '50K_74K' THEN 5
        WHEN INCOME_BUCKET = '75K_99K' AND HOUSEHOLD_INCOME_K < 85 THEN 5
        WHEN INCOME_BUCKET = '75K_99K' THEN 6
        WHEN INCOME_BUCKET = '100K_124K' THEN 7
        WHEN INCOME_BUCKET = '125K_149K' THEN 8
        WHEN INCOME_BUCKET IN ('150K_174K', '175K_199K') THEN 9
        WHEN INCOME_BUCKET IN ('200K_249K', '250K_PLUS') THEN 10
        WHEN INCOME_BUCKET IN ('UNKNOWN', 'UNDETERMINED') OR HOUSEHOLD_INCOME_K = 0 THEN 11
        ELSE 11
    END AS INCOME_BUCKET,
    
    -- Child Age Group as OBJECT (key-value pairs where key is age group and value is 1 if present)
    -- OBJECT_CONSTRUCT automatically skips NULL keys
    CASE 
        WHEN CHILD_AGE_GROUP IS NULL OR CHILD_AGE_GROUP = '' THEN NULL
        ELSE OBJECT_CONSTRUCT(
            CASE WHEN CHILD_AGE_GROUP LIKE '%0-3%' THEN '0-3' END, CASE WHEN CHILD_AGE_GROUP LIKE '%0-3%' THEN 1 END,
            CASE WHEN CHILD_AGE_GROUP LIKE '%4-6%' THEN '4-6' END, CASE WHEN CHILD_AGE_GROUP LIKE '%4-6%' THEN 1 END,
            CASE WHEN CHILD_AGE_GROUP LIKE '%7-9%' THEN '7-9' END, CASE WHEN CHILD_AGE_GROUP LIKE '%7-9%' THEN 1 END,
            CASE WHEN CHILD_AGE_GROUP LIKE '%10-12%' THEN '10-12' END, CASE WHEN CHILD_AGE_GROUP LIKE '%10-12%' THEN 1 END,
            CASE WHEN CHILD_AGE_GROUP LIKE '%13-15%' THEN '13-15' END, CASE WHEN CHILD_AGE_GROUP LIKE '%13-15%' THEN 1 END,
            CASE WHEN CHILD_AGE_GROUP LIKE '%16-18%' THEN '16-18' END, CASE WHEN CHILD_AGE_GROUP LIKE '%16-18%' THEN 1 END
        )
    END AS CHILD_AGE_GROUP,
    
    -- Home Ownership (numeric 0/1: 1=Owner, 0=Renter, NULL=Unknown)
    CASE
        WHEN HOMEOWNER_STATUS = 'HOMEOWNER' THEN 1
        WHEN HOMEOWNER_STATUS = 'RENTER' THEN 0
        ELSE NULL
    END AS HOMEOWNER,
    
    -- Household Composition
    NUMBER_OF_CHILDREN,
    PRESENCE_OF_CHILDREN,
    NUM_PEOPLE_IN_HOUSEHOLD_GROUP AS NUM_PEOPLE_IN_HOUSEHOLD,
    NUM_PEOPLE_IN_HOUSEHOLD_GROUP,
    
    -- Median Home Value by State as OBJECT (state as key, value as median home value)
    CASE 
        WHEN STATE_ABBR IS NOT NULL AND MEDIAN_HOME_VALUE_BY_STATE IS NOT NULL THEN
            OBJECT_CONSTRUCT(STATE_ABBR, MEDIAN_HOME_VALUE_BY_STATE)
        ELSE NULL
    END AS MEDIAN_HOME_VALUE_BY_STATE,
    
    -- Household Weight (fixed at 1 as per specification)
    1 AS HH_WEIGHT

FROM household_data

