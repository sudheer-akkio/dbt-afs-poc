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
    Grain: One row per AKKIO_ID (individual) per PARTITION_DATE
    
    Note: Currently AKKIO_HH_ID = AKKIO_ID (1:1), but structured for future scenarios
    where multiple individuals may share a household.
*/

SELECT
    -- Primary Keys
    attr.AKKIO_ID,
    attr.AKKIO_HH_ID,

    1 as WEIGHT,
    
    -- Demographics
    CASE 
        WHEN attr.GENDER = 'MALE' THEN 'M'
        WHEN attr.GENDER = 'FEMALE' THEN 'F'
        ELSE 'UNDETERMINED'
    END AS GENDER,
    attr.ZIP_CODE,
    attr.AGE,
    
    -- Age Bucket: Convert to integer codes (1-7) for insights
    -- 1: 18-24, 2: 25-34, 3: 35-44, 4: 45-54, 5: 55-64, 6: 65-74, 7: 75+
    CASE 
        WHEN attr.AGE_BUCKET = '19-24' THEN 1
        WHEN attr.AGE_BUCKET IN ('25-29', '30-34') THEN 2
        WHEN attr.AGE_BUCKET IN ('35-39', '40-44') THEN 3
        WHEN attr.AGE_BUCKET IN ('45-49', '50-54') THEN 4
        WHEN attr.AGE_BUCKET = '55-59' THEN 5
        WHEN attr.AGE_BUCKET = '60+' AND attr.AGE IS NOT NULL THEN
            CASE 
                WHEN attr.AGE BETWEEN 60 AND 74 THEN 6
                WHEN attr.AGE >= 75 THEN 7
                ELSE 6
            END
        WHEN attr.AGE_BUCKET = '60+' THEN 6
        ELSE 0 -- no null allowed for age 
    END AS AGE_BUCKET,
    
    -- Age Bucket Detailed: More granular age buckets (1-12)
    -- 1: 19-24, 2: 25-29, 3: 30-34, 4: 35-39, 5: 40-44, 6: 45-49, 7: 50-54, 8: 55-59, 9: 60-64, 10: 65-69, 11: 70-74, 12: 75+
    CASE 
        WHEN attr.AGE_BUCKET = '19-24' THEN 1
        WHEN attr.AGE_BUCKET = '25-29' THEN 2
        WHEN attr.AGE_BUCKET = '30-34' THEN 3
        WHEN attr.AGE_BUCKET = '35-39' THEN 4
        WHEN attr.AGE_BUCKET = '40-44' THEN 5
        WHEN attr.AGE_BUCKET = '45-49' THEN 6
        WHEN attr.AGE_BUCKET = '50-54' THEN 7
        WHEN attr.AGE_BUCKET = '55-59' THEN 8
        WHEN attr.AGE_BUCKET = '60+' AND attr.AGE IS NOT NULL THEN
            CASE 
                WHEN attr.AGE BETWEEN 60 AND 64 THEN 9
                WHEN attr.AGE BETWEEN 65 AND 69 THEN 10
                WHEN attr.AGE BETWEEN 70 AND 74 THEN 11
                WHEN attr.AGE >= 75 THEN 12
                ELSE 9
            END
        WHEN attr.AGE_BUCKET = '60+' THEN 9
        ELSE 0
    END AS AGE_BUCKET_DETAILED,
    
    attr.ETHNICITY AS ETHNICITY_PREDICTION,
    -- Education: Single value mapped to Education class (title case with spaces)
    CASE
        WHEN attr.EDUCATION_LEVEL = 'COMPLETED_COLLEGE' THEN 'College'
        WHEN attr.EDUCATION_LEVEL = 'SOME_COLLEGE' THEN 'Some College'
        WHEN attr.EDUCATION_LEVEL = 'HIGH_SCHOOL_DIPLOMA' THEN 'High School'
        WHEN attr.EDUCATION_LEVEL = 'GRADUATE_DEGREE' THEN 'Graduate'
        ELSE 'Unknown'
    END AS EDUCATION,
    attr.MARITAL_STATUS,
    attr.STATE AS STATE,
    attr.OCCUPATION,
    
    -- Contact identifiers (placeholders for future enrichment)
    0 AS EMAILS,
    0 AS MAIDS,
    0 AS PHONES,
    0 AS IPS,
    
    -- Interests as OBJECT (percentile < 50 means likely to have interest)
    OBJECT_CONSTRUCT(
        CASE WHEN TRY_CAST(attr.GENERAL_INTERESTS_ARTS_CRAFTS AS INT) < 50 THEN 'arts_and_crafts' END, 1,
        CASE WHEN TRY_CAST(attr.GENERAL_INTERESTS_BOOK_READER AS INT) < 50 THEN 'book_reader' END, 1,
        CASE WHEN TRY_CAST(attr.GENERAL_INTERESTS_CULTURAL_ARTS AS INT) < 50 THEN 'cultural_arts' END, 1,
        CASE WHEN TRY_CAST(attr.GENERAL_INTERESTS_DIY AS INT) < 50 THEN 'do_it_yourselfers' END, 1,
        CASE WHEN TRY_CAST(attr.GENERAL_INTERESTS_GOURMET_COOKING AS INT) < 50 THEN 'gourmet_cooking' END, 1,
        CASE WHEN TRY_CAST(attr.GENERAL_INTERESTS_HOME_IMPROVEMENT AS INT) < 50 THEN 'home_improvement' END, 1,
        CASE WHEN TRY_CAST(attr.GENERAL_INTERESTS_PHOTOGRAPHY AS INT) < 50 THEN 'photography' END, 1,
        CASE WHEN TRY_CAST(attr.GENERAL_INTERESTS_SCRAPBOOKING AS INT) < 50 THEN 'scrapbooking' END, 1
    ) AS GENERAL_INTERESTS,
    
    OBJECT_CONSTRUCT(
        CASE WHEN TRY_CAST(attr.SPORTS_INTERESTS_RUNNING AS INT) < 50 THEN 'running' END, 1,
        CASE WHEN TRY_CAST(attr.SPORTS_INTERESTS_FITNESS AS INT) < 50 THEN 'fitness_enthusiast' END, 1,
        CASE WHEN TRY_CAST(attr.SPORTS_INTERESTS_HUNTING AS INT) < 50 THEN 'hunting' END, 1,
        CASE WHEN TRY_CAST(attr.SPORTS_INTERESTS_NASCAR AS INT) < 50 THEN 'nascar_enthusiast' END, 1,
        CASE WHEN TRY_CAST(attr.SPORTS_INTERESTS_MLB AS INT) < 50 THEN 'mlb_enthusiast' END, 1,
        CASE WHEN TRY_CAST(attr.SPORTS_INTERESTS_NBA AS INT) < 50 THEN 'nba_enthusiast' END, 1,
        CASE WHEN TRY_CAST(attr.SPORTS_INTERESTS_NFL AS INT) < 50 THEN 'nfl_enthusiast' END, 1,
        CASE WHEN TRY_CAST(attr.SPORTS_INTERESTS_NHL AS INT) < 50 THEN 'nhl_enthusiast' END, 1,
        CASE WHEN TRY_CAST(attr.SPORTS_INTERESTS_OUTDOOR AS INT) < 50 THEN 'outdoor_enthusiast' END, 1,
        CASE WHEN TRY_CAST(attr.SPORTS_INTERESTS_PGA AS INT) < 50 THEN 'pga_tour_enthusiast' END, 1,
        CASE WHEN TRY_CAST(attr.SPORTS_INTERESTS_GOLF AS INT) < 50 THEN 'golf' END, 1,
        CASE WHEN TRY_CAST(attr.SPORTS_INTERESTS_HOCKEY AS INT) < 50 THEN 'hockey' END, 1,
        CASE WHEN TRY_CAST(attr.SPORTS_INTERESTS_SOCCER AS INT) < 50 THEN 'soccer' END, 1,
        CASE WHEN TRY_CAST(attr.SPORTS_INTERESTS_TENNIS AS INT) < 50 THEN 'tennis' END, 1,
        CASE WHEN TRY_CAST(attr.SPORTS_INTERESTS_SNOW AS INT) < 50 THEN 'snow_sports' END, 1,
        CASE WHEN TRY_CAST(attr.SPORTS_INTERESTS_GENERAL AS INT) < 50 THEN 'sports_enthusiast' END, 1
    ) AS SPORTS_INTERESTS,
    
    OBJECT_CONSTRUCT(
        CASE WHEN TRY_CAST(attr.READING_INTERESTS_AUDIO_BOOKS AS INT) < 50 THEN 'audio_book_listener' END, 1,
        CASE WHEN TRY_CAST(attr.READING_INTERESTS_BOOKS AS INT) < 50 THEN 'book_reader' END, 1,
        CASE WHEN TRY_CAST(attr.READING_INTERESTS_DIGITAL_MAGAZINES AS INT) < 50 THEN 'digital_magazines' END, 1,
        CASE WHEN TRY_CAST(attr.READING_INTERESTS_EBOOKS AS INT) < 50 THEN 'e_book_reader' END, 1
    ) AS READING_INTERESTS,
    
    OBJECT_CONSTRUCT(
        'amusement_park_visitors', CASE WHEN TRY_CAST(attr.TRAVEL_INTERESTS_AMUSEMENT_PARKS AS INT) < 50 THEN 1 ELSE 0 END,
        'boating', CASE WHEN TRY_CAST(attr.TRAVEL_INTERESTS_BOATING AS INT) < 50 THEN 1 ELSE 0 END,
        'canoeing_kayaking', CASE WHEN TRY_CAST(attr.TRAVEL_INTERESTS_CANOEING AS INT) < 50 THEN 1 ELSE 0 END,
        'disney', CASE WHEN TRY_CAST(attr.TRAVEL_INTERESTS_DISNEY AS INT) < 50 THEN 1 ELSE 0 END,
        'fishing', CASE WHEN TRY_CAST(attr.TRAVEL_INTERESTS_FISHING AS INT) < 50 THEN 1 ELSE 0 END,
        'heavy_travel', CASE WHEN TRY_CAST(attr.TRAVEL_INTERESTS_HEAVY_TRAVEL AS INT) < 50 THEN 1 ELSE 0 END,
        'travel_reward', CASE WHEN TRY_CAST(attr.TRAVEL_INTERESTS_TRAVEL_REWARDS AS INT) < 50 THEN 1 ELSE 0 END,
        'zoo_visitors', CASE WHEN TRY_CAST(attr.TRAVEL_INTERESTS_ZOO AS INT) < 50 THEN 1 ELSE 0 END
    ) AS TRAVEL_INTERESTS,
    
    -- Financial Health Bucket: Convert to numeric (1=LOW, 2=MEDIUM, 3=HIGH, 4=EXCELLENT)
    CASE
        WHEN attr.FINANCIAL_HEALTH_BUCKET = 'LOW' THEN 1
        WHEN attr.FINANCIAL_HEALTH_BUCKET = 'MEDIUM' THEN 2
        WHEN attr.FINANCIAL_HEALTH_BUCKET = 'HIGH' THEN 3
        WHEN attr.FINANCIAL_HEALTH_BUCKET = 'EXCELLENT' THEN 4
        ELSE NULL
    END AS FINANCIAL_HEALTH_BUCKET,
    
    -- Net Worth Bucket: Single letter mapped to NET_WORTH_MAP
    CASE
        WHEN attr.NET_WORTH_BUCKET = '0_4999' THEN 'B'
        WHEN attr.NET_WORTH_BUCKET = '5000_14999' THEN 'C'
        WHEN attr.NET_WORTH_BUCKET = '15000_24999' THEN 'D'
        WHEN attr.NET_WORTH_BUCKET = '25000_49999' THEN 'E'
        WHEN attr.NET_WORTH_BUCKET = '50000_99999' THEN 'F'
        WHEN attr.NET_WORTH_BUCKET IN ('100000_199999', '200000_299999') THEN 'G'
        WHEN attr.NET_WORTH_BUCKET IN ('300000_399999', '400000_499999') THEN 'H'
        WHEN attr.NET_WORTH_BUCKET IN ('500000_599999', '600000_699999', '700000_799999', '800000_999999', '1000000_2499999', '2500000+') THEN 'I'
        ELSE 'Unknown'
    END AS NET_WORTH_BUCKET,
    
    -- Credit Card Info as OBJECT (mapped to CreditCards class keys, uppercase)
    OBJECT_CONSTRUCT(
        'BANK_CARD', CASE WHEN TRY_CAST(attr.CREDIT_CARD_INFO_MAJOR_CC_USER AS INT) < 50 THEN 1 ELSE 0 END,
        'CREDIT_CARD', CASE WHEN TRY_CAST(attr.CREDIT_CARD_INFO_CREDIT_CARD_USER AS INT) < 50 THEN 1 ELSE 0 END,
        'GOLD_OR_PLATINUM_CARD', CASE WHEN TRY_CAST(attr.CREDIT_CARD_INFO_PREMIUM_CC_USER AS INT) < 50 THEN 1 ELSE 0 END,
        'PREMIUM_VISA_OR_MASTERCARD', CASE WHEN TRY_CAST(attr.CREDIT_CARD_INFO_VISA_SIGNATURE AS INT) <= 5 THEN 1 ELSE 0 END,
        'REGULAR_AMERICAN_EXPRESS', CASE WHEN TRY_CAST(attr.CREDIT_CARD_INFO_AMEX_USER AS INT) <= 5 THEN 1 ELSE 0 END,
        'REGULAR_DISCOVER', CASE WHEN TRY_CAST(attr.CREDIT_CARD_INFO_DISCOVER_USER AS INT) <= 5 THEN 1 ELSE 0 END,
        'REGULAR_STORE_OR_RETAIL', CASE WHEN TRY_CAST(attr.CREDIT_CARD_INFO_STORE_CC_USER AS INT) < 50 THEN 1 ELSE 0 END,
        'REGULAR_VISA_OR_MASTERCARD', CASE WHEN TRY_CAST(attr.CREDIT_CARD_INFO_MASTERCARD_USER AS INT) <= 5 THEN 1 ELSE 0 END
    ) AS CREDIT_CARD_INFO,
    
    -- Investment Type: Single letter mapped to INVESTMENTS_MAP (I=investment, S=stocks, T=trusts)
    -- Picks the one with lowest percentile (most likely)
    CASE
        WHEN LEAST(
            COALESCE(TRY_CAST(attr.INVESTMENT_TYPE_ACTIVE_INVESTOR AS INT), 100),
            COALESCE(TRY_CAST(attr.INVESTMENT_TYPE_BROKERAGE_ACCOUNT AS INT), 100),
            COALESCE(TRY_CAST(attr.INVESTMENT_TYPE_ONLINE_TRADING AS INT), 100),
            COALESCE(TRY_CAST(attr.INVESTMENT_TYPE_RETIREMENT_PLAN AS INT), 100)
        ) >= 50 THEN 'Unknown'
        WHEN COALESCE(TRY_CAST(attr.INVESTMENT_TYPE_ACTIVE_INVESTOR AS INT), 100) = LEAST(
            COALESCE(TRY_CAST(attr.INVESTMENT_TYPE_ACTIVE_INVESTOR AS INT), 100),
            COALESCE(TRY_CAST(attr.INVESTMENT_TYPE_BROKERAGE_ACCOUNT AS INT), 100),
            COALESCE(TRY_CAST(attr.INVESTMENT_TYPE_ONLINE_TRADING AS INT), 100),
            COALESCE(TRY_CAST(attr.INVESTMENT_TYPE_RETIREMENT_PLAN AS INT), 100)
        ) THEN 'I'
        WHEN COALESCE(TRY_CAST(attr.INVESTMENT_TYPE_BROKERAGE_ACCOUNT AS INT), 100) = LEAST(
            COALESCE(TRY_CAST(attr.INVESTMENT_TYPE_ACTIVE_INVESTOR AS INT), 100),
            COALESCE(TRY_CAST(attr.INVESTMENT_TYPE_BROKERAGE_ACCOUNT AS INT), 100),
            COALESCE(TRY_CAST(attr.INVESTMENT_TYPE_ONLINE_TRADING AS INT), 100),
            COALESCE(TRY_CAST(attr.INVESTMENT_TYPE_RETIREMENT_PLAN AS INT), 100)
        ) THEN 'S'
        WHEN COALESCE(TRY_CAST(attr.INVESTMENT_TYPE_ONLINE_TRADING AS INT), 100) = LEAST(
            COALESCE(TRY_CAST(attr.INVESTMENT_TYPE_ACTIVE_INVESTOR AS INT), 100),
            COALESCE(TRY_CAST(attr.INVESTMENT_TYPE_BROKERAGE_ACCOUNT AS INT), 100),
            COALESCE(TRY_CAST(attr.INVESTMENT_TYPE_ONLINE_TRADING AS INT), 100),
            COALESCE(TRY_CAST(attr.INVESTMENT_TYPE_RETIREMENT_PLAN AS INT), 100)
        ) THEN 'S'
        WHEN COALESCE(TRY_CAST(attr.INVESTMENT_TYPE_RETIREMENT_PLAN AS INT), 100) = LEAST(
            COALESCE(TRY_CAST(attr.INVESTMENT_TYPE_ACTIVE_INVESTOR AS INT), 100),
            COALESCE(TRY_CAST(attr.INVESTMENT_TYPE_BROKERAGE_ACCOUNT AS INT), 100),
            COALESCE(TRY_CAST(attr.INVESTMENT_TYPE_ONLINE_TRADING AS INT), 100),
            COALESCE(TRY_CAST(attr.INVESTMENT_TYPE_RETIREMENT_PLAN AS INT), 100)
        ) THEN 'T'
        ELSE 'Unknown'
    END AS INVESTMENT_TYPE,

    -- Partition Date
    attr.PARTITION_DATE
    
FROM {{ ref('v_akkio_attributes_latest') }} attr
