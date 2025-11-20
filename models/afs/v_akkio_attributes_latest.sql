{{ config(
    alias='V_AKKIO_ATTRIBUTES_LATEST',
    materialized='table',
    unique_key=['AKKIO_ID'],
    post_hook=[
        "alter table {{this}} cluster by (AKKIO_ID)"
    ]
)}}

-- ============================================================================
-- V_AKKIO_ATTRIBUTES_LATEST: Individual Attributes Dimension
-- One row per individual with all demographic attributes
-- ============================================================================

WITH source_data AS (
    SELECT DISTINCT *
    FROM {{ source('afs_poc', 'INDIVIDUAL_DEMOGRAPHIC_SPINE') }}
)

SELECT
    -- Primary Keys
    source_data.afs_individual_id AS AKKIO_ID,
    source_data.afs_individual_id AS AKKIO_HH_ID,  -- Currently 1:1, but structured for future household scenarios
    
    -- Temporal (use activity date if available, otherwise current date)
    -- lu_a_adat is in YYYYMMDD format (8 chars), convert to DATE
    COALESCE(
        CASE 
            WHEN source_data.lu_a_adat IS NOT NULL AND LENGTH(CAST(source_data.lu_a_adat AS VARCHAR)) = 8 
            THEN TO_DATE(CAST(source_data.lu_a_adat AS VARCHAR), 'YYYYMMDD')
            ELSE NULL
        END,
        CURRENT_DATE()
    ) AS PARTITION_DATE,
    
    -- Core Demographics
    -- Gender (normalized - original gndr_gndr_1 available via source_data.*)
    -- Values: M=Male, F=Female, U=Unknown, B=Male/Female Pair, blank=Null
    CASE 
        WHEN source_data.gndr_gndr_1 = 'M' THEN 'MALE'
        WHEN source_data.gndr_gndr_1 = 'F' THEN 'FEMALE'
        WHEN source_data.gndr_gndr_1 = 'U' THEN 'UNKNOWN'
        WHEN source_data.gndr_gndr_1 = 'B' THEN 'MALE_FEMALE_PAIR'
        ELSE 'UNDETERMINED'
    END AS GENDER,
    
    -- Age
    -- combined_age_1 format: E25 (Exact Age 25), I25 (Inferred Age 25), or blank
    CASE 
        WHEN source_data.combined_age_1 IS NULL OR source_data.combined_age_1 = '' THEN NULL
        WHEN LENGTH(source_data.combined_age_1) >= 2 THEN 
            CAST(SUBSTRING(source_data.combined_age_1, 2) AS INTEGER)
        ELSE NULL
    END AS AGE,
    CASE 
        WHEN source_data.combined_age_1 IS NULL OR source_data.combined_age_1 = '' THEN NULL
        WHEN LENGTH(source_data.combined_age_1) >= 2 THEN
            CASE 
                WHEN CAST(SUBSTRING(source_data.combined_age_1, 2) AS INTEGER) BETWEEN 19 AND 24 THEN '19-24'
                WHEN CAST(SUBSTRING(source_data.combined_age_1, 2) AS INTEGER) BETWEEN 25 AND 29 THEN '25-29'
                WHEN CAST(SUBSTRING(source_data.combined_age_1, 2) AS INTEGER) BETWEEN 30 AND 34 THEN '30-34'
                WHEN CAST(SUBSTRING(source_data.combined_age_1, 2) AS INTEGER) BETWEEN 35 AND 39 THEN '35-39'
                WHEN CAST(SUBSTRING(source_data.combined_age_1, 2) AS INTEGER) BETWEEN 40 AND 44 THEN '40-44'
                WHEN CAST(SUBSTRING(source_data.combined_age_1, 2) AS INTEGER) BETWEEN 45 AND 49 THEN '45-49'
                WHEN CAST(SUBSTRING(source_data.combined_age_1, 2) AS INTEGER) BETWEEN 50 AND 54 THEN '50-54'
                WHEN CAST(SUBSTRING(source_data.combined_age_1, 2) AS INTEGER) BETWEEN 55 AND 59 THEN '55-59'
                WHEN CAST(SUBSTRING(source_data.combined_age_1, 2) AS INTEGER) >= 60 THEN '60+'
                ELSE NULL
            END
        ELSE NULL
    END AS AGE_BUCKET,
    source_data.yearofbirth_1 AS YEAR_OF_BIRTH,
    
    -- Ethnicity (normalized - original et_grp_1 and et_code_1 available via source_data.*)
    -- et_grp_1 values: A=African American, B=Southeast Asian, C=South Asian, D=Central Asian, 
    -- E=Mediterranean, F=Native American, G=Scandinavian, H=Polynesian, I=Middle Eastern, 
    -- J=Jewish, K=Western European, L=Eastern European, M=Caribbean Non-Hispanic, N=East Asian, 
    -- O=Hispanic, Z=Uncoded, blank=Null
    CASE 
        WHEN source_data.et_grp_1 = 'O' THEN 'HISPANIC'
        WHEN source_data.et_grp_1 = 'A' THEN 'AFRICAN_AMERICAN'
        WHEN source_data.et_grp_1 = 'B' THEN 'SOUTHEAST_ASIAN'
        WHEN source_data.et_grp_1 = 'C' THEN 'SOUTH_ASIAN'
        WHEN source_data.et_grp_1 = 'D' THEN 'CENTRAL_ASIAN'
        WHEN source_data.et_grp_1 = 'E' THEN 'MEDITERRANEAN'
        WHEN source_data.et_grp_1 = 'F' THEN 'NATIVE_AMERICAN'
        WHEN source_data.et_grp_1 = 'G' THEN 'SCANDINAVIAN'
        WHEN source_data.et_grp_1 = 'H' THEN 'POLYNESIAN'
        WHEN source_data.et_grp_1 = 'I' THEN 'MIDDLE_EASTERN'
        WHEN source_data.et_grp_1 = 'J' THEN 'JEWISH'
        WHEN source_data.et_grp_1 = 'K' THEN 'WESTERN_EUROPEAN'
        WHEN source_data.et_grp_1 = 'L' THEN 'EASTERN_EUROPEAN'
        WHEN source_data.et_grp_1 = 'M' THEN 'CARIBBEAN_NON_HISPANIC'
        WHEN source_data.et_grp_1 = 'N' THEN 'EAST_ASIAN'
        WHEN source_data.et_grp_1 = 'Z' THEN 'UNCODED'
        ELSE 'UNDETERMINED'
    END AS ETHNICITY,
    
    -- Education (normalized - original educ_levl_1 and educ_model_1 available via source_data.*)
    source_data.educ_levl_1 AS EDUCATION_LEVEL,
    source_data.educ_model_1 AS EDUCATION_MODEL,
    
    -- Marital Status (normalized - original mrtl_model_1 available via source_data.*)
    source_data.mrtl_model_1 AS MARITAL_STATUS,
    
    -- Household Attributes (normalized - original combined_homeowner available via source_data.*)
    -- combined_homeowner values: H=Homeowner, R=Renter, T=Probably Renter, 
    -- 7=Probable Homeowner 70-79, 8=Probable Homeowner 80-89, 9=Probable homeowner 90-100, U=Unknown
    CASE 
        WHEN source_data.combined_homeowner IN ('H', '7', '8', '9') THEN 'HOMEOWNER'
        WHEN source_data.combined_homeowner IN ('R', 'T') THEN 'RENTER'
        ELSE 'UNKNOWN'
    END AS HOMEOWNER_STATUS,
    
    -- Income (normalized - original lu_inc_model_v6 and lu_inc_model_v6_amt available via source_data.*)
    -- lu_inc_model_v6: A=$1K-$14.9K, B=$15K-$24.9K, C=$25K-$34.9K, D=$35K-$49.9K, 
    -- E=$50K-$74.9K, F=$75K-$99.9K, G=$100K-$124.9K, H=$125K-$149.9K, 
    -- I=$150K-$174.9K, J=$175K-$199.9K, K=$200K-$249.9K, L=$250K+, U=Unknown
    source_data.lu_inc_model_v6_amt AS HOUSEHOLD_INCOME_K,  -- Values 001-250 (in thousands), 000=Unknown
    CASE 
        WHEN source_data.lu_inc_model_v6 = 'A' THEN '1K_14K'
        WHEN source_data.lu_inc_model_v6 = 'B' THEN '15K_24K'
        WHEN source_data.lu_inc_model_v6 = 'C' THEN '25K_34K'
        WHEN source_data.lu_inc_model_v6 = 'D' THEN '35K_49K'
        WHEN source_data.lu_inc_model_v6 = 'E' THEN '50K_74K'
        WHEN source_data.lu_inc_model_v6 = 'F' THEN '75K_99K'
        WHEN source_data.lu_inc_model_v6 = 'G' THEN '100K_124K'
        WHEN source_data.lu_inc_model_v6 = 'H' THEN '125K_149K'
        WHEN source_data.lu_inc_model_v6 = 'I' THEN '150K_174K'
        WHEN source_data.lu_inc_model_v6 = 'J' THEN '175K_199K'
        WHEN source_data.lu_inc_model_v6 = 'K' THEN '200K_249K'
        WHEN source_data.lu_inc_model_v6 = 'L' THEN '250K_PLUS'
        WHEN source_data.lu_inc_model_v6 = 'U' THEN 'UNKNOWN'
        ELSE 'UNDETERMINED'
    END AS INCOME_BUCKET,
    
    -- Wealth/Net Worth (normalized - original fin_fla_networth available via source_data.*)
    -- fin_fla_networth values: A=$4,999 and less, B=$5,000-$14,999, C=$15,000-$24,999, 
    -- D=$25,000-$49,999, E=$50,000-$99,999, F=$100,000-$199,999, G=$200,000-$299,999,
    -- H=$300,000-$399,999, I=$400,000-$499,999, J=$500,000-$599,999, K=$600,000-$699,999,
    -- L=$700,000-$799,999, M=$800,000-$999,999, N=$1,000,000-$2,499,999, O=$2,500,000+
    source_data.fin_fla_networth AS WEALTH,
    CASE 
        WHEN source_data.fin_fla_networth = 'A' THEN '0_4999'
        WHEN source_data.fin_fla_networth = 'B' THEN '5000_14999'
        WHEN source_data.fin_fla_networth = 'C' THEN '15000_24999'
        WHEN source_data.fin_fla_networth = 'D' THEN '25000_49999'
        WHEN source_data.fin_fla_networth = 'E' THEN '50000_99999'
        WHEN source_data.fin_fla_networth = 'F' THEN '100000_199999'
        WHEN source_data.fin_fla_networth = 'G' THEN '200000_299999'
        WHEN source_data.fin_fla_networth = 'H' THEN '300000_399999'
        WHEN source_data.fin_fla_networth = 'I' THEN '400000_499999'
        WHEN source_data.fin_fla_networth = 'J' THEN '500000_599999'
        WHEN source_data.fin_fla_networth = 'K' THEN '600000_699999'
        WHEN source_data.fin_fla_networth = 'L' THEN '700000_799999'
        WHEN source_data.fin_fla_networth = 'M' THEN '800000_999999'
        WHEN source_data.fin_fla_networth = 'N' THEN '1000000_2499999'
        WHEN source_data.fin_fla_networth = 'O' THEN '2500000_PLUS'
        ELSE 'UNKNOWN'
    END AS WEALTH_BUCKET,
    
    -- Politics (normalized - original political_code_1 available via source_data.*)
    -- political_code_1 values: 0U=Unknown, 1D=Known Democrat, 1I=Known Independent/Other,
    -- 1R=Known Republican, 5D=Inferred Democrat, 5I=Inferred Independent/Other,
    -- 5N=Inferred Non-Registered, 5R=Inferred Republican
    source_data.political_code_1 AS POLITICS,
    CASE 
        WHEN source_data.political_code_1 = '1D' THEN 'DEMOCRAT_KNOWN'
        WHEN source_data.political_code_1 = '5D' THEN 'DEMOCRAT_INFERRED'
        WHEN source_data.political_code_1 = '1R' THEN 'REPUBLICAN_KNOWN'
        WHEN source_data.political_code_1 = '5R' THEN 'REPUBLICAN_INFERRED'
        WHEN source_data.political_code_1 = '1I' THEN 'INDEPENDENT_KNOWN'
        WHEN source_data.political_code_1 = '5I' THEN 'INDEPENDENT_INFERRED'
        WHEN source_data.political_code_1 = '5N' THEN 'NON_REGISTERED_INFERRED'
        WHEN source_data.political_code_1 = '0U' THEN 'UNKNOWN'
        ELSE 'UNDETERMINED'
    END AS POLITICS_NORMALIZED,
    
    -- Business Owner (normalized - original biz_owner_1 available via source_data.*)
    -- biz_owner_1 values: Y=Yes, U=Unknown
    source_data.biz_owner_1 AS BUSINESS_OWNER,
    CASE 
        WHEN source_data.biz_owner_1 = 'Y' THEN 'YES'
        WHEN source_data.biz_owner_1 = 'U' THEN 'UNKNOWN'
        ELSE 'NO'
    END AS BUSINESS_OWNER_FLAG,
    
    -- Household Composition
    -- Number of Adults in Household (rec_adultcnt values: 0-8, blank=Blank)
    source_data.rec_adultcnt AS ADULTS_IN_HH,
    -- Number of Children in Household (rec_childcnt values: 0-8, blank=Blank)
    source_data.rec_childcnt AS CHILDREN,
    
    -- Geographic (normalized - original fields available via source_data.*)
    source_data.stat_abbr AS STATE_ABBR,
    source_data.recd_zipc AS ZIP_CODE,
    source_data.city_plac AS CITY,
    source_data.geo_cntyname AS COUNTY_NAME,
    
    -- Market Area (CBSA - Core Based Statistical Area, similar to DMA)
    -- cbsa_code: 5-digit CBSA code (e.g., 10100=ABERDEEN SD, 16980=CHICAGO-NAPERVILLE-ELGIN IL-IN-WI)
    -- cbsa_type: A=Metro CBSA, B=Micro CBSA, C=Not in CBSA
    -- metro_flag: Y=Metro Area, N=Not Metro Area, blank=Not a CBSA
    source_data.cbsa_code AS CBSA_CODE,
    source_data.cbsa_type AS CBSA_TYPE,
    source_data.metro_flag AS METRO_FLAG,
    CASE 
        WHEN source_data.cbsa_type = 'A' THEN 'METRO'
        WHEN source_data.cbsa_type = 'B' THEN 'MICRO'
        WHEN source_data.cbsa_type = 'C' THEN 'RURAL'
        ELSE 'UNKNOWN'
    END AS MARKET_AREA_TYPE,
    
    -- All other fields from source (explicitly reference to avoid duplicates)
    -- source_data.*
FROM source_data

