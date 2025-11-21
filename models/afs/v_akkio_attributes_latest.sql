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
        WHEN source_data.gndr_gndr_1 = 'B' THEN 'BOTH'
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
    -- educ_levl_1 values: 0=Unknown, 1=High School Diploma, 2=Some College, 3=Completed College, 4=Graduate Degree, 5=Retired, blank=Blank
    CASE 
        WHEN source_data.educ_levl_1 = '0' THEN 'UNKNOWN'
        WHEN source_data.educ_levl_1 = '1' THEN 'HIGH_SCHOOL_DIPLOMA'
        WHEN source_data.educ_levl_1 = '2' THEN 'SOME_COLLEGE'
        WHEN source_data.educ_levl_1 = '3' THEN 'COMPLETED_COLLEGE'
        WHEN source_data.educ_levl_1 = '4' THEN 'GRADUATE_DEGREE'
        WHEN source_data.educ_levl_1 = '5' THEN 'RETIRED'
        ELSE 'UNDETERMINED'
    END AS EDUCATION_LEVEL,
    -- educ_model_1 values: 00=Unknown, 11=HS Diploma - Extremely Likely, 12=Some College -Extremely Likely, 13=Bach Degree - Extremely Likely, 
    -- 14=Grad Degree - Extremely Likely, 15=Less than HS Diploma - Ex Like, 51=HS Diploma - Likely, 52=Some College - Likely, 
    -- 53=Bach Degree - Likely, 54=Grad Degree - Likely, 55=Less than HS Diploma - Likely, blank=Null
    CASE 
        WHEN source_data.educ_model_1 = '00' THEN 'UNKNOWN'
        WHEN source_data.educ_model_1 = '11' THEN 'HIGH_SCHOOL_DIPLOMA_EXTREMELY_LIKELY'
        WHEN source_data.educ_model_1 = '12' THEN 'SOME_COLLEGE_EXTREMELY_LIKELY'
        WHEN source_data.educ_model_1 = '13' THEN 'BACHELORS_DEGREE_EXTREMELY_LIKELY'
        WHEN source_data.educ_model_1 = '14' THEN 'GRADUATE_DEGREE_EXTREMELY_LIKELY'
        WHEN source_data.educ_model_1 = '15' THEN 'LESS_THAN_HS_DIPLOMA_EXTREMELY_LIKELY'
        WHEN source_data.educ_model_1 = '51' THEN 'HIGH_SCHOOL_DIPLOMA_LIKELY'
        WHEN source_data.educ_model_1 = '52' THEN 'SOME_COLLEGE_LIKELY'
        WHEN source_data.educ_model_1 = '53' THEN 'BACHELORS_DEGREE_LIKELY'
        WHEN source_data.educ_model_1 = '54' THEN 'GRADUATE_DEGREE_LIKELY'
        WHEN source_data.educ_model_1 = '55' THEN 'LESS_THAN_HS_DIPLOMA_LIKELY'
        ELSE 'UNDETERMINED'
    END AS EDUCATION_MODEL,
    
    -- Marital Status (normalized - original mrtl_model_1 available via source_data.*)
    -- mrtl_model_1 values: 0U=Unknown Not scored, 1M=Married Extremely Likely, 5M=Married Likely, 
    -- 5S=Single Likely never married, 5U=Unknown Scored, blank=Null
    CASE 
        WHEN source_data.mrtl_model_1 = '0U' THEN 'UNKNOWN_NOT_SCORED'
        WHEN source_data.mrtl_model_1 = '1M' THEN 'MARRIED_EXTREMELY_LIKELY'
        WHEN source_data.mrtl_model_1 = '5M' THEN 'MARRIED_LIKELY'
        WHEN source_data.mrtl_model_1 = '5S' THEN 'SINGLE_LIKELY_NEVER_MARRIED'
        WHEN source_data.mrtl_model_1 = '5U' THEN 'UNKNOWN_SCORED'
        ELSE 'UNDETERMINED'
    END AS MARITAL_STATUS,
    
    -- Occupation (from ocup_ocup_1 and ocup_model_v2_1)
    -- ocup_ocup_1 values: 00=Unknown, 02=Professional/Technical, 03=Upper Management/Executive, 04=Middle Management, 
    -- 05=Sales/Marketing, 06=Clerical/Office, 07=SkilledTrade/Machine/Laborer, 08=Retired, 10=Executive/Administrator, 
    -- 11=Self Employed, 12=Professional Driver, 13=Military, 14=Civil Servant, 15=Farming/Agriculture, 16=Work From Home,
    -- 17=Health Services, 18=Financial Services, 21=Teacher/Educator, 22=Retail Sales, 23=Computer Professional,
    -- 30=Beauty, 31=Real Estate, 32=Architects, 33=Interior Designers, 34=Landscape Architects, 35=Electricians, 36=Engineers,
    -- 37=Accountants/CPA, 38=Attorneys, 39=Social Worker, 40=Counselors, 41=Occupational Ther/Physical Ther, 
    -- 42=Speech Path./Audiologist, 43=Psychologist, 44=Pharmacist, 45=Opticians/Optometrist, 46=Veterinarian, 
    -- 47=Dentist/Dental Hygienist, 48=Nurse, 49=Doctors/Physicians/Surgeons, 50=Chiropractors, 51=Surveyors, 52=Clergy,
    -- 53=Insurance/Underwriters, 54=Services/Creative, blank=Blank
    CASE 
        WHEN source_data.ocup_ocup_1 = '00' THEN 'UNKNOWN'
        WHEN source_data.ocup_ocup_1 = '02' THEN 'PROFESSIONAL_TECHNICAL'
        WHEN source_data.ocup_ocup_1 = '03' THEN 'UPPER_MANAGEMENT_EXECUTIVE'
        WHEN source_data.ocup_ocup_1 = '04' THEN 'MIDDLE_MANAGEMENT'
        WHEN source_data.ocup_ocup_1 = '05' THEN 'SALES_MARKETING'
        WHEN source_data.ocup_ocup_1 = '06' THEN 'CLERICAL_OFFICE'
        WHEN source_data.ocup_ocup_1 = '07' THEN 'SKILLED_TRADE_MACHINE_LABORER'
        WHEN source_data.ocup_ocup_1 = '08' THEN 'RETIRED'
        WHEN source_data.ocup_ocup_1 = '10' THEN 'EXECUTIVE_ADMINISTRATOR'
        WHEN source_data.ocup_ocup_1 = '11' THEN 'SELF_EMPLOYED'
        WHEN source_data.ocup_ocup_1 = '12' THEN 'PROFESSIONAL_DRIVER'
        WHEN source_data.ocup_ocup_1 = '13' THEN 'MILITARY'
        WHEN source_data.ocup_ocup_1 = '14' THEN 'CIVIL_SERVANT'
        WHEN source_data.ocup_ocup_1 = '15' THEN 'FARMING_AGRICULTURE'
        WHEN source_data.ocup_ocup_1 = '16' THEN 'WORK_FROM_HOME'
        WHEN source_data.ocup_ocup_1 = '17' THEN 'HEALTH_SERVICES'
        WHEN source_data.ocup_ocup_1 = '18' THEN 'FINANCIAL_SERVICES'
        WHEN source_data.ocup_ocup_1 = '21' THEN 'TEACHER_EDUCATOR'
        WHEN source_data.ocup_ocup_1 = '22' THEN 'RETAIL_SALES'
        WHEN source_data.ocup_ocup_1 = '23' THEN 'COMPUTER_PROFESSIONAL'
        WHEN source_data.ocup_ocup_1 = '30' THEN 'BEAUTY'
        WHEN source_data.ocup_ocup_1 = '31' THEN 'REAL_ESTATE'
        WHEN source_data.ocup_ocup_1 = '32' THEN 'ARCHITECTS'
        WHEN source_data.ocup_ocup_1 = '33' THEN 'INTERIOR_DESIGNERS'
        WHEN source_data.ocup_ocup_1 = '34' THEN 'LANDSCAPE_ARCHITECTS'
        WHEN source_data.ocup_ocup_1 = '35' THEN 'ELECTRICIANS'
        WHEN source_data.ocup_ocup_1 = '36' THEN 'ENGINEERS'
        WHEN source_data.ocup_ocup_1 = '37' THEN 'ACCOUNTANTS_CPA'
        WHEN source_data.ocup_ocup_1 = '38' THEN 'ATTORNEYS'
        WHEN source_data.ocup_ocup_1 = '39' THEN 'SOCIAL_WORKER'
        WHEN source_data.ocup_ocup_1 = '40' THEN 'COUNSELORS'
        WHEN source_data.ocup_ocup_1 = '41' THEN 'OCCUPATIONAL_THERAPIST_PHYSICAL_THERAPIST'
        WHEN source_data.ocup_ocup_1 = '42' THEN 'SPEECH_PATHOLOGIST_AUDIOLOGIST'
        WHEN source_data.ocup_ocup_1 = '43' THEN 'PSYCHOLOGIST'
        WHEN source_data.ocup_ocup_1 = '44' THEN 'PHARMACIST'
        WHEN source_data.ocup_ocup_1 = '45' THEN 'OPTICIANS_OPTOMETRIST'
        WHEN source_data.ocup_ocup_1 = '46' THEN 'VETERINARIAN'
        WHEN source_data.ocup_ocup_1 = '47' THEN 'DENTIST_DENTAL_HYGIENIST'
        WHEN source_data.ocup_ocup_1 = '48' THEN 'NURSE'
        WHEN source_data.ocup_ocup_1 = '49' THEN 'DOCTORS_PHYSICIANS_SURGEONS'
        WHEN source_data.ocup_ocup_1 = '50' THEN 'CHIROPRACTORS'
        WHEN source_data.ocup_ocup_1 = '51' THEN 'SURVEYORS'
        WHEN source_data.ocup_ocup_1 = '52' THEN 'CLERGY'
        WHEN source_data.ocup_ocup_1 = '53' THEN 'INSURANCE_UNDERWRITERS'
        WHEN source_data.ocup_ocup_1 = '54' THEN 'SERVICES_CREATIVE'
        ELSE 'UNDETERMINED'
    END AS OCCUPATION,
    -- ocup_model_v2_1 values: blank=Null, I1=Management - Inferred, I2=Technical - Inferred, I3=Professional - Inferred,
    -- I4=Sales - Inferred, I5=Office Administration - Inferred, I6=Blue Collar - Inferred, I7=Farmer - Inferred,
    -- I8=Other - Inferred, I9=Retired - Inferred, K1=Management - Known, K2=Technical - Known, K3=Professional - Known,
    -- K4=Sales - Known, K5=Office Administration - Known, K6=Blue Collar - Known, K7=Farmer - Known, K8=Other - Known,
    -- K9=Retired - Known, U0=Unknown
    CASE 
        WHEN source_data.ocup_model_v2_1 IS NULL OR source_data.ocup_model_v2_1 = '' THEN 'UNKNOWN'
        WHEN source_data.ocup_model_v2_1 = 'U0' THEN 'UNKNOWN'
        WHEN source_data.ocup_model_v2_1 = 'I1' THEN 'MANAGEMENT_INFERRED'
        WHEN source_data.ocup_model_v2_1 = 'I2' THEN 'TECHNICAL_INFERRED'
        WHEN source_data.ocup_model_v2_1 = 'I3' THEN 'PROFESSIONAL_INFERRED'
        WHEN source_data.ocup_model_v2_1 = 'I4' THEN 'SALES_INFERRED'
        WHEN source_data.ocup_model_v2_1 = 'I5' THEN 'OFFICE_ADMINISTRATION_INFERRED'
        WHEN source_data.ocup_model_v2_1 = 'I6' THEN 'BLUE_COLLAR_INFERRED'
        WHEN source_data.ocup_model_v2_1 = 'I7' THEN 'FARMER_INFERRED'
        WHEN source_data.ocup_model_v2_1 = 'I8' THEN 'OTHER_INFERRED'
        WHEN source_data.ocup_model_v2_1 = 'I9' THEN 'RETIRED_INFERRED'
        WHEN source_data.ocup_model_v2_1 = 'K1' THEN 'MANAGEMENT_KNOWN'
        WHEN source_data.ocup_model_v2_1 = 'K2' THEN 'TECHNICAL_KNOWN'
        WHEN source_data.ocup_model_v2_1 = 'K3' THEN 'PROFESSIONAL_KNOWN'
        WHEN source_data.ocup_model_v2_1 = 'K4' THEN 'SALES_KNOWN'
        WHEN source_data.ocup_model_v2_1 = 'K5' THEN 'OFFICE_ADMINISTRATION_KNOWN'
        WHEN source_data.ocup_model_v2_1 = 'K6' THEN 'BLUE_COLLAR_KNOWN'
        WHEN source_data.ocup_model_v2_1 = 'K7' THEN 'FARMER_KNOWN'
        WHEN source_data.ocup_model_v2_1 = 'K8' THEN 'OTHER_KNOWN'
        WHEN source_data.ocup_model_v2_1 = 'K9' THEN 'RETIRED_KNOWN'
        ELSE 'UNDETERMINED'
    END AS OCCUPATION_MODEL,
    
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
    -- lu_inc_model_v6_amt: String values 001-250 represent income in thousands (e.g., "001" = $1K, "250" = $250K), "000" = Unknown
    -- Convert string to integer and multiply by 1000: "001" -> 1000 (representing $1,000), "250" -> 250000 (representing $250,000)
    CASE 
        WHEN source_data.lu_inc_model_v6_amt IS NULL 
             OR TRIM(source_data.lu_inc_model_v6_amt) = '' 
             OR TRIM(source_data.lu_inc_model_v6_amt) = '000' 
        THEN NULL
        ELSE CAST(TRIM(source_data.lu_inc_model_v6_amt) AS INTEGER) * 1000
    END AS HOUSEHOLD_INCOME_K,
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
    -- NET_WORTH_BUCKET (same as WEALTH_BUCKET but named as per spec)
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
        WHEN source_data.fin_fla_networth = 'O' THEN '2500000+'
        ELSE 'UNKNOWN'
    END AS NET_WORTH_BUCKET,
    -- Financial Health Bucket (derived from net worth)
    CASE 
        WHEN source_data.fin_fla_networth IN ('A', 'B', 'C', 'D') THEN 'LOW'
        WHEN source_data.fin_fla_networth IN ('E', 'F', 'G', 'H') THEN 'MEDIUM'
        WHEN source_data.fin_fla_networth IN ('I', 'J', 'K', 'L', 'M', 'N', 'O') THEN 'HIGH'
        ELSE 'UNKNOWN'
    END AS FINANCIAL_HEALTH_BUCKET,
    
    -- Politics (normalized - original political_code_1 available via source_data.*)
    -- political_code_1 values: 0U=Unknown, 1D=Known Democrat, 1I=Known Independent/Other,
    -- 1R=Known Republican, 5D=Inferred Democrat, 5I=Inferred Independent/Other,
    -- 5N=Inferred Non-Registered, 5R=Inferred Republican
    CASE 
        WHEN source_data.political_code_1 = '0U' THEN 'UNKNOWN'
        WHEN source_data.political_code_1 = '1D' THEN 'DEMOCRAT_KNOWN'
        WHEN source_data.political_code_1 = '1I' THEN 'INDEPENDENT_KNOWN'
        WHEN source_data.political_code_1 = '1R' THEN 'REPUBLICAN_KNOWN'
        WHEN source_data.political_code_1 = '5D' THEN 'DEMOCRAT_INFERRED'
        WHEN source_data.political_code_1 = '5I' THEN 'INDEPENDENT_INFERRED'
        WHEN source_data.political_code_1 = '5N' THEN 'NON_REGISTERED_INFERRED'
        WHEN source_data.political_code_1 = '5R' THEN 'REPUBLICAN_INFERRED'
        ELSE 'UNDETERMINED'
    END AS POLITICS,
    
    -- Business Owner (normalized - original biz_owner_1 available via source_data.*)
    -- biz_owner_1 values: Y=Yes, U=Unknown
    CASE 
        WHEN source_data.biz_owner_1 = 'Y' THEN 'YES'
        WHEN source_data.biz_owner_1 = 'U' THEN 'UNKNOWN'
        ELSE 'NO'
    END AS BUSINESS_OWNER,
    
    -- Household Composition
    -- Number of Adults in Household (rec_adultcnt values: 0-8, blank=Blank)
    CAST(COALESCE(source_data.rec_adultcnt, '0') AS INTEGER) AS ADULTS_IN_HH,
    -- Number of Children in Household (rec_childcnt values: 0-8, blank=Blank)
    CAST(COALESCE(source_data.rec_childcnt, '0') AS INTEGER) AS NUMBER_OF_CHILDREN,
    -- Number of People in Household (rec_perscnt values: 0-8, blank=Blank)
    CAST(COALESCE(source_data.rec_perscnt, '0') AS INTEGER) AS NUM_PEOPLE_IN_HOUSEHOLD_GROUP,
    -- Presence of Children (pocv4_code: presence of child 0-18)
    CASE 
        WHEN source_data.pocv4_code IS NOT NULL AND source_data.pocv4_code != '' THEN 1
        ELSE 0
    END AS PRESENCE_OF_CHILDREN,
    -- Child Age Group (aggregated from individual pocv4_*_code fields)
    -- pocv4_*_code values: 1Y=Known data (child present), 5Y=Modeled likely to have a child,
    -- 5N=Not likely, 5U=Modeled not likely, 00=Deceased/child only, 0U=Unmatched
    -- Returns comma-separated list of age groups where children are present (1Y or 5Y)
    ARRAY_TO_STRING(
        ARRAY_CONSTRUCT_COMPACT(
            CASE WHEN source_data.pocv4_0_3_code IN ('1Y', '5Y') THEN '0-3' END,
            CASE WHEN source_data.pocv4_4_6_code IN ('1Y', '5Y') THEN '4-6' END,
            CASE WHEN source_data.pocv4_7_9_code IN ('1Y', '5Y') THEN '7-9' END,
            CASE WHEN source_data.pocv4_10_12_code IN ('1Y', '5Y') THEN '10-12' END,
            CASE WHEN source_data.pocv4_13_15_code IN ('1Y', '5Y') THEN '13-15' END,
            CASE WHEN source_data.pocv4_16_18_code IN ('1Y', '5Y') THEN '16-18' END
        ),
        ', '
    ) AS CHILD_AGE_GROUP,
    
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
    
    -- Median Home Value by State (from CAPE2020 data)
    CAST(COALESCE(source_data.c20_cyemhuval, '0') AS INTEGER) AS MEDIAN_HOME_VALUE_BY_STATE,
    
    -- Interests (from act_int_* fields)
    -- General Interests
    source_data.act_int_arts_and_crafts AS GENERAL_INTERESTS_ARTS_CRAFTS,
    source_data.act_int_book_reader AS GENERAL_INTERESTS_BOOK_READER,
    source_data.act_int_cultural_arts AS GENERAL_INTERESTS_CULTURAL_ARTS,
    source_data.act_int_do_it_yourselfers AS GENERAL_INTERESTS_DIY,
    source_data.act_int_gourmet_cooking AS GENERAL_INTERESTS_GOURMET_COOKING,
    source_data.act_int_healthy_living AS GENERAL_INTERESTS_HEALTHY_LIVING,
    source_data.act_int_home_improvement_spenders AS GENERAL_INTERESTS_HOME_IMPROVEMENT,
    source_data.act_int_photography AS GENERAL_INTERESTS_PHOTOGRAPHY,
    source_data.act_int_scrapbooking AS GENERAL_INTERESTS_SCRAPBOOKING,
    -- Sports Interests
    source_data.act_int_avid_runners AS SPORTS_INTERESTS_RUNNING,
    source_data.act_int_fitness_enthusiast AS SPORTS_INTERESTS_FITNESS,
    source_data.act_int_hunting_enthusiasts AS SPORTS_INTERESTS_HUNTING,
    source_data.act_int_nascar_enthusiast AS SPORTS_INTERESTS_NASCAR,
    source_data.act_int_mlb_enthusiast AS SPORTS_INTERESTS_MLB,
    source_data.act_int_nba_enthusiast AS SPORTS_INTERESTS_NBA,
    source_data.act_int_nfl_enthusiast AS SPORTS_INTERESTS_NFL,
    source_data.act_int_nhl_enthusiast AS SPORTS_INTERESTS_NHL,
    source_data.act_int_outdoor_enthusiast AS SPORTS_INTERESTS_OUTDOOR,
    source_data.act_int_pga_tour_enthusiast AS SPORTS_INTERESTS_PGA,
    source_data.act_int_play_golf AS SPORTS_INTERESTS_GOLF,
    source_data.act_int_plays_hockey AS SPORTS_INTERESTS_HOCKEY,
    source_data.act_int_plays_soccer AS SPORTS_INTERESTS_SOCCER,
    source_data.act_int_plays_tennis AS SPORTS_INTERESTS_TENNIS,
    source_data.act_int_snow_sports AS SPORTS_INTERESTS_SNOW,
    source_data.act_int_sports_enthusiast AS SPORTS_INTERESTS_GENERAL,
    -- Reading Interests
    source_data.act_int_audio_book_listener AS READING_INTERESTS_AUDIO_BOOKS,
    source_data.act_int_book_reader AS READING_INTERESTS_BOOKS,
    source_data.act_int_digital_magazine_newspapers_buyers AS READING_INTERESTS_DIGITAL_MAGAZINES,
    source_data.act_int_e_book_reader AS READING_INTERESTS_EBOOKS,
    -- Travel Interests
    source_data.act_int_amusement_park_visitors AS TRAVEL_INTERESTS_AMUSEMENT_PARKS,
    source_data.act_int_boating AS TRAVEL_INTERESTS_BOATING,
    source_data.act_int_canoeing_kayaking AS TRAVEL_INTERESTS_CANOEING,
    source_data.act_int_disney AS TRAVEL_INTERESTS_DISNEY,
    source_data.act_int_fishing AS TRAVEL_INTERESTS_FISHING,
    source_data.act_int_heavy_travel AS TRAVEL_INTERESTS_HEAVY_TRAVEL,
    source_data.act_int_travel_reward AS TRAVEL_INTERESTS_TRAVEL_REWARDS,
    source_data.act_int_zoo_visitors AS TRAVEL_INTERESTS_ZOO,
    
    -- Credit Card Information (from financial_* fields)
    source_data.financial_credit_card_user AS CREDIT_CARD_INFO_CREDIT_CARD_USER,
    source_data.financial_major_credit_card_user AS CREDIT_CARD_INFO_MAJOR_CC_USER,
    source_data.financial_premium_credit_card_user AS CREDIT_CARD_INFO_PREMIUM_CC_USER,
    source_data.financial_store_credit_card_user AS CREDIT_CARD_INFO_STORE_CC_USER,
    source_data.finc_amex_card_user AS CREDIT_CARD_INFO_AMEX_USER,
    source_data.finc_discover_user AS CREDIT_CARD_INFO_DISCOVER_USER,
    source_data.finc_mastercard_user AS CREDIT_CARD_INFO_MASTERCARD_USER,
    source_data.finc_visa_signature AS CREDIT_CARD_INFO_VISA_SIGNATURE,
    
    -- Investment Type (from invest_* fields)
    source_data.invest_active_investor AS INVESTMENT_TYPE_ACTIVE_INVESTOR,
    source_data.invest_brokerage_account_owner AS INVESTMENT_TYPE_BROKERAGE_ACCOUNT,
    source_data.invest_have_a_retirement_financial_plan AS INVESTMENT_TYPE_RETIREMENT_PLAN,
    source_data.invest_mutual_fund_investor AS INVESTMENT_TYPE_MUTUAL_FUND,
    source_data.invest_participate_in_online_trading AS INVESTMENT_TYPE_ONLINE_TRADING,
    
    -- Media Consumption (from rs_* and tv_mov_* fields - will be aggregated in v_agg_akkio_ind_media)
    -- These are included here for reference but will be aggregated separately
    
    -- Media Spending Indicators
    source_data.rs_cable_tv_highspenders AS MEDIA_CABLE_TV_HIGH_SPEND,
    source_data.rs_audio_highspenders AS MEDIA_AUDIO_HIGH_SPEND,
    source_data.rs_video_high AS MEDIA_VIDEO_HIGH_SPEND,
    source_data.rs_cord_cutters_recent AS MEDIA_CORD_CUTTERS_RECENT,
    
    -- TV/Cable Providers (Networks)
    source_data.rs_tv_brand_comcast AS MEDIA_TV_BRAND_COMCAST,
    source_data.rs_tv_brand_directv AS MEDIA_TV_BRAND_DIRECTV,
    source_data.rs_tv_brand_dish_network AS MEDIA_TV_BRAND_DISH,
    source_data.rs_tv_brand_spectrum AS MEDIA_TV_BRAND_SPECTRUM,
    source_data.rs_tv_brand_xfinity AS MEDIA_TV_BRAND_XFINITY,
    
    -- Audio Streaming Services
    source_data.rs_audio_brand_pandora AS MEDIA_AUDIO_BRAND_PANDORA,
    source_data.rs_audio_brand_sirius_xm AS MEDIA_AUDIO_BRAND_SIRIUS_XM,
    source_data.rs_audio_brand_spotify AS MEDIA_AUDIO_BRAND_SPOTIFY,
    
    -- Video Streaming Services
    source_data.rs_video_brand_hbo AS MEDIA_VIDEO_BRAND_HBO,
    source_data.rs_video_brand_hulu AS MEDIA_VIDEO_BRAND_HULU,
    source_data.rs_video_brand_netflix AS MEDIA_VIDEO_BRAND_NETFLIX,
    source_data.rs_video_brand_sling_tv AS MEDIA_VIDEO_BRAND_SLING_TV,
    source_data.rs_video_brand_vudu AS MEDIA_VIDEO_BRAND_VUDU,
    
    -- TV/Movie Content Preferences - Genres
    source_data.tv_mov_horror AS MEDIA_TV_MOV_HORROR,
    source_data.tv_mov_comedy_fan AS MEDIA_TV_MOV_COMEDY,
    source_data.tv_mov_drama_fan AS MEDIA_TV_MOV_DRAMA,
    source_data.tv_mov_drama_movies AS MEDIA_TV_MOV_DRAMA_MOVIES,
    source_data.tv_mov_adventure_mov AS MEDIA_TV_MOV_ADVENTURE,
    source_data.tv_mov_family_films_ AS MEDIA_TV_MOV_FAMILY_FILMS,
    source_data.tv_mov_romantic_com AS MEDIA_TV_MOV_ROMANTIC_COMEDY,
    source_data.tv_mov_scifi_movie AS MEDIA_TV_MOV_SCIFI,
    source_data.tv_mov_thriller_mov AS MEDIA_TV_MOV_THRILLER,
    source_data.tv_mov_docu_foreign AS MEDIA_TV_MOV_DOCU_FOREIGN,
    source_data.tv_mov_cult_classic AS MEDIA_TV_MOV_CULT_CLASSIC,
    
    -- TV/Movie Content Preferences - Content Types
    source_data.tv_mov_reality_tv AS MEDIA_TV_MOV_REALITY_TV,
    source_data.tv_mov_game_shows AS MEDIA_TV_MOV_GAME_SHOWS,
    source_data.tv_mov_tv_news AS MEDIA_TV_MOV_TV_NEWS,
    source_data.tv_mov_tv_animation AS MEDIA_TV_MOV_TV_ANIMATION,
    source_data.tv_mov_tv_history AS MEDIA_TV_MOV_TV_HISTORY,
    source_data.tv_mov_tv_how_to AS MEDIA_TV_MOV_TV_HOW_TO,
    source_data.tv_mov_tv_mov AS MEDIA_TV_MOV_TV_MOV,
    
    -- TV/Movie Content Preferences - Event Viewing
    source_data.tv_mov_oscars AS MEDIA_TV_MOV_OSCARS,
    source_data.tv_mov_grammy_watcher AS MEDIA_TV_MOV_GRAMMY,
    source_data.tv_mov_summer_olym AS MEDIA_TV_MOV_SUMMER_OLYMPICS,
    source_data.tv_mov_winter_olym AS MEDIA_TV_MOV_WINTER_OLYMPICS,
    source_data.tv_mov_movie_opening AS MEDIA_TV_MOV_MOVIE_OPENING,
    source_data.tv_mov_oscars_fashin AS MEDIA_TV_MOV_OSCARS_FASHION,
    
    -- TV/Movie Content Preferences - Sports Content
    source_data.tv_mov_collg_basketbl AS MEDIA_TV_MOV_COLLEGE_BASKETBALL,
    source_data.tv_mov_collg_footbl AS MEDIA_TV_MOV_COLLEGE_FOOTBALL,
    source_data.tv_mov_tennis_on_tv AS MEDIA_TV_MOV_TENNIS,
    
    -- TV/Movie Content Preferences - Platform/Demographic Specific
    source_data.tv_mov_hbo_watcher AS MEDIA_TV_MOV_HBO_WATCHER,
    source_data.tv_mov_redbox AS MEDIA_TV_MOV_REDBOX,
    source_data.tv_mov_stream AS MEDIA_TV_MOV_STREAMING,
    source_data.tv_mov_female_tv AS MEDIA_TV_MOV_FEMALE_TV,
    source_data.tv_mov_guy_shows AS MEDIA_TV_MOV_GUY_SHOWS,
    source_data.tv_mov_oprah_fan AS MEDIA_TV_MOV_OPRAH_FAN,
    source_data.tv_mov_freq_movie AS MEDIA_TV_MOV_FREQUENT_MOVIE,
    source_data.tv_mov_disc_fastnloud AS MEDIA_TV_MOV_DISCOVERY_HISTORY,
    source_data.tv_mov_top_chef_tv AS MEDIA_TV_MOV_TOP_CHEF,
    
    -- Music Interests - Genres
    source_data.act_int_listens_to_80s_music AS MEDIA_MUSIC_80S,
    source_data.act_int_listens_to_alternative_music AS MEDIA_MUSIC_ALTERNATIVE,
    source_data.act_int_listens_to_christian_music AS MEDIA_MUSIC_CHRISTIAN,
    source_data.act_int_listens_to_classical_music AS MEDIA_MUSIC_CLASSICAL,
    source_data.act_int_listens_to_country_music AS MEDIA_MUSIC_COUNTRY,
    source_data.act_int_listens_to_hip_hop_music AS MEDIA_MUSIC_HIP_HOP,
    source_data.act_int_listens_to_jazz_music AS MEDIA_MUSIC_JAZZ,
    source_data.act_int_listens_to_oldies_music AS MEDIA_MUSIC_OLDIES,
    source_data.act_int_listens_to_pop_music AS MEDIA_MUSIC_POP,
    source_data.act_int_listens_to_rock_music AS MEDIA_MUSIC_ROCK,
    source_data.act_int_listens_to_music AS MEDIA_MUSIC_GENERAL,
    
    -- Music Interests - Platforms
    source_data.act_int_music_download AS MEDIA_MUSIC_DOWNLOAD,
    source_data.act_int_music_streaming AS MEDIA_MUSIC_STREAMING,
    
    -- Other Media-Related Interests
    source_data.online_watch_tv_movies AS MEDIA_ONLINE_WATCH_TV_MOVIES,
    source_data.act_int_video_gamer AS MEDIA_VIDEO_GAMER,
    source_data.act_int_political_viewing_on_tv_conservative AS MEDIA_POLITICAL_TV_CONSERVATIVE,
    source_data.act_int_political_viewing_on_tv_liberal AS MEDIA_POLITICAL_TV_LIBERAL,
    source_data.act_int_political_viewing_on_tv_liberal_comedy AS MEDIA_POLITICAL_TV_LIBERAL_COMEDY
    
FROM source_data

