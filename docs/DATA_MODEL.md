## Data Model ERD

### Entity Relationship Diagram

```mermaid
erDiagram
    INDIVIDUAL_DEMOGRAPHIC_SPINE ||--o{ CARD : "has"
    CARD ||--o{ TRANSACTION : "used_in"
    TRANSACTION }o--|| MERCHANT : "at"
    TRANSACTION }o--o{ BRAND_TAGGING : "tagged_as"
    BRAND_TAGGING }o--|| BRAND_TAXONOMY : "classified_in"
    BRAND_TAGGING }o--|| BRAND_LOCATION : "located_at"
    CARD ||--o| DEMOGRAPHICS_TRANSACTIONS : "has"
    INDIVIDUAL_DEMOGRAPHIC_SPINE ||--o{ FACT_TRANSACTION_ENRICHED : "has"
    
    INDIVIDUAL_DEMOGRAPHIC_SPINE {
        string afs_individual_id PK
        string city_plac
        string stat_abbr
        string recd_zipc
        string gndr_gndr_1
        string age_range_2529
        string lu_inc_model_v6
        string combined_age_1
        timestamp load_timestamp
    }
    
    CARD {
        string membccid PK
        string afs_individual_id FK
        string card_zip
        string card_type
        string areaid
        date delivery_date
        timestamp load_timestamp
    }
    
    TRANSACTION {
        string txid PK
        string mtid FK
        string membccid FK
        date trans_date
        time trans_time
        string trans_time_zone
        float trans_amount
        date delivery_date
        timestamp load_timestamp
    }
    
    MERCHANT {
        string mtid PK
        string MERCH_DESC
        string mcc
        string merch_city
        string merch_state
        string merch_zip
        string merch_country
        date delivery_date
        timestamp load_timestamp
    }
    
    BRAND_TAGGING {
        string mtid PK
        int store_id PK
        int brand_id PK
        string locationid FK
        string channel
        timestamp load_timestamp
    }
    
    BRAND_TAXONOMY {
        int store_id PK
        int brand_id PK
        string store_name
        string brand_name
        string store_type
        string brand_type
        int brand_tagging_classification
        timestamp load_timestamp
    }
    
    BRAND_LOCATION {
        string locationid PK
        string address
        string city
        string state
        string zip
        string country
        string est_open_date
        string est_close_date
        timestamp load_timestamp
    }
    
    DEMOGRAPHICS_TRANSACTIONS {
        string membccid PK
        string income
        int wealth
        string ethnicity
        string politics
        string ADULTS_IN_HH
        string age
        string business_owner
        string children
        string homeowner_probability
        string gender
        timestamp load_timestamp
    }
    
    FACT_TRANSACTION_ENRICHED {
        string txid PK
        string afs_individual_id FK
        string mtid FK
        date trans_date
        time trans_time
        float trans_amount
        string merchant_description
        string merchant_category_code
        string brand_name
        string store_name
        string transaction_channel
        string store_city
        string store_state
    }
```

- **INDIVIDUAL_DEMOGRAPHIC_SPINE** (1) ──────< (N) **CARD**: One individual can have many cards
- **CARD** (1) ──────< (N) **TRANSACTION**: One card can have many transactions
- **TRANSACTION** (N) ──────> (1) **MERCHANT**: Many transactions can be at one merchant
- **TRANSACTION** (N) ──────< (M) **BRAND_TAGGING**: Many transactions can be tagged with many brands
- **BRAND_TAGGING** (N) ──────> (1) **BRAND_TAXONOMY**: Many brand taggings reference one brand taxonomy
- **BRAND_TAGGING** (N) ──────> (1) **BRAND_LOCATION**: Many brand taggings reference one location
- **INDIVIDUAL_DEMOGRAPHIC_SPINE** (1) ──────< (N) **FACT_TRANSACTION_ENRICHED**: Many transactions belong to one individual

### Data Model Notes

- **DIM_INDIVIDUAL** (from `INDIVIDUAL_DEMOGRAPHIC_SPINE`): One row per individual with 800+ demographic attributes
- **FACT_TRANSACTION_ENRICHED**: Denormalized transaction table with `afs_individual_id` for easy joining
- Both tables have `afs_individual_id` as a bridge for flexible querying
- Transactions are kept separate from individual attributes for optimal LLM query performance