# DBT Pipeline for AFS POC

## Quick Start

### Setup

1. Create and activate virtual environment

    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

2. Install dependencies

    ```bash
    pip install dbt-core dbt-snowflake
    ```

3. Install dbt packages (if any)

    ```bash
    dbt deps
    ```

4. Configure profile in `~/.dbt/profiles.yml`

    ```yaml
    afs_poc_snowflake:
        outputs:
            dev:
                type: snowflake
                account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
                user: "{{ env_var('SNOWFLAKE_USER') }}"
                password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
                role: "{{ env_var('SNOWFLAKE_ROLE', 'ACCOUNTADMIN') }}"
                database: DEMO
                warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE') }}"
                schema: AFS_POC
                threads: 4
        target: dev
    ```

### Data Loading

Before running dbt models, ensure source data is loaded into Snowflake:

1. Navigate to the `data_loader/` directory
2. Run the SQL scripts in order:
   - `01_setup.sql` - Creates schema, S3 stage, and file formats
   - `02a_create_spine_table.sql` - Creates individual demographic spine table
   - `02b_create_transaction_tables.sql` - Creates transaction-related tables
   - `03a_load_data_spine.sql` - Loads demographic data from S3
   - `03b_load_data_transactions.sql` - Loads transaction data from S3
   - `04_verify.sql` - Verifies data loads

See `data_loader/README.md` for detailed information.

### Running the Pipeline

```bash
# Run all models
dbt run

# Run specific model
dbt run --select fact_transaction_enriched

# Run all models in the afs directory
dbt run --select afs

# Build and test together
dbt build --select afs

# Run tests only
dbt test --select afs
```

## Project Structure

```text
dbt-afs-poc/
├── models/afs/              # AFS data models
│   ├── schema.yml           # Model documentation + generic tests
│   ├── sources.yml          # Source table definitions
│   ├── fact_transaction_enriched.sql
│   ├── fact_transaction_summary.sql
│   ├── v_akkio_attributes_latest.sql
│   ├── v_agg_akkio_hh.sql
│   └── v_agg_akkio_ind.sql
├── data_loader/             # SQL scripts for loading data from S3
│   ├── 01_setup.sql
│   ├── 02a_create_spine_table.sql
│   ├── 02b_create_transaction_tables.sql
│   ├── 03a_load_data_spine.sql
│   ├── 03b_load_data_transactions.sql
│   ├── 04_verify.sql
│   └── README.md
├── docs/                     # Documentation
│   └── DATA_MODEL.md        # Data model ERD and documentation
├── tests/                    # Singular SQL tests
├── dbt_project.yml           # Project configuration
└── README.md                 # This file
```

## Models

### Fact Tables

1. **Transaction Facts**
   - `fact_transaction_summary` - Daily transaction summary table aggregated by trans_date and AKKIO_ID. Optimized for RAG engine queries with transaction metrics, aggregated merchant/brand attributes, and unique counts. Preferred table for most analytics queries.
     - **Grain**: One row per day per individual (trans_date, AKKIO_ID)
     - **Materialization**: Table (clustered by trans_date, AKKIO_ID)
     - **Source**: fact_transaction_enriched
   
   - `fact_transaction_enriched` - Detail transaction fact table with AKKIO_ID for easy joining to attributes. Includes merchant, brand, and location attributes. Use only when transaction-level detail is required; prefer fact_transaction_summary for most queries. Joins 6 source tables: TRANSACTION, CARD, MERCHANT, BRAND_TAGGING, BRAND_TAXONOMY, BRAND_LOCATION.
     - **Grain**: One row per transaction (txid)
     - **Materialization**: Incremental table (clustered by trans_date, AKKIO_ID)

### Dimension Tables

1. **Individual Attributes**
   - `v_akkio_attributes_latest` - Individual attributes dimension with all demographic attributes from AFS spine. One row per individual with normalized demographic fields including gender, age, ethnicity, income, wealth, politics, education, and geographic attributes.
   - **Grain**: One row per individual (AKKIO_ID)
   - **Materialization**: Table (clustered by AKKIO_ID)

2. **Aggregated Attributes**
   - `v_agg_akkio_ind` - Individual-level aggregation of demographic attributes for analytics. Includes weight, demographics, household attributes, political and business attributes, and geographic information.
   - **Grain**: One row per individual (AKKIO_ID)
   - **Materialization**: Table (clustered by PARTITION_DATE, AKKIO_ID)

   - `v_agg_akkio_hh` - Household-level aggregation of demographic attributes for analytics. Currently 1:1 with individuals but structured for future household scenarios.
   - **Grain**: One row per household (AKKIO_HH_ID)
   - **Materialization**: Table (clustered by PARTITION_DATE, AKKIO_HH_ID)

## Source Tables

The dbt models reference the following source tables in `DEMO.AFS_POC`:

- `INDIVIDUAL_DEMOGRAPHIC_SPINE` - Individual demographic spine with 800+ attributes
- `TRANSACTION` - Transaction fact records
- `CARD` - Card dimension linking cards to individuals
- `MERCHANT` - Merchant dimension with location and MCC codes
- `BRAND_TAGGING` - Brand tagging relationships linking transactions to brands
- `BRAND_TAXONOMY` - Brand and store taxonomy classifications
- `BRAND_LOCATION` - Store location details

## Development Workflow

1. **Load Source Data**: Run data loader scripts in `data_loader/` directory
2. **Run dbt Models**: Execute `dbt run` to build all models
3. **Test Models**: Run `dbt test` to validate data quality
4. **Documentation**: Generate docs with `dbt docs generate` and view with `dbt docs serve`

## Resources

### dbt Resources

- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

### Project Resources

- [DATA_MODEL.md](docs/DATA_MODEL.md) - Detailed data model ERD and documentation
- [Data Loader README](data_loader/README.md) - Instructions for loading source data from S3
- [Snowflake SQL Reference](https://docs.snowflake.com/en/sql-reference-commands.html)
