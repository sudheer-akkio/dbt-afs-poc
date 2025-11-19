-- ============================================================================
-- TABLE DEFINITIONS
-- ============================================================================
-- This script creates all tables based on the data dictionary
-- Delivery Date: 2025-11-10
-- Run this after 01_setup.sql
-- ============================================================================

USE SCHEMA DEMO.AFS_POC;

-- DEMO TRANSACTION TABLE
CREATE OR REPLACE TABLE DEMO.AFS_POC.DEMOGRAPHICS_TRANSACTIONS (
  membccid VARCHAR COMMENT 'Identifier of card. Unique for each card.',
  income VARCHAR COMMENT 'Estimated income range of a living unit.',
  wealth INTEGER COMMENT 'Predicted household net worth.',
  ethnicity VARCHAR COMMENT 'Individual ethnicity.',
  politics VARCHAR COMMENT 'Individual political affiliation.',
  ADULTS_IN_HH VARCHAR COMMENT 'Number of adults in household.',
  age VARCHAR COMMENT 'Individual age group.',
  business_owner VARCHAR COMMENT 'Business owner flag.',
  children VARCHAR COMMENT 'Number of children in the living unit.',
  homeowner_probability VARCHAR COMMENT 'Homeowner or renter flag.',
  gender VARCHAR COMMENT 'Individual gender.',
  load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- MERCHANT TABLE
-- Field descriptions per data dictionary:
CREATE OR REPLACE TABLE DEMO.AFS_POC.MERCHANT (
  mtid VARCHAR COMMENT 'Affinity generated pseudo/synthetic identifiers for a group of transactions.',
  MERCH_DESC VARCHAR COMMENT 'Merchant description - PII scrubbed by Affinity.',
  mcc VARCHAR(4) COMMENT '4 digit Merchant Category Code received from the FI.',
  merch_city VARCHAR COMMENT 'Merchant City as received from the financial institution.',
  merch_state VARCHAR COMMENT 'Merchant State as received from the financial institution.',
  merch_zip VARCHAR(5) COMMENT 'Merchant 5 digit Zip as received from the financial institution.',
  merch_country VARCHAR COMMENT 'Country code as received from the financial institution.',
  delivery_date DATE COMMENT 'Date when the data is delivered by Affinity Solutions',
  load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- CARD TABLE
-- Field descriptions per data dictionary:
CREATE OR REPLACE TABLE DEMO.AFS_POC.CARD (
  membccid VARCHAR COMMENT 'Affinity generated pseudo/synthetic identifier of a card.',
  card_zip VARCHAR COMMENT 'Most recent zip code associated with the card.',
  card_type VARCHAR COMMENT 'Most recent card type code from the financial institution.',
  areaid VARCHAR COMMENT 'Synthetic ID associated with card portfolio.',
  afs_individual_id VARCHAR COMMENT 'Affinity generated identifier of an individual.',
  delivery_date DATE COMMENT 'Date when the data is delivered by Affinity Solutions',
  load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- TRANSACTION TABLE
-- Field descriptions per data dictionary:
CREATE OR REPLACE TABLE DEMO.AFS_POC.TRANSACTION (
  txid VARCHAR COMMENT 'Affinity generated pseudo/synthetic identifier of a transaction. Unique for each transaction.',
  mtid VARCHAR COMMENT 'Affinity generated pseudo/synthetic identifiers for a group of transactions that has the same attributes. This may represent a merchant terminal.',
  membccid VARCHAR COMMENT 'Affinity generated pseudo/synthetic identifier of a card. This links transaction to the card member information.',
  trans_date DATE COMMENT 'Date when the transaction was authorized. Format is YYYY-MM-DD.',
  trans_time TIME COMMENT 'Transaction time provided with the transaction. Format HH24:MI:SS.',
  trans_time_zone VARCHAR COMMENT 'The timezone of the transaction time if available.',
  trans_amount FLOAT COMMENT 'Perturbed transaction amount in US dollars.',
  delivery_date DATE COMMENT 'Date when the data is delivered by Affinity Solutions',
  load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- BRAND TAGGING TABLE
-- Field descriptions per data dictionary:
CREATE OR REPLACE TABLE DEMO.AFS_POC.BRAND_TAGGING (
  mtid VARCHAR COMMENT 'Identifier of a merchant terminal.',
  store_id INTEGER COMMENT 'Identifier of the store under a brand.',
  brand_id INTEGER COMMENT 'Identifier of the brand.',
  channel VARCHAR COMMENT 'Transaction channel (ONLINE, B&M).',
  locationid VARCHAR COMMENT 'Identifier of a store location.',
  load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- BRAND LOCATION TABLE
-- Field descriptions per data dictionary:
CREATE OR REPLACE TABLE DEMO.AFS_POC.BRAND_LOCATION (
  locationid VARCHAR COMMENT 'Identifier of a store location.',
  address VARCHAR COMMENT 'Address of the store location.',
  city VARCHAR COMMENT 'City of the store location.',
  state VARCHAR COMMENT 'State of the store location.',
  zip VARCHAR COMMENT 'Zip code of the store location.',
  country VARCHAR COMMENT 'Country of the store location.',
  est_open_date VARCHAR COMMENT 'Estimated open date of the store location.',
  est_close_date VARCHAR COMMENT 'Estimated close date of the store location.',
  load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- BRAND TAXONOMY TABLE
-- Field descriptions per data dictionary:
CREATE OR REPLACE TABLE DEMO.AFS_POC.BRAND_TAXONOMY (
  store_id INTEGER COMMENT 'Identifier of the store under a brand.',
  brand_id INTEGER COMMENT 'Identifier of the brand.',
  store_name VARCHAR COMMENT 'Name of the store under a brand.',
  brand_name VARCHAR COMMENT 'Name of the merchant / brand.',
  store_type VARCHAR COMMENT 'Affinity assigned store type (D/P).',
  brand_type VARCHAR COMMENT 'Affinity assigned brand type (D/P).',
  brand_tagging_classification INTEGER COMMENT 'Tagging level: 1 = machine + human, 0 = machine only.',
  load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

