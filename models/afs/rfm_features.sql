{{ config(
    alias='RFM_FEATURES',
    materialized='table',
    post_hook=[
        "ALTER TABLE {{this}} CLUSTER BY (AKKIO_ID)"
    ]
) }}

-- ============================================================================
-- RFM_FEATURES: Pre-materialized RFM (Recency, Frequency, Monetary) features
-- Mirrors the Affinity Solutions data mart feature set with 5 time windows.
-- Source: fact_transaction_enriched
-- Grain: One row per AKKIO_ID
--
-- This table eliminates the need for lookalike and audience queries to scan
-- the full transaction table. Queries reference this table instead.
--
-- CONFIGURABLE REFERENCE DATE:
--   By default, ref_date = MAX(TRANS_DATE) (all available transaction data).
--   Override with: dbt run -s rfm_features --vars '{"rfm_ref_date": "2025-08-31"}'
--   This allows building RFM features with a temporal cutoff for holdout
--   validation (e.g., exclude September data when validating against September).
--   The upper-bound filter uses strict less-than (TRANS_DATE < ref_date + 1 day)
--   so that the ref_date itself is INCLUDED in the feature window.
--
-- FEATURE SET (per window: 12mo, 9mo, 6mo, 3mo, 1mo):
--   tot_trans_{window}           - Total transaction count
--   tot_spend_{window}           - Total spend amount
--   tot_online_trans_{window}    - Online transaction count
--   tot_online_spend_{window}    - Online spend amount
--   avg_days_btwn_trans_{window} - Average days between transactions (cadence)
--   brand_diversity_{window}     - Count of distinct brands transacted with
--
-- ADDITIONAL FEATURES:
--   last_txn_date                - Most recent transaction date
--   days_since_last_txn          - Days from last txn to reference date
--   online_ratio_12mo            - Online transaction ratio (12mo)
-- ============================================================================

WITH ref AS (
    {% if var('rfm_ref_date', none) is not none %}
    SELECT '{{ var("rfm_ref_date") }}'::DATE AS ref_date
    {% else %}
    SELECT MAX(TRANS_DATE) AS ref_date
    FROM {{ ref('fact_transaction_enriched') }}
    {% endif %}
)

SELECT
    f.AKKIO_ID,

    -- Reference date used for this build (for downstream transparency)
    (SELECT ref_date FROM ref) AS rfm_ref_date,

    -- Recency
    MAX(f.TRANS_DATE) AS last_txn_date,
    DATEDIFF(DAY, MAX(f.TRANS_DATE), (SELECT ref_date FROM ref)) AS days_since_last_txn,

    -- =========================================================================
    -- 12-MONTH WINDOW (full scan range — no CASE WHEN needed)
    -- =========================================================================
    COUNT(*) AS tot_trans_12mo,
    COALESCE(SUM(f.TRANS_AMOUNT), 0) AS tot_spend_12mo,
    COUNT(CASE WHEN f.TRANSACTION_CHANNEL = 'ONLINE' THEN 1 END) AS tot_online_trans_12mo,
    COALESCE(SUM(CASE WHEN f.TRANSACTION_CHANNEL = 'ONLINE' THEN f.TRANS_AMOUNT END), 0) AS tot_online_spend_12mo,
    CASE
        WHEN COUNT(*) > 1
        THEN DATEDIFF(DAY, MIN(f.TRANS_DATE), MAX(f.TRANS_DATE))::FLOAT
             / (COUNT(*) - 1)
        ELSE NULL
    END AS avg_days_btwn_trans_12mo,
    COUNT(DISTINCT f.BRAND_NAME) AS brand_diversity_12mo,

    -- Pre-computed ratio for convenience
    CASE
        WHEN COUNT(*) > 0
        THEN COUNT(CASE WHEN f.TRANSACTION_CHANNEL = 'ONLINE' THEN 1 END)::FLOAT / COUNT(*)
        ELSE NULL
    END AS online_ratio_12mo,

    -- =========================================================================
    -- 9-MONTH WINDOW
    -- =========================================================================
    COUNT(
        CASE WHEN f.TRANS_DATE >= DATEADD(MONTH, -9, (SELECT ref_date FROM ref)) THEN 1 END
    ) AS tot_trans_9mo,
    COALESCE(SUM(
        CASE WHEN f.TRANS_DATE >= DATEADD(MONTH, -9, (SELECT ref_date FROM ref)) THEN f.TRANS_AMOUNT END
    ), 0) AS tot_spend_9mo,
    COUNT(
        CASE WHEN f.TRANSACTION_CHANNEL = 'ONLINE'
              AND f.TRANS_DATE >= DATEADD(MONTH, -9, (SELECT ref_date FROM ref)) THEN 1 END
    ) AS tot_online_trans_9mo,
    COALESCE(SUM(
        CASE WHEN f.TRANSACTION_CHANNEL = 'ONLINE'
              AND f.TRANS_DATE >= DATEADD(MONTH, -9, (SELECT ref_date FROM ref)) THEN f.TRANS_AMOUNT END
    ), 0) AS tot_online_spend_9mo,
    CASE
        WHEN COUNT(CASE WHEN f.TRANS_DATE >= DATEADD(MONTH, -9, (SELECT ref_date FROM ref)) THEN 1 END) > 1
        THEN DATEDIFF(DAY,
                MIN(CASE WHEN f.TRANS_DATE >= DATEADD(MONTH, -9, (SELECT ref_date FROM ref)) THEN f.TRANS_DATE END),
                MAX(CASE WHEN f.TRANS_DATE >= DATEADD(MONTH, -9, (SELECT ref_date FROM ref)) THEN f.TRANS_DATE END)
             )::FLOAT
             / NULLIF(COUNT(CASE WHEN f.TRANS_DATE >= DATEADD(MONTH, -9, (SELECT ref_date FROM ref)) THEN 1 END) - 1, 0)
        ELSE NULL
    END AS avg_days_btwn_trans_9mo,
    COUNT(DISTINCT
        CASE WHEN f.TRANS_DATE >= DATEADD(MONTH, -9, (SELECT ref_date FROM ref)) THEN f.BRAND_NAME END
    ) AS brand_diversity_9mo,

    -- =========================================================================
    -- 6-MONTH WINDOW
    -- =========================================================================
    COUNT(
        CASE WHEN f.TRANS_DATE >= DATEADD(MONTH, -6, (SELECT ref_date FROM ref)) THEN 1 END
    ) AS tot_trans_6mo,
    COALESCE(SUM(
        CASE WHEN f.TRANS_DATE >= DATEADD(MONTH, -6, (SELECT ref_date FROM ref)) THEN f.TRANS_AMOUNT END
    ), 0) AS tot_spend_6mo,
    COUNT(
        CASE WHEN f.TRANSACTION_CHANNEL = 'ONLINE'
              AND f.TRANS_DATE >= DATEADD(MONTH, -6, (SELECT ref_date FROM ref)) THEN 1 END
    ) AS tot_online_trans_6mo,
    COALESCE(SUM(
        CASE WHEN f.TRANSACTION_CHANNEL = 'ONLINE'
              AND f.TRANS_DATE >= DATEADD(MONTH, -6, (SELECT ref_date FROM ref)) THEN f.TRANS_AMOUNT END
    ), 0) AS tot_online_spend_6mo,
    CASE
        WHEN COUNT(CASE WHEN f.TRANS_DATE >= DATEADD(MONTH, -6, (SELECT ref_date FROM ref)) THEN 1 END) > 1
        THEN DATEDIFF(DAY,
                MIN(CASE WHEN f.TRANS_DATE >= DATEADD(MONTH, -6, (SELECT ref_date FROM ref)) THEN f.TRANS_DATE END),
                MAX(CASE WHEN f.TRANS_DATE >= DATEADD(MONTH, -6, (SELECT ref_date FROM ref)) THEN f.TRANS_DATE END)
             )::FLOAT
             / NULLIF(COUNT(CASE WHEN f.TRANS_DATE >= DATEADD(MONTH, -6, (SELECT ref_date FROM ref)) THEN 1 END) - 1, 0)
        ELSE NULL
    END AS avg_days_btwn_trans_6mo,
    COUNT(DISTINCT
        CASE WHEN f.TRANS_DATE >= DATEADD(MONTH, -6, (SELECT ref_date FROM ref)) THEN f.BRAND_NAME END
    ) AS brand_diversity_6mo,

    -- =========================================================================
    -- 3-MONTH WINDOW
    -- =========================================================================
    COUNT(
        CASE WHEN f.TRANS_DATE >= DATEADD(MONTH, -3, (SELECT ref_date FROM ref)) THEN 1 END
    ) AS tot_trans_3mo,
    COALESCE(SUM(
        CASE WHEN f.TRANS_DATE >= DATEADD(MONTH, -3, (SELECT ref_date FROM ref)) THEN f.TRANS_AMOUNT END
    ), 0) AS tot_spend_3mo,
    COUNT(
        CASE WHEN f.TRANSACTION_CHANNEL = 'ONLINE'
              AND f.TRANS_DATE >= DATEADD(MONTH, -3, (SELECT ref_date FROM ref)) THEN 1 END
    ) AS tot_online_trans_3mo,
    COALESCE(SUM(
        CASE WHEN f.TRANSACTION_CHANNEL = 'ONLINE'
              AND f.TRANS_DATE >= DATEADD(MONTH, -3, (SELECT ref_date FROM ref)) THEN f.TRANS_AMOUNT END
    ), 0) AS tot_online_spend_3mo,
    CASE
        WHEN COUNT(CASE WHEN f.TRANS_DATE >= DATEADD(MONTH, -3, (SELECT ref_date FROM ref)) THEN 1 END) > 1
        THEN DATEDIFF(DAY,
                MIN(CASE WHEN f.TRANS_DATE >= DATEADD(MONTH, -3, (SELECT ref_date FROM ref)) THEN f.TRANS_DATE END),
                MAX(CASE WHEN f.TRANS_DATE >= DATEADD(MONTH, -3, (SELECT ref_date FROM ref)) THEN f.TRANS_DATE END)
             )::FLOAT
             / NULLIF(COUNT(CASE WHEN f.TRANS_DATE >= DATEADD(MONTH, -3, (SELECT ref_date FROM ref)) THEN 1 END) - 1, 0)
        ELSE NULL
    END AS avg_days_btwn_trans_3mo,
    COUNT(DISTINCT
        CASE WHEN f.TRANS_DATE >= DATEADD(MONTH, -3, (SELECT ref_date FROM ref)) THEN f.BRAND_NAME END
    ) AS brand_diversity_3mo,

    -- =========================================================================
    -- 1-MONTH WINDOW
    -- =========================================================================
    COUNT(
        CASE WHEN f.TRANS_DATE >= DATEADD(MONTH, -1, (SELECT ref_date FROM ref)) THEN 1 END
    ) AS tot_trans_1mo,
    COALESCE(SUM(
        CASE WHEN f.TRANS_DATE >= DATEADD(MONTH, -1, (SELECT ref_date FROM ref)) THEN f.TRANS_AMOUNT END
    ), 0) AS tot_spend_1mo,
    COUNT(
        CASE WHEN f.TRANSACTION_CHANNEL = 'ONLINE'
              AND f.TRANS_DATE >= DATEADD(MONTH, -1, (SELECT ref_date FROM ref)) THEN 1 END
    ) AS tot_online_trans_1mo,
    COALESCE(SUM(
        CASE WHEN f.TRANSACTION_CHANNEL = 'ONLINE'
              AND f.TRANS_DATE >= DATEADD(MONTH, -1, (SELECT ref_date FROM ref)) THEN f.TRANS_AMOUNT END
    ), 0) AS tot_online_spend_1mo,
    CASE
        WHEN COUNT(CASE WHEN f.TRANS_DATE >= DATEADD(MONTH, -1, (SELECT ref_date FROM ref)) THEN 1 END) > 1
        THEN DATEDIFF(DAY,
                MIN(CASE WHEN f.TRANS_DATE >= DATEADD(MONTH, -1, (SELECT ref_date FROM ref)) THEN f.TRANS_DATE END),
                MAX(CASE WHEN f.TRANS_DATE >= DATEADD(MONTH, -1, (SELECT ref_date FROM ref)) THEN f.TRANS_DATE END)
             )::FLOAT
             / NULLIF(COUNT(CASE WHEN f.TRANS_DATE >= DATEADD(MONTH, -1, (SELECT ref_date FROM ref)) THEN 1 END) - 1, 0)
        ELSE NULL
    END AS avg_days_btwn_trans_1mo,
    COUNT(DISTINCT
        CASE WHEN f.TRANS_DATE >= DATEADD(MONTH, -1, (SELECT ref_date FROM ref)) THEN f.BRAND_NAME END
    ) AS brand_diversity_1mo

FROM {{ ref('fact_transaction_enriched') }} f
WHERE f.AKKIO_ID IS NOT NULL
  AND f.TRANS_DATE >= DATEADD(MONTH, -12, (SELECT ref_date FROM ref))
  AND f.TRANS_DATE <= (SELECT ref_date FROM ref)
GROUP BY f.AKKIO_ID
