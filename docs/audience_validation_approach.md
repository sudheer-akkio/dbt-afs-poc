# Audience Validation Framework

## Overview

This document describes the audience validation approach used to measure and compare the quality of audiences built on the Akkio + Affinity Solutions (AFS) data platform. The framework provides a standardized, repeatable methodology for evaluating how well any audience — whether a known-shopper seed, a lookalike expansion, or a propensity-scored cohort — predicts real consumer purchase behavior during a future holdout period.

The goal is to provide **apples-to-apples comparisons** across audience construction methods, enabling data-driven decisions about which approach delivers the highest-quality audiences for activation.

---

## Key Concepts

### What We Measure

Every audience is evaluated against the same set of metrics, calculated over a **holdout window** — a time period that was deliberately excluded when the audience was built. This ensures we are measuring true predictive power, not just historical behavior.

| Metric | Formula | What It Tells You |
|---|---|---|
| **Total Audience IDs** | Count of distinct IDs in the audience | Audience reach / size |
| **Active Matched IDs** | Audience IDs with any transaction in the holdout period | Observable panel coverage within the audience |
| **Brand Shoppers** | Audience IDs with at least one brand transaction in holdout | How many audience members actually purchased |
| **Brand Transactions** | Total brand transactions from audience members in holdout | Volume of purchase activity |
| **Brand Spend** | Total dollar amount of brand transactions in holdout | Revenue impact |
| **Shop Rate** | Brand Shoppers / Active Matched IDs | % of active audience members who purchased the brand |
| **Spend Rate** | Brand Spend / Active Matched IDs | Average brand spend per active audience member |
| **Avg Ticket** | Brand Spend / Brand Transactions | Average purchase size |
| **Avg Transactions per Shopper** | Brand Transactions / Brand Shoppers | Purchase frequency among converters |

> **Why Active Matched IDs is the denominator (not Total Audience IDs):**
> Not every individual in the panel will have observable transaction activity in any given month. Using Total Active Matched IDs — audience members who had at least one transaction of *any* kind during the holdout — ensures we measure audience quality against observable behavior, rather than penalizing for panel coverage gaps.

### Seed vs. Lookalike — Why Both Matter

| Audience Type | What It Measures | Expected Behavior |
|---|---|---|
| **Seed (Known Shoppers)** | Retention / repeat purchase — "Do existing buyers keep buying?" | Higher shop rate (selecting on known buyers) |
| **Lookalike / Propensity** | Acquisition / prediction — "Can we find NEW brand shoppers before they buy?" | Lower shop rate, but demonstrates true predictive lift |

A **seed audience** of known brand shoppers validated in a holdout window measures **retention** — these individuals already purchased, so the shop rate will be naturally high. This is useful for understanding audience stickiness but does not demonstrate predictive modeling power.

A **lookalike audience** or **propensity-scored cohort** of individuals who have *not* been observed purchasing the brand measures **acquisition** — the ability to identify future brand shoppers before they convert. This is the more meaningful test of audience quality and the direct comparison between modeling approaches.

---

## Validation Methodology

### Timeline Split

The validation uses a strict temporal holdout design to prevent data leakage:

| Period | Role |
|---|---|
| All data through the build cutoff date | Used for audience construction (seed identification, feature engineering, model training) |
| Holdout window (post-cutoff) | Used **exclusively** for measuring audience performance — never seen during audience building |

For the current validation cycle, the holdout window is **September 1, 2025 through September 30, 2025**.

### How Validation Works

```
┌──────────────────────────────────────┐     ┌──────────────────────────┐
│         AUDIENCE BUILDING            │     │    HOLDOUT VALIDATION    │
│                                      │     │                          │
│  Transaction history + attributes    │     │  Unseen future period    │
│  used to identify seeds and build    │────▶│  used to measure actual  │
│  lookalike/propensity audiences      │     │  purchase behavior       │
│                                      │     │                          │
│  ◄── All data through cutoff ──►     │     │  ◄── Holdout window ──►  │
└──────────────────────────────────────┘     └──────────────────────────┘
```

**Step-by-step process:**

1. **Build the audience** using all transaction and attribute data up to the cutoff date
2. **Freeze the audience** — the list of IDs is fixed before looking at holdout data
3. **Join audience IDs** against all transactions in the holdout window
4. **Identify active members** — audience IDs that had *any* transaction (any brand) during the holdout
5. **Identify brand converters** — audience IDs that had at least one brand-specific transaction during the holdout
6. **Compute metrics** — Shop Rate, Spend Rate, Avg Ticket, and Avg Transactions per Shopper

### Brand Matching Logic

Brand transactions are identified by matching keywords (case-insensitive) against three columns in the transaction data:

- `BRAND_NAME`
- `STORE_NAME`
- `MERCHANT_DESCRIPTION`

For example, to identify ActBlue transactions, the keyword `ACTBLUE` is matched against all three columns. Multiple keywords per brand are supported and OR'd together.

---

## Audiences Under Validation

The following audiences are configured for validation. They span two brands — **ActBlue** and **Ross** — and include both known-shopper seeds and lookalike expansions for each.

### ActBlue Audiences

| # | Audience Name | Type | Audience ID |
|---|---|---|---|
| 1 | ActBlue Exposed Likely Prospects | Propensity / Prospects | `akkio_audience_a64a3518_0b98_428d_bd52_62193f77927b` |
| 2 | ActBlue Control Likely Prospects Lookalike | Lookalike Expansion | `akkio_audience_4213278e_2935_4131_a114_df8976f47fbb` |
| 3 | ActBlue Shoppers Exposed | Known Shoppers (Seed) | `akkio_audience_cf577ee6_08db_4294_9716_bbf06b7c389b` |
| 4 | ActBlue Shoppers Exposed Lookalike | Lookalike Expansion | `akkio_audience_9588a38d_8d82_4c7a_87b5_05299c694f6d` |

**ActBlue audience pairs:**
- **Shoppers Exposed** (seed of known ActBlue buyers) paired with its **Shoppers Exposed Lookalike** (expansion audience modeled from the seed). Comparing these two shows how well the lookalike retains the purchase signal of the original seed.
- **Exposed Likely Prospects** (propensity-scored prospects) paired with the **Control Likely Prospects Lookalike** (a broader lookalike built from the prospects pool). Comparing these two shows how targeted prospect scoring compares to broader lookalike expansion.

### Ross Audiences

| # | Audience Name | Type | Audience ID |
|---|---|---|---|
| 5 | Ross Likely Buyers | Propensity / Prospects | `akkio_audience_ab2b535c_3a4b_4a41_94b0_df0564f7a4c7` |
| 6 | Ross Likely Buyers Lookalike | Lookalike Expansion | `akkio_audience_b5ecc926_f421_4be2_acdf_e672c6d77c7a` |
| 7 | Ross Shoppers Exposed Non-Holiday | Known Shoppers (Seed) | `akkio_audience_62341a69_d801_487f_817b_a6a224f64f5b` |
| 8 | Ross Shoppers Exposed Non-Holiday Lookalike | Lookalike Expansion | `akkio_audience_1b2a8c56_c952_465b_a41e_1220e97a982e` |

**Ross audience pairs:**
- **Shoppers Exposed Non-Holiday** (seed of known Ross buyers, excluding holiday-driven purchases) paired with its **Non-Holiday Lookalike**. The "non-holiday" filter ensures the seed captures habitual Ross shoppers rather than one-time holiday gift buyers, providing a cleaner behavioral signal.
- **Likely Buyers** (propensity-scored prospects) paired with the **Likely Buyers Lookalike**. Demonstrates how the scoring model identifies high-value prospects compared to a broader expansion.

---

## Validation Results

The table below shows the holdout validation results for all eight audiences. The holdout window is **September 1 – September 30, 2025**.

### ActBlue Results

| Metric | Exposed Likely Prospects | Control Likely Prospects LAL | Shoppers Exposed (Seed) | Shoppers Exposed LAL |
|---|---|---|---|---|
| **Total Audience IDs** | 100,000 | 1,562,040 | 28,177 | 704,692 |
| **Active Matched IDs** | 99,578 | 208,938 | 26,636 | 74,854 |
| **Brand Shoppers** | 1,967 | 2,464 | 3,594 | 3,785 |
| **Brand Transactions** | 6,112 | 7,626 | 12,669 | 13,042 |
| **Brand Spend** | $346,064.70 | $394,588.69 | $315,304.18 | $326,642.82 |
| **Shop Rate** | **1.98%** | **1.18%** | **13.49%** | **5.06%** |
| **Spend Rate** | **$3.48** | **$1.89** | **$11.84** | **$4.36** |
| **Avg Ticket** | $56.62 | $51.74 | $24.89 | $25.05 |
| **Avg Trans / Shopper** | 3.11 | 3.09 | 3.53 | 3.45 |

**Key observations — ActBlue:**

- **Shoppers Exposed (Seed)** achieves the highest shop rate at 13.49%, as expected — these are known ActBlue buyers, so the high holdout conversion reflects strong retention behavior.
- **Shoppers Exposed Lookalike** retains meaningful signal at 5.06% shop rate, demonstrating that the lookalike model successfully identifies individuals with ActBlue purchase propensity beyond the seed.
- **Exposed Likely Prospects** shows a 1.98% shop rate — nearly **1.7x higher** than the broader Control Likely Prospects Lookalike (1.18%), indicating that targeted prospect scoring meaningfully outperforms broader lookalike expansion.
- Avg Ticket is notably higher for the prospect audiences ($52–$57) vs. the shopper-based audiences ($25), suggesting prospects who convert tend to make larger individual donations.

### Ross Results

| Metric | Likely Buyers | Likely Buyers LAL | Shoppers Exposed Non-Holiday (Seed) | Shoppers Exposed Non-Holiday LAL |
|---|---|---|---|---|
| **Total Audience IDs** | 597,559 | 1,019,936 | 437,060 | 519,377 |
| **Active Matched IDs** | 508,450 | 539,309 | 403,536 | 409,969 |
| **Brand Shoppers** | 6,567 | 8,298 | 68,307 | 68,781 |
| **Brand Transactions** | 7,715 | 10,363 | 110,043 | 110,787 |
| **Brand Spend** | $523,796.10 | $695,379.87 | $7,441,313.20 | $7,486,946.90 |
| **Shop Rate** | **1.29%** | **1.54%** | **16.93%** | **16.78%** |
| **Spend Rate** | **$1.03** | **$1.29** | **$18.44** | **$18.26** |
| **Avg Ticket** | $67.89 | $67.10 | $67.62 | $67.58 |
| **Avg Trans / Shopper** | 1.17 | 1.25 | 1.61 | 1.61 |

**Key observations — Ross:**

- **Shoppers Exposed Non-Holiday (Seed)** delivers a 16.93% shop rate — very strong retention, confirming that non-holiday Ross buyers are highly habitual repeat shoppers.
- **Shoppers Exposed Non-Holiday Lookalike** performs almost identically (16.78%), indicating that the Ross shopping behavior is broadly distributed and the lookalike captures a population with very similar purchase patterns to the seed.
- **Likely Buyers** and **Likely Buyers Lookalike** show lower but comparable shop rates (1.29% vs. 1.54%), which is expected for prospect/acquisition audiences.
- Avg Ticket is remarkably consistent across all Ross audiences (~$67–$68), reflecting the stable price-point nature of Ross's retail model.
- Avg Transactions per Shopper is notably lower for Ross (1.2–1.6) vs. ActBlue (3.1–3.5), consistent with the difference between monthly retail shopping and recurring political donation behavior.

---

## How to Interpret These Results

### Shop Rate Is the Primary Quality Signal

Shop Rate answers the core question: *"What percentage of active audience members actually purchased from the brand during the holdout window?"*

- **Higher shop rate = better audience quality** — the audience is more concentrated with likely buyers
- **Seed audiences will always have higher shop rates** — they are built from known buyers, so holdout shop rate measures retention, not prediction
- **Lookalike / propensity audiences are the true test** — their shop rate reflects the model's ability to identify *new* buyers

### Spend Rate Captures Dollar-Value Impact

Spend Rate combines shop rate with purchase value to answer: *"How much brand revenue does each active audience member represent?"*

This is the most actionable metric for media planning — it directly translates audience quality to economic value.

### Lift Over Baseline

To contextualize these results, compare against a **random baseline** — the shop rate you would observe by taking a random sample of the same size from the active population. Any audience's shop rate divided by the baseline shop rate gives you **lift**:

```
Lift = Audience Shop Rate / Random Baseline Shop Rate
```

A lift of 2.0x means the audience is twice as likely to contain brand shoppers as a random sample. Higher lift = better targeting precision.

---

## Automation & Reproducibility

### Configuration-Driven Validation

All audiences are defined in a single YAML configuration file (`audiences.yml`), making it easy to add, remove, or modify audiences without changing code:

```yaml
defaults:
  date_start: "2025-09-01"    # Holdout start (inclusive)
  date_end: "2025-10-01"      # Holdout end (exclusive)

audiences:
  - audience_id: "akkio_audience_a64a3518_..."
    name: "ActBlue Exposed Likely Prospects"
    brand_keywords:
      - "ACTBLUE"

  - audience_id: "akkio_audience_ab2b535c_..."
    name: "Ross Likely Buyers"
    brand_keywords:
      - "ROSS"
```

Each audience entry specifies:
- **audience_id** — the unique Akkio audience identifier (links to `AUDIENCE_LOOKUP`)
- **name** — a human-readable label for reporting
- **brand_keywords** — one or more keywords matched case-insensitively against `BRAND_NAME`, `STORE_NAME`, and `MERCHANT_DESCRIPTION`

Global defaults (holdout window dates, database/schema) can be overridden per audience.

### Execution

The validation is executed via a Python script that:

1. Reads the audience configuration from `audiences.yml`
2. Connects to Snowflake (credentials from environment variables or dbt profiles)
3. Runs a parameterized validation query for each audience against the holdout window
4. Aggregates all results into a summary table
5. Exports results to CSV for further analysis

```
python analyses/audience_validation/audience_validation.py
```

Results are written to `analyses/audience_validation/output/audience_validation_results.csv`.

---

## Validated SQL Query

The core validation query executed for each audience is shown below. It follows the same pattern for every audience — only the audience ID, brand keywords, and date range change.

```sql
WITH AUDIENCE AS (
  SELECT AKKIO_ID
  FROM AUDIENCE_LOOKUP
  WHERE audience_id = '<audience_id>'
    AND ver = (
      SELECT MAX(ver)
      FROM AUDIENCE_METADATA
      WHERE audience_id = '<audience_id>'
    )
),
TOTAL_LAL AS (
  SELECT COUNT(DISTINCT AKKIO_ID) AS TOTAL_LAL_IDS
  FROM AUDIENCE
),
ACTIVE_MATCHED AS (
  SELECT COUNT(DISTINCT A.AKKIO_ID) AS ACTIVE_MATCHED_IDS
  FROM AUDIENCE AS A
  INNER JOIN FACT_TRANSACTION_ENRICHED AS F
    ON A.AKKIO_ID = F.AKKIO_ID
  WHERE F.TRANS_DATE >= '<date_start>'
    AND F.TRANS_DATE <  '<date_end>'
),
BRAND_METRICS AS (
  SELECT
    COUNT(DISTINCT A.AKKIO_ID) AS BRAND_SHOPPERS,
    COUNT(F.TXID)              AS BRAND_TRANSACTIONS,
    SUM(F.TRANS_AMOUNT)        AS BRAND_SPEND
  FROM AUDIENCE AS A
  INNER JOIN FACT_TRANSACTION_ENRICHED AS F
    ON A.AKKIO_ID = F.AKKIO_ID
  WHERE F.TRANS_DATE >= '<date_start>'
    AND F.TRANS_DATE <  '<date_end>'
    AND (
      UPPER(F.BRAND_NAME)            LIKE '%<KEYWORD>%'
      OR UPPER(F.STORE_NAME)         LIKE '%<KEYWORD>%'
      OR UPPER(F.MERCHANT_DESCRIPTION) LIKE '%<KEYWORD>%'
    )
)
SELECT
  T.TOTAL_LAL_IDS           AS "Total Audience IDs",
  A.ACTIVE_MATCHED_IDS      AS "Active Matched IDs",
  B.BRAND_SHOPPERS          AS "Brand Shoppers",
  B.BRAND_TRANSACTIONS      AS "Brand Transactions",
  B.BRAND_SPEND             AS "Brand Spend",
  B.BRAND_SHOPPERS / A.ACTIVE_MATCHED_IDS  AS "Shop Rate",
  B.BRAND_SPEND   / A.ACTIVE_MATCHED_IDS  AS "Spend Rate",
  B.BRAND_SPEND   / B.BRAND_TRANSACTIONS   AS "Avg Ticket",
  B.BRAND_TRANSACTIONS / B.BRAND_SHOPPERS  AS "Avg Trans / Shopper"
FROM TOTAL_LAL   AS T
CROSS JOIN ACTIVE_MATCHED AS A
CROSS JOIN BRAND_METRICS  AS B;
```

**Critical rules enforced in every query:**
- Only transactions within the holdout window are counted
- The denominator for Shop Rate and Spend Rate is **Active Matched IDs** (audience members with any transaction in the holdout), not total audience size
- Brand matching uses case-insensitive LIKE across three columns
- The latest audience version is always used (`MAX(ver)` from `AUDIENCE_METADATA`)

---

## Summary

The audience validation framework provides a rigorous, transparent method for measuring audience quality:

1. **Temporal holdout design** prevents data leakage and ensures metrics reflect true predictive power
2. **Standardized metrics** (Shop Rate, Spend Rate, Avg Ticket) enable direct comparison across audiences, brands, and modeling approaches
3. **Active Matched IDs denominator** ensures fair comparison regardless of panel coverage differences
4. **Configuration-driven automation** makes it easy to add new audiences and re-run validation as new data becomes available
5. **Seed + Lookalike pairing** separates retention measurement from acquisition prediction, providing a complete picture of audience value

This framework can be extended to any brand or merchant available in the transaction data, and to any audience construction methodology — enabling ongoing benchmarking and optimization of audience quality over time.
