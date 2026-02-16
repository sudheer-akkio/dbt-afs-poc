#!/usr/bin/env python3
"""
Audience Validation Utility
============================
Validates Akkio audiences against the holdout set by running brand-level
metrics queries on FACT_TRANSACTION_ENRICHED.

Usage:
    python analyses/audience_validation/audience_validation.py

    Or from the analyses/audience_validation directory:
    python audience_validation.py

Configuration:
    Edit audiences.yml (in the same directory as this script) to add/remove
    audiences. Snowflake credentials are read from environment variables or
    from ~/.dbt/profiles.yml (afs_poc_snowflake profile).

Environment variables (if not using dbt profiles):
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE
"""

import os
import sys
import yaml
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import snowflake.connector
import pandas as pd

# Paths — resolve relative to this script's location
SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = SCRIPT_DIR / "audiences.yml"
OUTPUT_DIR = SCRIPT_DIR / "output"

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# Data classes
@dataclass
class AudienceConfig:
    """One audience + brand combination to validate."""

    audience_id: str                     # Akkio audience UUID
    name: str                            # Friendly label for reports
    brand_keywords: list[str]            # Keywords matched via LIKE on BRAND_NAME / STORE_NAME / MERCHANT_DESCRIPTION
    date_start: str = "2025-09-01"       # Inclusive start of the holdout window
    date_end: str = "2025-10-01"         # Exclusive end of the holdout window
    database: str = "DEMO"               # Snowflake database for AUDIENCE_LOOKUP / AUDIENCE_METADATA
    schema: str = "AFS_POC"              # Snowflake schema for AUDIENCE_LOOKUP / AUDIENCE_METADATA
    fact_database: str = "DEMO"          # Snowflake database for FACT_TRANSACTION_ENRICHED
    fact_schema: str = "AFS_POC"         # Snowflake schema for FACT_TRANSACTION_ENRICHED


# Config loader
def load_audiences(config_path: Path = CONFIG_PATH) -> list[AudienceConfig]:
    """Load audience definitions from the YAML config file."""
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config file not found: {config_path}\n"
            f"Expected at: {config_path.resolve()}"
        )

    with open(config_path) as f:
        raw = yaml.safe_load(f)

    defaults = raw.get("defaults", {})
    audiences_raw = raw.get("audiences", [])

    if not audiences_raw:
        raise ValueError(f"No audiences defined in {config_path}")

    audiences: list[AudienceConfig] = []
    for entry in audiences_raw:
        # Merge defaults with per-audience overrides
        merged = {**defaults, **entry}
        audiences.append(AudienceConfig(
            audience_id=merged["audience_id"],
            name=merged["name"],
            brand_keywords=merged["brand_keywords"],
            date_start=merged.get("date_start", "2025-08-01"),
            date_end=merged.get("date_end", "2025-10-01"),
            database=merged.get("database", "DEMO"),
            schema=merged.get("schema", "AFS_POC"),
            fact_database=merged.get("fact_database", "DEMO"),
            fact_schema=merged.get("fact_schema", "AFS_POC"),
        ))

    return audiences


# SQL Template — now includes a general-population baseline for lift calculation
VALIDATION_SQL_TEMPLATE = """
WITH AUDIENCE AS (
  SELECT AKKIO_ID
  FROM __DB__.__SCHEMA__.AUDIENCE_LOOKUP
  WHERE audience_id = %(audience_id)s
    AND ver = (
      SELECT MAX(ver)
      FROM __DB__.__SCHEMA__.AUDIENCE_METADATA
      WHERE audience_id = %(audience_id)s
    )
),
TOTAL_LAL AS (
  SELECT COUNT(DISTINCT AKKIO_ID) AS TOTAL_LAL_IDS
  FROM AUDIENCE
),
ACTIVE_MATCHED AS (
  SELECT COUNT(DISTINCT A.AKKIO_ID) AS ACTIVE_MATCHED_IDS
  FROM AUDIENCE AS A
  INNER JOIN __FACT_DB__.__FACT_SCHEMA__.FACT_TRANSACTION_ENRICHED AS F
    ON A.AKKIO_ID = F.AKKIO_ID
  WHERE F.TRANS_DATE >= %(date_start)s
    AND F.TRANS_DATE <  %(date_end)s
),
BRAND_METRICS AS (
  SELECT
    COUNT(DISTINCT A.AKKIO_ID) AS BRAND_SHOPPERS,
    COUNT(F.TXID)              AS BRAND_TRANSACTIONS,
    COALESCE(SUM(F.TRANS_AMOUNT), 0) AS BRAND_SPEND
  FROM AUDIENCE AS A
  INNER JOIN __FACT_DB__.__FACT_SCHEMA__.FACT_TRANSACTION_ENRICHED AS F
    ON A.AKKIO_ID = F.AKKIO_ID
  WHERE F.TRANS_DATE >= %(date_start)s
    AND F.TRANS_DATE <  %(date_end)s
    AND (__BRAND_FILTER__)
),
-- Baseline: general-population metrics over the same holdout window
-- Counts ALL active transactors and brand shoppers in the full universe
BASELINE_ACTIVE AS (
  SELECT COUNT(DISTINCT F.AKKIO_ID) AS BASELINE_ACTIVE_IDS
  FROM __FACT_DB__.__FACT_SCHEMA__.FACT_TRANSACTION_ENRICHED AS F
  WHERE F.TRANS_DATE >= %(date_start)s
    AND F.TRANS_DATE <  %(date_end)s
),
BASELINE_BRAND AS (
  SELECT
    COUNT(DISTINCT F.AKKIO_ID)           AS BASELINE_BRAND_SHOPPERS,
    COUNT(F.TXID)                        AS BASELINE_BRAND_TRANSACTIONS,
    COALESCE(SUM(F.TRANS_AMOUNT), 0)     AS BASELINE_BRAND_SPEND
  FROM __FACT_DB__.__FACT_SCHEMA__.FACT_TRANSACTION_ENRICHED AS F
  WHERE F.TRANS_DATE >= %(date_start)s
    AND F.TRANS_DATE <  %(date_end)s
    AND (__BRAND_FILTER__)
)
SELECT
  %(audience_name)s              AS AUDIENCE_NAME,
  %(audience_id)s                AS AUDIENCE_ID,
  T.TOTAL_LAL_IDS,
  A.ACTIVE_MATCHED_IDS,
  B.BRAND_SHOPPERS,
  B.BRAND_TRANSACTIONS,
  B.BRAND_SPEND,

  -- Audience rates
  CASE WHEN A.ACTIVE_MATCHED_IDS > 0
       THEN CAST(B.BRAND_SHOPPERS AS FLOAT) / A.ACTIVE_MATCHED_IDS
       ELSE 0 END               AS SHOP_RATE,
  CASE WHEN A.ACTIVE_MATCHED_IDS > 0
       THEN CAST(B.BRAND_SPEND AS FLOAT) / A.ACTIVE_MATCHED_IDS
       ELSE 0 END               AS SPEND_RATE,
  CASE WHEN B.BRAND_TRANSACTIONS > 0
       THEN CAST(B.BRAND_SPEND AS FLOAT) / B.BRAND_TRANSACTIONS
       ELSE 0 END               AS AVERAGE_TICKET,
  CASE WHEN B.BRAND_SHOPPERS > 0
       THEN CAST(B.BRAND_TRANSACTIONS AS FLOAT) / B.BRAND_SHOPPERS
       ELSE 0 END               AS AVG_TRANSACTIONS_PER_SHOPPER,

  -- Baseline rates (general population)
  BA.BASELINE_ACTIVE_IDS,
  BB.BASELINE_BRAND_SHOPPERS,
  CASE WHEN BA.BASELINE_ACTIVE_IDS > 0
       THEN CAST(BB.BASELINE_BRAND_SHOPPERS AS FLOAT) / BA.BASELINE_ACTIVE_IDS
       ELSE 0 END               AS BASELINE_SHOP_RATE,
  CASE WHEN BA.BASELINE_ACTIVE_IDS > 0
       THEN CAST(BB.BASELINE_BRAND_SPEND AS FLOAT) / BA.BASELINE_ACTIVE_IDS
       ELSE 0 END               AS BASELINE_SPEND_RATE,

  -- Lift metrics (audience rate / baseline rate)
  CASE WHEN BA.BASELINE_ACTIVE_IDS > 0 AND BB.BASELINE_BRAND_SHOPPERS > 0
       THEN (CAST(B.BRAND_SHOPPERS AS FLOAT) / A.ACTIVE_MATCHED_IDS)
          / (CAST(BB.BASELINE_BRAND_SHOPPERS AS FLOAT) / BA.BASELINE_ACTIVE_IDS)
       ELSE NULL END            AS SHOP_RATE_LIFT,
  CASE WHEN BA.BASELINE_ACTIVE_IDS > 0 AND BB.BASELINE_BRAND_SPEND > 0
       THEN (CAST(B.BRAND_SPEND AS FLOAT) / A.ACTIVE_MATCHED_IDS)
          / (CAST(BB.BASELINE_BRAND_SPEND AS FLOAT) / BA.BASELINE_ACTIVE_IDS)
       ELSE NULL END            AS SPEND_RATE_LIFT

FROM TOTAL_LAL         AS T
CROSS JOIN ACTIVE_MATCHED   AS A
CROSS JOIN BRAND_METRICS    AS B
CROSS JOIN BASELINE_ACTIVE  AS BA
CROSS JOIN BASELINE_BRAND   AS BB;
"""


def _build_brand_filter(keywords: list[str]) -> str:
    """Build a SQL OR-clause matching brand keywords against three columns.

    Note: The %% escaping is required because the Snowflake connector uses
    pyformat paramstyle — literal % in SQL must be doubled so they survive
    the bind-parameter substitution pass.
    """
    clauses = []
    for kw in keywords:
        kw_upper = kw.upper()
        clauses.append(f"UPPER(F.BRAND_NAME) LIKE '%%{kw_upper}%%'")
        clauses.append(f"UPPER(F.STORE_NAME) LIKE '%%{kw_upper}%%'")
        clauses.append(f"UPPER(F.MERCHANT_DESCRIPTION) LIKE '%%{kw_upper}%%'")
    return "\n      OR ".join(clauses)


# Snowflake connection helpers
def _get_snowflake_conn_from_env() -> dict:
    """Try to build connection params from environment variables."""
    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    if not all(os.environ.get(k) for k in required):
        return {}
    return {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "password": os.environ["SNOWFLAKE_PASSWORD"],
        "database": os.environ.get("SNOWFLAKE_DATABASE", "DEMO"),
        "schema": os.environ.get("SNOWFLAKE_SCHEMA", "AFS_POC"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "role": os.environ.get("SNOWFLAKE_ROLE", ""),
    }


def _get_snowflake_conn_from_dbt() -> dict:
    """Try to read connection params from ~/.dbt/profiles.yml for the 'afs_poc_snowflake' profile."""
    profiles_path = Path.home() / ".dbt" / "profiles.yml"
    if not profiles_path.exists():
        return {}
    try:
        with open(profiles_path) as f:
            profiles = yaml.safe_load(f)
        profile = profiles.get("afs_poc_snowflake", {})
        target_name = profile.get("target", "dev")
        outputs = profile.get("outputs", {})
        target = outputs.get(target_name, {})
        if not target:
            return {}
        return {
            "account": target.get("account", ""),
            "user": target.get("user", ""),
            "password": target.get("password", ""),
            "database": target.get("database", "DEMO"),
            "schema": target.get("schema", "AFS_POC"),
            "warehouse": target.get("warehouse", "COMPUTE_WH"),
            "role": target.get("role", ""),
        }
    except Exception as exc:
        log.warning("Could not read dbt profiles: %s", exc)
        return {}


def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    """Return a live Snowflake connection (env vars take precedence over dbt profiles)."""
    params = _get_snowflake_conn_from_env() or _get_snowflake_conn_from_dbt()
    if not params:
        raise RuntimeError(
            "No Snowflake credentials found.\n"
            "Set SNOWFLAKE_ACCOUNT / SNOWFLAKE_USER / SNOWFLAKE_PASSWORD env vars, "
            "or configure ~/.dbt/profiles.yml for the 'afs_poc_snowflake' profile."
        )
    # Remove empty strings
    params = {k: v for k, v in params.items() if v}
    log.info("Connecting to Snowflake account=%s, database=%s, schema=%s",
             params.get("account"), params.get("database"), params.get("schema"))
    return snowflake.connector.connect(**params)


# Core validation logic
def validate_audience(
    conn: snowflake.connector.SnowflakeConnection,
    audience: AudienceConfig,
) -> pd.DataFrame:
    """Run the validation query for a single audience and return a one-row DataFrame."""
    brand_filter = _build_brand_filter(audience.brand_keywords)
    sql = (
        VALIDATION_SQL_TEMPLATE
        .replace("__DB__", audience.database)
        .replace("__SCHEMA__", audience.schema)
        .replace("__FACT_DB__", audience.fact_database)
        .replace("__FACT_SCHEMA__", audience.fact_schema)
        .replace("__BRAND_FILTER__", brand_filter)
    )
    bind_params = {
        "audience_id": audience.audience_id,
        "audience_name": audience.name,
        "date_start": audience.date_start,
        "date_end": audience.date_end,
    }

    log.info("Validating audience: %s  [%s]", audience.name, audience.audience_id)
    log.info("  Brand keywords: %s | Date range: %s to %s",
             audience.brand_keywords, audience.date_start, audience.date_end)

    cur = conn.cursor()
    try:
        cur.execute(sql, bind_params)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=columns)
    finally:
        cur.close()

    log.info("  -> %d row(s) returned", len(df))
    return df


def validate_all(audiences: list[AudienceConfig]) -> pd.DataFrame:
    """Run validation for every audience and return the combined DataFrame."""
    conn = get_snowflake_connection()
    results: list[pd.DataFrame] = []

    try:
        for aud in audiences:
            try:
                df = validate_audience(conn, aud)
                results.append(df)
            except Exception as exc:
                log.error("FAILED for audience '%s': %s", aud.name, exc)
    finally:
        conn.close()

    if not results:
        log.warning("No results collected.")
        return pd.DataFrame()

    combined = pd.concat(results, ignore_index=True)
    return combined


# Display / export helpers
CURRENCY_COLS = {"BRAND_SPEND", "SPEND_RATE", "AVERAGE_TICKET", "BASELINE_SPEND_RATE"}
PERCENT_COLS = {"SHOP_RATE", "BASELINE_SHOP_RATE"}
INT_COLS = {
    "TOTAL_LAL_IDS", "ACTIVE_MATCHED_IDS", "BRAND_SHOPPERS",
    "BRAND_TRANSACTIONS", "BASELINE_ACTIVE_IDS", "BASELINE_BRAND_SHOPPERS",
}
LIFT_COLS = {"SHOP_RATE_LIFT", "SPEND_RATE_LIFT"}


def _fmt(val, col: str) -> str:
    """Format a single value for display."""
    if pd.isna(val):
        return "—"
    if col in INT_COLS:
        return f"{int(val):,}"
    if col in CURRENCY_COLS:
        return f"${val:,.2f}"
    if col in PERCENT_COLS:
        return f"{val:.4%}"
    if col in LIFT_COLS:
        return f"{val:,.2f}x"
    if isinstance(val, float):
        return f"{val:,.2f}"
    return str(val)


def print_summary(df: pd.DataFrame) -> None:
    """Pretty-print the validation results in two panels: audience metrics + baseline/lift."""
    if df.empty:
        print("\n(no results)\n")
        return

    # Panel 1: Core audience metrics
    core_cols = [
        "AUDIENCE_NAME", "TOTAL_LAL_IDS", "ACTIVE_MATCHED_IDS",
        "BRAND_SHOPPERS", "BRAND_TRANSACTIONS", "BRAND_SPEND",
        "SHOP_RATE", "SPEND_RATE", "AVERAGE_TICKET", "AVG_TRANSACTIONS_PER_SHOPPER",
    ]
    core_cols = [c for c in core_cols if c in df.columns]

    # Panel 2: Baseline and lift
    lift_cols = [
        "AUDIENCE_NAME",
        "BASELINE_ACTIVE_IDS", "BASELINE_BRAND_SHOPPERS",
        "BASELINE_SHOP_RATE", "BASELINE_SPEND_RATE",
        "SHOP_RATE_LIFT", "SPEND_RATE_LIFT",
    ]
    lift_cols = [c for c in lift_cols if c in df.columns]

    def _print_table(title: str, cols: list[str]) -> None:
        header = [c.replace("_", " ") for c in cols]
        rows = []
        for _, row in df.iterrows():
            rows.append([_fmt(row[c], c) for c in cols])

        widths = [max(len(h), *(len(r[i]) for r in rows)) for i, h in enumerate(header)]
        sep = "+-" + "-+-".join("-" * w for w in widths) + "-+"
        hdr_line = "| " + " | ".join(h.rjust(w) for h, w in zip(header, widths)) + " |"

        print("\n" + "=" * len(sep))
        print(f"  {title}")
        print("=" * len(sep))
        print(sep)
        print(hdr_line)
        print(sep)
        for r in rows:
            print("| " + " | ".join(v.rjust(w) for v, w in zip(r, widths)) + " |")
        print(sep)
        print()

    _print_table("AUDIENCE METRICS", core_cols)

    if lift_cols and len(lift_cols) > 1:
        _print_table("BASELINE COMPARISON & LIFT", lift_cols)


def export_csv(df: pd.DataFrame, output_dir: Path) -> Path:
    """Export the raw results to CSV in the output directory."""
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "audience_validation_results.csv"
    df.to_csv(csv_path, index=False)
    log.info("Results exported to %s", csv_path)
    return csv_path


# Main
def main():
    log.info("Loading config from %s", CONFIG_PATH)
    audiences = load_audiences()
    log.info("Starting validation for %d audience(s)...", len(audiences))

    df = validate_all(audiences)
    print_summary(df)

    if not df.empty:
        export_csv(df, OUTPUT_DIR)

    return df


if __name__ == "__main__":
    main()
