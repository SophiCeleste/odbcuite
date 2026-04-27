"""
ns_utils.py — shared utilities for NetSuite extract scripts.

Covers: logging, config loading, ODBC connections, DuckDB loading,
plant/manufacturer resolution, business-day math, and timing helpers.

Usage in a script:
    from ns_utils import log_setup, load_config, sql_to_df, connect_netsuite, load_duckdb, format_elapsed
"""

import json
import numpy as np
import pyodbc
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime


# ==============================================================
# LOGGING
# ==============================================================

def log_setup(script_path):
    """
    Create a timestamped log file under logs/<script_name>/ and return
    a log callable.

    Pass __file__ from the calling script so the log folder is named
    after that script, not this utility module.

    Returns
    -------
    log_path : Path
        Full path to the log file (print at script start for traceability).
    log : callable
        log(msg="") — appends msg + newline to the log file.

    Example
    -------
        log_path, log = log_setup(__file__)
        print(f"Log: {log_path}")
        log("Script started")
    """
    script_name = Path(script_path).stem
    log_dir = Path(script_path).parent / "logs" / script_name
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{script_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    def log(msg=""):
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(str(msg) + "\n")

    return log_path, log


# ==============================================================
# CONFIG
# ==============================================================

def load_config(config_path=None):
    """
    Load config.json and return as a dict.

    Parameters
    ----------
    config_path : str or Path, optional
        Defaults to config.json in the same directory as ns_utils.py.

    Expected config.json keys:
        netsuite_dsn  -- ODBC DSN name (System DSN, 64-bit)
        netsuite_uid  -- NetSuite login email
        netsuite_pwd  -- NetSuite login password
        db_path       -- Full path to the DuckDB .duckdb file
        raw_folder    -- Folder where CSV exports are saved
        table_name    -- DuckDB target table (schema.tablename)

    config.json is never committed — it contains credentials.
    """
    if config_path is None:
        config_path = Path(__file__).parent / "config.json"
    with open(config_path, encoding="utf-8") as f:
        return json.load(f)


# ==============================================================
# NETSUITE ODBC
# ==============================================================

def connect_netsuite(dsn, uid, pwd):
    """
    Open a NetSuite ODBC connection and return the connection object.

    autocommit=True is required — without it the connection hangs
    indefinitely waiting for a transaction that NetSuite never starts.

    Example
    -------
        conn = connect_netsuite(config["netsuite_dsn"], config["netsuite_uid"], config["netsuite_pwd"])
        # ... run queries ...
        conn.close()
    """
    return pyodbc.connect(f"DSN={dsn};UID={uid};PWD={pwd}", autocommit=True)


def sql_to_df(conn, query):
    """
    Execute a query via raw cursor and return a DataFrame.

    Uses cursor.fetchall() instead of pd.read_sql() to suppress the
    pandas UserWarning about non-SQLAlchemy connectables. The warning
    is cosmetic but noisy in scheduled/logged runs.

    Parameters
    ----------
    conn : pyodbc.Connection
    query : str

    Returns
    -------
    pd.DataFrame
    """
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [col[0] for col in cursor.description]
    return pd.DataFrame.from_records(cursor.fetchall(), columns=columns)


# ==============================================================
# DUCKDB
# ==============================================================

def load_duckdb(db_path, table_name, df):
    """
    Drop-and-recreate a DuckDB table from a DataFrame, return row count.

    Intentionally destructive — the raw schema is a staging layer refreshed
    on every run, not a historical archive. If you need accumulation, switch
    to INSERT with deduplication on the id column instead.

    Parameters
    ----------
    db_path : str or Path
    table_name : str
        Fully qualified name, e.g. "raw.sd_ontime_raw".
    df : pd.DataFrame

    Returns
    -------
    int
        Row count of the newly created table.

    Example
    -------
        count = load_duckdb(config["db_path"], config["table_name"], df)
        print(f"Rows loaded: {count}")
    """
    conn = duckdb.connect(str(db_path))
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    conn.close()
    return count


# ==============================================================
# TIMING
# ==============================================================

def format_elapsed(start_time):
    """
    Return elapsed wall time since start_time as 'Xh Ym Zs'.

    Parameters
    ----------
    start_time : datetime
        Typically datetime.now() captured at the top of the script.
    """
    total_secs = int((datetime.now() - start_time).total_seconds())
    h = total_secs // 3600
    m = (total_secs % 3600) // 60
    s = total_secs % 60
    return f"{h}h {m}m {s}s"


# ==============================================================
# PLANT & MANUFACTURER RESOLUTION
# ==============================================================
# custbody_items_sourced_from stores one or more plant IDs as a
# comma-separated string (e.g. "1", "1, 3", "1, 5, 7"). A standard
# SQL JOIN cannot handle multi-value fields, so resolution is done
# in Python using a dict lookup built from customlist_plant_codes.

def resolve_plants(val, plant_map):
    """
    Resolve comma-separated plant IDs to human-readable names.

    Parameters
    ----------
    val : str or None
        Raw value from custbody_items_sourced_from.
    plant_map : dict
        {str(id): plant_name} — build from the plant_codes lookup table:
            plant_map = dict(zip(plant_df["id"].astype(str), plant_df["plant_name"]))

    Returns
    -------
    str or None
        Comma-separated plant names, or None if val is null.
        Unknown IDs fall through as-is so missing mappings stay visible.

    Example
    -------
        df["sourced_from_name"] = df["custbody_items_sourced_from"].apply(
            lambda v: resolve_plants(v, plant_map)
        )
    """
    if pd.isna(val) or val is None:
        return None
    ids = [v.strip() for v in str(val).split(",")]
    return ", ".join(plant_map.get(i, i) for i in ids)


# Default plant classifications. Pass overrides to resolve_manufacturer_type
# if the plant list changes rather than editing these module-level defaults.
_DEFAULT_ATX_PLANTS    = {"Plant 01 - Screen Innovations"}
_DEFAULT_VENDOR_PLANTS = {"Plant 00 - Direct From Vendor"}


def resolve_manufacturer_type(val, atx_plants=None, vendor_plants=None):
    """
    Classify a resolved sourced_from_name as ATX, Contract Manufacturer,
    Vendor, Mixed, or Direct.

    Parameters
    ----------
    val : str or None
        Resolved plant name(s) from resolve_plants(), comma-separated.
    atx_plants : set, optional
        Plant names classified as ATX (in-house). Defaults to _DEFAULT_ATX_PLANTS.
    vendor_plants : set, optional
        Plant names classified as Vendor (direct-to-customer). Defaults to _DEFAULT_VENDOR_PLANTS.

    Returns
    -------
    str or None
        "ATX", "Contract Manufacturer", "Vendor", "Mixed", "Direct", or None.

    Multi-plant precedence:
        ATX + anything         → "Mixed"
        Vendor + CM (no ATX)   → "Direct"
        Multiple CM plants     → "Contract Manufacturer"

    Example
    -------
        df["manufacturer_type"] = df["sourced_from_name"].apply(resolve_manufacturer_type)
    """
    if atx_plants is None:
        atx_plants = _DEFAULT_ATX_PLANTS
    if vendor_plants is None:
        vendor_plants = _DEFAULT_VENDOR_PLANTS

    if pd.isna(val) or val is None:
        return None

    plants = [p.strip() for p in str(val).split(",")]
    types = set(
        "ATX" if p in atx_plants else
        "Vendor" if p in vendor_plants else
        "Contract Manufacturer"
        for p in plants
    )

    if len(types) == 1:
        return types.pop()
    if "ATX" in types:
        return "Mixed"
    # Vendor + CM with no ATX → treat as Direct
    if "Vendor" in types:
        return "Direct"
    return "Contract Manufacturer"


# ==============================================================
# BUSINESS DAYS
# ==============================================================

def business_days_diff(esd, asd):
    """
    Return signed business-day count between estimated and actual ship dates.

    Positive = late, negative = early, zero = on time.
    Returns None if either date is null.

    Parameters
    ----------
    esd : datetime or NaT
        Estimated ship date.
    asd : datetime or NaT
        Actual ship date.

    Example
    -------
        df["days_late"] = df.apply(
            lambda row: business_days_diff(
                row["custbody_si_estimated_ship_date"], row["actualshipdate"]
            ), axis=1
        )
    """
    if pd.isna(esd) or pd.isna(asd):
        return None
    return int(np.busday_count(esd.date(), asd.date()))


def bucket_days_late(days):
    """
    Bucket a signed business-day count into a human-readable lateness label.

    Parameters
    ----------
    days : int or None
        Output of business_days_diff(). Pass None for orders with no ESD.

    Returns
    -------
    str or None

    Buckets:
        <= -11  →  "2+ weeks early"
        -10..-6 →  "1 week early"
        -5..-3  →  "3-5 days early"
        -2..-1  →  "1-2 days early"
          0     →  "on-time"
         1-2    →  "1-2 days late"
         3-5    →  "3-5 days late"
         6-10   →  "1 week late"
        >= 11   →  "2+ weeks late"

    Example
    -------
        df["bucket"] = df["days_late"].apply(bucket_days_late)
    """
    if days is None or pd.isna(days):
        return None
    if days <= -11:
        return "2+ weeks early"
    elif days <= -6:
        return "1 week early"
    elif days <= -3:
        return "3-5 days early"
    elif days <= -1:
        return "1-2 days early"
    elif days == 0:
        return "on-time"
    elif days <= 2:
        return "1-2 days late"
    elif days <= 5:
        return "3-5 days late"
    elif days <= 10:
        return "1 week late"
    else:
        return "2+ weeks late"
