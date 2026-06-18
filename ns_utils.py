"""
ns_utils.py — shared utilities for NetSuite extract scripts.

Covers: logging, config loading, ODBC connections, DuckDB loading,
comma-separated ID resolution, manufacturer classification, business-day math, and timing helpers.

Usage in a script:
    from ns_utils import log_setup, load_config, sql_to_df, connect_netsuite, load_duckdb, format_elapsed
"""

import json
import re
import numpy as np
import pyodbc
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime

_SAFE_IDENT = re.compile(r"^[A-Za-z0-9_.]+$")

# --- credential resolution cache ---
_secret_client = None   # SecretClient instance; set on first get_secret() call
_vault_url = None       # resolved vault URL; cached to avoid re-reading config per call


def _check_ident(name, param="table_name"):
    if not _SAFE_IDENT.match(name):
        raise ValueError(f"Unsafe SQL identifier for {param}: {name!r}")


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
# CONSOLE OUTPUT
# ==============================================================

def cprint(msg, color="white"):
    """
    Print msg to the console in the given color using ANSI escape codes.

    Parameters
    ----------
    msg : str
    color : str
        "cyan"   — progress / status  ("Connecting...", "Running queries...")
        "yellow" — warnings
        "red"    — errors
        "green"  — success / done
        "white"  — informational output (row counts, elapsed time) [default]
    """
    _codes = {"green": 32, "yellow": 33, "red": 31, "cyan": 36, "white": 37}
    print(f"\033[{_codes.get(color, 37)}m{msg}\033[0m")


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
        env                          -- "dev" or "prod"
        key_vault.url                -- Azure Key Vault URL (non-sensitive)
        netsuite.dev.dsn             -- ODBC DSN name for dev (e.g. "NetSuiteDev")
        netsuite.dev.auth            -- "password"
        netsuite.dev.secret_uid      -- Key Vault secret name for the NetSuite login (dev)
        netsuite.dev.secret_pwd      -- Key Vault secret name for the NetSuite password (dev)
        netsuite.prod.dsn            -- ODBC DSN name for prod (e.g. "NetSuiteProd")
        netsuite.prod.auth           -- "tba"
        netsuite.prod.account_id     -- NetSuite account ID
        netsuite.prod.secret_consumer_key / secret_consumer_secret /
            secret_token_id / secret_token_secret
                                     -- Key Vault secret names for the TBA token fields
        databases.dev / databases.prod -- target database config
        databases.prod.secret_uid / secret_pwd
                                     -- Key Vault secret names for the Azure SQL login (prod)
        tables                       -- fully qualified target table names
        raw_folder                   -- folder where CSV exports are saved

    Credential values are never stored in config.json — only non-sensitive
    secret-name pointers and the Key Vault URL. Actual secrets are resolved at
    connection time via get_secret().
    """
    if config_path is None:
        config_path = Path(__file__).parent / "config.json"
    with open(config_path, encoding="utf-8") as f:
        return json.load(f)


# ==============================================================
# NETSUITE ODBC
# ==============================================================

def check_odbc_driver(odbc_driver, log=None):
    """
    Verify that odbc_driver is installed. Logs and raises RuntimeError if not.

    Call this before building any ODBC connection string so the error is
    clear and the run terminates cleanly rather than surfacing a cryptic
    pyodbc.InterfaceError deep in SQLAlchemy.

    Parameters
    ----------
    odbc_driver : str
        Driver name as it appears in config (e.g. "ODBC Driver 18 for SQL Server").
    log : callable, optional
        log(msg) from log_setup(). If provided, the error is also written to the log.
    """
    installed = pyodbc.drivers()
    if odbc_driver not in installed:
        msg = (
            f"ODBC driver mismatch: config requires '{odbc_driver}' "
            f"but it is not installed.\n"
            f"  Installed drivers: {installed}"
        )
        print(msg)
        if log:
            log(msg)
        raise RuntimeError(msg)


# ==============================================================
# CREDENTIAL RESOLUTION
# ==============================================================

def _get_config_vault_url():
    """
    Return the Azure Key Vault URL from config.json, or None.

    Reads config["key_vault"]["url"] via load_config(). Any error
    (missing config.json, missing key_vault key) returns None so the
    caller can fall back to the environment-variable path.

    Returns
    -------
    str or None
        The vault URL string, or None if not configured.
    """
    try:
        config = load_config()
        return config.get("key_vault", {}).get("url") or None
    except (KeyError, TypeError):
        return None


def get_secret(name: str) -> str:
    """
    Resolve a secret value by name.

    The vault URL is located by checking the AZURE_KEYVAULT_URL environment
    variable first, then config["key_vault"]["url"]. When a vault URL is
    configured, the secret is fetched from Azure Key Vault using
    DefaultAzureCredential and the configured client is cached at module
    level for the life of the process. When no vault URL is configured, the
    value is read from the environment variable derived from the secret name
    (uppercased with hyphens replaced by underscores), which is the local-dev
    fallback path. A Key Vault authentication or lookup failure is never
    swallowed — the underlying SDK exception propagates to the caller.

    Parameters
    ----------
    name : str
        Secret name as stored in Key Vault (e.g. "netsuite-uid").
        The env-var fallback name is derived: "netsuite-uid" -> NETSUITE_UID.

    Returns
    -------
    str
        The secret value.

    Raises
    ------
    KeyError
        If no vault URL is configured and the derived env var is not set.
    """
    import os
    global _secret_client, _vault_url

    # Resolve the vault URL once and cache it at module level. The empty-string
    # sentinel distinguishes "checked, none configured" from "not yet checked".
    if _vault_url is None:
        _vault_url = os.environ.get("AZURE_KEYVAULT_URL") or _get_config_vault_url() or ""

    if not _vault_url:
        # No vault configured — fall back to the environment variable.
        env_key = name.upper().replace("-", "_")
        return os.environ[env_key]  # raises KeyError with env var name if absent

    if _secret_client is None:
        from azure.keyvault.secrets import SecretClient
        from azure.identity import DefaultAzureCredential
        _secret_client = SecretClient(
            vault_url=_vault_url,
            credential=DefaultAzureCredential(),
        )

    return _secret_client.get_secret(name).value


def connect_netsuite(config):
    """
    Open a NetSuite ODBC connection and return the connection object.

    autocommit=True is required — without it the connection hangs
    indefinitely waiting for a transaction that NetSuite never starts.

    Reads config["env"] to select the auth method:
        dev  — email/password from config["netsuite"]["dev"]
        prod — TBA token password generated by ns_token.build_token_password()

    Parameters
    ----------
    config : dict
        From load_config(). Must contain a "netsuite" key with "dsn" and
        an env-keyed sub-dict (e.g. config["netsuite"]["prod"]).

    Example
    -------
        config = load_config()
        conn   = connect_netsuite(config)
        # ... run queries ...
        conn.close()
    """
    env    = config.get("env", "prod")
    ns     = config["netsuite"]
    ns_env = ns[env]

    conn_str = f"DSN={ns_env['dsn']}"

    if ns_env["auth"] == "password":
        conn_str += f";UID={get_secret(ns_env['secret_uid'])};PWD={get_secret(ns_env['secret_pwd'])}"
    else:
        from ns_token import build_token_password
        conn_str += f";UID=TBA;PWD={build_token_password()}"

    return pyodbc.connect(conn_str, autocommit=True)


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

def load_duckdb(db_path, table_name, df, log=None, upsert_keys=None):
    """
    Load a DataFrame into a DuckDB table, return total row count.

    Default (upsert_keys=None): drop-and-recreate on every run.
    Pass upsert_keys to switch to append mode — only rows whose key
    combination is not already present are inserted, so re-running on
    the same day is safe and history accumulates across runs.

    Parameters
    ----------
    db_path : str or Path
    table_name : str
        Fully qualified name, e.g. "raw.sd_ontime_raw".
    df : pd.DataFrame
    log : callable, optional
        log(msg) from log_setup(). If provided, schema creation and
        inserted row counts are logged.
    upsert_keys : list of str, optional
        Columns that uniquely identify a row. When supplied, the table is
        created on the first run and new rows are appended on subsequent
        runs. Example: ["item_id", "location_name", "snapshot_date"]

    Returns
    -------
    int
        Total row count of the table after the load.

    Example
    -------
        count = load_duckdb(config["db_path"], config["table_name"], df, log=log)
        count = load_duckdb(db_path, table_name, df, log=log,
                            upsert_keys=["item_id", "location_name", "snapshot_date"])
    """
    _check_ident(table_name)
    conn = duckdb.connect(str(db_path))
    if "." in table_name:
        schema = table_name.split(".")[0]
        exists = conn.execute(
            "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?",
            [schema]
        ).fetchone()[0]
        if not exists:
            conn.execute(f"CREATE SCHEMA {schema}")
            msg = f"Created schema: {schema}"
            print(msg)
            if log:
                log(msg)

    if upsert_keys:
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df WHERE 1=0")
        before = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        key_conditions = " AND ".join(f"t.{k} = s.{k}" for k in upsert_keys)
        conn.execute(f"""
            INSERT INTO {table_name}
            SELECT s.* FROM df s
            WHERE NOT EXISTS (
                SELECT 1 FROM {table_name} t
                WHERE {key_conditions}
            )
        """)
        inserted = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0] - before
        msg = f"Rows inserted into {table_name}: {inserted}"
        print(msg)
        if log:
            log(msg)
    else:
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")

    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    conn.close()
    return count


# ==============================================================
# AZURE SQL
# ==============================================================

def connect_azure_sql(db_config, log=None):
    """
    Build and return a SQLAlchemy engine for Azure SQL Server.

    Runs check_odbc_driver() before creating the engine so a missing driver
    terminates with a clear message rather than a cryptic pyodbc.InterfaceError.

    Parameters
    ----------
    db_config : dict
        The database entry from config["databases"][env]. Required keys:
            server      -- Azure SQL server hostname
            database    -- database name
        Optional keys:
            secret_uid  -- Key Vault secret name for the SQL login username
                           (omit for Azure AD Integrated auth)
            secret_pwd  -- Key Vault secret name for the SQL login password
                           (omit for Azure AD Integrated auth)
            odbc_driver -- ODBC driver name (default: "ODBC Driver 18 for SQL Server")
    log : callable, optional
        log(msg) from log_setup(). Passed to check_odbc_driver for error logging.

    Returns
    -------
    sqlalchemy.engine.Engine
        Caller is responsible for calling engine.dispose() when done.

    Example
    -------
        engine = connect_azure_sql(config["databases"]["prod"], log=log)
        # ... use engine ...
        engine.dispose()
    """
    from sqlalchemy import create_engine, event
    import urllib
    import struct

    server      = db_config["server"]
    database    = db_config["database"]

    if ("secret_uid" in db_config) != ("secret_pwd" in db_config):
        raise ValueError("db_config must have both secret_uid and secret_pwd, or neither")

    uid         = get_secret(db_config["secret_uid"]) if "secret_uid" in db_config else None
    pwd         = get_secret(db_config["secret_pwd"]) if "secret_pwd" in db_config else None
    odbc_driver = db_config.get("odbc_driver", "ODBC Driver 18 for SQL Server")

    check_odbc_driver(odbc_driver, log=log)

    base_odbc = (
        f"DRIVER={{{odbc_driver}}};"
        f"SERVER={server};DATABASE={database};"
        f"Encrypt=yes;TrustServerCertificate=no;"
    )

    if uid and pwd:
        odbc_str = base_odbc + f"UID={uid};PWD={pwd};"
        return create_engine(
            f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(odbc_str)}"
        )

    from azure.identity import AzureCliCredential
    _SQL_COPT_SS_ACCESS_TOKEN = 1256
    _credential = AzureCliCredential()

    def _get_token():
        raw = _credential.get_token("https://database.windows.net/.default").token
        encoded = raw.encode("utf-16-le")
        return struct.pack(f"<I{len(encoded)}s", len(encoded), encoded)

    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(base_odbc)}"
    )

    @event.listens_for(engine, "do_connect")
    def provide_token(dialect, conn_rec, cargs, cparams):
        cparams["attrs_before"] = {_SQL_COPT_SS_ACCESS_TOKEN: _get_token()}

    return engine


def load_azure_sql(db_config, table_name, df, log=None, upsert_keys=None):
    """
    Load a DataFrame into an Azure SQL table, return total row count.

    Default (upsert_keys=None): drop-and-recreate on every run.
    Pass upsert_keys to switch to append mode — only rows whose key
    combination is not already present are inserted via a staging table,
    so re-running on the same day is safe and history accumulates.

    Parameters
    ----------
    db_config : dict
        Passed directly to connect_azure_sql(). See that function for keys.
    table_name : str
        Fully qualified name, e.g. "raw.inventory_position_raw".
    df : pd.DataFrame
    log : callable, optional
        log(msg) from log_setup(). If provided, schema creation and
        inserted row counts are logged.
    upsert_keys : list of str, optional
        Columns that uniquely identify a row. When supplied, the table is
        created on the first run and new rows are appended on subsequent
        runs. Example: ["item_id", "location_name", "snapshot_date"]

    Returns
    -------
    int
        Total row count of the table after the load.

    Example
    -------
        count = load_azure_sql(db_config, config["tables"]["inventory_position"], df, log=log)
        count = load_azure_sql(db_config, table_name, df, log=log,
                               upsert_keys=["item_id", "location_name", "snapshot_date"])
    """
    _check_ident(table_name)
    from sqlalchemy import text

    engine = connect_azure_sql(db_config, log=log)

    schema, tbl = (table_name.split(".", 1) if "." in table_name else (None, table_name))

    if schema:
        with engine.connect() as conn:
            exists = conn.execute(
                text("SELECT COUNT(*) FROM sys.schemas WHERE name = :s"),
                {"s": schema}
            ).scalar()
            if not exists:
                conn.execute(text(f"CREATE SCHEMA [{schema}]"))
                conn.commit()
                msg = f"Created schema: {schema}"
                print(msg)
                if log:
                    log(msg)

    schema_prefix = f"[{schema}]." if schema else ""

    if upsert_keys:
        with engine.connect() as c:
            table_exists = c.execute(
                text("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = :s AND table_name = :t"),
                {"s": schema or "dbo", "t": tbl}
            ).scalar()

        if not table_exists:
            df.to_sql(tbl, engine, schema=schema, if_exists="replace", index=False)
        else:
            staging = f"_stg_{tbl}"
            df.to_sql(staging, engine, schema=schema, if_exists="replace", index=False)
            key_conds = " AND ".join(f"t.[{k}] = s.[{k}]" for k in upsert_keys)
            with engine.connect() as c:
                before = c.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
                c.execute(text(f"""
                    INSERT INTO {schema_prefix}[{tbl}]
                    SELECT s.*
                    FROM {schema_prefix}[{staging}] s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM {schema_prefix}[{tbl}] t
                        WHERE {key_conds}
                    )
                """))
                c.execute(text(f"DROP TABLE {schema_prefix}[{staging}]"))
                c.commit()
            with engine.connect() as c:
                inserted = c.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar() - before
            msg = f"Rows inserted into {table_name}: {inserted}"
            print(msg)
            if log:
                log(msg)
    else:
        df.to_sql(tbl, engine, schema=schema, if_exists="replace", index=False)

    with engine.connect() as conn:
        count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()

    engine.dispose()
    return count


# ==============================================================
# SCD TYPE 2
# ==============================================================

def _scd2_diff(current_df, active_df, identity_keys, tracked_cols, log=None):
    """
    Compute inserts and closes for one SCD Type 2 cycle. Pure pandas — no DB dependency.

    Parameters
    ----------
    current_df : pd.DataFrame
        Fresh snapshot from the source system. Must contain identity_keys + tracked_cols.
    active_df : pd.DataFrame or None
        Active rows from the history table (is_current = 1).
        Pass None on the first run — all current rows become the initial snapshot.
    identity_keys : list of str
        Columns that uniquely identify a row (e.g. ["item_id", "location_name"]).
    tracked_cols : list of str
        Columns watched for value changes.
    log : callable, optional

    Returns
    -------
    to_insert : pd.DataFrame
        Rows from current_df to be inserted (new + changed).
    close_keys : pd.DataFrame
        identity_keys-only DataFrame of rows to be closed (changed + retired).
    """
    if active_df is None:
        if log:
            log("First run: no prior history found — loading full initial snapshot")
        return current_df.copy(), pd.DataFrame(columns=identity_keys)

    def _values_differ(s_new, s_old):
        both_null = s_new.isna() & s_old.isna()
        return ~(both_null | (s_new == s_old))

    merged = current_df.merge(
        active_df[identity_keys + tracked_cols],
        on=identity_keys,
        how="outer",
        suffixes=("_new", "_old"),
        indicator=True,
    )

    new_mask     = merged["_merge"] == "left_only"
    retired_mask = merged["_merge"] == "right_only"
    both_mask    = merged["_merge"] == "both"
    changed_mask = both_mask & pd.concat(
        [_values_differ(merged[f"{c}_new"], merged[f"{c}_old"]) for c in tracked_cols],
        axis=1,
    ).any(axis=1)

    close_keys  = merged.loc[changed_mask | retired_mask, identity_keys].drop_duplicates()
    insert_keys = merged.loc[changed_mask | new_mask,     identity_keys].drop_duplicates()
    to_insert   = current_df.merge(insert_keys, on=identity_keys, how="inner").copy()

    n_changed = int(changed_mask.sum())
    n_retired = int(retired_mask.sum())
    n_new     = int(new_mask.sum())
    print(f"Changed: {n_changed}  |  Retired: {n_retired}  |  New: {n_new}")
    if log:
        log(f"Changed pairs: {n_changed}")
        log(f"Retired pairs: {n_retired}")
        log(f"New pairs:     {n_new}")

    return to_insert, close_keys


def scd2_load_duckdb(conn_db, table_name, current_df, identity_keys, tracked_cols,
                     today, recorded_at, log=None):
    """
    Run one SCD Type 2 cycle against a DuckDB table.

    Reads active rows, diffs against current_df, closes changed/retired rows,
    and inserts new rows. Creates the table and schema on the first run.

    Parameters
    ----------
    conn_db : duckdb.DuckDBPyConnection
    table_name : str
        Fully qualified, e.g. "raw.inventory_policy_history_raw".
    current_df : pd.DataFrame
        Fresh snapshot from the source system.
    identity_keys : list of str
    tracked_cols : list of str
    today : datetime.date
        Stamped as effective_to on closed rows and effective_from on new rows.
    recorded_at : datetime
        Timestamp stamped on inserted rows.
    log : callable, optional

    Returns
    -------
    total : int
    current_count : int
    n_inserted : int
    n_closed : int

    Example
    -------
        conn_db = duckdb.connect(str(DB_PATH))
        total, current, n_inserted, n_closed = scd2_load_duckdb(
            conn_db, TABLE_NAME, current_df,
            identity_keys=["item_id", "location_name"],
            tracked_cols=TRACKED_COLS,
            today=today, recorded_at=recorded_at, log=log,
        )
        conn_db.close()
    """
    _check_ident(table_name)
    if "." in table_name:
        schema = table_name.split(".")[0]
        if not conn_db.execute(
            "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?", [schema]
        ).fetchone()[0]:
            conn_db.execute(f"CREATE SCHEMA {schema}")
            msg = f"Created schema: {schema}"
            print(msg)
            if log:
                log(msg)

    schema_name, tbl = table_name.split(".", 1) if "." in table_name else ("main", table_name)
    table_exists = conn_db.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
        [schema_name, tbl]
    ).fetchone()[0]

    active_df = (
        conn_db.execute(f"SELECT * FROM {table_name} WHERE is_current = 1").df()
        if table_exists else None
    )

    to_insert, close_keys = _scd2_diff(current_df, active_df, identity_keys, tracked_cols, log=log)

    to_insert = to_insert.copy()
    to_insert["effective_from"] = today
    to_insert["effective_to"]   = None
    to_insert["is_current"]     = 1
    to_insert["recorded_at"]    = recorded_at

    if not table_exists:
        conn_db.execute(f"CREATE TABLE {table_name} AS SELECT * FROM to_insert")
    else:
        if len(close_keys) > 0:
            conn_db.execute("CREATE TEMP TABLE _close_keys AS SELECT * FROM close_keys")
            key_tuple = f"({', '.join(identity_keys)})"
            conn_db.execute(f"""
                UPDATE {table_name}
                SET    effective_to = ?, is_current = 0
                WHERE  is_current = 1
                  AND  {key_tuple} IN (SELECT {key_tuple} FROM _close_keys)
            """, [today])
            conn_db.execute("DROP TABLE _close_keys")

        if len(to_insert) > 0:
            conn_db.execute(f"INSERT INTO {table_name} SELECT * FROM to_insert")

    total         = conn_db.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    current_count = conn_db.execute(f"SELECT COUNT(*) FROM {table_name} WHERE is_current = 1").fetchone()[0]
    n_inserted    = len(to_insert)
    n_closed      = len(close_keys)

    if log:
        log(f"Rows inserted: {n_inserted}")
        log(f"Rows closed:   {n_closed}")
        log(f"Total rows in {table_name}: {total}")
        log(f"Active rows (is_current=1): {current_count}")

    return total, current_count, n_inserted, n_closed


def scd2_load_azure(engine, table_name, current_df, identity_keys, tracked_cols,
                    today, recorded_at, log=None):
    """
    Run one SCD Type 2 cycle against an Azure SQL table.

    Reads active rows, diffs against current_df, closes changed/retired rows,
    and inserts new rows. Creates the table and schema on the first run.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        From connect_azure_sql(). Caller is responsible for engine.dispose().
    table_name : str
        Fully qualified, e.g. "raw.inventory_policy_history_raw".
    current_df : pd.DataFrame
        Fresh snapshot from the source system.
    identity_keys : list of str
    tracked_cols : list of str
    today : datetime.date
    recorded_at : datetime
    log : callable, optional

    Returns
    -------
    total : int
    current_count : int
    n_inserted : int
    n_closed : int

    Example
    -------
        engine = connect_azure_sql(config["databases"]["prod"], log=log)
        total, current, n_inserted, n_closed = scd2_load_azure(
            engine, TABLE_NAME, current_df,
            identity_keys=["item_id", "location_name"],
            tracked_cols=TRACKED_COLS,
            today=today, recorded_at=recorded_at, log=log,
        )
        engine.dispose()
    """
    _check_ident(table_name)
    from sqlalchemy import text

    schema, tbl = table_name.split(".", 1) if "." in table_name else (None, table_name)
    schema_prefix = f"[{schema}]." if schema else ""

    if schema:
        with engine.connect() as c:
            if not c.execute(
                text("SELECT COUNT(*) FROM sys.schemas WHERE name = :s"), {"s": schema}
            ).scalar():
                c.execute(text(f"CREATE SCHEMA [{schema}]"))
                c.commit()
                msg = f"Created schema: {schema}"
                print(msg)
                if log:
                    log(msg)

    with engine.connect() as c:
        table_exists = c.execute(
            text("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = :s AND table_name = :t"),
            {"s": schema or "dbo", "t": tbl}
        ).scalar()

    active_df = (
        pd.read_sql(f"SELECT * FROM {table_name} WHERE is_current = 1", engine)
        if table_exists else None
    )

    to_insert, close_keys = _scd2_diff(current_df, active_df, identity_keys, tracked_cols, log=log)

    to_insert = to_insert.copy()
    to_insert["effective_from"] = today
    to_insert["effective_to"]   = None
    to_insert["is_current"]     = 1
    to_insert["recorded_at"]    = recorded_at

    if not table_exists:
        to_insert.to_sql(tbl, engine, schema=schema, if_exists="replace", index=False)
    else:
        if len(close_keys) > 0:
            key_joins = " AND ".join(f"t.[{k}] = s.[{k}]" for k in identity_keys)
            close_keys.to_sql("_close_keys", engine, schema=schema, if_exists="replace", index=False)
            with engine.connect() as c:
                c.execute(text(f"""
                    UPDATE t
                    SET    t.effective_to = :today, t.is_current = 0
                    FROM   {schema_prefix}[{tbl}] t
                    INNER JOIN {schema_prefix}[_close_keys] s
                        ON  {key_joins}
                    WHERE  t.is_current = 1
                """), {"today": today})
                c.execute(text(f"DROP TABLE {schema_prefix}[_close_keys]"))
                c.commit()

        if len(to_insert) > 0:
            to_insert.to_sql(tbl, engine, schema=schema, if_exists="append", index=False)

    with engine.connect() as c:
        total         = c.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
        current_count = c.execute(text(f"SELECT COUNT(*) FROM {table_name} WHERE is_current = 1")).scalar()

    n_inserted = len(to_insert)
    n_closed   = len(close_keys)

    if log:
        log(f"Rows inserted: {n_inserted}")
        log(f"Rows closed:   {n_closed}")
        log(f"Total rows in {table_name}: {total}")
        log(f"Active rows (is_current=1): {current_count}")

    return total, current_count, n_inserted, n_closed


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

def resolve_id_list(val, id_map):
    """
    Resolve a comma-separated string of IDs to human-readable names.

    Works for any field that stores one or more IDs as a delimited string
    (e.g. custbody_items_sourced_from, subsidiary).

    Parameters
    ----------
    val : str or None
        Raw comma-separated ID string (e.g. "3", "3, 4", "1, 5, 7").
    id_map : dict
        {str(id): name} — keys must be strings.

    Returns
    -------
    str or None
        Comma-separated names, or None if val is null.
        Unknown IDs fall through as-is so missing mappings stay visible.

    Example
    -------
        plant_map = dict(zip(plant_df["id"].astype(str), plant_df["plant_name"]))
        df["sourced_from_name"] = df["custbody_items_sourced_from"].apply(
            lambda v: resolve_id_list(v, plant_map)
        )

        subsidiary_map = {"3": "Screen Innovations", "4": "Shade Innovations"}
        df["subsidiary_name"] = df["subsidiary"].apply(
            lambda v: resolve_id_list(v, subsidiary_map)
        )
    """
    if pd.isna(val) or val is None:
        return None
    ids = [v.strip() for v in str(val).split(",") if v.strip()]
    return ", ".join(id_map.get(i, i) for i in ids)


# Default plant classifications. Pass overrides to resolve_manufacturer_type
# if the plant list changes rather than editing these module-level defaults.
#
# ⚠ EXACT STRING MATCH — SOURCE: customlist_plant_codes (NetSuite)
# These strings must match the `name` field in customlist_plant_codes exactly,
# character for character. resolve_manufacturer_type() uses set membership
# (p in atx_plants), not substring or pattern matching — a plant rename in
# NetSuite will silently reclassify that plant as Contract Manufacturer.
#
# To verify current plant names, run:
#   SELECT id, name FROM customlist_plant_codes
#
# Action required if a plant is renamed, added, or retired in NetSuite:
#   1. Re-run the query above and compare against the sets below.
#   2. Update the affected set(s) to match the new name(s) exactly.
#   3. If a new ATX facility or vendor relationship is added, add the
#      plant name to the appropriate set — new plants default to CM
#      and will not self-classify correctly.
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
