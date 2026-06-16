# Architecture

**Analysis Date:** 2026-06-15

## Overview

`odbcuite` is a single-purpose Python utility library. It provides a
standardized set of building blocks for scripts that extract data from
NetSuite via ODBC and stage it into either a local DuckDB file (dev) or
Azure SQL Server (prod). There is no application server, no web layer, and no
daemon process. The library is consumed by caller scripts that import from it
directly.

## Architectural Style

**Pattern:** Shared utility library — not a framework, not a service.

The design is deliberately flat. There are two modules (`ns_utils`,
`ns_token`) and no layers, no dependency injection container, and no class
hierarchy. Callers orchestrate the workflow; the library provides stateless
functions that do one thing each. This keeps individual scripts simple to
read and audit while eliminating copy-paste duplication across them.

## Module Responsibilities

### `ns_utils.py` — All shared utilities

The single entry point for extract scripts. Organized into named sections
delimited by banner comments:

| Section | Responsibility |
|---|---|
| Logging | `log_setup()` — timestamped log file factory |
| Console Output | `cprint()` — ANSI-colored status messages |
| Config | `load_config()` — reads `config.json` on demand |
| NetSuite ODBC | `check_odbc_driver()`, `connect_netsuite()`, `sql_to_df()` |
| DuckDB | `load_duckdb()` — drop-recreate or upsert |
| Azure SQL | `connect_azure_sql()`, `load_azure_sql()` — drop-recreate or upsert |
| SCD Type 2 | `_scd2_diff()`, `scd2_load_duckdb()`, `scd2_load_azure()` |
| Timing | `format_elapsed()` |
| Plant/Manufacturer resolution | `resolve_id_list()`, `resolve_manufacturer_type()` |
| Business Days | `business_days_diff()`, `bucket_days_late()` |

### `ns_token.py` — TBA credential generator

Generates the HMAC-SHA256 token password for NetSuite SuiteAnalytics Connect
(prod). Contains no logic beyond the token construction algorithm. Loads
`config.json` once at import time and exposes a single public function:
`build_token_password()`. Called automatically by `connect_netsuite()` when
`env = "prod"` — callers do not need to invoke it directly.

## Data Flow

```
NetSuite (ODBC)
      |
      | pyodbc connection (DSN)
      |   dev  → email/password
      |   prod → TBA token (ns_token.build_token_password)
      v
sql_to_df()  →  pd.DataFrame  (raw query result)
      |
      |  optional transform in caller script
      |  (resolve_id_list, resolve_manufacturer_type,
      |   business_days_diff, bucket_days_late, ...)
      v
  ┌───────────────────────────┐
  │  dev  → load_duckdb()     │  DuckDB file (local)
  │         scd2_load_duckdb()|
  │                           │
  │  prod → load_azure_sql()  │  Azure SQL (cloud)
  │         scd2_load_azure() │
  └───────────────────────────┘
```

## Environment Routing

A single `"env"` key in `config.json` controls auth and destination for every
function. No code changes are needed to switch between dev and prod.

| env | NetSuite auth | Target database |
|---|---|---|
| `"dev"` | Email/password | DuckDB file |
| `"prod"` | TBA token (HMAC-SHA256) | Azure SQL Server |

`connect_netsuite()` reads `config["env"]` and branches internally. Caller
scripts typically propagate `env` to database routing with a simple check:

```python
if db_config["type"] == "duckdb":
    load_duckdb(...)
else:
    load_azure_sql(...)
```

## Load Strategies

Both `load_duckdb` and `load_azure_sql` support two modes, selected by the
`upsert_keys` parameter:

**Drop-and-recreate (default, `upsert_keys=None`):**
- Table is dropped and fully rebuilt on every run.
- Suitable for point-in-time snapshots where history is not required.

**Append/upsert (`upsert_keys=[...]`):**
- Table is created on the first run; subsequent runs insert only rows whose
  key combination is not already present.
- Suitable for accumulating historical records across daily runs.
- Azure SQL implementation uses a staging table (`_stg_<table>`) as an
  intermediate step to isolate the NOT EXISTS check from the live table.

## SCD Type 2 Pattern

History tracking uses the Slowly Changing Dimension Type 2 pattern, implemented
across a three-function stack:

1. **`_scd2_diff(current_df, active_df, identity_keys, tracked_cols)`** —
   pure pandas diff engine. Compares the current source snapshot against the
   active rows in the history table and returns two DataFrames: rows to insert
   (new + changed) and identity keys to close (changed + retired). No database
   dependency; testable independently.

2. **`scd2_load_duckdb(conn_db, ...)`** — applies the diff output to a DuckDB
   table. Closes rows via a keyed `UPDATE ... SET is_current=0` using a temp
   table for the key set, then inserts new rows.

3. **`scd2_load_azure(engine, ...)`** — applies the diff output to Azure SQL.
   Same semantics as the DuckDB variant but uses SQLAlchemy + T-SQL syntax.

History tables carry four SCD2 columns stamped by these functions:
`effective_from`, `effective_to` (NULL when active), `is_current` (1/0),
`recorded_at`.

## Authentication Architecture

### NetSuite TBA (prod)

Token password is generated locally — no network call to any auth service.
The algorithm (per Oracle NetSuite TBA procedure):

1. Base string: `account_id&consumer_key&token_id&nonce&timestamp`
2. Signing key: `consumer_secret&token_secret`
3. Signature: HMAC-SHA256(base_string, signing_key), Base64-encoded
4. Token password: `base_string&signature&HMAC-SHA256`

A new nonce and timestamp are generated on every `build_token_password()` call.
The token is never cached or reused between connections.

### Azure SQL (prod, no-password path)

When `uid`/`pwd` are absent from `db_config`, `connect_azure_sql()` uses
`AzureCliCredential` from `azure-identity`. The Azure AD access token is
fetched and injected into the ODBC connection as a binary struct via a
SQLAlchemy `do_connect` event hook (`SQL_COPT_SS_ACCESS_TOKEN = 1256`).
Requires `az login` to be current on the host.

## Credential Management

All credentials live in `config.json` at the repo root. The file is excluded
from git via `.gitignore` and is never committed. `ns_token.py` reads it once
at import time; `ns_utils.py` reads it on demand via `load_config()`. There is
no secrets manager, vault, or environment-variable injection — the config file
is the single credential store.

## Logging Architecture

`log_setup(script_path)` returns a `(log_path, log)` pair where `log` is a
closure writing append-only to a timestamped file under
`logs/<script_name>/<script_name>_YYYYMMDD_HHMMSS.log`. All utility functions
accept `log=None`; passing the callable causes key events (schema creation, row
counts) to be written to the file. Console output is separated into a colored
`cprint()` helper using ANSI escape codes.

## Key Design Decisions

**Stateless functions, not classes:** All public functions are module-level and
stateless. Connection objects and engines are created by the caller and passed
in. This makes each function independently testable and keeps scripts
straightforward.

**Deferred imports for optional dependencies:** `sqlalchemy`, `azure.identity`,
`urllib`, and `struct` are imported inside the function bodies that need them,
not at module top. This prevents import-time failures when optional
dependencies are absent (e.g., running a DuckDB-only script without
`azure-identity` installed).

**`ns_token.py` loads config at import time:** A deliberate trade-off.
Importing `ns_token` always triggers a config file read, which will raise
`FileNotFoundError` if `config.json` is absent. This is an accepted constraint
because `ns_token` is only ever used on machines configured for prod access.

**`autocommit=True` on NetSuite connections:** Required by NetSuite ODBC
behavior. Without it, the connection hangs indefinitely waiting for a
transaction the server never starts.

**Null-safe resolution functions:** `resolve_id_list`, `business_days_diff`,
and `bucket_days_late` return `None` for null inputs rather than raising.
Unknown IDs in `resolve_id_list` fall through as-is so missing mappings stay
visible in output rather than silently disappearing.

---

*Architecture analysis: 2026-06-15*
