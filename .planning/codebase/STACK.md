# Technology Stack

**Analysis Date:** 2026-06-15

## Languages

**Primary:**
- Python 3.13 - All source code (`ns_utils.py`, `ns_token.py`)

**Secondary:**
- None detected

## Runtime

**Environment:**
- CPython 3.13.13

**Package Manager:**
- pip / setuptools (setuptools >= 64)
- Lockfile: Not present (no `requirements.txt` lockfile or `pip.lock`)

## Frameworks

**Core:**
- None — pure Python library, no web or application framework

**Testing:**
- Not detected — no test files, no pytest/unittest configuration found

**Build/Dev:**
- setuptools >= 64 — package build backend (`pyproject.toml`)

## Key Dependencies

**Critical:**
- `pyodbc` (unpinned) — ODBC connectivity for NetSuite SuiteAnalytics Connect; also used for Azure SQL fallback
- `duckdb` (unpinned) — embedded analytical database, used as local/dev data store
- `pandas` (unpinned) — DataFrame I/O and transformation throughout all extract logic
- `numpy` (unpinned) — business-day math (`np.busday_count`) and null handling
- `sqlalchemy` (unpinned) — SQLAlchemy engine for Azure SQL writes via `to_sql()`; event hooks for token injection
- `azure-identity` (unpinned) — `AzureCliCredential` for Azure AD token auth to Azure SQL (prod, no-password path)

**Infrastructure:**
- `hmac`, `hashlib`, `base64`, `secrets` (stdlib) — NetSuite TBA token password generation in `ns_token.py`
- `json`, `pathlib`, `datetime` (stdlib) — config loading, logging, timestamping

## Configuration

**Environment:**
- All credentials and environment settings live in `config.json` (not committed to git)
- `config.json` is read at import time in `ns_token.py` and on demand via `load_config()` in `ns_utils.py`
- Key configs required:
  - `env` — `"dev"` or `"prod"` switches auth and database targets
  - `netsuite.dev.*` — DSN name, UID/PWD for password auth
  - `netsuite.prod.*` — DSN name, account_id, consumer_key, consumer_secret, token_id, token_secret for TBA
  - `databases.dev.*` — DuckDB file path
  - `databases.prod.*` — Azure SQL server, database, ODBC driver name, schema names
  - `tables.*` — fully qualified target table names (e.g. `"raw.inventory_position_raw"`)
  - `raw_folder` — directory for CSV exports

**Build:**
- `pyproject.toml` — single build config; declares package name `odbcuite`, version `0.1.0`, Python `>=3.9` requirement, and the two py-modules (`ns_utils`, `ns_token`)

## Platform Requirements

**Development:**
- Python 3.9+ (enforced by `pyproject.toml`)
- ODBC driver installed on host: `"NetSuiteDev"` DSN or `"NetSuiteProd"` DSN configured via system ODBC manager
- Azure CLI authenticated (`az login`) when using Azure AD integrated auth path for Azure SQL

**Production:**
- Windows or Linux host with ODBC Driver 17 for SQL Server installed
- Azure SQL server accessible from execution host
- NetSuite SuiteAnalytics Connect ODBC DSN configured

---

*Stack analysis: 2026-06-15*
