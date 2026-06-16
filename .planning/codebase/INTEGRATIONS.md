# External Integrations

**Analysis Date:** 2026-06-15

## APIs & External Services

**ERP / Source System:**
- NetSuite SuiteAnalytics Connect — primary data source for all extract scripts
  - SDK/Client: `pyodbc` — ODBC connection via system DSN
  - Dev Auth: Username/password (`UID` + `PWD` in connection string)
  - Prod Auth: Token-Based Authentication (TBA) — HMAC-SHA256 token password generated locally by `ns_token.build_token_password()` (`ns_token.py`)
  - DSNs: `"NetSuiteDev"` (dev), `"NetSuiteProd"` (prod) — configured in system ODBC manager, named in `config.json`
  - Protocol: ODBC with `autocommit=True` required (NetSuite never starts a transaction)

## Data Storage

**Databases:**
- DuckDB (dev/local)
  - Type: Embedded file-based analytical database
  - Connection: File path from `config["databases"]["dev"]["path"]` (e.g. `raw/dev.duckdb`)
  - Client: `duckdb` Python library (`ns_utils.load_duckdb`, `ns_utils.scd2_load_duckdb`)
  - Schemas: Created dynamically (e.g. `raw`)

- Azure SQL Server (prod)
  - Type: Cloud-hosted relational database (Microsoft Azure)
  - Connection: `config["databases"]["prod"]["server"]` — `si-sql-01.database.windows.net`
  - Database: `si-db`
  - ODBC Driver: `"ODBC Driver 17 for SQL Server"` (configured in `config.json`)
  - Client: SQLAlchemy engine (`mssql+pyodbc`) via `ns_utils.connect_azure_sql` and `ns_utils.load_azure_sql`
  - Auth (SQL login): `uid` + `pwd` from `config["databases"]["prod"]` when present
  - Auth (Azure AD): `AzureCliCredential` from `azure-identity`; token injected via SQLAlchemy `do_connect` event hook when no `uid`/`pwd` supplied (`ns_utils.py` lines 345–362)
  - Schemas used: `raw` (raw extracts), `reporting` (reporting layer)

**File Storage:**
- Local filesystem — raw CSV exports written to `config["raw_folder"]` (default: `raw/` directory)
- DuckDB database file also stored on local filesystem (dev only)

**Caching:**
- None

## Authentication & Identity

**NetSuite (prod):**
- Token-Based Authentication (TBA) per Oracle NetSuite TBA procedure
- Implementation: Pure local HMAC-SHA256 crypto in `ns_token.py`
  - Credentials: `account_id`, `consumer_key`, `consumer_secret`, `token_id`, `token_secret` from `config.json`
  - Token password format: `{account_id}&{consumer_key}&{token_id}&{nonce}&{timestamp}&{base64_sig}&HMAC-SHA256`
  - New nonce and timestamp generated on every call — no caching of token

**NetSuite (dev):**
- Email/password auth — `UID` and `PWD` passed directly in ODBC connection string

**Azure SQL (prod, no-password path):**
- Azure AD integrated auth via `AzureCliCredential` from `azure-identity`
- Token scope: `https://database.windows.net/.default`
- Token injected as binary struct into ODBC connection attributes (`SQL_COPT_SS_ACCESS_TOKEN = 1256`)
- Requires `az login` on the host before running

## Monitoring & Observability

**Error Tracking:**
- None — no external error tracking service integrated

**Logs:**
- File-based logging via `ns_utils.log_setup(__file__)`
- Log files written to `logs/<script_name>/<script_name>_YYYYMMDD_HHMMSS.log`
- Appended line-by-line with a plain callable `log(msg)`
- ANSI-colored console output via `ns_utils.cprint(msg, color)` for human-readable status

## CI/CD & Deployment

**Hosting:**
- Not detected — no deployment manifests, Dockerfiles, or cloud config files present

**CI Pipeline:**
- Not detected — no GitHub Actions, Azure Pipelines, or other CI configuration found

## Environment Configuration

**Required config.json keys (never committed):**
- `env` — selects active environment (`"dev"` or `"prod"`)
- `netsuite.dev.dsn`, `netsuite.dev.uid`, `netsuite.dev.pwd`
- `netsuite.prod.dsn`, `netsuite.prod.account_id`, `netsuite.prod.consumer_key`, `netsuite.prod.consumer_secret`, `netsuite.prod.token_id`, `netsuite.prod.token_secret`
- `databases.dev.path`
- `databases.prod.server`, `databases.prod.database`, `databases.prod.odbc_driver`
- `tables.*` — fully qualified destination table names
- `raw_folder` — output directory for CSV exports

**Secrets location:**
- `config.json` in the repo root — excluded from git via `.gitignore`

## Webhooks & Callbacks

**Incoming:**
- None

**Outgoing:**
- None

---

*Integration audit: 2026-06-15*
