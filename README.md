# odbcuite

Lightweight Python utility library for extracting data from NetSuite via ODBC. Standardizes logging, config, connections, DuckDB/Azure SQL staging, SCD Type 2 history, plant/manufacturer resolution, and business-day math across extract scripts.

> **NOTICE:** This project is an independent, unofficial utility for working with NetSuite ODBC connections. It is **not affiliated with, endorsed by, or supported by Oracle NetSuite or any of its subsidiaries.** "NetSuite" is a registered trademark of Oracle Corporation.

---

## Modules

### `ns_utils.py` — shared utilities

| Function | Purpose |
|---|---|
| `log_setup(script_path)` | Timestamped log file under `logs/<script_name>/` |
| `load_config(config_path)` | Load `config.json` credentials and paths |
| `check_odbc_driver(odbc_driver, log)` | Verify ODBC driver is installed; raise clear error if not |
| `connect_netsuite(config)` | Open pyodbc connection; routes dev→password, prod→TBA automatically |
| `sql_to_df(conn, query)` | Execute query, return DataFrame without pandas UserWarning |
| `load_duckdb(db_path, table_name, df, ...)` | Drop-and-recreate or upsert staging table in DuckDB |
| `connect_azure_sql(db_config, log)` | Build SQLAlchemy engine for Azure SQL (SQL login or Azure AD) |
| `load_azure_sql(db_config, table_name, df, ...)` | Drop-and-recreate or upsert staging table in Azure SQL |
| `scd2_load_duckdb(conn_db, table_name, ...)` | SCD Type 2 cycle against a DuckDB table |
| `scd2_load_azure(engine, table_name, ...)` | SCD Type 2 cycle against an Azure SQL table |
| `format_elapsed(start_time)` | Wall time since start as `Xh Ym Zs` |
| `resolve_id_list(val, id_map)` | Comma-separated IDs → human-readable names |
| `resolve_manufacturer_type(val, ...)` | Classify resolved plants as ATX / CM / Vendor / Mixed / Direct |
| `business_days_diff(esd, asd)` | Signed business-day delta (positive = late) |
| `bucket_days_late(days)` | Bucket delta into lateness label (e.g. `"1 week late"`) |

### `ns_token.py` — TBA token password generator

Generates the HMAC-SHA256 token password string for NetSuite SuiteAnalytics Connect (ODBC) per the [NetSuite TBA procedure](https://docs.oracle.com/en/cloud/saas/netsuite/ns-online-help/article_163240164565.html). No network calls — pure local crypto. Called automatically by `connect_netsuite` when `env = "prod"`.

| Function | Purpose |
|---|---|
| `build_token_password()` | Generate a fresh token password from prod TBA credentials in config |

---

## Config

`config.json` (never committed — contains credentials):

```json
{
  "env": "dev",
  "netsuite": {
    "dev": {
      "dsn": "NetSuiteDev",
      "auth": "password",
      "uid": "you@example.com",
      "pwd": "yourpassword"
    },
    "prod": {
      "dsn": "NetSuiteProd",
      "auth": "tba",
      "account_id": "1234567",
      "consumer_key": "...",
      "consumer_secret": "...",
      "token_id": "...",
      "token_secret": "..."
    }
  },
  "databases": {
    "dev": {
      "type": "duckdb",
      "path": "raw/dev.duckdb"
    },
    "prod": {
      "type": "mssql",
      "server": "yourserver.database.windows.net",
      "database": "yourdb",
      "odbc_driver": "ODBC Driver 17 for SQL Server",
      "schema_raw": "raw",
      "schema_reporting": "reporting"
    }
  },
  "tables": {
    "your_table": "raw.your_table_raw"
  },
  "raw_folder": "raw"
}
```

Switch between dev and prod by changing `"env"` — all auth and database routing follows automatically.

---

## Quick start

```python
from ns_utils import log_setup, load_config, connect_netsuite, sql_to_df, load_duckdb, format_elapsed
from datetime import datetime

start      = datetime.now()
log_path, log = log_setup(__file__)
config     = load_config()
env        = config["env"]
db_config  = config["databases"][env]

conn = connect_netsuite(config)   # uses password (dev) or TBA (prod) automatically
df   = sql_to_df(conn, "SELECT * FROM transaction WHERE rownum <= 1000")
conn.close()

if db_config["type"] == "duckdb":
    count = load_duckdb(db_config["path"], "raw.your_table", df, log=log)
else:
    count = load_azure_sql(db_config, "raw.your_table", df, log=log)

log(f"Loaded {count} rows in {format_elapsed(start)}")
```

---

## Installation

Clone the repo, then install as an editable package into your virtual environment:

```bash
git clone https://github.com/your-org/odbcuite.git
pip install -e "C:/path/to/odbcuite"
```

Run once per virtual environment. After that, import directly in any script:

```python
from ns_utils import connect_netsuite, sql_to_df
```

---

## Requirements

- Python 3.9+
- `pyodbc`, `duckdb`, `pandas`, `numpy`, `sqlalchemy`, `azure-identity`
- NetSuite ODBC driver + two System DSNs configured (64-bit): `NetSuiteDev`, `NetSuiteProd`
