# odbcuite

Lightweight Python utility library for extracting data from NetSuite via ODBC. Standardizes logging, config, connections, DuckDB staging, plant/manufacturer resolution, and business-day math across extract scripts.

> **NOTICE:** This project is an independent, unofficial utility for working with NetSuite ODBC connections. It is **not affiliated with, endorsed by, or supported by Oracle NetSuite or any of its subsidiaries.** "NetSuite" is a registered trademark of Oracle Corporation.

---

## Modules (`ns_utils.py`)

| Function | Purpose |
|---|---|
| `log_setup(script_path)` | Timestamped log file under `logs/<script_name>/` |
| `load_config(config_path)` | Load `config.json` credentials/paths |
| `connect_netsuite(dsn, uid, pwd)` | Open pyodbc connection (`autocommit=True` required) |
| `sql_to_df(conn, query)` | Execute query, return DataFrame without pd warning |
| `load_duckdb(db_path, table_name, df)` | Drop-and-recreate staging table in DuckDB |
| `format_elapsed(start_time)` | Wall time since start as `Xh Ym Zs` |
| `resolve_plants(val, plant_map)` | Comma-separated plant IDs → human-readable names |
| `resolve_manufacturer_type(val, ...)` | Classify resolved plants as ATX / CM / Vendor / Mixed / Direct |
| `business_days_diff(esd, asd)` | Signed business-day delta (positive = late) |
| `bucket_days_late(days)` | Bucket delta into lateness label (e.g. "1 week late") |

## Config

`config.json` (never committed — contains credentials):

```json
{
  "netsuite_dsn": "YourSystemDSN",
  "netsuite_uid": "you@example.com",
  "netsuite_pwd": "yourpassword",
  "db_path": "C:/path/to/data.duckdb",
  "raw_folder": "C:/path/to/csv_exports",
  "table_name": "raw.your_table"
}
```

## Quick start

```python
from ns_utils import log_setup, load_config, connect_netsuite, sql_to_df, load_duckdb, format_elapsed
from datetime import datetime

start = datetime.now()
log_path, log = log_setup(__file__)
config = load_config()

conn = connect_netsuite(config["netsuite_dsn"], config["netsuite_uid"], config["netsuite_pwd"])
df = sql_to_df(conn, "SELECT * FROM transaction WHERE rownum <= 1000")
conn.close()

count = load_duckdb(config["db_path"], config["table_name"], df)
log(f"Loaded {count} rows in {format_elapsed(start)}")
```

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

## Requirements

- Python 3.9+
- `pyodbc`, `duckdb`, `pandas`, `numpy`
- NetSuite ODBC driver + System DSN configured (64-bit)
