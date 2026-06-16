# Coding Conventions

**Analysis Date:** 2026-06-15

## Naming Patterns

**Files:**
- `snake_case` with a namespace prefix: `ns_utils.py`, `ns_token.py`
- New utility modules should follow `ns_<purpose>.py`

**Functions:**
- `snake_case` for all public functions: `log_setup`, `connect_netsuite`, `load_duckdb`, `sql_to_df`, `format_elapsed`
- `_leading_underscore` for module-private helpers: `_scd2_diff`, `_DEFAULT_ATX_PLANTS`, `_DEFAULT_VENDOR_PLANTS`, `_get_token`, `_values_differ`

**Variables:**
- `snake_case` for locals and parameters: `conn_str`, `table_name`, `upsert_keys`, `key_conditions`
- `UPPER_SNAKE_CASE` for module-level constants loaded at import time: `ACCOUNT_ID`, `CONSUMER_KEY`, `CONSUMER_SECRET`, `TOKEN_ID`, `TOKEN_SECRET`
- Short aligned names for closely related pairs — columns are padded with spaces to align assignment operators:
  ```python
  env    = config.get("env", "prod")
  ns     = config["netsuite"]
  ns_env = ns[env]
  ```
  ```python
  n_changed = int(changed_mask.sum())
  n_retired = int(retired_mask.sum())
  n_new     = int(new_mask.sum())
  ```

**Parameters:**
- Optional callable parameters named `log` throughout: `log=None`
- Boolean flag parameters use `is_` prefix in data columns (`is_current`) but not in function signatures

**Internal temporaries:**
- Mask variables suffixed `_mask`: `new_mask`, `retired_mask`, `both_mask`, `changed_mask`
- Key DataFrames suffixed `_keys`: `close_keys`, `insert_keys`, `upsert_keys`

## Code Style

**Formatting:**
- No linter or formatter config detected (no `.flake8`, `.pylintrc`, `pyproject.toml [tool.ruff]`, `.prettierrc`)
- Consistent 4-space indentation throughout
- Blank lines used to separate logical blocks within functions
- Section banners delimit top-level sections within a module:
  ```python
  # ==============================================================
  # SECTION NAME
  # ==============================================================
  ```

**Line length:**
- No enforced limit, but long SQL strings are broken into multi-line f-strings using triple-quoted blocks

**String style:**
- f-strings for interpolation universally; no `%` formatting or `.format()`
- Double quotes for strings

## Module Documentation

**Module docstring:** Each file opens with a triple-quoted module docstring covering purpose, scope, and usage example (`ns_utils.py`, `ns_token.py`)

**Function docstrings:** NumPy-style docstrings on every public function with:
- One-line summary
- Narrative explanation where behavior is non-obvious
- `Parameters` section with type and description
- `Returns` section with type and description
- `Example` section showing actual calling code

Private helpers (`_scd2_diff`, `_values_differ`) also carry full NumPy-style docstrings.

**Inline comments:**
- Section-level `# --- comment ---` or freeform comments explain non-obvious decisions
- Module-level constants annotated with warning comments (`# ⚠ EXACT STRING MATCH`) when correctness depends on exact values matching an external system

## Import Organization

**Order (observed):**
1. Standard library (`json`, `base64`, `hashlib`, `hmac`, `secrets`, `string`, `time`, `pathlib`, `datetime`)
2. Third-party (`numpy`, `pyodbc`, `duckdb`, `pandas`)
3. Local (`from ns_utils import load_config`)

**Deferred imports:** Heavy or conditionally-needed imports (`sqlalchemy`, `azure.identity`, `urllib`, `struct`) are placed inside the function body that requires them, not at the module top:
```python
def connect_azure_sql(db_config, log=None):
    from sqlalchemy import create_engine, event
    import urllib
    import struct
```
This pattern is intentional — it avoids import-time failures when optional dependencies are absent.

**Path Aliases:**
- Not used; `pathlib.Path` used directly for filesystem operations

## Error Handling

**Strategy:** Fail fast with a clear message rather than swallowing exceptions.

**Patterns:**
- `check_odbc_driver()` in `ns_utils.py` raises `RuntimeError` with a human-readable message before attempting connection; always called before any ODBC engine is built
- No broad `try/except` blocks — exceptions propagate to the caller
- Validation before use: `config.get("env", "prod")` for optional config keys, `db_config.get("uid")` for optional params
- Null checks use pandas idioms: `pd.isna(val) or val is None` before operating on values that may be NaT/None

## Logging

**Framework:** Custom file-based logger via `log_setup()` in `ns_utils.py` — no stdlib `logging` module used.

**Pattern:**
- `log_setup(__file__)` returns `(log_path, log)` where `log` is a closure over a timestamped `.log` file under `logs/<script_name>/`
- All utility functions accept `log=None` as an optional parameter; when provided, key events (schema creation, row counts, errors) are written to the log
- Console output uses `cprint()` for colored status messages and plain `print()` for inline counts and row stats
- Color conventions enforced via `cprint()`:
  - `"cyan"` — progress/status
  - `"yellow"` — warnings
  - `"red"` — errors
  - `"green"` — success/done
  - `"white"` — informational (row counts, elapsed time) [default]

## Function Design

**Size:** Functions are long when necessary (e.g., `load_azure_sql` at ~100 lines), but each does a single cohesive operation. Complex functions use inline blank-line blocks to separate phases.

**Parameters:**
- Consistent parameter order: `(primary_resource, table_name, df, log=None, upsert_keys=None)`
- Config dicts passed whole (`db_config`, `config`) rather than individual fields, except where a sub-dict is extracted at call site

**Return Values:**
- Functions return meaningful values: `load_duckdb` and `load_azure_sql` return total row count (`int`); SCD2 functions return `(total, current_count, n_inserted, n_closed)` tuple
- `None` returned for null inputs rather than raising (e.g., `resolve_id_list`, `business_days_diff`, `bucket_days_late`)

## Module Design

**Exports:**
- No `__all__` defined; all non-underscore names are implicitly public
- Private helpers prefixed with `_` to signal non-public use

**Barrel Files:**
- Not used; callers import directly from `ns_utils` and `ns_token`

**Module-level side effects:**
- `ns_token.py` loads `config.json` and extracts credentials at import time (lines 39–44). This means importing `ns_token` always triggers a config file read and will raise `FileNotFoundError` if `config.json` is absent.

---

*Convention analysis: 2026-06-15*
