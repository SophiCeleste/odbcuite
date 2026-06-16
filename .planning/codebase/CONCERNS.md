# CONCERNS.md — Technical Debt & Issues

**Project:** odbcuite
**Mapped:** 2026-06-15
**Focus:** Risks, debt, fragile areas, security issues

---

## Security

### Critical: Credentials in Working Tree
- `config.json` contains live database credentials stored in the working tree
- No `.gitignore` entry confirmed to exclude it — risk of accidental commit
- Credentials are loaded at module import time in `ns_token.py` and `ns_utils.py`

### Plaintext Passwords in ODBC Strings
- ODBC connection strings include plaintext `PWD=...` values
- No use of secrets managers, environment variable injection, or credential vaults

### SQL Injection Surface
- f-string interpolation is used for table names and column names in dynamically-built SQL queries
- Example pattern: `f"INSERT INTO {table_name} ..."` — user-supplied or externally-sourced values could be injected
- No parameterization or allowlist validation observed for identifier names

---

## Tech Debt

### Zero Test Coverage
- No test files found anywhere in the repository
- The entire library is untested — all functions, edge cases, and integrations

### No Linting or CI
- No `.flake8`, `pyproject.toml` linting config, `pre-commit` hooks, or CI pipeline (GitHub Actions, etc.)
- No automated quality gate on commits

### Duplicate Import
- `import struct` appears duplicated in at least one module

### Stale Artifacts
- `*.egg-info/` directory and `__pycache__/` present in working tree
- Should be excluded via `.gitignore`

---

## Fragile Areas

### Plant Classification by Exact String Match
- Plants are classified using exact string matching against NetSuite-provided names
- Any rename in NetSuite silently misfires classification — no fallback or alert

### SCD2 NaN Comparison Edge Cases
- Slowly Changing Dimension Type 2 logic uses NaN comparisons that can behave unexpectedly in pandas
- `NaN != NaN` is True in Python — equality checks on nullable fields may produce incorrect change detection

### Upsert Staging Table Not Cleaned Up on Error
- Staging table used during upsert operations is not guaranteed to be dropped if an error occurs mid-operation
- Leaves orphaned tables in the database on failure

---

## Performance

### Full Table Scan in DuckDB Upsert
- Upsert implementation scans full tables rather than using indexed lookups
- Will degrade on larger datasets

### `pd.read_sql` Workaround
- Uses `pd.read_sql` instead of a more direct approach — noted as a workaround in code comments

---

## Dependencies at Risk

### No Version Pins
- `pyproject.toml` / `requirements` do not pin dependency versions
- Breaking changes in upstream packages (pandas, pyodbc, etc.) will silently affect behavior

### `azure-identity` as Hard Dependency
- `azure-identity` is required even for non-Azure deployments
- Adds unnecessary overhead and potential auth complexity for users not using Azure

---

## Missing Features / Incomplete Implementation

### No Retry Logic on NetSuite Connections
- NetSuite API calls have no retry/backoff — transient failures will propagate as hard errors

### No Config Schema Validation
- `config.json` is loaded and used without validation — missing or malformed keys produce runtime errors rather than clear messages

---

## Test Coverage Gaps

- **Entire library untested** — no unit tests, integration tests, or smoke tests
- Key untested areas:
  - `resolve_plants()` classification logic
  - SCD2 change detection
  - Upsert staging lifecycle
  - Azure SQL connection guard
  - NetSuite token refresh
