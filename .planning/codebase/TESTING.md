# Testing Patterns

**Analysis Date:** 2026-06-15

## Test Framework

**Runner:** Not configured — no test files, no pytest/unittest config found.

**Config files checked:**
- `pyproject.toml` — no `[tool.pytest.ini_options]` section present
- No `pytest.ini`, `setup.cfg`, `tox.ini`, or `conftest.py` found

**Run Commands:**
```bash
# Install pytest first (not in project dependencies)
pip install pytest pytest-mock

# Then run
pytest                  # Run all tests
pytest -v               # Verbose
pytest --tb=short       # Short tracebacks
```

## Current Test Coverage

**No tests exist in this codebase.** All modules (`ns_utils.py`, `ns_token.py`) are
untested. The project has no `tests/` directory and no co-located test files.

## Recommended Test File Organization

**Location:** Create a `tests/` directory at the project root.

**Naming pattern:** `test_<module>.py` per module.

**Proposed structure:**
```
odbcuite/
├── ns_utils.py
├── ns_token.py
├── pyproject.toml
└── tests/
    ├── conftest.py          # shared fixtures
    ├── test_ns_utils.py     # unit tests for ns_utils
    └── test_ns_token.py     # unit tests for ns_token
```

## What Is Testable (Pure Functions)

These functions in `ns_utils.py` have no I/O dependencies and are directly unit-testable:

**`resolve_id_list(val, id_map)`**
- Handles None/NaN input, comma-separated IDs, unknown ID fallthrough
- No mocking required

**`resolve_manufacturer_type(val, atx_plants, vendor_plants)`**
- Handles all classification branches: ATX, CM, Vendor, Mixed, Direct, None
- No mocking required

**`business_days_diff(esd, asd)`**
- Handles NaT/None, positive (late), negative (early), zero (on-time) cases
- No mocking required

**`bucket_days_late(days)`**
- Pure bucketing logic with nine boundary cases
- No mocking required

**`format_elapsed(start_time)`**
- Simple elapsed-time formatting
- No mocking required

**`_scd2_diff(current_df, active_df, identity_keys, tracked_cols)`**
- Pure pandas — accepts DataFrames, returns DataFrames
- No mocking required; use `pd.DataFrame` literals as inputs

## What Requires Mocking

**`load_config(config_path)`** — reads `config.json` from disk.
- Mock: `unittest.mock.patch("builtins.open", mock_open(read_data=json.dumps({...})))`
- Or: pass an explicit `config_path` pointing to a fixture JSON file.

**`check_odbc_driver(odbc_driver)`** — calls `pyodbc.drivers()`.
- Mock: `unittest.mock.patch("pyodbc.drivers", return_value=["ODBC Driver 18 for SQL Server"])`

**`connect_netsuite(config)`** — calls `pyodbc.connect()`.
- Mock: `unittest.mock.patch("pyodbc.connect")`

**`connect_azure_sql(db_config)`** — calls `pyodbc.drivers()`, `create_engine()`, and optionally `AzureCliCredential`.
- Mock `pyodbc.drivers` and `sqlalchemy.create_engine`.

**`load_duckdb(db_path, table_name, df)`** — opens a real DuckDB file.
- Use `duckdb.connect(":memory:")` and patch or pass a temp path via `tmp_path` fixture.

**`load_azure_sql(db_config, table_name, df)`** — calls `connect_azure_sql` + SQLAlchemy engine.
- Mock `connect_azure_sql` to return a mock engine.

**`ns_token.build_token_password()`** — module-level `load_config()` call at import time.
- Patch `ns_utils.load_config` before importing `ns_token`, or provide a valid `config.json`
  fixture. The function itself is pure HMAC crypto once credentials are loaded.

## Recommended Test Patterns

**Suite structure:**
```python
# tests/test_ns_utils.py
import pytest
import pandas as pd
from ns_utils import resolve_id_list, resolve_manufacturer_type, business_days_diff, bucket_days_late

class TestResolveIdList:
    def test_none_returns_none(self):
        assert resolve_id_list(None, {}) is None

    def test_single_id(self):
        assert resolve_id_list("3", {"3": "Plant A"}) == "Plant A"

    def test_multi_id(self):
        assert resolve_id_list("1, 3", {"1": "A", "3": "B"}) == "A, B"

    def test_unknown_id_falls_through(self):
        assert resolve_id_list("99", {"1": "A"}) == "99"


class TestResolveManufacturerType:
    def test_atx(self):
        assert resolve_manufacturer_type("Plant 01 - Screen Innovations") == "ATX"

    def test_vendor(self):
        assert resolve_manufacturer_type("Plant 00 - Direct From Vendor") == "Vendor"

    def test_mixed_atx_and_cm(self):
        result = resolve_manufacturer_type(
            "Plant 01 - Screen Innovations, Some CM Plant"
        )
        assert result == "Mixed"

    def test_none_returns_none(self):
        assert resolve_manufacturer_type(None) is None


class TestBusinessDaysDiff:
    def test_on_time(self):
        esd = pd.Timestamp("2026-06-10")
        asd = pd.Timestamp("2026-06-10")
        assert business_days_diff(esd, asd) == 0

    def test_late(self):
        esd = pd.Timestamp("2026-06-09")
        asd = pd.Timestamp("2026-06-11")
        assert business_days_diff(esd, asd) == 2

    def test_null_returns_none(self):
        assert business_days_diff(pd.NaT, pd.Timestamp("2026-06-10")) is None
```

**Mocking external I/O:**
```python
# tests/test_ns_utils_io.py
from unittest.mock import patch, MagicMock
import json

def test_load_config_reads_json(tmp_path):
    cfg = {"env": "dev", "netsuite": {"dev": {"dsn": "TestDSN", "auth": "password"}}}
    config_file = tmp_path / "config.json"
    config_file.write_text(json.dumps(cfg))
    from ns_utils import load_config
    result = load_config(config_path=config_file)
    assert result["env"] == "dev"

def test_check_odbc_driver_raises_when_missing():
    with patch("pyodbc.drivers", return_value=[]):
        with pytest.raises(RuntimeError, match="ODBC driver mismatch"):
            from ns_utils import check_odbc_driver
            check_odbc_driver("ODBC Driver 18 for SQL Server")

def test_load_duckdb_drop_and_recreate(tmp_path):
    import pandas as pd
    from ns_utils import load_duckdb
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    db_path = tmp_path / "test.db"
    count = load_duckdb(db_path, "raw.test_table", df)
    assert count == 2
```

## Fixtures Strategy

**Shared fixtures** (`tests/conftest.py`):
```python
import pytest
import pandas as pd
import json

@pytest.fixture
def sample_config(tmp_path):
    cfg = {
        "env": "dev",
        "netsuite": {
            "dev": {"dsn": "TestDSN", "auth": "password", "uid": "u", "pwd": "p"}
        }
    }
    p = tmp_path / "config.json"
    p.write_text(json.dumps(cfg))
    return cfg, p

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "item_id": [1, 2, 3],
        "location_name": ["Austin", "Dallas", "Houston"],
        "qty": [10, 20, 30],
    })
```

## Coverage

**Requirements:** None enforced (no CI, no coverage config).

**Recommended additions to `pyproject.toml`:**
```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "--tb=short"

[tool.coverage.run]
source = ["ns_utils", "ns_token"]
omit = ["tests/*"]
```

**Install coverage:**
```bash
pip install pytest-cov
pytest --cov=ns_utils --cov=ns_token --cov-report=term-missing
```

## Priority Testing Order

1. **`resolve_id_list`** — highest usage, pure function, zero dependencies
2. **`resolve_manufacturer_type`** — complex branch logic, pure function
3. **`bucket_days_late`** — nine boundary cases, pure function
4. **`business_days_diff`** — null handling + numpy busday logic
5. **`_scd2_diff`** — core SCD2 logic, pure pandas, highest business impact if broken
6. **`load_duckdb`** — use in-memory DuckDB (`:memory:`) to avoid filesystem coupling
7. **`load_config`** — use `tmp_path` fixture with a real JSON file

## CI/CD

No CI pipeline is configured. No `.github/workflows/` directory exists.

**To add basic CI**, create `.github/workflows/test.yml`:
```yaml
name: Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install pytest pytest-mock pandas numpy duckdb
      - run: pytest
```

---

*Testing analysis: 2026-06-15*
