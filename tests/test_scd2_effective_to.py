import datetime as dt
import duckdb
import pandas as pd
from ns_utils import scd2_load_duckdb


def test_duckdb_close_path_sets_effective_to(tmp_path):
    """Regression: closing a row must not fail casting DATE->INTEGER, and
    the closed row's effective_to must be stamped. (effective_to dtype bug)"""
    table = "raw.widget_history"
    ids = ["widget_id"]
    tracked = ["status"]
    con = duckdb.connect(str(tmp_path / "t.duckdb"))
    try:
        df1 = pd.DataFrame({"widget_id": [1, 2], "status": ["open", "open"]})
        scd2_load_duckdb(con, table, df1, ids, tracked,
                         today=dt.date(2026, 1, 1),
                         recorded_at=dt.datetime(2026, 1, 1, 8, 0))
        # widget 1 changes -> triggers a close (the previously-failing path)
        df2 = pd.DataFrame({"widget_id": [1, 2], "status": ["closed", "open"]})
        total, current, inserted, closed = scd2_load_duckdb(
            con, table, df2, ids, tracked,
            today=dt.date(2026, 1, 2),
            recorded_at=dt.datetime(2026, 1, 2, 8, 0))
        assert inserted == 1
        assert closed == 1
        row = con.execute(
            f"SELECT effective_to FROM {table} "
            f"WHERE widget_id = 1 AND is_current = 0"
        ).fetchone()
        assert row[0] is not None  # closed row has effective_to stamped
    finally:
        con.close()
