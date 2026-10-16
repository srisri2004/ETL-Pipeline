import sys
from datetime import datetime
from pathlib import Path
import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def lib(tmp_path, monkeypatch):
    sys.path.insert(0, str(ROOT / "dags"))
    import etl_lib as lib
    monkeypatch.setattr(lib, "DB_PATH", str(tmp_path / "wh.db"))
    monkeypatch.setattr(lib, "SQL_DIR", str(ROOT / "sql"))
    monkeypatch.setattr(lib, "WORK_DIR", str(tmp_path / "work"))
    monkeypatch.setattr(lib, "INPUT_DIR", str(tmp_path / "in"))
    lib.init_schema()
    return lib


def src(lib):
    return lib.generate_source_file(datetime(2026, 1, 1), datetime(2026, 1, 2))


def test_validation_quarantines_bad_rows(lib):
    s = lib.validate_file(src(lib))
    assert s["rows_rejected"] > 0 and s["rows_clean"] > 0


def test_incremental_load_is_idempotent(lib):
    s = lib.validate_file(src(lib))
    r1 = lib.incremental_load(s["clean_path"])
    r2 = lib.incremental_load(s["clean_path"])      # same file again
    assert r1["rows_loaded"] > 0 and r2["rows_loaded"] == 0


def test_threshold_breach_fails(lib, monkeypatch):
    monkeypatch.setattr(lib, "REJECT_THRESHOLD", 0.0)
    with pytest.raises(lib.DataQualityError):
        lib.validate_file(src(lib))


def test_elt_summary(lib):
    p = src(lib)
    assert lib.land_raw(p, "b1") == 120
    assert lib.land_raw(p, "b2") == 0               # idempotent landing
    assert lib.run_sql_transform() >= 1
    assert lib.dq_checks() >= 1
