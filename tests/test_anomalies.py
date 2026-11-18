"""异常核查：超功率区间、补传拆区间、阈值变更、过期核实结论、失败回滚。"""
import threading

import pytest

from app.anomalies import (
    StaleAnomalyError,
    ThresholdError,
    _load_reading_points,
    build_overview,
    compute_power_intervals,
    parse_threshold,
    verify_anomaly,
)
from app.database import close_conn, get_conn
from tests.conftest import all_readings, post_csv

BASE = [
    "2026-03-01T00:00:00Z,1,100.0",
    "2026-03-01T02:00:00Z,1,120.0",   # 20 kWh / 2h = 10 kWh/h -> 超限
    "2026-03-01T03:00:00Z,1,125.0",   # 5 kWh / 1h = 5 kWh/h -> 等于阈值，不算超限
    "2026-03-01T04:00:00Z,2,0.0",     # 换表，跨周期跳过
    "2026-03-01T05:00:00Z,2,10.0",    # 10 kWh / 1h -> 超限
]


def _set(client, value, fail=False):
    headers = {"X-Fail-Anomaly-Write": "1"} if fail else {}
    return client.post("/anomalies/threshold",
                       data={"max_kwh_per_hour": value}, headers=headers)


def _verify(client, record_id, note="", fail=False):
    headers = {"X-Fail-Anomaly-Write": "1"} if fail else {}
    return client.post(f"/anomalies/{record_id}/verify",
                       data={"note": note}, headers=headers)


def _records():
    conn = get_conn()
    try:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM power_anomalies ORDER BY id").fetchall()]
    finally:
        close_conn(conn)


def _snapshot_counts():
    conn = get_conn()
    try:
        return {
            "readings": conn.execute("SELECT COUNT(*) c FROM readings").fetchone()["c"],
            "thresholds": conn.execute(
                "SELECT COUNT(*) c FROM power_threshold_versions").fetchone()["c"],
            "records": conn.execute("SELECT COUNT(*) c FROM power_anomalies").fetchone()["c"],
            "verified": conn.execute(
                "SELECT COUNT(*) c FROM power_anomalies WHERE status='verified'"
            ).fetchone()["c"],
        }
    finally:
        close_conn(conn)


# ---------------------------------------------------------------- 阈值校验

@pytest.mark.parametrize("bad", ["", "   ", "abc", "0", "-1", "nan", "inf", "NaN"])
def test_invalid_threshold_is_rejected_without_side_effects(client, bad):
    post_csv(client, BASE)
    before = _snapshot_counts()
    resp = _set(client, bad)
    assert resp.status_code == 400
    assert "每小时最大 kWh" in resp.text
    assert _snapshot_counts() == before
    assert len(all_readings()) == len(BASE)


def test_parse_threshold_accepts_positive_numbers():
    assert parse_threshold("3.5") == 3.5
    assert parse_threshold(" 0.001 ") == 0.001
    with pytest.raises(ThresholdError):
        parse_threshold("")


# ---------------------------------------------------------------- 区间识别

def test_over_limit_intervals_become_pending_records(client):
    post_csv(client, BASE)
    resp = _set(client, "5")
    assert resp.status_code == 200
    records = _records()
    assert len(records) == 2  # 恰好等于阈值的不算，跨周期跳过

    by_start = {r["start_ts"]: r for r in records}
    first = by_start["2026-03-01T00:00:00"]
    assert first["end_ts"] == "2026-03-01T02:00:00"
    assert first["start_cumulative"] == 100.0
    assert first["end_cumulative"] == 120.0
    assert first["elapsed_hours"] == pytest.approx(2.0)
    assert first["usage_kwh"] == pytest.approx(20.0)
    assert first["avg_kwh_per_hour"] == pytest.approx(10.0)
    assert first["threshold_kwh_per_hour"] == pytest.approx(5.0)
    assert first["status"] == "pending"

    second = by_start["2026-03-01T04:00:00"]
    assert second["start_period_no"] == 2
    assert second["avg_kwh_per_hour"] == pytest.approx(10.0)

    conn = get_conn()
    try:
        overview = build_overview(conn)
    finally:
        close_conn(conn)
    assert len(overview["pending"]) == 2
    assert overview["missing_records"] == 0
    assert overview["current_over_limit"] == 2


def test_reading_regression_intervals_are_not_evaluated(client):
    # 上传规则本身拒收倒退，这里直接构造库内数据验证兜底跳过
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        bid = conn.execute(
            "INSERT INTO upload_batches(filename,status,message)"
            " VALUES ('x','accepted','x')").lastrowid
        for ts, cum in (("2026-03-01T00:00:00", 100.0),
                        ("2026-03-01T01:00:00", 90.0)):
            conn.execute(
                "INSERT INTO readings(ts,period_no,cumulative,batch_id)"
                " VALUES (?,1,?,?)", (ts, cum, bid))
        conn.execute(
            "INSERT INTO power_threshold_versions(max_kwh_per_hour) VALUES (1)")
        conn.commit()
    finally:
        close_conn(conn)

    conn = get_conn()
    try:
        from app.anomalies import sync_anomalies
        conn.execute("BEGIN IMMEDIATE")
        assert sync_anomalies(conn) == 0
        conn.commit()
    finally:
        close_conn(conn)
    assert _records() == []


# ---------------------------------------------------------------- 核实

def test_verify_pending_record_and_show_as_current(client):
    post_csv(client, BASE)
    _set(client, "5")
    record_id = _records()[0]["id"]

    resp = _verify(client, record_id, note="已到现场复核，表计正常")
    assert resp.status_code == 200
    assert "已标记为已核实" in resp.text

    record = [r for r in _records() if r["id"] == record_id][0]
    assert record["status"] == "verified"
    assert record["verified_at"]
    assert record["verified_note"] == "已到现场复核，表计正常"

    page = client.get("/anomalies").text
    assert "现行阈值下已核实" in page
    assert f"#{record_id}" in page

    # 重复核实：幂等，不改变已有结论
    first_time = record["verified_at"]
    resp = _verify(client, record_id, note="再次点击")
    assert resp.status_code == 200 and "此前已核实" in resp.text
    again = [r for r in _records() if r["id"] == record_id][0]
    assert again["verified_at"] == first_time
    assert again["verified_note"] == "已到现场复核，表计正常"


def test_verify_unknown_record_returns_404(client):
    assert _verify(client, 99999).status_code == 404


# ---------------------------------------------------------------- 补传拆区间

def test_late_backfill_splits_interval_and_old_verdict_stays_historical(client):
    post_csv(client, BASE)
    _set(client, "5")
    old = [r for r in _records() if r["start_ts"] == "2026-03-01T00:00:00"][0]
    _verify(client, old["id"])

    # 在 00:00 与 02:00 之间补传：旧区间被拆成 00->01 与 01->02
    post_csv(client, ["2026-03-01T01:00:00Z,1,105.0"])

    records = _records()
    old_after = [r for r in records if r["id"] == old["id"]][0]
    # 旧记录原封不动，核实结论保留
    assert old_after["status"] == "verified"
    assert old_after["start_cumulative"] == 100.0
    assert old_after["end_cumulative"] == 120.0
    assert old_after["threshold_kwh_per_hour"] == pytest.approx(5.0)

    conn = get_conn()
    try:
        overview = build_overview(conn)
    finally:
        close_conn(conn)
    pending_pairs = {(r["start_ts"], r["end_ts"]) for r in overview["pending"]}
    verified_pairs = {(r["start_ts"], r["end_ts"])
                      for r in overview["verified_current"]}
    # 旧核实结论不能出现在当前已核实中
    assert ("2026-03-01T00:00:00", "2026-03-01T02:00:00") not in verified_pairs
    # 新形成的 01->02 区间是待办（15 kWh / 1h = 15 > 5）
    assert ("2026-03-01T01:00:00", "2026-03-01T02:00:00") in pending_pairs
    # 00->01 恰好 5 kWh/h，不超限
    assert ("2026-03-01T00:00:00", "2026-03-01T01:00:00") not in pending_pairs
    assert any(r["id"] == old["id"] and r["stale_reason"]
               for r in overview["historical"])

    # 旧记录（已过期）不能再被核实
    resp = _verify(client, old["id"])
    assert resp.status_code == 409 and "已过期" in resp.text
    assert [r for r in _records() if r["id"] == old["id"]][0]["status"] == "verified"


def test_backfill_creates_new_record_automatically(client):
    post_csv(client, BASE)
    _set(client, "5")
    before = len(_records())
    # 05:00 -> 06:00 新增区间 30 kWh / 1h 超限，无需重新设置阈值即形成记录
    post_csv(client, ["2026-03-01T06:00:00Z,2,40.0"])
    after = _records()
    assert len(after) == before + 1
    new = [r for r in after if r["start_ts"] == "2026-03-01T05:00:00"][0]
    assert new["avg_kwh_per_hour"] == pytest.approx(30.0)
    assert new["status"] == "pending"


# ---------------------------------------------------------------- 阈值变更

def test_threshold_change_makes_old_records_historical_and_old_verdict_unusable(client):
    post_csv(client, BASE)
    _set(client, "5")
    old = [r for r in _records() if r["start_ts"] == "2026-03-01T00:00:00"][0]
    _verify(client, old["id"])

    resp = _set(client, "50")  # 调高阈值：所有区间不再超限
    assert resp.status_code == 200
    assert "本次新形成 0 条" in resp.text

    conn = get_conn()
    try:
        overview = build_overview(conn)
    finally:
        close_conn(conn)
    assert overview["pending"] == []
    assert overview["verified_current"] == []
    assert overview["current_over_limit"] == 0
    old_hist = [r for r in overview["historical"] if r["id"] == old["id"]][0]
    assert "阈值已变更" in old_hist["stale_reason"]
    # 记录仍保留当时阈值
    assert old_hist["threshold_kwh_per_hour"] == pytest.approx(5.0)
    assert old_hist["status"] == "verified"

    # 旧核实结论不能用于现行阈值下的核实
    resp = _verify(client, old["id"])
    assert resp.status_code == 409
    assert [r for r in _records() if r["id"] == old["id"]][0]["status"] == "verified"

    # 调回 5：会形成新的阈值版本，旧记录仍是历史，不自动复活
    _set(client, "5")
    conn = get_conn()
    try:
        overview = build_overview(conn)
    finally:
        close_conn(conn)
    assert all(r["threshold_version_id"] != old["threshold_version_id"]
               for r in overview["pending"])
    assert any(r["id"] == old["id"] for r in overview["historical"])
    assert all(r["id"] != old["id"] for r in overview["verified_current"])


def test_saving_same_threshold_is_idempotent_and_keeps_verdict(client):
    post_csv(client, BASE)
    _set(client, "5")
    record_id = _records()[0]["id"]
    _verify(client, record_id)
    before = _snapshot_counts()

    resp = _set(client, "5.0")
    assert resp.status_code == 200 and "未新建版本" in resp.text
    assert _snapshot_counts() == before

    page = client.get("/anomalies").text
    assert f"#{record_id}" in page


# ---------------------------------------------------------------- 失败与并发

def test_threshold_write_failure_rolls_back_everything(client):
    post_csv(client, BASE)
    _set(client, "5")
    before = _snapshot_counts()
    before_readings = all_readings()

    resp = _set(client, "1", fail=True)
    assert resp.status_code == 500 and "已回滚" in resp.text
    assert _snapshot_counts() == before
    assert all_readings() == before_readings


def test_verify_write_failure_keeps_pending_state(client):
    post_csv(client, BASE)
    _set(client, "5")
    record_id = _records()[0]["id"]

    resp = _verify(client, record_id, note="x", fail=True)
    assert resp.status_code == 500 and "已回滚" in resp.text
    record = [r for r in _records() if r["id"] == record_id][0]
    assert record["status"] == "pending"
    assert record["verified_at"] is None
    assert record["verified_note"] == ""


def test_failed_upload_does_not_create_or_change_records(client):
    post_csv(client, BASE)
    _set(client, "5")
    before = _records()
    # 同刻异内容 -> 整批拒收；即便里面有一条会造成超限的新区间也不能落地
    resp = post_csv(client, [
        "2026-03-01T00:00:00Z,1,999.0",
        "2026-03-01T07:00:00Z,2,999.0",
    ])
    assert resp.status_code == 303
    assert _records() == before


def test_interleaved_backfill_verify_and_threshold_change_never_leaks_stale_verdict(
        client, db_path):
    """补传、改阈值、核实交错：页面上的“当前”结论始终对应当前读数与现行阈值。"""
    post_csv(client, BASE)
    _set(client, "5")
    errors: list[Exception] = []

    def do_backfill():
        try:
            for hour, cum in ((1, 105.0), (7, 50.0), (8, 200.0)):
                post_csv(client, [f"2026-03-01T{hour:02d}:00:00Z,2,{cum}"])
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    def do_verify():
        try:
            for _ in range(12):
                ids = [r["id"] for r in _records() if r["status"] == "pending"]
                for rid in ids:
                    # 409 表示已过期，正是期望行为；只记录非预期异常
                    _verify(client, rid, note="thread")
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    def do_threshold():
        try:
            for value in ("4", "5", "7", "5"):
                _set(client, value)
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=fn)
               for fn in (do_backfill, do_verify, do_threshold)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors

    conn = get_conn()
    try:
        overview = build_overview(conn)
        current_power = {
            (iv.start.id, iv.end.id): iv.avg_kwh_per_hour
            for iv in compute_power_intervals(_load_reading_points(conn))
        }
    finally:
        close_conn(conn)

    threshold = overview["threshold"]
    assert threshold is not None
    limit = float(threshold["max_kwh_per_hour"])
    for r in overview["pending"] + overview["verified_current"]:
        pair = (r["start_reading_id"], r["end_reading_id"])
        # 当前显示的记录必须仍由当前相邻读数构成，且按现行阈值超限
        assert pair in current_power
        assert current_power[pair] > limit
        assert r["threshold_version_id"] == threshold["id"]
    # 页面渲染同样不得把历史结论混进当前列表
    page = client.get("/anomalies").text
    assert page.count("<h2>历史留档") == 1

    current_ids = {r["id"] for r in overview["pending"] + overview["verified_current"]}
    historical_ids = {r["id"] for r in overview["historical"]}
    assert current_ids.isdisjoint(historical_ids)
    # 所有已核实记录只能出现在“当前已核实”或“历史留档”，不能凭空消失
    conn = get_conn()
    try:
        verified_ids = {r["id"] for r in conn.execute(
            "SELECT id FROM power_anomalies WHERE status='verified'")}
    finally:
        close_conn(conn)
    assert verified_ids <= current_ids | historical_ids
    assert overview["missing_records"] == 0


def test_service_level_verify_rejects_stale_record(db_path):
    """服务层直接调用也做当前性校验（不依赖页面逻辑）。"""
    # 直接写库：三条读数（中间一条把记录区间拆开）+ 阈值 + 一条过期待办记录
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO upload_batches(filename,status,message)"
            " VALUES ('x','accepted','x')")
        bid = conn.execute("SELECT id FROM upload_batches").fetchone()["id"]
        for ts, cum in (("2026-03-01T00:00:00", 100.0),
                        ("2026-03-01T01:00:00", 200.0),
                        ("2026-03-01T02:00:00", 210.0)):
            conn.execute(
                "INSERT INTO readings(ts,period_no,cumulative,batch_id)"
                " VALUES (?,1,?,?)", (ts, cum, bid))
        conn.execute("INSERT INTO power_threshold_versions(max_kwh_per_hour)"
                     " VALUES (5)")
        conn.execute(
            "INSERT INTO power_anomalies(threshold_version_id, threshold_kwh_per_hour,"
            " start_reading_id, end_reading_id, start_ts, start_period_no,"
            " start_cumulative, end_ts, end_period_no, end_cumulative,"
            " elapsed_hours, usage_kwh, avg_kwh_per_hour)"
            " VALUES (1, 5, 1, 3, '2026-03-01T00:00:00', 1, 100,"
            " '2026-03-01T02:00:00', 1, 210, 2, 110, 55)")
        conn.commit()
    finally:
        close_conn(conn)

    conn = get_conn()
    try:
        with pytest.raises(StaleAnomalyError):
            verify_anomaly(conn, 1)
        # 回滚后连接仍可用，状态未变
        assert conn.execute(
            "SELECT status FROM power_anomalies WHERE id=1").fetchone()["status"] == "pending"
    finally:
        close_conn(conn)
