"""超功率读数区间核查。

业务规则
--------
1. 质控员设置一个全局阈值：每小时最大 kWh（即电表额定功率，kWh/h）。
2. 对**当前全部已提交读数**按 UTC 时间排序，仅相邻且同周期的两条读数
   形成区间；平均功率 =（后笔累计 - 前笔累计）/ UTC 时差（小时）。
3. 平均功率 **严格大于** 阈值时，形成一条待复测记录。记录快照两端读数
   （读数 id、UTC 时间、周期号、累计值）、时差、用量、平均功率和当时的阈值。
4. 质控员可把当前待办标记为已核实；核实结论只属于“该区间 + 该阈值版本”。
5. 迟到补传可能把旧区间拆成两个新区间；阈值变更会生成新的评估版本。
   旧记录（含旧核实结论）永久保留可查，但不会自动套用到新形成的区间。
6. 阈值变更、补传触发的重算与核实都在 BEGIN IMMEDIATE 事务内完成；
   任一写入失败整体回滚，不破坏既有记录和读数。
"""
from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone


class ThresholdError(Exception):
    """阈值设置无效（不产生任何写入）。"""


class StaleAnomalyError(Exception):
    """记录已不属于当前读数状态/现行阈值，不能核实。"""


# ---------------------------------------------------------------- 数据结构

@dataclass(frozen=True)
class ReadingPoint:
    id: int
    ts: str                 # 规范 UTC 时间 YYYY-MM-DDTHH:MM:SS
    period_no: int
    cumulative: float


@dataclass(frozen=True)
class PowerInterval:
    start: ReadingPoint
    end: ReadingPoint
    elapsed_hours: float
    usage_kwh: float
    avg_kwh_per_hour: float


# ---------------------------------------------------------------- 基础计算

def parse_threshold(raw: object) -> float:
    """校验并规范化“每小时最大 kWh”。必须是有限正数。"""
    text = str(raw).strip()
    if not text:
        raise ThresholdError("每小时最大 kWh 不能为空")
    try:
        value = float(text)
    except (TypeError, ValueError):
        raise ThresholdError(f"每小时最大 kWh 必须是数字，收到 '{text}'")
    if not math.isfinite(value):
        raise ThresholdError("每小时最大 kWh 必须是有限的数字")
    if value <= 0:
        raise ThresholdError("每小时最大 kWh 必须大于 0")
    return value


def _parse_utc(ts: str) -> datetime:
    # 库内统一保存为不带时区的 UTC 规范字符串
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _load_reading_points(conn: sqlite3.Connection) -> list[ReadingPoint]:
    rows = conn.execute(
        "SELECT id, ts, period_no, cumulative FROM readings"
        " ORDER BY ts, period_no, id").fetchall()
    return [ReadingPoint(r["id"], r["ts"], r["period_no"], r["cumulative"])
            for r in rows]


def compute_power_intervals(
        readings: list[ReadingPoint]) -> list[PowerInterval]:
    """相邻且同周期的读数形成区间；换表跨周期或时差非正则跳过。"""
    points = sorted(readings, key=lambda p: (p.ts, p.period_no, p.id))
    intervals: list[PowerInterval] = []
    for prev, cur in zip(points, points[1:]):
        if prev.period_no != cur.period_no:
            continue  # 换表跨周期：不估算
        seconds = (_parse_utc(cur.ts) - _parse_utc(prev.ts)).total_seconds()
        if seconds <= 0:
            continue
        usage = cur.cumulative - prev.cumulative
        if usage < 0:
            # 上传规则已拦截倒退；这里再兜底，异常核查只处理未倒退区间
            continue
        hours = seconds / 3600.0
        intervals.append(PowerInterval(
            start=prev, end=cur, elapsed_hours=hours, usage_kwh=usage,
            avg_kwh_per_hour=usage / hours,
        ))
    return intervals


# ---------------------------------------------------------------- 阈值版本

def current_threshold_version(conn: sqlite3.Connection):
    return conn.execute(
        "SELECT * FROM power_threshold_versions ORDER BY id DESC LIMIT 1"
    ).fetchone()


def _get_threshold_version(conn: sqlite3.Connection, version_id: int):
    return conn.execute(
        "SELECT * FROM power_threshold_versions WHERE id = ?", (version_id,)
    ).fetchone()


# ---------------------------------------------------------------- 记录同步

def sync_anomalies(conn: sqlite3.Connection,
                   threshold_version_id: int | None = None) -> int:
    """把当前读数状态下所有超限区间补成待复测记录。

    只插入缺失记录，不修改已有记录（尤其不覆盖已核实状态）。
    调用方负责事务；这里不自行 BEGIN/COMMIT。
    """
    version = None
    if threshold_version_id is None:
        version = current_threshold_version(conn)
    else:
        version = _get_threshold_version(conn, threshold_version_id)
    if version is None:
        return 0  # 尚未设置阈值，无需评估

    threshold = float(version["max_kwh_per_hour"])
    inserted = 0
    for iv in compute_power_intervals(_load_reading_points(conn)):
        if iv.avg_kwh_per_hour <= threshold:
            continue
        cur = conn.execute(
            "INSERT INTO power_anomalies("
            " threshold_version_id, threshold_kwh_per_hour,"
            " start_reading_id, end_reading_id,"
            " start_ts, start_period_no, start_cumulative,"
            " end_ts, end_period_no, end_cumulative,"
            " elapsed_hours, usage_kwh, avg_kwh_per_hour, status"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')"
            " ON CONFLICT(threshold_version_id, start_reading_id, end_reading_id)"
            " DO NOTHING",
            (version["id"], threshold,
             iv.start.id, iv.end.id,
             iv.start.ts, iv.start.period_no, iv.start.cumulative,
             iv.end.ts, iv.end.period_no, iv.end.cumulative,
             iv.elapsed_hours, iv.usage_kwh, iv.avg_kwh_per_hour),
        )
        if cur.rowcount > 0:
            inserted += 1
    return inserted


def set_threshold(conn: sqlite3.Connection, raw_value: object,
                  fail_before_commit: bool = False) -> dict:
    """设置现行阈值并重算记录；同一数值不新建版本，保留已有核实结论。"""
    value = parse_threshold(raw_value)  # 校验失败时任何数据都未触碰

    conn.execute("BEGIN IMMEDIATE")
    try:
        latest = current_threshold_version(conn)
        changed = latest is None or float(latest["max_kwh_per_hour"]) != value
        if changed:
            cur = conn.execute(
                "INSERT INTO power_threshold_versions(max_kwh_per_hour)"
                " VALUES (?)", (value,))
            version_id = int(cur.lastrowid)
        else:
            version_id = int(latest["id"])

        inserted = sync_anomalies(conn, version_id)

        if fail_before_commit:
            # 自测用故障注入：验证阈值与记录同事务回滚
            raise RuntimeError("故障注入：模拟阈值设置提交前失败")

        conn.commit()
        return {"threshold_version_id": version_id,
                "max_kwh_per_hour": value,
                "changed": changed,
                "new_records": inserted}
    except Exception:
        conn.rollback()
        raise


# ---------------------------------------------------------------- 核实

def verify_anomaly(conn: sqlite3.Connection, record_id: int,
                   note: str = "", fail_before_commit: bool = False) -> dict:
    """把当前有效的待复测记录标记为已核实。

    事务内重新校验：记录必须属于现行阈值版本，且两端读数当前仍然相邻
    并超过阈值；否则视为过期，拒绝写入核实结论。
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        record = conn.execute(
            "SELECT * FROM power_anomalies WHERE id = ?", (record_id,)
        ).fetchone()
        if record is None:
            raise LookupError(f"待复测记录 #{record_id} 不存在")

        latest = current_threshold_version(conn)
        if latest is None:
            raise StaleAnomalyError("尚未设置阈值，记录不能核实")
        if record["threshold_version_id"] != latest["id"]:
            raise StaleAnomalyError(
                "阈值已变更，该记录只作历史留档，不能按现行阈值核实")

        interval = _find_current_interval(
            conn, record["start_reading_id"], record["end_reading_id"])
        if interval is None:
            raise StaleAnomalyError(
                "读数已因补传发生变化，该区间不再由当前相邻读数构成")
        if interval.avg_kwh_per_hour <= float(latest["max_kwh_per_hour"]):
            raise StaleAnomalyError(
                "按现行阈值，该区间已不再超限，无需核实")

        if record["status"] == "verified":
            conn.commit()
            return {"record_id": record_id, "already_verified": True,
                    "verified_at": record["verified_at"]}

        conn.execute(
            "UPDATE power_anomalies SET status = 'verified',"
            " verified_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),"
            " verified_note = ? WHERE id = ?",
            (note.strip()[:500], record_id))

        if fail_before_commit:
            # 自测用故障注入：验证核实状态与事务一起回滚
            raise RuntimeError("故障注入：模拟核实提交前失败")

        conn.commit()
        updated = conn.execute(
            "SELECT verified_at FROM power_anomalies WHERE id = ?",
            (record_id,)).fetchone()
        return {"record_id": record_id, "already_verified": False,
                "verified_at": updated["verified_at"]}
    except Exception:
        conn.rollback()
        raise


def _find_current_interval(conn: sqlite3.Connection, start_id: int,
                           end_id: int) -> PowerInterval | None:
    for iv in compute_power_intervals(_load_reading_points(conn)):
        if iv.start.id == start_id and iv.end.id == end_id:
            return iv
    return None


# ---------------------------------------------------------------- 页面视图

def build_overview(conn: sqlite3.Connection) -> dict:
    """读取当前阈值下的待办、当前已核实与历史留档。

    当前性不是依赖某个可漂移的“激活”标志，而是每次都用**当前已提交读数**
    重新计算相邻区间，并用**现行阈值版本**逐条匹配记录。这样即使补传、
    改阈值和核实交错，旧结论也不会被显示成当前结论。

    多条 SELECT 包在一个只读事务里，保证阈值、读数、记录来自同一快照。
    """
    own_txn = not conn.in_transaction
    if own_txn:
        conn.execute("BEGIN")
    try:
        threshold = current_threshold_version(conn)
        intervals = compute_power_intervals(_load_reading_points(conn))
        current_pairs = {(iv.start.id, iv.end.id): iv for iv in intervals}

        pending: list[dict] = []
        verified_current: list[dict] = []
        historical: list[dict] = []

        records = conn.execute(
            "SELECT * FROM power_anomalies ORDER BY id DESC").fetchall()
        for rec in records:
            item = dict(rec)
            pair = (rec["start_reading_id"], rec["end_reading_id"])
            iv = current_pairs.get(pair)
            reason = _stale_reason(rec, threshold, iv)
            if reason is not None:
                item["stale_reason"] = reason
                historical.append(item)
            elif rec["status"] == "verified":
                verified_current.append(item)
            else:
                pending.append(item)

        pending.sort(key=lambda r: (-r["avg_kwh_per_hour"], r["id"]))
        verified_current.sort(key=lambda r: (-r["avg_kwh_per_hour"], r["id"]))

        if threshold is not None:
            limit = float(threshold["max_kwh_per_hour"])
            expected = [iv for iv in intervals if iv.avg_kwh_per_hour > limit]
        else:
            expected = []
        recorded_pairs = {
            (r["start_reading_id"], r["end_reading_id"])
            for r in pending + verified_current
        }

        return {
            "threshold": threshold,
            "pending": pending,
            "verified_current": verified_current,
            "historical": historical,
            "current_over_limit": len(expected),
            "missing_records": sum(
                1 for iv in expected
                if (iv.start.id, iv.end.id) not in recorded_pairs),
            "reading_count": conn.execute(
                "SELECT COUNT(*) c FROM readings").fetchone()["c"],
        }
    finally:
        if own_txn:
            conn.rollback()  # 只读事务，回滚即释放快照


def _stale_reason(rec, threshold, iv: PowerInterval | None) -> str | None:
    """返回记录为何不再是“当前待办”的原因；None 表示仍是当前记录。"""
    if threshold is None:
        return "尚未设置现行阈值"
    if rec["threshold_version_id"] != threshold["id"]:
        old = float(rec["threshold_kwh_per_hour"])
        new = float(threshold["max_kwh_per_hour"])
        if old == new:
            return ("阈值版本已更新（数值同为 "
                    f"{new:g} kWh/h），该记录属旧版本，需按现行版本重新形成")
        return (f"阈值已变更：记录按 {old:g} kWh/h 形成，"
                f"现行阈值为 {new:g} kWh/h")
    if iv is None:
        return "读数补传后该两端读数已不再相邻，区间已重新形成"
    if iv.avg_kwh_per_hour <= float(threshold["max_kwh_per_hour"]):
        return "按现行阈值该区间已不超限"
    return None
