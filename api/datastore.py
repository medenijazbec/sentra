"""
datastore.py
------------

Thin wrapper around MySQL for sentra.

We store short-timescale samples of:
- cpu_samples
- mem_samples
- gpu_samples
- disk_samples
- net_samples
- fan_samples

We expose:
    init_db_if_needed()
    insert_snapshot(snapshot, gpus)
    get_cpu_mem_history(minutes=60)
    get_gpu_history(minutes=60)
    purge_before(cutoff_ts)
"""

import json
import time
from typing import Any, Dict, List

import pandas as pd
import mysql.connector
from mysql.connector import errorcode

from config import config

_initialized = False


def _conn() -> mysql.connector.connection.MySQLConnection:
    return mysql.connector.connect(**config.get_db_config())


def init_db_if_needed() -> None:
    """Create tables if they don't already exist."""
    global _initialized
    if _initialized:
        return

    conn = _conn()
    cursor = conn.cursor()
    try:
        for sql in (
            """
            CREATE TABLE IF NOT EXISTS cpu_samples (
                ts BIGINT NOT NULL,
                total_util DOUBLE,
                iowait DOUBLE,
                per_core TEXT,
                cpu_temp DOUBLE,
                load1 DOUBLE,
                load5 DOUBLE,
                load15 DOUBLE,
                uptime_sec DOUBLE,
                user_pct DOUBLE,
                system_pct DOUBLE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS mem_samples (
                ts BIGINT NOT NULL,
                used_percent DOUBLE,
                used_bytes BIGINT,
                total_bytes BIGINT,
                swap_used_percent DOUBLE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS gpu_samples (
                ts BIGINT NOT NULL,
                gpu_index INT,
                temp DOUBLE,
                util DOUBLE,
                power_w DOUBLE,
                vram_used_mb INT,
                vram_total_mb INT,
                fan_percent DOUBLE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS disk_samples (
                ts BIGINT NOT NULL,
                device TEXT,
                read_bps DOUBLE,
                write_bps DOUBLE,
                usage_percent DOUBLE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS net_samples (
                ts BIGINT NOT NULL,
                iface TEXT,
                rx_bps DOUBLE,
                tx_bps DOUBLE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS fan_samples (
                ts BIGINT NOT NULL,
                label TEXT,
                rpm DOUBLE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS dashboard_settings (
                `key` VARCHAR(128) PRIMARY KEY,
                `value` TEXT NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS gpu_labels (
                gpu_index INT PRIMARY KEY,
                label TEXT NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS gpu_visibility (
                gpu_index INT PRIMARY KEY,
                hidden TINYINT NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
        ):
            cursor.execute(sql)

        for sql in (
            "CREATE INDEX idx_cpu_ts ON cpu_samples(ts)",
            "CREATE INDEX idx_mem_ts ON mem_samples(ts)",
            "CREATE INDEX idx_gpu_ts ON gpu_samples(ts)",
            "CREATE INDEX idx_disk_ts ON disk_samples(ts)",
            "CREATE INDEX idx_net_ts ON net_samples(ts)",
            "CREATE INDEX idx_fan_ts ON fan_samples(ts)",
        ):
            try:
                cursor.execute(sql)
            except mysql.connector.Error as exc:
                # 1061: Duplicate key name '...'
                if exc.errno == 1061:
                    continue
                raise

        def _ensure_column(table: str, column: str, definition: str) -> None:
            try:
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
            except mysql.connector.Error as exc:
                if exc.errno == errorcode.ER_DUP_FIELDNAME:
                    return
                raise

        _ensure_column("cpu_samples", "user_pct", "DOUBLE")
        _ensure_column("cpu_samples", "system_pct", "DOUBLE")

        conn.commit()
        _initialized = True
    finally:
        cursor.close()
        conn.close()


def insert_snapshot(snapshot: Dict[str, Any], gpus: List[Dict[str, Any]]) -> None:
    """
    Take snapshot dict from system_collector + list of GPU dicts from gpu_collector,
    and append them to MySQL.
    """
    init_db_if_needed()

    conn = _conn()
    cursor = conn.cursor()
    try:
        ts = int(snapshot["ts"])

        cpu = snapshot["cpu"]
        meta = snapshot["meta"]
        breakdown = cpu.get("breakdown", {})
        cursor.execute(
            """
            INSERT INTO cpu_samples
            (ts,total_util,iowait,per_core,cpu_temp,load1,load5,load15,uptime_sec,user_pct,system_pct)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                ts,
                cpu["total_util"],
                cpu["iowait"],
                json.dumps(cpu["per_core"]),
                cpu["temp"],
                cpu["load"]["1m"],
                cpu["load"]["5m"],
                cpu["load"]["15m"],
                meta["uptime_sec"],
                breakdown.get("user"),
                breakdown.get("system"),
            ),
        )

        mem = snapshot["mem"]
        cursor.execute(
            """
            INSERT INTO mem_samples
            (ts,used_percent,used_bytes,total_bytes,swap_used_percent)
            VALUES (%s,%s,%s,%s,%s)
            """,
            (
                ts,
                mem["used_percent"],
                mem["used_bytes"],
                mem["total_bytes"],
                mem["swap_used_percent"],
            ),
        )

        for g in gpus:
            if "error" in g:
                continue
            cursor.execute(
                """
                INSERT INTO gpu_samples
                (ts,gpu_index,temp,util,power_w,vram_used_mb,vram_total_mb,fan_percent)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    ts,
                    g["index"],
                    g["temp"],
                    g["util"],
                    g["power_w"],
                    g["vram_used_mb"],
                    g["vram_total_mb"],
                    g["fan_percent"],
                ),
            )

        disk_data = snapshot["disk"]
        usage_percent = disk_data["root_usage_percent"]
        for dev, vals in disk_data["throughput"].items():
            cursor.execute(
                """
                INSERT INTO disk_samples
                (ts,device,read_bps,write_bps,usage_percent)
                VALUES (%s,%s,%s,%s,%s)
                """,
                (
                    ts,
                    dev,
                    vals["read_bps"],
                    vals["write_bps"],
                    usage_percent,
                ),
            )

        for iface, vals in snapshot["net"]["throughput"].items():
            cursor.execute(
                """
                INSERT INTO net_samples
                (ts,iface,rx_bps,tx_bps)
                VALUES (%s,%s,%s,%s)
                """,
                (
                    ts,
                    iface,
                    vals["rx_bps"],
                    vals["tx_bps"],
                ),
            )

        for label, rpm in snapshot["fans"].items():
            cursor.execute(
                """
                INSERT INTO fan_samples
                (ts,label,rpm)
                VALUES (%s,%s,%s)
                """,
                (
                    ts,
                    label,
                    rpm,
                ),
            )

        conn.commit()
    finally:
        cursor.close()
        conn.close()


def get_cpu_mem_history(minutes: int = 60) -> pd.DataFrame:
    """
    Return CPU+memory+swap history for the last `minutes` minutes as one DataFrame.
    Columns:
        ts, total_util, cpu_temp, used_percent, swap_used_percent, timestamp
    """
    init_db_if_needed()

    since = int(time.time() - minutes * 60)
    conn = _conn()
    try:
        df_cpu = pd.read_sql_query(
            """
            SELECT ts, total_util, cpu_temp
            FROM cpu_samples
            WHERE ts >= %s
            ORDER BY ts ASC
            """,
            conn,
            params=(since,),
        )

        df_mem = pd.read_sql_query(
            """
            SELECT ts, used_percent, swap_used_percent
            FROM mem_samples
            WHERE ts >= %s
            ORDER BY ts ASC
            """,
            conn,
            params=(since,),
        )
    finally:
        conn.close()

    if df_cpu.empty and df_mem.empty:
        return pd.DataFrame()

    df = pd.merge(df_cpu, df_mem, on="ts", how="outer").sort_values("ts")
    df["timestamp"] = pd.to_datetime(df["ts"], unit="s")
    return df


def get_gpu_history(minutes: int = 60) -> pd.DataFrame:
    """
    Return GPU telemetry for the last `minutes` minutes as a pandas DataFrame
    with a timestamp column for plotting.
    """
    init_db_if_needed()

    since = int(time.time() - minutes * 60)
    conn = _conn()
    try:
        df = pd.read_sql_query(
            """
            SELECT ts, gpu_index, temp, util, power_w, vram_used_mb, vram_total_mb, fan_percent
            FROM gpu_samples
            WHERE ts >= %s
            ORDER BY ts ASC
            """,
            conn,
            params=(since,),
        )
    finally:
        conn.close()

    if df.empty:
        return df

    df["timestamp"] = pd.to_datetime(df["ts"], unit="s")
    return df


def purge_before(cutoff_ts: float) -> None:
    """
    Delete all rows older than cutoff_ts from every table.
    """
    init_db_if_needed()

    conn = _conn()
    cursor = conn.cursor()
    try:
        cutoff = int(cutoff_ts)
        for tbl in (
            "cpu_samples",
            "mem_samples",
            "gpu_samples",
            "disk_samples",
            "net_samples",
            "fan_samples",
        ):
            cursor.execute(f"DELETE FROM {tbl} WHERE ts < %s", (cutoff,))

        conn.commit()
    finally:
        cursor.close()
        conn.close()
