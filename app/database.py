"""SQLite 数据访问层与建表逻辑。

表结构
======
patches       事故通道下的录波片段，(accident_id, channel_id, pid) 唯一，
              同号片段以新读数整体替换（upsert）。
jobs          核查任务。每起事故至多一份“现行结果”，任务以 generation 标记
              它依据的片段版本；片段补传若涉及该事故选段会令在途结果失效，
              generation 自增，旧任务作废重算。
results       已发布的核查结果（每起事故一行现行结果）。补传涉及选段时先把
              现行结果 withdrawn，再由新任务成功后替换；写入失败或两侧均无解
              时保留原状。
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from typing import Any, Iterator, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS patches (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    accident_id  TEXT NOT NULL,
    channel_id   TEXT NOT NULL,
    pid          INTEGER NOT NULL,
    start_sample INTEGER NOT NULL,
    readings     TEXT NOT NULL,
    created_at   TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(accident_id, channel_id, pid)
);

CREATE TABLE IF NOT EXISTS generations (
    accident_id TEXT PRIMARY KEY,
    generation  INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS jobs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    accident_id      TEXT NOT NULL,
    channel_id       TEXT NOT NULL,
    lo               INTEGER NOT NULL,
    hi               INTEGER NOT NULL,
    threshold        INTEGER NOT NULL,
    generation       INTEGER NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending',
    error            TEXT,
    created_at       TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS results (
    accident_id      TEXT PRIMARY KEY,
    channel_id       TEXT NOT NULL,
    lo               INTEGER NOT NULL,
    hi               INTEGER NOT NULL,
    threshold        INTEGER NOT NULL,
    generation       INTEGER NOT NULL,
    payload          TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'active',
    created_at       TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_jobs_accident ON jobs(accident_id);
CREATE INDEX IF NOT EXISTS idx_patches_lookup
    ON patches(accident_id, channel_id);
"""


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def upsert_patch(
    conn: sqlite3.Connection,
    accident_id: str,
    channel_id: str,
    pid: int,
    start_sample: int,
    readings: list[int],
) -> None:
    conn.execute(
        """
        INSERT INTO patches (accident_id, channel_id, pid, start_sample, readings)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(accident_id, channel_id, pid) DO UPDATE SET
            start_sample=excluded.start_sample,
            readings=excluded.readings,
            created_at=datetime('now')
        """,
        (accident_id, channel_id, pid, start_sample, json.dumps(readings)),
    )


def count_distinct_pids(
    conn: sqlite3.Connection, accident_id: str, channel_id: str
) -> int:
    row = conn.execute(
        "SELECT COUNT(DISTINCT pid) AS c FROM patches "
        "WHERE accident_id=? AND channel_id=?",
        (accident_id, channel_id),
    ).fetchone()
    return int(row["c"])


def fetch_patches(
    conn: sqlite3.Connection, accident_id: str, channel_id: str
) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT pid, start_sample, readings FROM patches "
        "WHERE accident_id=? AND channel_id=? ORDER BY pid",
        (accident_id, channel_id),
    ).fetchall()
    return [
        {
            "pid": r["pid"],
            "start": r["start_sample"],
            "readings": json.loads(r["readings"]),
        }
        for r in rows
    ]


def create_job(
    conn: sqlite3.Connection,
    accident_id: str,
    channel_id: str,
    lo: int,
    hi: int,
    threshold: int,
    generation: int,
) -> int:
    cur = conn.execute(
        "INSERT INTO jobs (accident_id, channel_id, lo, hi, threshold, generation)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        (accident_id, channel_id, lo, hi, threshold, generation),
    )
    return int(cur.lastrowid)


def get_job(conn: sqlite3.Connection, job_id: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM jobs WHERE id=?", (job_id,)
    ).fetchone()


def list_jobs(
    conn: sqlite3.Connection, accident_id: Optional[str] = None
) -> list[sqlite3.Row]:
    if accident_id is None:
        return list(conn.execute("SELECT * FROM jobs ORDER BY id DESC LIMIT 100"))
    return list(
        conn.execute(
            "SELECT * FROM jobs WHERE accident_id=? ORDER BY id DESC LIMIT 100",
            (accident_id,),
        )
    )


def mark_job_status(
    conn: sqlite3.Connection,
    job_id: int,
    status: str,
    error: Optional[str] = None,
) -> None:
    conn.execute(
        "UPDATE jobs SET status=?, error=?, updated_at=datetime('now') WHERE id=?",
        (status, error, job_id),
    )


def next_generation(conn: sqlite3.Connection, accident_id: str) -> int:
    """事故的片段版本号自增并返回新值。

    版本号独立持久化在 generations 表，使“补传导致的版本推进”即使没有
    对应 job 行也能被在途任务观察到。
    """
    conn.execute(
        "INSERT INTO generations (accident_id, generation) VALUES (?, 1) "
        "ON CONFLICT(accident_id) DO UPDATE SET "
        "generation = generation + 1",
        (accident_id,),
    )
    row = conn.execute(
        "SELECT generation FROM generations WHERE accident_id=?",
        (accident_id,),
    ).fetchone()
    return int(row["generation"])


def current_generation(conn: sqlite3.Connection, accident_id: str) -> int:
    row = conn.execute(
        "SELECT generation FROM generations WHERE accident_id=?",
        (accident_id,),
    ).fetchone()
    return 0 if row is None else int(row["generation"])


def withdraw_active_result(
    conn: sqlite3.Connection, accident_id: str
) -> None:
    conn.execute(
        "UPDATE results SET status='withdrawn', updated_at=datetime('now') "
        "WHERE accident_id=? AND status='active'",
        (accident_id,),
    )


def get_active_result(
    conn: sqlite3.Connection, accident_id: str
) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM results WHERE accident_id=? AND status='active'",
        (accident_id,),
    ).fetchone()


def publish_result(
    conn: sqlite3.Connection,
    accident_id: str,
    channel_id: str,
    lo: int,
    hi: int,
    threshold: int,
    generation: int,
    payload: dict[str, Any],
) -> None:
    """发布新现行结果：每起事故仅一行，整行 UPSERT 替换。

    失败由外层事务回滚，保留此前状态（“写入失败保留原状”）。
    """
    conn.execute(
        """
        INSERT INTO results (accident_id, channel_id, lo, hi, threshold,
                             generation, payload, status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'active', datetime('now'), datetime('now'))
        ON CONFLICT(accident_id) DO UPDATE SET
            channel_id=excluded.channel_id,
            lo=excluded.lo,
            hi=excluded.hi,
            threshold=excluded.threshold,
            generation=excluded.generation,
            payload=excluded.payload,
            status='active',
            updated_at=datetime('now')
        """,
        (
            accident_id, channel_id, lo, hi, threshold, generation,
            json.dumps(payload, ensure_ascii=False),
        ),
    )
