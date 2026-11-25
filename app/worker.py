"""异步核查 worker 与片段版本（generation）协调。

规则
====
* 每起事故至多一份“现行结果”（results 中 status=active 的行）。
* 计算期间允许补传。补传的片段若与某事故核查选段相交，则：
    - 该事故现行结果立即撤下（withdrawn）；
    - 在途 / 待处理核查所依据的 generation 作废，需要按新片段重算；
  仅落在选段外的更新不撤结果、不影响在途核查。
* worker 串行处理任务。任务开工前读取事故最新 generation，若任务自身的
  generation 已落后则标记 stale 并跳过；同一事故的待处理旧任务会被去重，
  始终只保留针对最新 generation 的一个有效任务。
* 两侧均无解，或结果写入数据库失败时，保留原状（不发布、不破坏既有数据）。
"""

from __future__ import annotations

import json
import logging
import queue
import sqlite3
import threading
from typing import Optional

from . import database as db
from .solver import solve

logger = logging.getLogger("wavecheck.worker")


class JobCoordinator:
    """串行化所有数据库写操作，并驱动后台核查线程。"""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.lock = threading.RLock()
        self._wakeup = queue.Queue[int]()
        self._stop = threading.Event()
        # 测试钩子：任务开工、generation 校验之前调用，便于制造在途窗口。
        self.pre_solve_hook = None
        self.thread = threading.Thread(
            target=self._run, name="verify-worker", daemon=True
        )

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._wakeup.put(-1)
        self.thread.join(timeout=5)

    def notify(self) -> None:
        self._wakeup.put(1)

    # ---- 片段补传 --------------------------------------------------------
    def submit_patches(
        self,
        accident_id: str,
        channel_id: str,
        patches: list[dict],
    ) -> dict:
        """写入片段（同号替换），并在涉及选段时撤下现行结果。

        返回受理信息，包括是否影响了现行结果与新的 generation。
        """
        with self.lock, db.transaction(self.conn):
            existing = {
                p["pid"] for p in db.fetch_patches(
                    self.conn, accident_id, channel_id
                )
            }
            replaced = sorted(
                p["pid"] for p in patches if p["pid"] in existing
            )
            for p in patches:
                db.upsert_patch(
                    self.conn,
                    accident_id,
                    channel_id,
                    p["pid"],
                    p["start_sample"],
                    p["readings"],
                )
            distinct = db.count_distinct_pids(
                self.conn, accident_id, channel_id
            )

            active = db.get_active_result(self.conn, accident_id)
            affected = False
            if active is not None:
                lo, hi = active["lo"], active["hi"]
                if any(_intersects(p, lo, hi) for p in patches):
                    db.withdraw_active_result(self.conn, accident_id)
                    affected = True

            generation = db.next_generation(self.conn, accident_id)
            # 让该事故所有在途 / 待处理任务立即过期。
            self._expire_pending_jobs(accident_id, generation)
            return {
                "accident_id": accident_id,
                "channel_id": channel_id,
                "accepted": len(patches),
                "replaced_pids": replaced,
                "distinct_pid_count": distinct,
                "affected_active_result": affected,
                "generation": generation,
            }

    # ---- 提交核查 --------------------------------------------------------
    def submit_verification(
        self,
        accident_id: str,
        channel_id: str,
        lo: int,
        hi: int,
        threshold: int,
    ) -> int:
        with self.lock, db.transaction(self.conn):
            generation = db.next_generation(self.conn, accident_id)
            # 同一事故已有待处理 / 运行中的任务：先作废，再以新 generation 重开。
            self._expire_pending_jobs(accident_id, generation)
            job_id = db.create_job(
                self.conn, accident_id, channel_id,
                lo, hi, threshold, generation,
            )
        self.notify()
        return job_id

    def _expire_pending_jobs(
        self, accident_id: str, generation: int
    ) -> None:
        rows = self.conn.execute(
            "SELECT id, status FROM jobs WHERE accident_id=? "
            "AND status IN ('pending','running')",
            (accident_id,),
        ).fetchall()
        for r in rows:
            self.conn.execute(
                "UPDATE jobs SET status='stale', updated_at=datetime('now') "
                "WHERE id=?",
                (r["id"],),
            )

    # ---- 后台执行 --------------------------------------------------------
    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._wakeup.get(timeout=0.5)
            except queue.Empty:
                pass
            if self._stop.is_set():
                return
            try:
                self._process_next()
            except Exception:  # pragma: no cover - 防御性日志
                logger.exception("worker loop error")

    def _process_next(self) -> None:
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM jobs WHERE status='pending' ORDER BY id LIMIT 1"
            ).fetchone()
            if row is None:
                return
            job_id = row["id"]
            db.mark_job_status(self.conn, job_id, "running")
            self.conn.commit()

        try:
            self._execute_job(job_id)
        except Exception as exc:  # 计算异常：任务失败，不动任何现行结果
            logger.exception("job %s failed", job_id)
            with self.lock, db.transaction(self.conn):
                db.mark_job_status(self.conn, job_id, "failed", str(exc))

    def _execute_job(self, job_id: int) -> None:
        if self.pre_solve_hook is not None:
            self.pre_solve_hook(job_id)
        with self.lock:
            job = db.get_job(self.conn, job_id)
        if job is None:
            return

        accident_id = job["accident_id"]
        with self.lock, db.transaction(self.conn):
            latest_gen = self._latest_generation(accident_id)
            if latest_gen != job["generation"]:
                # 在途期间发生了涉及选段的补传：本任务依据的版本已过期。
                db.mark_job_status(self.conn, job_id, "stale")
                return

            patches = db.fetch_patches(
                self.conn, accident_id, job["channel_id"]
            )
            result = solve(
                patches, job["lo"], job["hi"], job["threshold"]
            )

            # 两侧均无解：保留原状，任务标记 done 但不发布结果。
            if (
                not result.over_limit.solvable
                and not result.within_limit.solvable
            ):
                db.mark_job_status(self.conn, job_id, "done")
                return

            payload = {
                "accident_id": accident_id,
                "channel_id": job["channel_id"],
                "lo": job["lo"],
                "hi": job["hi"],
                "threshold": job["threshold"],
                "generation": job["generation"],
                "considered_pids": result.considered_pids,
                "uncovered_points": result.uncovered_points,
                "over_limit": result.over_limit.to_dict(),
                "within_limit": result.within_limit.to_dict(),
            }
            # publish_result 若抛 sqlite3.Error，异常会穿出 transaction 上下文
            # 触发回滚（保留原状），再由 _process_next 把任务标记为 failed。
            db.publish_result(
                self.conn,
                accident_id,
                job["channel_id"],
                job["lo"],
                job["hi"],
                job["threshold"],
                job["generation"],
                payload,
            )
            db.mark_job_status(self.conn, job_id, "done")

    def _latest_generation(self, accident_id: str) -> int:
        return db.current_generation(self.conn, accident_id)

    # ---- 查询 ------------------------------------------------------------
    def get_job_view(self, job_id: int) -> Optional[dict]:
        with self.lock:
            job = db.get_job(self.conn, job_id)
            if job is None:
                return None
            view = {
                "job_id": job["id"],
                "accident_id": job["accident_id"],
                "channel_id": job["channel_id"],
                "lo": job["lo"],
                "hi": job["hi"],
                "threshold": job["threshold"],
                "generation": job["generation"],
                "status": job["status"],
                "error": job["error"],
                "result": None,
            }
            if job["status"] == "done":
                active = db.get_active_result(
                    self.conn, job["accident_id"]
                )
                if (
                    active is not None
                    and active["generation"] == job["generation"]
                ):
                    view["result"] = json.loads(active["payload"])
            return view

    def get_active_view(self, accident_id: str) -> Optional[dict]:
        with self.lock:
            row = db.get_active_result(self.conn, accident_id)
            return None if row is None else json.loads(row["payload"])


def _intersects(patch: dict, lo: int, hi: int) -> bool:
    start = patch["start_sample"]
    end = start + len(patch["readings"]) - 1
    return not (end < lo or start > hi)
