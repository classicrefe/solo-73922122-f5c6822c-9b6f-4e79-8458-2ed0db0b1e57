"""异步核查 worker 与片段版本（generation）协调。

规则
====
* 每起事故至多一份“现行结果”（results 中 status=active 的行）。
* 计算期间允许补传：慢求解在协调锁之外进行，补传请求不会被求解阻塞。
* 补传是否影响某个目标（现行结论 / pending / running 任务），由目标
  **自己的通道与样点闭区间 [lo, hi]** 决定：
    - 通道必须相同；
    - 本批次任一片段的覆盖闭区间与 [lo, hi] 相交。
  同号替换必须同时考察该片的**旧覆盖范围与新覆盖范围**：旧范围曾覆盖
  选段而新范围移出时，旧结论同样失效；新片号只看新范围。
* 补传事务成功提交时，同步撤下受影响的现行结论（withdrawn），并把受影响
  的 pending/running 任务标记为 stale。未受影响的结论继续供数，未受影响
  的任务继续执行并可发布。
* 发布栅栏：worker 在锁外求解得到结果后，于同一把锁、同一个数据库事务内
  复查任务状态；任务若已被补传或新核查置为 stale，则旧计算结果一律不发布。
* 同一事故提交新的核查时，仍事故级作废所有在途 / 待处理旧任务（不限通道）。
* 片段写入、上限校验、撤结论、任务失效、generation 推进在同一事务内；
  任一步失败整体回滚，片段 / 现行结论 / 任务状态保持补传前原状。
* 两侧均无解，或结果写入数据库失败时，不发布新结果（发布失败保留原状）。
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


class PatchLimitExceeded(Exception):
    """补传后不同片号总数超过每通道上限。"""

    def __init__(self, limit: int, existing_count: int, new_count: int) -> None:
        self.limit = limit
        self.existing_count = existing_count
        self.new_count = new_count
        super().__init__(
            f"同一事故同一通道至多 {limit} 个不同片号，"
            f"当前已有 {existing_count} 个，本次新增 {new_count} 个"
        )


class JobCoordinator:
    """串行化所有数据库写操作，并驱动后台核查线程。

    数据库临界区（``self.lock``）内只做快速读写；求解器 ``solve`` 在锁外
    运行，避免慢计算阻塞补传 / 查询 / 提交核查。
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self.lock = threading.RLock()
        self._wakeup = queue.Queue[int]()
        self._stop = threading.Event()
        # 测试钩子：任务开工、状态 / 版本校验之前调用，便于制造在途窗口。
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
        max_pids: Optional[int] = None,
    ) -> dict:
        """写入片段（同号替换），并按通道 + 闭区间精确处理失效。

        全部副作用在同一个数据库事务内完成；任何异常都会回滚，返回受理
        信息（是否影响现行结果、新 generation 等）。
        """
        with self.lock, db.transaction(self.conn):
            old_patches = db.fetch_patches(
                self.conn, accident_id, channel_id
            )
            old_by_pid = {p["pid"]: p for p in old_patches}
            existing = set(old_by_pid)

            incoming_pids = {p["pid"] for p in patches}
            new_pids = incoming_pids - existing
            # 上限按“不同片号总数”计；同号替换不新增片号。
            if (
                max_pids is not None
                and len(existing) + len(new_pids) > max_pids
            ):
                raise PatchLimitExceeded(
                    max_pids, len(existing), len(new_pids)
                )

            replaced = sorted(
                pid for pid in incoming_pids if pid in existing
            )

            # 每个批次条目的 (新覆盖窗, 旧覆盖窗|None)。同号替换时旧窗
            # 来自数据库中的替换前版本；新片号没有旧窗。
            changes: list[tuple[tuple[int, int], Optional[tuple[int, int]]]] = []
            for p in patches:
                new_window = _coverage_window(
                    p["start_sample"], len(p["readings"])
                )
                old = old_by_pid.get(p["pid"])
                old_window = (
                    _coverage_window(old["start"], len(old["readings"]))
                    if old is not None
                    else None
                )
                changes.append((new_window, old_window))

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

            # 现行结论按“它自己的”通道与选段判断是否受影响。
            active = db.get_active_result(self.conn, accident_id)
            affected_active = False
            if (
                active is not None
                and active["channel_id"] == channel_id
                and _changes_hit_target(changes, active["lo"], active["hi"])
            ):
                db.withdraw_active_result(self.conn, accident_id)
                affected_active = True

            # 在途 / 待处理任务逐个按其自身通道 + 闭区间判定，只作废受
            # 影响者；同一事故其他通道 / 不相交选段的任务继续有效。
            # 放在 generation 推进之前：若后续步骤失败，任务状态随事务
            # 一并回滚（回滚测试可覆盖“已标 stale 后再失败”的情形）。
            stale_job_ids = self._expire_affected_jobs(
                accident_id, channel_id, changes
            )

            generation = db.next_generation(self.conn, accident_id)

            return {
                "accident_id": accident_id,
                "channel_id": channel_id,
                "accepted": len(patches),
                "replaced_pids": replaced,
                "distinct_pid_count": distinct,
                "affected_active_result": affected_active,
                "stale_job_ids": stale_job_ids,
                "generation": generation,
            }

    def list_patches(
        self, accident_id: str, channel_id: str
    ) -> list[dict]:
        """加锁读取通道当前片段，避免与写入并发使用同一连接。"""
        with self.lock:
            return db.fetch_patches(self.conn, accident_id, channel_id)

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
            # 同一事故已有待处理 / 运行中的任务：先全部作废，再以新
            # generation 重开（新核查事故级生效，不限通道 / 选段）。
            self._expire_pending_jobs(accident_id, generation)
            job_id = db.create_job(
                self.conn, accident_id, channel_id,
                lo, hi, threshold, generation,
            )
        self.notify()
        return job_id

    def _expire_pending_jobs(
        self, accident_id: str, generation: int
    ) -> list[int]:
        """事故级作废所有 pending/running 任务（提交新核查时使用）。"""
        rows = self.conn.execute(
            "SELECT id FROM jobs WHERE accident_id=? "
            "AND status IN ('pending','running')",
            (accident_id,),
        ).fetchall()
        ids = [int(r["id"]) for r in rows]
        for job_id in ids:
            self.conn.execute(
                "UPDATE jobs SET status='stale', updated_at=datetime('now') "
                "WHERE id=?",
                (job_id,),
            )
        return ids

    def _expire_affected_jobs(
        self,
        accident_id: str,
        channel_id: str,
        changes: list[
            tuple[tuple[int, int], Optional[tuple[int, int]]]
        ],
    ) -> list[int]:
        """只作废同通道且自身闭区间被任一片的新 / 旧覆盖波及的任务。"""
        rows = self.conn.execute(
            "SELECT id, channel_id, lo, hi FROM jobs "
            "WHERE accident_id=? AND status IN ('pending','running')",
            (accident_id,),
        ).fetchall()
        stale_ids: list[int] = []
        for r in rows:
            if r["channel_id"] != channel_id:
                continue
            if not _changes_hit_target(changes, r["lo"], r["hi"]):
                continue
            self.conn.execute(
                "UPDATE jobs SET status='stale', updated_at=datetime('now') "
                "WHERE id=?",
                (r["id"],),
            )
            stale_ids.append(int(r["id"]))
        return stale_ids

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
        except Exception as exc:  # 计算 / 发布异常：任务失败，不动现行结果
            logger.exception("job %s failed", job_id)
            with self.lock, db.transaction(self.conn):
                current = db.get_job(self.conn, job_id)
                # 若异常传播期间任务已被补传 / 新核查置 stale，保留 stale，
                # 不用 failed 覆盖。
                if current is not None and current["status"] == "running":
                    db.mark_job_status(
                        self.conn, job_id, "failed", str(exc)
                    )

    def _execute_job(self, job_id: int) -> None:
        if self.pre_solve_hook is not None:
            self.pre_solve_hook(job_id)

        # 1) 状态确认与片段快照放在同一临界区：要么看到补传前的 running
        #    状态与旧片段，要么看到补传已把本任务置 stale。
        with self.lock:
            job = db.get_job(self.conn, job_id)
            if job is None or job["status"] != "running":
                return
            accident_id = job["accident_id"]
            channel_id = job["channel_id"]
            lo, hi = job["lo"], job["hi"]
            threshold = job["threshold"]
            generation = job["generation"]
            patches = db.fetch_patches(
                self.conn, accident_id, channel_id
            )

        # 2) 求解在锁外进行：期间补传 / 查询 / 提交核查都能立即拿到锁。
        result = solve(patches, lo, hi, threshold)

        # 3) 发布栅栏：状态复查与发布在同一事务。补传若已提交，本任务
        #    必然已是 stale，于是旧计算结果不会发布、也不会覆盖 stale。
        with self.lock, db.transaction(self.conn):
            current = db.get_job(self.conn, job_id)
            if current is None or current["status"] != "running":
                return

            # 两侧均无解：保留原状，任务标记 done 但不发布结果。
            if (
                not result.over_limit.solvable
                and not result.within_limit.solvable
            ):
                db.mark_job_status(self.conn, job_id, "done")
                return

            payload = {
                "accident_id": accident_id,
                "channel_id": channel_id,
                "lo": lo,
                "hi": hi,
                "threshold": threshold,
                "generation": generation,
                "considered_pids": result.considered_pids,
                "uncovered_points": result.uncovered_points,
                "over_limit": result.over_limit.to_dict(),
                "within_limit": result.within_limit.to_dict(),
            }
            # publish_result 若抛 sqlite3.Error，异常穿出 transaction 上下文
            # 触发回滚（保留原状），再由 _process_next 把任务标记为 failed。
            db.publish_result(
                self.conn,
                accident_id,
                channel_id,
                lo,
                hi,
                threshold,
                generation,
                payload,
            )
            db.mark_job_status(self.conn, job_id, "done")

    def _latest_generation(self, accident_id: str) -> int:
        # 保留为兼容辅助；发布栅栏以 jobs.status 为准，不再用全局
        # generation 相等性误杀未受影响的任务。
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


def _coverage_window(
    start_sample: int, readings_len: int
) -> tuple[int, int]:
    """片段覆盖的样点闭区间 [start, end]。"""
    return start_sample, start_sample + readings_len - 1


def _window_intersects_target(
    window: tuple[int, int], lo: int, hi: int
) -> bool:
    """两个闭区间是否相交（端点相接也算）。"""
    start, end = window
    return not (end < lo or start > hi)


def _changes_hit_target(
    changes: list[
        tuple[tuple[int, int], Optional[tuple[int, int]]]
    ],
    lo: int,
    hi: int,
) -> bool:
    """任一片段的新覆盖窗或（同号替换的）旧覆盖窗与 [lo, hi] 相交。"""
    for new_window, old_window in changes:
        if _window_intersects_target(new_window, lo, hi):
            return True
        if (
            old_window is not None
            and _window_intersects_target(old_window, lo, hi)
        ):
            return True
    return False


def _intersects(patch: dict, lo: int, hi: int) -> bool:
    """兼容保留：判断传入片段（start_sample/readings）是否与选段相交。"""
    start = patch["start_sample"]
    end = start + len(patch["readings"]) - 1
    return _window_intersects_target((start, end), lo, hi)
