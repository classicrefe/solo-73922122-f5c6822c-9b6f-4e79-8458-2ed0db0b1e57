"""补传 × 核查交错语义的回归自测。

覆盖任务说明中的关键规则：
1. 同号替换移出 / 移入选段都按“新旧覆盖窗”判定，闭区间端点相接算相交；
2. 现行结论、pending/running 任务各按自己的通道 + [lo, hi] 判定受影响；
3. 补传在慢求解期间也能立即提交（solve 不持协调锁）；
4. 补传成功同步撤结论、把受影响任务标 stale；旧计算完成后不能发布；
5. 未受影响结论继续供数，未受影响任务仍可发布；
6. 同一事故提交新核查仍事故级作废在途旧任务（含跨通道）；
7. 补传写入中途失败时，片段 / 结论 / 任务状态 / generation 一并回滚。

既可 ``python tests/test_patch_interleave.py`` 直接运行，也可被 pytest 收集。
"""
from __future__ import annotations

import os
import sys
import tempfile
import threading
import time

sys.path.insert(0, "/workspace")

from fastapi.testclient import TestClient  # noqa: E402

from app import database as db, worker as worker_mod  # noqa: E402
from app.main import create_app  # noqa: E402
from app.worker import (  # noqa: E402
    JobCoordinator,
    _changes_hit_target,
)


# ---------------------------------------------------------------- helpers

def _wait_status(client, job_id, targets, timeout=8.0):
    if isinstance(targets, str):
        targets = (targets,)
    deadline = time.time() + timeout
    body = None
    while time.time() < deadline:
        body = client.get(f"/jobs/{job_id}").json()
        if body["status"] in targets:
            return body
        time.sleep(0.02)
    raise AssertionError(f"job {job_id} status {body and body['status']} "
                         f"never reached {targets}")


def _post_in_thread(client, url, payload):
    """从独立线程发 POST，返回 (thread, box)；用于验证请求不被锁阻塞。"""
    box: dict = {}

    def run():
        try:
            box["response"] = client.post(url, json=payload)
        except Exception as exc:  # noqa: BLE001
            box["error"] = exc

    th = threading.Thread(target=run, daemon=True)
    th.start()
    return th, box


def _temp_db():
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    return tmp.name


def _cleanup_db(path):
    for suffix in ("", "-wal", "-shm"):
        try:
            os.unlink(path + suffix)
        except OSError:
            pass


def _install_blocking_solve():
    """把 worker.solve 换成门控版本，返回 (entered, gate, done, restore)。"""
    entered = threading.Event()
    gate = threading.Event()
    done = threading.Event()
    real_solve = worker_mod.solve

    def blocking(patches, lo, hi, threshold):
        entered.set()
        gate.wait(timeout=10)
        try:
            return real_solve(patches, lo, hi, threshold)
        finally:
            done.set()

    worker_mod.solve = blocking

    def restore():
        worker_mod.solve = real_solve

    return entered, gate, done, restore


# ------------------------------------------------------------------ tests

def test_coverage_window_closed_interval_rules():
    new_hit = [((3, 4), None)]
    assert _changes_hit_target(new_hit, 0, 3)          # 端点 3 相接
    assert not _changes_hit_target([((4, 5), None)], 0, 3)
    assert _changes_hit_target([((-2, 0), None)], 0, 3)
    assert not _changes_hit_target([((-2, -1), None)], 0, 3)
    # 同号替换：新窗已移出，但旧窗曾覆盖 -> 仍受影响
    moved_out = [((100, 103), (0, 3))]
    assert _changes_hit_target(moved_out, 0, 3)
    # 旧窗不相交、新窗移入 -> 受影响
    moved_in = [((2, 2), (100, 103))]
    assert _changes_hit_target(moved_in, 0, 3)
    # 新旧窗都不相交 -> 不受影响
    assert not _changes_hit_target([((100, 101), (50, 51))], 0, 3)


def test_replacement_moved_out_withdraws_old_result():
    path = _temp_db()
    app = create_app(db_path=path)
    with TestClient(app) as client:
        base = "/accidents/A1/channels/C1"
        client.post(f"{base}/patches", json={"patches": [
            {"pid": 1, "start_sample": 0, "readings": [6, 6, 1, 1]},
            {"pid": 2, "start_sample": 0, "readings": [1, 1]},
            {"pid": 3, "start_sample": 2, "readings": [1, 1]},
        ]})
        jid = client.post(
            f"{base}/verify", json={"lo": 0, "hi": 3, "threshold": 5}
        ).json()["job_id"]
        _wait_status(client, jid, "done")
        assert client.get("/accidents/A1/result").status_code == 200

        # 同号 p1 整体移到选段外：旧覆盖 [0,3] 曾参与结论，必须撤下。
        r = client.post(f"{base}/patches", json={"patches": [
            {"pid": 1, "start_sample": 100, "readings": [6, 6, 6, 6]},
        ]})
        assert r.status_code == 201, r.text
        ack = r.json()
        assert ack["affected_active_result"] is True
        assert client.get("/accidents/A1/result").status_code == 404
    _cleanup_db(path)


def test_other_channel_patch_does_not_withdraw_result():
    path = _temp_db()
    app = create_app(db_path=path)
    with TestClient(app) as client:
        c1 = "/accidents/A1/channels/C1"
        client.post(f"{c1}/patches", json={"patches": [
            {"pid": 1, "start_sample": 0, "readings": [1, 1, 1, 1]},
        ]})
        jid = client.post(
            f"{c1}/verify", json={"lo": 0, "hi": 3, "threshold": 5}
        ).json()["job_id"]
        _wait_status(client, jid, "done")

        # C2 通道、数值区间恰好重叠，也不能影响 C1 的现行结论。
        r = client.post("/accidents/A1/channels/C2/patches", json={"patches": [
            {"pid": 1, "start_sample": 0, "readings": [9, 9]},
        ]})
        assert r.json()["affected_active_result"] is False
        resp = client.get("/accidents/A1/result")
        assert resp.status_code == 200
        assert resp.json()["channel_id"] == "C1"
    _cleanup_db(path)


def test_affected_result_withdrawn_but_unrelated_job_survives():
    """完整事故场景：C1 结论被撤；同事故选段外的任务不 stale、可发布。"""
    path = _temp_db()
    app = create_app(db_path=path)
    coord = app.state.coordinator
    entered = threading.Event()
    gate = threading.Event()
    gate.set()  # setup 阶段放行；需要在途窗口时再 clear
    coord.pre_solve_hook = lambda _jid: (entered.set(), gate.wait(5))
    try:
        with TestClient(app) as client:
            base = "/accidents/A1/channels/C1"
            client.post(f"{base}/patches", json={"patches": [
                {"pid": 1, "start_sample": 0, "readings": [6, 6, 1, 1]},
                {"pid": 2, "start_sample": 0, "readings": [1, 1]},
                {"pid": 3, "start_sample": 2, "readings": [1, 1]},
            ]})
            jr = client.post(
                f"{base}/verify", json={"lo": 0, "hi": 3, "threshold": 5}
            ).json()["job_id"]
            _wait_status(client, jr, "done")

            # 另一个选段 [100,101] 的任务正在运行。
            client.post(f"{base}/patches", json={"patches": [
                {"pid": 9, "start_sample": 100, "readings": [1, 1]},
            ]})
            gate.clear()
            entered.clear()
            jj = client.post(
                f"{base}/verify", json={"lo": 100, "hi": 101, "threshold": 5}
            ).json()["job_id"]
            assert entered.wait(5)

            # 替换 p1：旧窗 [0,3] 命中现行结论；新旧窗都不沾 [100,101]。
            r = client.post(f"{base}/patches", json={"patches": [
                {"pid": 1, "start_sample": 0, "readings": [1]},
            ]})
            ack = r.json()
            assert ack["affected_active_result"] is True
            assert jj not in ack["stale_job_ids"]
            assert client.get("/accidents/A1/result").status_code == 404

            body = client.get(f"/jobs/{jj}").json()
            assert body["status"] == "running", body  # 没有被误杀

            gate.set()
            body = _wait_status(client, jj, "done")
            assert body["result"] is not None  # 未受影响任务仍可发布
    finally:
        coord.pre_solve_hook = None
    _cleanup_db(path)


def test_patch_submittable_during_solve_and_stale_compute_not_published():
    path = _temp_db()
    app = create_app(db_path=path)
    entered = gate = done = restore = None
    try:
        with TestClient(app) as client:
            base = "/accidents/A1/channels/C1"
            client.post(f"{base}/patches", json={"patches": [
                {"pid": 1, "start_sample": 0, "readings": [6, 6, 1, 1]},
                {"pid": 2, "start_sample": 0, "readings": [1, 1]},
                {"pid": 3, "start_sample": 2, "readings": [1, 1]},
            ]})
            j0 = client.post(
                f"{base}/verify", json={"lo": 0, "hi": 3, "threshold": 5}
            ).json()["job_id"]
            _wait_status(client, j0, "done")

            # setup 结论发布后再装门控，只阻塞 j1 的求解。
            entered, gate, done, restore = _install_blocking_solve()
            j1 = client.post(
                f"{base}/verify", json={"lo": 0, "hi": 3, "threshold": 5}
            ).json()["job_id"]
            assert entered.wait(5)

            # 慢求解进行中：补传请求必须立即被受理。
            th, box = _post_in_thread(client, f"{base}/patches", {
                "patches": [
                    {"pid": 1, "start_sample": 100,
                     "readings": [6, 6, 6, 6]},
                ]
            })
            th.join(timeout=5)
            assert not th.is_alive(), "补传在求解期间被阻塞"
            assert "response" in box, box.get("error")
            resp = box["response"]
            assert resp.status_code == 201, resp.text
            ack = resp.json()
            assert ack["affected_active_result"] is True
            assert j1 in ack["stale_job_ids"]

            # 求解仍被门控挂着时，任务已经是 stale。
            body = client.get(f"/jobs/{j1}").json()
            assert body["status"] == "stale", body
            assert client.get("/accidents/A1/result").status_code == 404

            # 放行旧计算：它会“算完”，但发布栅栏必须拦住它。
            gate.set()
            assert done.wait(5)
            time.sleep(0.15)
            body = client.get(f"/jobs/{j1}").json()
            assert body["status"] == "stale"
            assert body["result"] is None
            assert client.get("/accidents/A1/result").status_code == 404
    finally:
        if restore is not None:
            restore()
    _cleanup_db(path)


def test_unaffected_patch_during_solve_keeps_job_publishable():
    path = _temp_db()
    app = create_app(db_path=path)
    entered, gate, done, restore = _install_blocking_solve()
    try:
        with TestClient(app) as client:
            c2 = "/accidents/A2/channels/C2"
            client.post(f"{c2}/patches", json={"patches": [
                {"pid": 1, "start_sample": 0, "readings": [1, 1, 1, 1]},
            ]})
            jid = client.post(
                f"{c2}/verify", json={"lo": 0, "hi": 3, "threshold": 5}
            ).json()["job_id"]
            assert entered.wait(5)

            # 别的通道补传：立即受理，不 stale 本任务。
            th, box = _post_in_thread(
                client, "/accidents/A2/channels/C3/patches",
                {"patches": [{"pid": 1, "start_sample": 0,
                              "readings": [7, 7]}]},
            )
            th.join(timeout=5)
            assert not th.is_alive(), "跨通道补传被求解阻塞"
            assert box["response"].status_code == 201
            assert box["response"].json()["stale_job_ids"] == []

            # 同通道但完全在选段外的补传也不影响。
            r = client.post(f"{c2}/patches", json={"patches": [
                {"pid": 50, "start_sample": 1000, "readings": [9]},
            ]})
            assert r.json()["stale_job_ids"] == []
            assert client.get(f"/jobs/{jid}").json()["status"] == "running"

            gate.set()
            assert done.wait(5)
            body = _wait_status(client, jid, "done")
            assert body["result"] is not None
            assert client.get("/accidents/A2/result").status_code == 200
    finally:
        restore()
    _cleanup_db(path)


def test_new_verify_still_expires_inflight_job_accident_wide():
    path = _temp_db()
    app = create_app(db_path=path)
    coord = app.state.coordinator
    entered = threading.Event()
    gate = threading.Event()
    coord.pre_solve_hook = lambda _jid: (entered.set(), gate.wait(5))
    try:
        with TestClient(app) as client:
            c2 = "/accidents/A3/channels/C2"
            client.post(f"{c2}/patches", json={"patches": [
                {"pid": 1, "start_sample": 0, "readings": [1, 1, 1, 1]},
            ]})
            old = client.post(
                f"{c2}/verify", json={"lo": 0, "hi": 3, "threshold": 5}
            ).json()["job_id"]
            assert entered.wait(5)

            # 同事故、不同通道提交新核查：旧在途任务仍应事故级失效。
            new = client.post(
                "/accidents/A3/channels/C9/verify",
                json={"lo": 0, "hi": 1, "threshold": 5},
            ).json()["job_id"]
            assert client.get(f"/jobs/{old}").json()["status"] == "stale"

            gate.set()  # 放行两个任务的钩子（set 后不再阻塞）
            body = _wait_status(client, new, ("done", "failed"))
            assert body["status"] == "done"
    finally:
        coord.pre_solve_hook = None
    _cleanup_db(path)


def test_patch_failure_rolls_back_everything():
    path = _temp_db()
    conn = db.connect(path)
    db.init_db(conn)
    coord = JobCoordinator(conn)  # 不起 worker，纯事务测试

    db.upsert_patch(conn, "A", "C", 1, 0, [6, 6, 1, 1])
    conn.commit()
    gen = db.next_generation(conn, "A")
    conn.commit()
    db.publish_result(conn, "A", "C", 0, 3, 5, gen, {
        "accident_id": "A", "channel_id": "C", "lo": 0, "hi": 3,
        "threshold": 5, "generation": gen, "considered_pids": [1],
        "uncovered_points": [],
        "over_limit": {"solvable": True, "min_excluded": 0,
                       "excluded_pids": []},
        "within_limit": {"solvable": False, "min_excluded": None,
                         "excluded_pids": None},
    })
    job_run = db.create_job(conn, "A", "C", 0, 3, 5, gen)
    db.mark_job_status(conn, job_run, "running")
    job_pend = db.create_job(conn, "A", "C", 0, 3, 5, gen)
    conn.commit()

    # 故障 1：upsert 全部成功后，推进 generation 时爆炸。
    real_next_gen = db.next_generation

    def boom(*_a, **_k):
        raise RuntimeError("injected generation failure")

    db.next_generation = boom
    try:
        try:
            coord.submit_patches("A", "C", [
                {"pid": 2, "start_sample": 0, "readings": [1, 1]},
            ])
            raise AssertionError("expected RuntimeError")
        except RuntimeError as exc:
            assert "injected" in str(exc)
    finally:
        db.next_generation = real_next_gen

    chk = db.connect(path)
    try:
        assert {p["pid"] for p in db.fetch_patches(chk, "A", "C")} == {1}
        active = db.get_active_result(chk, "A")
        assert active is not None and active["status"] == "active"
        assert int(active["generation"]) == gen
        rows = {r["id"]: r["status"] for r in chk.execute(
            "SELECT id, status FROM jobs").fetchall()}
        assert rows[job_run] == "running"
        assert rows[job_pend] == "pending"
        assert db.current_generation(chk, "A") == gen
    finally:
        chk.close()

    # 故障 2：批次第二片写入失败 -> 第一片的写入也随事务回滚。
    real_upsert = db.upsert_patch
    calls = {"n": 0}

    def flaky_upsert(c, *args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("injected disk failure")
        return real_upsert(c, *args, **kwargs)

    db.upsert_patch = flaky_upsert
    try:
        try:
            coord.submit_patches("A", "C", [
                {"pid": 2, "start_sample": 0, "readings": [1]},
                {"pid": 3, "start_sample": 0, "readings": [1]},
            ])
            raise AssertionError("expected RuntimeError")
        except RuntimeError as exc:
            assert "disk failure" in str(exc)
    finally:
        db.upsert_patch = real_upsert

    chk = db.connect(path)
    try:
        assert {p["pid"] for p in db.fetch_patches(chk, "A", "C")} == {1}
        assert db.get_active_result(chk, "A")["status"] == "active"
    finally:
        chk.close()
    conn.close()
    _cleanup_db(path)


def test_patch_expiration_targets_pending_and_running_rows_individually():
    """待处理 / 运行中任务按各自通道 + 闭区间判定（直接构造任务矩阵）。"""
    path = _temp_db()
    conn = db.connect(path)
    db.init_db(conn)
    coord = JobCoordinator(conn)  # 不起 worker，避免 pending 被取走

    db.upsert_patch(conn, "A", "C1", 1, 0, [1, 1, 1, 1])  # 旧窗 [0,3]
    conn.commit()
    gen = db.next_generation(conn, "A")

    jr_hit = db.create_job(conn, "A", "C1", 0, 3, 5, gen)
    db.mark_job_status(conn, jr_hit, "running")
    jp_edge = db.create_job(conn, "A", "C1", 3, 8, 5, gen)   # 端点 3 相接
    jp_far = db.create_job(conn, "A", "C1", 200, 201, 5, gen)
    jp_c2 = db.create_job(conn, "A", "C2", 0, 3, 5, gen)
    conn.commit()

    # pid1 同号替换并移出到 [100,100]：旧窗 [0,3] 仍应判定为波及。
    ack = coord.submit_patches("A", "C1", [
        {"pid": 1, "start_sample": 100, "readings": [1]},
    ])
    assert sorted(ack["stale_job_ids"]) == sorted([jr_hit, jp_edge])

    status = {r["id"]: r["status"] for r in conn.execute(
        "SELECT id, status FROM jobs").fetchall()}
    assert status[jr_hit] == "stale"
    assert status[jp_edge] == "stale"
    assert status[jp_far] == "pending"
    assert status[jp_c2] == "pending"
    conn.close()
    _cleanup_db(path)


def test_pending_unaffected_job_survives_and_runs_after_gate():
    """端到端：新核查把旧运行任务事故级作废后，新 pending 任务不受无关
    补传影响，关闸放行后仍能完成发布。"""
    path = _temp_db()
    app = create_app(db_path=path)
    coord = app.state.coordinator
    entered = threading.Event()
    gate = threading.Event()
    coord.pre_solve_hook = lambda _jid: (entered.set(), gate.wait(5))
    try:
        with TestClient(app) as client:
            c1 = "/accidents/A4/channels/C1"
            client.post(f"{c1}/patches", json={"patches": [
                {"pid": 1, "start_sample": 0, "readings": [1, 1, 1, 1]},
            ]})

            j1 = client.post(
                f"{c1}/verify", json={"lo": 0, "hi": 3, "threshold": 5}
            ).json()["job_id"]
            assert entered.wait(5)

            # 同事故提交新核查 -> j1 被事故级作废，j2 pending。
            j2 = client.post(
                f"{c1}/verify", json={"lo": 0, "hi": 3, "threshold": 5}
            ).json()["job_id"]
            assert client.get(f"/jobs/{j1}").json()["status"] == "stale"

            # 段外新片：不波及 [0,3] 的 j2。
            r = client.post(f"{c1}/patches", json={"patches": [
                {"pid": 9, "start_sample": 100, "readings": [1]},
            ]})
            assert r.json()["stale_job_ids"] == []
            assert client.get(f"/jobs/{j2}").json()["status"] == "pending"

            gate.set()
            b2 = _wait_status(client, j2, "done")
            assert b2["result"] is not None
    finally:
        coord.pre_solve_hook = None
    _cleanup_db(path)


def test_resubmitted_verify_uses_replaced_patches():
    """与 tests/test_api_e2e.py 第 4-5 步一致的关键重算路径。"""
    path = _temp_db()
    app = create_app(db_path=path)
    with TestClient(app) as client:
        base = "/accidents/A9/channels/C1"
        client.post(f"{base}/patches", json={"patches": [
            {"pid": 1, "start_sample": 0, "readings": [6, 6, 1, 1]},
            {"pid": 2, "start_sample": 0, "readings": [1, 1]},
            {"pid": 3, "start_sample": 2, "readings": [1, 1]},
        ]})
        j1 = client.post(
            f"{base}/verify", json={"lo": 0, "hi": 3, "threshold": 5}
        ).json()["job_id"]
        b1 = _wait_status(client, j1, "done")
        assert b1["result"]["over_limit"]["excluded_pids"] == [2]

        client.post(f"{base}/patches", json={"patches": [
            {"pid": 1, "start_sample": 0, "readings": [1, 1, 6, 6]},
        ]})
        j2 = client.post(
            f"{base}/verify", json={"lo": 0, "hi": 3, "threshold": 5}
        ).json()["job_id"]
        b2 = _wait_status(client, j2, "done")
        assert b2["result"]["over_limit"]["excluded_pids"] == [3]
    _cleanup_db(path)


ALL_MAIN = [
    test_coverage_window_closed_interval_rules,
    test_replacement_moved_out_withdraws_old_result,
    test_other_channel_patch_does_not_withdraw_result,
    test_affected_result_withdrawn_but_unrelated_job_survives,
    test_patch_submittable_during_solve_and_stale_compute_not_published,
    test_unaffected_patch_during_solve_keeps_job_publishable,
    test_new_verify_still_expires_inflight_job_accident_wide,
    test_patch_expiration_targets_pending_and_running_rows_individually,
    test_pending_unaffected_job_survives_and_runs_after_gate,
    test_patch_failure_rolls_back_everything,
    test_resubmitted_verify_uses_replaced_patches,
]


def main():
    for fn in ALL_MAIN:
        fn()
        print(f"PASS {fn.__name__}")
    print("\nALL INTERLEAVING TESTS PASSED")


if __name__ == "__main__":
    main()
