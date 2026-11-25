"""并发与失效语义测试：
1. 在途核查期间补传涉及选段 -> 旧任务 stale，新任务按新片段重算；
2. 段外补传不影响在途核查；
3. 写入失败时现行结果保留原状。
"""
import os
import sys
import tempfile
import threading
import time

sys.path.insert(0, "/workspace")

from fastapi.testclient import TestClient  # noqa: E402


def _wait_status(client, job_id, target):
    if isinstance(target, str):
        target = (target,)
    deadline = time.time() + 5
    while time.time() < deadline:
        body = client.get(f"/jobs/{job_id}").json()
        if body["status"] in target:
            return body
        time.sleep(0.02)
    raise AssertionError(f"job {job_id} never reached {target}")


def main():
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    from app.main import create_app
    from app import worker as worker_mod

    app = create_app(db_path=tmp.name)
    base = "/accidents/B1/channels/C1"

    # 通过 coordinator 的开工钩子阻塞，制造确定性的“在途”窗口（在 generation
    # 校验之前挂起，这样补传能可靠令任务过期）。
    gate = threading.Event()
    entered = threading.Event()
    coordinator = app.state.coordinator

    def hook(job_id):
        entered.set()
        gate.wait(timeout=5)

    coordinator.pre_solve_hook = hook
    try:
        with TestClient(app) as client:
            gate.set()  # 初始阶段不阻塞
            client.post(f"{base}/patches", json={"patches": [
                {"pid": 1, "start_sample": 0, "readings": [6, 6, 1, 1]},
                {"pid": 2, "start_sample": 0, "readings": [1, 1]},
                {"pid": 3, "start_sample": 2, "readings": [1, 1]},
            ]})
            r = client.post(
                f"{base}/verify", json={"lo": 0, "hi": 3, "threshold": 5}
            )
            jid1 = r.json()["job_id"]
            _wait_status(client, jid1, ("done", "failed", "stale"))
            gen_before = client.get("/accidents/B1/result").json()[
                "generation"
            ]
            print("1. initial result generation:", gen_before)

            # 关闸：提交后任务进入在途并阻塞在 generation 校验之前。
            gate.clear()
            entered.clear()
            r = client.post(
                f"{base}/verify", json={"lo": 0, "hi": 3, "threshold": 5}
            )
            jid2 = r.json()["job_id"]
            assert entered.wait(timeout=5)  # 确定性等待任务进入在途

            # 在途期间补传涉及选段的替换片段 -> 现行结果撤下、任务作废。
            client.post(f"{base}/patches", json={"patches": [
                {"pid": 1, "start_sample": 0, "readings": [1, 1, 6, 6]},
            ]})
            assert client.get("/accidents/B1/result").status_code == 404
            gate.set()
            b2 = _wait_status(client, jid2, ("done", "failed", "stale"))
            assert b2["status"] == "stale", b2
            print("2. in-flight job stale, active result withdrawn")

            # 再核查，基于替换后的片段重算。
            r = client.post(
                f"{base}/verify", json={"lo": 0, "hi": 3, "threshold": 5}
            )
            jid3 = r.json()["job_id"]
            b3 = _wait_status(client, jid3, ("done", "failed", "stale"))
            assert b3["status"] == "done"
            assert b3["result"]["over_limit"]["excluded_pids"] == [3]
            print(
                "3. recomputed on replaced data:",
                b3["result"]["over_limit"],
            )

            # 段外补传：现行结果保留、generation 不变。
            active_before = client.get("/accidents/B1/result").json()
            client.post(f"{base}/patches", json={"patches": [
                {"pid": 50, "start_sample": 1000, "readings": [9, 9]},
            ]})
            active_after = client.get("/accidents/B1/result")
            assert active_after.status_code == 200
            assert (
                active_after.json()["generation"]
                == active_before["generation"]
            )
            print("4. out-of-range patch keeps active result")
    finally:
        coordinator.pre_solve_hook = None

    # ---- 写入失败保留原状 ----
    from app import database as db
    from app.worker import JobCoordinator

    conn = db.connect(tmp.name)
    db.init_db(conn)
    coord = JobCoordinator(conn)
    current_gen = db.current_generation(conn, "B1")
    with coord.lock:
        conn.execute("ALTER TABLE results RENAME TO results_bak")
        job_id = db.create_job(
            conn, "B1", "C1", 0, 3, 5, generation=current_gen
        )
        conn.commit()
    coord.start()
    deadline = time.time() + 5
    status = None
    while time.time() < deadline:
        with coord.lock:
            row = db.get_job(conn, job_id)
        status = row["status"]
        if status in ("done", "failed", "stale"):
            break
        time.sleep(0.05)
    coord.stop()
    assert status == "failed", status
    with coord.lock:
        conn.execute("ALTER TABLE results_bak RENAME TO results")
        row = db.get_active_result(conn, "B1")
    assert row is not None
    print("5. publish failure marks job failed and preserves prior result")
    conn.close()
    os.unlink(tmp.name)
    print("\nALL CONCURRENCY TESTS PASSED")


if __name__ == "__main__":
    main()
