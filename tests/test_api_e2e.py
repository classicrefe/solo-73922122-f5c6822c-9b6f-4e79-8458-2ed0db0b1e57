"""端到端 API 测试：使用 FastAPI TestClient + 临时 SQLite 库，
覆盖补传、核查、替换、失效撤结果、在途重算、上限、取数等关键流程。"""
import os
import sys
import tempfile
import time

sys.path.insert(0, "/workspace")

from fastapi.testclient import TestClient  # noqa: E402


def build_client():
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.close()
    os.environ["WAVECHECK_DB"] = tmp.name
    # 延迟导入，确保环境变量生效
    from app.main import create_app

    app = create_app(db_path=tmp.name)
    client = TestClient(app)
    client.__enter__()  # 触发 lifespan，启动后台 worker
    return client, tmp.name


def wait_done(client, job_id, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = client.get(f"/jobs/{job_id}")
        assert r.status_code == 200
        body = r.json()
        if body["status"] in ("done", "failed", "stale"):
            return body
        time.sleep(0.02)
    raise AssertionError("job did not finish in time")


def main():
    client, db_path = build_client()
    base = "/accidents/A1/channels/C1"

    # 1. 补传片段：构造可超限与可不超限两种解
    # 选段 [0,3]，门槛 5。
    # p1=[6,6,1,1] 覆盖全段；p2=[1,1] 覆盖 0-1；p3=[1,1] 覆盖 2-3。
    # 超限：保留 p1（点 0,1 相邻双高）+ p3（与 p1 在点 2,3 同为 1，相容），
    #       仅排除与 p1 在点 0 冲突的 p2 -> 1 片。
    # 不超限：保留全低值的 p2,p3（公共值全为 1，无相邻双高），排除 p1 -> 1 片。
    patches = [
        {"pid": 1, "start_sample": 0, "readings": [6, 6, 1, 1]},
        {"pid": 2, "start_sample": 0, "readings": [1, 1]},
        {"pid": 3, "start_sample": 2, "readings": [1, 1]},
    ]
    r = client.post(f"{base}/patches", json={"patches": patches})
    assert r.status_code == 201, r.text
    ack = r.json()
    assert ack["distinct_pid_count"] == 3
    assert ack["affected_active_result"] is False
    print("1. upload ok:", ack)

    # 2. 发起核查
    r = client.post(f"{base}/verify", json={"lo": 0, "hi": 3, "threshold": 5})
    assert r.status_code == 202
    job_id = r.json()["job_id"]
    body = wait_done(client, job_id)
    assert body["status"] == "done", body
    result = body["result"]
    assert result is not None
    # 超限：排除 p2，保留 p1 + p3
    assert result["over_limit"]["solvable"]
    assert result["over_limit"]["min_excluded"] == 1
    assert result["over_limit"]["excluded_pids"] == [2]
    # 不超限：排除 p1，保留全低值 p2 + p3
    assert result["within_limit"]["solvable"]
    assert result["within_limit"]["min_excluded"] == 1
    assert result["within_limit"]["excluded_pids"] == [1]
    print("2. verify ok:", result["over_limit"], result["within_limit"])

    # 3. 每起事故只供一份现行结果
    r = client.get("/accidents/A1/result")
    assert r.status_code == 200
    first_generation = r.json()["generation"]

    # 再次核查（同参数）应替换为新结果，仍是一份 active
    r = client.post(f"{base}/verify", json={"lo": 0, "hi": 3, "threshold": 5})
    job2 = r.json()["job_id"]
    wait_done(client, job2)
    r = client.get("/accidents/A1/result")
    assert r.status_code == 200
    assert r.json()["generation"] > first_generation
    print("3. single active result maintained")

    # 4. 同号替换：替换 p1，使冲突结构变化
    r = client.post(
        f"{base}/patches",
        json={"patches": [
            {"pid": 1, "start_sample": 0, "readings": [1, 1, 6, 6]}
        ]},
    )
    assert r.status_code == 201
    ack = r.json()
    assert ack["replaced_pids"] == [1]
    assert ack["affected_active_result"] is True  # 涉及选段，撤下现行结果
    # 现行结果已撤
    r = client.get("/accidents/A1/result")
    assert r.status_code == 404
    print("4. replacement withdraws active result")

    # 5. 重新核查，验证替换生效：p1 变为 [1,1,6,6]（点 2,3 双高）。
    #    保留 p1 + p2（点 0,1 同为 1 相容），排除点 2,3 冲突的 p3，可超限。
    r = client.post(f"{base}/verify", json={"lo": 0, "hi": 3, "threshold": 5})
    job3 = r.json()["job_id"]
    body = wait_done(client, job3)
    result = body["result"]
    assert result["over_limit"]["solvable"]
    assert result["over_limit"]["min_excluded"] == 1
    assert result["over_limit"]["excluded_pids"] == [3]
    print("5. recompute after replacement ok:", result["over_limit"])

    # 6. 段外更新不撤结果：补一个完全在选段外的通道片段
    r = client.post(
        f"{base}/patches",
        json={"patches": [
            {"pid": 9, "start_sample": 100, "readings": [1, 2, 3]}
        ]},
    )
    assert r.status_code == 201
    assert r.json()["affected_active_result"] is False
    r = client.get("/accidents/A1/result")
    assert r.status_code == 200  # 结果仍在
    print("6. out-of-range update keeps result")

    # 7. 取数接口
    r = client.get(f"{base}/patches")
    assert r.status_code == 200
    assert r.json()["count"] == 4
    print("7. fetch patches ok")

    # 8. 24 片上限
    for pid in range(20, 44):  # 再加 24 个新片号 -> 总数 28，超限
        r = client.post(
            f"{base}/patches",
            json={"patches": [
                {"pid": pid, "start_sample": 0, "readings": [1]}
            ]},
        )
        if r.status_code == 422:
            print(f"8. limit enforced at pid={pid}, detail={r.json()['detail']}")
            break
    else:
        raise AssertionError("24-patch limit not enforced")

    # 9. 覆盖缺口 -> 两侧均无解，且不发布（保留上一份 active）
    client2_path_db = None
    r = client.post(
        "/accidents/A2/channels/C1/patches",
        json={"patches": [
            {"pid": 1, "start_sample": 0, "readings": [5]},
            {"pid": 2, "start_sample": 2, "readings": [5]},
        ]},
    )
    assert r.status_code == 201
    r = client.post(
        "/accidents/A2/channels/C1/verify",
        json={"lo": 0, "hi": 2, "threshold": 5},
    )
    job_gap = r.json()["job_id"]
    body = wait_done(client, job_gap)
    assert body["status"] == "done"
    assert body["result"] is None  # 两侧无解，不发布
    r = client.get("/accidents/A2/result")
    assert r.status_code == 404
    print("9. unsolvable both sides publishes nothing")

    # 10. 非法请求校验
    r = client.post(
        f"{base}/verify", json={"lo": 5, "hi": 2, "threshold": 5}
    )
    assert r.status_code == 422
    r = client.post(
        f"{base}/verify", json={"lo": 0, "hi": 3, "threshold": 0}
    )
    assert r.status_code == 422
    print("10. request validation ok")

    # 11. 不存在的任务
    r = client.get("/jobs/999999")
    assert r.status_code == 404
    print("11. missing job 404 ok")

    client.close()
    os.unlink(db_path)
    print("\nALL API E2E TESTS PASSED")


if __name__ == "__main__":
    main()
