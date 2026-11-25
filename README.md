# 录波补片核查服务（wavecheck）

复盘人员核对波形超限结论时，录波补片可能互相冲突。本服务用 FastAPI + SQLite
实现补传接收与核查 API：对指定样点闭区间和门槛，分别求出“超限”和“不超限”
两种结论下所需排除的**最少片段数**，并各给出一组达到该数目的排除片号；
无解的一侧明确标注。

## 判定口径

- 连续**两个样点的绝对值都达到门槛**才算一次超限（相邻对，两点均 `|x| >= T`）。
- 核查只考虑与选段 `[lo, hi]` 相交的片段；片段在选段外的读数不参与计算。
- 保留的片段必须：
  1. 覆盖选段内**每一个**样点（并集无缺口）；
  2. 在任何重叠位置读数一致。
- 选段内只要存在未被任何保留片段覆盖的点，两侧均判无解。

## 算法（不穷举片段组合）

扫描线 + 记忆化动态规划（`app/solver.py`）。逐个样点选择“公共读数”，把覆盖
片段划分为三个互斥状态（整数掩码，片段上限 24）：

| 状态 | 含义 |
| --- | --- |
| `match` | 已承诺保留的存活片段，读数处处等于公共值，承诺不可撤销 |
| `undecided` | 存活且至今每点都认同、尚未承诺的片段 |
| `stained` | 曾在某点不认同公共值，确定排除（累计） |

分支只发生在：(a) 每个样点选哪个公共读数（候选数 ≤ 覆盖片段数）；
(b) 无延续承诺片段时选哪个认同片段作覆盖锚点（候选数 ≤ 片段数）。
**不枚举任何片段子集**。终点保留集 = `match ∪ undecided`，排除集 = `stained`。
实测 24 片全重叠、41 点的最坏情形约千级状态、毫秒级完成。

## 数据与并发语义

- 每起事故、每个通道至多 24 个**不同片号**；同号片段以新读数整体替换（UPSERT）。
- 每起事故至多**一份现行核查结果**（`results` 表中 `status=active` 的唯一行）。
- 计算期间允许补传：求解器在协调锁之外运行，补传请求不会被慢计算阻塞。
- 补传是否波及某个目标，由该目标**自己的通道与样点闭区间 `[lo, hi]`** 判定：
  通道相同，且本批次任一片段的覆盖闭区间（端点相接也算相交）与其 `[lo, hi]`
  相交。对同号替换片，会同时比较该片在库中的**旧覆盖范围与本次的新覆盖范围**
  ——替换后移出选段时，旧范围曾支撑过旧结论，结论同样要撤。
  - 受影响的**现行结果**在补传事务内立即置为 `withdrawn`，复盘人员无法再取到；
  - 受影响的 `pending` / `running` 任务在同一事务内标记 `stale`。worker 算完后
    还要在发布事务里复查任务状态，因此旧计算即使完成也不能发布；
  - 通道不同或闭区间不相交的结论继续供数，任务不会被置 `stale`，完成后照常发布。
- 同一事故提交新的核查时，仍**事故级**作废所有在途 / 待处理旧任务（不限通道）。
- 片段写入、24 片上限校验、撤结果、任务失效、generation 推进在**同一个事务**内；
  补传写入中途失败时整体回滚，片段 / 现行结论 / 任务状态 / 版本号保持补传前原状。
- 两侧均无解，或结果写入数据库失败时，**保留原状**（事务回滚，不破坏既有数据）。
- 版本号持久化在独立的 `generations` 表；任务能否发布以其 `jobs.status` 为准
  （补传 / 新核查在同一写事务内把受影响任务置 `stale`，构成发布栅栏）。

## 目录结构

```
app/
  solver.py     核查求解器（扫描 DP，含完整 docstring）
  database.py   SQLite 建表与数据访问
  schemas.py    Pydantic 请求/响应模型
  worker.py     异步核查线程、generation 失效与重算
  main.py       FastAPI 应用与路由
tests/
  test_solver_bruteforce.py  求解器 vs 暴力枚举子集，2000 随机用例对拍
  test_api_e2e.py            端到端 API 关键操作
  test_concurrency.py        在途失效、段外更新、写入失败保留原状
Dockerfile / docker-compose.yml / requirements.txt
```

## 启动

```bash
docker compose up --build
```

服务监听 `http://localhost:8000`，SQLite 数据落在命名卷 `wavecheck-data`
（容器内 `/data/wavecheck.db`）。

浏览器打开 `http://localhost:8000/docs` 即可在线试用“补传片段 / 发起核查 /
查询任务 / 取现行结果 / 取片段”。

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/accidents/{aid}/channels/{cid}/patches` | 补传片段（同号替换，≤24 片） |
| POST | `/accidents/{aid}/channels/{cid}/verify` | 异步发起核查，返回 `job_id` |
| GET | `/jobs/{job_id}` | 查询任务状态与完成后的结果 |
| GET | `/accidents/{aid}/result` | 取该事故唯一现行结果 |
| GET | `/accidents/{aid}/channels/{cid}/patches` | 读取通道当前片段 |

### 快速试用（curl）

```bash
B=http://localhost:8000/accidents/DEMO/channels/CH1

# 1) 补传三个片段
curl -s -X POST $B/patches -H 'Content-Type: application/json' -d '{
  "patches": [
    {"pid": 1, "start_sample": 0, "readings": [6, 6, 1, 1]},
    {"pid": 2, "start_sample": 0, "readings": [1, 1]},
    {"pid": 3, "start_sample": 2, "readings": [1, 1]}
  ]}'

# 2) 发起核查（选段 [0,3]，门槛 5）
curl -s -X POST $B/verify -H 'Content-Type: application/json' \
  -d '{"lo": 0, "hi": 3, "threshold": 5}'

# 3) 用返回的 job_id 查询；4) 取现行结果
curl -s http://localhost:8000/accidents/DEMO/result
```

## 本地自测

```bash
pip install -r requirements.txt
pip install pytest httpx          # 运行测试需要
python -m pytest -q tests/        # 全部用例（含交错回归）
# 或直接运行：
python tests/test_solver_bruteforce.py
python tests/test_api_e2e.py
python tests/test_concurrency.py
python tests/test_patch_interleave.py
```

- 对拍测试把 DP 结果与“枚举全部片段子集”的暴力最优解逐案比较，并独立校验
  返回的排除片号确实构成可行解（全覆盖、重叠一致、结论侧正确）。
- 端到端测试覆盖补传、同号替换、24 片上限、核查、单一现行结果、取数、
  无解不发布、参数校验。
- 并发测试用确定性钩子复现“在途补传 → 旧任务 stale → 按新片段重算”，
  并验证段外更新不撤结果、写入失败保留原状。
- 交错回归测试覆盖：同号替换移出 / 移入选段（新旧覆盖窗）、闭区间端点、
  通道隔离、受影响结论撤下而不相关任务存活可发布、慢求解期间补传即时受理、
  stale 旧计算完成后不能发布、新核查事故级作废在途任务、补传失败整体回滚。
