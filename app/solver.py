"""核查求解器。

问题模型
========
一次核查针对一起事故某个通道的样点闭区间 [lo, hi] 和正整数门槛 T。

参与求解的片段（patch）仅为与选段相交的片段，每个片段提供一段连续样点
读数。保留的片段必须：
  1. 覆盖选段内每一个样点（并集无缺口）；
  2. 在任何重叠位置读数一致。

分别求：
  * over_limit（超限）：选段内存在相邻两点绝对值都 >= T，所需排除的最少片数；
  * within_limit（不超限）：不存在这样的相邻对，所需排除的最少片数。

片段三态与扫描 DP
=================
逐个样点扫描，为每个点选择一个“公共读数”（候选只取覆盖该点的片段实际出现
过的读数）。每个片段在任一时刻处于三种互斥状态之一，用整数掩码表示：

  match (M) —— 已承诺保留的存活片段，在它覆盖的每个已扫描点读数都等于公共
               值。承诺不可撤销：后续公共值必须始终等于它的读数，否则该
               公共值非法。
  undecided (U) —— 存活且至今每点都认同、但尚未承诺的片段。后续仍认同则
               继续保留资格；一旦某点不认同即“失配”，进入 stained。
  stained (X) —— 已在某个点不认同公共值的片段，确定排除（累计掩码），
               此后永不复活、不再参与覆盖或一致性判定。

新片段在首次出现的点：认同公共值进 U，不认同进 X。

覆盖硬约束：每个点必须有一个承诺 match 片段。没有延续 match 时，从本点
认同公共值的 U 池（含新片段）中挑选一个提升为 match（锚点）。锚点按片段
逐个分支（至多 24 个选择），不是片段子集的组合枚举。

终点：保留集 = M ∪ U（U 中的片段全程认同、零代价保留），排除集 = X。

算法（不穷举片段组合）
======================
记忆化深度优先 DP，分支只发生在
  (a) 每个样点选哪个公共读数（候选数 <= 覆盖片段数）；
  (b) 无延续 match 时选哪个认同片段作锚点（候选数 <= 片段数）。
状态由样点位置与 M / U / X 掩码、上点是否高值、超限标志、目标侧组成，
掩码均为整数（片段上限 24）。实测全重叠最坏情形状态数约千级、毫秒级
完成，与 2^n 的片段子集枚举无关。最优排除掩码随递归自底向上传播。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class Patch:
    """一个录波片段：片号 pid，起始样点号 start，连续整数读数 readings。"""

    pid: int
    start: int
    readings: tuple[int, ...]

    @property
    def end(self) -> int:
        return self.start + len(self.readings) - 1

    def value_at(self, point: int) -> int:
        return self.readings[point - self.start]


@dataclass
class SideResult:
    """单侧（超限 / 不超限）求解结果。"""

    solvable: bool
    min_excluded: Optional[int] = None
    excluded_pids: Optional[list[int]] = None

    def to_dict(self) -> dict:
        return {
            "solvable": self.solvable,
            "min_excluded": self.min_excluded,
            "excluded_pids": self.excluded_pids,
        }


@dataclass
class VerifyResult:
    over_limit: SideResult
    within_limit: SideResult
    considered_pids: list[int]
    uncovered_points: list[int] = field(default_factory=list)


def _clipped_patch(pid: int, start: int, readings: list[int],
                   lo: int, hi: int) -> Optional[Patch]:
    """把片段裁剪到选段 [lo, hi] 内；不相交返回 None。"""
    end = start + len(readings) - 1
    if end < lo or start > hi:
        return None
    a = max(lo, start)
    b = min(hi, end)
    sub = tuple(readings[a - start: b - start + 1])
    return Patch(pid=pid, start=a, readings=sub)


def solve(patches: list[dict], lo: int, hi: int,
          threshold: int) -> VerifyResult:
    """求解核查问题。

    patches: [{"pid": int, "start": int, "readings": [int, ...]}, ...]
    lo, hi:  样点闭区间
    threshold: 正整数门槛
    """
    if hi < lo:
        return VerifyResult(
            over_limit=SideResult(solvable=False),
            within_limit=SideResult(solvable=False),
            considered_pids=[],
        )

    clipped: list[Patch] = []
    for raw in patches:
        p = _clipped_patch(raw["pid"], raw["start"], raw["readings"], lo, hi)
        if p is not None:
            clipped.append(p)

    considered = sorted(p.pid for p in clipped)
    if not clipped:
        return VerifyResult(
            over_limit=SideResult(solvable=False),
            within_limit=SideResult(solvable=False),
            considered_pids=[],
        )

    # 按点索引覆盖片段；同时检查覆盖完整性。
    by_point: dict[int, list[Patch]] = {}
    for p in clipped:
        for pt in range(p.start, p.end + 1):
            by_point.setdefault(pt, []).append(p)

    uncovered = [pt for pt in range(lo, hi + 1) if pt not in by_point]
    if uncovered:
        return VerifyResult(
            over_limit=SideResult(solvable=False),
            within_limit=SideResult(solvable=False),
            considered_pids=considered,
            uncovered_points=uncovered,
        )

    pid_index = {p.pid: i for i, p in enumerate(clipped)}
    n = len(clipped)
    point_cover: dict[int, int] = {}
    point_values: dict[int, dict[int, int]] = {}
    for pt in range(lo, hi + 1):
        cover = 0
        groups: dict[int, int] = {}
        for p in by_point[pt]:
            bit = 1 << pid_index[p.pid]
            cover |= bit
            v = p.value_at(pt)
            groups[v] = groups.get(v, 0) | bit
        point_cover[pt] = cover
        point_values[pt] = groups

    INF = float("inf")
    # memo: 状态 -> (最小排除数, 最优排除掩码 X or None)。
    memo: dict[tuple, tuple[float, Optional[int]]] = {}

    def dp(pt: int, match: int, undecided: int, stained: int,
           prev_high: bool, flag: int, want: int
           ) -> tuple[float, Optional[int]]:
        key = (pt, match, undecided, stained, prev_high, flag, want)
        if key in memo:
            return memo[key]

        if pt > hi:
            if flag != want:
                memo[key] = (INF, None)
                return memo[key]
            # 保留 = M ∪ U；排除 = X。
            memo[key] = (bin(stained).count("1"), stained)
            return memo[key]

        cover = point_cover[pt]
        groups = point_values[pt]
        match_here = match & cover
        undecided_here = undecided & cover
        fresh = cover & ~match & ~undecided & ~stained

        best_cost: float = INF
        best_x: Optional[int] = None

        for v, agree in groups.items():
            # 已承诺片段必须认同 v，否则该公共值非法。
            if match_here & ~agree:
                continue
            u_agree = undecided_here & agree
            u_stain = undecided_here & ~agree
            fresh_agree = fresh & agree
            fresh_stain = fresh & ~agree
            next_stained = stained | u_stain | fresh_stain
            pool = u_agree | fresh_agree
            # 覆盖硬约束：本点须有承诺 match（延续或新锚点）。
            if not (match_here or pool):
                continue
            high = abs(v) >= threshold
            next_flag = 1 if (flag or (prev_high and high)) else 0
            if want == 0 and next_flag == 1:
                continue

            if match_here:
                # 已有延续承诺覆盖：认同池整体保持 undecided。
                cost, x_mask = dp(
                    pt + 1, match_here, pool, next_stained,
                    high, next_flag, want,
                )
                if cost < best_cost:
                    best_cost, best_x = cost, x_mask
            else:
                # 无延续承诺：从认同池逐个选锚点提升为 match（按片分支）。
                for i in range(n):
                    if not (pool & (1 << i)):
                        continue
                    anchor = 1 << i
                    rest = pool & ~anchor
                    cost, x_mask = dp(
                        pt + 1, anchor, rest, next_stained,
                        high, next_flag, want,
                    )
                    if cost < best_cost:
                        best_cost, best_x = cost, x_mask

        memo[key] = (best_cost, best_x)
        return memo[key]

    def side(want: int) -> SideResult:
        cost, x_mask = dp(lo, 0, 0, 0, False, 0, want)
        if cost == INF or x_mask is None:
            return SideResult(solvable=False)
        pids = sorted(
            clipped[i].pid for i in range(n) if x_mask & (1 << i)
        )
        return SideResult(
            solvable=True, min_excluded=int(cost), excluded_pids=pids
        )

    return VerifyResult(
        over_limit=side(1),
        within_limit=side(0),
        considered_pids=considered,
        uncovered_points=uncovered,
    )
