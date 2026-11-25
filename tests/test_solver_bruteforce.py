"""求解器对拍测试：正式 solve() 与枚举全部片段子集的暴力解比较，
并独立校验返回的排除片号确实构成可行解。"""
import itertools
import random
import sys

sys.path.insert(0, "/workspace/app")
from solver import solve  # noqa: E402


def brute(patches, lo, hi, threshold):
    cp = []
    for p in patches:
        s = p["start"]
        e = s + len(p["readings"]) - 1
        if e < lo or s > hi:
            continue
        a, b = max(lo, s), min(hi, e)
        cp.append((a, b, tuple(p["readings"][a - p["start"]: b - p["start"] + 1])))
    n = len(cp)
    best = {0: None, 1: None}
    for r in range(n + 1):
        for ex in itertools.combinations(range(n), r):
            vals = {}
            ok = True
            for i, (s, e, rd) in enumerate(cp):
                if i in ex:
                    continue
                for k in range(e - s + 1):
                    pt = s + k
                    if pt in vals and vals[pt] != rd[k]:
                        ok = False
                    vals[pt] = rd[k]
            if not ok or any(pt not in vals for pt in range(lo, hi + 1)):
                continue
            flag = 0
            for pt in range(lo, hi):
                if abs(vals[pt]) >= threshold and abs(vals[pt + 1]) >= threshold:
                    flag = 1
                    break
            if best[flag] is None:
                best[flag] = r
    return best


def verify_feasible(patches, lo, hi, threshold, excluded_pids, want_flag):
    """独立验证：给定排除片号，保留片是否全覆盖、一致、且满足目标侧。"""
    excl = set(excluded_pids)
    vals = {}
    for p in patches:
        if p["pid"] in excl:
            continue
        for k, v in enumerate(p["readings"]):
            pt = p["start"] + k
            if lo <= pt <= hi:
                if pt in vals and vals[pt] != v:
                    return False, "inconsistent"
                vals[pt] = v
    if any(pt not in vals for pt in range(lo, hi + 1)):
        return False, "uncovered"
    flag = 0
    for pt in range(lo, hi):
        if abs(vals[pt]) >= threshold and abs(vals[pt + 1]) >= threshold:
            flag = 1
            break
    if flag != want_flag:
        return False, "wrong-flag"
    return True, "ok"


def main():
    random.seed(2026)
    trials = 2000
    for t in range(trials):
        lo = random.randint(0, 8)
        hi = lo + random.randint(0, 10)
        n = random.randint(1, 10)
        patches = []
        used = set()
        for _ in range(n):
            pid = random.randint(1, 60)
            while pid in used:
                pid = random.randint(1, 60)
            used.add(pid)
            start = random.randint(lo - 5, hi)
            length = random.randint(1, 8)
            patches.append({
                "pid": pid,
                "start": start,
                "readings": [random.randint(-12, 12) for _ in range(length)],
            })
        threshold = random.randint(1, 10)

        res = solve(patches, lo, hi, threshold)
        exp = brute(patches, lo, hi, threshold)

        for want, side in ((1, res.over_limit), (0, res.within_limit)):
            e = exp[want]
            if e is None:
                assert not side.solvable, (t, want, "should be unsolvable")
                continue
            assert side.solvable, (t, want, "should be solvable")
            assert side.min_excluded == e, (t, want, side, e)
            assert len(side.excluded_pids) == side.min_excluded
            assert len(set(side.excluded_pids)) == side.min_excluded
            ok, reason = verify_feasible(
                patches, lo, hi, threshold, side.excluded_pids, want
            )
            assert ok, (t, want, reason, side)
    print(f"OK: {trials} random trials matched brute force and passed feasibility")


if __name__ == "__main__":
    main()
