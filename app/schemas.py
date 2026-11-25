"""请求 / 响应 Pydantic 模式。"""

from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator


class PatchIn(BaseModel):
    """单个录波片段。"""

    pid: int = Field(..., description="片号，同一事故通道内同号可替换")
    start_sample: int = Field(..., description="起始样点号（整数，可负）")
    readings: list[int] = Field(
        ..., description="连续整数读数，非空", min_length=1
    )


class PatchBatch(BaseModel):
    """一次补传的片段批次，同一事故同通道至多 24 个不同片号。"""

    patches: list[PatchIn] = Field(..., min_length=1)


class VerifyRequest(BaseModel):
    """核查请求：样点闭区间 [lo, hi] 与正整数门槛。"""

    lo: int = Field(..., description="闭区间起点样点号")
    hi: int = Field(..., description="闭区间终点样点号，须 >= lo")
    threshold: int = Field(..., description="正整数门槛", ge=1)

    @field_validator("hi")
    @classmethod
    def _check_range(cls, v: int, info) -> int:
        lo = info.data.get("lo")
        if lo is not None and v < lo:
            raise ValueError("hi 必须大于等于 lo")
        return v


class SideOut(BaseModel):
    solvable: bool
    min_excluded: Optional[int] = None
    excluded_pids: Optional[list[int]] = None


class ResultPayload(BaseModel):
    accident_id: str
    channel_id: str
    lo: int
    hi: int
    threshold: int
    generation: int
    considered_pids: list[int]
    uncovered_points: list[int]
    over_limit: SideOut
    within_limit: SideOut


class JobOut(BaseModel):
    job_id: int
    accident_id: str
    channel_id: str
    lo: int
    hi: int
    threshold: int
    generation: int
    status: Literal["pending", "running", "done", "stale", "failed"]
    error: Optional[str] = None
    result: Optional[ResultPayload] = None


class PatchAck(BaseModel):
    accident_id: str
    channel_id: str
    accepted: int
    replaced_pids: list[int]
    distinct_pid_count: int
    affected_active_result: bool
    generation: int
