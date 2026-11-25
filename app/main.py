"""录波补片核查 FastAPI 应用。

端点
====
POST /accidents/{accident_id}/channels/{channel_id}/patches
    补传片段（同号替换，单事故单通道至多 24 片）。
POST /accidents/{accident_id}/channels/{channel_id}/verify
    发起核查（异步），返回任务 id。
GET  /jobs/{job_id}
    查询任务状态与（完成后的）核查结果。
GET  /accidents/{accident_id}/result
    取该事故当前唯一一份现行核查结果。
GET  /accidents/{accident_id}/patches
    读取该事故某通道当前片段（取数 / 核对用）。
GET  /docs
    Swagger UI，可在线试用。
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, status

from . import database as db
from .schemas import (
    JobOut,
    PatchAck,
    PatchBatch,
    ResultPayload,
    VerifyRequest,
)
from .worker import JobCoordinator, PatchLimitExceeded

MAX_PIDS_PER_CHANNEL = 24


def create_app(db_path: str | None = None) -> FastAPI:
    db_path = db_path or os.environ.get(
        "WAVECHECK_DB", "/data/wavecheck.db"
    )
    parent = os.path.dirname(os.path.abspath(db_path))
    os.makedirs(parent, exist_ok=True)
    conn = db.connect(db_path)
    db.init_db(conn)
    coordinator = JobCoordinator(conn)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        coordinator.start()
        yield
        coordinator.stop()
        conn.close()

    app = FastAPI(
        title="录波补片核查 API",
        version="1.0.0",
        description=(
            "接收录波片段并对指定样点闭区间做超限/不超限核查，"
            "给出两侧所需排除的最少片数与一组排除片号。"
        ),
        lifespan=lifespan,
    )
    app.state.coordinator = coordinator
    app.state.db_conn = conn

    def get_coordinator() -> JobCoordinator:
        return coordinator

    @app.post(
        "/accidents/{accident_id}/channels/{channel_id}/patches",
        response_model=PatchAck,
        status_code=status.HTTP_201_CREATED,
        summary="补传片段（同号替换）",
    )
    def upload_patches(
        accident_id: str,
        channel_id: str,
        batch: PatchBatch,
        coord: JobCoordinator = Depends(get_coordinator),
    ) -> PatchAck:
        # 同号片段在批次内去重（后者覆盖前者）。
        dedup: dict[int, dict] = {}
        for p in batch.patches:
            dedup[p.pid] = {
                "pid": p.pid,
                "start_sample": p.start_sample,
                "readings": p.readings,
            }
        incoming = list(dedup.values())

        # 上限校验与写入 / 失效在协调器的同一把锁、同一个事务内完成，
        # 避免并发补传突破 24 片上限，也保证写入失败时整体回滚。
        try:
            ack = coord.submit_patches(
                accident_id,
                channel_id,
                incoming,
                max_pids=MAX_PIDS_PER_CHANNEL,
            )
        except PatchLimitExceeded as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=str(exc),
            ) from exc
        return PatchAck(**ack)

    @app.post(
        "/accidents/{accident_id}/channels/{channel_id}/verify",
        response_model=JobOut,
        status_code=status.HTTP_202_ACCEPTED,
        summary="发起核查（异步）",
    )
    def verify(
        accident_id: str,
        channel_id: str,
        req: VerifyRequest,
        coord: JobCoordinator = Depends(get_coordinator),
    ) -> JobOut:
        job_id = coord.submit_verification(
            accident_id, channel_id, req.lo, req.hi, req.threshold
        )
        view = coord.get_job_view(job_id)
        return JobOut(**view)

    @app.get(
        "/jobs/{job_id}",
        response_model=JobOut,
        summary="查询核查任务状态与结果",
    )
    def get_job(
        job_id: int,
        coord: JobCoordinator = Depends(get_coordinator),
    ) -> JobOut:
        view = coord.get_job_view(job_id)
        if view is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"任务 {job_id} 不存在",
            )
        return JobOut(**view)

    @app.get(
        "/accidents/{accident_id}/result",
        response_model=ResultPayload,
        summary="取事故现行核查结果",
    )
    def get_result(
        accident_id: str,
        coord: JobCoordinator = Depends(get_coordinator),
    ) -> ResultPayload:
        payload = coord.get_active_view(accident_id)
        if payload is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="该事故当前没有现行核查结果",
            )
        return ResultPayload(**payload)

    @app.get(
        "/accidents/{accident_id}/channels/{channel_id}/patches",
        summary="读取通道当前片段",
    )
    def list_patches(
        accident_id: str,
        channel_id: str,
        coord: JobCoordinator = Depends(get_coordinator),
    ) -> dict:
        patches = coord.list_patches(accident_id, channel_id)
        return {
            "accident_id": accident_id,
            "channel_id": channel_id,
            "count": len(patches),
            "patches": patches,
        }

    return app


__all__ = ["create_app"]
