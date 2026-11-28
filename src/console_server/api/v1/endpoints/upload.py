"""断点续传 + 秒传端点。

所有端点经 AuthMiddleware 全局鉴权（Bearer token），
get_current_user 依赖用于在路由层再次确认会话有效并绑定当前用户上下文。
"""

from fastapi import APIRouter, Depends, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession

from console_server.db.database import get_db
from console_server.model.rbac import User
from console_server.schema.upload import (
    UploadCheckResponse,
    UploadChunkResponse,
    UploadInitHitResponse,
    UploadInitRequest,
    UploadInitSessionResponse,
    UploadMergeRequest,
    UploadMergeResponse,
)
from console_server.services import upload as upload_service
from console_server.services.upload import UploadError
from console_server.utils.auth import get_current_user

router = APIRouter(prefix="/upload", tags=["upload"])

log = logger.bind(log_source="upload")


def _to_http_error(exc: UploadError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail=exc.detail)


@router.post(
    "/init",
    summary="初始化上传 / 秒传判定",
    description="命中秒传直接返回文件信息；未命中创建（或复用）上传会话并返回已传分片",
    response_model=UploadInitHitResponse | UploadInitSessionResponse,
)
async def init_upload(
    payload: UploadInitRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    try:
        result = await upload_service.init_upload(
            db,
            file_hash=payload.file_hash,
            file_name=payload.file_name,
            file_size=payload.file_size,
            chunk_size=payload.chunk_size,
            chunk_count=payload.chunk_count,
            total_hash=payload.total_hash,
        )
    except UploadError as exc:
        raise _to_http_error(exc) from exc

    if result["hit"]:
        return UploadInitHitResponse(
            file_path=result["file_path"],
            file_name=result["file_name"],
            download_url=f"/api/v1/upload/download/{payload.file_hash}",
        )
    return UploadInitSessionResponse(uploaded=result["uploaded"])


@router.get(
    "/check",
    summary="查询断点（已传分片）",
    description="返回服务端已收到的分片下标列表，用于刷新页面后续传",
    response_model=UploadCheckResponse,
)
async def check_upload(
    file_hash: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    try:
        uploaded = await upload_service.check_uploaded(db, file_hash)
    except UploadError as exc:
        raise _to_http_error(exc) from exc
    return UploadCheckResponse(uploaded=uploaded)


@router.post(
    "/chunk",
    summary="上传单个分片",
    description="multipart/form-data：file_hash + index + chunk_hash + file 二进制",
    response_model=UploadChunkResponse,
)
async def upload_chunk(
    file_hash: str = Form(..., min_length=32, max_length=32),
    index: int = Form(..., ge=0),
    chunk_hash: str = Form(..., min_length=32, max_length=32),
    file: UploadFile = Form(...),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    data = await file.read()
    try:
        received = await upload_service.receive_chunk(
            db, file_hash=file_hash, index=index, chunk_hash=chunk_hash, data=data
        )
    except UploadError as exc:
        raise _to_http_error(exc) from exc
    return UploadChunkResponse(index=index, received=received)


@router.post(
    "/merge",
    summary="合并分片",
    description="校验分片完整后顺序合并，整体 MD5 校验通过则落盘并登记秒传索引",
    response_model=UploadMergeResponse,
)
async def merge_upload(
    payload: UploadMergeRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    try:
        result = await upload_service.merge_upload(db, payload.file_hash)
    except UploadError as exc:
        raise _to_http_error(exc) from exc
    return UploadMergeResponse(
        path=result["path"],
        size=result["size"],
        hash=result["hash"],
        deduplicated=result["deduplicated"],
        download_url=f"/api/v1/upload/download/{payload.file_hash}",
    )


@router.get(
    "/download/{file_hash}",
    summary="下载文件",
    description="优先查秒传索引，其次查已合并会话；支持秒传命中后直接下载",
)
async def download_file(
    file_hash: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    try:
        path, file_name = await upload_service.resolve_download(db, file_hash)
    except UploadError as exc:
        raise _to_http_error(exc) from exc
    return FileResponse(path, filename=file_name)
