"""断点续传 + 秒传 schema。

JSON 字段沿用项目现有 snake_case 风格（与分页接口 page_size 等一致）。
"""

from typing import Literal

from pydantic import BaseModel, Field


class UploadInitRequest(BaseModel):
    """POST /upload/init 请求体。"""

    file_hash: str = Field(min_length=32, max_length=32, description="整文件 MD5")
    file_name: str = Field(min_length=1, max_length=512, description="原始文件名")
    file_size: int = Field(ge=1, description="文件字节数")
    chunk_size: int = Field(gt=0, description="切片大小（字节）")
    chunk_count: int = Field(gt=0, description="切片总数")
    total_hash: str = Field(min_length=32, max_length=32, description="整体哈希（合并校验用）")


class UploadInitHitResponse(BaseModel):
    """init 响应：秒传命中。hit 是成功信号，前端据此跳过分片与 merge。"""

    ok: bool = True
    hit: Literal[True] = True
    instant: Literal[True] = True
    file_path: str = Field(description="已有文件相对 UPLOAD_DIR 的落盘路径")
    file_name: str = Field(description="服务端侧文件名（同名不同内容已被消歧）")
    download_url: str = Field(description="下载地址")


class UploadInitSessionResponse(BaseModel):
    """init 响应：未命中秒传，会话已就绪。uploaded 可直接作为续传起点。"""

    ok: bool = True
    hit: Literal[False] = False
    uploaded: list[int] = Field(default_factory=list, description="服务端已收分片下标")


class UploadCheckResponse(BaseModel):
    ok: bool = True
    uploaded: list[int] = Field(default_factory=list)


class UploadChunkResponse(BaseModel):
    ok: bool = True
    index: int = Field(description="本次落盘的分片下标")
    received: int = Field(description="该会话已收分片总数")


class UploadMergeRequest(BaseModel):
    file_hash: str = Field(min_length=32, max_length=32)


class UploadMergeResponse(BaseModel):
    ok: bool = True
    merged: bool = True
    path: str = Field(description="最终落盘路径（相对 UPLOAD_DIR）")
    size: int = Field(description="最终文件字节数")
    hash: str = Field(description="整体 MD5")
    deduplicated: bool = Field(description="是否复用了同名同内容的已有文件")
    download_url: str = Field(description="下载地址")
