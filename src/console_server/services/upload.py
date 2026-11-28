"""断点续传 + 秒传核心逻辑。

存储布局（UPLOAD_DIR 下）::

    storage/
    ├── chunks/<fileHash>/     # 分片目录：<index> 已校验分片、<index>.tmp 写入中、<hash>.merging 合并中
    └── files/<fileName>       # 合并后的最终文件（同名不同内容加 hash 后缀消歧）

状态源是 PostgreSQL 三张表（upload_sessions / upload_chunks / upload_files），
磁盘只存分片与最终文件；秒传索引独立于会话生命周期，清理会话不影响秒传。
"""

import hashlib
import math
import os
import re
import shutil
from datetime import datetime, timedelta, timezone
from functools import partial
from pathlib import Path

from anyio import to_thread
from loguru import logger
from sqlalchemy import delete as sa_delete
from sqlalchemy import func as sa_func
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from console_server.core.config import settings
from console_server.model.upload import UploadChunk, UploadFile, UploadSession

log = logger.bind(log_source="upload")

HASH_LENGTH = 32
_NAME_UNSAFE = re.compile(r"[\\/]|\.\.")
_COPY_BUFFER = 1024 * 1024


class UploadError(Exception):
    """带 HTTP 状态码的上传业务异常，端点层转换为 HTTPException。"""

    def __init__(self, status_code: int, detail: str):
        self.status_code = status_code
        self.detail = detail
        super().__init__(detail)


# ---------------------------------------------------------------------------
# 目录与文件名
# ---------------------------------------------------------------------------


def upload_root() -> Path:
    return Path(settings.UPLOAD_DIR)


def chunks_dir(file_hash: str) -> Path:
    return upload_root() / "chunks" / file_hash


def files_dir() -> Path:
    return upload_root() / "files"


def ensure_upload_dirs() -> None:
    files_dir().mkdir(parents=True, exist_ok=True)
    (upload_root() / "chunks").mkdir(parents=True, exist_ok=True)


def sanitize_file_name(name: str) -> str:
    """过滤路径穿越字符；合法则原样返回。"""
    cleaned = name.strip()
    if not cleaned or cleaned in {".", ".."} or _NAME_UNSAFE.search(cleaned):
        raise UploadError(400, "fileName 含有非法字符")
    return cleaned[:512]


# ---------------------------------------------------------------------------
# 哈希工具
# ---------------------------------------------------------------------------


def md5_of_path(path: Path, buffer_size: int = _COPY_BUFFER) -> str:
    digest = hashlib.md5()
    with open(path, "rb") as f:
        while chunk := f.read(buffer_size):
            digest.update(chunk)
    return digest.hexdigest()


# ---------------------------------------------------------------------------
# 秒传索引
# ---------------------------------------------------------------------------


async def _drop_stale_index(db: AsyncSession, record: UploadFile) -> None:
    """摘除脏索引记录。比对 path，避免误删并发写入的新条目。"""
    await db.execute(
        sa_delete(UploadFile).where(
            UploadFile.file_hash == record.file_hash,
            UploadFile.path == record.path,
        )
    )
    await db.commit()
    log.warning(f"已摘除失效秒传索引：{record.file_hash} -> {record.path}")


async def lookup_instant(
    db: AsyncSession, file_hash: str, expected_size: int
) -> UploadFile | None:
    """秒传命中判定：索引有 + 文件存在 + 大小一致，三者缺一不可。

    大小不一致时重算一次 MD5 复核：内容一致则顺手修正索引；内容不同判定为脏记录并摘除。
    """
    record = await db.get(UploadFile, file_hash)
    if record is None:
        return None
    path = upload_root() / record.path
    if not path.is_file():
        await _drop_stale_index(db, record)
        return None
    actual_size = path.stat().st_size
    if actual_size == record.size == expected_size:
        return record
    actual_hash = await to_thread.run_sync(md5_of_path, path)
    if actual_hash == file_hash:
        record.size = actual_size
        await db.commit()
        return record if actual_size == expected_size else None
    await _drop_stale_index(db, record)
    return None


async def register_instant(
    db: AsyncSession, *, file_hash: str, path: str, file_name: str, size: int
) -> None:
    """写入/更新秒传索引（upsert）。"""
    stmt = pg_insert(UploadFile).values(
        file_hash=file_hash, path=path, file_name=file_name, size=size
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[UploadFile.file_hash],
        set_={"path": path, "file_name": file_name, "size": size},
    )
    await db.execute(stmt)


# ---------------------------------------------------------------------------
# 分片查询
# ---------------------------------------------------------------------------


async def get_uploaded_indexes(db: AsyncSession, file_hash: str) -> list[int]:
    result = await db.execute(
        select(UploadChunk.chunk_index)
        .where(UploadChunk.file_hash == file_hash)
        .order_by(UploadChunk.chunk_index)
    )
    return list(result.scalars().all())


# ---------------------------------------------------------------------------
# init / check
# ---------------------------------------------------------------------------


def _validate_hash(value: str, field: str) -> str:
    value = value.strip().lower()
    if len(value) != HASH_LENGTH or not re.fullmatch(r"[0-9a-f]{32}", value):
        raise UploadError(400, f"{field} 不是合法的 32 位 MD5")
    return value


def _check_metadata(session: UploadSession, *, file_name: str, file_size: int,
                    chunk_size: int, chunk_count: int, total_hash: str) -> None:
    """复用已有会话前校验元信息一致，不一致说明是「同 hash 不同切法」，拒绝。"""
    if (
        session.file_name != file_name
        or session.file_size != file_size
        or session.chunk_size != chunk_size
        or session.chunk_count != chunk_count
        or session.total_hash != total_hash
    ):
        raise UploadError(409, "与已有上传会话的元信息不一致，请检查文件与切片参数")


async def init_upload(
    db: AsyncSession, *, file_hash: str, file_name: str, file_size: int,
    chunk_size: int, chunk_count: int, total_hash: str,
) -> dict:
    file_name = sanitize_file_name(file_name)
    file_hash = _validate_hash(file_hash, "fileHash")
    total_hash = _validate_hash(total_hash, "totalHash")
    if file_hash != total_hash:
        raise UploadError(400, "fileHash 与 totalHash 不一致")
    if file_size > settings.UPLOAD_MAX_FILE_SIZE:
        raise UploadError(413, f"文件超过大小上限（{settings.UPLOAD_MAX_FILE_SIZE} 字节）")
    if chunk_count != math.ceil(file_size / chunk_size):
        raise UploadError(400, "chunkCount 与 fileSize/chunkSize 不匹配")

    # 秒传：命中即成功，不建会话
    hit = await lookup_instant(db, file_hash, file_size)
    if hit is not None:
        return {
            "hit": True,
            "file_path": hit.path,
            "file_name": hit.file_name,
            "uploaded": [],
        }

    ensure_upload_dirs()
    session = await db.get(UploadSession, file_hash)
    if session is not None:
        _check_metadata(
            session, file_name=file_name, file_size=file_size,
            chunk_size=chunk_size, chunk_count=chunk_count, total_hash=total_hash,
        )
        if session.merged:
            # 已合并但秒传索引缺失的兜底：能重建就按秒传返回，否则重置会话
            merged_path = session.merged_path or ""
            path = upload_root() / merged_path
            if merged_path and path.is_file() and path.stat().st_size == session.file_size:
                await register_instant(
                    db, file_hash=file_hash, path=merged_path,
                    file_name=session.file_name, size=session.file_size,
                )
                await db.commit()
                return {
                    "hit": True, "file_path": merged_path,
                    "file_name": session.file_name, "uploaded": [],
                }
            # 文件已不在：重置会话为未合并，允许重新上传
            await db.execute(sa_delete(UploadChunk).where(UploadChunk.file_hash == file_hash))
            session.merged = False
            session.merged_path = None
            session.last_error = None
            await db.commit()
        uploaded = await get_uploaded_indexes(db, file_hash)
        return {"hit": False, "uploaded": uploaded}

    db.add(
        UploadSession(
            file_hash=file_hash, file_name=file_name, file_size=file_size,
            chunk_size=chunk_size, chunk_count=chunk_count, total_hash=total_hash,
        )
    )
    await db.commit()
    return {"hit": False, "uploaded": []}


async def check_uploaded(db: AsyncSession, file_hash: str) -> list[int]:
    file_hash = _validate_hash(file_hash, "fileHash")
    return await get_uploaded_indexes(db, file_hash)


# ---------------------------------------------------------------------------
# 分片写入
# ---------------------------------------------------------------------------


def _save_chunk_sync(file_hash: str, index: int, chunk_hash: str, data: bytes) -> None:
    """原子写分片：写 <index>.tmp → MD5 校验 → os.replace 为 <index>。"""
    directory = chunks_dir(file_hash)
    directory.mkdir(parents=True, exist_ok=True)
    tmp_path = directory / f"{index}.tmp"
    final_path = directory / str(index)
    try:
        tmp_path.write_bytes(data)
        if hashlib.md5(data).hexdigest() != chunk_hash:
            raise UploadError(422, "分片哈希校验失败")
        os.replace(tmp_path, final_path)
    finally:
        tmp_path.unlink(missing_ok=True)


async def receive_chunk(
    db: AsyncSession, *, file_hash: str, index: int, chunk_hash: str, data: bytes
) -> int:
    """落盘单个分片并登记，返回该会话已收分片总数。"""
    file_hash = _validate_hash(file_hash, "fileHash")
    chunk_hash = _validate_hash(chunk_hash, "chunkHash")
    session = await db.get(UploadSession, file_hash)
    if session is None:
        raise UploadError(404, "上传会话不存在，请先调用 init")
    if session.merged:
        raise UploadError(409, "文件已合并，无需继续上传分片")
    if not 0 <= index < session.chunk_count:
        raise UploadError(400, f"分片下标越界：{index}")
    expected = min(session.chunk_size, session.file_size - index * session.chunk_size)
    if len(data) != expected:
        raise UploadError(
            400, f"分片大小不符：期望 {expected} 字节，实际 {len(data)} 字节"
        )

    await to_thread.run_sync(_save_chunk_sync, file_hash, index, chunk_hash, data)
    # upsert（而非 do_nothing）：磁盘分片被清理后重传同一分片可修复脏记录
    stmt = pg_insert(UploadChunk).values(
        file_hash=file_hash, chunk_index=index, chunk_hash=chunk_hash
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[UploadChunk.file_hash, UploadChunk.chunk_index],
        set_={"chunk_hash": chunk_hash},
    )
    await db.execute(stmt)
    received = await db.scalar(
        select(sa_func.count())
        .select_from(UploadChunk)
        .where(UploadChunk.file_hash == file_hash)
    )
    await db.commit()
    return int(received or 0)


# ---------------------------------------------------------------------------
# 合并
# ---------------------------------------------------------------------------


def _merge_sync(file_hash: str, chunk_count: int, total_hash: str) -> tuple[Path, str]:
    """顺序拼接分片到 <hash>.merging 并校验整体 MD5。返回 (临时路径, 实际哈希)。"""
    directory = chunks_dir(file_hash)
    merging = directory / f"{file_hash}.merging"
    digest = hashlib.md5()
    try:
        with open(merging, "wb") as out:
            for i in range(chunk_count):
                with open(directory / str(i), "rb") as src:
                    while chunk := src.read(_COPY_BUFFER):
                        digest.update(chunk)
                        out.write(chunk)
    except UploadError:
        raise
    except Exception as exc:
        merging.unlink(missing_ok=True)
        raise UploadError(500, f"合并分片失败：{exc}") from exc
    actual = digest.hexdigest()
    if actual != total_hash:
        merging.unlink(missing_ok=True)
        raise UploadError(422, "整体哈希校验失败（hash mismatch）")
    return merging, actual


def _resolve_target_sync(file_name: str, file_hash: str, merging: Path) -> tuple[Path, bool]:
    """决定合并产物的最终落盘位置。返回 (目标路径, 是否复用已有文件)。

    - files/<fileName> 不存在：直接使用；
    - 存在且 MD5 相同：复用已有文件，丢弃本次合并产物（deduplicated）；
    - 存在但 MD5 不同：<stem>_<hash前8位><ext>，冲突则退到 12/16/32 位。
    """
    target = files_dir() / file_name
    if not target.exists():
        return target, False
    if md5_of_path(target) == file_hash:
        return target, True
    stem, ext = os.path.splitext(file_name)
    for width in (8, 12, 16, 32):
        candidate = files_dir() / f"{stem}_{file_hash[:width]}{ext}"
        if not candidate.exists():
            return candidate, False
    raise UploadError(500, "无法为目标文件分配落盘路径")


def _finalize_merge_sync(merging: Path, target: Path, reused: bool) -> int:
    """reused=True 时丢弃合并产物复用已有文件；否则原子改名落盘。返回最终大小。"""
    if reused:
        merging.unlink(missing_ok=True)
    else:
        os.replace(merging, target)
    return target.stat().st_size


async def merge_upload(db: AsyncSession, file_hash: str) -> dict:
    file_hash = _validate_hash(file_hash, "fileHash")
    # 行锁串行化并发 merge（重复 merge 幂等返回）
    result = await db.execute(
        select(UploadSession)
        .where(UploadSession.file_hash == file_hash)
        .with_for_update()
    )
    session = result.scalar_one_or_none()
    if session is None:
        raise UploadError(404, "上传会话不存在，请先调用 init")

    if session.merged:
        record = await db.get(UploadFile, file_hash)
        path = record.path if record else (session.merged_path or "")
        return {
            "path": path,
            "size": record.size if record else session.file_size,
            "hash": file_hash,
            "deduplicated": False,
        }

    # DB 层缺失检查
    have = set(await get_uploaded_indexes(db, file_hash))
    missing = sorted(set(range(session.chunk_count)) - have)
    if missing:
        preview = missing[:10]
        raise UploadError(400, f"缺少分片: {preview}{' ...' if len(missing) > 10 else ''}")

    # 磁盘层缺失检查（DB 有记录但文件被清理）
    directory = chunks_dir(file_hash)
    absent = [i for i in range(session.chunk_count) if not (directory / str(i)).is_file()]
    if absent:
        await db.execute(
            sa_delete(UploadChunk).where(
                UploadChunk.file_hash == file_hash,
                UploadChunk.chunk_index.in_(absent),
            )
        )
        await db.commit()
        preview = absent[:10]
        raise UploadError(400, f"缺少分片: {preview}{' ...' if len(absent) > 10 else ''}")

    try:
        merging, actual = await to_thread.run_sync(
            _merge_sync, file_hash, session.chunk_count, session.total_hash
        )
        target, reused = await to_thread.run_sync(
            _resolve_target_sync, session.file_name, file_hash, merging
        )
        size = await to_thread.run_sync(_finalize_merge_sync, merging, target, reused)
    except UploadError as exc:
        if exc.status_code == 422:
            session.merged = False
            session.last_error = "hash mismatch"
            await db.commit()
        raise

    relative = target.relative_to(upload_root()).as_posix()
    await register_instant(
        db, file_hash=file_hash, path=relative,
        file_name=session.file_name, size=size,
    )
    session.merged = True
    session.merged_path = relative
    session.last_error = None
    # 合并成功即清理：分片行与分片目录不再需要（秒传索引在 upload_files 表中，不受影响）
    await db.execute(sa_delete(UploadChunk).where(UploadChunk.file_hash == file_hash))
    await db.commit()
    await to_thread.run_sync(
        partial(shutil.rmtree, chunks_dir(file_hash), ignore_errors=True)
    )
    log.info(f"合并完成：{file_hash} -> {relative}（deduplicated={reused}，分片已清理）")
    return {"path": relative, "size": size, "hash": actual, "deduplicated": reused}


# ---------------------------------------------------------------------------
# 下载
# ---------------------------------------------------------------------------


async def resolve_download(db: AsyncSession, file_hash: str) -> tuple[Path, str]:
    """返回 (文件绝对路径, 下载文件名)。优先查秒传索引，其次查已合并会话。"""
    file_hash = _validate_hash(file_hash, "fileHash")
    record = await db.get(UploadFile, file_hash)
    if record is not None:
        path = upload_root() / record.path
        if path.is_file():
            return path, record.file_name
        await _drop_stale_index(db, record)
    session = await db.get(UploadSession, file_hash)
    if session is not None and session.merged and session.merged_path:
        path = upload_root() / session.merged_path
        if path.is_file():
            return path, session.file_name
    raise UploadError(404, "文件不存在或尚未上传完成")


# ---------------------------------------------------------------------------
# 过期清理
# ---------------------------------------------------------------------------


async def cleanup_expired_sessions(db: AsyncSession) -> int:
    """清理过期上传会话的分片目录。

    - 未合并且超过过期时间的会话：删 DB 行（分片行级联）+ 删分片目录；
    - 已合并会话：分片目录与分片行不再需要，一并删除（秒传索引不受影响）。

    返回本次删除的过期会话数。
    """
    now = datetime.now(timezone.utc)
    expire_before = now - timedelta(hours=settings.UPLOAD_SESSION_EXPIRE_HOURS)
    stale_dirs: list[Path] = []
    removed = 0

    expired = await db.execute(
        select(UploadSession.file_hash).where(
            UploadSession.merged.is_(False),
            UploadSession.created_at < expire_before,
        )
    )
    for file_hash in expired.scalars():
        await db.execute(sa_delete(UploadChunk).where(UploadChunk.file_hash == file_hash))
        await db.execute(sa_delete(UploadSession).where(UploadSession.file_hash == file_hash))
        stale_dirs.append(chunks_dir(file_hash))
        removed += 1

    merged = await db.execute(
        select(UploadSession.file_hash).where(UploadSession.merged.is_(True))
    )
    for file_hash in merged.scalars():
        await db.execute(sa_delete(UploadChunk).where(UploadChunk.file_hash == file_hash))
        stale_dirs.append(chunks_dir(file_hash))

    await db.commit()

    for directory in stale_dirs:
        if directory.exists():
            await to_thread.run_sync(
                partial(shutil.rmtree, directory, ignore_errors=True)
            )
    if stale_dirs:
        log.info(f"上传清理：移除 {len(stale_dirs)} 个分片目录，删除过期会话 {removed} 个")
    return removed
