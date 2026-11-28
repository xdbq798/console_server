"""断点续传 + 秒传服务层回归：完整上传、秒传、校验失败、消歧、清理。

依赖 PostgreSQL（pg upsert 语法），未配置 CONSOLE_TEST_DATABASE_URL 时跳过，
与 test_auth_tokens.py 的 PostgreSQL 用例约定一致。
"""

import asyncio
import hashlib
import math
import os
import shutil
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from sqlalchemy import delete as sa_delete
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from console_server.core.config import settings
from console_server.model.common import Base
from console_server.model.upload import UploadChunk, UploadFile, UploadSession
from console_server.services import upload as upload_service
from console_server.services.upload import UploadError

CHUNK = 64 * 1024


@unittest.skipUnless(
    os.getenv("CONSOLE_TEST_DATABASE_URL"), "未配置独立 PostgreSQL 测试连接"
)
class UploadServiceTests(unittest.IsolatedAsyncioTestCase):
    # asyncpg 连接与事件循环绑定，engine 必须每个用例独立创建（IsolatedAsyncioTestCase 每用例新循环）

    async def asyncSetUp(self):
        self.engine = create_async_engine(
            os.environ["CONSOLE_TEST_DATABASE_URL"], echo=False
        )
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            # 只清上传相关表，不影响其他用例数据
            for table in ("upload_chunks", "upload_sessions", "upload_files"):
                await conn.exec_driver_sql(f"DELETE FROM {table}")
        self.storage = tempfile.mkdtemp(prefix="console_upload_test_")
        settings.UPLOAD_DIR = self.storage
        upload_service.ensure_upload_dirs()

    async def asyncTearDown(self):
        await self.engine.dispose()
        shutil.rmtree(self.storage, ignore_errors=True)

    def session(self) -> AsyncSession:
        return AsyncSession(self.engine, expire_on_commit=False)

    async def upload_bytes(self, content: bytes, name: str):
        """走完整 init → chunk × N → merge 流程，返回 (file_hash, result)。"""
        file_hash = hashlib.md5(content).hexdigest()
        chunk_count = math.ceil(len(content) / CHUNK)
        async with self.session() as db:
            init = await upload_service.init_upload(
                db,
                file_hash=file_hash,
                file_name=name,
                file_size=len(content),
                chunk_size=CHUNK,
                chunk_count=chunk_count,
                total_hash=file_hash,
            )
            self.assertFalse(init["hit"])
            for index in range(chunk_count):
                data = content[index * CHUNK : (index + 1) * CHUNK]
                await upload_service.receive_chunk(
                    db,
                    file_hash=file_hash,
                    index=index,
                    chunk_hash=hashlib.md5(data).hexdigest(),
                    data=data,
                )
            result = await upload_service.merge_upload(db, file_hash)
        return file_hash, result

    # ------------------------------------------------------------------
    # 完整上传 + 秒传
    # ------------------------------------------------------------------

    async def test_full_upload_then_instant_hit(self):
        content = os.urandom(CHUNK * 3 + 17)
        file_hash, result = await self.upload_bytes(content, "big.bin")
        self.assertFalse(result["deduplicated"])
        self.assertEqual(result["hash"], file_hash)

        # 落盘文件内容一致
        path = upload_service.upload_root() / result["path"]
        self.assertEqual(path.read_bytes(), content)

        # 再次 init 同一内容 → 秒传命中，无需任何分片
        async with self.session() as db:
            init = await upload_service.init_upload(
                db,
                file_hash=file_hash,
                file_name="big.bin",
                file_size=len(content),
                chunk_size=CHUNK,
                chunk_count=math.ceil(len(content) / CHUNK),
                total_hash=file_hash,
            )
        self.assertTrue(init["hit"])
        self.assertEqual(init["file_path"], result["path"])

        # merge 后分片行已清理，check 返回空
        async with self.session() as db:
            uploaded = await upload_service.check_uploaded(db, file_hash)
        self.assertEqual(uploaded, [])

    # ------------------------------------------------------------------
    # 校验失败
    # ------------------------------------------------------------------

    async def test_chunk_hash_mismatch_rejected(self):
        content = b"0123456789"  # 10 字节，与声明的 file_size 一致
        file_hash = hashlib.md5(content).hexdigest()
        async with self.session() as db:
            await upload_service.init_upload(
                db,
                file_hash=file_hash,
                file_name="a.bin",
                file_size=len(content),
                chunk_size=CHUNK,
                chunk_count=1,
                total_hash=file_hash,
            )
            with self.assertRaises(UploadError) as ctx:
                await upload_service.receive_chunk(
                    db,
                    file_hash=file_hash,
                    index=0,
                    # 大小一致但内容哈希对不上
                    chunk_hash=hashlib.md5(b"tampered-data").hexdigest(),
                    data=content,
                )
        self.assertEqual(ctx.exception.status_code, 422)
        # 失败分片不落盘
        self.assertFalse((upload_service.chunks_dir(file_hash) / "0").exists())

    async def test_merge_with_missing_chunk_rejected(self):
        content = os.urandom(CHUNK * 2)
        file_hash = hashlib.md5(content).hexdigest()
        async with self.session() as db:
            await upload_service.init_upload(
                db,
                file_hash=file_hash,
                file_name="partial.bin",
                file_size=len(content),
                chunk_size=CHUNK,
                chunk_count=2,
                total_hash=file_hash,
            )
            data = content[:CHUNK]
            await upload_service.receive_chunk(
                db,
                file_hash=file_hash,
                index=0,
                chunk_hash=hashlib.md5(data).hexdigest(),
                data=data,
            )
            with self.assertRaises(UploadError) as ctx:
                await upload_service.merge_upload(db, file_hash)
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn("缺少分片", ctx.exception.detail)

    async def test_metadata_conflict_rejected(self):
        content = os.urandom(CHUNK)
        file_hash = hashlib.md5(content).hexdigest()
        async with self.session() as db:
            await upload_service.init_upload(
                db,
                file_hash=file_hash,
                file_name="a.bin",
                file_size=len(content),
                chunk_size=CHUNK,
                chunk_count=1,
                total_hash=file_hash,
            )
            with self.assertRaises(UploadError) as ctx:
                await upload_service.init_upload(
                    db,
                    file_hash=file_hash,
                    file_name="a.bin",
                    file_size=len(content),
                    chunk_size=CHUNK * 2,
                    chunk_count=1,
                    total_hash=file_hash,
                )
        self.assertEqual(ctx.exception.status_code, 409)

    # ------------------------------------------------------------------
    # 同名不同内容消歧
    # ------------------------------------------------------------------

    async def test_same_name_different_content_disambiguated(self):
        content_a = os.urandom(CHUNK)
        content_b = os.urandom(CHUNK)
        hash_a, result_a = await self.upload_bytes(content_a, "dup.bin")
        hash_b, result_b = await self.upload_bytes(content_b, "dup.bin")

        self.assertNotEqual(result_a["path"], result_b["path"])
        self.assertIn(f"_{hash_b[:8]}", result_b["path"])
        # A 的秒传仍指向原内容
        path_a = upload_service.upload_root() / result_a["path"]
        self.assertEqual(path_a.read_bytes(), content_a)

    async def test_same_name_same_content_deduplicated(self):
        content = os.urandom(CHUNK)
        _, result_first = await self.upload_bytes(content, "same.bin")
        # 摘掉秒传索引并重置会话（模拟索引与会话状态双双丢失），merge 应复用同名同内容文件
        async with self.session() as db:
            await db.execute(sa_delete(UploadFile))
            await db.execute(sa_delete(UploadChunk))
            await db.execute(sa_delete(UploadSession))
            await db.commit()
        _, result_second = await self.upload_bytes(content, "same.bin")
        self.assertTrue(result_second["deduplicated"])
        self.assertEqual(result_first["path"], result_second["path"])

    # ------------------------------------------------------------------
    # 过期清理
    # ------------------------------------------------------------------

    async def test_cleanup_expires_unmerged_and_keeps_instant(self):
        content = os.urandom(CHUNK)
        file_hash, result = await self.upload_bytes(content, "keep.bin")
        # merge 成功后分片目录应被立即清理
        self.assertFalse(upload_service.chunks_dir(file_hash).exists())

        # 伪造一个过期未合并会话
        stale_hash = hashlib.md5(b"stale").hexdigest()
        async with self.session() as db:
            db.add(
                UploadSession(
                    file_hash=stale_hash,
                    file_name="stale.bin",
                    file_size=CHUNK,
                    chunk_size=CHUNK,
                    chunk_count=1,
                    total_hash=stale_hash,
                    created_at=datetime.now(timezone.utc)
                    - timedelta(hours=settings.UPLOAD_SESSION_EXPIRE_HOURS + 1),
                )
            )
            await db.commit()
        stale_dir = upload_service.chunks_dir(stale_hash)
        stale_dir.mkdir(parents=True, exist_ok=True)

        async with self.session() as db:
            removed = await upload_service.cleanup_expired_sessions(db)
        self.assertEqual(removed, 1)
        self.assertFalse(stale_dir.exists())
        # 秒传索引与文件不受清理影响
        async with self.session() as db:
            record = await db.get(UploadFile, file_hash)
        self.assertIsNotNone(record)
        self.assertTrue((upload_service.upload_root() / record.path).exists())

    # ------------------------------------------------------------------
    # 脏索引摘除
    # ------------------------------------------------------------------

    async def test_stale_index_dropped_when_file_missing(self):
        content = os.urandom(CHUNK)
        file_hash, result = await self.upload_bytes(content, "gone.bin")
        (upload_service.upload_root() / result["path"]).unlink()

        async with self.session() as db:
            init = await upload_service.init_upload(
                db,
                file_hash=file_hash,
                file_name="gone.bin",
                file_size=len(content),
                chunk_size=CHUNK,
                chunk_count=1,
                total_hash=file_hash,
            )
        # 文件被删后索引摘除，回落为普通上传
        self.assertFalse(init["hit"])


if __name__ == "__main__":
    unittest.main()
