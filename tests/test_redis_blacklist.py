"""token 黑名单 Redis 快速路径测试。

- 降级路径：Redis 不可用时查询返回 False，业务回退数据库（始终运行）。
- 真实路径：配置 CONSOLE_TEST_REDIS_URL 时验证写穿透与命中拒绝。
"""

import io
import os
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone

from fastapi import HTTPException

with redirect_stdout(io.StringIO()):
    from console_server.core import redis as redis_module
    from console_server.utils.auth import add_token_to_blacklist, is_token_blacklisted

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.schema import CreateSchema, DropSchema
from uuid import uuid4

from console_server.model.common import Base
from console_server.model.rbac import User
from console_server.model.token import TokenBlacklist


def _fake_token(payload_suffix: str = "a") -> str:
    # 黑名单按 token 哈希存取，本测试不需要合法 JWT，只要字符串唯一即可
    return f"fake-token-{payload_suffix}-{uuid4().hex}"


@unittest.skipUnless(os.getenv('CONSOLE_TEST_DATABASE_URL'), '未配置 PostgreSQL 测试连接')
class BlacklistDegradationTests(unittest.IsolatedAsyncioTestCase):
    """Redis 不可用时，黑名单功能完全依赖数据库路径。"""

    async def asyncSetUp(self):
        self.schema = 'redis_test_' + uuid4().hex
        self.engine = create_async_engine(os.environ['CONSOLE_TEST_DATABASE_URL'], echo=False, hide_parameters=True)
        async with self.engine.begin() as conn:
            await conn.execute(CreateSchema(self.schema))
        translated = self.engine.execution_options(schema_translate_map={None: self.schema})
        async with translated.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        self.sessions = async_sessionmaker(translated, expire_on_commit=False)
        self._old_client = redis_module.redis_client
        self._old_available = redis_module.redis_available
        redis_module.redis_client = None
        redis_module.redis_available = False

    async def asyncTearDown(self):
        async with self.engine.begin() as conn:
            await conn.execute(DropSchema(self.schema, cascade=True))
        await self.engine.dispose()
        redis_module.redis_client = self._old_client
        redis_module.redis_available = self._old_available

    async def test_add_and_check_blacklist_without_redis(self):
        token = _fake_token()
        async with self.sessions() as db:
            await add_token_to_blacklist(token, db)
            self.assertTrue(await is_token_blacklisted(token, db))

    async def test_redis_unavailable_never_blocks(self):
        # 客户端存在但标记不可用时，快速路径返回 False，不抛异常
        redis_module.redis_client = object()
        redis_module.redis_available = False
        self.assertFalse(await redis_module.is_token_blacklisted_cached("any-hash"))


@unittest.skipUnless(os.getenv('CONSOLE_TEST_REDIS_URL'), '未配置 Redis 测试连接')
class RedisBlacklistPathTests(unittest.IsolatedAsyncioTestCase):
    """真实 Redis：写穿透、命中快速拒绝、TTL 自动过期。"""

    async def asyncSetUp(self):
        self._old_client = redis_module.redis_client
        self._old_url = redis_module.REDIS_URL
        self._old_available = redis_module.redis_available
        redis_module.REDIS_URL = os.environ['CONSOLE_TEST_REDIS_URL']
        redis_module.redis_client = redis_module._create_client()
        if not await redis_module.check_redis():
            self.skipTest('Redis 不可达')

    async def asyncTearDown(self):
        if redis_module.redis_client is not None:
            await redis_module.redis_client.aclose()
        redis_module.redis_client = self._old_client
        redis_module.REDIS_URL = self._old_url
        redis_module.redis_available = self._old_available

    async def test_write_through_then_fast_hit(self):
        token = _fake_token()
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=5)
        token_hash = __import__('hashlib').sha256(token.encode()).hexdigest()
        await redis_module.cache_token_blacklist(token_hash, expires_at)
        self.assertTrue(await redis_module.is_token_blacklisted_cached(token_hash))
        # TTL 至少 1 秒且不超过剩余时间
        ttl = await redis_module.redis_client.ttl(redis_module.token_blacklist_key(token_hash))
        self.assertGreater(ttl, 0)
        self.assertLessEqual(ttl, 300)

    async def test_expired_entry_disappears(self):
        token_hash = uuid4().hex
        past = datetime.now(timezone.utc) + timedelta(seconds=-1)
        # 剩余时间 <= 0 时不写入
        self.assertFalse(await redis_module.cache_token_blacklist(token_hash, past))
        self.assertFalse(await redis_module.is_token_blacklisted_cached(token_hash))


if __name__ == '__main__':
    unittest.main()
