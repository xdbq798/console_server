"""Redis 异步客户端封装。

- 连接池 + decode_responses，对齐 db/database.py 的模块级单例风格。
- REDIS_URL 为空表示禁用；连接失败或运行期异常均降级为不可用，
  业务侧（token 黑名单等）自动回退到数据库路径，不引入新的单点故障。
"""

import os

import redis.asyncio as aioredis
from redis.exceptions import RedisError
from loguru import logger

from console_server.core.config import settings
from console_server.utils.console import print_info

log = logger.bind(log_source="main")

# 键规范：统一 console: 前缀
TOKEN_BLACKLIST_PREFIX = "console:token:bl:"

# 支持环境变量覆盖，便于 Docker 部署（与 db/database.py 的模式一致）
REDIS_URL = os.getenv("REDIS_URL", settings.REDIS_URL).strip()

# 模块级客户端；None 表示禁用，available 表示当前可用
redis_client: aioredis.Redis | None = None
redis_available: bool = False


def _create_client() -> aioredis.Redis | None:
    if not REDIS_URL:
        print_info("Redis 未配置（REDIS_URL 为空），已禁用，业务将使用数据库路径")
        return None
    return aioredis.from_url(
        REDIS_URL,
        decode_responses=True,
        # 连接池参数：池耗尽快速失败，避免请求堆积
        max_connections=50,
        socket_connect_timeout=2,
        socket_timeout=2,
        health_check_interval=30,
    )


async def check_redis() -> bool:
    """PING 验证连通性，更新全局可用状态。"""
    global redis_available
    if redis_client is None:
        redis_available = False
        return False
    try:
        await redis_client.ping()
        redis_available = True
        return True
    except RedisError as e:
        redis_available = False
        log.warning(f"Redis 连接失败，业务降级为数据库路径：{e}")
        return False


async def close_redis() -> None:
    global redis_client, redis_available
    if redis_client is not None:
        await redis_client.aclose()
        redis_client = None
    redis_available = False


def get_redis() -> aioredis.Redis | None:
    """FastAPI 依赖：返回可用客户端，不可用时返回 None（业务自行降级）。"""
    return redis_client if redis_available else None


def token_blacklist_key(token_hash: str) -> str:
    return f"{TOKEN_BLACKLIST_PREFIX}{token_hash}"


async def cache_token_blacklist(token_hash: str, expires_at) -> bool:
    """将黑名单 token 写入 Redis，TTL 取剩余有效期（秒，最少 1 秒）。

    expires_at 为带时区 datetime；失败仅告警，不影响数据库写入结果。
    """
    client = get_redis()
    if client is None:
        return False
    from datetime import datetime, timezone

    remaining = (expires_at - datetime.now(timezone.utc)).total_seconds()
    if remaining <= 0:
        return False
    try:
        await client.set(token_blacklist_key(token_hash), "1", ex=max(1, int(remaining)))
        return True
    except RedisError as e:
        log.warning(f"Redis 写入黑名单失败（不影响数据库写入）：{e}")
        return False


async def is_token_blacklisted_cached(token_hash: str) -> bool:
    """Redis 快速路径查询。Redis 不可用或键不存在时返回 False（回退数据库）。

    注意：Redis 未命中不代表未拉黑，调用方必须继续走数据库校验；
    本函数只用于「命中即快速拒绝」。
    """
    client = get_redis()
    if client is None:
        return False
    try:
        return await client.exists(token_blacklist_key(token_hash)) == 1
    except RedisError as e:
        log.warning(f"Redis 查询黑名单失败，回退数据库：{e}")
        return False


# 模块加载时创建客户端，连通性由 lifespan 启动时验证
redis_client = _create_client()
