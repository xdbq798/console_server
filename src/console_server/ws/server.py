"""Socket.IO 服务器实例与 ASGIApp。

- 多 worker 跨进程广播：配置 REDIS_URL 时启用 AsyncRedisManager，
  未配置则退化为单进程内广播（与 token 黑名单的降级策略一致）。
- Origin 白名单复用 AUTH_ALLOWED_ORIGINS；为空时允许所有来源——
  连接仍需合法 access token，跨站劫持无法通过认证。
"""

import os

from socketio import AsyncRedisManager, AsyncServer

from console_server.core.config import settings

log_source = "ws"


def _redis_url() -> str:
    # 与 core/redis.py 的优先级保持一致：环境变量优先于配置文件
    return os.getenv("REDIS_URL", settings.REDIS_URL).strip()


def _build_client_manager() -> AsyncRedisManager | None:
    url = _redis_url()
    if not url:
        return None
    return AsyncRedisManager(url, write_only=True)


sio = AsyncServer(
    async_mode="asgi",
    client_manager=_build_client_manager(),
    # 历史来源已在 Settings 中完成规范化
    cors_allowed_origins=settings.AUTH_ALLOWED_ORIGINS or "*",
    ping_interval=25,
    ping_timeout=20,
)
