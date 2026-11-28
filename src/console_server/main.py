# ✅ 先导入标准库和第三方库
import os

# 日志初始化必须在所有本地/第三方模块导入之前调用，
# 否则 uvicorn、apscheduler 等库的日志不会被拦截收编。
from console_server.core.logger import setup_logging

setup_logging()

from loguru import logger
from console_server.core.config import settings
from console_server.middleware.auth import AuthMiddleware
from console_server.middleware.request_logging import RequestLoggingMiddleware

env = settings.ENV
_debug = settings.DEBUG

log = logger.bind(log_source="main")

log.info(f"版本: {settings.APP_VERSION}")
log.info(f"环境: {env}")
log.info(f"DEBUG: {_debug}")

from fastapi import FastAPI
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

# ✅ 第二步：导入本地模块（必须放在前面）
from socketio import ASGIApp as SocketASGIApp

from .db import database
from .core import redis as redis_module
from .api.router import router
from .services import upload as upload_service
from .services.upload import ensure_upload_dirs
from .utils import auth
from .ws.server import sio
from .ws import events  # noqa: F401  导入即注册 connect/disconnect 等事件处理器


# 创建全局调度器
scheduler = AsyncIOScheduler()


async def cleanup_expired_tokens_task():
    """定时清理过期 token 的任务"""
    try:
        async with database.AsyncSessionLocal() as db:
            deleted_count = await auth.cleanup_expired_tokens(db)
            if deleted_count > 0:
                log.info(f"定时任务：清理了 {deleted_count} 个过期 token")
            else:
                log.debug("定时任务：没有需要清理的过期 token")
    except Exception as e:
        log.exception(f"定时任务执行失败：{e}")


async def cleanup_upload_sessions_task():
    """定时清理过期上传会话与已合并会话的分片目录（秒传索引不受影响）"""
    try:
        async with database.AsyncSessionLocal() as db:
            removed = await upload_service.cleanup_expired_sessions(db)
            if removed > 0:
                log.info(f"定时任务：清理了 {removed} 个过期上传会话")
            else:
                log.debug("定时任务：没有需要清理的过期上传会话")
    except Exception as e:
        log.exception(f"上传会话清理任务执行失败：{e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.success("✅ 应用启动中")
    if env == "dev":
        log.warning("请执行 sql/init.sql 初始化数据库")

    # Redis：启动时验证连通性；失败降级为数据库路径，不阻塞启动
    if await redis_module.check_redis():
        log.success(f"✅ Redis 已连接：{redis_module.REDIS_URL}")
    elif redis_module.REDIS_URL:
        log.warning("Redis 不可用，token 黑名单等业务降级为数据库路径")
    else:
        log.info("Redis 未配置，已禁用")

    # 启动定时任务
    log.success("✅ 应用启动：启动定时任务")
    scheduler.add_job(
        cleanup_expired_tokens_task,
        trigger=IntervalTrigger(hours=settings.CLEANUP_EXPIRED_TOKENS_INTERVAL_HOURS),
        id="cleanup_expired_tokens",
        name="清理过期 token",
        replace_existing=True,
    )
    scheduler.add_job(
        cleanup_upload_sessions_task,
        trigger=IntervalTrigger(hours=settings.UPLOAD_CLEANUP_INTERVAL_HOURS),
        id="cleanup_upload_sessions",
        name="清理过期上传会话",
        replace_existing=True,
    )
    scheduler.start()
    log.info(
        f"✅ 定时任务已启动：每 {settings.CLEANUP_EXPIRED_TOKENS_INTERVAL_HOURS} 小时执行一次 token 清理，"
        f"每 {settings.UPLOAD_CLEANUP_INTERVAL_HOURS} 小时执行一次上传会话清理"
    )

    # 上传存储目录 + 启动时执行一次过期清理（对应原方案的启动 sweep）
    ensure_upload_dirs()
    await cleanup_upload_sessions_task()

    yield

    # 关闭调度器
    log.info("应用关闭：停止定时任务")
    scheduler.shutdown(wait=False)
    await redis_module.close_redis()
    log.info("应用关闭：Redis 连接已释放")


# ✅ 定义 FastAPI 应用
app = FastAPI(
    debug=_debug,
    lifespan=lifespan,
    docs_url="/docs" if _debug else None,
    redoc_url="/redoc" if _debug else None,
    openapi_url="/api/openapi.json" if _debug else None,
)

# 添加中间件（后注册的先执行：请求日志 → 认证 → 业务）
app.add_middleware(AuthMiddleware)
app.add_middleware(RequestLoggingMiddleware)

app.include_router(router, prefix=settings.API_STR)

# WebSocket：用 Socket.IO 的 ASGIApp 包裹整个应用并作为 uvicorn 入口
# （scripts/start.py 启动 console_server.main:socket_app）。
# /{WS_PATH} 路径由 Socket.IO 处理，其余请求转发给 app；
# Socket.IO 握手不经过 AuthMiddleware，认证在 connect 事件中独立完成。
socket_app = SocketASGIApp(sio, other_asgi_app=app, socketio_path=settings.WS_PATH)
log.success(f"✅ WebSocket 已就绪：/{settings.WS_PATH}")
