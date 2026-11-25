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
from .db import database
from .core import redis as redis_module
from .api.router import router
from .utils import auth


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
    scheduler.start()
    log.info(
        f"✅ 定时任务已启动：每 {settings.CLEANUP_EXPIRED_TOKENS_INTERVAL_HOURS} 小时执行一次清理"
    )

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
