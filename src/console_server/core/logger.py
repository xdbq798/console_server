####################################
# LOGGING (loguru)
####################################
# 统一日志入口：
# 1. InterceptHandler 把标准库 logging 的输出转发给 loguru，
#    uvicorn / apscheduler / sqlalchemy 等第三方库日志一并收编。
# 2. setup_logging() 必须在任何第三方库被导入前调用（main.py 顶部）。

import logging
import os
import sys

from loguru import logger

# 保留 env.py 的按模块等级机制：全局默认等级
DEFAULT_LOG_LEVEL = "INFO"

# 与 env.py 的 log_sources 保持一致（小写 key，用于 logger.bind）
LOG_SOURCES = ("config", "db", "main", "socket", "oauth")


def _module_level(source: str) -> int:
    """读取 XXX_LOG_LEVEL 环境变量，无效则回退全局等级"""
    level = os.environ.get(f"{source.upper()}_LOG_LEVEL", "").upper()
    if level in logging.getLevelNamesMapping():
        return logging.getLevelNamesMapping()[level]
    return logging.getLevelNamesMapping()[DEFAULT_LOG_LEVEL]


def _module_filter(record):
    """对 bind 了 log_source 的日志，按对应模块等级过滤"""
    source = record["extra"].get("log_source")
    if source is None:
        return True
    return record["level"].no >= _module_level(source)


def module_logger(source: str):
    """获取带模块标记的 logger，等级由 {SOURCE}_LOG_LEVEL 环境变量控制"""
    return logger.bind(log_source=source.lower())

_DEV_FORMAT = (
    "<green>{time:HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
    "<level>{message}</level>"
)


class InterceptHandler(logging.Handler):
    """标准库 logging -> loguru 桥接器"""

    def emit(self, record: logging.LogRecord):
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        # 计算跳帧深度：emit 自身 1 帧 + 调用链上所有 stdlib logging 内部帧，
        # 让日志定位到真实调用处（如 uvicorn/apscheduler 的调用代码）
        depth = 1
        frame = logging.currentframe().f_back if logging.currentframe() else None
        while frame is not None:
            if frame.f_code.co_filename == logging.__file__:
                depth += 1
            frame = frame.f_back

        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


def setup_logging():
    """初始化日志系统。应在程序最早入口调用（所有本地/第三方模块导入之前）。"""
    global_level = os.environ.get("GLOBAL_LOG_LEVEL", "").upper()
    if global_level not in logging.getLevelNamesMapping():
        global_level = DEFAULT_LOG_LEVEL

    # 1) 收编标准库 logging：根 logger 换上拦截器，放行所有日志，
    #    由各模块自己的 setLevel 与 loguru sink 过滤。
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

    # uvicorn 有独立的 logger 体系，单独接管
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        uv_logger = logging.getLogger(name)
        uv_logger.handlers = [InterceptHandler()]
        uv_logger.propagate = False

    # 2) 配置 loguru 输出
    logger.remove()
    log_format = os.environ.get("LOG_FORMAT", "").lower()
    colorize = not (os.getenv("NO_COLOR") or os.getenv("RUNNING_IN_DOCKER"))
    if log_format == "json":
        # 生产/集中采集场景：结构化 JSON
        logger.add(sys.stderr, level=global_level, filter=_module_filter, serialize=True)
    else:
        # 开发场景：彩色文本
        logger.add(
            sys.stderr,
            level=global_level,
            filter=_module_filter,
            format=_DEV_FORMAT,
            colorize=colorize,
        )

    return logger
