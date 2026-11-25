"""控制台输出辅助函数（已迁移到 loguru）。

保留 print_* 系列函数名作为过渡适配层，
调用方（db/database.py、api/auth.py、core/redis.py 等）无需改动；
新代码建议直接使用 loguru：

    from loguru import logger
    logger.info("...")
"""

from loguru import logger

# depth=1 让日志定位到 print_* 的真实调用方，而非本文件的适配函数
def print_error(message: str):
    """错误信息（红色）"""
    logger.opt(depth=1).error(message)


def print_warn(message: str):
    """警告信息（黄色）"""
    logger.opt(depth=1).warning(message)


def print_success(message: str):
    """成功信息（绿色）"""
    logger.opt(depth=1).success(message)


def print_info(message: str):
    """普通信息（蓝色）"""
    logger.opt(depth=1).info(message)
