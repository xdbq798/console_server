"""服务端推送封装。

业务代码统一通过本模块推送，不直接操作 sio：
- push_to_user(user_id, event, data)  : 推送给某个在线用户（可能多端）
- broadcast(event, data)              : 推送给所有在线用户

推送失败只记录日志，不向业务抛异常——推送是尽力而为的旁路通知，
不应让 HTTP 请求因此失败（与 Redis 降级策略一致）。
"""

from loguru import logger

from console_server.ws.auth import user_room
from console_server.ws.server import sio, log_source

log = logger.bind(log_source=log_source)


async def push_to_user(user_id: str, event: str, data: dict) -> None:
    """按用户 UUID 推送；未在线则投递无消费方，安全无副作用。"""
    try:
        await sio.emit(event, data, room=user_room(user_id))
    except Exception as e:
        log.warning(f"WS 推送失败（user={user_id} event={event}）：{e}")


async def broadcast(event: str, data: dict) -> None:
    try:
        await sio.emit(event, data)
    except Exception as e:
        log.warning(f"WS 全员推送失败（event={event}）：{e}")
