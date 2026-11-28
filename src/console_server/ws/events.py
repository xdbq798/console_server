"""Socket.IO 事件注册：connect / disconnect 与业务事件。

- connect 返回 False 即拒绝连接（客户端收到 ConnectionError）。
- 认证通过后登记 user_id 并加入 user:{id} 房间，服务端推送按房间投递。
- 业务事件示例：客户端可发送 "ping" 探活；后续聊天/通知事件在此注册。
"""

from loguru import logger

from console_server.ws.auth import (
    WS_SESSION_USER_KEY,
    authenticate_socket,
    extract_handshake_token,
    user_room,
)
from console_server.ws.server import sio, log_source

log = logger.bind(log_source=log_source)


@sio.on("connect")
async def on_connect(sid: str, environ: dict, auth: dict | None = None):
    try:
        token = extract_handshake_token(environ, auth)
        if not token:
            log.warning(f"WS 连接被拒绝（缺少 token）：sid={sid}")
            return False
        user = await authenticate_socket(token)
        if user is None:
            log.warning(f"WS 连接被拒绝（认证失败）：sid={sid}")
            return False
        user_id = str(user.uuid)
        await sio.save_session(sid, {WS_SESSION_USER_KEY: user_id})
        await sio.enter_room(sid, user_room(user_id))
        await sio.emit("connected", {"message": "WebSocket 已连接"}, to=sid)
        log.info(f"WS 已连接：sid={sid} user={user_id}")
    except Exception:
        # 连接登记环节的任何异常都按拒绝处理，避免留下未认证的悬挂连接
        log.exception(f"WS 连接处理异常：sid={sid}")
        return False


@sio.on("disconnect")
async def on_disconnect(sid: str):
    log.info(f"WS 已断开：sid={sid}")


@sio.on("ping")
async def on_ping(sid: str):
    await sio.emit("pong", to=sid)


# 在此注册业务事件，例如：
# @sio.on("chat_subscribe")
# async def on_chat_subscribe(sid, data): ...
