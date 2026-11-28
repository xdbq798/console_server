"""WebSocket 握手认证。

浏览器发起的 Socket.IO 握手无法携带自定义 Authorization 头，
token 依次从以下位置提取：
1. 客户端 auth 负载：socketio.AsyncClient(url, auth={"token": ...})
2. 握手 URL 查询参数：?token=...
3. 原生 WebSocket 场景下的 Authorization 头

认证逻辑完全复用 HTTP 侧的 decode_token + 黑名单 + 实时会话校验，
确保停用用户、撤销会话、拉黑 token 的 WebSocket 连接同样立即失效。
"""

from urllib.parse import parse_qs

from console_server.db import database
from console_server.model.rbac import User
from console_server.utils.auth import _get_user_from_token

WS_SESSION_USER_KEY = "user_id"


def extract_handshake_token(environ: dict, auth: dict | None) -> str | None:
    """从握手负载、查询参数或 Authorization 头中提取 access token。"""
    if isinstance(auth, dict):
        token = auth.get("token")
        if isinstance(token, str) and token.strip():
            return token.strip()
    query_bytes = environ.get("QUERY_STRING")
    if query_bytes:
        values = parse_qs(query_bytes.decode("latin-1")).get("token")
        if values and values[0].strip():
            return values[0].strip()
    header = environ.get("HTTP_AUTHORIZATION", "")
    scheme, _, value = header.partition(" ")
    if scheme.lower() == "bearer" and value.strip():
        return value.strip()
    return None


async def authenticate_socket(token: str) -> User | None:
    """校验 access token：验签 + 黑名单 + 实时会话/停用状态。

    通过 database.AsyncSessionLocal 取连接，测试可整体替换该工厂。
    任何异常（无效凭证、撤销会话、数据库错误）一律返回 None，
    由调用方拒绝连接。
    """
    try:
        async with database.AsyncSessionLocal() as db:
            return await _get_user_from_token(token, db, expected_use="access")
    except Exception:
        return None


def user_room(user_id: str) -> str:
    """按用户划分的推送房间；sid 之外的服务端推送统一走 user:{id} 房间。"""
    return f"user:{user_id}"
