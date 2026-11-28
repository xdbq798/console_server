"""WebSocket（Socket.IO）握手认证与推送测试。

- 单元层：token 提取与 authenticate_socket（内存 SQLite + 同步会话适配）。
- 全链路：进程内 uvicorn + socketio.AsyncClient，验证真实握手、
  拒绝路径与按用户推送。

运行：PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_ws.py'
"""

import asyncio
import contextlib
import io
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from uuid import UUID, uuid4

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

with redirect_stdout(io.StringIO()):
    import socketio
    import uvicorn
    from console_server.db import database
    from console_server.model.auth_session import AuthSession
    from console_server.model.common import Base
    from console_server.model.rbac import User
    from console_server.utils.auth import get_password_hash, issue_session_tokens
    from console_server.ws import manager
    from console_server.ws.auth import (
        authenticate_socket,
        extract_handshake_token,
        user_room,
    )
    from console_server.ws.server import sio

import test_user_crud as user_tests  # AsyncSessionAdapter

PASSWORD_HASH = get_password_hash("password123")


class _PatchedSessionFactory:
    """把同步 SQLite 会话包装成 ws/auth 中 `async with AsyncSessionLocal()` 可用的替身。"""

    def __init__(self, session: Session):
        self._session = session

    async def __aenter__(self):
        return user_tests.AsyncSessionAdapter(self._session)

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is None:
            self._session.commit()
        else:
            self._session.rollback()
        return False


def _make_user(db: Session, *, name: str = "ws_user", email: str = "ws@example.com") -> User:
    user = User(name=name, email=email, password=PASSWORD_HASH, is_active=True, roles=[])
    db.add(user)
    db.commit()
    return user


class ExtractTokenTests(unittest.TestCase):
    """握手 token 提取的优先级与容错。"""

    def test_from_auth_payload(self):
        environ = {"QUERY_STRING": b"token=from-query"}
        self.assertEqual(extract_handshake_token(environ, {"token": "from-auth"}), "from-auth")

    def test_from_query_string(self):
        environ = {"QUERY_STRING": b"EIO=4&transport=polling&token=abc"}
        self.assertEqual(extract_handshake_token(environ, None), "abc")

    def test_from_authorization_header(self):
        environ = {"HTTP_AUTHORIZATION": "Bearer header-token"}
        self.assertEqual(extract_handshake_token(environ, None), "header-token")

    def test_missing_token(self):
        self.assertIsNone(extract_handshake_token({}, None))
        self.assertIsNone(extract_handshake_token({}, {"token": "   "}))
        self.assertIsNone(extract_handshake_token({"HTTP_AUTHORIZATION": "Basic xxx"}, None))


class AuthenticateSocketTests(unittest.IsolatedAsyncioTestCase):
    """认证通过的条件与拒绝路径；完全离线，不依赖网络。"""

    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        with Session(self.engine) as db:
            user = _make_user(db)
            self.user_uuid = str(user.uuid)
            auth_session = AuthSession(
                id=uuid4(), user_uuid=user.uuid,
                expires_at=datetime.now(timezone.utc) + timedelta(days=30),
            )
            self.token = issue_session_tokens(user, auth_session)["access_token"]
            db.add(auth_session)
            db.commit()
        self.db_session = Session(self.engine, expire_on_commit=False)

        def session_factory():
            return _PatchedSessionFactory(self.db_session)

        patcher = patch.object(database, "AsyncSessionLocal", session_factory)
        patcher.start()
        self.addCleanup(patcher.stop)
        # 用内存 manager 替换，避免测试依赖 Redis；必须重新绑定 server 引用
        self._old_manager = sio.manager
        sio.manager = socketio.AsyncManager()
        sio.manager.set_server(sio)
        self.addCleanup(self._restore_manager)

    def _restore_manager(self):
        sio.manager = self._old_manager

    async def test_valid_token_registers_session_and_room(self):
        user = await authenticate_socket(self.token)
        self.assertIsNotNone(user)
        self.assertEqual(str(user.uuid), self.user_uuid)

    async def test_garbage_token_rejected(self):
        self.assertIsNone(await authenticate_socket("not-a-jwt"))

    async def test_token_of_other_use_rejected(self):
        # refresh token 不能用于 WS 握手
        with Session(self.engine) as db:
            user = db.query(User).filter(User.uuid == UUID(self.user_uuid)).one()
            auth_session = AuthSession(
                id=uuid4(), user_uuid=user.uuid,
                expires_at=datetime.now(timezone.utc) + timedelta(days=30),
            )
            refresh = issue_session_tokens(user, auth_session)["refresh_token"]
            db.add(auth_session)
            db.commit()
        self.assertIsNone(await authenticate_socket(refresh))

    async def test_blacklisted_token_rejected(self):
        from console_server.utils.auth import add_token_to_blacklist
        adapter = user_tests.AsyncSessionAdapter(self.db_session)
        await add_token_to_blacklist(self.token, adapter)
        self.assertIsNone(await authenticate_socket(self.token))

    async def test_revoked_session_rejected(self):
        with Session(self.engine) as db:
            row = db.get(AuthSession, db.query(AuthSession).first().id)
            row.revoked_at = datetime.now(timezone.utc)
            db.commit()
        self.assertIsNone(await authenticate_socket(self.token))


class WsEndToEndTests(unittest.IsolatedAsyncioTestCase):
    """真实握手：进程内 uvicorn + socketio.AsyncClient。"""

    async def asyncSetUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        with Session(self.engine) as db:
            user = _make_user(db)
            self.user_uuid = str(user.uuid)
            auth_session = AuthSession(
                id=uuid4(), user_uuid=user.uuid,
                expires_at=datetime.now(timezone.utc) + timedelta(days=30),
            )
            self.token = issue_session_tokens(user, auth_session)["access_token"]
            db.add(auth_session)
            db.commit()

        self.db_session = Session(self.engine, expire_on_commit=False)

        def session_factory():
            return _PatchedSessionFactory(self.db_session)

        patcher = patch.object(database, "AsyncSessionLocal", session_factory)
        patcher.start()
        self.addCleanup(patcher.stop)
        self._old_manager = sio.manager
        sio.manager = socketio.AsyncManager()
        sio.manager.set_server(sio)
        self.addCleanup(self._restore_manager)

        from console_server.main import socket_app
        self.server = uvicorn.Server(
            uvicorn.Config(socket_app, host="127.0.0.1", port=0, log_level="critical")
        )
        self._serve_task = asyncio.create_task(self.server.serve())
        for _ in range(200):
            if self.server.started:
                break
            await asyncio.sleep(0.05)
        else:
            self.fail("uvicorn 未能在超时内启动")
        self.port = self.server.servers[0].sockets[0].getsockname()[1]

    def _restore_manager(self):
        sio.manager = self._old_manager

    async def asyncTearDown(self):
        self.server.should_exit = True
        try:
            await asyncio.wait_for(self._serve_task, timeout=10)
        except asyncio.TimeoutError:
            self._serve_task.cancel()
        self.engine.dispose()

    async def _connect(self, *, token=None):
        client = socketio.AsyncClient()
        received = asyncio.Queue()

        @client.on("connected")
        def _on_connected(data):
            received.put_nowait(data)

        kwargs = {"socketio_path": "socket.io", "wait_timeout": 5}
        if token is not None:
            kwargs["auth"] = {"token": token}
        await client.connect(f"http://127.0.0.1:{self.port}", **kwargs)
        return client, received

    async def test_connect_with_valid_token_receives_connected_event(self):
        client, received = await self._connect(token=self.token)
        try:
            message = await asyncio.wait_for(received.get(), timeout=5)
            self.assertIn("message", message)
        finally:
            await client.disconnect()

    async def test_connect_rejected_without_token(self):
        with self.assertRaises(socketio.exceptions.ConnectionError):
            await self._connect()

    async def test_connect_rejected_with_invalid_token(self):
        with self.assertRaises(socketio.exceptions.ConnectionError):
            await self._connect(token="not-a-jwt")

    async def test_push_to_user_reaches_connected_client(self):
        client, _ = await self._connect(token=self.token)
        received = asyncio.Queue()

        @client.on("notice")
        def _on_notice(data):
            received.put_nowait(data)

        try:
            await manager.push_to_user(self.user_uuid, "notice", {"text": "hello"})
            message = await asyncio.wait_for(received.get(), timeout=5)
            self.assertEqual(message["text"], "hello")
        finally:
            await client.disconnect()


if __name__ == "__main__":
    unittest.main()
