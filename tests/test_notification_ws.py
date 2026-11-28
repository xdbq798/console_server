"""系统通知全链路测试：HTTP 触发 self-test → push_to_user → Socket.IO 客户端收到。

进程内 uvicorn + 真实 socketio.AsyncClient；数据库用内存 SQLite
（同时替换 ws 握手用的 AsyncSessionLocal 和 HTTP 依赖 get_db）。

运行：PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_notification_ws.py'
"""

import asyncio
import io
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

with redirect_stdout(io.StringIO()):
    import httpx
    import socketio
    import uvicorn
    from console_server.db import database
    from console_server.model.auth_session import AuthSession
    from console_server.model.common import Base
    from console_server.model.rbac import User
    from console_server.utils.auth import get_password_hash, issue_session_tokens
    from console_server.ws.server import sio

import test_user_crud as user_tests  # AsyncSessionAdapter
from test_ws import _PatchedSessionFactory, _make_user

PASSWORD_HASH = get_password_hash("password123")


class NotificationWsE2ETests(unittest.IsolatedAsyncioTestCase):
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

        self.session_patch = patch.object(database, "AsyncSessionLocal", session_factory)
        self.session_patch.start()
        self.addCleanup(self.session_patch.stop)

        # HTTP 依赖 get_current_user 走 database.get_db，同样替换为 SQLite 会话
        async def override_get_db():
            yield user_tests.AsyncSessionAdapter(self.db_session)

        from console_server.main import app, socket_app
        app.dependency_overrides[database.get_db] = override_get_db
        self.addCleanup(app.dependency_overrides.pop, database.get_db)

        self._old_manager = sio.manager
        sio.manager = socketio.AsyncManager()
        sio.manager.set_server(sio)
        self.addCleanup(self._restore_manager)

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

    async def test_self_test_notification_reaches_ws_client(self):
        client = socketio.AsyncClient()
        received = asyncio.Queue()

        @client.on("notice")
        def _on_notice(data):
            received.put_nowait(data)

        await client.connect(
            f"http://127.0.0.1:{self.port}", auth={"token": self.token},
            socketio_path="socket.io", wait_timeout=5,
        )
        try:
            async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{self.port}") as http:
                response = await http.post(
                    "/api/v1/notifications/self-test",
                    headers={"Authorization": f"Bearer {self.token}"},
                )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["detail"], "通知已通过 WebSocket 推送")

            payload = await asyncio.wait_for(received.get(), timeout=5)
            self.assertEqual(payload["title"], "测试通知")
            self.assertEqual(payload["level"], "info")
        finally:
            await client.disconnect()

    async def test_notification_not_received_by_other_users(self):
        other_client = socketio.AsyncClient()
        received = asyncio.Queue()

        @other_client.on("notice")
        def _on_notice(data):
            received.put_nowait(data)

        await other_client.connect(
            f"http://127.0.0.1:{self.port}", auth={"token": self.token},
            socketio_path="socket.io", wait_timeout=5,
        )
        try:
            # 未触发通知前不应收到任何 notice
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(received.get(), timeout=1)
        finally:
            await other_client.disconnect()


if __name__ == "__main__":
    unittest.main()
