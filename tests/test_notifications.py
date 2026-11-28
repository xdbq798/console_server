"""系统通知接口测试：认证、推送调用与载荷格式。"""

import io
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

import httpx

with redirect_stdout(io.StringIO()):
    import test_auth_tokens as token_tests
    from console_server.api.v1.endpoints import notifications
    from console_server.ws import manager


class NotificationEndpointTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.fixture = token_tests.TokenIdentityTests()
        self.fixture.password_hash = "unused-for-notifications"
        self.fixture.setUp()
        self.fixture.app.include_router(notifications.router, prefix="/api/v1")
        self.token = self.fixture.tokens()["access_token"]
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.fixture.app), base_url="http://test"
        )

    async def asyncTearDown(self):
        await self.client.aclose()
        self.fixture.tearDown()

    async def request(self, *, token=None):
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        return await self.client.post("/api/v1/notifications/self-test", headers=headers)

    async def test_pushes_notice_payload_to_current_user(self):
        pushed = []

        async def fake_push(user_id, event, data):
            pushed.append((user_id, event, data))

        # 端点模块直接导入了 push_to_user，需 patch 模块命名空间内的引用
        with patch.object(notifications, "push_to_user", fake_push):
            response = await self.request(token=self.token)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["detail"], "通知已通过 WebSocket 推送")
        self.assertEqual(len(pushed), 1)
        user_id, event, data = pushed[0]
        self.assertEqual(user_id, str(self.fixture.user_uuid))
        self.assertEqual(event, "notice")
        self.assertEqual(data["title"], "测试通知")
        self.assertEqual(data["level"], "info")
        self.assertIn("id", data)
        self.assertIn("created_at", data)

    async def test_requires_authentication(self):
        with patch.object(manager, "push_to_user") as fake_push:
            response = await self.request()
        self.assertEqual(response.status_code, 401)
        fake_push.assert_not_awaited()

    async def test_rejects_invalid_token(self):
        with patch.object(manager, "push_to_user") as fake_push:
            response = await self.request(token="not-a-jwt")
        self.assertEqual(response.status_code, 401)
        fake_push.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()


class NotifyUserStatusToAdminsTests(unittest.IsolatedAsyncioTestCase):
    """用户停用/启用通知：管理员圈定、载荷内容与异常安全。"""

    def setUp(self):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import Session
        from console_server.model.common import Base
        from console_server.model.rbac import Permission, Role, User
        from console_server.utils.auth import get_password_hash

        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.password_hash = get_password_hash("password123")
        with Session(self.engine) as db:
            # api:* 权限名唯一：所有管理员共享同一个全局管理员角色
            self.admin_role = Role(name="global_admin", display_name="全局管理员", is_active=True,
                                   permissions=[Permission(name="api:*", display_name="全局管理")])
            db.add(self.admin_role)
            self.admin_uuid = str(self._make_user(db, "admin", "admin@example.com").uuid)
            self.other_admin_uuid = str(self._make_user(db, "admin2", "admin2@example.com").uuid)
            self._make_user(db, "plain", "plain@example.com", roles=[])
            # 被停用的管理员不应收到通知
            self._make_user(db, "off_admin", "off@example.com", is_active=False)
            db.commit()

    def _make_user(self, db, name, email, *, is_active=True, is_editable=False, roles=None):
        from console_server.model.rbac import User
        from console_server.utils.auth import get_password_hash
        user = User(name=name, email=email, password=get_password_hash("password123"),
                    is_active=is_active, is_editable=is_editable)
        if roles is None:
            roles = [self.admin_role]
        user.roles = roles
        db.add(user)
        db.commit()
        return user

    async def asyncTearDown(self):
        self.engine.dispose()

    async def test_notifies_only_active_admins_on_deactivation(self):
        import asyncio
        from console_server.api.v1.endpoints.notifications import notify_user_status_to_admins

        pushed = []

        async def fake_push(user_id, event, data):
            pushed.append((user_id, event, data))

        from sqlalchemy.orm import Session
        import test_user_crud as user_tests
        with patch.object(notifications, "push_to_user", fake_push):
            with Session(self.engine) as db:
                await notify_user_status_to_admins(
                    user_tests.AsyncSessionAdapter(db),
                    actor_name="操作者", target_name="张三", is_active=False,
                )

        notified = {user_id for user_id, _, _ in pushed}
        # 激活的管理员都收到；被停用的管理员和普通用户不收到
        self.assertEqual(notified, {self.admin_uuid, self.other_admin_uuid})
        for _, event, data in pushed:
            self.assertEqual(event, "notice")
            self.assertEqual(data["title"], "用户已停用")
            self.assertEqual(data["level"], "warning")
            self.assertEqual(data["content"], "操作者 已停用用户「张三」")
        # 同一条通知的多次投递共用同一个 id
        self.assertEqual(len({data["id"] for _, _, data in pushed}), 1)

    async def test_enable_notification_uses_success_level(self):
        from console_server.api.v1.endpoints.notifications import notify_user_status_to_admins

        pushed = []

        async def fake_push(user_id, event, data):
            pushed.append(data)

        from sqlalchemy.orm import Session
        import test_user_crud as user_tests
        with patch.object(notifications, "push_to_user", fake_push):
            with Session(self.engine) as db:
                await notify_user_status_to_admins(
                    user_tests.AsyncSessionAdapter(db),
                    actor_name="操作者", target_name="张三", is_active=True,
                )

        self.assertEqual(len(pushed), 2)
        self.assertEqual(pushed[0]["title"], "用户已启用")
        self.assertEqual(pushed[0]["level"], "success")

    async def test_query_failure_never_raises(self):
        from console_server.api.v1.endpoints.notifications import notify_user_status_to_admins

        class BrokenSession:
            async def execute(self, statement):
                raise RuntimeError("db down")

        with patch.object(notifications, "push_to_user") as fake_push:
            # 不应抛出异常，也不应推送
            await notify_user_status_to_admins(
                BrokenSession(), actor_name="操作者", target_name="张三", is_active=False,
            )
        fake_push.assert_not_awaited()


class NotifyUserDeactivatedToSelfTests(unittest.IsolatedAsyncioTestCase):
    """被停用用户本人的通知：走 force_logout 事件，载荷面向本人。"""

    async def test_pushes_force_logout_event_to_target(self):
        from uuid import uuid4

        from console_server.api.v1.endpoints.notifications import (
            notify_user_deactivated_to_self,
        )
        from console_server.model.rbac import User

        target = User(name="张三", uuid=uuid4())
        pushed = []

        async def fake_push(user_id, event, data):
            pushed.append((user_id, event, data))

        with patch.object(notifications, "push_to_user", fake_push):
            await notify_user_deactivated_to_self(target, actor_name="操作者")

        self.assertEqual(len(pushed), 1)
        user_id, event, data = pushed[0]
        self.assertEqual(user_id, str(target.uuid))
        self.assertEqual(event, "force_logout")
        self.assertEqual(data["title"], "账号已停用")
        self.assertEqual(data["level"], "error")
        self.assertEqual(data["content"], "您的账号已被 操作者 停用，如有疑问请联系管理员")


class UserStatusHookTests(unittest.IsolatedAsyncioTestCase):
    """停用/启用用户的接口在状态翻转时触发管理员通知。"""

    def setUp(self):
        from sqlalchemy.orm import Session
        self.fixture = token_tests.TokenIdentityTests()
        self.fixture.password_hash = "unused-for-notifications"
        self.fixture.setUp()
        from console_server.api.v1.endpoints import user as user_endpoint
        from console_server.model.rbac import Permission, Role, User
        self.user_endpoint = user_endpoint
        self.fixture.app.include_router(user_endpoint.router, prefix="/api/v1")
        with Session(self.fixture.engine) as db:
            admin = db.get(User, 1)
            admin.roles = [Role(name="admin", display_name="管理员", is_active=True,
                                permissions=[Permission(name="api:*", display_name="全局管理")])]
            target = db.get(User, 2)
            target.is_editable = True
            db.commit()
        self.token = self.fixture.tokens()["access_token"]
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.fixture.app), base_url="http://test"
        )

    async def asyncTearDown(self):
        await self.client.aclose()
        self.fixture.tearDown()

    async def test_deactivation_triggers_admin_notification(self):
        captured = []

        async def fake_notify(db, *, actor_name, target_name, is_active):
            captured.append((actor_name, target_name, is_active))

        with patch.object(self.user_endpoint, "notify_user_status_to_admins", fake_notify):
            response = await self.client.put(
                "/api/v1/user/2/disable",
                json={"is_active": False},
                headers={"Authorization": f"Bearer {self.token}"},
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(captured), 1)
        actor_name, target_name, is_active = captured[0]
        self.assertEqual(is_active, False)
        self.assertEqual(target_name, "other")

    async def test_no_notification_without_status_change(self):
        with patch.object(self.user_endpoint, "notify_user_status_to_admins") as fake, \
             patch.object(self.user_endpoint, "notify_user_deactivated_to_self") as fake_self:
            response = await self.client.put(
                "/api/v1/user/2/disable",
                json={"is_active": True},
                headers={"Authorization": f"Bearer {self.token}"},
            )
        self.assertEqual(response.status_code, 200)
        fake.assert_not_awaited()
        fake_self.assert_not_awaited()

    async def test_deactivation_notifies_target_user_via_disable(self):
        captured = []

        async def fake_self_notify(target, *, actor_name):
            captured.append((target.name, actor_name))

        with patch.object(self.user_endpoint, "notify_user_status_to_admins"), \
             patch.object(self.user_endpoint, "notify_user_deactivated_to_self", fake_self_notify):
            response = await self.client.put(
                "/api/v1/user/2/disable",
                json={"is_active": False},
                headers={"Authorization": f"Bearer {self.token}"},
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(captured), 1)
        target_name, actor_name = captured[0]
        self.assertEqual(target_name, "other")
        self.assertEqual(actor_name, "original")

    async def test_deactivation_notifies_target_user_via_update(self):
        captured = []

        async def fake_self_notify(target, *, actor_name):
            captured.append(target.name)

        with patch.object(self.user_endpoint, "notify_user_status_to_admins"), \
             patch.object(self.user_endpoint, "notify_user_deactivated_to_self", fake_self_notify):
            response = await self.client.put(
                "/api/v1/user/2/update",
                json={"name": "renamed", "email": "other@example.com",
                      "description": "", "is_active": False},
                headers={"Authorization": f"Bearer {self.token}"},
            )
        self.assertEqual(response.status_code, 200)
        # 通知的是提交后的新名字
        self.assertEqual(captured, ["renamed"])

    async def test_enable_does_not_notify_target_user(self):
        with patch.object(self.user_endpoint, "notify_user_status_to_admins"), \
             patch.object(self.user_endpoint, "notify_user_deactivated_to_self") as fake_self:
            response = await self.client.put(
                "/api/v1/user/2/disable",
                json={"is_active": True},
                headers={"Authorization": f"Bearer {self.token}"},
            )
        self.assertEqual(response.status_code, 200)
        fake_self.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
