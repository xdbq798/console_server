"""系统通知接口。

通知通过 WebSocket（Socket.IO）实时推送给在线用户，
离线用户不会收到历史补偿（无持久化），属尽力而为的旁路通知。
"""

from datetime import datetime, timezone
from typing import cast
from uuid import uuid4

from fastapi import APIRouter, Depends
from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from console_server.model.rbac import Permission, Role, User
from console_server.schema.common import SuccessResponse
from console_server.schema.notifications import NotificationPayload
from console_server.utils.auth import ADMIN_API, get_current_user
from console_server.ws.manager import push_to_user

NOTICE_PATH = "notifications"

router = APIRouter(prefix=f"/{NOTICE_PATH}", tags=[NOTICE_PATH])

log = logger.bind(log_source="notifications")


@router.post(
    "/self-test",
    summary="发送测试系统通知",
    description="向当前登录用户推送一条测试通知，用于验证 WebSocket 推送链路",
    response_model=SuccessResponse,
)
async def send_self_test_notification(
    current_user: User = Depends(get_current_user),
) -> SuccessResponse:
    payload = NotificationPayload(
        id=str(uuid4()),
        title="测试通知",
        content=f"{current_user.name}，这是一条实时推送的系统通知（{datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC）",
        level="info",
        created_at=datetime.now(timezone.utc),
    )
    await push_to_user(str(current_user.uuid), "notice", payload.model_dump(mode="json"))
    return SuccessResponse(detail="通知已通过 WebSocket 推送")


async def notify_user_status_to_admins(
    db: AsyncSession, *, actor_name: str, target_name: str, is_active: bool
) -> None:
    """用户停用/启用后通知拥有全局管理权限（api:*）的激活管理员。

    - 只推送给在线管理员：离线投递无消费方，安全跳过（无持久化补偿）。
    - 任何异常只记录日志，不影响已提交完成的业务操作。
    """
    try:
        result = await db.execute(
            select(User.uuid)
            .join(User.roles)
            .join(Role.permissions)
            .where(Permission.name == ADMIN_API, User.is_active.is_(True))
        )
        admin_uuids = result.scalars().all()
    except Exception as e:
        log.warning(f"查询管理员列表失败，跳过用户状态通知：{e}")
        return
    if not admin_uuids:
        return

    action = "停用" if not is_active else "启用"
    payload = NotificationPayload(
        id=str(uuid4()),
        title=f"用户已{action}",
        content=f"{actor_name} 已{action}用户「{target_name}」",
        level="warning" if not is_active else "success",
        created_at=datetime.now(timezone.utc),
    )
    for admin_uuid in admin_uuids:
        await push_to_user(str(admin_uuid), "notice", payload.model_dump(mode="json"))


async def notify_user_deactivated_to_self(target: User, *, actor_name: str) -> None:
    """停用后通知被停用的用户本人，并通过 force_logout 事件让前端强制登出。

    - 走独立事件而非 "notice"：前端需要按事件语义执行登出流程，而不只是弹窗。
    - 用户此刻仍在线（连接尚未被服务端断开），推送尽力而为；
      push_to_user 内部吞异常，不会影响已提交完成的业务操作。
    """
    payload = NotificationPayload(
        id=str(uuid4()),
        title="账号已停用",
        content=f"您的账号已被 {actor_name} 停用，如有疑问请联系管理员",
        level="error",
        created_at=datetime.now(timezone.utc),
    )
    await push_to_user(str(target.uuid), "force_logout", payload.model_dump(mode="json"))
