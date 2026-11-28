"""系统通知 schema。"""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class NotificationPayload(BaseModel):
    """WS "notice" 事件的推送载荷；前端 SystemNotification 按此渲染。"""

    id: str = Field(description="通知唯一标识（uuid）")
    title: str = Field(min_length=1, max_length=100, description="通知标题")
    content: str = Field(min_length=1, max_length=500, description="通知内容")
    level: Literal["info", "success", "warning", "error"] = Field(
        default="info", description="通知级别，对应前端弹窗样式"
    )
    created_at: datetime = Field(description="通知产生时间（UTC）")
