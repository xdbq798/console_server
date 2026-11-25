"""请求日志中间件。

为每个 HTTP 请求记录一行日志：request_id、方法、路径、状态码、耗时，
并在响应头中返回 X-Request-ID，便于客户端报障时反查日志。
"""

import time
import uuid

from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

log = logger.bind(log_source="main")


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        request_id = uuid.uuid4().hex[:8]
        start = time.perf_counter()

        try:
            response = await call_next(request)
        except Exception:
            cost_ms = (time.perf_counter() - start) * 1000
            log.bind(request_id=request_id).exception(
                f"{request.method} {request.url.path} -> 500 {cost_ms:.1f}ms"
            )
            raise

        cost_ms = (time.perf_counter() - start) * 1000
        level = "WARNING" if response.status_code >= 500 else "INFO"
        log.bind(request_id=request_id).log(
            level,
            f"{request.method} {request.url.path} -> {response.status_code} {cost_ms:.1f}ms",
        )
        response.headers["X-Request-ID"] = request_id
        return response
